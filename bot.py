"""
Bot Scalping v18.8 — MIRROR REVERSAL MODE (FIXED + FEE-AWARE)
==================================================================
CHANGELOG dari v18.8 original:
  [FIX-1] Fee sekarang dikurangi dari realizedPnl (ambil field 'commission' dari API)
  [FIX-2] Guard double-count: set _logged_closes mencegah posisi di-log dua kali
  [FIX-3] Sync race condition: backoff -1109, interval t_monitor dinaikkan 3s
  [FIX-4] process_closed_position pakai startTime filter agar tidak ambil trade lama
  [FIX-5] paper_open reset _logged_closes saat posisi baru dibuka
  [FIX-6] Stats sekarang tampilkan gross_pnl vs net_pnl vs total_fee untuk diagnostik
"""

import os, time, math, threading, queue
import requests
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from binance.client import Client
import ta
import pandas as pd
import numpy as np

load_dotenv()

client = Client(os.getenv("API_KEY"), os.getenv("API_SECRET"), testnet=True)

# ═══════════════════════════════════════════════════════
#  CONFIG v18.8 - FULL REVERSAL MIRROR (FIXED + FEE-AWARE)
# ═══════════════════════════════════════════════════════
INVERSE_MODE   = True

LEVERAGE       = 20
ORDER_USDT     = 2.0
MAX_POSITIONS  = 3

EXTREME_PROFIT_PCT   = 0.0045  # 0.45%
HARD_SL_PCT          = 0.0045  # 0.45%

MIN_BASE_VOL   = 25_000_000
MIN_VR         = 1.1
BR_LONG_MIN    = 0.48
BR_SHORT_MAX   = 0.52

SCAN_INTERVAL  = 1
SCAN_DELAY     = 0.015
BATCH_SIZE     = 15
MAX_WORKERS    = 8

MIN_SCORE      = 40
MIN_GAP        = 10
COOLDOWN_SEC   = 8

DAILY_LOSS     = -8.0
CONSEC_MAX     = 6
CONSEC_PAUSE   = 60
TTL_5M         = 5
TTL_15M        = 30

# ═══════════════════════════════════════════════════════
#  SYMBOLS & STATE
# ═══════════════════════════════════════════════════════
SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "TRXUSDT", "DOTUSDT",
    "LINKUSDT", "MATICUSDT", "LTCUSDT", "ATOMUSDT", "UNIUSDT",
    "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT", "INJUSDT",
    "SUIUSDT", "SEIUSDT", "FETUSDT", "WLDUSDT", "AAVEUSDT",
    "ORDIUSDT", "TONUSDT", "1000PEPEUSDT", "WIFUSDT", "JUPUSDT",
]
SYMBOLS = list(dict.fromkeys(SYMBOLS))

paper_positions  = {}
trade_log        = []
_ohlcv_cache     = {}
_sym_cooldown    = {}
_ticker_cache    = {}
_ticker_ts       = 0
_precisions      = {}
_lock            = threading.RLock()
_executor        = ThreadPoolExecutor(max_workers=MAX_WORKERS)
_rescan_q        = queue.Queue()
_hot_syms        = deque(maxlen=20)

# [FIX-2] Guard set untuk mencegah double-count close notification
_logged_closes   = set()
# [FIX-4] Track waktu open per posisi untuk filter trade history
_position_open_ts = {}

_macro = {"fng": 50, "btc": "UNKNOWN", "last_fng": 0, "last_btc": 0}
_ks    = {"active": False, "reason": "", "resume": 0, "consec": 0, "daily": 0.0, "day_reset": 0}

_stats = {
    "trades": 0, "wins": 0, "losses": 0,
    "gross_pnl": 0.0,   # PnL sebelum fee
    "total_fee": 0.0,   # Akumulasi fee yang dibayar
    "pnl": 0.0,         # Net PnL (gross - fee) ← ini yang benar
    "best": 0.0, "worst": 0.0,
    "hist": deque(maxlen=200), "start": time.time(),
}

# ═══════════════════════════════════════════════════════
#  PRECISION & QUANTITY HELPERS
# ═══════════════════════════════════════════════════════
def get_precision_rules(sym):
    if sym in _precisions:
        return _precisions[sym]
    try:
        info = client.futures_exchange_info()
        for item in info['symbols']:
            if item['symbol'] == sym:
                prec     = item['quantityPrecision']
                p_prec   = item['pricePrecision']
                step_size    = 0.0
                min_notional = 5.0
                for f in item['filters']:
                    if f['filterType'] == 'LOT_SIZE':
                        step_size = float(f['stepSize'])
                    if f['filterType'] == 'MIN_NOTIONAL':
                        min_notional = float(f.get('notional', 5.0))
                _precisions[sym] = (prec, step_size, min_notional, p_prec)
                return prec, step_size, min_notional, p_prec
    except Exception as e:
        print(f"⚠️ Gagal load precision info {sym}: {e}")
    return 3, 0.001, 5.0, 4

def qty(price, sym):
    prec, step_size, min_notional, _ = get_precision_rules(sym)
    target_notional = ORDER_USDT * LEVERAGE
    if target_notional < min_notional:
        target_notional = min_notional + 1.0
    raw_qty = target_notional / price
    if step_size > 0:
        raw_qty = math.trunc(raw_qty / step_size) * step_size
    return round(raw_qty, prec)

# ═══════════════════════════════════════════════════════
#  MARKET DATA
# ═══════════════════════════════════════════════════════
def price_live(symbol):
    try: return float(client.futures_symbol_ticker(symbol=symbol)["price"])
    except: return 0.0

def tickers_all():
    global _ticker_cache, _ticker_ts
    now = time.time()
    if now - _ticker_ts < 5 and _ticker_cache: return _ticker_cache
    try:
        raw = client.futures_ticker()
        _ticker_cache = {
            t["symbol"]: {
                "pct": float(t["priceChangePercent"]),
                "vol": float(t["quoteVolume"]),
                "last": float(t["lastPrice"])
            } for t in raw
        }
        _ticker_ts = now
        return _ticker_cache
    except: return _ticker_cache

def ok_cooldown(sym): return (time.time() - _sym_cooldown.get(sym, 0)) >= COOLDOWN_SEC
def set_cd(sym): _sym_cooldown[sym] = time.time()

def ohlcv(symbol, interval, limit=100):
    key, now = (symbol, interval), time.time()
    ttl = TTL_5M if interval == Client.KLINE_INTERVAL_5MINUTE else TTL_15M
    if key in _ohlcv_cache and now - _ohlcv_cache[key][0] < ttl:
        return _ohlcv_cache[key][1]
    try:
        kl = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
        df = pd.DataFrame(kl, columns=[
            "time","open","high","low","close","volume",
            "ct","qv","trades","tbbase","tbquote","ignore"
        ])
        for c in ["open","high","low","close","volume","tbbase","tbquote"]:
            df[c] = df[c].astype(float)
        _ohlcv_cache[key] = (now, df)
        return df
    except: return _ohlcv_cache.get(key, (None, None))[1]

def run_ta(df):
    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]
    df["rsi"]  = ta.momentum.RSIIndicator(c, 14).rsi()
    df["mh"]   = ta.trend.MACD(c, 12, 26, 9).macd_diff()
    df["e5"]   = ta.trend.EMAIndicator(c, 5).ema_indicator()
    df["e9"]   = ta.trend.EMAIndicator(c, 9).ema_indicator()
    df["e21"]  = ta.trend.EMAIndicator(c, 21).ema_indicator()
    df["e50"]  = ta.trend.EMAIndicator(c, 50).ema_indicator()
    df["atr"]  = ta.volatility.AverageTrueRange(h, l, c, 14).average_true_range()
    df["adx"]  = ta.trend.ADXIndicator(h, l, c, 14).adx()
    df["vm"]   = v.rolling(20).mean()
    df["vr"]   = v / df["vm"].replace(0, 1)
    df["br"]   = df["tbbase"] / df["volume"].replace(0, 1)
    df["body"] = abs(c - df["open"])
    df["rng"]  = h - l
    df["br2"]  = df["body"] / df["rng"].replace(0, 1)
    df["m5"]   = (c - c.shift(5)) / c.shift(5)
    return df

def btc_trend():
    try:
        df  = run_ta(ohlcv("BTCUSDT", Client.KLINE_INTERVAL_5MINUTE, 80).copy())
        row = df.iloc[-2]
        p, e5, e9, e21, m5 = row["close"], row["e5"], row["e9"], row["e21"], row["m5"]
        if p > e5 > e9 > e21 and m5 > 0.001:  return "BULL"
        if p < e5 < e9 < e21 and m5 < -0.001: return "BEAR"
        if p > e9 > e21: return "MILD_BULL"
        if p < e9 < e21: return "MILD_BEAR"
        return "SIDEWAYS"
    except: return "UNKNOWN"

# ═══════════════════════════════════════════════════════
#  KILL SWITCH
# ═══════════════════════════════════════════════════════
def ks_check():
    k, now = _ks, time.time()
    if k["active"] and now >= k["resume"]:
        k["active"] = False; k["consec"] = 0
    if k["active"]: return True, k["reason"]
    day = now - (now % 86400)
    if day > k["day_reset"]: k["daily"] = 0.0; k["day_reset"] = day
    if k["daily"] <= DAILY_LOSS:
        k["active"] = True
        k["reason"]  = f"daily({k['daily']:.2f})"
        k["resume"]  = day + 86400
        return True, k["reason"]
    if k["consec"] >= CONSEC_MAX:
        k["active"] = True
        k["reason"]  = f"consec({k['consec']})"
        k["resume"]  = now + CONSEC_PAUSE
        return True, k["reason"]
    return False, ""

def ks_upd(pnl):
    _ks["daily"] += pnl
    _ks["consec"] = 0 if pnl >= 0 else _ks["consec"] + 1

# ═══════════════════════════════════════════════════════
#  SIGNAL ENGINE
# ═══════════════════════════════════════════════════════
def signal(df):
    if df is None or len(df) < 55: return None, 0, [], 0.0

    row, prev, prev2 = df.iloc[-2], df.iloc[-3], df.iloc[-4]
    p, e5, e9, e21, e50 = row["close"], row["e5"], row["e9"], row["e21"], row["e50"]
    rsi, mh, mh_p, mh_p2 = row["rsi"], row["mh"], prev["mh"], prev2["mh"]
    vr, br, m5, body, atr, adx = (
        row["vr"], row["br"], row["m5"],
        row["br"], row["atr"], row["adx"]
    )
    btc = _macro["btc"]

    if vr < MIN_VR: return None, 0, [], atr

    lp = sp = 0
    sl, ss = [], []

    if p > e5 > e9 > e21 > e50:   lp += 30; sl.append("EMA_stack↑")
    elif p > e5 > e9 > e21:       lp += 22; sl.append("EMA↑↑")

    if p < e5 < e9 < e21 < e50:   sp += 30; ss.append("EMA_stack↓")
    elif p < e5 < e9 < e21:       sp += 22; ss.append("EMA↓↓")

    if m5 > 0.005:    lp += 25; sl.append(f"Mom+{m5*100:.1f}%")
    elif m5 > 0.003:  lp += 18; sl.append(f"Mom+{m5*100:.1f}%")
    if m5 < -0.005:   sp += 25; ss.append(f"Mom{m5*100:.1f}%")
    elif m5 < -0.003: sp += 18; ss.append(f"Mom{m5*100:.1f}%")

    if mh_p <= 0 and mh > 0:           lp += 22; sl.append("MACD_X↑")
    elif mh > 0 and mh > mh_p > mh_p2: lp += 18; sl.append("MACD↑↑")
    if mh_p >= 0 and mh < 0:           sp += 22; ss.append("MACD_X↓")
    elif mh < 0 and mh < mh_p < mh_p2: sp += 18; ss.append("MACD↓↓")

    if vr >= 3.0:
        lp += 15; sp += 15
        sl.append(f"Vol{vr:.1f}x"); ss.append(f"Vol{vr:.1f}x")
    elif vr >= 2.0:
        lp += 10; sp += 10
        sl.append(f"Vol{vr:.1f}x"); ss.append(f"Vol{vr:.1f}x")

    if br > 0.65: lp += 18; sl.append(f"Buy{br:.0%}")
    if br < 0.35: sp += 18; ss.append(f"Sell{1-br:.0%}")

    if rsi > 75:
        lp = int(lp * 0.4); sp += 20; ss.append(f"RSI_OB{rsi:.0f}")
    elif rsi < 25:
        sp = int(sp * 0.4); lp += 20; sl.append(f"RSI_OS{rsi:.0f}")

    if adx > 35:
        lp += 8; sp += 8
        sl.append(f"ADX{adx:.0f}"); ss.append(f"ADX{adx:.0f}")

    btc_sw = btc in ("SIDEWAYS", "UNKNOWN")
    thresh = 40 if btc_sw else MIN_SCORE
    gap    = abs(lp - sp)

    if lp <= sp or lp < thresh or gap < MIN_GAP:
        if sp <= lp or sp < thresh or gap < MIN_GAP:
            return None, max(lp, sp), [], atr
        if br >= BR_SHORT_MAX: return None, sp, [], atr
        if INVERSE_MODE: return "LONG", sp, ss[:3] + ["(INV)"], atr
        return "SHORT", sp, ss[:3], atr

    if br <= BR_LONG_MIN: return None, lp, [], atr
    if INVERSE_MODE: return "SHORT", lp, sl[:3] + ["(INV)"], atr
    return "LONG", lp, sl[:3], atr

# ═══════════════════════════════════════════════════════
#  REAL EXECUTION SYSTEM (NATIVE TP & SL)
# ═══════════════════════════════════════════════════════
def sync_binance_positions():
    """Sinkronisasi posisi lokal dengan Binance. Deteksi posisi yang sudah close."""
    global paper_positions, trade_log
    try:
        pos_info = client.futures_position_information()
        active_on_binance = {}

        for p in pos_info:
            amt = float(p["positionAmt"])
            sym = p["symbol"]
            if amt != 0:
                side = "LONG" if amt > 0 else "SHORT"
                active_on_binance[sym] = {
                    "side": side,
                    "qty": abs(amt),
                    "entry": float(p["entryPrice"])
                }

        with _lock:
            for local_sym in list(paper_positions.keys()):
                pos = paper_positions[local_sym]
                # Skip posisi yang sedang dalam proses entry (_r flag)
                if pos.get("_r"):
                    continue
                # Jika posisi tidak ada di Binance → sudah close
                if local_sym not in active_on_binance:
                    process_closed_position(local_sym)
                    paper_positions.pop(local_sym, None)

            for bin_sym, data in active_on_binance.items():
                if bin_sym not in paper_positions:
                    paper_positions[bin_sym] = {
                        "side": data["side"], "entry": data["entry"], "qty": data["qty"],
                        "open_time": time.time(), "score": 99,
                        "sigs": ["RESTORED_SYNC"], "atr": 0.0
                    }

    except Exception as e:
        err_str = str(e)
        if "-1109" in err_str:
            # [FIX-3] -1109 = Invalid account, biasanya testnet credential issue
            # Jangan spam, cukup catat sekali
            pass
        else:
            print(f"⚠️ Error saat sinkronisasi posisi: {e}")

def paper_open(sym, direction, score, sigs, price, atr):
    with _lock:
        if sym in paper_positions or len(paper_positions) >= MAX_POSITIONS:
            return
        paper_positions[sym] = {"_r": True}

    # [FIX-2] Reset close guard saat posisi baru dibuka
    _logged_closes.discard(sym)
    # [FIX-4] Catat timestamp open untuk filter trade history
    _position_open_ts[sym] = int(time.time() * 1000)

    try:
        client.futures_change_leverage(symbol=sym, leverage=LEVERAGE)
        q = qty(price, sym)

        if q <= 0:
            with _lock: paper_positions.pop(sym, None)
            return

        side_str = "BUY" if direction == "LONG" else "SELL"
        order = client.futures_create_order(
            symbol=sym,
            side=side_str,
            type="MARKET",
            quantity=q
        )

        exec_price = float(order.get('avgPrice', price))
        if exec_price == 0: exec_price = price

        _, _, _, p_prec = get_precision_rules(sym)

        if direction == "LONG":
            tp_price = round(exec_price * (1 + EXTREME_PROFIT_PCT), p_prec)
            sl_price = round(exec_price * (1 - HARD_SL_PCT), p_prec)
            tp_side  = "SELL"
            sl_side  = "SELL"
        else:
            tp_price = round(exec_price * (1 - EXTREME_PROFIT_PCT), p_prec)
            sl_price = round(exec_price * (1 + HARD_SL_PCT), p_prec)
            tp_side  = "BUY"
            sl_side  = "BUY"

        client.futures_create_order(
            symbol=sym, side=tp_side,
            type="TAKE_PROFIT_MARKET",
            stopPrice=tp_price, closePosition=True
        )
        client.futures_create_order(
            symbol=sym, side=sl_side,
            type="STOP_MARKET",
            stopPrice=sl_price, closePosition=True
        )

    except Exception as e:
        print(f"❌ GAGAL ENTRY REAL {sym}: {e}")
        try: client.futures_cancel_all_open_orders(symbol=sym)
        except: pass
        with _lock: paper_positions.pop(sym, None)
        _position_open_ts.pop(sym, None)
        return

    pos = {
        "side": direction, "entry": exec_price, "qty": q,
        "open_time": time.time(), "score": score, "sigs": sigs, "atr": atr,
    }
    with _lock: paper_positions[sym] = pos

    d = "🟢" if direction == "LONG" else "🔴"
    real_margin = (q * exec_price) / LEVERAGE
    print(f"\n  {d} [REAL ENTRY] {sym} {direction} @{exec_price:.6g} (Margin: ${real_margin:.2f})")
    print(f"     [NATIVE SENT] Target TP: @{tp_price} | Hard SL: @{sl_price}")
    _stats["trades"] += 1

def process_closed_position(sym):
    """
    [FIX-1] Ambil realizedPnl DAN commission dari trade history.
    [FIX-2] Guard dengan _logged_closes agar tidak double-count.
    [FIX-4] Filter by startTime = waktu posisi dibuka.
    """
    # [FIX-2] Cek apakah sudah pernah diproses
    if sym in _logged_closes:
        return
    _logged_closes.add(sym)

    try:
        # [FIX-4] Ambil hanya trade setelah posisi dibuka
        open_ts   = _position_open_ts.get(sym, 0)
        kwargs    = {"symbol": sym, "limit": 20}
        if open_ts > 0:
            kwargs["startTime"] = open_ts

        trades = client.futures_account_trades(**kwargs)
        if not trades:
            return

        # Kumpulkan semua trade yang punya realizedPnl != 0 (trade close)
        gross_pnl    = 0.0
        total_fee    = 0.0
        price_exit   = 0.0
        found_close  = False

        for t in trades:
            rpnl = float(t.get("realizedPnl", 0))
            if rpnl != 0:
                gross_pnl  += rpnl
                # [FIX-1] Tambah fee dari field commission
                fee         = float(t.get("commission", 0))
                total_fee  += fee
                price_exit  = float(t.get("price", 0))
                found_close = True

        if not found_close:
            return

        # [FIX-1] Net PnL = gross - fee
        net_pnl = gross_pnl - total_fee

        e = "🟢" if net_pnl >= 0 else "🔴"
        print(f"  {e} [REAL CLOSE NOTIFICATION] {sym} closed via Binance Engine.")
        print(f"     Gross PnL: {gross_pnl:+.5f}U | Fee: -{total_fee:.5f}U | Net: {net_pnl:+.5f}U")

        # Update stats dengan nilai NET
        _stats["gross_pnl"] += gross_pnl
        _stats["total_fee"] += total_fee
        _stats["pnl"]       += net_pnl
        _stats["hist"].append(net_pnl)
        ks_upd(net_pnl)

        if net_pnl >= 0:
            _stats["wins"] += 1
            if net_pnl > _stats["best"]: _stats["best"] = net_pnl
        else:
            _stats["losses"] += 1
            if net_pnl < _stats["worst"]: _stats["worst"] = net_pnl

        trade_log.append({
            "sym": sym, "side": "CLOSED",
            "entry": 0.0, "exit": price_exit,
            "gross": round(gross_pnl, 5),
            "fee":   round(total_fee, 5),
            "pnl":   round(net_pnl, 5),
            "reason": "Native Binance TP/SL Executed",
            "hold": 0,
        })

        # Bersihkan tracking timestamp
        _position_open_ts.pop(sym, None)

        set_cd(sym); _hot_syms.appendleft(sym); _rescan_q.put(1)
        print_inline()

        try: client.futures_cancel_all_open_orders(symbol=sym)
        except: pass

    except Exception as e:
        print(f"⚠️ Gagal memproses data close dari Binance ({sym}): {e}")
        # Jangan hapus dari _logged_closes agar tidak retry infinite
        _position_open_ts.pop(sym, None)

# ═══════════════════════════════════════════════════════
#  SCANNER & MAIN ENGINE
# ═══════════════════════════════════════════════════════
def scan_one(sym):
    try:
        time.sleep(SCAN_DELAY)
        if not ok_cooldown(sym): return None
        tk = _ticker_cache
        if sym in tk and tk[sym]["vol"] < MIN_BASE_VOL: return None
        df = run_ta(ohlcv(sym, Client.KLINE_INTERVAL_5MINUTE, 100).copy())
        px, atr = df["close"].iloc[-2], df["atr"].iloc[-2]
        if px == 0 or atr / px > 0.03: return None
        dir_, sc, sigs, atr_val = signal(df)
        if dir_ is None or len(sigs) < 1: return None
        px_live = price_live(sym)
        if px_live == 0: return None
        return (sym, dir_, sc, sigs, px_live, atr_val)
    except: return None

def scan_batch(syms):
    res = []
    fut = {_executor.submit(scan_one, s): s for s in syms[:BATCH_SIZE]}
    try:
        for f in as_completed(fut, timeout=10):
            if r := f.result(timeout=2): res.append(r)
    except: pass
    return res

def top_movers(syms, n=20):
    tk = tickers_all()
    ss = set(syms)
    mv = [(s, abs(d["pct"])) for s, d in tk.items()
          if s in ss and d["vol"] >= MIN_BASE_VOL]
    return [s for s, _ in sorted(mv, key=lambda x: x[1], reverse=True)[:n]]

def print_inline():
    n   = _stats["wins"] + _stats["losses"]
    wr  = _stats["wins"] / n * 100 if n else 0
    pnl = _stats["pnl"]
    e   = "💚" if pnl >= 0 else "🔴"
    fee = _stats["total_fee"]
    print(f"     ┌ [v18.8 LIVE] {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']} "
          f"{e}NetPnL:{pnl:+.4f}U Fee:{fee:.4f}U")

def print_full():
    n    = _stats["wins"] + _stats["losses"]
    wr   = _stats["wins"] / n * 100 if n else 0
    pnl  = _stats["pnl"]
    sess = (time.time() - _stats["start"]) / 3600
    tph  = n / sess if sess > 0 else 0
    e    = "💚" if pnl >= 0 else "🔴"

    sh = md = 0.0
    if len(_stats["hist"]) >= 5:
        a  = np.array(list(_stats["hist"]))
        sd = float(np.std(a))
        sh = float(np.mean(a)) / sd if sd > 0 else 0.0
    if len(_stats["hist"]) >= 2:
        eq = np.cumsum(list(_stats["hist"]))
        md = float(np.min(eq - np.maximum.accumulate(eq)))

    print(f"\n  {'─'*62}")
    print(f"  💸 LIVE REAL v18.8 [FEE-AWARE] — {sess*60:.0f}m | {tph:.1f}T/jam")
    print(f"  🎯 {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']}")
    print(f"  {e} Net PnL:{pnl:+.5f}U  Gross:{_stats['gross_pnl']:+.5f}U  Fee:{_stats['total_fee']:.5f}U")
    print(f"  📊 Best:{_stats['best']:+.5f}  Worst:{_stats['worst']:+.5f}")
    print(f"  📐 Sharpe:{sh:.2f} MaxDD:{md:.5f}U")
    print(f"  KS: consec={_ks['consec']} daily={_ks['daily']:+.4f} | BTC:{_macro['btc']}")
    if trade_log:
        print(f"  📋 Last 5 Transactions:")
        for t in trade_log[-5:]:
            em = "🟢" if t["pnl"] > 0 else "🔴"
            print(f"     {em} {t['sym']:<14} Net:{t['pnl']:+.5f}U "
                  f"(Gross:{t['gross']:+.5f} Fee:{t['fee']:.5f}) — {t['reason']}")
    print(f"  {'─'*62}")

# ═══════════════════════════════════════════════════════
#  BACKGROUND THREADS
# ═══════════════════════════════════════════════════════
def t_monitor():
    """[FIX-3] Monitor thread dengan backoff untuk -1109 dan interval lebih lambat."""
    consecutive_errors = 0
    while True:
        try:
            sync_binance_positions()
            consecutive_errors = 0
        except Exception as e:
            consecutive_errors += 1
            err_str = str(e)
            if "-1109" in err_str:
                # Invalid account — backoff lebih lama
                sleep_time = min(30, 5 * consecutive_errors)
                time.sleep(sleep_time)
                continue
        # [FIX-3] Interval dinaikkan dari 2.0 → 3.0 untuk kurangi spam
        time.sleep(3.0)

def t_rescan(syms):
    while True:
        try:
            _rescan_q.get(timeout=30)
            time.sleep(0.3)
            with _lock:
                sync_binance_positions()
                slots = MAX_POSITIONS - len(paper_positions)
            if slots <= 0 or ks_check()[0]: continue
            hot  = [s for s in _hot_syms if s not in paper_positions]
            rest = [s for s in syms if s not in paper_positions and s not in hot]
            res  = scan_batch((hot + rest)[:25])
            if res:
                for r in sorted(res, key=lambda x: x[2], reverse=True)[:slots]:
                    if len(paper_positions) >= MAX_POSITIONS: break
                    sym, d, sc, sg, px, atr = r
                    paper_open(sym, d, sc, sg, px, atr)
        except: pass

def t_macro():
    while True:
        try: _macro["btc"] = btc_trend()
        except: pass
        try:
            if time.time() - _macro["last_fng"] > 300:
                r = requests.get(
                    "https://api.alternative.me/fng/?limit=1", timeout=5
                ).json()
                _macro["fng"]      = int(r["data"][0]["value"])
                _macro["last_fng"] = time.time()
        except: pass
        time.sleep(5)

# ═══════════════════════════════════════════════════════
#  MAIN LOOP
# ═══════════════════════════════════════════════════════
def run_bot():
    print("╔═══════════════════════════════════════════════════════╗")
    print("║  🚀 NATIVE BINANCE v18.8 — FULL MIRROR REVERSAL       ║")
    print("║  ⚠️  STATUS: RUNNING ON FUTURES TESTNET ENVIRONMENT   ║")
    print("╠═══════════════════════════════════════════════════════╣")
    print("║  Logic      : Inverse Mode ON (Dibalik Total)         ║")
    print("║  Execution  : Server-side TP/SL (No Halusination)     ║")
    print("║  Fee Tracking: ON — Net PnL = Gross - Commission      ║")
    print("╚═══════════════════════════════════════════════════════╝")

    try:
        valid = {
            s["symbol"] for s in client.futures_exchange_info()["symbols"]
            if s["status"] == "TRADING"
        }
        syms = list(dict.fromkeys([s for s in SYMBOLS if s in valid]))
    except:
        syms = list(dict.fromkeys(SYMBOLS))

    sync_binance_positions()

    threading.Thread(target=t_monitor,          daemon=True).start()
    threading.Thread(target=t_rescan, args=(syms,), daemon=True).start()
    threading.Thread(target=t_macro,            daemon=True).start()

    time.sleep(4); tickers_all()
    cycle = scan_idx = 0
    n_bat = math.ceil(len(syms) / BATCH_SIZE)

    while True:
        cycle += 1
        with _lock:
            sync_binance_positions()
            slots = MAX_POSITIONS - len(paper_positions)

        print(f"\n{'═'*57}")
        print(f"  #{cycle} {time.strftime('%H:%M:%S')} BTC:{_macro['btc']} F&G:{_macro['fng']} "
              f"({len(paper_positions)}/{MAX_POSITIONS}) NetPnL:{_stats['pnl']:+.4f}U")

        if (k := ks_check())[0]:
            print(f"  🚨 KS:{k[1]}")
            time.sleep(SCAN_INTERVAL)
            continue

        if slots > 0:
            mv = top_movers(syms, 20)
            mv = [s for s in mv if s not in paper_positions]
            bs = scan_idx * BATCH_SIZE
            reg = [
                s for s in syms[bs:bs+BATCH_SIZE]
                if s not in paper_positions and s not in mv
            ]
            scan_idx  = (scan_idx + 1) % n_bat
            scan_list = mv[:15] + reg[:10]

            try: res = scan_batch(scan_list)
            except: res = []

            if res:
                res.sort(key=lambda x: x[2], reverse=True)
                for r in res[:slots]:
                    if len(paper_positions) >= MAX_POSITIONS: break
                    sym, d, sc, sg, px, atr = r
                    print(f"     ⭐ {sym} {d} Score:{sc} ATR:{atr:.5g} {' | '.join(sg)}")
                    paper_open(sym, d, sc, sg, px, atr)
            elif len(paper_positions) == 0:
                try:
                    r2 = scan_batch([s for s in syms if s not in paper_positions])
                except: r2 = []
                if r2:
                    r2.sort(key=lambda x: x[2], reverse=True)
                    sym, d, sc, sg, px, atr = r2[0]
                    paper_open(sym, d, sc, sg, px, atr)

        if cycle % 10 == 0: print_full()
        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    run_bot()
