"""
Bot Scalping v18.4 — INVERSE EXTREME PROFIT MODE (LIVE)
====================================================
- LIVE MODE: Order nyata ke Binance Futures Testnet/Live.
- INVERSE MODE: Sinyal LONG dieksekusi SHORT, sinyal SHORT dieksekusi LONG.
- EXTREME PROFIT: Profit +0.05% langsung bungkus.
- IMPATIENT PROFIT: Hold 5 detik → kalau PLUS langsung bungkus, kalau MINUS tahan (biarkan Hard SL).
- HARD SL: 0.2% jaring pengaman terakhir.

⚠️  PERINGATAN: Bot ini melakukan order NYATA ke Binance.
    Pastikan API Key memiliki izin Trading Futures.
    Gunakan Testnet dulu sebelum ke akun live.

CHANGELOG v18.4:
  - ImpatientLoss  → DIHAPUS (posisi minus tidak langsung diclose)
  - ImpatientWin   → DIGANTI menjadi ImpatientProfit
  - Posisi minus setelah 5 detik: ditahan, diserahkan ke Hard SL (-0.2%) atau ExtremeProfit
  - Stat key: impatient_cut → impatient_profit
"""

import os, time, math, threading, queue
import requests
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from binance.client import Client
from binance.exceptions import BinanceAPIException
import ta
import pandas as pd
import numpy as np

load_dotenv()
client = Client(os.getenv("API_KEY"), os.getenv("API_SECRET"))

# ─── PILIH MODE ────────────────────────────────────────────
# Testnet: client.FUTURES_URL = "https://testnet.binancefuture.com/fapi"
# Live:    client.FUTURES_URL = "https://fapi.binance.com/fapi"
client.FUTURES_URL = "https://testnet.binancefuture.com/fapi"  # ← Ganti ke live jika siap
# ───────────────────────────────────────────────────────────

# ═══════════════════════════════════════════════════════
#  CONFIG v18.4 - INVERSE EXTREME PROFIT
# ═══════════════════════════════════════════════════════
INVERSE_MODE   = True

LEVERAGE       = 20
ORDER_USDT     = 2.0
MAX_POSITIONS  = 3

EXTREME_PROFIT_PCT = 0.0005  # +0.05%
HARD_SL_PCT        = 0.0020  # -0.20%

MIN_BASE_VOL   = 25_000_000
MIN_VR         = 1.1
BR_LONG_MIN    = 0.48
BR_SHORT_MAX   = 0.52

SCAN_INTERVAL  = 1
MONITOR_INT    = 0.25
SCAN_DELAY     = 0.015
BATCH_SIZE     = 15
MAX_WORKERS    = 8
MAX_HOLD_SEC   = 60

MIN_SCORE      = 40
MIN_GAP        = 10
COOLDOWN_SEC   = 3

DAILY_LOSS     = -8.0
CONSEC_MAX     = 6
CONSEC_PAUSE   = 60
TTL_5M         = 5
TTL_15M        = 30

# ═══════════════════════════════════════════════════════
#  SYMBOLS & STATE
# ═══════════════════════════════════════════════════════
# ✅ v18.4: SYMBOLS diisi dinamis saat startup via get_tradable_symbols()
# Daftar ini hanya fallback jika exchange_info gagal diambil
SYMBOLS_FALLBACK = [
    "SOLUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT", "AVAXUSDT",
    "TRXUSDT", "DOTUSDT", "LINKUSDT", "LTCUSDT", "ATOMUSDT",
    "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT", "INJUSDT",
    "SUIUSDT", "SEIUSDT", "FETUSDT", "WLDUSDT", "AAVEUSDT",
    "ORDIUSDT", "TONUSDT", "1000PEPEUSDT", "WIFUSDT", "JUPUSDT",
]
SYMBOLS = []  # Diisi oleh get_tradable_symbols() saat run_bot()

live_positions  = {}   # sym -> {side, entry, qty, open_time, score, sigs, atr, order_id}
trade_log       = []
_ohlcv_cache    = {}
_sym_cooldown   = {}
_ticker_cache   = {}
_ticker_ts      = 0
_sym_info       = {}   # Cache precision info per symbol
_lock           = threading.Lock()
_executor       = ThreadPoolExecutor(max_workers=MAX_WORKERS)
_rescan_q       = queue.Queue()
_hot_syms       = deque(maxlen=20)

_macro = {"fng": 50, "btc": "UNKNOWN", "last_fng": 0, "last_btc": 0}
_ks    = {"active": False, "reason": "", "resume": 0, "consec": 0, "daily": 0.0, "day_reset": 0}
_stats = {
    "trades": 0, "wins": 0, "losses": 0, "pnl": 0.0, "best": 0.0, "worst": 0.0,
    # ✅ v18.4: impatient_cut diganti impatient_profit
    "extreme_tp": 0, "hard_sl": 0, "impatient_profit": 0, "force": 0,
    "hist": deque(maxlen=200), "start": time.time(),
}

# ═══════════════════════════════════════════════════════
#  SYMBOL INFO & PRECISION
# ═══════════════════════════════════════════════════════
def get_sym_info(symbol):
    if symbol in _sym_info:
        return _sym_info[symbol]
    try:
        info = client.futures_exchange_info()
        for s in info["symbols"]:
            if s["symbol"] == symbol:
                qty_step = price_tick = 0
                for f in s["filters"]:
                    if f["filterType"] == "LOT_SIZE":
                        qty_step = float(f["stepSize"])
                    if f["filterType"] == "PRICE_FILTER":
                        price_tick = float(f["tickSize"])
                _sym_info[symbol] = {"qty_step": qty_step, "price_tick": price_tick}
                return _sym_info[symbol]
    except:
        pass
    return {"qty_step": 0.001, "price_tick": 0.0001}

def round_qty(symbol, raw_qty):
    info = get_sym_info(symbol)
    step = info["qty_step"]
    if step == 0:
        return round(raw_qty, 3)
    precision = len(str(step).rstrip("0").split(".")[-1]) if "." in str(step) else 0
    return round(math.floor(raw_qty / step) * step, precision)

# ═══════════════════════════════════════════════════════
#  LEVERAGE SETUP
# ═══════════════════════════════════════════════════════
def set_leverage(symbol):
    try:
        client.futures_change_leverage(symbol=symbol, leverage=LEVERAGE)
    except BinanceAPIException as e:
        if e.code != -4046:
            print(f"  ⚠️  set_leverage {symbol}: {e.message}")

# ═══════════════════════════════════════════════════════
#  UTILS
# ═══════════════════════════════════════════════════════
def qty_calc(price):
    return (ORDER_USDT * LEVERAGE) / price

def price_live(symbol):
    try:
        return float(client.futures_symbol_ticker(symbol=symbol)["price"])
    except:
        return 0.0

def tickers_all():
    global _ticker_cache, _ticker_ts
    now = time.time()
    if now - _ticker_ts < 5 and _ticker_cache:
        return _ticker_cache
    try:
        raw = client.futures_ticker()
        _ticker_cache = {
            t["symbol"]: {
                "pct": float(t["priceChangePercent"]),
                "vol": float(t["quoteVolume"]),
                "last": float(t["lastPrice"]),
            }
            for t in raw
        }
        _ticker_ts = now
        return _ticker_cache
    except:
        return _ticker_cache

def ok_cooldown(sym):
    return (time.time() - _sym_cooldown.get(sym, 0)) >= COOLDOWN_SEC

def set_cd(sym):
    _sym_cooldown[sym] = time.time()

def ohlcv(symbol, interval, limit=100):
    key, now = (symbol, interval), time.time()
    ttl = TTL_5M if interval == Client.KLINE_INTERVAL_5MINUTE else TTL_15M
    if key in _ohlcv_cache and now - _ohlcv_cache[key][0] < ttl:
        return _ohlcv_cache[key][1]
    try:
        kl = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
        df = pd.DataFrame(
            kl,
            columns=["time","open","high","low","close","volume","ct","qv","trades","tbbase","tbquote","ignore"],
        )
        for c in ["open","high","low","close","volume","tbbase","tbquote"]:
            df[c] = df[c].astype(float)
        _ohlcv_cache[key] = (now, df)
        return df
    except:
        return _ohlcv_cache.get(key, (None, None))[1]

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
        df = run_ta(ohlcv("BTCUSDT", Client.KLINE_INTERVAL_5MINUTE, 80).copy())
        row = df.iloc[-2]
        p, e5, e9, e21, m5 = row["close"], row["e5"], row["e9"], row["e21"], row["m5"]
        if p > e5 > e9 > e21 and m5 > 0.001:  return "BULL"
        if p < e5 < e9 < e21 and m5 < -0.001: return "BEAR"
        if p > e9 > e21:  return "MILD_BULL"
        if p < e9 < e21:  return "MILD_BEAR"
        return "SIDEWAYS"
    except:
        return "UNKNOWN"

def ks_check():
    k, now = _ks, time.time()
    if k["active"] and now >= k["resume"]:
        k["active"] = False; k["consec"] = 0
    if k["active"]:
        return True, k["reason"]
    day = now - (now % 86400)
    if day > k["day_reset"]:
        k["daily"] = 0.0; k["day_reset"] = day
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
    if df is None or len(df) < 55:
        return None, 0, [], 0.0

    row, prev, prev2 = df.iloc[-2], df.iloc[-3], df.iloc[-4]
    p, e5, e9, e21, e50 = row["close"], row["e5"], row["e9"], row["e21"], row["e50"]
    rsi, mh, mh_p, mh_p2 = row["rsi"], row["mh"], prev["mh"], prev2["mh"]
    vr, br, m5, body, atr, adx = row["vr"], row["br"], row["m5"], row["br2"], row["atr"], row["adx"]
    btc = _macro["btc"]

    if vr < MIN_VR:
        return None, 0, [], atr

    lp = sp = 0
    sl, ss = [], []

    if p > e5 > e9 > e21 > e50:   lp += 30; sl.append("EMA_stack↑")
    elif p > e5 > e9 > e21:       lp += 22; sl.append("EMA↑↑")

    if p < e5 < e9 < e21 < e50:   sp += 30; ss.append("EMA_stack↓")
    elif p < e5 < e9 < e21:       sp += 22; ss.append("EMA↓↓")

    if m5 > 0.005:   lp += 25; sl.append(f"Mom+{m5*100:.1f}%")
    elif m5 > 0.003: lp += 18; sl.append(f"Mom+{m5*100:.1f}%")
    if m5 < -0.005:  sp += 25; ss.append(f"Mom{m5*100:.1f}%")
    elif m5 < -0.003: sp += 18; ss.append(f"Mom{m5*100:.1f}%")

    if mh_p <= 0 and mh > 0:             lp += 22; sl.append("MACD_X↑")
    elif mh > 0 and mh > mh_p > mh_p2:  lp += 18; sl.append("MACD↑↑")
    if mh_p >= 0 and mh < 0:             sp += 22; ss.append("MACD_X↓")
    elif mh < 0 and mh < mh_p < mh_p2:  sp += 18; ss.append("MACD↓↓")

    if vr >= 3.0:   lp += 15; sp += 15; sl.append(f"Vol{vr:.1f}x"); ss.append(f"Vol{vr:.1f}x")
    elif vr >= 2.0: lp += 10; sp += 10; sl.append(f"Vol{vr:.1f}x"); ss.append(f"Vol{vr:.1f}x")

    if br > 0.65: lp += 18; sl.append(f"Buy{br:.0%}")
    if br < 0.35: sp += 18; ss.append(f"Sell{1-br:.0%}")

    if rsi > 75:   lp = int(lp * 0.4); sp += 20; ss.append(f"RSI_OB{rsi:.0f}")
    elif rsi < 25: sp = int(sp * 0.4); lp += 20; sl.append(f"RSI_OS{rsi:.0f}")

    if adx > 35: lp += 8; sp += 8; sl.append(f"ADX{adx:.0f}"); ss.append(f"ADX{adx:.0f}")

    btc_sw = btc in ("SIDEWAYS", "UNKNOWN")
    thresh = 40 if btc_sw else MIN_SCORE
    gap    = abs(lp - sp)

    if lp <= sp or lp < thresh or gap < MIN_GAP:
        if sp <= lp or sp < thresh or gap < MIN_GAP:
            return None, max(lp, sp), [], atr
        if br >= BR_SHORT_MAX:
            return None, sp, [], atr
        if INVERSE_MODE:
            return "LONG", sp, ss[:3] + ["(INV)"], atr
        return "SHORT", sp, ss[:3], atr

    if br <= BR_LONG_MIN:
        return None, lp, [], atr
    if INVERSE_MODE:
        return "SHORT", lp, sl[:3] + ["(INV)"], atr
    return "LONG", lp, sl[:3], atr

# ═══════════════════════════════════════════════════════
#  LIVE ORDER HELPERS
# ═══════════════════════════════════════════════════════
def place_market_order(symbol, side, quantity):
    try:
        order = client.futures_create_order(
            symbol=symbol,
            side=side,
            type="MARKET",
            quantity=quantity,
            reduceOnly=False,
        )
        order_id  = order["orderId"]
        avg_price = float(order.get("avgPrice", 0) or 0)
        if avg_price == 0:
            fills = order.get("fills", [])
            if fills:
                total_qty = sum(float(f["qty"]) for f in fills)
                avg_price = sum(float(f["price"]) * float(f["qty"]) for f in fills) / total_qty if total_qty else 0
        if avg_price == 0:
            avg_price = price_live(symbol)
        return order_id, avg_price
    except BinanceAPIException as e:
        print(f"  ❌ place_market_order {symbol} {side}: [{e.code}] {e.message}")
        return None, 0
    except Exception as e:
        print(f"  ❌ place_market_order {symbol} {side}: {e}")
        return None, 0

def close_position_market(symbol, side, quantity):
    close_side = "SELL" if side == "LONG" else "BUY"
    try:
        order = client.futures_create_order(
            symbol=symbol,
            side=close_side,
            type="MARKET",
            quantity=quantity,
            reduceOnly=True,
        )
        avg_price = float(order.get("avgPrice", 0) or 0)
        if avg_price == 0:
            avg_price = price_live(symbol)
        return avg_price
    except BinanceAPIException as e:
        print(f"  ❌ close_position_market {symbol} {side}: [{e.code}] {e.message}")
        try:
            order = client.futures_create_order(
                symbol=symbol,
                side=close_side,
                type="MARKET",
                quantity=quantity,
            )
            avg_price = float(order.get("avgPrice", 0) or price_live(symbol))
            print(f"  ⚠️  Fallback close tanpa reduceOnly berhasil: {symbol}")
            return avg_price
        except Exception as e2:
            print(f"  ❌ Fallback close juga gagal {symbol}: {e2}")
            return price_live(symbol)
    except Exception as e:
        print(f"  ❌ close_position_market {symbol}: {e}")
        return price_live(symbol)

# ═══════════════════════════════════════════════════════
#  LIVE OPEN / CLOSE
# ═══════════════════════════════════════════════════════
def live_open(sym, direction, score, sigs, price, atr):
    with _lock:
        if sym in live_positions or len(live_positions) >= MAX_POSITIONS:
            return
        live_positions[sym] = {"_r": True}

    set_leverage(sym)

    raw_q = qty_calc(price)
    q = round_qty(sym, raw_q)
    if q <= 0:
        print(f"  ❌ Qty 0 untuk {sym}, skip.")
        with _lock:
            live_positions.pop(sym, None)
        return

    order_side = "BUY" if direction == "LONG" else "SELL"

    print(f"\n  {'🟢' if direction=='LONG' else '🔴'} [LIVE] {sym} {direction} — Placing {order_side} {q} unit...")

    order_id, filled_px = place_market_order(sym, order_side, q)

    if order_id is None:
        print(f"  ❌ Order gagal untuk {sym}")
        with _lock:
            live_positions.pop(sym, None)
        return

    pos = {
        "side":      direction,
        "entry":     filled_px,
        "qty":       q,
        "open_time": time.time(),
        "score":     score,
        "sigs":      sigs,
        "atr":       atr,
        "order_id":  order_id,
    }
    with _lock:
        live_positions[sym] = pos

    d = "🟢" if direction == "LONG" else "🔴"
    print(f"  {d} [LIVE] {sym} {direction} OPEN @{filled_px:.6g} | OrderID:{order_id}")
    print(f"     Qty:{q} | Target:±{EXTREME_PROFIT_PCT*100}% | HardSL:±{HARD_SL_PCT*100}%")
    print(f"     Score:{score} | BTC:{_macro['btc']} | Sigs:{' | '.join(sigs)}")
    _stats["trades"] += 1

def live_close(sym, reason, close_px=None):
    with _lock:
        pos = live_positions.pop(sym, None)
    if pos is None or pos.get("_r"):
        return

    side  = pos["side"]
    entry = pos["entry"]
    q     = pos["qty"]

    filled_close = close_position_market(sym, side, q)
    if close_px is None:
        close_px = filled_close

    pnl  = (close_px - entry) * q if side == "LONG" else (entry - close_px) * q
    pct  = (close_px - entry) / entry * 100 if side == "LONG" else (entry - close_px) / entry * 100
    hold = time.time() - pos["open_time"]
    e    = "🟢" if pnl >= 0 else "🔴"

    print(f"  {e} [LIVE] {sym} {side} CLOSE — {reason}")
    print(f"     {entry:.6g}→{close_px:.6g} ({pct:+.3f}%) hold:{hold:.0f}s | PnL:{pnl:+.5f}U")

    _stats["pnl"] += pnl
    _stats["hist"].append(pnl)
    ks_upd(pnl)

    if pnl >= 0:
        _stats["wins"] += 1
        if pnl > _stats["best"]: _stats["best"] = pnl
    else:
        _stats["losses"] += 1
        if pnl < _stats["worst"]: _stats["worst"] = pnl

    # ✅ v18.4: tracking reason yang diperbarui
    if   "ExtremeProfit"    in reason: _stats["extreme_tp"]      += 1
    elif "HardSL"           in reason: _stats["hard_sl"]         += 1
    elif "ImpatientProfit"  in reason: _stats["impatient_profit"] += 1
    elif "Force"            in reason: _stats["force"]           += 1

    trade_log.append({
        "sym":    sym,
        "side":   side,
        "entry":  round(entry, 7),
        "exit":   round(close_px, 7),
        "pnl":    round(pnl, 5),
        "reason": reason,
        "hold":   int(hold),
    })

    set_cd(sym)
    _hot_syms.appendleft(sym)
    _rescan_q.put(1)
    print_inline()

# ═══════════════════════════════════════════════════════
#  MONITOR — IMPATIENT PROFIT LOGIC (v18.4)
# ═══════════════════════════════════════════════════════
def monitor_positions():
    for sym in list(live_positions.keys()):
        pos = live_positions.get(sym)
        if pos is None or pos.get("_r"):
            continue

        px = price_live(sym)
        if px == 0:
            continue

        side  = pos["side"]
        entry = pos["entry"]
        hold  = time.time() - pos["open_time"]

        # Force close jika terlalu lama
        if hold >= MAX_HOLD_SEC:
            live_close(sym, "ForceTimeout")
            continue

        if side == "LONG":
            prof_pct = (px - entry) / entry

            # 1. Extreme Profit → bungkus langsung
            if prof_pct >= EXTREME_PROFIT_PCT:
                live_close(sym, "ExtremeProfit"); continue

            # 2. Hard SL → jaring pengaman terakhir
            if prof_pct <= -HARD_SL_PCT:
                live_close(sym, "HardSL"); continue

            # 3. ✅ IMPATIENT PROFIT (v18.4)
            #    Setelah 5 detik:
            #    - Kalau PLUS sekecil apapun → bungkus (ImpatientProfit)
            #    - Kalau MINUS → TAHAN, biarkan Hard SL atau ExtremeProfit yang handle
            if hold >= 5:
                if prof_pct > 0:
                    live_close(sym, "ImpatientProfit")
                    continue
                # minus = tidak diclose, lanjut monitor

            pnl_now = (px - entry) * pos["qty"]
            print(f"  📌 {sym} L@{entry:.5g}→{px:.5g}({prof_pct*100:+.2f}%) {pnl_now:+.4f}U {hold:.0f}s")

        else:  # SHORT
            prof_pct = (entry - px) / entry

            # 1. Extreme Profit → bungkus langsung
            if prof_pct >= EXTREME_PROFIT_PCT:
                live_close(sym, "ExtremeProfit"); continue

            # 2. Hard SL → jaring pengaman terakhir
            if prof_pct <= -HARD_SL_PCT:
                live_close(sym, "HardSL"); continue

            # 3. ✅ IMPATIENT PROFIT (v18.4)
            #    Setelah 5 detik:
            #    - Kalau PLUS sekecil apapun → bungkus (ImpatientProfit)
            #    - Kalau MINUS → TAHAN, biarkan Hard SL atau ExtremeProfit yang handle
            if hold >= 5:
                if prof_pct > 0:
                    live_close(sym, "ImpatientProfit")
                    continue
                # minus = tidak diclose, lanjut monitor

            pnl_now = (entry - px) * pos["qty"]
            print(f"  📌 {sym} S@{entry:.5g}→{px:.5g}({prof_pct*100:+.2f}%) {pnl_now:+.4f}U {hold:.0f}s")

# ═══════════════════════════════════════════════════════
#  SCANNER
# ═══════════════════════════════════════════════════════
def scan_one(sym):
    try:
        time.sleep(SCAN_DELAY)
        if not ok_cooldown(sym): return None
        # Pakai ticker yang ada; kalau kosong, fetch dulu
        tk = _ticker_cache if _ticker_cache else tickers_all()
        if sym in tk and tk[sym]["vol"] < MIN_BASE_VOL: return None
        df = ohlcv(sym, Client.KLINE_INTERVAL_5MINUTE, 100)
        if df is None or len(df) < 55: return None
        df  = run_ta(df.copy())
        px  = df["close"].iloc[-2]
        atr = df["atr"].iloc[-2]
        if px == 0 or atr / px > 0.03: return None
        dir_, sc, sigs, atr_val = signal(df)
        if dir_ is None or len(sigs) < 1: return None
        px_live = price_live(sym)
        if px_live == 0: return None
        return (sym, dir_, sc, sigs, px_live, atr_val)
    except Exception as e:
        print(f"  ⚠️  scan_one {sym}: {e}")
        return None

def scan_batch(syms):
    res = []
    fut = {_executor.submit(scan_one, s): s for s in syms[:BATCH_SIZE]}
    try:
        for f in as_completed(fut, timeout=10):
            if r := f.result(timeout=2):
                res.append(r)
    except:
        pass
    return res

def top_movers(syms, n=20):
    tk = tickers_all()
    ss = set(syms)
    mv = [(s, abs(d["pct"])) for s, d in tk.items() if s in ss and d["vol"] >= MIN_BASE_VOL]
    return [s for s, _ in sorted(mv, key=lambda x: x[1], reverse=True)[:n]]

# ═══════════════════════════════════════════════════════
#  PRINT
# ═══════════════════════════════════════════════════════
def print_inline():
    n   = _stats["wins"] + _stats["losses"]
    wr  = _stats["wins"] / n * 100 if n else 0
    pnl = _stats["pnl"]
    e   = "💚" if pnl >= 0 else "🔴"
    # ✅ v18.4: label Imp-Cut → Imp-Profit
    print(f"     ┌ [v18.4 LIVE] {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']} {e}PnL:{pnl:+.4f}U")
    print(f"     └ Ex-Profit:{_stats['extreme_tp']} HardSL:{_stats['hard_sl']} "
          f"Imp-Profit:{_stats['impatient_profit']} Force:{_stats['force']}")

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
    print(f"  🚀 LIVE v18.4 [INVERSE EXTREME PROFIT] — {sess*60:.0f}m | {tph:.1f}T/jam")
    print(f"  🎯 {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']}")
    print(f"  {e} PnL:{pnl:+.5f}U Best:{_stats['best']:+.5f} Worst:{_stats['worst']:+.5f}")
    print(f"  📐 Sharpe:{sh:.2f} MaxDD:{md:.5f}U")
    print(f"  KS: consec={_ks['consec']} daily={_ks['daily']:+.4f} | BTC:{_macro['btc']}")
    if trade_log:
        print(f"  📋 Last 5:")
        for t in trade_log[-5:]:
            em = "🟢" if t["pnl"] > 0 else "🔴"
            print(f"     {em} {t['sym']:<14} {t['side']} {t['pnl']:+.5f}U {t['hold']}s — {t['reason']}")
    print(f"  {'─'*62}")

# ═══════════════════════════════════════════════════════
#  THREADS
# ═══════════════════════════════════════════════════════
def t_monitor():
    while True:
        try:
            if live_positions:
                monitor_positions()
        except:
            pass
        time.sleep(MONITOR_INT)

def t_rescan(syms):
    while True:
        try:
            _rescan_q.get(timeout=30)
            time.sleep(0.3)
            slots = MAX_POSITIONS - len(live_positions)
            if slots <= 0 or ks_check()[0]:
                continue
            hot  = [s for s in _hot_syms if s not in live_positions]
            rest = [s for s in syms if s not in live_positions and s not in hot]
            res  = scan_batch((hot + rest)[:25])
            if res:
                for r in sorted(res, key=lambda x: x[2], reverse=True)[:slots]:
                    if len(live_positions) >= MAX_POSITIONS: break
                    sym, d, sc, sg, px, atr = r
                    live_open(sym, d, sc, sg, px, atr)
        except:
            pass

def t_macro():
    while True:
        try:
            _macro["btc"] = btc_trend()
        except:
            pass
        try:
            if time.time() - _macro["last_fng"] > 300:
                _macro["fng"] = int(
                    requests.get("https://api.alternative.me/fng/?limit=1", timeout=5)
                    .json()["data"][0]["value"]
                )
                _macro["last_fng"] = time.time()
        except:
            pass
        time.sleep(5)

# ═══════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════
#  SYMBOL FILTER — hanya simbol yang bisa diorder dengan modal kita
# ═══════════════════════════════════════════════════════
def get_tradable_symbols():
    """
    Ambil semua simbol USDT-margined futures dari Binance, lalu filter:
    1. Status TRADING
    2. Hanya pair xxxUSDT (bukan BUSD, USDC, dsb)
    3. Notional cukup: (ORDER_USDT × LEVERAGE) / harga >= stepSize minimum
       dan notional value >= MIN_NOTIONAL Binance (biasanya $100, testnet $50)
    Mengembalikan list simbol yang siap diorder.
    """
    MIN_NOTIONAL_USD = ORDER_USDT * LEVERAGE  # modal efektif kita per trade

    print(f"  🔍 Scanning simbol Binance Futures (notional min: ${MIN_NOTIONAL_USD:.0f})...")

    try:
        info    = client.futures_exchange_info()
        tickers = {t["symbol"]: float(t["lastPrice"]) for t in client.futures_ticker()}
    except Exception as e:
        print(f"  ❌ Gagal fetch exchange info: {e}")
        return list(SYMBOLS_FALLBACK)

    tradable = []
    skipped_price  = []
    skipped_notional = []

    for s in info["symbols"]:
        sym = s["symbol"]

        # Hanya USDT pairs, status TRADING
        if not sym.endswith("USDT"):          continue
        if s["status"] != "TRADING":          continue
        if s.get("contractType") != "PERPETUAL": continue

        # Ambil harga live
        px = tickers.get(sym, 0)
        if px == 0:
            skipped_price.append(sym)
            continue

        # Ambil stepSize dari LOT_SIZE filter
        step = 0.001
        min_notional_filter = 0
        for f in s["filters"]:
            if f["filterType"] == "LOT_SIZE":
                step = float(f["stepSize"])
            if f["filterType"] == "MIN_NOTIONAL":
                min_notional_filter = float(f.get("notional", 0))

        # Hitung qty yang akan kita order
        raw_qty = (ORDER_USDT * LEVERAGE) / px
        precision = len(str(step).rstrip("0").split(".")[-1]) if "." in str(step) else 0
        actual_qty = round(math.floor(raw_qty / step) * step, precision)

        # Notional aktual setelah pembulatan
        actual_notional = actual_qty * px

        # Cek apakah notional cukup (Binance minimum $100, testnet $50)
        # Pakai threshold lebih longgar: notional kita >= 50 USD
        threshold = max(min_notional_filter, 50.0)

        if actual_qty <= 0 or actual_notional < threshold:
            skipped_notional.append(f"{sym}(${actual_notional:.1f})")
            continue

        tradable.append(sym)

    # Sort: prioritaskan yang volume tinggi (sudah ada di ticker)
    vol_map = {t["symbol"]: float(t["quoteVolume"]) for t in client.futures_ticker()
               if t["symbol"] in set(tradable)}
    tradable.sort(key=lambda s: vol_map.get(s, 0), reverse=True)

    print(f"  ✅ {len(tradable)} simbol lolos filter notional")
    print(f"  ⏭  {len(skipped_notional)} dilewati (notional terlalu kecil untuk modal $2×{LEVERAGE}x):")
    if skipped_notional:
        # Tampilkan sebagian saja agar tidak banjir log
        shown = skipped_notional[:10]
        more  = len(skipped_notional) - len(shown)
        print(f"     {', '.join(shown)}{f' ... +{more} lainnya' if more else ''}")

    return tradable if tradable else list(SYMBOLS_FALLBACK)


def run_bot():
    is_testnet = "testnet" in client.FUTURES_URL
    mode_str   = "TESTNET" if is_testnet else "⚠️  LIVE REAL MONEY ⚠️"

    print("╔═══════════════════════════════════════════════════════╗")
    print(f"║  🚀 LIVE TRADE v18.4 — INVERSE EXTREME PROFIT         ║")
    print(f"║  MODE: {mode_str:<47}║")
    print("╠═══════════════════════════════════════════════════════╣")
    print(f"║  Order      : ${ORDER_USDT} notional × {LEVERAGE}x leverage             ║")
    print(f"║  Target     : +{EXTREME_PROFIT_PCT*100:.2f}% per trade                      ║")
    print(f"║  Hard SL    : -{HARD_SL_PCT*100:.2f}%                                ║")
    print(f"║  Impatient  : 5s → PLUS bungkus, MINUS tahan terus   ║")
    print(f"║  Jaring akhir: Hard SL -0.2% atau ForceTimeout 60s   ║")
    print("╚═══════════════════════════════════════════════════════╝\n")

    if not is_testnet:
        print("  ❗ LIVE MODE AKTIF — Order akan dikirim ke akun NYATA!")
        print("  Tekan Ctrl+C dalam 5 detik untuk batalkan...")
        time.sleep(5)

    # ✅ v18.4: filter dinamis — hanya simbol yang notional-nya cukup untuk $2×20x
    syms = get_tradable_symbols()
    SYMBOLS[:] = syms  # update global agar t_rescan bisa pakai list terbaru
    print(f"  ✅ {len(syms)} simbol siap ditrading")

    threading.Thread(target=t_monitor,          daemon=True).start()
    threading.Thread(target=t_rescan, args=(syms,), daemon=True).start()
    threading.Thread(target=t_macro,            daemon=True).start()

    time.sleep(4)
    tickers_all()

    cycle    = 0
    scan_idx = 0
    n_bat    = math.ceil(len(syms) / BATCH_SIZE)

    while True:
        cycle += 1
        slots  = MAX_POSITIONS - len(live_positions)

        print(f"\n{'═'*57}")
        print(
            f"  #{cycle} {time.strftime('%H:%M:%S')} BTC:{_macro['btc']} F&G:{_macro['fng']} "
            f"({len(live_positions)}/{MAX_POSITIONS}) PnL:{_stats['pnl']:+.4f}U"
        )

        if (k := ks_check())[0]:
            print(f"  🚨 KS:{k[1]}")
            time.sleep(SCAN_INTERVAL)
            continue

        if slots > 0:
            # Pastikan ticker cache terisi
            if not _ticker_cache:
                tickers_all()

            mv = top_movers(syms, 20)
            mv = [s for s in mv if s not in live_positions]

            bs  = scan_idx * BATCH_SIZE
            reg = [s for s in syms[bs:bs+BATCH_SIZE] if s not in live_positions and s not in mv]
            scan_idx  = (scan_idx + 1) % n_bat
            scan_list = mv[:15] + reg[:10]

            print(f"  🔎 Scan {len(scan_list)} syms (mv:{len(mv[:15])} reg:{len(reg[:10])}) | pool:{len(syms)}")

            try:
                res = scan_batch(scan_list)
            except Exception as e:
                print(f"  ❌ scan_batch error: {e}")
                res = []

            print(f"  📊 Sinyal ditemukan: {len(res)}")

            if res:
                res.sort(key=lambda x: x[2], reverse=True)
                for r in res[:slots]:
                    if len(live_positions) >= MAX_POSITIONS: break
                    sym, d, sc, sg, px, atr = r
                    print(f"     ⭐ {sym} {d} Score:{sc} ATR:{atr:.5g} {' | '.join(sg)}")
                    live_open(sym, d, sc, sg, px, atr)
            elif len(live_positions) == 0:
                print(f"  🔎 No signal, fallback scan semua {len(syms)} syms...")
                try:
                    r2 = scan_batch([s for s in syms if s not in live_positions])
                except:
                    r2 = []
                print(f"  📊 Fallback sinyal: {len(r2)}")
                if r2:
                    r2.sort(key=lambda x: x[2], reverse=True)
                    sym, d, sc, sg, px, atr = r2[0]
                    live_open(sym, d, sc, sg, px, atr)
        else:
            print(f"  ✅ Full ({MAX_POSITIONS}/{MAX_POSITIONS})")

        if cycle % 20 == 0:
            print_full()

        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    run_bot()
