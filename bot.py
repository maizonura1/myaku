"""
Bot Scalping v19.2 — INVERSE & BUG FIX EXECUTION (LIVE)
====================================================
- PATCHED: Bug harga 0 / loss -100% (Minus puluhan dollar palsu).
- PATCHED: Bug posisi ghost/zombie (lupa close karena pop memori duluan).
- PATCHED: Bug margin bengkak > $2 (kuantitas sekarang selalu floor/pembulatan bawah).
- INVERSE MODE AKTIF: Menangkap koreksi/pullback.
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
client.FUTURES_URL = "https://testnet.binancefuture.com/fapi"  # ← Ganti ke live jika siap
# ───────────────────────────────────────────────────────────

# ═══════════════════════════════════════════════════════
#  CONFIG v19.2
# ═══════════════════════════════════════════════════════
INVERSE_MODE   = True

LEVERAGE       = 20
ORDER_USDT     = 2.0  # Modal (Margin) yang digunakan per trade
MAX_POSITIONS  = 3

EXTREME_PROFIT_PCT = 0.0020  # +0.20%
HARD_SL_PCT        = 0.0015  # -0.15%

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
SYMBOLS_FALLBACK = [
    "SOLUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT", "AVAXUSDT",
    "TRXUSDT", "DOTUSDT", "LINKUSDT", "LTCUSDT", "ATOMUSDT",
    "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT", "INJUSDT",
    "SUIUSDT", "SEIUSDT", "FETUSDT", "WLDUSDT", "AAVEUSDT",
    "ORDIUSDT", "TONUSDT", "1000PEPEUSDT", "WIFUSDT", "JUPUSDT",
]
SYMBOLS = []

live_positions  = {}
trade_log       = []
_ohlcv_cache    = {}
_sym_cooldown   = {}
_ticker_cache   = {}
_ticker_ts      = 0
_sym_info       = {}
_lock           = threading.Lock()
_executor       = ThreadPoolExecutor(max_workers=MAX_WORKERS)
_rescan_q       = queue.Queue()
_hot_syms       = deque(maxlen=20)

_macro = {"fng": 50, "btc": "UNKNOWN", "last_fng": 0, "last_btc": 0}
_ks    = {"active": False, "reason": "", "resume": 0, "consec": 0, "daily": 0.0, "day_reset": 0}
_stats = {
    "trades": 0, "wins": 0, "losses": 0, "pnl": 0.0, "best": 0.0, "worst": 0.0,
    "extreme_tp": 0, "hard_sl": 0, "impatient_loss": 0, "force": 0,
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
    # FIX: Selalu floor (pembulatan bawah) agar margin tidak melebihi setting
    qty = math.floor(raw_qty / step) * step
    return round(qty, precision)

def set_leverage(symbol):
    try:
        client.futures_change_leverage(symbol=symbol, leverage=LEVERAGE)
    except BinanceAPIException as e:
        if e.code != -4046:
            pass

# ═══════════════════════════════════════════════════════
#  UTILS & TA
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
            t["symbol"]: {"pct": float(t["priceChangePercent"]), "vol": float(t["quoteVolume"]), "last": float(t["lastPrice"])}
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
        df = pd.DataFrame(kl, columns=["time","open","high","low","close","volume","ct","qv","trades","tbbase","tbquote","ignore"])
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
    if k["active"]: return True, k["reason"]
    day = now - (now % 86400)
    if day > k["day_reset"]:
        k["daily"] = 0.0; k["day_reset"] = day
    if k["daily"] <= DAILY_LOSS:
        k["active"] = True; k["reason"] = f"daily({k['daily']:.2f})"; k["resume"] = day + 86400
        return True, k["reason"]
    if k["consec"] >= CONSEC_MAX:
        k["active"] = True; k["reason"] = f"consec({k['consec']})"; k["resume"] = now + CONSEC_PAUSE
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
    vr, br, m5, atr, adx = row["vr"], row["br"], row["m5"], row["atr"], row["adx"]
    btc = _macro["btc"]

    if vr < MIN_VR: return None, 0, [], atr

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

    if mh_p <= 0 and mh > 0:            lp += 22; sl.append("MACD_X↑")
    elif mh > 0 and mh > mh_p > mh_p2:  lp += 18; sl.append("MACD↑↑")
    if mh_p >= 0 and mh < 0:            sp += 22; ss.append("MACD_X↓")
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
        if sp <= lp or sp < thresh or gap < MIN_GAP: return None, max(lp, sp), [], atr
        if br >= BR_SHORT_MAX: return None, sp, [], atr
        if INVERSE_MODE: return "LONG", sp, ss[:3] + ["(INV)"], atr
        return "SHORT", sp, ss[:3], atr

    if br <= BR_LONG_MIN: return None, lp, [], atr
    if INVERSE_MODE: return "SHORT", lp, sl[:3] + ["(INV)"], atr
    return "LONG", lp, sl[:3], atr

# ═══════════════════════════════════════════════════════
#  LIVE ORDER HELPERS
# ═══════════════════════════════════════════════════════
def place_market_order(symbol, side, quantity):
    try:
        order = client.futures_create_order(
            symbol=symbol, side=side, type="MARKET", quantity=quantity, reduceOnly=False
        )
        avg_price = float(order.get("avgPrice", 0))
        if avg_price == 0: avg_price = price_live(symbol)
        return order["orderId"], avg_price
    except Exception as e:
        print(f"  ❌ place_market_order {symbol} {side}: {e}")
        return None, 0

def close_position_market(symbol, side, quantity):
    close_side = "SELL" if side == "LONG" else "BUY"
    try:
        order = client.futures_create_order(
            symbol=symbol, side=close_side, type="MARKET", quantity=quantity, reduceOnly=True
        )
        avg_price = float(order.get("avgPrice", 0))
        if avg_price == 0: avg_price = price_live(symbol)
        return avg_price
    except BinanceAPIException as e:
        if e.code == -2022:
            print(f"  ⚠️  Posisi {symbol} sudah tidak ada di exchange (-2022).")
            # Kembalikan harga saat ini agar tidak dihitung loss 100%
            px = price_live(symbol)
            return px if px > 0 else -1 
        print(f"  ❌ close_position {symbol}: [{e.code}] {e.message}")
        return 0
    except Exception as e:
        print(f"  ❌ close_position {symbol}: {e}")
        return 0

# ═══════════════════════════════════════════════════════
#  LIVE OPEN / CLOSE
# ═══════════════════════════════════════════════════════
def live_open(sym, direction, score, sigs, price, atr):
    with _lock:
        if sym in live_positions or len(live_positions) >= MAX_POSITIONS:
            return
        # Set indikator bahwa posisi sedang dalam proses pembukaan
        live_positions[sym] = {"_r": True, "_closing": False}

    set_leverage(sym)
    q = round_qty(sym, qty_calc(price))
    
    if q <= 0:
        with _lock: live_positions.pop(sym, None)
        return

    order_side = "BUY" if direction == "LONG" else "SELL"
    order_id, filled_px = place_market_order(sym, order_side, q)

    if order_id is None or filled_px == 0:
        with _lock: live_positions.pop(sym, None)
        return

    with _lock:
        live_positions[sym] = {
            "side": direction, "entry": filled_px, "qty": q, 
            "open_time": time.time(), "score": score, "sigs": sigs, 
            "atr": atr, "order_id": order_id, "_r": False, "_closing": False
        }

    d = "🟢" if direction == "LONG" else "🔴"
    print(f"\n  {d} [LIVE] {sym} {direction} OPEN @{filled_px:.6g} | Margin: ~${ORDER_USDT}")
    _stats["trades"] += 1

def live_close(sym, reason):
    with _lock:
        pos = live_positions.get(sym)
        # Jika posisi tidak ada, atau sedang loading awal, atau SEDANG dalam proses close -> Abaikan
        if pos is None or pos.get("_r") or pos.get("_closing"):
            return
        pos["_closing"] = True  # Kunci agar thread lain tidak ikut meng-close

    side, entry, q = pos["side"], pos["entry"], pos["qty"]
    close_px = close_position_market(sym, side, q)

    if close_px == 0:
        # Gagal close (API error). Buka kuncian agar bisa dicoba close lagi detik berikutnya.
        with _lock:
            if sym in live_positions: live_positions[sym]["_closing"] = False
        return

    # Jika close sukses / atau memang posisi sudah hilang (-2022), baru hapus dari memori
    with _lock:
        live_positions.pop(sym, None)

    # Antisipasi Bug Harga API = 0 / -1 agar tidak -100% PnL
    if close_px == -1 or close_px == 0:
        close_px = entry

    pnl = (close_px - entry) * q if side == "LONG" else (entry - close_px) * q
    pct = (close_px - entry) / entry * 100 if side == "LONG" else (entry - close_px) / entry * 100
    hold = time.time() - pos["open_time"]
    e = "🟢" if pnl >= 0 else "🔴"

    print(f"  {e} [LIVE] {sym} {side} CLOSE — {reason}")
    print(f"     {entry:.6g}→{close_px:.6g} ({pct:+.3f}%) hold:{hold:.0f}s | PnL:{pnl:+.5f}U")

    _stats["pnl"] += pnl
    _stats["hist"].append(pnl)
    ks_upd(pnl)

    if pnl >= 0:
        _stats["wins"] += 1; _stats["best"] = max(_stats["best"], pnl)
    else:
        _stats["losses"] += 1; _stats["worst"] = min(_stats["worst"], pnl)

    if "ExtremeProfit" in reason: _stats["extreme_tp"] += 1
    elif "HardSL" in reason: _stats["hard_sl"] += 1
    elif "ImpatientLoss" in reason: _stats["impatient_loss"] += 1
    elif "Force" in reason: _stats["force"] += 1

    trade_log.append({"sym": sym, "side": side, "entry": round(entry, 7), "exit": round(close_px, 7), "pnl": round(pnl, 5), "reason": reason, "hold": int(hold)})
    set_cd(sym); _hot_syms.appendleft(sym); _rescan_q.put(1)
    print_inline()

# ═══════════════════════════════════════════════════════
#  MONITOR
# ═══════════════════════════════════════════════════════
def monitor_positions():
    for sym in list(live_positions.keys()):
        with _lock:
            pos = live_positions.get(sym)
            if pos is None or pos.get("_r") or pos.get("_closing"):
                continue

        px = price_live(sym)
        if px == 0: continue

        side, entry, hold = pos["side"], pos["entry"], time.time() - pos["open_time"]
        prof_pct = (px - entry) / entry if side == "LONG" else (entry - px) / entry

        if prof_pct >= EXTREME_PROFIT_PCT: live_close(sym, "ExtremeProfit"); continue
        if prof_pct <= -HARD_SL_PCT: live_close(sym, "HardSL"); continue
        if hold >= MAX_HOLD_SEC: live_close(sym, "ForceTimeout"); continue
        if hold >= 15 and prof_pct < -0.0005: live_close(sym, "ImpatientLoss"); continue

        pnl_now = (px - entry) * pos["qty"] if side == "LONG" else (entry - px) * pos["qty"]
        char = "L" if side == "LONG" else "S"
        print(f"  📌 {sym} {char}@{entry:.5g}→{px:.5g}({prof_pct*100:+.2f}%) {pnl_now:+.4f}U {hold:.0f}s")

# ═══════════════════════════════════════════════════════
#  SCANNER & THREADS
# ═══════════════════════════════════════════════════════
def scan_one(sym):
    try:
        time.sleep(SCAN_DELAY)
        if not ok_cooldown(sym): return None
        if sym in _ticker_cache and _ticker_cache[sym]["vol"] < MIN_BASE_VOL: return None
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
    mv = [(s, abs(d["pct"])) for s, d in tk.items() if s in set(syms) and d["vol"] >= MIN_BASE_VOL]
    return [s for s, _ in sorted(mv, key=lambda x: x[1], reverse=True)[:n]]

def print_inline():
    n = _stats["wins"] + _stats["losses"]
    wr = _stats["wins"] / n * 100 if n else 0
    e = "💚" if _stats["pnl"] >= 0 else "🔴"
    print(f"     ┌ [v19.2 LIVE] {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']} {e}PnL:{_stats['pnl']:+.4f}U")
    print(f"     └ Ex-Profit:{_stats['extreme_tp']} HardSL:{_stats['hard_sl']} Imp-Loss:{_stats['impatient_loss']} Force:{_stats['force']}")

def print_full():
    n = _stats["wins"] + _stats["losses"]
    wr = _stats["wins"] / n * 100 if n else 0
    sess = (time.time() - _stats["start"]) / 3600
    tph = n / sess if sess > 0 else 0
    e = "💚" if _stats["pnl"] >= 0 else "🔴"
    print(f"\n  {'─'*62}")
    print(f"  🚀 LIVE v19.2 [BUG PATCHED] — {sess*60:.0f}m | {tph:.1f}T/jam")
    print(f"  🎯 {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']}")
    print(f"  {e} PnL:{_stats['pnl']:+.5f}U Best:{_stats['best']:+.5f} Worst:{_stats['worst']:+.5f}")
    if trade_log:
        print(f"  📋 Last 5:")
        for t in trade_log[-5:]:
            em = "🟢" if t["pnl"] > 0 else "🔴"
            print(f"     {em} {t['sym']:<14} {t['side']} {t['pnl']:+.5f}U {t['hold']}s — {t['reason']}")
    print(f"  {'─'*62}")

def t_monitor():
    while True:
        try:
            if live_positions: monitor_positions()
        except: pass
        time.sleep(MONITOR_INT)

def t_rescan(syms):
    while True:
        try:
            _rescan_q.get(timeout=30)
            time.sleep(0.3)
            with _lock: slots = MAX_POSITIONS - len(live_positions)
            if slots <= 0 or ks_check()[0]: continue
            hot = [s for s in _hot_syms if s not in live_positions]
            rest = [s for s in syms if s not in live_positions and s not in hot]
            res = scan_batch((hot + rest)[:25])
            if res:
                for r in sorted(res, key=lambda x: x[2], reverse=True)[:slots]:
                    with _lock:
                        if len(live_positions) >= MAX_POSITIONS: break
                    sym, d, sc, sg, px, atr = r
                    live_open(sym, d, sc, sg, px, atr)
        except: pass

def t_macro():
    while True:
        try: _macro["btc"] = btc_trend()
        except: pass
        time.sleep(5)

# ═══════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════
def get_tradable_symbols():
    try:
        info = client.futures_exchange_info()
        tickers = {t["symbol"]: float(t["lastPrice"]) for t in client.futures_ticker()}
    except: return list(SYMBOLS_FALLBACK)
    tradable = []
    for s in info["symbols"]:
        sym = s["symbol"]
        if not sym.endswith("USDT") or s["status"] != "TRADING" or s.get("contractType") != "PERPETUAL": continue
        px = tickers.get(sym, 0)
        if px == 0: continue
        step = 0.001
        for f in s["filters"]:
            if f["filterType"] == "LOT_SIZE": step = float(f["stepSize"])
        raw_qty = (ORDER_USDT * LEVERAGE) / px
        precision = len(str(step).rstrip("0").split(".")[-1]) if "." in str(step) else 0
        actual_qty = round(math.floor(raw_qty / step) * step, precision)
        if actual_qty <= 0: continue
        tradable.append(sym)
    vol_map = {t["symbol"]: float(t["quoteVolume"]) for t in client.futures_ticker() if t["symbol"] in set(tradable)}
    tradable.sort(key=lambda s: vol_map.get(s, 0), reverse=True)
    return tradable if tradable else list(SYMBOLS_FALLBACK)

def run_bot():
    print("╔═══════════════════════════════════════════════════════╗")
    print(f"║  🚀 LIVE TRADE v19.2 — EXECUTION BUG PATCHED          ║")
    print("╚═══════════════════════════════════════════════════════╝\n")
    syms = get_tradable_symbols()
    SYMBOLS[:] = syms 
    threading.Thread(target=t_monitor, daemon=True).start()
    threading.Thread(target=t_rescan, args=(syms,), daemon=True).start()
    threading.Thread(target=t_macro, daemon=True).start()
    time.sleep(4); tickers_all()
    cycle = 0; scan_idx = 0; n_bat = math.ceil(len(syms) / BATCH_SIZE)

    while True:
        cycle += 1
        with _lock: slots = MAX_POSITIONS - len(live_positions)
        print(f"\n{'═'*57}")
        print(f"  #{cycle} {time.strftime('%H:%M:%S')} BTC:{_macro['btc']} ({len(live_positions)}/{MAX_POSITIONS}) PnL:{_stats['pnl']:+.4f}U")
        if (k := ks_check())[0]:
            print(f"  🚨 KS:{k[1]}"); time.sleep(SCAN_INTERVAL); continue
        
        if slots > 0:
            mv = [s for s in top_movers(syms, 20) if s not in live_positions]
            bs = scan_idx * BATCH_SIZE
            reg = [s for s in syms[bs:bs+BATCH_SIZE] if s not in live_positions and s not in mv]
            scan_idx = (scan_idx + 1) % n_bat
            try: res = scan_batch(mv[:15] + reg[:10])
            except: res = []
            if res:
                for r in sorted(res, key=lambda x: x[2], reverse=True)[:slots]:
                    with _lock:
                        if len(live_positions) >= MAX_POSITIONS: break
                    sym, d, sc, sg, px, atr = r
                    live_open(sym, d, sc, sg, px, atr)
        if cycle % 20 == 0: print_full()
        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    run_bot()
