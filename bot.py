"""
Bot Scalping v20.0 — TREND-FOLLOW + PROFIT-FIRST LOGIC
=======================================================
PERUBAHAN DARI v19.2:
- [FIX] INVERSE_MODE dihapus. Sinyal sekarang IKUT tren, bukan melawan.
  Dulu: sinyal LONG → eksekusi SHORT (salah saat tren aktif)
  Sekarang: sinyal LONG → eksekusi LONG (ikut momentum)

- [FIX] HardSL diubah jadi HardProfit:
  Dulu:  prof_pct <= -0.15% → cut loss paksa
  Sekarang: prof_pct >= +0.15% → ambil profit paksa

- [FIX] ImpatientLoss diubah jadi ImpatientProfit:
  Dulu:  hold >= 15s DAN rugi > -0.05% → cut loss
  Sekarang: hold >= 15s DAN untung >= +0.05% → ambil profit

- [FIX] R:R disesuaikan proporsional dengan pola loss lama:
  ExtremeProfit (TP besar) tetap di +0.20%
  HardProfit (TP paksa)    sekarang +0.15% (dulu SL -0.15%)
  ImpatientProfit          sekarang +0.05% / 15s (dulu loss -0.05%)
  SL keras (HardSL) tetap ada di -0.30% sebagai emergency stop

- [FIX] BTC filter aktif: block entry saat BEAR / MILD_BEAR
- [FIX] MIN_SCORE naik ke 60, MIN_GAP naik ke 15
- [FIX] MAX_POSITIONS turun ke 2 untuk kurangi drawdown simultan
- [FIX] CONSEC_MAX turun ke 4 untuk proteksi lebih cepat
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
#  CONFIG v20.0
# ═══════════════════════════════════════════════════════
# INVERSE_MODE dihapus total — bot sekarang ikut tren

LEVERAGE       = 20
ORDER_USDT     = 2.0       # Modal (Margin) per trade
MAX_POSITIONS  = 2         # Diturunkan dari 3 → kurangi drawdown simultan

# ── EXIT CONFIG (dibalik dari v19.2) ──────────────────
EXTREME_PROFIT_PCT  = 0.0020   # +0.20% → TP besar (sama seperti dulu)
HARD_PROFIT_PCT     = 0.0015   # +0.15% → profit paksa (dulu ini adalah SL -0.15%)
IMPATIENT_PROFIT_PCT= 0.0005   # +0.05% / 15s → ambil profit kecil cepat (dulu ini cut loss)
HARD_SL_PCT         = 0.0030   # -0.30% → SL emergency (lebih longgar, beri ruang gerak)

MIN_BASE_VOL   = 25_000_000
MIN_VR         = 1.2          # Naik dari 1.1 → filter volume lebih ketat
BR_LONG_MIN    = 0.50         # Naik dari 0.48 → buy ratio harus lebih dominan untuk LONG
BR_SHORT_MAX   = 0.50         # Turun dari 0.52 → sell ratio harus lebih dominan untuk SHORT

SCAN_INTERVAL  = 1
MONITOR_INT    = 0.25
SCAN_DELAY     = 0.015
BATCH_SIZE     = 15
MAX_WORKERS    = 8
MAX_HOLD_SEC   = 60

MIN_SCORE      = 60           # Naik dari 40 → hanya masuk di setup kuat
MIN_GAP        = 15           # Naik dari 10 → selisih long vs short score harus lebih tegas
COOLDOWN_SEC   = 5            # Naik dari 3

DAILY_LOSS     = -6.0         # Lebih konservatif dari -8.0
CONSEC_MAX     = 4            # Turun dari 6 → pause lebih cepat setelah seri loss
CONSEC_PAUSE   = 120          # Naik dari 60s
TTL_5M         = 5
TTL_15M        = 30

# BTC trend yang diblokir untuk entry
BTC_BLOCK_TRENDS = {"BEAR", "MILD_BEAR"}

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
    "extreme_tp": 0, "hard_profit": 0, "impatient_profit": 0, "hard_sl": 0, "force": 0,
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
#  SIGNAL ENGINE v20.0 — TREND FOLLOW (bukan inverse)
# ═══════════════════════════════════════════════════════
def signal(df):
    if df is None or len(df) < 55: return None, 0, [], 0.0

    row, prev, prev2 = df.iloc[-2], df.iloc[-3], df.iloc[-4]
    p, e5, e9, e21, e50 = row["close"], row["e5"], row["e9"], row["e21"], row["e50"]
    rsi, mh, mh_p, mh_p2 = row["rsi"], row["mh"], prev["mh"], prev2["mh"]
    vr, br, m5, atr, adx = row["vr"], row["br"], row["m5"], row["atr"], row["adx"]
    btc = _macro["btc"]

    # BLOCK entry jika BTC sedang bear — ini penyebab utama loss di v19.2
    if btc in BTC_BLOCK_TRENDS:
        return None, 0, [], atr

    if vr < MIN_VR: return None, 0, [], atr

    lp = sp = 0
    sl, ss = [], []

    # EMA stack scoring
    if p > e5 > e9 > e21 > e50:   lp += 30; sl.append("EMA_stack↑")
    elif p > e5 > e9 > e21:       lp += 22; sl.append("EMA↑↑")
    if p < e5 < e9 < e21 < e50:   sp += 30; ss.append("EMA_stack↓")
    elif p < e5 < e9 < e21:       sp += 22; ss.append("EMA↓↓")

    # Momentum scoring
    if m5 > 0.005:    lp += 25; sl.append(f"Mom+{m5*100:.1f}%")
    elif m5 > 0.003:  lp += 18; sl.append(f"Mom+{m5*100:.1f}%")
    if m5 < -0.005:   sp += 25; ss.append(f"Mom{m5*100:.1f}%")
    elif m5 < -0.003: sp += 18; ss.append(f"Mom{m5*100:.1f}%")

    # MACD scoring
    if mh_p <= 0 and mh > 0:             lp += 22; sl.append("MACD_X↑")
    elif mh > 0 and mh > mh_p > mh_p2:  lp += 18; sl.append("MACD↑↑")
    if mh_p >= 0 and mh < 0:             sp += 22; ss.append("MACD_X↓")
    elif mh < 0 and mh < mh_p < mh_p2:  sp += 18; ss.append("MACD↓↓")

    # Volume scoring
    if vr >= 3.0:   lp += 15; sp += 15; sl.append(f"Vol{vr:.1f}x"); ss.append(f"Vol{vr:.1f}x")
    elif vr >= 2.0: lp += 10; sp += 10; sl.append(f"Vol{vr:.1f}x"); ss.append(f"Vol{vr:.1f}x")

    # Buy/sell ratio — threshold lebih ketat dari v19.2
    if br > 0.65: lp += 18; sl.append(f"Buy{br:.0%}")
    if br < 0.35: sp += 18; ss.append(f"Sell{1-br:.0%}")

    # RSI filter
    if rsi > 75:   lp = int(lp * 0.4); sp += 20; ss.append(f"RSI_OB{rsi:.0f}")
    elif rsi < 25: sp = int(sp * 0.4); lp += 20; sl.append(f"RSI_OS{rsi:.0f}")

    # ADX bonus
    if adx > 35: lp += 8; sp += 8; sl.append(f"ADX{adx:.0f}"); ss.append(f"ADX{adx:.0f}")

    # BTC bonus/penalty berdasarkan trend
    if btc == "BULL":      lp += 15; sl.append("BTC:BULL")
    elif btc == "MILD_BULL": lp += 8; sl.append("BTC:MBULL")
    elif btc == "SIDEWAYS":  pass  # netral, tidak ada bonus/penalti

    thresh = MIN_SCORE
    gap    = abs(lp - sp)

    # ── TREND FOLLOW (bukan inverse) ──────────────────────
    # Jika long lebih kuat → LONG (ikut naik)
    if lp > sp and lp >= thresh and gap >= MIN_GAP:
        if br <= BR_LONG_MIN: return None, lp, [], atr  # buy ratio tidak cukup kuat
        return "LONG", lp, sl[:3], atr

    # Jika short lebih kuat → SHORT (ikut turun)
    if sp > lp and sp >= thresh and gap >= MIN_GAP:
        if br >= BR_SHORT_MAX: return None, sp, [], atr  # sell ratio tidak cukup kuat
        return "SHORT", sp, ss[:3], atr

    return None, max(lp, sp), [], atr

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
    print(f"\n  {d} [LIVE] {sym} {direction} OPEN @{filled_px:.6g} | Margin: ~${ORDER_USDT} | Score:{score}")
    _stats["trades"] += 1

def live_close(sym, reason):
    with _lock:
        pos = live_positions.get(sym)
        if pos is None or pos.get("_r") or pos.get("_closing"):
            return
        pos["_closing"] = True

    side, entry, q = pos["side"], pos["entry"], pos["qty"]
    close_px = close_position_market(sym, side, q)

    if close_px == 0:
        with _lock:
            if sym in live_positions: live_positions[sym]["_closing"] = False
        return

    with _lock:
        live_positions.pop(sym, None)

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

    # Statistik exit reason
    if "ExtremeProfit" in reason:    _stats["extreme_tp"] += 1
    elif "HardProfit" in reason:     _stats["hard_profit"] += 1
    elif "ImpatientProfit" in reason: _stats["impatient_profit"] += 1
    elif "HardSL" in reason:         _stats["hard_sl"] += 1
    elif "Force" in reason:          _stats["force"] += 1

    trade_log.append({"sym": sym, "side": side, "entry": round(entry, 7), "exit": round(close_px, 7), "pnl": round(pnl, 5), "reason": reason, "hold": int(hold)})
    set_cd(sym); _hot_syms.appendleft(sym); _rescan_q.put(1)
    print_inline()

# ═══════════════════════════════════════════════════════
#  MONITOR — exit logic dibalik: profit diambil, SL jauh
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

        # TP besar: +0.20% → ExtremeProfit
        if prof_pct >= EXTREME_PROFIT_PCT:
            live_close(sym, "ExtremeProfit"); continue

        # TP paksa: +0.15% → HardProfit (dulu posisi ini adalah HardSL -0.15%)
        if prof_pct >= HARD_PROFIT_PCT:
            live_close(sym, "HardProfit"); continue

        # SL emergency: -0.30% → stop loss keras (lebih longgar, beri ruang gerak)
        if prof_pct <= -HARD_SL_PCT:
            live_close(sym, "HardSL"); continue

        # Timeout paksa
        if hold >= MAX_HOLD_SEC:
            live_close(sym, "ForceTimeout"); continue

        # ImpatientProfit: setelah 15s, jika sudah untung >= +0.05% → ambil profit
        # (dulu posisi ini adalah ImpatientLoss: 15s rugi -0.05% → cut loss)
        if hold >= 15 and prof_pct >= IMPATIENT_PROFIT_PCT:
            live_close(sym, "ImpatientProfit"); continue

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
    print(f"     ┌ [v20.0 LIVE] {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']} {e}PnL:{_stats['pnl']:+.4f}U")
    print(f"     └ ExtTP:{_stats['extreme_tp']} HardP:{_stats['hard_profit']} ImpP:{_stats['impatient_profit']} SL:{_stats['hard_sl']} Force:{_stats['force']}")

def print_full():
    n = _stats["wins"] + _stats["losses"]
    wr = _stats["wins"] / n * 100 if n else 0
    sess = (time.time() - _stats["start"]) / 3600
    tph = n / sess if sess > 0 else 0
    e = "💚" if _stats["pnl"] >= 0 else "🔴"
    print(f"\n  {'─'*62}")
    print(f"  🚀 LIVE v20.0 [TREND-FOLLOW + PROFIT-FIRST] — {sess*60:.0f}m | {tph:.1f}T/jam")
    print(f"  🎯 {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']}")
    print(f"  {e} PnL:{_stats['pnl']:+.5f}U Best:{_stats['best']:+.5f} Worst:{_stats['worst']:+.5f}")
    print(f"  📊 Exit: ExtTP:{_stats['extreme_tp']} HardProfit:{_stats['hard_profit']} ImpProfit:{_stats['impatient_profit']} HardSL:{_stats['hard_sl']} Force:{_stats['force']}")
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
    print(f"║  🚀 LIVE TRADE v20.0 — TREND-FOLLOW + PROFIT-FIRST   ║")
    print(f"║  TP: +0.20% / +0.15% / +0.05%@15s  SL: -0.30%       ║")
    print(f"║  BTC BEAR/MILD_BEAR → SKIP ENTRY                     ║")
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
        btc = _macro["btc"]
        print(f"\n{'═'*57}")
        print(f"  #{cycle} {time.strftime('%H:%M:%S')} BTC:{btc} ({len(live_positions)}/{MAX_POSITIONS}) PnL:{_stats['pnl']:+.4f}U")

        if (k := ks_check())[0]:
            print(f"  🚨 KS:{k[1]}"); time.sleep(SCAN_INTERVAL); continue

        # Tampilkan info jika BTC sedang di kondisi block
        if btc in BTC_BLOCK_TRENDS:
            print(f"  ⏸  BTC:{btc} — entry diblokir, menunggu kondisi membaik")
            time.sleep(SCAN_INTERVAL); continue

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
