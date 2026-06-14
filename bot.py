"""
Bot Scalping v20.0.0 — MOMENTUM CONTINUATION + REGIME ADAPTIVE (PAPER TRADING)
====================================================
PERUBAHAN UTAMA vs v19.4.0:
- Hapus total logika mean reversion fade → ganti dengan momentum continuation
- SHORT hanya ketika trend bearish, LONG hanya ketika trend bullish
- Deteksi regime pasar: TRENDING / RANGING / TRANSITION
- TP/SL adaptif berdasarkan ATR dan regime
- Hapus MACD (redundant), gunakan EMA stack + momentum + volume flow
- Filter baru: volume minimum, body candle minimum, hindari BTC opposing trend
- Leverage turun 20→10, order size naik 2→5 USDT, max positions 3→2
- MinScore 60→55, hapus MIN_GAP
- Daily loss limit -20U → -10U
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
client = Client(os.getenv("API_KEY"), os.getenv("API_SECRET"))
client.FUTURES_URL = "https://testnet.binancefuture.com/fapi"

# ═══════════════════════════════════════════════════════
#  CONFIG v20.0.0
# ═══════════════════════════════════════════════════════

LEVERAGE        = 10          # Turun dari 20
ORDER_USDT      = 5.0         # Naik dari 2
MAX_POSITIONS   = 2           # Turun dari 3

# ── TP/SL akan dihitung adaptif, tidak fixed ───────────
FUTURES_FEE_PCT = 0.0005      # Taker fee 0.05% (total round-trip 0.10%)

SCAN_INTERVAL   = 0.2
MONITOR_INT     = 0.05
SCAN_DELAY      = 0.002
BATCH_SIZE      = 40
MAX_WORKERS     = 20
SLOT_FILL_INT   = 0.01

MIN_SCORE       = 55           # Turun dari 60
SLIPPAGE_GUARD  = 0.0015
TTL_5M          = 2

DAILY_LOSS      = -10.0        # Lebih ketat dari -20
CONSEC_MAX      = 15
CONSEC_PAUSE    = 10

# ═══════════════════════════════════════════════════════
#  SYMBOLS (sama seperti sebelumnya)
# ═══════════════════════════════════════════════════════
SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "TRXUSDT", "DOTUSDT",
    "LINKUSDT", "MATICUSDT", "LTCUSDT", "ATOMUSDT", "UNIUSDT",
    "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT", "INJUSDT",
    "SUIUSDT", "SEIUSDT", "FETUSDT", "WLDUSDT", "AAVEUSDT",
    "ORDIUSDT", "TONUSDT", "1000PEPEUSDT", "WIFUSDT", "JUPUSDT",
    "FTMUSDT", "SANDUSDT", "MANAUSDT", "GALAUSDT", "APEUSDT",
    "CRVUSDT", "1000SHIBUSDT", "COMPUSDT", "MKRUSDT", "SNXUSDT",
]
SYMBOLS = list(dict.fromkeys(SYMBOLS))

# ═══════════════════════════════════════════════════════
#  STATE
# ═══════════════════════════════════════════════════════
live_positions  = {}
trade_log       = []
_ohlcv_cache    = {}
_ticker_cache   = {}
_ticker_ts      = 0
_lock           = threading.Lock()
_executor       = ThreadPoolExecutor(max_workers=MAX_WORKERS)
_rescan_q       = queue.Queue()
_hot_syms       = deque(maxlen=30)

_macro = {"fng": 50, "btc": "UNKNOWN", "last_fng": 0, "last_btc": 0}
_ks    = {"active": False, "reason": "", "resume": 0, "consec": 0, "daily": 0.0, "day_reset": 0}
_stats = {
    "trades": 0, "wins": 0, "losses": 0, "pnl": 0.0, "best": 0.0, "worst": 0.0,
    "extreme_tp": 0, "hard_sl": 0, "force": 0, "btc_block": 0,
    "hist": deque(maxlen=200), "start": time.time(),
}

# ═══════════════════════════════════════════════════════
#  BINANCE UTILS (tidak berubah)
# ═══════════════════════════════════════════════════════
_precision_cache = {}
def get_precision(symbol):
    if symbol in _precision_cache: return _precision_cache[symbol]
    try:
        info = client.futures_exchange_info()
        for s in info['symbols']:
            if s['symbol'] == symbol:
                prec = int(s['quantityPrecision'])
                _precision_cache[symbol] = prec
                return prec
    except: pass
    return 2

def qty(symbol, price):
    raw_qty = (ORDER_USDT * LEVERAGE) / price
    prec = get_precision(symbol)
    return round(raw_qty, prec)

def price_live(symbol):
    try: return float(client.futures_symbol_ticker(symbol=symbol)["price"])
    except: return 0.0

def tickers_all():
    global _ticker_cache, _ticker_ts
    now = time.time()
    if now - _ticker_ts < 2 and _ticker_cache: return _ticker_cache
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

def ohlcv(symbol, interval, limit=100):
    key, now = (symbol, interval), time.time()
    ttl = TTL_5M
    if key in _ohlcv_cache and now - _ohlcv_cache[key][0] < ttl:
        return _ohlcv_cache[key][1]
    try:
        kl = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
        df = pd.DataFrame(kl, columns=["time","open","high","low","close","volume",
                                        "ct","qv","trades","tbbase","tbquote","ignore"])
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
    df["m3"]   = (c - c.shift(3)) / c.shift(3)
    return df

def btc_trend():
    try:
        df = run_ta(ohlcv("BTCUSDT", Client.KLINE_INTERVAL_5MINUTE, 80).copy())
        row = df.iloc[-2]
        p, e5, e9, e21, m5 = row["close"], row["e5"], row["e9"], row["e21"], row["m5"]
        if p > e5 > e9 > e21 and m5 > 0.001: return "BULL"
        if p < e5 < e9 < e21 and m5 < -0.001: return "BEAR"
        if p > e9 > e21: return "MILD_BULL"
        if p < e9 < e21: return "MILD_BEAR"
        return "SIDEWAYS"
    except: return "UNKNOWN"

def ks_check():
    k, now = _ks, time.time()
    if k["active"] and now >= k["resume"]:
        k["active"] = False; k["consec"] = 0
    if k["active"]: return True, k["reason"]
    day = now - (now % 86400)
    if day > k["day_reset"]: k["daily"] = 0.0; k["day_reset"] = day
    if k["daily"] <= DAILY_LOSS:
        k["active"] = True
        k["reason"] = f"daily({k['daily']:.2f})"
        k["resume"] = day + 86400
        return True, k["reason"]
    if k["consec"] >= CONSEC_MAX:
        k["active"] = True
        k["reason"] = f"consec({k['consec']})"
        k["resume"] = now + CONSEC_PAUSE
        return True, k["reason"]
    return False, ""

def ks_upd(pnl):
    _ks["daily"] += pnl
    _ks["consec"] = 0 if pnl >= 0 else _ks["consec"] + 1

# ═══════════════════════════════════════════════════════
#  NEW: REGIME DETECTION & ADAPTIVE TP/SL
# ═══════════════════════════════════════════════════════
def get_regime(df):
    """Tentukan regime pasar berdasarkan ADX dan ATR%"""
    adx = df["adx"].iloc[-2]
    atr_pct = df["atr"].iloc[-2] / df["close"].iloc[-2]
    if adx > 25 and atr_pct > 0.002:
        return "TRENDING"
    elif adx < 20:
        return "RANGING"
    else:
        return "TRANSITION"

def get_tp_sl(regime, atr_pct, direction, btc_trend):
    """
    Hitung TP dan SL dalam persen berdasarkan regime dan ATR.
    Mengembalikan (sl_pct, tp_pct)
    """
    if regime == "TRENDING":
        sl = max(atr_pct * 1.5, 0.0025)   # minimal 0.25%
        tp = sl * 2.5                      # reward:risk = 2.5:1
        # Bonus jika searah dengan trend BTC
        if (direction == "LONG" and btc_trend in ["BULL", "MILD_BULL"]) or \
           (direction == "SHORT" and btc_trend in ["BEAR", "MILD_BEAR"]):
            tp *= 1.2
    else:  # RANGING or TRANSITION
        sl = max(atr_pct * 1.2, 0.0020)   # minimal 0.20%
        tp = sl * 2.0                      # reward:risk = 2:1
    # Batasan keamanan
    sl = min(sl, 0.01)    # maksimal SL 1%
    tp = min(tp, 0.03)    # maksimal TP 3%
    return sl, tp

# ═══════════════════════════════════════════════════════
#  SIGNAL v20.0.0 — MOMENTUM CONTINUATION
# ═══════════════════════════════════════════════════════
def signal(df, btc_trend):
    if df is None or len(df) < 55:
        return None, 0, [], 0.0, 0.0, 0.0

    row = df.iloc[-2]
    prev = df.iloc[-3]
    p = row["close"]
    e5, e9, e21 = row["e5"], row["e9"], row["e21"]
    rsi = row["rsi"]
    m5 = row["m5"]
    vr = row["vr"]
    br = row["br"]
    atr_pct = row["atr"] / p
    body_ratio = row["br2"]
    adx = row["adx"]

    # ── FILTER KUALITAS ──────────────────────────────
    if vr < 0.7:                 # volume terlalu rendah
        return None, 0, [], atr_pct, 0.0, 0.0
    if body_ratio < 0.2:         # doji / candle terlalu kecil
        return None, 0, [], atr_pct, 0.0, 0.0

    # ── REGIME ───────────────────────────────────────
    regime = get_regime(df)

    score = 0
    reasons = []
    direction = None

    # ========== TRENDING ==========
    if regime == "TRENDING":
        bullish_stack = (p > e5 > e9 > e21)
        bearish_stack = (p < e5 < e9 < e21)

        # Prioritaskan sinyal searah BTC
        if bullish_stack and btc_trend in ["BULL", "MILD_BULL"]:
            direction = "LONG"
            score = 50
            reasons.append("TrendUp+BTC")
        elif bearish_stack and btc_trend in ["BEAR", "MILD_BEAR"]:
            direction = "SHORT"
            score = 50
            reasons.append("TrendDown+BTC")
        elif bullish_stack:
            direction = "LONG"
            score = 35
            reasons.append("TrendUp")
        elif bearish_stack:
            direction = "SHORT"
            score = 35
            reasons.append("TrendDown")

        if direction:
            # Konfirmasi momentum
            if direction == "LONG" and m5 > 0.002:
                score += 20
                reasons.append("MomUp")
            elif direction == "SHORT" and m5 < -0.002:
                score += 20
                reasons.append("MomDown")
            # Konfirmasi volume flow (buy/sell pressure)
            if direction == "LONG" and br > 0.52:
                score += 15
                reasons.append("BuyVol")
            elif direction == "SHORT" and br < 0.48:
                score += 15
                reasons.append("SellVol")

    # ========== RANGING ==========
    elif regime == "RANGING":
        # Fade hanya di extreme RSI + tidak ada volume breakout
        if rsi > 70 and vr < 1.5:
            direction = "SHORT"
            score = 40 + (rsi - 70) * 2
            reasons.append(f"Overbought{rsi:.0f}")
        elif rsi < 30 and vr < 1.5:
            direction = "LONG"
            score = 40 + (30 - rsi) * 2
            reasons.append(f"Oversold{rsi:.0f}")

        if direction:
            # Sederhana divergence: harga turun tapi MACD naik = bullish divergence
            if direction == "LONG" and row["mh"] > prev["mh"] and p < prev["close"]:
                score += 15
                reasons.append("BullDiv")
            elif direction == "SHORT" and row["mh"] < prev["mh"] and p > prev["close"]:
                score += 15
                reasons.append("BearDiv")

    # ========== TRANSITION ==========
    else:
        # Kombinasi RSI extreme + momentum berlawanan (potensi pembalikan awal)
        if rsi > 65 and m5 < -0.001:
            direction = "SHORT"
            score = 45
            reasons.append("RSIhigh+MomDown")
        elif rsi < 35 and m5 > 0.001:
            direction = "LONG"
            score = 45
            reasons.append("RSIlow+MomUp")

    # ── FINAL CHECK ──────────────────────────────────
    if direction is None or score < MIN_SCORE:
        return None, 0, [], atr_pct, 0.0, 0.0

    sl_pct, tp_pct = get_tp_sl(regime, atr_pct, direction, btc_trend)
    return direction, score, reasons[:4], atr_pct, sl_pct, tp_pct

# ═══════════════════════════════════════════════════════
#  DRY RUN OPEN (minor change: now uses dynamic sl/tp)
# ═══════════════════════════════════════════════════════
def live_open(sym, direction, score, sigs, price, atr_pct, sl_pct, tp_pct):
    with _lock:
        if sym in live_positions or len(live_positions) >= MAX_POSITIONS:
            return
        live_positions[sym] = {"_r": True}

    px_now = price_live(sym)
    if px_now > 0:
        slip = abs(px_now - price) / price
        if slip > SLIPPAGE_GUARD:
            with _lock: live_positions.pop(sym, None)
            return
        price = px_now

    try:
        q_val = qty(sym, price)
    except:
        with _lock: live_positions.pop(sym, None)
        return

    if direction == "LONG":
        sl_price = price * (1 - sl_pct)
        tp_price = price * (1 + tp_pct)
    else:
        sl_price = price * (1 + sl_pct)
        tp_price = price * (1 - tp_pct)

    pos = {
        "side": direction, "entry": price, "qty": q_val,
        "open_time": time.time(), "score": score, "sigs": sigs, "atr_pct": atr_pct,
        "sl_price": sl_price, "tp_price": tp_price,
        "sl_pct": sl_pct, "tp_pct": tp_pct
    }
    with _lock: live_positions[sym] = pos

    d = "🟢" if direction == "LONG" else "🔴"
    print(f"\n  {d} [DRY] {sym} {direction} @{price:.6g} SL:{sl_pct*100:.2f}% TP:{tp_pct*100:.2f}% [{' | '.join(sigs)}]")
    _stats["trades"] += 1

# ═══════════════════════════════════════════════════════
#  DRY RUN CLOSE (tidak berubah)
# ═══════════════════════════════════════════════════════
def live_close(sym, reason, price=None):
    with _lock:
        pos = live_positions.pop(sym, None)
    if pos is None or pos.get("_r"): return

    if price is None: price = price_live(sym)
    if price == 0: return

    side, entry, q_val = pos["side"], pos["entry"], pos["qty"]

    gross_pnl = (price - entry) * q_val if side == "LONG" else (entry - price) * q_val
    open_fee  = (entry * q_val) * FUTURES_FEE_PCT
    close_fee = (price * q_val) * FUTURES_FEE_PCT
    total_fee = open_fee + close_fee
    pnl = gross_pnl - total_fee

    pct  = (price - entry) / entry * 100 if side == "LONG" else (entry - price) / entry * 100
    hold = time.time() - pos["open_time"]
    e    = "🟢" if pnl >= 0 else "🔴"

    print(f"  {e} [DRY] {sym} {side} CLOSE — {reason}")
    print(f"     {entry:.6g}→{price:.6g} ({pct:+.3f}%) hold:{hold:.0f}s | PnL Net:{pnl:+.5f}U (Fee:{total_fee:.5f}U)")

    _stats["pnl"]  += pnl
    _stats["hist"].append(pnl)
    ks_upd(pnl)

    if pnl >= 0:
        _stats["wins"] += 1
        if pnl > _stats["best"]: _stats["best"] = pnl
    else:
        _stats["losses"] += 1
        if pnl < _stats["worst"]: _stats["worst"] = pnl

    if "ExtremeTP" in reason:   _stats["extreme_tp"] += 1
    elif "HardSL"  in reason:   _stats["hard_sl"]    += 1

    trade_log.append({
        "sym": sym, "side": side, "entry": round(entry, 7), "exit": round(price, 7),
        "pnl": round(pnl, 5), "reason": reason, "hold": int(hold),
    })
    _hot_syms.appendleft(sym)
    _rescan_q.put(1)
    print_inline()

# ═══════════════════════════════════════════════════════
#  MONITOR POSITIONS (tidak berubah)
# ═══════════════════════════════════════════════════════
def monitor_positions():
    for sym in list(live_positions.keys()):
        pos = live_positions.get(sym)
        if pos is None or pos.get("_r"): continue

        px = price_live(sym)
        if px == 0: continue

        side  = pos["side"]
        entry = pos["entry"]
        sl_px = pos["sl_price"]
        tp_px = pos["tp_price"]
        hold  = time.time() - pos["open_time"]

        if side == "LONG":
            prof_pct = (px - entry) / entry
            if px <= sl_px:
                live_close(sym, "HardSL", px); continue
            if px >= tp_px:
                live_close(sym, "ExtremeTP", px); continue
            pnl_now = ((px - entry) * pos["qty"]) - ((entry * pos["qty"] + px * pos["qty"]) * FUTURES_FEE_PCT)
            print(f"    📌 {sym} L@{entry:.5g}→{px:.5g}({prof_pct*100:+.2f}%) {pnl_now:+.4f}U {hold:.0f}s [DRY]")
        else:
            prof_pct = (entry - px) / entry
            if px >= sl_px:
                live_close(sym, "HardSL", px); continue
            if px <= tp_px:
                live_close(sym, "ExtremeTP", px); continue
            pnl_now = ((entry - px) * pos["qty"]) - ((entry * pos["qty"] + px * pos["qty"]) * FUTURES_FEE_PCT)
            print(f"    📌 {sym} S@{entry:.5g}→{px:.5g}({prof_pct*100:+.2f}%) {pnl_now:+.4f}U {hold:.0f}s [DRY]")

# ═══════════════════════════════════════════════════════
#  SCANNER (modified: pass btc_trend to signal)
# ═══════════════════════════════════════════════════════
def scan_one(sym):
    try:
        time.sleep(SCAN_DELAY)
        df5 = run_ta(ohlcv(sym, Client.KLINE_INTERVAL_5MINUTE, 100).copy())
        if df5 is None: return None

        px  = df5["close"].iloc[-2]
        atr_pct = df5["atr"].iloc[-2] / px if px != 0 else 0
        if px == 0: return None

        dir_, sc, sigs, atr_pct_val, sl_p, tp_p = signal(df5, _macro["btc"])
        if dir_ is None: return None

        px_live = price_live(sym)
        if px_live == 0: return None

        return (sym, dir_, sc, sigs, px_live, atr_pct_val, sl_p, tp_p)
    except Exception as e:
        # silent fail
        return None

def scan_batch(syms):
    res = []
    fut = {_executor.submit(scan_one, s): s for s in syms[:BATCH_SIZE]}
    try:
        for f in as_completed(fut, timeout=5):
            try:
                if r := f.result(timeout=1): res.append(r)
            except: pass
    except: pass
    return res

def top_movers(syms, n=30):
    tk, ss = tickers_all(), set(syms)
    mv = [(s, abs(d["pct"])) for s, d in tk.items() if s in ss]
    return [s for s, _ in sorted(mv, key=lambda x: x[1], reverse=True)[:n]]

# ═══════════════════════════════════════════════════════
#  PRINT UTILS (sedikit modifikasi untuk tampilan)
# ═══════════════════════════════════════════════════════
def print_inline():
    n  = _stats["wins"] + _stats["losses"]
    wr = _stats["wins"] / n * 100 if n else 0
    pnl, e = _stats["pnl"], "💚" if _stats["pnl"] >= 0 else "🔴"
    print(f"       ┌ [v20.0.0 DRY] {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']} {e}PnL Net:{pnl:+.4f}U")
    print(f"       └ ExTP:{_stats['extreme_tp']} HardSL:{_stats['hard_sl']}")

def print_full():
    n    = _stats["wins"] + _stats["losses"]
    wr   = _stats["wins"] / n * 100 if n else 0
    pnl  = _stats["pnl"]
    sess = (time.time() - _stats["start"]) / 3600
    tph  = n / sess if sess > 0 else 0
    e    = "💚" if pnl >= 0 else "🔴"

    print(f"\n  {'─'*70}")
    print(f"    ✅ DRY RUN v20.0.0 [MOMENTUM CONTINUATION | ADAPTIVE TP/SL]")
    print(f"    🎯 {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']} ({tph:.1f}T/hr)")
    print(f"    {e} PnL Net:{pnl:+.5f}U Best:{_stats['best']:+.5f} Worst:{_stats['worst']:+.5f}")
    print(f"    💰 ExtremeTP:{_stats['extreme_tp']} HardSL:{_stats['hard_sl']}")
    print(f"    ⚙️  Config: Leverage={LEVERAGE} Order={ORDER_USDT}U MaxPos={MAX_POSITIONS} MinScore={MIN_SCORE}")
    if trade_log:
        print(f"    📋 Last 5:")
        for t in trade_log[-5:]:
            em = "🟢" if t["pnl"] > 0 else "🔴"
            print(f"       {em} {t['sym']:<16} {t['side']} {t['pnl']:+.5f}U {t['hold']}s — {t['reason']}")
    print(f"  {'─'*70}")

# ═══════════════════════════════════════════════════════
#  THREADS (tidak berubah)
# ═══════════════════════════════════════════════════════
def t_monitor():
    while True:
        try:
            if live_positions:
                monitor_positions()
        except: pass
        time.sleep(MONITOR_INT)

def t_slot_filler(syms):
    scan_idx = 0
    n_bat    = math.ceil(len(syms) / BATCH_SIZE)

    while True:
        try:
            slots = MAX_POSITIONS - len(live_positions)
            if slots <= 0 or ks_check()[0]:
                time.sleep(SLOT_FILL_INT)
                continue

            hot  = [s for s in _hot_syms if s not in live_positions]
            mv   = top_movers(syms, 30)
            mv   = [s for s in mv if s not in live_positions]

            bs   = scan_idx * BATCH_SIZE
            reg  = [s for s in syms[bs:bs+BATCH_SIZE] if s not in live_positions and s not in mv]
            scan_idx = (scan_idx + 1) % n_bat

            scan_list = list(dict.fromkeys(hot[:5] + mv[:20] + reg[:15]))[:BATCH_SIZE]
            if not scan_list:
                time.sleep(SLOT_FILL_INT)
                continue

            res = scan_batch(scan_list)
            if res:
                res.sort(key=lambda x: x[2], reverse=True)
                for r in res[:slots]:
                    if len(live_positions) >= MAX_POSITIONS: break
                    sym, d, sc, sg, px, atr, sl_p, tp_p = r
                    live_open(sym, d, sc, sg, px, atr, sl_p, tp_p)

        except: pass
        time.sleep(SLOT_FILL_INT)

def t_rescan(syms):
    while True:
        try:
            _rescan_q.get(timeout=5)
            time.sleep(0.05)
            slots = MAX_POSITIONS - len(live_positions)
            if slots <= 0 or ks_check()[0]: continue

            hot  = [s for s in _hot_syms if s not in live_positions]
            rest = [s for s in syms if s not in live_positions and s not in hot]
            res  = scan_batch((hot + rest)[:30])
            if res:
                res.sort(key=lambda x: x[2], reverse=True)
                for r in res[:slots]:
                    if len(live_positions) >= MAX_POSITIONS: break
                    sym, d, sc, sg, px, atr, sl_p, tp_p = r
                    live_open(sym, d, sc, sg, px, atr, sl_p, tp_p)
        except: pass

def t_macro():
    while True:
        try: _macro["btc"] = btc_trend()
        except: pass
        time.sleep(10)

# ═══════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════
def run_bot():
    print("╔════════════════════════════════════════════════════════════════════╗")
    print("║  ✅ DRY RUN v20.0.0 — MOMENTUM CONTINUATION ENGINE                 ║")
    print("║  ✅ Ikut tren, tidak fade | Regime adaptive TP/SL                  ║")
    print("║  ✅ Leverage 10 | Order 5 USDT | Max 2 posisi                      ║")
    print("║  ✅ Filter: volume minimal, body candle, arah BTC                  ║")
    print("╚════════════════════════════════════════════════════════════════════╝")

    try:
        valid = {s["symbol"] for s in client.futures_exchange_info()["symbols"] if s["status"] == "TRADING"}
        syms  = list(dict.fromkeys([s for s in SYMBOLS if s in valid]))
    except:
        syms  = list(dict.fromkeys(SYMBOLS))

    print(f"  📋 {len(syms)} simbol aktif terpantau")

    threading.Thread(target=t_monitor,              daemon=True).start()
    threading.Thread(target=t_slot_filler, args=(syms,), daemon=True).start()
    threading.Thread(target=t_rescan,      args=(syms,), daemon=True).start()
    threading.Thread(target=t_macro,               daemon=True).start()

    time.sleep(2)
    tickers_all()

    cycle = 0
    while True:
        cycle += 1
        slots = MAX_POSITIONS - len(live_positions)
        print(f"\n{'═'*62}")
        print(f"  #{cycle} {time.strftime('%H:%M:%S')} BTC:{_macro['btc']} ({len(live_positions)}/{MAX_POSITIONS}) PnL:{_stats['pnl']:+.4f}U")

        if (k := ks_check())[0]:
            print(f"  🚨 KS:{k[1]}")
        elif slots == 0:
            print(f"  ✅ Slots full")
        else:
            print(f"  🔍 {slots} slot kosong — Scanning for momentum...")

        if cycle % 30 == 0:
            print_full()

        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    run_bot()
