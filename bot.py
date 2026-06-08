"""
Bot Scalping v20.0 — SPEED-FIRST REBUILD
==================================================================
MASALAH v19.0 yang diperbaiki:

  [ROOT CAUSE-1] RR BURUK DI PRAKTIK
    - v19: TP=0.9% tapi trailing stop kena duluan di 0.3% → profit kecil
    - v19: SL=0.45% tapi sering kena slippage → loss lebih besar
    - FIX: TP=0.4% SL=0.2% — lebih ketat, lebih realistis, selesai cepat

  [ROOT CAUSE-2] ENTRY TERLALU JARANG (0.1 T/jam)
    - v19: MIN_SCORE=45/55 terlalu tinggi → jarang trigger
    - v19: COOLDOWN=15s, scan delay 0.015s → lambat
    - FIX: MIN_SCORE=38, COOLDOWN=8s, scan lebih agresif

  [ROOT CAUSE-3] FEE MEMAKAN PROFIT
    - $2 margin × 20x = $40 notional
    - Fee = $40 × 0.04% × 2 = $0.032 per trade
    - Dengan TP=0.4%: profit = $40 × 0.4% = $0.16 → fee ratio = 20% ✓
    - FEE_BUDGET_RATIO dinaikkan ke 25% (lebih fleksibel tapi masih terkontrol)

  [ROOT CAUSE-4] SIGNAL ENGINE LAMBAT — SIDEWAYS SELALU
    - Tambah konfirmasi 1m candle untuk momentum cepat
    - VWAP sebagai filter arah
    - Order book imbalance proxy (taker buy ratio lebih sensitif)

  PARAMETER UTAMA:
    - LEVERAGE = 20, ORDER_USDT = 2.0 (margin per posisi)
    - TP = 0.4%, SL = 0.2% (RR 2:1 — butuh WR > 34% untuk profit)
    - MAX_POSITIONS = 3 (lebih banyak posisi simultan)
    - MIN_SCORE = 38 (lebih sering entry)
    - COOLDOWN = 8s
    - TRAIL_PCT = 0.15% (lebih ketat dari 0.3%)
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
#  CONFIG v20.0 — SPEED-FIRST
# ═══════════════════════════════════════════════════════
LEVERAGE       = 20
ORDER_USDT     = 2.0        # margin per posisi = $2
MAX_POSITIONS  = 3          # Naik dari 2 → 3

# ─── TP/SL REALISTIS ───────────────────────────────────
# Dengan $2 margin × 20x = $40 notional:
#   TP = $40 × 0.4% = $0.16 per trade
#   SL = $40 × 0.2% = $0.08 per trade
#   Fee = $40 × 0.04% × 2 = $0.032 per trade (20% dari TP — masih profit)
#   Break-even WR = 0.2 / (0.4 + 0.2) = 33.3%
EXTREME_PROFIT_PCT = 0.004  # 0.4% TP
HARD_SL_PCT        = 0.002  # 0.2% SL

# ─── ATR FILTER ────────────────────────────────────────
# TP=0.4%, jadi ATR minimal harus ada ruang gerak
ATR_MIN_RATIO  = 0.0002     # ATR/price > 0.02% (ada volatilitas)
ATR_MAX_RATIO  = 0.008      # ATR/price < 0.8% (tidak terlalu liar)

# ─── TRAILING STOP ─────────────────────────────────────
TRAIL_PCT      = 0.0015     # 0.15% — lebih ketat dari 0.3%, protect profit lebih awal

# ─── FEE BUDGET ────────────────────────────────────────
FEE_BUDGET_RATIO = 0.25     # max 25% dari expected TP profit
TAKER_FEE_RATE   = 0.0004   # 0.04% per side

# ─── VOLUME & SIGNAL FILTER ────────────────────────────
MIN_BASE_VOL   = 15_000_000  # Turun dari 25M → 15M (lebih banyak coin tersedia)
MIN_VR         = 1.0         # Turun dari 1.1 → 1.0 (lebih sensitif)
BR_LONG_MIN    = 0.45        # Sedikit lebih longgar
BR_SHORT_MAX   = 0.55

# ─── SCAN SPEED ────────────────────────────────────────
SCAN_INTERVAL  = 1
SCAN_DELAY     = 0.008       # Turun dari 0.015 → 0.008 (scan lebih cepat)
BATCH_SIZE     = 20          # Naik dari 15 → 20
MAX_WORKERS    = 12          # Naik dari 8 → 12

# ─── SCORE THRESHOLD ───────────────────────────────────
MIN_SCORE      = 38          # Turun dari 45 → 38 (lebih sering entry)
MIN_SCORE_SW   = 45          # Sideways: turun dari 55 → 45
MIN_GAP        = 10          # Turun dari 15 → 10

# ─── COOLDOWN & KILL SWITCH ────────────────────────────
COOLDOWN_SEC   = 8           # Turun dari 15 → 8
TRAIL_CHECK_INTERVAL = 1.5   # Lebih sering check trailing
DAILY_LOSS     = -8.0        # Sedikit lebih longgar dari -6
CONSEC_MAX     = 6
CONSEC_PAUSE   = 60
TTL_5M         = 5
TTL_1M         = 2

# ═══════════════════════════════════════════════════════
#  SYMBOLS — fokus coin dengan volume & volatilitas tinggi
# ═══════════════════════════════════════════════════════
SYMBOLS = [
    # Tier-1: liquid + volatile
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "DOGEUSDT", "AVAXUSDT", "ADAUSDT", "TRXUSDT", "DOTUSDT",
    # Tier-2: mid-cap volatile
    "LINKUSDT", "MATICUSDT", "LTCUSDT", "ATOMUSDT", "UNIUSDT",
    "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT", "INJUSDT",
    "SUIUSDT", "SEIUSDT", "FETUSDT", "WLDUSDT", "AAVEUSDT",
    # Tier-3: meme/high-beta
    "ORDIUSDT", "TONUSDT", "1000PEPEUSDT", "WIFUSDT", "JUPUSDT",
    "BONKUSDT", "RENDERUSDT", "TIAUSDT", "STXUSDT", "RUNEUSDT",
    "HBARUSDT", "ALGOUSDT", "VETUSDT", "FTMUSDT", "GALAUSDT",
]
SYMBOLS = list(dict.fromkeys(SYMBOLS))

# ═══════════════════════════════════════════════════════
#  STATE
# ═══════════════════════════════════════════════════════
paper_positions   = {}
trade_log         = []
_ohlcv_cache      = {}
_sym_cooldown     = {}
_ticker_cache     = {}
_ticker_ts        = 0
_precisions       = {}
_lock             = threading.RLock()
_executor         = ThreadPoolExecutor(max_workers=MAX_WORKERS)
_rescan_q         = queue.Queue()
_hot_syms         = deque(maxlen=30)
_logged_closes    = set()
_position_open_ts = {}
_position_peak    = {}

_macro = {"fng": 50, "btc": "UNKNOWN", "last_fng": 0, "last_btc": 0}
_ks    = {"active": False, "reason": "", "resume": 0, "consec": 0, "daily": 0.0, "day_reset": 0}

_stats = {
    "trades": 0, "wins": 0, "losses": 0,
    "gross_pnl": 0.0, "total_fee": 0.0, "pnl": 0.0,
    "best": 0.0, "worst": 0.0,
    "hist": deque(maxlen=200), "start": time.time(),
    "trail_exits": 0, "tp_exits": 0, "sl_exits": 0,
    "skipped_fee": 0, "skipped_atr": 0,
}

# ═══════════════════════════════════════════════════════
#  PRECISION HELPERS
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
        print(f"⚠️ Gagal load precision {sym}: {e}")
    return 3, 0.001, 5.0, 4

def qty(price, sym):
    prec, step_size, min_notional, _ = get_precision_rules(sym)
    target_notional = ORDER_USDT * LEVERAGE  # $2 × 20 = $40
    if target_notional < min_notional:
        target_notional = min_notional + 1.0
    raw_qty = target_notional / price
    if step_size > 0:
        raw_qty = math.trunc(raw_qty / step_size) * step_size
    return round(raw_qty, prec)

# ═══════════════════════════════════════════════════════
#  FEE BUDGET CHECK
# ═══════════════════════════════════════════════════════
def check_fee_budget(price, q):
    notional         = q * price
    estimated_fee    = notional * TAKER_FEE_RATE * 2  # open + close
    tp_profit_actual = notional * EXTREME_PROFIT_PCT
    if tp_profit_actual == 0:
        return False
    return (estimated_fee / tp_profit_actual) <= FEE_BUDGET_RATIO

# ═══════════════════════════════════════════════════════
#  MARKET DATA
# ═══════════════════════════════════════════════════════
def price_live(symbol):
    try:
        return float(client.futures_symbol_ticker(symbol=symbol)["price"])
    except:
        return 0.0

def tickers_all():
    global _ticker_cache, _ticker_ts
    now = time.time()
    if now - _ticker_ts < 4 and _ticker_cache:
        return _ticker_cache
    try:
        raw = client.futures_ticker()
        _ticker_cache = {
            t["symbol"]: {
                "pct":  float(t["priceChangePercent"]),
                "vol":  float(t["quoteVolume"]),
                "last": float(t["lastPrice"])
            } for t in raw
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
    ttl = TTL_1M if interval == Client.KLINE_INTERVAL_1MINUTE else TTL_5M
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
    except:
        return _ohlcv_cache.get(key, (None, None))[1]

def run_ta(df):
    """TA indicators untuk 5m candle."""
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
    df["m3"]   = (c - c.shift(3)) / c.shift(3).replace(0, 1)   # momentum 3 candle
    df["m5"]   = (c - c.shift(5)) / c.shift(5).replace(0, 1)
    # VWAP sederhana (rolling 20 bar)
    tp = (h + l + c) / 3
    df["vwap"] = (tp * v).rolling(20).sum() / v.rolling(20).sum()
    return df

def run_ta_1m(df):
    """TA cepat untuk konfirmasi 1m."""
    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]
    df["rsi1"] = ta.momentum.RSIIndicator(c, 7).rsi()   # RSI lebih cepat
    df["e3"]   = ta.trend.EMAIndicator(c, 3).ema_indicator()
    df["e8"]   = ta.trend.EMAIndicator(c, 8).ema_indicator()
    df["mh1"]  = ta.trend.MACD(c, 6, 13, 4).macd_diff()  # MACD lebih cepat
    df["vm1"]  = v.rolling(10).mean()
    df["vr1"]  = v / df["vm1"].replace(0, 1)
    df["m2"]   = (c - c.shift(2)) / c.shift(2).replace(0, 1)
    return df

def btc_trend():
    try:
        df  = run_ta(ohlcv("BTCUSDT", Client.KLINE_INTERVAL_5MINUTE, 80).copy())
        row = df.iloc[-2]
        p, e5, e9, e21, m5 = row["close"], row["e5"], row["e9"], row["e21"], row["m5"]
        if p > e5 > e9 > e21 and m5 > 0.001:   return "BULL"
        if p < e5 < e9 < e21 and m5 < -0.001:  return "BEAR"
        if p > e9 > e21:  return "MILD_BULL"
        if p < e9 < e21:  return "MILD_BEAR"
        return "SIDEWAYS"
    except:
        return "UNKNOWN"

# ═══════════════════════════════════════════════════════
#  KILL SWITCH
# ═══════════════════════════════════════════════════════
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
#  COIN SCORING — pilih coin terbaik sebelum entry
# ═══════════════════════════════════════════════════════
def coin_quality_score(sym):
    """
    Skor kualitas coin untuk prioritas entry.
    Faktor: volume, % change, volatilitas historis.
    Return score 0-100.
    """
    tk = _ticker_cache
    if sym not in tk:
        return 0
    t    = tk[sym]
    vol  = t["vol"]
    pct  = abs(t["pct"])
    score = 0

    # Volume score (0-40)
    if vol >= 100_000_000:  score += 40
    elif vol >= 50_000_000: score += 30
    elif vol >= 25_000_000: score += 20
    elif vol >= 15_000_000: score += 10

    # % change score — volatile tapi tidak terlalu jauh (0-35)
    if 0.5 <= pct <= 2.0:   score += 35   # sweet spot
    elif 0.3 <= pct < 0.5:  score += 20
    elif 2.0 < pct <= 4.0:  score += 25   # masih bagus
    elif pct > 4.0:         score += 10   # sudah over-extended

    # Bonus tier-1 coins
    if sym in ("BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT"):
        score += 25

    return min(score, 100)

# ═══════════════════════════════════════════════════════
#  SIGNAL ENGINE v20 — DUAL TIMEFRAME + VWAP + 1M CONFIRM
# ═══════════════════════════════════════════════════════
def signal(df5, df1=None):
    """
    Signal engine dengan dual timeframe (5m primary + 1m confirm).

    Perubahan dari v19:
    1. VWAP filter — harga harus di atas/bawah VWAP untuk konfirmasi arah
    2. 1m momentum check — konfirmasi arah sebelum entry
    3. Score lebih agresif tapi filter lebih cerdas
    4. ATR check lebih relaxed karena TP/SL sudah lebih kecil
    """
    if df5 is None or len(df5) < 55:
        return None, 0, [], 0.0

    row, prev, prev2 = df5.iloc[-2], df5.iloc[-3], df5.iloc[-4]
    p       = row["close"]
    e5, e9, e21, e50 = row["e5"], row["e9"], row["e21"], row["e50"]
    rsi     = row["rsi"]
    mh, mh_p, mh_p2 = row["mh"], prev["mh"], prev2["mh"]
    vr, br  = row["vr"], row["br"]
    m3, m5  = row["m3"], row["m5"]
    atr     = row["atr"]
    adx     = row["adx"]
    vwap    = row["vwap"]
    btc     = _macro["btc"]

    # ── ATR filter ──────────────────────────────────────
    atr_ratio = atr / p if p > 0 else 0
    if atr_ratio < ATR_MIN_RATIO or atr_ratio > ATR_MAX_RATIO:
        _stats["skipped_atr"] += 1
        return None, 0, [], atr

    if vr < MIN_VR:
        return None, 0, [], atr

    # ── BTC context ─────────────────────────────────────
    is_sideways  = btc in ("SIDEWAYS", "UNKNOWN")
    use_inverse  = btc in ("BULL", "BEAR", "MILD_BULL", "MILD_BEAR")
    score_thresh = MIN_SCORE_SW if is_sideways else MIN_SCORE

    lp = sp = 0
    sl, ss  = [], []

    # ── EMA stack (30-22 poin) ──────────────────────────
    if p > e5 > e9 > e21 > e50:   lp += 30; sl.append("EMA4↑")
    elif p > e5 > e9 > e21:       lp += 22; sl.append("EMA3↑")
    elif p > e9 > e21:            lp += 12; sl.append("EMA2↑")

    if p < e5 < e9 < e21 < e50:   sp += 30; ss.append("EMA4↓")
    elif p < e5 < e9 < e21:       sp += 22; ss.append("EMA3↓")
    elif p < e9 < e21:            sp += 12; ss.append("EMA2↓")

    # ── VWAP filter (15-8 poin) ─────────────────────────
    if not pd.isna(vwap):
        vwap_pct = (p - vwap) / vwap if vwap > 0 else 0
        if vwap_pct > 0.001:   lp += 15; sl.append(f"VWAP+{vwap_pct*100:.1f}%")
        elif vwap_pct > 0:     lp += 8;  sl.append("VWAP+")
        if vwap_pct < -0.001:  sp += 15; ss.append(f"VWAP-{abs(vwap_pct)*100:.1f}%")
        elif vwap_pct < 0:     sp += 8;  ss.append("VWAP-")

    # ── Momentum 3-bar & 5-bar (20-10 poin) ────────────
    if m3 > 0.004:    lp += 20; sl.append(f"M3+{m3*100:.1f}%")
    elif m3 > 0.002:  lp += 12; sl.append(f"M3+{m3*100:.1f}%")
    if m3 < -0.004:   sp += 20; ss.append(f"M3{m3*100:.1f}%")
    elif m3 < -0.002: sp += 12; ss.append(f"M3{m3*100:.1f}%")

    if m5 > 0.006:    lp += 15; sl.append(f"M5+")
    if m5 < -0.006:   sp += 15; ss.append(f"M5-")

    # ── MACD crossover (20-15 poin) ─────────────────────
    if mh_p <= 0 and mh > 0:           lp += 20; sl.append("MACD_X↑")
    elif mh > 0 and mh > mh_p > mh_p2: lp += 15; sl.append("MACD↑↑")
    if mh_p >= 0 and mh < 0:           sp += 20; ss.append("MACD_X↓")
    elif mh < 0 and mh < mh_p < mh_p2: sp += 15; ss.append("MACD↓↓")

    # ── Volume (15-8 poin) ──────────────────────────────
    if vr >= 3.0:
        lp += 15; sp += 15
        sl.append(f"Vol{vr:.1f}x"); ss.append(f"Vol{vr:.1f}x")
    elif vr >= 2.0:
        lp += 10; sp += 10
        sl.append(f"Vol{vr:.1f}x"); ss.append(f"Vol{vr:.1f}x")
    elif vr >= 1.5:
        lp += 5; sp += 5

    # ── Buy/Sell ratio (18-10 poin) ─────────────────────
    if br > 0.62:   lp += 18; sl.append(f"Buy{br:.0%}")
    elif br > 0.55: lp += 10; sl.append(f"Buy{br:.0%}")
    if br < 0.38:   sp += 18; ss.append(f"Sell{1-br:.0%}")
    elif br < 0.45: sp += 10; ss.append(f"Sell{1-br:.0%}")

    # ── RSI filter ──────────────────────────────────────
    if rsi > 78:
        lp = int(lp * 0.35); sp += 15; ss.append(f"OB{rsi:.0f}")
    elif rsi > 70:
        lp = int(lp * 0.6)
    elif rsi < 22:
        sp = int(sp * 0.35); lp += 15; sl.append(f"OS{rsi:.0f}")
    elif rsi < 30:
        sp = int(sp * 0.6)

    # ── ADX (trend strength) ────────────────────────────
    if adx > 30:
        lp += 10; sp += 10
        sl.append(f"ADX{adx:.0f}"); ss.append(f"ADX{adx:.0f}")
    elif adx < 15:
        # Trend lemah — kurangi score kedua arah
        lp = int(lp * 0.8)
        sp = int(sp * 0.8)

    # ── 1m konfirmasi (bonus 15 poin) ───────────────────
    if df1 is not None and len(df1) >= 20:
        try:
            r1  = df1.iloc[-2]
            m2  = r1["m2"]
            e3  = r1["e3"]
            e8  = r1["e8"]
            rsi1 = r1["rsi1"]
            mh1  = r1["mh1"]
            vr1  = r1["vr1"]
            p1   = r1["close"]

            # 1m LONG konfirmasi
            if m2 > 0.001 and e3 > e8 and rsi1 < 75:
                lp += 15; sl.append("1m↑")
            elif m2 > 0.0005 and mh1 > 0:
                lp += 8

            # 1m SHORT konfirmasi
            if m2 < -0.001 and e3 < e8 and rsi1 > 25:
                sp += 15; ss.append("1m↓")
            elif m2 < -0.0005 and mh1 < 0:
                sp += 8
        except:
            pass

    gap = abs(lp - sp)

    # ── Determine raw direction ──────────────────────────
    if lp > sp and lp >= score_thresh and gap >= MIN_GAP:
        raw_dir, raw_sc, raw_sigs = "LONG", lp, sl[:4]
    elif sp > lp and sp >= score_thresh and gap >= MIN_GAP:
        raw_dir, raw_sc, raw_sigs = "SHORT", sp, ss[:4]
    else:
        return None, max(lp, sp), [], atr

    # ── Inverse mode (saat BTC trend jelas) ─────────────
    if use_inverse:
        final_dir  = "SHORT" if raw_dir == "LONG" else "LONG"
        final_sigs = raw_sigs + ["(INV)"]
    else:
        final_dir  = raw_dir
        final_sigs = raw_sigs + ["(NRM)"]

    # ── Buy ratio check ──────────────────────────────────
    if final_dir == "LONG"  and br <= BR_LONG_MIN:  return None, raw_sc, [], atr
    if final_dir == "SHORT" and br >= BR_SHORT_MAX: return None, raw_sc, [], atr

    return final_dir, raw_sc, final_sigs, atr

# ═══════════════════════════════════════════════════════
#  REAL EXECUTION
# ═══════════════════════════════════════════════════════
def sync_binance_positions():
    try:
        pos_info = client.futures_position_information()
        active_on_binance = {}
        for p in pos_info:
            amt = float(p["positionAmt"])
            sym = p["symbol"]
            if amt != 0:
                side = "LONG" if amt > 0 else "SHORT"
                active_on_binance[sym] = {
                    "side": side, "qty": abs(amt),
                    "entry": float(p["entryPrice"])
                }
        with _lock:
            for local_sym in list(paper_positions.keys()):
                pos = paper_positions[local_sym]
                if pos.get("_r"):
                    continue
                if local_sym not in active_on_binance:
                    process_closed_position(local_sym, reason="Binance TP/SL")
                    paper_positions.pop(local_sym, None)
                    _position_peak.pop(local_sym, None)
            for bin_sym, data in active_on_binance.items():
                if bin_sym not in paper_positions:
                    paper_positions[bin_sym] = {
                        "side": data["side"], "entry": data["entry"],
                        "qty": data["qty"], "open_time": time.time(),
                        "score": 99, "sigs": ["RESTORED"],
                        "atr": 0.0, "trail_peak": data["entry"],
                        "trail_sl": data["entry"],
                    }
                    _position_peak[bin_sym] = data["entry"]
    except Exception as e:
        if "-1109" not in str(e):
            print(f"⚠️ Sync error: {e}")

def paper_open(sym, direction, score, sigs, price, atr):
    with _lock:
        if sym in paper_positions or len(paper_positions) >= MAX_POSITIONS:
            return
        paper_positions[sym] = {"_r": True}  # placeholder

    _logged_closes.discard(sym)
    _position_open_ts[sym] = int(time.time() * 1000)

    try:
        client.futures_change_leverage(symbol=sym, leverage=LEVERAGE)
        q = qty(price, sym)
        if q <= 0:
            with _lock: paper_positions.pop(sym, None)
            return

        # Fee budget check
        if not check_fee_budget(price, q):
            print(f"  ⚠️ [FEE SKIP] {sym} — fee ratio terlalu tinggi")
            _stats["skipped_fee"] += 1
            with _lock: paper_positions.pop(sym, None)
            _position_open_ts.pop(sym, None)
            return

        side_str = "BUY" if direction == "LONG" else "SELL"
        order = client.futures_create_order(
            symbol=sym, side=side_str,
            type="MARKET", quantity=q
        )
        exec_price = float(order.get('avgPrice', price))
        if exec_price == 0:
            exec_price = price

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
        print(f"❌ GAGAL ENTRY {sym}: {e}")
        try: client.futures_cancel_all_open_orders(symbol=sym)
        except: pass
        with _lock: paper_positions.pop(sym, None)
        _position_open_ts.pop(sym, None)
        return

    pos = {
        "side": direction, "entry": exec_price, "qty": q,
        "open_time": time.time(), "score": score, "sigs": sigs, "atr": atr,
        "tp": tp_price, "sl": sl_price,
        "trail_peak": exec_price,
        "trail_sl": sl_price,
    }
    with _lock:
        paper_positions[sym] = pos
        _position_peak[sym]  = exec_price

    d = "🟢" if direction == "LONG" else "🔴"
    notional     = q * exec_price
    real_margin  = notional / LEVERAGE
    expected_tp  = notional * EXTREME_PROFIT_PCT
    expected_fee = notional * TAKER_FEE_RATE * 2
    mode_tag     = "INV" if "(INV)" in sigs else "NRM"
    print(f"\n  {d} [ENTRY v20] {sym} {direction}[{mode_tag}] @{exec_price:.6g}")
    print(f"     Margin:${real_margin:.2f} | TP:+{EXTREME_PROFIT_PCT*100:.1f}%(${expected_tp:.4f}) SL:-{HARD_SL_PCT*100:.1f}%")
    print(f"     Fee est:${expected_fee:.4f} ({expected_fee/expected_tp*100:.0f}% TP) | Score:{score} | {' | '.join(sigs[:3])}")
    _stats["trades"] += 1

# ═══════════════════════════════════════════════════════
#  TRAILING STOP MONITOR
# ═══════════════════════════════════════════════════════
def update_trailing_stops():
    with _lock:
        positions_copy = dict(paper_positions)

    for sym, pos in positions_copy.items():
        if pos.get("_r") or "trail_sl" not in pos:
            continue
        try:
            current_price = price_live(sym)
            if current_price == 0:
                continue
            side = pos["side"]
            _, _, _, p_prec = get_precision_rules(sym)

            with _lock:
                if sym not in paper_positions:
                    continue
                pos_live = paper_positions[sym]

                if side == "LONG":
                    new_peak     = max(pos_live.get("trail_peak", current_price), current_price)
                    new_trail_sl = round(new_peak * (1 - TRAIL_PCT), p_prec)
                    new_trail_sl = max(new_trail_sl, pos_live["sl"])
                    pos_live["trail_peak"] = new_peak
                    pos_live["trail_sl"]   = new_trail_sl

                    if current_price <= new_trail_sl and current_price > pos_live["sl"]:
                        _execute_trailing_close(sym, pos_live, current_price, "TRAIL↓")

                else:  # SHORT
                    new_peak     = min(pos_live.get("trail_peak", current_price), current_price)
                    new_trail_sl = round(new_peak * (1 + TRAIL_PCT), p_prec)
                    new_trail_sl = min(new_trail_sl, pos_live["sl"])
                    pos_live["trail_peak"] = new_peak
                    pos_live["trail_sl"]   = new_trail_sl

                    if current_price >= new_trail_sl and current_price < pos_live["sl"]:
                        _execute_trailing_close(sym, pos_live, current_price, "TRAIL↑")

        except:
            pass

def _execute_trailing_close(sym, pos, current_price, reason):
    try:
        side       = pos["side"]
        close_side = "SELL" if side == "LONG" else "BUY"
        q          = pos["qty"]

        client.futures_cancel_all_open_orders(symbol=sym)
        client.futures_create_order(
            symbol=sym, side=close_side,
            type="MARKET", quantity=q, reduceOnly=True
        )

        entry = pos["entry"]
        gross = (current_price - entry) * q if side == "LONG" else (entry - current_price) * q
        fee   = current_price * q * TAKER_FEE_RATE
        pnl   = gross - fee

        e = "🟢" if pnl >= 0 else "🔴"
        print(f"  {e} [TRAIL] {sym} {side} @{current_price:.6g} | Net:{pnl:+.5f}U ({reason})")

        _stats["gross_pnl"]   += gross
        _stats["total_fee"]   += fee
        _stats["pnl"]         += pnl
        _stats["hist"].append(pnl)
        _stats["trail_exits"] += 1
        ks_upd(pnl)

        if pnl >= 0:
            _stats["wins"] += 1
            if pnl > _stats["best"]: _stats["best"] = pnl
        else:
            _stats["losses"] += 1
            if pnl < _stats["worst"]: _stats["worst"] = pnl

        trade_log.append({
            "sym": sym, "side": side,
            "entry": entry, "exit": current_price,
            "gross": round(gross, 5), "fee": round(fee, 5),
            "pnl": round(pnl, 5), "reason": reason,
        })
        _logged_closes.add(sym)
        _position_open_ts.pop(sym, None)
        _position_peak.pop(sym, None)
        paper_positions.pop(sym, None)
        set_cd(sym); _hot_syms.appendleft(sym); _rescan_q.put(1)
        print_inline()

    except Exception as e:
        print(f"⚠️ Gagal trail close {sym}: {e}")

def process_closed_position(sym, reason="Binance TP/SL"):
    if sym in _logged_closes:
        return
    _logged_closes.add(sym)
    try:
        open_ts = _position_open_ts.get(sym, 0)
        kwargs  = {"symbol": sym, "limit": 20}
        if open_ts > 0:
            kwargs["startTime"] = open_ts

        trades = client.futures_account_trades(**kwargs)
        if not trades:
            return

        gross_pnl = total_fee = price_exit = 0.0
        found = False
        for t in trades:
            rpnl = float(t.get("realizedPnl", 0))
            if rpnl != 0:
                gross_pnl += rpnl
                total_fee += float(t.get("commission", 0))
                price_exit = float(t.get("price", 0))
                found = True

        if not found:
            return

        net_pnl = gross_pnl - total_fee
        pos = paper_positions.get(sym, {})
        if pos:
            if net_pnl > 0: _stats["tp_exits"] += 1
            else:           _stats["sl_exits"] += 1

        e = "🟢" if net_pnl >= 0 else "🔴"
        print(f"  {e} [BNB CLOSE] {sym} | Gross:{gross_pnl:+.5f} Fee:{total_fee:.5f} Net:{net_pnl:+.5f}U")

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
            "gross": round(gross_pnl, 5), "fee": round(total_fee, 5),
            "pnl": round(net_pnl, 5), "reason": reason,
        })
        _position_open_ts.pop(sym, None)
        set_cd(sym); _hot_syms.appendleft(sym); _rescan_q.put(1)
        print_inline()
        try: client.futures_cancel_all_open_orders(symbol=sym)
        except: pass

    except Exception as e:
        print(f"⚠️ Gagal proses close {sym}: {e}")
        _position_open_ts.pop(sym, None)

# ═══════════════════════════════════════════════════════
#  SCANNER
# ═══════════════════════════════════════════════════════
def scan_one(sym):
    try:
        time.sleep(SCAN_DELAY)
        if not ok_cooldown(sym):
            return None
        tk = _ticker_cache
        if sym in tk and tk[sym]["vol"] < MIN_BASE_VOL:
            return None

        # Ambil 5m dan 1m data
        df5 = ohlcv(sym, Client.KLINE_INTERVAL_5MINUTE, 100)
        df1 = ohlcv(sym, Client.KLINE_INTERVAL_1MINUTE, 50)
        if df5 is None or len(df5) < 55:
            return None

        df5 = run_ta(df5.copy())
        df1 = run_ta_1m(df1.copy()) if df1 is not None and len(df1) >= 20 else None

        px  = df5["close"].iloc[-2]
        atr = df5["atr"].iloc[-2]
        if px == 0:
            return None

        dir_, sc, sigs, atr_val = signal(df5, df1)
        if dir_ is None or len(sigs) < 1:
            return None

        px_live = price_live(sym)
        if px_live == 0:
            return None

        # Tambah coin quality ke score
        cq = coin_quality_score(sym)
        final_score = sc + int(cq * 0.2)  # bonus max 20 poin dari coin quality

        return (sym, dir_, final_score, sigs, px_live, atr_val)
    except:
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

def top_movers(syms, n=25):
    """Pilih coin dengan volume + volatilitas terbaik."""
    tk = tickers_all()
    ss = set(syms)
    # Gabungkan % change dan volume untuk ranking
    mv = []
    for s, d in tk.items():
        if s not in ss or d["vol"] < MIN_BASE_VOL:
            continue
        pct = abs(d["pct"])
        vol = d["vol"]
        # Score: volatilitas 1-4% adalah sweet spot
        vol_score = min(vol / 10_000_000, 10)   # max 10
        pct_score = 10 - abs(pct - 2.0)         # peak di 2%, berkurang jika terlalu tinggi/rendah
        combined  = vol_score + max(pct_score, 0)
        mv.append((s, combined))
    return [s for s, _ in sorted(mv, key=lambda x: x[1], reverse=True)[:n]]

# ═══════════════════════════════════════════════════════
#  PRINT STATS
# ═══════════════════════════════════════════════════════
def print_inline():
    n   = _stats["wins"] + _stats["losses"]
    wr  = _stats["wins"] / n * 100 if n else 0
    pnl = _stats["pnl"]
    e   = "💚" if pnl >= 0 else "🔴"
    fee = _stats["total_fee"]
    print(f"     ┌ [v20] {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']} "
          f"{e}Net:{pnl:+.4f}U Fee:{fee:.4f}U "
          f"Trail:{_stats['trail_exits']} TP:{_stats['tp_exits']} SL:{_stats['sl_exits']}")

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

    # Break-even WR berdasarkan RR aktual
    rr_ratio = EXTREME_PROFIT_PCT / HARD_SL_PCT  # = 2.0
    be_wr    = 100 / (1 + rr_ratio)               # = 33.3%

    print(f"\n  {'─'*68}")
    print(f"  🚀 BOT SCALPING v20.0 — {sess*60:.0f}m | {tph:.1f}T/jam")
    print(f"  🎯 {n}T WR:{wr:.1f}% W:{_stats['wins']} L:{_stats['losses']}")
    print(f"  📐 RR:{rr_ratio:.0f}:1 (BE_WR:{be_wr:.0f}%) TP:{EXTREME_PROFIT_PCT*100:.1f}% SL:{HARD_SL_PCT*100:.1f}%")
    print(f"  {e} Net:{pnl:+.5f}U  Gross:{_stats['gross_pnl']:+.5f}U  Fee:{_stats['total_fee']:.5f}U")
    print(f"  📊 Best:{_stats['best']:+.5f}  Worst:{_stats['worst']:+.5f}")
    print(f"  📐 Sharpe:{sh:.2f} MaxDD:{md:.5f}U")
    print(f"  🔧 Trail:{_stats['trail_exits']} TP:{_stats['tp_exits']} SL:{_stats['sl_exits']} "
          f"SkipFee:{_stats['skipped_fee']} SkipATR:{_stats['skipped_atr']}")
    print(f"  📡 BTC:{_macro['btc']} | Mode:{'INV' if _macro['btc'] in ('BULL','BEAR','MILD_BULL','MILD_BEAR') else 'NRM'} "
          f"| F&G:{_macro['fng']}")
    print(f"  KS: consec={_ks['consec']} daily={_ks['daily']:+.4f}")
    if trade_log:
        print(f"  📋 Last 5 trades:")
        for t in trade_log[-5:]:
            em = "🟢" if t["pnl"] > 0 else "🔴"
            print(f"     {em} {t['sym']:<14} Net:{t['pnl']:+.5f}U "
                  f"(G:{t['gross']:+.5f} F:{t['fee']:.5f}) — {t['reason']}")
    print(f"  {'─'*68}")

# ═══════════════════════════════════════════════════════
#  BACKGROUND THREADS
# ═══════════════════════════════════════════════════════
def t_monitor():
    consecutive_errors = 0
    while True:
        try:
            sync_binance_positions()
            consecutive_errors = 0
        except Exception as e:
            consecutive_errors += 1
            if "-1109" in str(e):
                time.sleep(min(30, 5 * consecutive_errors))
                continue
        time.sleep(3.0)

def t_trailing():
    while True:
        try:
            update_trailing_stops()
        except:
            pass
        time.sleep(TRAIL_CHECK_INTERVAL)

def t_rescan(syms):
    while True:
        try:
            _rescan_q.get(timeout=30)
            time.sleep(0.2)
            with _lock:
                sync_binance_positions()
                slots = MAX_POSITIONS - len(paper_positions)
            if slots <= 0 or ks_check()[0]:
                continue
            hot  = [s for s in _hot_syms if s not in paper_positions]
            rest = [s for s in syms if s not in paper_positions and s not in hot]
            res  = scan_batch((hot + rest)[:30])
            if res:
                res.sort(key=lambda x: x[2], reverse=True)
                for r in res[:slots]:
                    if len(paper_positions) >= MAX_POSITIONS:
                        break
                    sym, d, sc, sg, px, atr = r
                    paper_open(sym, d, sc, sg, px, atr)
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
                r = requests.get(
                    "https://api.alternative.me/fng/?limit=1", timeout=5
                ).json()
                _macro["fng"]      = int(r["data"][0]["value"])
                _macro["last_fng"] = time.time()
        except:
            pass
        time.sleep(5)

# ═══════════════════════════════════════════════════════
#  MAIN LOOP
# ═══════════════════════════════════════════════════════
def run_bot():
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║  🚀 BOT SCALPING v20.0 — SPEED-FIRST REBUILD                ║")
    print("╠══════════════════════════════════════════════════════════════╣")
    print(f"║  Margin: ${ORDER_USDT:.1f} × {LEVERAGE}x Lev = ${ORDER_USDT*LEVERAGE:.0f} notional/posisi       ║")
    print(f"║  TP: {EXTREME_PROFIT_PCT*100:.1f}% (${ORDER_USDT*LEVERAGE*EXTREME_PROFIT_PCT:.4f})  SL: {HARD_SL_PCT*100:.1f}% (${ORDER_USDT*LEVERAGE*HARD_SL_PCT:.4f})  RR: 2:1      ║")
    print(f"║  Break-even WR: 33.3% | Fee/trade ~${ORDER_USDT*LEVERAGE*TAKER_FEE_RATE*2:.4f}             ║")
    print(f"║  Trail: {TRAIL_PCT*100:.2f}% | MIN_SCORE: {MIN_SCORE} | MAX_POS: {MAX_POSITIONS}                    ║")
    print("╚══════════════════════════════════════════════════════════════╝")

    try:
        valid = {
            s["symbol"] for s in client.futures_exchange_info()["symbols"]
            if s["status"] == "TRADING"
        }
        syms = list(dict.fromkeys([s for s in SYMBOLS if s in valid]))
    except:
        syms = list(dict.fromkeys(SYMBOLS))

    print(f"  ✅ {len(syms)} symbol aktif")
    sync_binance_positions()

    threading.Thread(target=t_monitor,              daemon=True).start()
    threading.Thread(target=t_trailing,             daemon=True).start()
    threading.Thread(target=t_rescan, args=(syms,), daemon=True).start()
    threading.Thread(target=t_macro,                daemon=True).start()

    time.sleep(4)
    tickers_all()

    cycle    = scan_idx = 0
    n_bat    = math.ceil(len(syms) / BATCH_SIZE)

    while True:
        cycle += 1
        with _lock:
            sync_binance_positions()
            slots = MAX_POSITIONS - len(paper_positions)

        mode_str = "INV" if _macro["btc"] in ("BULL","BEAR","MILD_BULL","MILD_BEAR") else "NRM"
        pos_str  = " | ".join(
            f"{s}({v['side']})" for s, v in paper_positions.items() if not v.get("_r")
        ) or "none"
        print(f"\n{'═'*62}")
        print(f"  #{cycle} {time.strftime('%H:%M:%S')} BTC:{_macro['btc']}[{mode_str}] "
              f"({len(paper_positions)}/{MAX_POSITIONS}) Net:{_stats['pnl']:+.4f}U")
        print(f"  Positions: {pos_str}")

        if (k := ks_check())[0]:
            print(f"  🚨 KILL SWITCH: {k[1]}")
            time.sleep(SCAN_INTERVAL)
            continue

        if slots > 0:
            # Ambil top movers (volatilitas + volume terbaik)
            mv = top_movers(syms, 25)
            mv = [s for s in mv if s not in paper_positions]

            # Scan batch reguler
            bs  = scan_idx * BATCH_SIZE
            reg = [
                s for s in syms[bs:bs+BATCH_SIZE]
                if s not in paper_positions and s not in mv
            ]
            scan_idx  = (scan_idx + 1) % n_bat
            scan_list = mv[:18] + reg[:10]  # prioritas movers

            try:
                res = scan_batch(scan_list)
            except:
                res = []

            if res:
                # Sort by score (coin quality sudah masuk ke score)
                res.sort(key=lambda x: x[2], reverse=True)
                for r in res[:slots]:
                    if len(paper_positions) >= MAX_POSITIONS:
                        break
                    sym, d, sc, sg, px, atr = r
                    cq  = coin_quality_score(sym)
                    mode_tag = "INV" if "(INV)" in sg else "NRM"
                    print(f"     ⭐ {sym} {d}[{mode_tag}] Score:{sc}(CQ:{cq}) ATR:{atr:.4g} → {' | '.join(sg[:3])}")
                    paper_open(sym, d, sc, sg, px, atr)

            elif len(paper_positions) == 0:
                # Full scan jika tidak ada posisi sama sekali
                try:
                    r2 = scan_batch([s for s in syms if s not in paper_positions])
                except:
                    r2 = []
                if r2:
                    r2.sort(key=lambda x: x[2], reverse=True)
                    sym, d, sc, sg, px, atr = r2[0]
                    paper_open(sym, d, sc, sg, px, atr)
                else:
                    print(f"  ⏳ No signal found (MIN_SCORE={MIN_SCORE})")

        if cycle % 10 == 0:
            print_full()

        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    run_bot()
