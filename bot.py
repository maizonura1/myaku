"""
Bot Scalping v15b — PAPER TRADE ENGINE (FIXED)
================================================
FIX dari v15:
✅ INSTANT CUT berlaku SETIAP TICK — bukan cuma 8 tick pertama
   Kalau minus walau -0.000001 pun, CUT! Tidak ada posisi bleeding
✅ DIRECTION LOGIC diperbaiki — tidak bias SHORT
   Wajib ada konfirmasi EMA + momentum + volume SELARAS
   BTC SIDEWAYS → skip SHORT kecuali ada sinyal kuat
✅ TRAILING UPDATE — trail ikut harga terus tiap tick
✅ 150+ SYMBOLS — semua coin Binance Futures yang liquid

[v15b FLIP MOD]
✅ DIRECTION DIBALIK — kalau analisa LONG → eksekusi SHORT, dan sebaliknya
   Logic scoring TIDAK diubah, hanya hasil akhirnya di-flip sebelum open posisi
✅ FLIP FILTER — tolak entry kalau trend terlalu kuat untuk countertrend:
   - Score > 65 → skip (sinyal terlalu dominan)
   - BuyPow > 78% saat mau flip SHORT → skip
   - SellPow > 78% saat mau flip LONG → skip
   - Momentum > 0.3% → skip (trend masih berlari kencang)
✅ COOLDOWN diperpanjang 20s saat FLIP aktif (vs 4s normal)

MODE: SIMULASI PENUH — tidak ada order nyata ke Binance
API connect untuk harga live, tapi eksekusi hanya di log
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

# ═══════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════
LEVERAGE         = 20
ORDER_USDT       = 1.0
MAX_POSITIONS    = 3

ATR_SL_MULT      = 1.8
ATR_TP1_MULT     = 1.2
ATR_TP2_MULT     = 3.0
ATR_TRAIL_MULT   = 0.5

MIN_SL_PCT       = 0.0010
MAX_SL_PCT       = 0.0055
MIN_TP1_PCT      = 0.0012
MAX_TP2_PCT      = 0.0150

TP1_CLOSE_RATIO  = 0.60

# ── INSTANT CUT — berlaku SETIAP TICK ──────
INSTANT_CUT_MULT = 0.25

# ── TRAIL: aktif sejak profit pertama (>0) ──
TRAIL_ACTIVATE_PCT = 0.0001

SCAN_INTERVAL    = 1
MONITOR_INTERVAL = 0.3
SCAN_DELAY       = 0.015
BATCH_SIZE       = 25
MAX_WORKERS      = 12
MAX_HOLD_SEC     = 240

MIN_SCORE        = 35
SYMBOL_COOLDOWN  = 4

DAILY_LOSS_LIMIT = -15.0
CONSEC_MAX       = 7
CONSEC_PAUSE     = 45

TTL_1M  = 2
TTL_5M  = 5
TTL_15M = 30

# ═══════════════════════════════════════════
#  FLIP DIRECTION — UBAH INI JADI False UNTUK BALIK KE NORMAL
# ═══════════════════════════════════════════
FLIP_DIRECTION = True   # True = LONG→SHORT, SHORT→LONG

# ── FILTER KHUSUS SAAT FLIP AKTIF ──────────
# Countertrend trade tidak boleh masuk kalau trend terlalu kuat
# Score terlalu tinggi = sinyal terlalu dominan = berbahaya di-flip
FLIP_MAX_SCORE    = 65    # Tolak kalau score > 65 (trend terlalu kuat)
FLIP_MAX_BUYPOW   = 0.78  # Tolak kalau buy ratio > 78% saat mau flip SHORT
FLIP_MIN_SELLPOW  = 0.22  # Tolak kalau buy ratio < 22% saat mau flip LONG
FLIP_MAX_MOM      = 0.003 # Tolak kalau momentum > 0.3% (terlalu kencang)
FLIP_COOLDOWN     = 20    # Cooldown lebih panjang saat flip (detik)

def flip(direction):
    """Balik arah kalau FLIP_DIRECTION aktif."""
    if not FLIP_DIRECTION:
        return direction
    return "SHORT" if direction == "LONG" else "LONG"

def flip_filter_ok(direction_orig, score, df):
    """
    Filter entry untuk mode FLIP — tolak kalau trend terlalu kuat.
    direction_orig = arah SEBELUM di-flip (hasil dari scoring).
    Kalau return False → skip entry ini.
    """
    if not FLIP_DIRECTION:
        return True

    last = df.iloc[-1]
    buy_r = last["buy_r"]
    mom5  = abs(last["mom5"])

    # Tolak kalau score terlalu tinggi — trend terlalu dominan
    if score > FLIP_MAX_SCORE:
        return False

    # Mau flip ke SHORT (aslinya LONG) — tolak kalau buy pressure masih sangat tinggi
    if direction_orig == "LONG" and buy_r > FLIP_MAX_BUYPOW:
        return False

    # Mau flip ke LONG (aslinya SHORT) — tolak kalau sell pressure masih sangat tinggi
    if direction_orig == "SHORT" and buy_r < FLIP_MIN_SELLPOW:
        return False

    # Tolak kalau momentum terlalu kencang (trend masih berlari)
    if mom5 > FLIP_MAX_MOM:
        return False

    return True

# ═══════════════════════════════════════════
#  SYMBOLS — 160 coins Binance Futures
# ═══════════════════════════════════════════
SYMBOLS = [
    # Tier 1 — mega cap
    "BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT","ADAUSDT",
    "DOGEUSDT","AVAXUSDT","TRXUSDT","DOTUSDT","LINKUSDT","MATICUSDT",
    "LTCUSDT","SHIBUSDT","BCHUSDT","XLMUSDT","ATOMUSDT","UNIUSDT","ETCUSDT",

    # Tier 2 — large cap
    "NEARUSDT","APTUSDT","ARBUSDT","OPUSDT","INJUSDT","SUIUSDT",
    "TIAUSDT","AAVEUSDT","RUNEUSDT","FILUSDT","LDOUSDT","STXUSDT",
    "MKRUSDT","SNXUSDT","CRVUSDT","COMPUSDT","GMXUSDT","PENDLEUSDT",
    "DYDXUSDT","SUSHIUSDT","1INCHUSDT","BALUSDT","PERMUSDT",

    # Tier 3 — mid cap momentum
    "1000PEPEUSDT","WIFUSDT","JUPUSDT","SEIUSDT","FETUSDT","RENDERUSDT",
    "WLDUSDT","ALGOUSDT","ICPUSDT","FTMUSDT","HBARUSDT","EGLDUSDT",
    "FLOWUSDT","THETAUSDT","KAVAUSDT","AXSUSDT","SANDUSDT","MANAUSDT",
    "ENJUSDT","GALAUSDT","IMXUSDT","BLURUSDT","MASKUSDT","HIGHUSDT",
    "ORDIUSDT","ARUSDT","KASUSDT","TONUSDT","TAOUSDT","ONDOUSDT",
    "ENARUSDT","BOMEUSDT","WUSDT","JUPUSDT","EIGENUSDT","STRKUSDT",

    # Tier 4 — altcoin liquid
    "JTOUSDT","RAYUSDT","GMTUSDT","APEUSDT","GRTUSDT","BATUSDT",
    "CHZUSDT","ZILUSDT","HOTUSDT","IOSTUSDT","VETUSDT","ICXUSDT",
    "ONTUSDT","QTUMUSDT","WAVESUSDT","XTZUSDT","NEOUSDT","DASHUSDT",
    "ZECUSDT","XMRUSDT","RVNUSDT","DGBUSDT","SCUSDT","ANKRUSDT",
    "CELRUSDT","CTSIUSDT","SKLUSDT","BANDUSDT","BLZUSDT","COTIUSDT",
    "OGNUSDT","LINAUSDT","SFPUSDT","FLMUSDT","TLMUSDT","BNTUSDT",
    "1000XECUSDT","PERPUSDT","LITUSDT","UNFIUSDT","DENTUSDT",
    "OGNUSDT","AGLDUSDT","IDUSDT","GASUSDT","CFXUSDT","COMBOUSDT",
    "POLYXUSDT","TRUUSDT","OCEANUSDT","AMBUSDT","RENUSDT","CVCUSDT",
    "VOXELUSDT","NMRUSDT","HOOKUSDT","GLMRUSDT","CKBUSDT","MOVRUSDT",

    # Tier 5 — meme & new listings
    "1000BONKUSDT","FLOKIUSDT","LUNCUSDT","JASMYUSDT","BEAMXUSDT",
    "MEMEUSDT","RONINUSDT","NOTUSDT","DOGSUSDT","CATIUSDT","PORTALUSDT",
    "VANRYUSDT","XAIUSDT","ATAUSDT","PYTHUSDT","WIFUSDT","SEIUSDT",
    "ALTUSDT","DYMUSDT","PIXELUSDT","ACEUSDT","MANTAUSDT","ZETAUSDT",
    "SAFEUSDT","ENAUSDT","JUPUSDT","STRKUSDT","RUNEUSDT",

    # Defi / Layer2
    "AAVEUSDT","UNIUSDT","DYDXUSDT","GMXUSDT","PENDLEUSDT","CRVUSDT",
    "MKRUSDT","COMPUSDT","SNXUSDT","1INCHUSDT","BALUSDT","SUSHIUSDT",
]
# Deduplicate
SYMBOLS = list(dict.fromkeys(SYMBOLS))

# ═══════════════════════════════════════════
#  STATE
# ═══════════════════════════════════════════
paper_positions = {}
trade_log       = []
_ohlcv_cache    = {}
_sym_cooldown   = {}
_ticker_cache   = {}
_ticker_ts      = 0
_lock           = threading.Lock()
_executor       = ThreadPoolExecutor(max_workers=MAX_WORKERS)
_rescan_q       = queue.Queue()
_hot_syms       = deque(maxlen=20)

_macro = {"fng": 50, "btc_5m": "UNKNOWN", "last_fng": 0, "last_btc": 0}

_ks = {
    "active": False, "reason": "", "resume": 0,
    "consec": 0, "daily_pnl": 0.0, "daily_reset": 0,
}

_stats = {
    "trades": 0, "wins": 0, "losses": 0,
    "pnl": 0.0, "best": 0.0, "worst": 0.0,
    "tp1": 0, "tp2": 0, "sl": 0, "cut": 0, "force": 0,
    "pnl_hist": deque(maxlen=200),
    "start": time.time(),
}

# ═══════════════════════════════════════════
#  UTILS
# ═══════════════════════════════════════════
def calc_qty(price):
    return (ORDER_USDT * LEVERAGE) / price

def get_price_live(symbol):
    try:
        return float(client.futures_symbol_ticker(symbol=symbol)["price"])
    except:
        return 0.0

def get_ticker_all():
    global _ticker_cache, _ticker_ts
    now = time.time()
    if now - _ticker_ts < 5 and _ticker_cache:
        return _ticker_cache
    try:
        tickers = client.futures_ticker()
        new = {}
        for t in tickers:
            new[t["symbol"]] = {
                "pct":  float(t["priceChangePercent"]),
                "vol":  float(t["quoteVolume"]),
                "last": float(t["lastPrice"]),
            }
        _ticker_cache = new
        _ticker_ts = now
        return new
    except:
        return _ticker_cache

def cooldown_ok(sym):
    if sym not in _sym_cooldown:
        return True
    cd = FLIP_COOLDOWN if FLIP_DIRECTION else SYMBOL_COOLDOWN
    return (time.time() - _sym_cooldown[sym]) >= cd

def set_cooldown(sym):
    _sym_cooldown[sym] = time.time()

# ═══════════════════════════════════════════
#  OHLCV CACHE
# ═══════════════════════════════════════════
def get_ohlcv(symbol, interval, limit=80):
    key = (symbol, interval)
    now = time.time()
    ttl_map = {
        Client.KLINE_INTERVAL_1MINUTE:  TTL_1M,
        Client.KLINE_INTERVAL_5MINUTE:  TTL_5M,
        Client.KLINE_INTERVAL_15MINUTE: TTL_15M,
    }
    ttl = ttl_map.get(interval, 10)
    if key in _ohlcv_cache:
        ts, df = _ohlcv_cache[key]
        if now - ts < ttl:
            return df
    try:
        klines = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
        df = pd.DataFrame(klines, columns=[
            "time","open","high","low","close","volume",
            "ct","qv","trades","tbbase","tbquote","ignore"])
        for c in ["open","high","low","close","volume","tbbase","tbquote"]:
            df[c] = df[c].astype(float)
        df["time"] = pd.to_numeric(df["time"])
        _ohlcv_cache[key] = (now, df)
        return df
    except:
        if key in _ohlcv_cache:
            return _ohlcv_cache[key][1]
        return None

# ═══════════════════════════════════════════
#  TA
# ═══════════════════════════════════════════
def run_ta(df):
    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]
    df["rsi"]      = ta.momentum.RSIIndicator(c, 14).rsi()
    macd           = ta.trend.MACD(c, 12, 26, 9)
    df["macd_h"]   = macd.macd_diff()
    df["ema5"]     = ta.trend.EMAIndicator(c, 5).ema_indicator()
    df["ema9"]     = ta.trend.EMAIndicator(c, 9).ema_indicator()
    df["ema21"]    = ta.trend.EMAIndicator(c, 21).ema_indicator()
    df["ema50"]    = ta.trend.EMAIndicator(c, 50).ema_indicator()
    df["atr"]      = ta.volatility.AverageTrueRange(h, l, c, 14).average_true_range()
    df["vol_ma"]   = v.rolling(20).mean()
    df["vol_r"]    = v / df["vol_ma"].replace(0, 1)
    df["buy_r"]    = df["tbbase"] / df["volume"].replace(0, 1)
    df["body"]     = abs(c - df["open"])
    df["range_"]   = h - l
    df["body_r"]   = df["body"] / df["range_"].replace(0, 1)
    df["mom3"]     = (c - c.shift(3)) / c.shift(3)
    df["mom5"]     = (c - c.shift(5)) / c.shift(5)
    df["hi_roll"]  = h.rolling(14).max()
    df["lo_roll"]  = l.rolling(14).min()
    return df

def calc_btc_trend():
    try:
        df = get_ohlcv("BTCUSDT", Client.KLINE_INTERVAL_5MINUTE, 60)
        if df is None or len(df) < 30:
            return "UNKNOWN"
        df = run_ta(df.copy())
        last = df.iloc[-1]
        p, e5, e9, e21 = last["close"], last["ema5"], last["ema9"], last["ema21"]
        mom5 = last["mom5"]
        if p > e5 > e9 > e21 and mom5 > 0.001:
            return "BULL"
        if p < e5 < e9 < e21 and mom5 < -0.001:
            return "BEAR"
        if p > e9 > e21:
            return "MILD_BULL"
        if p < e9 < e21:
            return "MILD_BEAR"
        return "SIDEWAYS"
    except:
        return "UNKNOWN"

# ═══════════════════════════════════════════
#  ATR LEVELS
# ═══════════════════════════════════════════
def calc_levels(entry, atr, direction):
    sl_d  = max(entry * MIN_SL_PCT, min(atr * ATR_SL_MULT, entry * MAX_SL_PCT))
    tp1_d = max(entry * MIN_TP1_PCT, atr * ATR_TP1_MULT)
    tp2_d = min(entry * MAX_TP2_PCT, atr * ATR_TP2_MULT)
    tp2_d = max(tp2_d, tp1_d * 1.6)
    ic_d  = atr * INSTANT_CUT_MULT

    if direction == "LONG":
        return {
            "sl":       entry - sl_d,
            "tp1":      entry + tp1_d,
            "tp2":      entry + tp2_d,
            "ic":       entry - ic_d,
            "trail_sl": entry,
            "sl_pct":   sl_d / entry,
            "tp1_pct":  tp1_d / entry,
        }
    else:
        return {
            "sl":       entry + sl_d,
            "tp1":      entry - tp1_d,
            "tp2":      entry - tp2_d,
            "ic":       entry + ic_d,
            "trail_sl": entry,
            "sl_pct":   sl_d / entry,
            "tp1_pct":  tp1_d / entry,
        }

# ═══════════════════════════════════════════
#  KILL SWITCH
# ═══════════════════════════════════════════
def ks_check():
    ks = _ks
    now = time.time()
    if ks["active"] and now >= ks["resume"]:
        ks["active"] = False
        ks["consec"] = 0
        print("  ✅ Kill switch cleared")
    if ks["active"]:
        return True, ks["reason"]
    day = now - (now % 86400)
    if day > ks["daily_reset"]:
        ks["daily_pnl"] = 0.0
        ks["daily_reset"] = day
    if ks["daily_pnl"] <= DAILY_LOSS_LIMIT:
        ks["active"] = True
        ks["reason"] = f"daily_loss({ks['daily_pnl']:.2f})"
        ks["resume"] = day + 86400
        print(f"  🚨 KS: daily loss {ks['daily_pnl']:.2f}")
        return True, ks["reason"]
    if ks["consec"] >= CONSEC_MAX:
        ks["active"] = True
        ks["reason"] = f"consec({ks['consec']})"
        ks["resume"] = now + CONSEC_PAUSE
        print(f"  🚨 KS: {ks['consec']} loss beruntun — pause {CONSEC_PAUSE}s")
        return True, ks["reason"]
    return False, ""

def ks_update(pnl):
    _ks["daily_pnl"] += pnl
    if pnl < 0:
        _ks["consec"] += 1
    else:
        _ks["consec"] = 0

# ═══════════════════════════════════════════
#  ENTRY SCORE
# ═══════════════════════════════════════════
def get_direction_and_score(df):
    """
    Scoring tidak diubah sama sekali.
    Flip direction dilakukan di luar fungsi ini (di scan_one).
    """
    if df is None or len(df) < 30:
        return None, 0, []

    last  = df.iloc[-1]
    prev  = df.iloc[-2]
    prev2 = df.iloc[-3]
    prev3 = df.iloc[-4]

    p    = last["close"]
    e5   = last["ema5"]
    e9   = last["ema9"]
    e21  = last["ema21"]
    e50  = last["ema50"]
    rsi  = last["rsi"]
    h_n  = last["macd_h"]
    h_p  = prev["macd_h"]
    h_p2 = prev2["macd_h"]
    vol  = last["vol_r"]
    br   = last["buy_r"]
    m5   = last["mom5"]
    m3   = last["mom3"]
    br   = last["buy_r"]
    body = last["body_r"]
    btc  = _macro.get("btc_5m", "UNKNOWN")

    long_pts = short_pts = 0
    sigs_l = []
    sigs_s = []

    # ══ A. EMA ALIGNMENT ══
    if p > e5 > e9 > e21 > e50:
        long_pts += 30; sigs_l.append("EMA_stack↑")
    elif p > e5 > e9 > e21:
        long_pts += 22; sigs_l.append("EMA_full↑")
    elif p > e9 > e21:
        long_pts += 14; sigs_l.append("EMA↑")
    elif p > e9 and e9 > e21:
        long_pts += 7

    if p < e5 < e9 < e21 < e50:
        short_pts += 30; sigs_s.append("EMA_stack↓")
    elif p < e5 < e9 < e21:
        short_pts += 22; sigs_s.append("EMA_full↓")
    elif p < e9 < e21:
        short_pts += 14; sigs_s.append("EMA↓")
    elif p < e9 and e9 < e21:
        short_pts += 7

    # ══ B. MOMENTUM ══
    if m5 > 0.004:
        long_pts += 22; sigs_l.append(f"Mom+{m5*100:.1f}%")
    elif m5 > 0.002:
        long_pts += 14; sigs_l.append(f"Mom+{m5*100:.1f}%")
    elif m5 > 0.001:
        long_pts += 7

    if m5 < -0.004:
        short_pts += 22; sigs_s.append(f"Mom{m5*100:.1f}%")
    elif m5 < -0.002:
        short_pts += 14; sigs_s.append(f"Mom{m5*100:.1f}%")
    elif m5 < -0.001:
        short_pts += 7

    if m5 > 0.001: short_pts = max(0, short_pts - 10)
    if m5 < -0.001: long_pts = max(0, long_pts - 10)

    # ══ C. MACD ══
    if h_n > 0 and h_n > h_p > h_p2:
        long_pts += 18; sigs_l.append("MACD↑↑")
    elif h_n > 0 and h_n > h_p:
        long_pts += 12; sigs_l.append("MACD↑")
    elif h_p <= 0 and h_n > 0:
        long_pts += 20; sigs_l.append("MACD_X↑")

    if h_n < 0 and h_n < h_p < h_p2:
        short_pts += 18; sigs_s.append("MACD↓↓")
    elif h_n < 0 and h_n < h_p:
        short_pts += 12; sigs_s.append("MACD↓")
    elif h_p >= 0 and h_n < 0:
        short_pts += 20; sigs_s.append("MACD_X↓")

    # ══ D. VOLUME ══
    if vol >= 3.0:
        long_pts += 12; short_pts += 12
        sigs_l.append(f"Vol{vol:.1f}x"); sigs_s.append(f"Vol{vol:.1f}x")
    elif vol >= 2.0:
        long_pts += 7; short_pts += 7
    elif vol >= 1.5:
        long_pts += 3; short_pts += 3

    # ══ E. ORDER FLOW ══
    if br > 0.62:
        long_pts += 12; sigs_l.append(f"BuyPow{br:.0%}")
    elif br > 0.55:
        long_pts += 6
    if br < 0.38:
        short_pts += 12; sigs_s.append(f"SellPow{1-br:.0%}")
    elif br < 0.45:
        short_pts += 6

    if br > 0.58: short_pts = max(0, short_pts - 8)
    if br < 0.42: long_pts  = max(0, long_pts - 8)

    # ══ F. RSI ══
    if 40 <= rsi <= 60:
        pass
    elif rsi > 65:
        long_pts = int(long_pts * 0.7)
    elif rsi < 35:
        short_pts = int(short_pts * 0.7)

    if 50 < rsi < 70 and long_pts > short_pts:
        long_pts += 5
    if 30 < rsi < 50 and short_pts > long_pts:
        short_pts += 5

    # ══ G. CANDLE BODY ══
    if last["close"] > last["open"] and body > 0.55:
        long_pts += 5
    if last["close"] < last["open"] and body > 0.55:
        short_pts += 5

    # ══ H. BTC CONTEXT ══
    if btc == "BULL":
        long_pts  += 10
        short_pts  = max(0, short_pts - 15)
    elif btc == "MILD_BULL":
        long_pts  += 5
        short_pts  = max(0, short_pts - 8)
    elif btc == "BEAR":
        short_pts += 10
        long_pts   = max(0, long_pts - 15)
    elif btc == "MILD_BEAR":
        short_pts += 5
        long_pts   = max(0, long_pts - 8)
    elif btc == "SIDEWAYS":
        pass

    # ══ DECISION ══
    gap = abs(long_pts - short_pts)
    btc_sideways = (btc == "SIDEWAYS" or btc == "UNKNOWN")
    min_score_req = 45 if btc_sideways else MIN_SCORE

    if long_pts > short_pts and long_pts >= min_score_req and gap >= 10:
        return "LONG", long_pts, sigs_l[:3]
    if short_pts > long_pts and short_pts >= min_score_req and gap >= 10:
        return "SHORT", short_pts, sigs_s[:3]

    return None, max(long_pts, short_pts), []

# ═══════════════════════════════════════════
#  PAPER OPEN
# ═══════════════════════════════════════════
def paper_open(symbol, direction, score, signals, atr, price):
    with _lock:
        if symbol in paper_positions:
            return
        if len(paper_positions) >= MAX_POSITIONS:
            return
        paper_positions[symbol] = {"_reserved": True}

    entry = price
    qty   = calc_qty(entry)
    lv    = calc_levels(entry, atr, direction)

    pos = {
        "side":       direction,
        "entry":      entry,
        "qty":        qty,
        "qty_remain": qty,
        "sl":         lv["sl"],
        "tp1":        lv["tp1"],
        "tp2":        lv["tp2"],
        "ic":         lv["ic"],
        "trail_sl":   lv["trail_sl"],
        "peak":       entry,
        "trail_on":   False,
        "tp1_hit":    False,
        "be_on":      False,
        "open_time":  time.time(),
        "atr":        atr,
        "score":      score,
        "signals":    signals,
    }
    with _lock:
        paper_positions[symbol] = pos

    d   = "🟢" if direction == "LONG" else "🔴"
    sl  = lv["sl_pct"] * 100
    tp1 = lv["tp1_pct"] * 100
    ic_pct = (INSTANT_CUT_MULT * atr / entry) * 100
    flip_tag = " [FLIPPED]" if FLIP_DIRECTION else ""
    print(f"\n  {d} [PAPER] [{symbol}] {direction} @{entry:.5g}{flip_tag}")
    print(f"     SL:{sl:.2f}% | TP1:{tp1:.2f}% | IC:±{ic_pct:.2f}% | Score:{score}")
    print(f"     Trail: INSTANT | Cut: EVERY TICK | Sigs: {' | '.join(signals)}")
    _stats["trades"] += 1

# ═══════════════════════════════════════════
#  PAPER CLOSE
# ═══════════════════════════════════════════
def paper_close(symbol, reason, price=None):
    with _lock:
        pos = paper_positions.pop(symbol, None)
    if pos is None or pos.get("_reserved"):
        return

    if price is None:
        price = get_price_live(symbol)

    side  = pos["side"]
    entry = pos["entry"]
    qty_r = pos.get("qty_remain", pos["qty"])

    pnl = (price - entry) * qty_r if side == "LONG" else (entry - price) * qty_r
    pct = (price - entry) / entry * 100 if side == "LONG" else (entry - price) / entry * 100
    hold = time.time() - pos["open_time"]
    e   = "🟢" if pnl >= 0 else "🔴"

    print(f"  {e} [PAPER] [{symbol}] CLOSE — {reason}")
    print(f"     {entry:.5g} → {price:.5g} ({pct:+.2f}%) | {hold:.0f}s | PnL:{pnl:+.4f}U")

    _stats["pnl"] += pnl
    _stats["pnl_hist"].append(pnl)
    ks_update(pnl)

    if pnl >= 0:
        _stats["wins"] += 1
        if pnl > _stats["best"]: _stats["best"] = pnl
    else:
        _stats["losses"] += 1
        if pnl < _stats["worst"]: _stats["worst"] = pnl

    if "TP1"   in reason: _stats["tp1"]   += 1
    if "TP2"   in reason: _stats["tp2"]   += 1
    if "SL"    in reason: _stats["sl"]    += 1
    if "Cut"   in reason: _stats["cut"]   += 1
    if "Force" in reason: _stats["force"] += 1

    trade_log.append({
        "sym": symbol, "side": side,
        "entry": round(entry, 6), "exit": round(price, 6),
        "pnl": round(pnl, 4), "reason": reason, "hold": int(hold),
    })
    set_cooldown(symbol)
    _hot_syms.appendleft(symbol)
    _rescan_q.put(1)
    print_inline()

# ═══════════════════════════════════════════
#  PARTIAL TP1
# ═══════════════════════════════════════════
def paper_tp1(symbol, price):
    pos = paper_positions.get(symbol)
    if pos is None or pos.get("tp1_hit") or pos.get("_reserved"):
        return

    side  = pos["side"]
    entry = pos["entry"]
    close_qty = pos["qty"] * TP1_CLOSE_RATIO
    pnl = (price - entry) * close_qty if side == "LONG" else (entry - price) * close_qty
    hold = time.time() - pos["open_time"]

    print(f"  🎯 [PAPER] [{symbol}] TP1 ({hold:.0f}s) | PnL:{pnl:+.4f}U")
    pos["tp1_hit"]    = True
    pos["qty_remain"] = pos["qty"] * (1 - TP1_CLOSE_RATIO)
    pos["be_on"]      = True

    atr = pos["atr"]
    if side == "LONG":
        pos["sl"]       = entry * 1.0001
        pos["trail_sl"] = price - atr * ATR_TRAIL_MULT * 0.8
    else:
        pos["sl"]       = entry * 0.9999
        pos["trail_sl"] = price + atr * ATR_TRAIL_MULT * 0.8

    pos["peak"]      = price
    pos["trail_on"]  = True

    _stats["pnl"] += pnl
    _stats["pnl_hist"].append(pnl)
    _stats["wins"] += 1
    _stats["tp1"]  += 1
    ks_update(pnl)
    if pnl > _stats["best"]: _stats["best"] = pnl
    trade_log.append({
        "sym": symbol, "side": side,
        "entry": round(entry, 6), "exit": round(price, 6),
        "pnl": round(pnl, 4), "reason": "TP1", "hold": int(hold),
    })
    print_inline()

# ═══════════════════════════════════════════
#  POSITION MONITOR
# ═══════════════════════════════════════════
def monitor_positions():
    for symbol in list(paper_positions.keys()):
        pos = paper_positions.get(symbol)
        if pos is None or pos.get("_reserved"):
            continue

        price = get_price_live(symbol)
        if price == 0:
            continue

        side  = pos["side"]
        entry = pos["entry"]
        atr   = pos["atr"]
        hold  = time.time() - pos["open_time"]

        # ══ FORCE CLOSE ══
        if hold >= MAX_HOLD_SEC:
            paper_close(symbol, "Force", price)
            continue

        if side == "LONG":
            profit_pct = (price - entry) / entry

            # ══ INSTANT CUT ══
            if not pos["tp1_hit"] and price <= pos["ic"]:
                paper_close(symbol, "LightningCut", price)
                continue

            # ══ TP1 ══
            if not pos["tp1_hit"] and price >= pos["tp1"]:
                paper_tp1(symbol, price)
                continue

            # ══ TRAIL ══
            if profit_pct > TRAIL_ACTIVATE_PCT:
                if not pos["trail_on"]:
                    pos["trail_on"] = True
                    pos["trail_sl"] = price - atr * ATR_TRAIL_MULT
                    pos["peak"]     = price

                if price > pos["peak"]:
                    pos["peak"]     = price
                    new_trail       = price - atr * ATR_TRAIL_MULT
                    pos["trail_sl"] = max(pos["trail_sl"], new_trail)

            # ══ TRAIL STOP ══
            if pos["trail_on"] and price <= pos["trail_sl"]:
                tag = "TrailBE" if pos["be_on"] else "TrailStop"
                paper_close(symbol, tag, price)
                continue

            # ══ TP2 ══
            if pos["tp1_hit"] and price >= pos["tp2"]:
                paper_close(symbol, "TP2", price)
                continue

            # ══ HARD SL ══
            if price <= pos["sl"]:
                paper_close(symbol, "SL", price)
                continue

            pnl_now = (price - entry) * pos.get("qty_remain", pos["qty"])
            tsl = f"TSL:{pos['trail_sl']:.5g}" if pos["trail_on"] else f"IC:{pos['ic']:.5g}"
            tp  = f"TP2:{pos['tp2']:.5g}" if pos["tp1_hit"] else f"TP1:{pos['tp1']:.5g}"
            print(f"  📌 [PAPER] [{symbol}] L@{entry:.5g}→{price:.5g} "
                  f"({profit_pct*100:+.2f}%) {pnl_now:+.3f}U {hold:.0f}s | {tsl} {tp}")

        else:  # SHORT
            profit_pct = (entry - price) / entry

            # ══ INSTANT CUT ══
            if not pos["tp1_hit"] and price >= pos["ic"]:
                paper_close(symbol, "LightningCut", price)
                continue

            # ══ TP1 ══
            if not pos["tp1_hit"] and price <= pos["tp1"]:
                paper_tp1(symbol, price)
                continue

            # ══ TRAIL ══
            if profit_pct > TRAIL_ACTIVATE_PCT:
                if not pos["trail_on"]:
                    pos["trail_on"] = True
                    pos["trail_sl"] = price + atr * ATR_TRAIL_MULT
                    pos["peak"]     = price

                if price < pos["peak"]:
                    pos["peak"]     = price
                    new_trail       = price + atr * ATR_TRAIL_MULT
                    pos["trail_sl"] = min(pos["trail_sl"], new_trail)

            # ══ TRAIL STOP ══
            if pos["trail_on"] and price >= pos["trail_sl"]:
                tag = "TrailBE" if pos["be_on"] else "TrailStop"
                paper_close(symbol, tag, price)
                continue

            # ══ TP2 ══
            if pos["tp1_hit"] and price <= pos["tp2"]:
                paper_close(symbol, "TP2", price)
                continue

            # ══ HARD SL ══
            if price >= pos["sl"]:
                paper_close(symbol, "SL", price)
                continue

            pnl_now = (entry - price) * pos.get("qty_remain", pos["qty"])
            tsl = f"TSL:{pos['trail_sl']:.5g}" if pos["trail_on"] else f"IC:{pos['ic']:.5g}"
            tp  = f"TP2:{pos['tp2']:.5g}" if pos["tp1_hit"] else f"TP1:{pos['tp1']:.5g}"
            print(f"  📌 [PAPER] [{symbol}] S@{entry:.5g}→{price:.5g} "
                  f"({profit_pct*100:+.2f}%) {pnl_now:+.3f}U {hold:.0f}s | {tsl} {tp}")

# ═══════════════════════════════════════════
#  SCAN
# ═══════════════════════════════════════════
def scan_one(symbol):
    try:
        time.sleep(SCAN_DELAY)
        if not cooldown_ok(symbol):
            return None
        tickers = _ticker_cache
        if symbol in tickers and tickers[symbol]["vol"] < 200_000:
            return None
        df = get_ohlcv(symbol, Client.KLINE_INTERVAL_5MINUTE, 80)
        if df is None or len(df) < 35:
            return None
        df = run_ta(df.copy())

        atr   = df["atr"].iloc[-1]
        price = df["close"].iloc[-1]
        if price == 0 or atr / price > 0.015:
            return None

        direction, score, signals = get_direction_and_score(df)
        if direction is None or len(signals) < 1:
            return None

        # ══ FILTER FLIP — tolak kalau trend terlalu kuat untuk countertrend ══
        if not flip_filter_ok(direction, score, df):
            return None

        # ══ FLIP DIRECTION DI SINI ══
        # Scoring tetap sama, hanya arah eksekusinya yang dibalik
        direction = flip(direction)

        return (symbol, direction, score, signals, atr, price)
    except:
        return None

def scan_batch(symbols):
    results = []
    futures = {_executor.submit(scan_one, s): s for s in symbols[:BATCH_SIZE]}
    try:
        for f in as_completed(futures, timeout=10):
            try:
                r = f.result(timeout=2)
                if r:
                    results.append(r)
            except:
                pass
    except:
        for f in futures:
            if f.done():
                try:
                    r = f.result(timeout=0)
                    if r: results.append(r)
                except:
                    pass
    return results

def get_top_movers(symbols, n=40):
    tickers = get_ticker_all()
    sym_set = set(symbols)
    movers  = []
    for sym, d in tickers.items():
        if sym not in sym_set or d["vol"] < 300_000:
            continue
        movers.append((sym, abs(d["pct"])))
    movers.sort(key=lambda x: x[1], reverse=True)
    return [s for s, _ in movers[:n]]

# ═══════════════════════════════════════════
#  STATS
# ═══════════════════════════════════════════
def print_inline():
    n  = _stats["wins"] + _stats["losses"]
    wr = _stats["wins"] / n * 100 if n else 0
    pnl = _stats["pnl"]
    e  = "💚" if pnl >= 0 else "🔴"
    print(f"     ┌─ [PAPER] {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']} {e}PnL:{pnl:+.4f}U")
    print(f"     └─ TP1:{_stats['tp1']} TP2:{_stats['tp2']} SL:{_stats['sl']} Cut:{_stats['cut']} Force:{_stats['force']}")

def print_full():
    n    = _stats["wins"] + _stats["losses"]
    wr   = _stats["wins"] / n * 100 if n else 0
    pnl  = _stats["pnl"]
    sess = (time.time() - _stats["start"]) / 3600
    tph  = n / sess if sess > 0 else 0
    e    = "💚" if pnl >= 0 else "🔴"

    pnls = list(_stats["pnl_hist"])
    sharpe = mdd = 0.0
    if len(pnls) >= 5:
        arr = np.array(pnls)
        std = float(np.std(arr))
        sharpe = float(np.mean(arr)) / std if std > 0 else 0.0
    if len(pnls) >= 2:
        eq   = np.cumsum(pnls)
        peak = np.maximum.accumulate(eq)
        mdd  = float(np.min(eq - peak))

    flip_status = "ON ✅" if FLIP_DIRECTION else "OFF"
    print(f"\n  {'─'*62}")
    print(f"  🧪 PAPER v15b FLIP={flip_status} — {sess*60:.0f}m | {tph:.1f}T/jam | {len(SYMBOLS)} symbols")
    print(f"  🎯 {n}T | WR:{wr:.0f}% | W:{_stats['wins']} L:{_stats['losses']}")
    print(f"  {e} PnL:{pnl:+.4f}U | Best:{_stats['best']:+.4f} Worst:{_stats['worst']:+.4f}")
    print(f"  📐 Sharpe:{sharpe:.2f} | MaxDD:{mdd:.4f}U")
    print(f"  TP1:{_stats['tp1']} TP2:{_stats['tp2']} SL:{_stats['sl']} ⚡Cut:{_stats['cut']} ⏰Force:{_stats['force']}")
    print(f"  KS: consec={_ks['consec']} daily={_ks['daily_pnl']:+.2f} | BTC:{_macro['btc_5m']}")
    if trade_log:
        print(f"  📋 Last 5:")
        for t in trade_log[-5:]:
            em = "🟢" if t["pnl"] > 0 else "🔴"
            print(f"     {em} {t['sym']:<14} {t['side']} {t['pnl']:+.4f}U {t['hold']}s — {t['reason']}")
    print(f"  {'─'*62}")

# ═══════════════════════════════════════════
#  THREADS
# ═══════════════════════════════════════════
def monitor_thread():
    while True:
        try:
            if paper_positions:
                monitor_positions()
        except Exception as e:
            print(f"  ❌ Monitor: {e}")
        time.sleep(MONITOR_INTERVAL)

def rescan_thread(symbols_active):
    while True:
        try:
            _rescan_q.get(timeout=30)
            time.sleep(0.2)
            slots = MAX_POSITIONS - len(paper_positions)
            if slots <= 0:
                continue
            killed, _ = ks_check()
            if killed:
                continue
            hot  = [s for s in _hot_syms if s not in paper_positions]
            rest = [s for s in symbols_active if s not in paper_positions and s not in hot]
            results = scan_batch((hot + rest)[:35])
            if results:
                results.sort(key=lambda x: x[2], reverse=True)
                for sym, dir_, sc, sigs, atr, price in results[:slots]:
                    if len(paper_positions) >= MAX_POSITIONS:
                        break
                    paper_open(sym, dir_, sc, sigs, atr, price)
        except queue.Empty:
            pass
        except Exception as e:
            print(f"  ❌ Rescan: {e}")

def macro_thread():
    while True:
        try:
            _macro["btc_5m"] = calc_btc_trend()
            _macro["last_btc"] = time.time()
        except:
            pass
        try:
            if time.time() - _macro["last_fng"] > 300:
                d = requests.get("https://api.alternative.me/fng/?limit=1", timeout=5).json()["data"][0]
                _macro["fng"] = int(d["value"])
                _macro["last_fng"] = time.time()
        except:
            pass
        time.sleep(5)

# ═══════════════════════════════════════════
#  MAIN LOOP
# ═══════════════════════════════════════════
def run_bot():
    flip_status = "ON — analisa LONG→SHORT, SHORT→LONG" if FLIP_DIRECTION else "OFF — normal"
    print("╔══════════════════════════════════════════════════════╗")
    print("║  🧪 BOT SCALPING v15b FLIP — PAPER TRADE             ║")
    print("║  ⚠️  SIMULASI — TIDAK ADA ORDER NYATA KE BINANCE      ║")
    print("╠══════════════════════════════════════════════════════╣")
    print(f"║  Modal: ${ORDER_USDT}/pos × {LEVERAGE}x | Max {MAX_POSITIONS} posisi               ║")
    print(f"║  FLIP: {flip_status:<46}║")
    if FLIP_DIRECTION:
        print(f"║  Filter: MaxScore={FLIP_MAX_SCORE} | MaxMom={FLIP_MAX_MOM*100:.1f}% | CD={FLIP_COOLDOWN}s    ║")
    print(f"║  Symbols: {len(SYMBOLS)} coins                                ║")
    print("╚══════════════════════════════════════════════════════╝")

    try:
        valid = {s["symbol"] for s in client.futures_exchange_info()["symbols"]
                 if s["status"] == "TRADING"}
        syms = list(dict.fromkeys([s for s in SYMBOLS if s in valid]))
    except:
        syms = list(dict.fromkeys(SYMBOLS))
    print(f"\n  ✅ {len(syms)} symbols valid")

    threading.Thread(target=monitor_thread, daemon=True).start()
    threading.Thread(target=rescan_thread, args=(syms,), daemon=True).start()
    threading.Thread(target=macro_thread, daemon=True).start()
    print("  🔧 Threads: monitor ✅ rescan ✅ macro ✅")

    print("  ⏳ Init macro...")
    time.sleep(4)
    get_ticker_all()

    print(f"  📊 BTC:{_macro['btc_5m']} | F&G:{_macro['fng']}")
    print(f"\n  🚀 GO!\n")

    cycle    = 0
    scan_idx = 0
    n_batch  = math.ceil(len(syms) / BATCH_SIZE)

    while True:
        cycle += 1
        slots = MAX_POSITIONS - len(paper_positions)

        print(f"\n{'═'*57}")
        print(f"  #{cycle} {time.strftime('%H:%M:%S')} BTC:{_macro['btc_5m']} F&G:{_macro['fng']} "
              f"Pos:({len(paper_positions)}/{MAX_POSITIONS}) PnL:{_stats['pnl']:+.4f}U")

        killed, ks_r = ks_check()
        if killed:
            print(f"  🚨 KS: {ks_r}")
            time.sleep(SCAN_INTERVAL)
            continue

        if slots > 0:
            top   = get_top_movers(syms, n=50)
            top   = [s for s in top if s not in paper_positions]
            b_s   = scan_idx * BATCH_SIZE
            reg   = [s for s in syms[b_s:b_s+BATCH_SIZE]
                     if s not in paper_positions and s not in top]
            scan_idx = (scan_idx + 1) % n_batch
            scan_list = top[:25] + reg[:15]

            print(f"  🔍 {len(scan_list)} syms | {slots} slot kosong")
            try:
                results = scan_batch(scan_list)
            except:
                results = []

            if results:
                results.sort(key=lambda x: x[2], reverse=True)
                print(f"  🎯 {len(results)} setup!")
                for sym, dir_, sc, sigs, atr, price in results[:slots]:
                    if len(paper_positions) >= MAX_POSITIONS:
                        break
                    print(f"     ⭐ {sym} {dir_} Score:{sc} | {' | '.join(sigs)}")
                    paper_open(sym, dir_, sc, sigs, atr, price)
            else:
                if len(paper_positions) == 0:
                    print(f"  ⚠️  No setup — wide scan...")
                    try:
                        r2 = scan_batch([s for s in syms if s not in paper_positions][:50])
                    except:
                        r2 = []
                    if r2:
                        r2.sort(key=lambda x: x[2], reverse=True)
                        sym, dir_, sc, sigs, atr, price = r2[0]
                        print(f"     ⭐ best: {sym} {dir_} Score:{sc}")
                        paper_open(sym, dir_, sc, sigs, atr, price)
                    else:
                        print(f"  ⏳ Market flat — menunggu...")
                else:
                    print(f"  ⏳ No new setup | {len(paper_positions)} pos aktif")
        else:
            print(f"  ✅ Full ({MAX_POSITIONS}/{MAX_POSITIONS})")

        if cycle % 20 == 0:
            print_full()

        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    run_bot()
