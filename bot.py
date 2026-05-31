"""
Bot Scalping v15c — PAPER TRADE ENGINE (ROOT CAUSE FIXED)
===========================================================
FIX dari v15b — 3 root cause diperbaiki:

ROOT CAUSE #1: IC KEGEDEAN
  Dulu: IC = entry ± 0.25x ATR → bisa -0.2% sebelum cut
  Fix:  IC = entry ± 0.08% FIXED → max loss -0.08% × 20lev = -$0.016/trade
  
ROOT CAUSE #2: TP1 TERLALU JAUH  
  Dulu: TP1 = 1.2x ATR → susah kena, profit jarang hit
  Fix:  TP1 = entry ± 0.12% FIXED → lebih sering kena, WR naik

ROOT CAUSE #3: POSISI BLEEDING TIDAK DI-CUT
  Dulu: Trail hanya update kalau price bergerak sesuai arah
         Kalau melawan → trail diam → posisi bisa bleeding bebas
  Fix:  HARD PNL GUARD — kalau pnl < -0.10% dari entry → CUT SEKARANG
         Tidak peduli trail, tidak peduli IC, langsung potong

LOGIKA BARU:
  Entry → Profit? Trail ikut terus naik/turun
  Entry → Minus -0.08%? IC cut langsung
  Entry → Minus > -0.10% (any time)? Hard PNL guard cut
  Profit → Trail → Harga balik? Trail stop
  
MODE: SIMULASI — tidak ada order nyata ke Binance
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
#  CONFIG v15c — TIGHT RISK, QUICK PROFIT
# ═══════════════════════════════════════════════════════
LEVERAGE       = 20
ORDER_USDT     = 1.0
MAX_POSITIONS  = 3

# ── FIXED % LEVELS (bukan ATR-based untuk SL/TP) ──────
# Dengan leverage 20x:
#   TP1 0.12% × 20 = +2.4% dari modal ($0.024 profit dari $1)
#   IC  0.08% × 20 = -1.6% dari modal ($0.016 loss max per trade)
#   Guard 0.10% × 20 = -2.0% dari modal ($0.020 hard max)
TP1_PCT        = 0.0012   # +0.12% dari entry → TP1 partial
TP2_PCT        = 0.0035   # +0.35% dari entry → TP2 full
IC_PCT         = 0.0008   # -0.08% dari entry → instant cut
HARD_LOSS_PCT  = 0.0010   # -0.10% dari entry → hard PNL guard (cut no matter what)

# Trail: aktif setelah profit 0.05%, width 0.08%
TRAIL_ACTIVATE = 0.0005   # Profit 0.05% baru trail on
TRAIL_WIDTH    = 0.0008   # Trail ikut harga dengan jarak 0.08%

# TP1 partial — tutup 60%, sisakan 40% untuk trail ke TP2
TP1_RATIO      = 0.60

# Kecepatan
SCAN_INTERVAL  = 1
MONITOR_INT    = 0.25     # Monitor tiap 250ms
SCAN_DELAY     = 0.015
BATCH_SIZE     = 25
MAX_WORKERS    = 12
MAX_HOLD_SEC   = 180      # Max hold 3 menit

# Score threshold
MIN_SCORE      = 38
COOLDOWN_SEC   = 4

# Kill switch
DAILY_LOSS     = -10.0    # Lebih ketat karena per-trade loss lebih kecil
CONSEC_MAX     = 8
CONSEC_PAUSE   = 30

# Cache TTL
TTL_5M         = 5
TTL_15M        = 30

# ═══════════════════════════════════════════════════════
#  SYMBOLS — 158 coins
# ═══════════════════════════════════════════════════════
SYMBOLS = [
    # Tier 1 — mega cap, spread kecil
    "BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT","ADAUSDT",
    "DOGEUSDT","AVAXUSDT","TRXUSDT","DOTUSDT","LINKUSDT","MATICUSDT",
    "LTCUSDT","SHIBUSDT","BCHUSDT","XLMUSDT","ATOMUSDT","UNIUSDT","ETCUSDT",

    # Tier 2 — large cap
    "NEARUSDT","APTUSDT","ARBUSDT","OPUSDT","INJUSDT","SUIUSDT",
    "TIAUSDT","AAVEUSDT","RUNEUSDT","FILUSDT","LDOUSDT","STXUSDT",
    "MKRUSDT","SNXUSDT","CRVUSDT","COMPUSDT","GMXUSDT","PENDLEUSDT",
    "DYDXUSDT","SUSHIUSDT","1INCHUSDT","BALUSDT",

    # Tier 3 — mid cap momentum
    "1000PEPEUSDT","WIFUSDT","JUPUSDT","SEIUSDT","FETUSDT","RENDERUSDT",
    "WLDUSDT","ALGOUSDT","ICPUSDT","FTMUSDT","HBARUSDT","EGLDUSDT",
    "FLOWUSDT","THETAUSDT","KAVAUSDT","AXSUSDT","SANDUSDT","MANAUSDT",
    "ENJUSDT","GALAUSDT","IMXUSDT","MASKUSDT","HIGHUSDT",
    "ORDIUSDT","ARUSDT","KASUSDT","TONUSDT","TAOUSDT","ONDOUSDT",
    "ENARUSDT","WUSDT","EIGENUSDT","STRKUSDT",

    # Tier 4 — altcoin liquid
    "JTOUSDT","RAYUSDT","GMTUSDT","APEUSDT","GRTUSDT","BATUSDT",
    "CHZUSDT","ZILUSDT","HOTUSDT","IOSTUSDT","VETUSDT","ICXUSDT",
    "ONTUSDT","QTUMUSDT","WAVESUSDT","XTZUSDT","NEOUSDT","DASHUSDT",
    "ZECUSDT","RVNUSDT","ANKRUSDT","CELRUSDT","CTSIUSDT","SKLUSDT",
    "BANDUSDT","BLZUSDT","COTIUSDT","OGNUSDT","LINAUSDT","SFPUSDT",
    "FLMUSDT","TLMUSDT","BNTUSDT","1000XECUSDT","PERPUSDT","LITUSDT",
    "UNFIUSDT","DENTUSDT","AGLDUSDT","CFXUSDT","COMBOUSDT",
    "POLYXUSDT","TRUUSDT","OCEANUSDT","AMBUSDT","RENUSDT","CVCUSDT",
    "VOXELUSDT","NMRUSDT","HOOKUSDT","GLMRUSDT","CKBUSDT","MOVRUSDT",

    # Tier 5 — meme & new
    "1000BONKUSDT","FLOKIUSDT","LUNCUSDT","JASMYUSDT","BEAMXUSDT",
    "MEMEUSDT","RONINUSDT","NOTUSDT","DOGSUSDT","CATIUSDT","PORTALUSDT",
    "VANRYUSDT","XAIUSDT","ATAUSDT","PYTHUSDT","ALTUSDT","DYMUSDT",
    "PIXELUSDT","ACEUSDT","MANTAUSDT","ZETAUSDT","SAFEUSDT",

    # DeFi
    "AAVEUSDT","UNIUSDT","DYDXUSDT","GMXUSDT","PENDLEUSDT","CRVUSDT",
    "MKRUSDT","COMPUSDT","SNXUSDT","1INCHUSDT","BALUSDT","SUSHIUSDT",
]
SYMBOLS = list(dict.fromkeys(SYMBOLS))

# ═══════════════════════════════════════════════════════
#  STATE
# ═══════════════════════════════════════════════════════
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

_macro = {"fng": 50, "btc": "UNKNOWN", "last_fng": 0, "last_btc": 0}
_ks    = {"active": False, "reason": "", "resume": 0,
          "consec": 0, "daily": 0.0, "day_reset": 0}
_stats = {
    "trades": 0, "wins": 0, "losses": 0,
    "pnl": 0.0, "best": 0.0, "worst": 0.0,
    "tp1": 0, "tp2": 0, "sl": 0, "cut": 0, "guard": 0, "force": 0,
    "hist": deque(maxlen=200), "start": time.time(),
}

# ═══════════════════════════════════════════════════════
#  UTILS
# ═══════════════════════════════════════════════════════
def qty(price):
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
        new = {t["symbol"]: {
            "pct": float(t["priceChangePercent"]),
            "vol": float(t["quoteVolume"]),
            "last": float(t["lastPrice"]),
        } for t in raw}
        _ticker_cache = new
        _ticker_ts    = now
        return new
    except:
        return _ticker_cache

def ok_cooldown(sym):
    return (time.time() - _sym_cooldown.get(sym, 0)) >= COOLDOWN_SEC

def set_cd(sym):
    _sym_cooldown[sym] = time.time()

# ═══════════════════════════════════════════════════════
#  OHLCV
# ═══════════════════════════════════════════════════════
def ohlcv(symbol, interval, limit=80):
    key = (symbol, interval)
    now = time.time()
    ttl = {Client.KLINE_INTERVAL_5MINUTE: TTL_5M,
           Client.KLINE_INTERVAL_15MINUTE: TTL_15M}.get(interval, 10)
    if key in _ohlcv_cache:
        ts, df = _ohlcv_cache[key]
        if now - ts < ttl:
            return df
    try:
        kl = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
        df = pd.DataFrame(kl, columns=[
            "time","open","high","low","close","volume",
            "ct","qv","trades","tbbase","tbquote","ignore"])
        for c in ["open","high","low","close","volume","tbbase","tbquote"]:
            df[c] = df[c].astype(float)
        df["time"] = pd.to_numeric(df["time"])
        _ohlcv_cache[key] = (now, df)
        return df
    except:
        return _ohlcv_cache.get(key, (None, None))[1]

# ═══════════════════════════════════════════════════════
#  TA
# ═══════════════════════════════════════════════════════
def run_ta(df):
    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]
    df["rsi"]  = ta.momentum.RSIIndicator(c, 14).rsi()
    df["mh"]   = ta.trend.MACD(c, 12, 26, 9).macd_diff()
    df["e5"]   = ta.trend.EMAIndicator(c, 5).ema_indicator()
    df["e9"]   = ta.trend.EMAIndicator(c, 9).ema_indicator()
    df["e21"]  = ta.trend.EMAIndicator(c, 21).ema_indicator()
    df["e50"]  = ta.trend.EMAIndicator(c, 50).ema_indicator()
    df["atr"]  = ta.volatility.AverageTrueRange(h, l, c, 14).average_true_range()
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
        df = ohlcv("BTCUSDT", Client.KLINE_INTERVAL_5MINUTE, 60)
        if df is None or len(df) < 30:
            return "UNKNOWN"
        df  = run_ta(df.copy())
        row = df.iloc[-1]
        p, e5, e9, e21 = row["close"], row["e5"], row["e9"], row["e21"]
        m5 = row["m5"]
        if p > e5 > e9 > e21 and m5 > 0.001: return "BULL"
        if p < e5 < e9 < e21 and m5 < -0.001: return "BEAR"
        if p > e9 > e21: return "MILD_BULL"
        if p < e9 < e21: return "MILD_BEAR"
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
        print("  ✅ Kill switch off")
    if k["active"]:
        return True, k["reason"]
    day = now - (now % 86400)
    if day > k["day_reset"]:
        k["daily"] = 0.0; k["day_reset"] = day
    if k["daily"] <= DAILY_LOSS:
        k["active"] = True; k["reason"] = f"daily({k['daily']:.2f})"
        k["resume"] = day + 86400
        return True, k["reason"]
    if k["consec"] >= CONSEC_MAX:
        k["active"] = True; k["reason"] = f"consec({k['consec']})"
        k["resume"] = now + CONSEC_PAUSE
        print(f"  🚨 {k['consec']} loss beruntun — pause {CONSEC_PAUSE}s")
        return True, k["reason"]
    return False, ""

def ks_upd(pnl):
    _ks["daily"] += pnl
    _ks["consec"] = 0 if pnl >= 0 else _ks["consec"] + 1

# ═══════════════════════════════════════════════════════
#  SIGNAL ENGINE v15c — BALANCED DIRECTION
# ═══════════════════════════════════════════════════════
def signal(df):
    if df is None or len(df) < 35:
        return None, 0, []

    row   = df.iloc[-1]
    prev  = df.iloc[-2]
    prev2 = df.iloc[-3]

    p, e5, e9, e21, e50 = row["close"], row["e5"], row["e9"], row["e21"], row["e50"]
    rsi   = row["rsi"]
    mh    = row["mh"]
    mh_p  = prev["mh"]
    mh_p2 = prev2["mh"]
    vr    = row["vr"]
    br    = row["br"]
    m5    = row["m5"]
    m3    = row["m3"]
    body  = row["br2"]
    btc   = _macro["btc"]

    lp = sp = 0
    sl = ss = []

    # ═ A. EMA stack ═══════════════════
    if p > e5 > e9 > e21 > e50:   lp += 30; sl.append("EMA_stack↑")
    elif p > e5 > e9 > e21:        lp += 22; sl.append("EMA↑↑")
    elif p > e9 > e21:             lp += 14; sl.append("EMA↑")
    elif p > e9:                   lp += 7

    if p < e5 < e9 < e21 < e50:   sp += 30; ss.append("EMA_stack↓")
    elif p < e5 < e9 < e21:        sp += 22; ss.append("EMA↓↓")
    elif p < e9 < e21:             sp += 14; ss.append("EMA↓")
    elif p < e9:                   sp += 7

    # ═ B. Momentum — WAJIB selaras ════
    if m5 > 0.005:    lp += 25; sl.append(f"Mom+{m5*100:.1f}%")
    elif m5 > 0.003:  lp += 18; sl.append(f"Mom+{m5*100:.1f}%")
    elif m5 > 0.001:  lp += 10
    elif m5 < -0.001: lp = max(0, lp - 12)   # momentum berlawanan = penalti

    if m5 < -0.005:   sp += 25; ss.append(f"Mom{m5*100:.1f}%")
    elif m5 < -0.003: sp += 18; ss.append(f"Mom{m5*100:.1f}%")
    elif m5 < -0.001: sp += 10
    elif m5 > 0.001:  sp = max(0, sp - 12)

    # ═ C. MACD ════════════════════════
    if mh_p <= 0 and mh > 0:       lp += 22; sl.append("MACD_X↑")   # crossover
    elif mh > 0 and mh > mh_p > mh_p2: lp += 18; sl.append("MACD↑↑")
    elif mh > 0 and mh > mh_p:     lp += 12; sl.append("MACD↑")

    if mh_p >= 0 and mh < 0:       sp += 22; ss.append("MACD_X↓")
    elif mh < 0 and mh < mh_p < mh_p2: sp += 18; ss.append("MACD↓↓")
    elif mh < 0 and mh < mh_p:     sp += 12; ss.append("MACD↓")

    # ═ D. Volume ══════════════════════
    if vr >= 3.0:
        lp += 15; sp += 15
        sl.append(f"Vol{vr:.1f}x"); ss.append(f"Vol{vr:.1f}x")
    elif vr >= 2.0: lp += 8; sp += 8
    elif vr >= 1.5: lp += 4; sp += 4
    elif vr < 0.8:  lp = max(0, lp - 5); sp = max(0, sp - 5)  # volume sepi = penalti

    # ═ E. Buy/Sell pressure ══════════
    if br > 0.65:    lp += 15; sl.append(f"Buy{br:.0%}")
    elif br > 0.57:  lp += 8
    elif br < 0.43:  lp = max(0, lp - 10)

    if br < 0.35:    sp += 15; ss.append(f"Sell{1-br:.0%}")
    elif br < 0.43:  sp += 8
    elif br > 0.57:  sp = max(0, sp - 10)

    # ═ F. RSI ═════════════════════════
    if 48 < rsi < 65 and m5 > 0:   lp += 6   # RSI rising, not overbought
    if 35 < rsi < 52 and m5 < 0:   sp += 6   # RSI falling, not oversold
    if rsi > 72: lp = int(lp * 0.6)           # overbought → pangkas long
    if rsi < 28: sp = int(sp * 0.6)           # oversold → pangkas short

    # ═ G. Candle body ═════════════════
    if row["close"] > row["open"] and body > 0.6: lp += 6
    if row["close"] < row["open"] and body > 0.6: sp += 6

    # ═ H. BTC alignment — BOBOT BESAR ═
    if btc == "BULL":
        lp += 12
        sp  = max(0, sp - 20)   # Melawan BTC bull = -20 poin
    elif btc == "MILD_BULL":
        lp += 6
        sp  = max(0, sp - 10)
    elif btc == "BEAR":
        sp += 12
        lp  = max(0, lp - 20)
    elif btc == "MILD_BEAR":
        sp += 6
        lp  = max(0, lp - 10)
    elif btc in ("SIDEWAYS", "UNKNOWN"):
        # Sideways: naikkan threshold, tidak ada tambahan poin
        pass

    # ═ DECISION ═══════════════════════
    btc_sw = btc in ("SIDEWAYS", "UNKNOWN")
    thresh = 50 if btc_sw else MIN_SCORE  # lebih ketat saat BTC sideways
    gap    = abs(lp - sp)

    # Gap minimal 15 poin — supaya tidak entry saat ambiguous
    if lp > sp and lp >= thresh and gap >= 15:
        return "LONG", lp, sl[:3]
    if sp > lp and sp >= thresh and gap >= 15:
        return "SHORT", sp, ss[:3]
    return None, max(lp, sp), []

# ═══════════════════════════════════════════════════════
#  PAPER OPEN
# ═══════════════════════════════════════════════════════
def paper_open(sym, direction, score, sigs, price):
    with _lock:
        if sym in paper_positions or len(paper_positions) >= MAX_POSITIONS:
            return
        paper_positions[sym] = {"_r": True}

    q = qty(price)

    # Levels berdasarkan FIXED %
    if direction == "LONG":
        ic      = price * (1 - IC_PCT)
        hard_sl = price * (1 - HARD_LOSS_PCT)
        tp1     = price * (1 + TP1_PCT)
        tp2     = price * (1 + TP2_PCT)
        trail   = price * (1 - TRAIL_WIDTH)  # initial trail = dekat entry
    else:
        ic      = price * (1 + IC_PCT)
        hard_sl = price * (1 + HARD_LOSS_PCT)
        tp1     = price * (1 - TP1_PCT)
        tp2     = price * (1 - TP2_PCT)
        trail   = price * (1 + TRAIL_WIDTH)

    pos = {
        "side":      direction,
        "entry":     price,
        "qty":       q,
        "qty_rem":   q,
        "ic":        ic,
        "hard_sl":   hard_sl,   # ← BARU: hard PNL guard, selalu aktif
        "tp1":       tp1,
        "tp2":       tp2,
        "trail_sl":  trail,
        "peak":      price,
        "trail_on":  False,
        "tp1_hit":   False,
        "be_on":     False,
        "open_time": time.time(),
        "score":     score,
        "sigs":      sigs,
    }
    with _lock:
        paper_positions[sym] = pos

    d = "🟢" if direction == "LONG" else "🔴"
    print(f"\n  {d} [PAPER] {sym} {direction} @{price:.6g}")
    print(f"     IC:±{IC_PCT*100:.2f}% | Guard:±{HARD_LOSS_PCT*100:.2f}% | TP1:+{TP1_PCT*100:.2f}% | TP2:+{TP2_PCT*100:.2f}%")
    print(f"     Trail: ON sejak +{TRAIL_ACTIVATE*100:.2f}% profit | Score:{score} | {' | '.join(sigs)}")
    _stats["trades"] += 1

# ═══════════════════════════════════════════════════════
#  PAPER CLOSE
# ═══════════════════════════════════════════════════════
def paper_close(sym, reason, price=None):
    with _lock:
        pos = paper_positions.pop(sym, None)
    if pos is None or pos.get("_r"):
        return

    if price is None:
        price = price_live(sym)

    side  = pos["side"]
    entry = pos["entry"]
    qr    = pos.get("qty_rem", pos["qty"])
    pnl   = (price - entry) * qr if side == "LONG" else (entry - price) * qr
    pct   = (price - entry) / entry * 100 if side == "LONG" else (entry - price) / entry * 100
    hold  = time.time() - pos["open_time"]
    e     = "🟢" if pnl >= 0 else "🔴"

    print(f"  {e} [PAPER] {sym} {side} CLOSE — {reason}")
    print(f"     {entry:.6g}→{price:.6g} ({pct:+.3f}%) hold:{hold:.0f}s | PnL:{pnl:+.5f}U")

    _stats["pnl"] += pnl
    _stats["hist"].append(pnl)
    ks_upd(pnl)

    if pnl >= 0:
        _stats["wins"] += 1
        if pnl > _stats["best"]: _stats["best"] = pnl
    else:
        _stats["losses"] += 1
        if pnl < _stats["worst"]: _stats["worst"] = pnl

    r = reason
    if "TP1"   in r: _stats["tp1"]   += 1
    if "TP2"   in r: _stats["tp2"]   += 1
    if "SL"    in r: _stats["sl"]    += 1
    if "Cut"   in r: _stats["cut"]   += 1
    if "Guard" in r: _stats["guard"] += 1
    if "Force" in r: _stats["force"] += 1

    trade_log.append({
        "sym": sym, "side": side,
        "entry": round(entry, 7), "exit": round(price, 7),
        "pnl": round(pnl, 5), "reason": reason, "hold": int(hold),
    })
    set_cd(sym)
    _hot_syms.appendleft(sym)
    _rescan_q.put(1)
    print_inline()

def paper_tp1(sym, price):
    pos = paper_positions.get(sym)
    if pos is None or pos.get("tp1_hit") or pos.get("_r"):
        return

    side  = pos["side"]
    entry = pos["entry"]
    cq    = pos["qty"] * TP1_RATIO
    pnl   = (price - entry) * cq if side == "LONG" else (entry - price) * cq
    hold  = time.time() - pos["open_time"]

    print(f"  🎯 [PAPER] {sym} TP1 @{price:.6g} hold:{hold:.0f}s | PnL:{pnl:+.5f}U")

    pos["tp1_hit"] = True
    pos["qty_rem"] = pos["qty"] * (1 - TP1_RATIO)
    pos["be_on"]   = True
    # Setelah TP1: geser hard_sl ke break even, trail lebih ketat
    if side == "LONG":
        pos["hard_sl"]  = entry * 1.00005   # BE + tiny buffer
        pos["trail_sl"] = price * (1 - TRAIL_WIDTH * 0.7)
    else:
        pos["hard_sl"]  = entry * 0.99995
        pos["trail_sl"] = price * (1 + TRAIL_WIDTH * 0.7)
    pos["peak"]    = price
    pos["trail_on"] = True

    _stats["pnl"] += pnl
    _stats["hist"].append(pnl)
    _stats["wins"] += 1
    _stats["tp1"]  += 1
    ks_upd(pnl)
    if pnl > _stats["best"]: _stats["best"] = pnl
    trade_log.append({
        "sym": sym, "side": side,
        "entry": round(entry, 7), "exit": round(price, 7),
        "pnl": round(pnl, 5), "reason": "TP1", "hold": int(hold),
    })
    print_inline()

# ═══════════════════════════════════════════════════════
#  MONITOR — MULTI-LAYER PROTECTION
# ═══════════════════════════════════════════════════════
def monitor_positions():
    for sym in list(paper_positions.keys()):
        pos = paper_positions.get(sym)
        if pos is None or pos.get("_r"):
            continue

        px    = price_live(sym)
        if px == 0:
            continue

        side  = pos["side"]
        entry = pos["entry"]
        hold  = time.time() - pos["open_time"]

        # ─── FORCE CLOSE ───────────────────────────
        if hold >= MAX_HOLD_SEC:
            paper_close(sym, "Force", px); continue

        if side == "LONG":
            prof_pct = (px - entry) / entry

            # ─── LAYER 1: INSTANT CUT ──────────────
            # Harga turun melewati IC → potong LANGSUNG
            if px <= pos["ic"]:
                paper_close(sym, "LightningCut", px); continue

            # ─── LAYER 2: HARD PNL GUARD ───────────
            # Ini baru di v15c: bahkan kalau IC belum kena,
            # kalau PnL sudah -HARD_LOSS_PCT → potong
            if not pos["tp1_hit"] and px <= pos["hard_sl"]:
                paper_close(sym, "HardGuard", px); continue

            # ─── TP1 ────────────────────────────────
            if not pos["tp1_hit"] and px >= pos["tp1"]:
                paper_tp1(sym, px); continue

            # ─── TRAIL: aktif sejak profit TRAIL_ACTIVATE ──
            if prof_pct >= TRAIL_ACTIVATE and not pos["trail_on"]:
                pos["trail_on"] = True
                pos["peak"]     = px
                pos["trail_sl"] = px * (1 - TRAIL_WIDTH)
                # Setelah trail on: hard_sl ikut naik ke break even
                pos["hard_sl"]  = entry * 1.00005

            # Update trail ikut harga naik
            if pos["trail_on"] and px > pos["peak"]:
                pos["peak"]     = px
                new_t           = px * (1 - TRAIL_WIDTH)
                pos["trail_sl"] = max(pos["trail_sl"], new_t)
                # Hard sl juga naik (trailing BE)
                new_hard        = px * (1 - TRAIL_WIDTH * 1.5)
                pos["hard_sl"]  = max(pos["hard_sl"], new_hard)

            # ─── TRAIL STOP ─────────────────────────
            if pos["trail_on"] and px <= pos["trail_sl"]:
                tag = "TrailBE" if pos["be_on"] else "TrailStop"
                paper_close(sym, tag, px); continue

            # ─── TP2 ────────────────────────────────
            if pos["tp1_hit"] and px >= pos["tp2"]:
                paper_close(sym, "TP2", px); continue

            pnl_now = (px - entry) * pos.get("qty_rem", pos["qty"])
            tsl = f"T:{pos['trail_sl']:.5g}" if pos["trail_on"] else f"IC:{pos['ic']:.5g}"
            tp  = f"TP2:{pos['tp2']:.5g}" if pos["tp1_hit"] else f"TP1:{pos['tp1']:.5g}"
            print(f"  📌 {sym} L@{entry:.5g}→{px:.5g}({prof_pct*100:+.2f}%) {pnl_now:+.4f}U {hold:.0f}s {tsl} {tp}")

        else:  # SHORT
            prof_pct = (entry - px) / entry

            # ─── LAYER 1: INSTANT CUT ──────────────
            if px >= pos["ic"]:
                paper_close(sym, "LightningCut", px); continue

            # ─── LAYER 2: HARD PNL GUARD ───────────
            if not pos["tp1_hit"] and px >= pos["hard_sl"]:
                paper_close(sym, "HardGuard", px); continue

            # ─── TP1 ────────────────────────────────
            if not pos["tp1_hit"] and px <= pos["tp1"]:
                paper_tp1(sym, px); continue

            # ─── TRAIL ──────────────────────────────
            if prof_pct >= TRAIL_ACTIVATE and not pos["trail_on"]:
                pos["trail_on"] = True
                pos["peak"]     = px
                pos["trail_sl"] = px * (1 + TRAIL_WIDTH)
                pos["hard_sl"]  = entry * 0.99995

            if pos["trail_on"] and px < pos["peak"]:
                pos["peak"]     = px
                new_t           = px * (1 + TRAIL_WIDTH)
                pos["trail_sl"] = min(pos["trail_sl"], new_t)
                new_hard        = px * (1 + TRAIL_WIDTH * 1.5)
                pos["hard_sl"]  = min(pos["hard_sl"], new_hard)

            # ─── TRAIL STOP ─────────────────────────
            if pos["trail_on"] and px >= pos["trail_sl"]:
                tag = "TrailBE" if pos["be_on"] else "TrailStop"
                paper_close(sym, tag, px); continue

            # ─── TP2 ────────────────────────────────
            if pos["tp1_hit"] and px <= pos["tp2"]:
                paper_close(sym, "TP2", px); continue

            pnl_now = (entry - px) * pos.get("qty_rem", pos["qty"])
            tsl = f"T:{pos['trail_sl']:.5g}" if pos["trail_on"] else f"IC:{pos['ic']:.5g}"
            tp  = f"TP2:{pos['tp2']:.5g}" if pos["tp1_hit"] else f"TP1:{pos['tp1']:.5g}"
            print(f"  📌 {sym} S@{entry:.5g}→{px:.5g}({prof_pct*100:+.2f}%) {pnl_now:+.4f}U {hold:.0f}s {tsl} {tp}")

# ═══════════════════════════════════════════════════════
#  SCANNER
# ═══════════════════════════════════════════════════════
def scan_one(sym):
    try:
        time.sleep(SCAN_DELAY)
        if not ok_cooldown(sym): return None
        tk = _ticker_cache
        if sym in tk and tk[sym]["vol"] < 200_000: return None

        df = ohlcv(sym, Client.KLINE_INTERVAL_5MINUTE, 80)
        if df is None or len(df) < 35: return None
        df = run_ta(df.copy())

        px  = df["close"].iloc[-1]
        atr = df["atr"].iloc[-1]
        if px == 0 or atr / px > 0.02: return None  # skip terlalu volatile

        dir_, sc, sigs = signal(df)
        if dir_ is None or len(sigs) < 1: return None

        return (sym, dir_, sc, sigs, px)
    except:
        return None

def scan_batch(syms):
    res = []
    fut = {_executor.submit(scan_one, s): s for s in syms[:BATCH_SIZE]}
    try:
        for f in as_completed(fut, timeout=10):
            try:
                r = f.result(timeout=2)
                if r: res.append(r)
            except: pass
    except:
        for f in fut:
            if f.done():
                try:
                    r = f.result(timeout=0)
                    if r: res.append(r)
                except: pass
    return res

def top_movers(syms, n=50):
    tk  = tickers_all()
    ss  = set(syms)
    mv  = [(s, abs(d["pct"])) for s, d in tk.items()
           if s in ss and d["vol"] >= 200_000]
    mv.sort(key=lambda x: x[1], reverse=True)
    return [s for s, _ in mv[:n]]

# ═══════════════════════════════════════════════════════
#  STATS
# ═══════════════════════════════════════════════════════
def print_inline():
    n   = _stats["wins"] + _stats["losses"]
    wr  = _stats["wins"] / n * 100 if n else 0
    pnl = _stats["pnl"]
    e   = "💚" if pnl >= 0 else "🔴"
    print(f"     ┌ {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']} {e}PnL:{pnl:+.4f}U")
    print(f"     └ TP1:{_stats['tp1']} TP2:{_stats['tp2']} SL:{_stats['sl']} "
          f"Cut:{_stats['cut']} Guard:{_stats['guard']} Force:{_stats['force']}")

def print_full():
    n    = _stats["wins"] + _stats["losses"]
    wr   = _stats["wins"] / n * 100 if n else 0
    pnl  = _stats["pnl"]
    sess = (time.time() - _stats["start"]) / 3600
    tph  = n / sess if sess > 0 else 0
    e    = "💚" if pnl >= 0 else "🔴"

    pnls  = list(_stats["hist"])
    sh = md = 0.0
    if len(pnls) >= 5:
        a  = np.array(pnls)
        sd = float(np.std(a))
        sh = float(np.mean(a)) / sd if sd > 0 else 0.0
    if len(pnls) >= 2:
        eq = np.cumsum(pnls)
        md = float(np.min(eq - np.maximum.accumulate(eq)))

    max_loss_theory = ORDER_USDT * HARD_LOSS_PCT * LEVERAGE
    max_win_theory  = ORDER_USDT * TP1_PCT * LEVERAGE * TP1_RATIO

    print(f"\n  {'─'*62}")
    print(f"  🧪 PAPER v15c — {sess*60:.0f}m | {tph:.1f}T/jam | {len(SYMBOLS)}symbols")
    print(f"  🎯 {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']}")
    print(f"  {e} PnL:{pnl:+.5f}U Best:{_stats['best']:+.5f} Worst:{_stats['worst']:+.5f}")
    print(f"  📐 Sharpe:{sh:.2f} MaxDD:{md:.5f}U")
    print(f"  Theory max loss/trade: -{max_loss_theory:.4f}U | max profit TP1: +{max_win_theory:.4f}U")
    print(f"  TP1:{_stats['tp1']} TP2:{_stats['tp2']} SL:{_stats['sl']} "
          f"⚡Cut:{_stats['cut']} 🛡️Guard:{_stats['guard']} ⏰Force:{_stats['force']}")
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
            if paper_positions: monitor_positions()
        except Exception as e:
            print(f"  ❌ mon:{e}")
        time.sleep(MONITOR_INT)

def t_rescan(syms):
    while True:
        try:
            _rescan_q.get(timeout=30)
            time.sleep(0.2)
            slots = MAX_POSITIONS - len(paper_positions)
            if slots <= 0: continue
            if ks_check()[0]: continue
            hot  = [s for s in _hot_syms if s not in paper_positions]
            rest = [s for s in syms if s not in paper_positions and s not in hot]
            res  = scan_batch((hot + rest)[:35])
            if res:
                res.sort(key=lambda x: x[2], reverse=True)
                for sym, d, sc, sg, px in res[:slots]:
                    if len(paper_positions) >= MAX_POSITIONS: break
                    paper_open(sym, d, sc, sg, px)
        except queue.Empty: pass
        except Exception as e: print(f"  ❌ rescan:{e}")

def t_macro():
    while True:
        try:
            _macro["btc"] = btc_trend()
        except: pass
        try:
            if time.time() - _macro["last_fng"] > 300:
                d = requests.get("https://api.alternative.me/fng/?limit=1", timeout=5).json()["data"][0]
                _macro["fng"] = int(d["value"])
                _macro["last_fng"] = time.time()
        except: pass
        time.sleep(5)

# ═══════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════
def run_bot():
    print("╔═══════════════════════════════════════════════════════╗")
    print("║  🧪 PAPER TRADE v15c — TIGHT RISK FIXED               ║")
    print("║  ⚠️  SIMULASI — NO REAL ORDERS TO BINANCE              ║")
    print("╠═══════════════════════════════════════════════════════╣")
    print(f"║  ${ORDER_USDT}/pos ×{LEVERAGE}x | IC:±{IC_PCT*100:.2f}% | Guard:±{HARD_LOSS_PCT*100:.2f}% | TP1:+{TP1_PCT*100:.2f}%  ║")
    print(f"║  Max loss/trade: -{ORDER_USDT*HARD_LOSS_PCT*LEVERAGE:.4f}U (hard cap)          ║")
    print(f"║  Target profit TP1: +{ORDER_USDT*TP1_PCT*LEVERAGE*TP1_RATIO:.4f}U per trade           ║")
    print(f"║  {len(SYMBOLS)} symbols | monitor tiap {MONITOR_INT*1000:.0f}ms                     ║")
    print("╚═══════════════════════════════════════════════════════╝")

    try:
        valid = {s["symbol"] for s in client.futures_exchange_info()["symbols"]
                 if s["status"] == "TRADING"}
        syms = list(dict.fromkeys([s for s in SYMBOLS if s in valid]))
    except:
        syms = list(dict.fromkeys(SYMBOLS))
    print(f"\n  ✅ {len(syms)} symbols valid")

    threading.Thread(target=t_monitor, daemon=True).start()
    threading.Thread(target=t_rescan, args=(syms,), daemon=True).start()
    threading.Thread(target=t_macro, daemon=True).start()
    print("  🔧 Threads: monitor ✅ rescan ✅ macro ✅")

    print("  ⏳ Init 4s...")
    time.sleep(4)
    tickers_all()
    print(f"  📊 BTC:{_macro['btc']} F&G:{_macro['fng']}\n")

    cycle = scan_idx = 0
    n_bat = math.ceil(len(syms) / BATCH_SIZE)

    while True:
        cycle += 1
        slots = MAX_POSITIONS - len(paper_positions)

        print(f"\n{'═'*57}")
        print(f"  #{cycle} {time.strftime('%H:%M:%S')} BTC:{_macro['btc']} F&G:{_macro['fng']} "
              f"({len(paper_positions)}/{MAX_POSITIONS}) PnL:{_stats['pnl']:+.4f}U")

        if (k := ks_check())[0]:
            print(f"  🚨 KS:{k[1]}"); time.sleep(SCAN_INTERVAL); continue

        if slots > 0:
            mv  = top_movers(syms, 50)
            mv  = [s for s in mv if s not in paper_positions]
            bs  = scan_idx * BATCH_SIZE
            reg = [s for s in syms[bs:bs+BATCH_SIZE]
                   if s not in paper_positions and s not in mv]
            scan_idx = (scan_idx + 1) % n_bat
            scan_list = mv[:25] + reg[:15]

            print(f"  🔍 {len(scan_list)} syms | {slots} slot kosong")
            try: res = scan_batch(scan_list)
            except: res = []

            if res:
                res.sort(key=lambda x: x[2], reverse=True)
                print(f"  🎯 {len(res)} setup!")
                for sym, d, sc, sg, px in res[:slots]:
                    if len(paper_positions) >= MAX_POSITIONS: break
                    print(f"     ⭐ {sym} {d} Score:{sc} {' | '.join(sg)}")
                    paper_open(sym, d, sc, sg, px)
            elif len(paper_positions) == 0:
                print("  ⚠️  Wide scan...")
                try:
                    r2 = scan_batch([s for s in syms if s not in paper_positions][:50])
                except: r2 = []
                if r2:
                    r2.sort(key=lambda x: x[2], reverse=True)
                    sym, d, sc, sg, px = r2[0]
                    print(f"     ⭐ best: {sym} {d} Score:{sc}")
                    paper_open(sym, d, sc, sg, px)
                else:
                    print("  ⏳ Market flat — tunggu...")
            else:
                print(f"  ⏳ {len(paper_positions)} pos aktif")
        else:
            print(f"  ✅ Full ({MAX_POSITIONS}/{MAX_POSITIONS})")

        if cycle % 20 == 0:
            print_full()

        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    run_bot()
