"""
Bot Scalping v15.3 — SMART HOLD/CUT ENGINE
============================================

ROOT CAUSE dari masalah v15.2 (WR 20%, semua SHORT loss):
───────────────────────────────────────────────────────────
❌ MEAN_REV SHORT terlalu longgar — BTC MILD_BEAR + Breadth 30%
   sudah cukup untuk allow SHORT. Padahal MILD ≠ BEAR.
   → Fix: SHORT di MEAN_REV hanya kalau BTC 5m = "BEAR" (bukan MILD)
     DAN 15m BEAR DAN breadth < 25% DAN 1h BEAR. Semua harus agree.

❌ SmartCut terlalu agresif — cut di 3m45s (time_ratio > 0.4)
   Posisi baru buka belum sempat bergerak sudah di-cut.
   → Fix: SmartCut hanya kalau time_ratio > 0.65 DAN loss > 0.2%
     DAN sudah 60% jalan ke SL DAN semua 3 momentum sinyal melawan.

❌ _get_live_momentum hanya 2 sinyal (EMA + satu candle) — sangat
   noise. Satu candle bear bisa trigger -2 dan SmartCut.
   → Fix: 3 sinyal independen (EMA9>21, price vs EMA9, 3 candle trend).
     Score -2 hanya kalau KETIGA sinyal melawan.

❌ TrailStop kena di +0.00% (SUSHIUSDT) — trail aktif di 0.15%
   dengan ATR×0.7 langsung jadi TSL terlalu dekat dari harga.
   → Fix: Initial trail SL pakai ATR×1.05 (1.5× lebih lebar).
     Baru ketat saat harga bergerak lebih jauh.

❌ Kill switch 4 loss = 30 menit pause — terlalu sensitif di awal sesi.
   → Fix: kembali ke 5 loss, pause 20 menit (bukan 30).

❌ BEAR market detection pakai MILD_BEAR — terlalu broad.
   Breadth 30% + BTC MILD_BEAR bukan "bear market yang clear".
   → Fix: is_strong_bear = BTC 5m BEAR + 15m BEAR + 1h BEAR + breadth < 25%

PERUBAHAN TEKNIS v15.3
───────────────────────
✅ MEAN_REV: hanya allow SHORT kalau is_strong_bear (semua 4 kondisi)
✅ SmartCut: 5 syarat ketat (bukan 3), time_ratio 0.65 (bukan 0.40)
✅ Momentum check: 3-sinyal voting (butuh semua 3 untuk score ±2)
✅ Trail activation: initial width ATR×1.05 (bukan ATR×0.7)
✅ CONSEC_LOSS_MAX: 5 (kembali dari 4), pause 20m (bukan 30m)
✅ BTC alignment filter: hanya block kalau "BEAR" eksplisit, bukan "MILD_BEAR"
"""

import os, time, math, json, threading, queue
import requests
from collections import deque, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from binance.client import Client
from binance.enums import *
import ta
import pandas as pd
import numpy as np

load_dotenv()
client = Client(os.getenv("API_KEY"), os.getenv("API_SECRET"))

# ══════════════════════════════════════════════════════════
# MODE SELECTOR
# ══════════════════════════════════════════════════════════
# PAPER_TRADING = True  → tidak ada order ke exchange, semua simulasi
# PAPER_TRADING = False → order beneran (hati-hati!)
PAPER_TRADING = True

# Untuk koneksi data market tetap pakai testnet/real sesuai kebutuhan
client.FUTURES_URL = "https://testnet.binancefuture.com/fapi"

if PAPER_TRADING:
    print("=" * 60)
    print("  ⚠️  PAPER TRADING MODE AKTIF — TIDAK ADA ORDER BENERAN")
    print("  Semua trade hanya di log internal untuk backtest.")
    print("=" * 60)


# ════════════════════════════════════════════════════
#  CONFIG v15.5 — RIDE THE MOVE
# ════════════════════════════════════════════════════

# ── CORE ─────────────────────────────────────────────────
LEVERAGE              = 20
ORDER_USDT            = 1
MAX_POSITIONS         = 3

# ── ATR MULTIPLIER ───────────────────────────────────────
# MASALAH v15.x: TP1 jauh (ATR×1.8), SL dekat (ATR×1.0) → RR < 1:1
# FIX: TP1 lebih dekat (ATR×1.2 ≈ capture momentum cepat)
#      SL sedikit lebih lebar (ATR×1.3 ≈ beri ruang napas)
#      TP2 tetap ambisius tapi realistis (ATR×2.2)
ATR_SL_MULT           = 1.1          # SL kecil — cut cepat kalau salah
ATR_TP1_MULT          = 1.0          # TP1 = SL distance, capture cepat
ATR_TP2_MULT          = 2.0          # TP2 setelah TP1 hit
ATR_TRAIL_MULT        = 0.6          # Trail agresif
ATR_TRAIL_TIGHT_MULT  = 0.4          # Trail phase 3

MIN_SL_PCT            = 0.0010
MAX_SL_PCT            = 0.0055
MIN_TP1_PCT           = 0.0010       # Min TP1 0.10%
MAX_TP2_PCT           = 0.0200

# ── TRAIL — lebih agresif protect profit ─────────────────
TRAIL_ACTIVATE_PCT    = 0.0008       # Trail aktif di 0.08% profit
TRAIL_BE_PCT          = 0.0001
TRAIL_TIGHT_PCT       = 0.0020       # Phase 3 di 0.20%

# ── PARTIAL CLOSE ─────────────────────────────────────────
TP1_CLOSE_RATIO       = 0.65         # Tutup 65% di TP1
TP2_CLOSE_RATIO       = 0.35

# ── INSTANT CUT — lebih agresif untuk cut rugi awal ───────
INSTANT_CUT_MULT      = 0.50
INSTANT_CUT_WINDOW    = 5

# ── CHOP FILTER ──────────────────────────────────────────
CHOP_INDEX_THRESHOLD  = 56.0         # Lebih ketat dari 58
MIN_BB_WIDTH_PCT      = 0.005
MAX_EMA_CROSS_FREQ    = 3
MIN_ADX               = 18

# ── MOMENTUM FILTER ──────────────────────────────────────
MIN_MOMENTUM_PCT      = 0.0015
MIN_VOL_SURGE         = 1.4
MIN_TREND_CANDLES     = 3

# ── MULTI-TF ALIGNMENT ───────────────────────────────────
REQUIRE_MTF_ALIGN     = True
MTF_MIN_AGREE         = 2

# ── SMART COIN SELECTOR ──────────────────────────────────
PERF_WINDOW           = 10
MIN_PERF_WR           = 0.40
PERF_BOOST_WR         = 0.65
TEMP_BLACKLIST_SL     = 3
TEMP_BLACKLIST_MIN    = 25
PRIORITY_TP_STREAK    = 2

# ── CANDLE TIMING FILTER ─────────────────────────────────
MAX_CANDLE_AGE_PCT    = 0.85

# ── VOLATILITY SWEET SPOT ────────────────────────────────
MIN_ATR_PCT           = 0.0008
MAX_ATR_PCT           = 0.0080
SWEET_ATR_PCT         = 0.0025

# ── BEAR MARKET ──────────────────────────────────────────
BEAR_MARKET_BREADTH   = 0.30
BEAR_SHORT_SCORE_BONUS= 10
BEAR_MIN_SCORE_SHORT  = 44
BEAR_BLOCK_LONG       = True

# ── HOLDING ──────────────────────────────────────────────
MAX_HOLDING_MIN       = 7            # Max 7 menit — tapi jarang kena karena smart exit

HOLD_MULT_TP1_HIT     = 1.7          # TP1 hit → 12 menit kejar TP2
HOLD_MULT_PROFIT      = 1.20         # Profit & momentum bagus → 8.4 menit
HOLD_MULT_LOSS_BIG    = 0.65         # Rugi besar + negatif → 4.5 menit
HOLD_MULT_LOSS_SMALL  = 0.90         # Rugi kecil → 6.3 menit

LOSS_BIG_THRESHOLD    = -0.003       # Rugi > 0.3% = besar

# ── MOMENTUM EXIT (v15.5) ────────────────────────────────
# Keluar dari posisi profit saat momentum melambat/berbalik
MOM_EXIT_PROFIT_MIN   = 0.0006       # Minimal profit 0.06%
MOM_EXIT_TIME_MIN     = 0.35         # Sudah lewat minimal 35% waktu
MOM_EXIT_SCORE_MAX    = 0            # Momentum <= 0 (netral atau negatif)

# ── STAGNATION EXIT (v15.6 fix) ──────────────────────────
# ONDOUSDT bug: profit 0.06% stuck 5 menit tidak keluar karena
# STAG_PROFIT_MAX 0.02% terlalu ketat — harga lebih dari itu.
# Fix: stagnasi = harga TIDAK NAIK dalam window terakhir,
# bukan hanya kalau profit kecil.
# Pakai 2 kondisi:
# A) Profit kecil (< 0.05%) + sudah 2.5m + momentum netral → keluar
# B) Profit ada tapi tidak bertambah selama 3 menit + sudah 55% waktu → keluar
STAG_CHECK_MIN        = 2.5          # Cek mulai menit 2.5
STAG_PROFIT_MAX       = 0.0005       # Kondisi A: profit < 0.05%
STAG_MOM_MAX          = 0            # Dan momentum tidak positif
STAG_STUCK_MIN        = 3.0          # Kondisi B: stuck selama 3 menit
STAG_STUCK_TIME_RATIO = 0.55         # Dan sudah 55% dari max hold

# ── SCAN & TIMING ────────────────────────────────────────
SCAN_INTERVAL         = 3
POSITION_MONITOR_SEC  = 1
SCAN_DELAY_MS         = 0.050
BATCH_SIZE            = 15
SYMBOL_COOLDOWN_SEC   = 10
RE_SCAN_DELAY_SEC     = 0.3

# ── SESSION FILTER ────────────────────────────────────────
BAD_HOURS_UTC         = {4, 5, 6}
BAD_HOURS_MIN_SCORE   = 62

# ── KILL SWITCH ───────────────────────────────────────────
DAILY_LOSS_LIMIT      = -3.0
CONSEC_LOSS_MAX       = 5
CONSEC_LOSS_PAUSE_MIN = 20
MAX_API_LAG_SEC       = 3.0

# ── CACHE TTL ─────────────────────────────────────────────
OHLCV_CACHE_TTL_1M    = 2
OHLCV_CACHE_TTL_3M    = 4
OHLCV_CACHE_TTL_5M    = 5
OHLCV_CACHE_TTL_15M   = 30
OHLCV_CACHE_TTL_1H    = 1800
TICKER24H_TTL         = 8
FUNDING_TTL           = 30
TOP_MOVERS_TTL        = 8

# ── FILTER UTAMA ──────────────────────────────────────────
MIN_SCORE             = 48       # v15.6: turun sedikit untuk frekuensi lebih baik
MIN_ENTRY_SIGNALS     = 2        # Tetap 2 — sudah cukup ketat dari scoring baru
MIN_FNG               = 15
MAX_FNG_LONG          = 92
MIN_BREADTH           = 0.0
MAX_SL_ATR_PCT        = 0.009

# Hard cap dollar loss per trade — worst case tidak boleh lebih dari ini
# Dengan $1 trade × 20x leverage, jika SL 0.5% → loss = $1 × 0.5% × 20 = $0.10
# Set cap di $0.12 = SL max efektif 0.6% dengan leverage 20x
MAX_LOSS_PER_TRADE_USD = 0.12

# ── SPREAD ────────────────────────────────────────────────
MAX_SPREAD_RATIO      = 0.28

# ── SYMBOLS ───────────────────────────────────────────────
SYMBOLS = [
    "BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT",
    "ADAUSDT","DOGEUSDT","AVAXUSDT","LINKUSDT","DOTUSDT",
    "MATICUSDT","LTCUSDT","ATOMUSDT","UNIUSDT","ETCUSDT",
    "NEARUSDT","APTUSDT","ARBUSDT","OPUSDT","INJUSDT",
    "SUIUSDT","TIAUSDT","AAVEUSDT","RUNEUSDT","FILUSDT",
    "1000PEPEUSDT","WIFUSDT","JUPUSDT","SEIUSDT","PYTHUSDT",
    "FETUSDT","RENDERUSDT","WLDUSDT","STRKUSDT","ALTUSDT",
    "DYMUSDT","PIXELUSDT","ACEUSDT","MANTAUSDT","ZETAUSDT",
    "RONINUSDT","NOTUSDT","DOGSUSDT","EIGENUSDT","CATIUSDT",
    "1000BONKUSDT","PORTALUSDT",
    "CRVUSDT","MKRUSDT","COMPUSDT","SUSHIUSDT",
    "SNXUSDT","1INCHUSDT","BALUSDT","DYDXUSDT",
    "GMXUSDT","PENDLEUSDT","JTOUSDT","RAYUSDT",
    "ALGOUSDT","ICPUSDT","FTMUSDT","HBARUSDT","FLOWUSDT",
    "EGLDUSDT","THETAUSDT","KAVAUSDT","BANDUSDT",
    "SKLUSDT","CELRUSDT","CTSIUSDT",
    "AXSUSDT","SANDUSDT","MANAUSDT","ENJUSDT","GALAUSDT",
    "IMXUSDT","BLURUSDT","MASKUSDT","HIGHUSDT",
    "BEAMXUSDT","MEMEUSDT","ORDIUSDT",
    "ARUSDT","OCEANUSDT","TRUUSDT","POLYXUSDT","BLZUSDT",
    "SHIBUSDT","FLOKIUSDT","BONKUSDT","JASMYUSDT",
    "LUNCUSDT","CFXUSDT","COMBOUSDT","AGLDUSDT","IDUSDT","GASUSDT",
    "STXUSDT","KASUSDT","TONUSDT","TAOUSDT","ONDOUSDT",
    "ENARUSDT","WUSDT","BOMEUSDT","SAFEUSDT",
    "VANRYUSDT","XAIUSDT","ATAUSDT",
    "MOVRUSDT","CKBUSDT","NMRUSDT","HOOKUSDT",
    "GLMRUSDT","AMBUSDT","RENUSDT","CVCUSDT","VOXELUSDT",
    "PERPUSDT","LITUSDT","UNFIUSDT","DENTUSDT",
    "HOTUSDT","IOSTUSDT","OGNUSDT","LINAUSDT","SFPUSDT",
    "1000XECUSDT","BNTUSDT","FLMSUSDT","TLMUSDT",
]


# ════════════════════════════════════════════════════
#  STATE GLOBAL
# ════════════════════════════════════════════════════
open_positions      = {}
trade_log           = []
_ohlcv_cache        = {}
_sym_info           = {}
_sym_cooldown       = {}
_btc_price_history  = deque(maxlen=300)
_scan_batch_idx     = 0
_lock               = threading.Lock()
_executor           = ThreadPoolExecutor(max_workers=15)
_rescan_queue       = queue.Queue()
_hot_symbols        = deque(maxlen=30)
_ticker24h_cache    = {}
_ticker24h_ts       = 0
_funding_cache      = {}
_funding_ts         = 0
_top_movers         = []
_top_movers_ts      = 0

# ── Smart Coin Selector state ─────────────────────────────
_coin_perf = defaultdict(lambda: {
    "trades": deque(maxlen=PERF_WINDOW),   # deque berisi True/False (win/loss)
    "sl_streak": 0,                         # SL beruntun saat ini
    "tp_streak": 0,                         # TP beruntun saat ini
    "blacklist_until": 0,                   # timestamp kapan blacklist berakhir
    "priority_until": 0,                    # timestamp priority aktif
    "total_pnl": 0.0,
    "last_trade_ts": 0,
})

# ── Paper trading ledger ──────────────────────────────────
_paper_balance = {
    "initial_usdt": 1000.0,    # Modal virtual
    "equity": 1000.0,
    "pnl_total": 0.0,
}

# ── Kill switch ───────────────────────────────────────────
_kill_switch = {
    "active":           False,
    "reason":           "",
    "resume_time":      0,
    "consec_losses":    0,
    "daily_pnl":        0.0,
    "daily_reset_ts":   0,
    "last_api_check":   0,
    "api_lag":          0.0,
}

_perf         = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0, "trades": 0})
_perf_regime  = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0})

_macro = {
    "fng": 50, "fng_label": "Neutral",
    "btc_trend_1m":  "UNKNOWN",
    "btc_trend_5m":  "UNKNOWN",
    "btc_trend_15m": "UNKNOWN",
    "btc_trend_1h":  "UNKNOWN",
    "market_breadth": 0.5,
    "news": "neutral",
    "scalp_mode": "TREND",
    "last_fng": 0, "last_btc": 0, "last_breadth": 0, "last_news": 0,
}

_stats = {
    "total_trades": 0,
    "wins": 0,
    "losses": 0,
    "total_pnl": 0.0,
    "best_trade": 0.0,
    "worst_trade": 0.0,
    "tp1_hits": 0,
    "tp2_hits": 0,
    "sl_hits": 0,
    "instant_cuts": 0,
    "force_closes": 0,
    "smart_cuts":   0,
    "mom_exits":    0,
    "stag_exits":   0,
    "rescans": 0,
    "skipped_no_momentum": 0,
    "skipped_chop": 0,
    "skipped_spread": 0,
    "skipped_session": 0,
    "skipped_mean_rev": 0,
    "skipped_candle_age": 0,
    "skipped_blacklist": 0,
    "skipped_mtf": 0,
    "skipped_mq": 0,
    "pnl_history": deque(maxlen=200),
    "session_start": time.time(),
}

BULL_TRENDS = {"BULL", "MILD_BULL"}
BEAR_TRENDS = {"BEAR", "MILD_BEAR"}


# ════════════════════════════════════════════════════
#  PAPER TRADING ENGINE
# ════════════════════════════════════════════════════
def paper_create_order(symbol, side, order_type, quantity, reduce_only=False):
    """
    Simulasi order — tidak kirim ke exchange sama sekali.
    Return struktur mirip response Binance agar kode tidak perlu diubah.
    """
    price = get_price(symbol)
    mode  = "REDUCE" if reduce_only else "OPEN"
    dir_  = "BUY" if side == SIDE_BUY else "SELL"
    print(f"     📝 [PAPER] {mode} {dir_} {quantity} {symbol} @{price:.5g}")
    return {
        "orderId":       int(time.time() * 1000),
        "symbol":        symbol,
        "side":          side,
        "type":          order_type,
        "origQty":       str(quantity),
        "executedQty":   str(quantity),
        "avgPrice":      str(price),
        "status":        "FILLED",
        "paper":         True,
    }


def paper_get_position_amt(symbol):
    """
    Ambil posisi dari state internal open_positions (bukan dari exchange).
    """
    pos = open_positions.get(symbol)
    if pos is None or pos.get("_reserved"):
        return 0.0
    qty = pos.get("qty_remain", pos.get("qty", 0))
    if pos["side"] == "LONG":
        return float(qty)
    else:
        return -float(qty)


def paper_set_leverage(symbol):
    """Leverage disimpan di config saja, tidak perlu API call."""
    pass


# ════════════════════════════════════════════════════
#  SMART COIN SELECTOR
# ════════════════════════════════════════════════════
def is_coin_blacklisted(symbol):
    """Return True jika coin sedang di-blacklist karena SL beruntun."""
    cp = _coin_perf[symbol]
    if time.time() < cp["blacklist_until"]:
        return True
    return False


def is_coin_priority(symbol):
    """Return True jika coin sedang di-priority karena TP beruntun."""
    cp = _coin_perf[symbol]
    if time.time() < cp["priority_until"]:
        return True
    return False


def get_coin_perf_score(symbol):
    """
    Hitung skor performance coin berdasarkan riwayat trade di sesi ini.
    Return float antara -20 (jelek) sampai +20 (bagus).
    """
    cp = _coin_perf[symbol]
    trades = list(cp["trades"])
    if len(trades) < 2:
        return 0.0  # Belum cukup data

    wins   = sum(1 for t in trades if t)
    total  = len(trades)
    wr     = wins / total

    if wr >= PERF_BOOST_WR:
        base = 15.0 + (wr - PERF_BOOST_WR) * 50   # Bonus untuk high WR
    elif wr < MIN_PERF_WR:
        base = -10.0 - (MIN_PERF_WR - wr) * 30    # Penalti untuk low WR
    else:
        base = (wr - 0.5) * 30                      # Linear di tengah

    # Bonus untuk streak
    base += cp["tp_streak"] * 3.0
    base -= cp["sl_streak"] * 2.0

    return round(max(-20.0, min(20.0, base)), 2)


def update_coin_perf(symbol, is_win, pnl):
    """Update riwayat performance coin setelah trade selesai."""
    cp = _coin_perf[symbol]
    cp["trades"].append(is_win)
    cp["total_pnl"] += pnl
    cp["last_trade_ts"] = time.time()

    if is_win:
        cp["sl_streak"] = 0
        cp["tp_streak"] += 1
        if cp["tp_streak"] >= PRIORITY_TP_STREAK:
            cp["priority_until"] = time.time() + 1800  # 30 menit priority
            print(f"     ⭐ [{symbol}] Masuk PRIORITY LIST ({cp['tp_streak']} TP streak)")
    else:
        cp["tp_streak"] = 0
        cp["sl_streak"] += 1
        if cp["sl_streak"] >= TEMP_BLACKLIST_SL:
            cp["blacklist_until"] = time.time() + (TEMP_BLACKLIST_MIN * 60)
            print(f"     🚫 [{symbol}] Masuk BLACKLIST {TEMP_BLACKLIST_MIN}m ({cp['sl_streak']} SL streak)")


def calc_adx(df, period=14):
    """Hitung ADX untuk ukur kekuatan trend."""
    try:
        adx_ind = ta.trend.ADXIndicator(df["high"], df["low"], df["close"], period)
        adx_val = adx_ind.adx().iloc[-1]
        return float(adx_val) if not math.isnan(adx_val) else 0.0
    except:
        return 0.0


def get_candle_age_pct(symbol, interval_sec=300):
    """
    Hitung berapa persen candle 5m sudah berlalu.
    Return 0.0 (candle baru) sampai 1.0 (candle hampir habis).
    """
    try:
        now_ms = int(time.time() * 1000)
        candle_start_ms = (now_ms // (interval_sec * 1000)) * (interval_sec * 1000)
        elapsed = (now_ms - candle_start_ms) / 1000
        return elapsed / interval_sec
    except:
        return 0.5


def get_mtf_alignment(symbol, direction):
    """
    Cek alignment trend di 1m, 5m, 15m.
    Return (agree_count, total, detail_string)
    """
    try:
        results = []

        # 1m trend
        df_1m = get_ohlcv(symbol, Client.KLINE_INTERVAL_1MINUTE, 30)
        if df_1m is not None and len(df_1m) >= 20:
            df_1m = run_ta_lite(df_1m.copy())
            trend_1m = _calc_trend(df_1m)
            if direction == "LONG":
                results.append(("1m", trend_1m in BULL_TRENDS, trend_1m))
            else:
                results.append(("1m", trend_1m in BEAR_TRENDS, trend_1m))

        # 5m trend (sudah diambil sebelumnya, bisa dilewat)
        # 15m trend
        df_15m = get_ohlcv(symbol, Client.KLINE_INTERVAL_15MINUTE, 40)
        if df_15m is not None and len(df_15m) >= 25:
            df_15m = run_ta_lite(df_15m.copy())
            trend_15m = _calc_trend(df_15m)
            if direction == "LONG":
                results.append(("15m", trend_15m in BULL_TRENDS, trend_15m))
            else:
                results.append(("15m", trend_15m in BEAR_TRENDS, trend_15m))

        agree  = sum(1 for _, ok, _ in results if ok)
        detail = " ".join(f"{tf}:{t}" for tf, _, t in results)
        return agree, len(results), detail

    except Exception as e:
        return 0, 0, f"err:{e}"


def run_ta_lite(df):
    """TA minimal untuk MTF check (lebih cepat dari run_ta penuh)."""
    c = df["close"]
    df["ema9"]  = ta.trend.EMAIndicator(c, 9).ema_indicator()
    df["ema21"] = ta.trend.EMAIndicator(c, 21).ema_indicator()
    df["ema50"] = ta.trend.EMAIndicator(c, 50).ema_indicator()
    return df


def calc_coin_priority_score(symbol, base_score, direction, df_5m):
    """
    Hitung total priority score untuk smart coin ranking.
    Digunakan untuk sort kandidat sebelum eksekusi.

    Components:
    - base_score: dari get_entry_score (0-100)
    - perf_bonus: dari riwayat WR coin ini (-20 to +20)
    - adx_bonus: trend strength (0 to +10)
    - vol_quality: kualitas volume (0 to +8)
    - volatility_fit: seberapa pas ATR-nya (0 to +5)
    - priority_flag: apakah sedang di priority list (+10)
    """
    total = float(base_score)

    # 1. Performance history bonus
    perf_bonus = get_coin_perf_score(symbol)
    total += perf_bonus

    # 2. ADX bonus — trend lebih kuat = lebih baik
    if df_5m is not None and len(df_5m) >= 20:
        adx = calc_adx(df_5m, 14)
        if adx >= 30:
            total += 8.0
        elif adx >= 22:
            total += 5.0
        elif adx >= MIN_ADX:
            total += 2.0

    # 3. Volume quality — buy/sell ratio harus jelas
    if df_5m is not None and len(df_5m) > 0:
        last = df_5m.iloc[-1]
        br   = last.get("buy_ratio", 0.5)
        if direction == "LONG" and br > 0.62:
            total += 6.0
        elif direction == "LONG" and br > 0.55:
            total += 3.0
        elif direction == "SHORT" and br < 0.38:
            total += 6.0
        elif direction == "SHORT" and br < 0.45:
            total += 3.0

    # 4. Volatility sweet spot
    if df_5m is not None and len(df_5m) > 0:
        last = df_5m.iloc[-1]
        price = last["close"]
        atr   = last.get("atr", 0)
        atr_p = atr / price if price > 0 else 0
        # Ideal range: 0.002 - 0.005
        if SWEET_ATR_PCT * 0.6 <= atr_p <= SWEET_ATR_PCT * 1.6:
            total += 4.0
        elif atr_p < MIN_ATR_PCT or atr_p > MAX_ATR_PCT:
            total -= 5.0  # Penalti: terlalu flat atau terlalu volatile

    # 5. Priority flag
    if is_coin_priority(symbol):
        total += 10.0

    return round(total, 2)


# ════════════════════════════════════════════════════
#  KILL SWITCH ENGINE
# ════════════════════════════════════════════════════
def check_kill_switch():
    ks  = _kill_switch
    now = time.time()

    # Resume setelah pause
    if ks["active"] and now >= ks["resume_time"]:
        ks["active"]        = False
        ks["reason"]        = ""
        ks["consec_losses"] = 0   # reset counter SETELAH pause selesai
        print(f"\n  ✅ Kill switch CLEARED — bot aktif kembali")

    if ks["active"]:
        return True, ks["reason"]

    # Daily reset
    day_start = now - (now % 86400)
    if day_start > ks["daily_reset_ts"]:
        ks["daily_pnl"]      = 0.0
        ks["daily_reset_ts"] = day_start
        ks["consec_losses"]  = 0

    if ks["daily_pnl"] <= DAILY_LOSS_LIMIT:
        ks["active"]      = True
        ks["reason"]      = f"daily_loss({ks['daily_pnl']:.2f}U)"
        ks["resume_time"] = day_start + 86400
        print(f"\n  🚨 KILL SWITCH: daily loss limit ({ks['daily_pnl']:.2f}U)")
        return True, ks["reason"]

    if ks["consec_losses"] >= CONSEC_LOSS_MAX:
        ks["active"]      = True
        ks["reason"]      = f"consec_loss({ks['consec_losses']})"
        ks["resume_time"] = now + (CONSEC_LOSS_PAUSE_MIN * 60)
        # Reset counter SEKARANG supaya setelah resume tidak langsung trigger lagi
        # Counter akan mulai dari 0 setelah bot aktif kembali
        ks["consec_losses"] = 0
        print(f"\n  🚨 KILL SWITCH: consec loss — pause {CONSEC_LOSS_PAUSE_MIN}m")
        return True, ks["reason"]

    return False, ""


def update_kill_switch_after_trade(pnl):
    ks = _kill_switch
    ks["daily_pnl"] += pnl
    if pnl < 0:
        ks["consec_losses"] += 1
    elif pnl > 0.005:
        ks["consec_losses"] = 0
    # pnl 0–0.005: tidak ubah counter (TP1 partial tidak reset streak)


def check_api_latency():
    if PAPER_TRADING:
        _kill_switch["api_lag"] = 0.001
        return True
    try:
        t0 = time.time()
        client.futures_ping()
        lag = time.time() - t0
        _kill_switch["api_lag"] = lag
        if lag > MAX_API_LAG_SEC:
            print(f"  ⚠️ API lag tinggi: {lag:.2f}s — skip entry")
            return False
        return True
    except:
        return False


# ════════════════════════════════════════════════════
#  CHOP / REGIME FILTER
# ════════════════════════════════════════════════════
def calc_choppiness_index(df, period=14):
    if df is None or len(df) < period + 2:
        return 50.0
    try:
        high  = df["high"].values
        low   = df["low"].values
        close = df["close"].values
        tr_sum = 0.0
        for i in range(-period, 0):
            tr = max(
                high[i] - low[i],
                abs(high[i] - close[i - 1]),
                abs(low[i]  - close[i - 1])
            )
            tr_sum += tr
        highest_high = max(high[-period:])
        lowest_low   = min(low[-period:])
        price_range  = highest_high - lowest_low
        if price_range == 0 or tr_sum == 0:
            return 50.0
        ci = 100 * math.log10(tr_sum / price_range) / math.log10(period)
        return round(ci, 2)
    except:
        return 50.0


def calc_ema_cross_frequency(df, period=20):
    if df is None or len(df) < period + 10:
        return 0
    try:
        e3 = df["ema3"].values[-period:]
        e9 = df["ema9"].values[-period:]
        cross_count = 0
        for i in range(1, len(e3)):
            if (e3[i-1] > e9[i-1] and e3[i] <= e9[i]) or \
               (e3[i-1] < e9[i-1] and e3[i] >= e9[i]):
                cross_count += 1
        return cross_count
    except:
        return 0


def is_chop_market(df_5m, direction):
    if df_5m is None or len(df_5m) < 20:
        return False, "no_data"
    reasons = []
    ci = calc_choppiness_index(df_5m, 14)
    if ci > CHOP_INDEX_THRESHOLD:
        reasons.append(f"CI={ci:.1f}")
    last = df_5m.iloc[-1]
    bb_width = last.get("bb_width", 0.01)
    if bb_width < MIN_BB_WIDTH_PCT:
        reasons.append(f"BB_narrow({bb_width*100:.2f}%)")
    cross_freq = calc_ema_cross_frequency(df_5m, 20)
    if cross_freq > MAX_EMA_CROSS_FREQ:
        reasons.append(f"EMA_x{cross_freq}")
    recent_hist = df_5m["macd_hist"].values[-10:]
    hist_std = float(np.std(recent_hist)) if len(recent_hist) >= 5 else 0
    if hist_std < 0.00001:
        reasons.append(f"MACD_flat")
    is_chop = len(reasons) >= 2
    return is_chop, "|".join(reasons) if reasons else "ok"


# ════════════════════════════════════════════════════
#  SPREAD FILTER
# ════════════════════════════════════════════════════
def get_spread_ratio(symbol, tp1_price, entry_price):
    if PAPER_TRADING:
        # Di paper mode, asumsikan spread OK untuk semua coin liquid
        return 0.10
    try:
        ob = client.futures_order_book(symbol=symbol, limit=5)
        best_bid = float(ob["bids"][0][0])
        best_ask = float(ob["asks"][0][0])
        spread = best_ask - best_bid
        tp1_dist = abs(tp1_price - entry_price)
        if tp1_dist == 0:
            return 1.0
        ratio = spread / tp1_dist
        return round(ratio, 3)
    except:
        return 0.0


# ════════════════════════════════════════════════════
#  SESSION FILTER
# ════════════════════════════════════════════════════
def get_session_min_score():
    utc_hour = time.gmtime().tm_hour
    if utc_hour in BAD_HOURS_UTC:
        return BAD_HOURS_MIN_SCORE
    return MIN_SCORE


# ════════════════════════════════════════════════════
#  UTILS
# ════════════════════════════════════════════════════
def get_sym_info(symbol):
    if symbol in _sym_info: return _sym_info[symbol]
    try:
        for s in client.futures_exchange_info()["symbols"]:
            if s["symbol"] == symbol:
                for f in s["filters"]:
                    if f["filterType"] == "LOT_SIZE":
                        _sym_info[symbol] = {
                            "step": float(f["stepSize"]),
                            "minQty": float(f["minQty"])
                        }
                        return _sym_info[symbol]
    except: pass
    return {"step": 1.0, "minQty": 1.0}

def round_step(qty, step):
    p = max(0, int(round(-math.log(step, 10), 0))) if step < 1 else 0
    return round(math.floor(qty / step) * step, p)

def calc_qty(symbol, price):
    info = get_sym_info(symbol)
    raw  = (ORDER_USDT * LEVERAGE) / price
    return max(round_step(raw, info["step"]), info["minQty"])

def set_leverage(symbol):
    if PAPER_TRADING:
        paper_set_leverage(symbol)
        return
    try: client.futures_change_leverage(symbol=symbol, leverage=LEVERAGE)
    except: pass

def get_price(symbol):
    try: return float(client.futures_symbol_ticker(symbol=symbol)["price"])
    except: return 0.0

def get_exchange_amt(symbol):
    if PAPER_TRADING:
        return paper_get_position_amt(symbol)
    try:
        for p in client.futures_position_information(symbol=symbol):
            amt = float(p["positionAmt"])
            if amt != 0: return amt
        return 0
    except: return None

def is_symbol_cooling_down(symbol):
    if symbol not in _sym_cooldown: return False
    return (time.time() - _sym_cooldown[symbol]) < SYMBOL_COOLDOWN_SEC

def set_symbol_cooldown(symbol):
    _sym_cooldown[symbol] = time.time()

def validate_symbols():
    try:
        valid = {s["symbol"] for s in client.futures_exchange_info()["symbols"]
                 if s["status"] == "TRADING"}
        result = list(dict.fromkeys([s for s in SYMBOLS if s in valid]))
        print(f"  ✅ {len(result)}/{len(SYMBOLS)} symbols valid")
        return result
    except:
        return list(dict.fromkeys(SYMBOLS))


# ════════════════════════════════════════════════════
#  SUMBER DATA
# ════════════════════════════════════════════════════
def fetch_ticker24h_all():
    global _ticker24h_cache, _ticker24h_ts
    now = time.time()
    if now - _ticker24h_ts < TICKER24H_TTL and _ticker24h_cache:
        return _ticker24h_cache
    try:
        tickers = client.futures_ticker()
        new_cache = {}
        for t in tickers:
            sym = t["symbol"]
            new_cache[sym] = {
                "pct":    float(t["priceChangePercent"]),
                "price":  float(t["lastPrice"]),
                "vol24h": float(t["quoteVolume"]),
                "high24": float(t["highPrice"]),
                "low24":  float(t["lowPrice"]),
                "count":  int(t["count"]),
            }
        _ticker24h_cache = new_cache
        _ticker24h_ts    = now
        return new_cache
    except:
        return _ticker24h_cache


def fetch_funding_rates():
    global _funding_cache, _funding_ts
    now = time.time()
    if now - _funding_ts < FUNDING_TTL and _funding_cache:
        return _funding_cache
    try:
        premium = client.futures_mark_price()
        new_cache = {}
        for p in premium:
            sym = p["symbol"]
            fr  = float(p.get("lastFundingRate", 0))
            new_cache[sym] = fr
        _funding_cache = new_cache
        _funding_ts    = now
        return new_cache
    except:
        return _funding_cache


def get_top_movers(symbols_active, n=30):
    global _top_movers, _top_movers_ts
    now = time.time()
    if now - _top_movers_ts < TOP_MOVERS_TTL and _top_movers:
        return _top_movers
    try:
        tickers    = fetch_ticker24h_all()
        active_set = set(symbols_active)
        movers     = []
        for sym, data in tickers.items():
            if sym not in active_set: continue
            pct = data["pct"]
            vol = data["vol24h"]
            if vol < 1_000_000: continue
            movers.append((sym, pct, vol))
        movers.sort(key=lambda x: abs(x[1]), reverse=True)
        result = []
        for sym, pct, vol in movers[:n]:
            direction = "LONG" if pct > 0 else "SHORT"
            result.append((sym, pct, direction))
        _top_movers    = result
        _top_movers_ts = now
        return result
    except:
        return _top_movers


def get_funding_bias(symbol):
    rates = fetch_funding_rates()
    fr = rates.get(symbol, 0)
    if fr > 0.0005:  return "bearish_bias", fr
    if fr < -0.0005: return "bullish_bias", fr
    return "neutral", fr


# ════════════════════════════════════════════════════
#  OHLCV CACHE
# ════════════════════════════════════════════════════
def get_ohlcv(symbol, interval, limit=100):
    cache_key = (symbol, interval)
    now = time.time()
    ttl_map = {
        Client.KLINE_INTERVAL_1MINUTE:  OHLCV_CACHE_TTL_1M,
        Client.KLINE_INTERVAL_3MINUTE:  OHLCV_CACHE_TTL_3M,
        Client.KLINE_INTERVAL_5MINUTE:  OHLCV_CACHE_TTL_5M,
        Client.KLINE_INTERVAL_15MINUTE: OHLCV_CACHE_TTL_15M,
        Client.KLINE_INTERVAL_1HOUR:    OHLCV_CACHE_TTL_1H,
    }
    ttl = ttl_map.get(interval, 30)
    if cache_key in _ohlcv_cache:
        ts, df_cached = _ohlcv_cache[cache_key]
        if now - ts < ttl:
            return df_cached
    try:
        klines = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
        df = pd.DataFrame(klines, columns=[
            "time","open","high","low","close","volume",
            "ct","qv","trades","tbbase","tbquote","ignore"])
        for c in ["open","high","low","close","volume","tbbase","tbquote"]:
            df[c] = df[c].astype(float)
        df["time"] = pd.to_numeric(df["time"])
        _ohlcv_cache[cache_key] = (now, df)
        return df
    except:
        if cache_key in _ohlcv_cache:
            return _ohlcv_cache[cache_key][1]
        return None


# ════════════════════════════════════════════════════
#  TECHNICAL ANALYSIS
# ════════════════════════════════════════════════════
def run_ta(df):
    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]
    df["rsi"]       = ta.momentum.RSIIndicator(c, 14).rsi()
    df["rsi_fast"]  = ta.momentum.RSIIndicator(c, 7).rsi()
    macd            = ta.trend.MACD(c, 12, 26, 9)
    df["macd"]      = macd.macd()
    df["macd_sig"]  = macd.macd_signal()
    df["macd_hist"] = macd.macd_diff()
    df["ema3"]      = ta.trend.EMAIndicator(c, 3).ema_indicator()
    df["ema5"]      = ta.trend.EMAIndicator(c, 5).ema_indicator()
    df["ema9"]      = ta.trend.EMAIndicator(c, 9).ema_indicator()
    df["ema21"]     = ta.trend.EMAIndicator(c, 21).ema_indicator()
    df["ema50"]     = ta.trend.EMAIndicator(c, 50).ema_indicator()
    bb              = ta.volatility.BollingerBands(c, 20, 2)
    df["bb_hi"]     = bb.bollinger_hband()
    df["bb_lo"]     = bb.bollinger_lband()
    df["bb_mid"]    = bb.bollinger_mavg()
    df["bb_width"]  = (df["bb_hi"] - df["bb_lo"]) / df["bb_mid"]
    stoch           = ta.momentum.StochasticOscillator(h, l, c, 14, 3)
    df["stk"]       = stoch.stoch()
    df["std"]       = stoch.stoch_signal()
    df["atr"]       = ta.volatility.AverageTrueRange(h, l, c, 14).average_true_range()
    df["vol_ma"]    = v.rolling(20).mean()
    df["vol_ratio"] = v / df["vol_ma"].replace(0, 1)
    df["buy_ratio"] = df["tbbase"] / df["volume"].replace(0, 1)
    df["body"]      = abs(df["close"] - df["open"])
    df["range_"]    = df["high"] - df["low"]
    df["body_ratio"]= df["body"] / df["range_"].replace(0, 1)
    df["bb_squeeze"]= df["bb_width"] < df["bb_width"].rolling(20).mean() * 0.85
    df["mom5"]      = (c - c.shift(5)) / c.shift(5)
    df["mom3"]      = (c - c.shift(3)) / c.shift(3)
    return df


def _calc_trend(df):
    """
    v15.6: Lebih konservatif — hanya BULL/BEAR kalau semua kondisi terpenuhi.
    MILD hanya kalau cukup kuat, tidak sembarangan.
    Ini penting karena TREND WR 52% PnL -2.54U menunjukkan
    banyak entry di kondisi yang salah dikira TREND.
    """
    if df is None or len(df) < 25: return "UNKNOWN"
    c     = df["close"]
    price = c.iloc[-1]
    ema9  = ta.trend.EMAIndicator(c, 9).ema_indicator().iloc[-1]
    ema21 = ta.trend.EMAIndicator(c, 21).ema_indicator().iloc[-1]
    ema50 = ta.trend.EMAIndicator(c, 50).ema_indicator().iloc[-1]
    # Pakai 6 candle terakhir untuk chg — lebih reliable dari 4
    chg   = (price - c.iloc[-7]) / c.iloc[-7] * 100

    # BULL: alignment ketat + pergerakan nyata ke atas
    if price > ema9 > ema21 > ema50 and chg > 0.3:
        return "BULL"
    # BEAR: alignment ketat + pergerakan nyata ke bawah
    elif price < ema9 < ema21 < ema50 and chg < -0.3:
        return "BEAR"
    # MILD hanya kalau pergerakan cukup signifikan
    elif price > ema9 > ema21 and chg > 0.1:
        return "MILD_BULL"
    elif price < ema9 < ema21 and chg < -0.1:
        return "MILD_BEAR"
    return "SIDEWAYS"


# ════════════════════════════════════════════════════
#  ATR-BASED LEVELS
# ════════════════════════════════════════════════════
def calc_atr_levels(entry, atr, direction):
    raw_sl_dist  = atr * ATR_SL_MULT
    raw_tp1_dist = atr * ATR_TP1_MULT
    raw_tp2_dist = atr * ATR_TP2_MULT
    raw_ic_dist  = atr * INSTANT_CUT_MULT

    # Hard cap SL: tidak boleh lebih dari MAX_SL_PCT% dari entry
    sl_dist  = max(entry * MIN_SL_PCT, min(raw_sl_dist, entry * MAX_SL_PCT))

    # Hard cap dollar loss: dengan leverage 20x dan ORDER_USDT $1,
    # max loss per trade = ORDER_USDT × (sl_pct × LEVERAGE)
    # Worst case v15.5: -$0.32 dari $1 trade = 32% loss = SL 1.6% × 20x
    # Fix: cap SL sehingga max loss tidak lebih dari MAX_LOSS_PER_TRADE_USD
    max_sl_dollar = MAX_LOSS_PER_TRADE_USD / LEVERAGE  # sebagai % dari modal per trade
    sl_dist = min(sl_dist, entry * max_sl_dollar)

    tp1_dist = max(entry * MIN_TP1_PCT, raw_tp1_dist)
    tp2_dist = min(entry * MAX_TP2_PCT, raw_tp2_dist)
    tp2_dist = max(tp2_dist, tp1_dist * 1.4)

    if direction == "LONG":
        sl          = round(entry - sl_dist,  8)
        tp1         = round(entry + tp1_dist, 8)
        tp2         = round(entry + tp2_dist, 8)
        instant_cut = round(entry - raw_ic_dist, 8)
    else:
        sl          = round(entry + sl_dist,  8)
        tp1         = round(entry - tp1_dist, 8)
        tp2         = round(entry - tp2_dist, 8)
        instant_cut = round(entry + raw_ic_dist, 8)

    return {
        "sl":          sl,
        "tp1":         tp1,
        "tp2":         tp2,
        "instant_cut": instant_cut,
        "sl_pct":      sl_dist  / entry,
        "tp1_pct":     tp1_dist / entry,
        "tp2_pct":     tp2_dist / entry,
        "atr":         atr,
        "atr_pct":     atr / entry,
    }


# ════════════════════════════════════════════════════
#  MOMENTUM CHECK
# ════════════════════════════════════════════════════
def check_momentum_strength(df, direction):
    if df is None or len(df) < 10:
        return False, 0, "no_data"

    last   = df.iloc[-1]
    recent = df.iloc[-6:-1]

    price_now  = last["close"]
    price_5ago = df.iloc[-6]["close"]
    momentum_pct = (price_now - price_5ago) / price_5ago

    if direction == "LONG" and momentum_pct < MIN_MOMENTUM_PCT:
        return False, momentum_pct, f"mom_weak({momentum_pct*100:.2f}%)"
    if direction == "SHORT" and momentum_pct > -MIN_MOMENTUM_PCT:
        return False, momentum_pct, f"mom_weak({momentum_pct*100:.2f}%)"

    vol_ratio = last["vol_ratio"]
    if vol_ratio < MIN_VOL_SURGE:
        return False, momentum_pct, f"vol_low({vol_ratio:.1f}x)"

    if direction == "LONG":
        bullish_candles = sum(1 for _, row in recent.iterrows() if row["close"] > row["open"])
        if bullish_candles < MIN_TREND_CANDLES:
            return False, momentum_pct, f"candles_weak({bullish_candles}/5)"
    else:
        bearish_candles = sum(1 for _, row in recent.iterrows() if row["close"] < row["open"])
        if bearish_candles < MIN_TREND_CANDLES:
            return False, momentum_pct, f"candles_weak({bearish_candles}/5)"

    if last["body_ratio"] < 0.35:
        return False, momentum_pct, f"weak_candle(body:{last['body_ratio']:.2f})"

    desc = f"mom={momentum_pct*100:+.2f}% vol={vol_ratio:.1f}x"
    return True, momentum_pct, desc


# ════════════════════════════════════════════════════
#  CONTINUATION CONFIRMATION
# ════════════════════════════════════════════════════
def check_continuation(df, direction):
    if df is None or len(df) < 5:
        return False, "no_data"

    last  = df.iloc[-1]
    prev  = df.iloc[-2]
    prev2 = df.iloc[-3]

    if direction == "LONG":
        if last["close"] <= last["open"]:
            return False, "last_bearish"
        if last["high"] <= prev["high"] and prev["high"] <= prev2["high"]:
            return False, "no_hh"
        if prev["close"] < prev["open"] and prev["body_ratio"] > 0.7:
            return False, "engulf_bear_prev"
        return True, "ok"
    else:
        if last["close"] >= last["open"]:
            return False, "last_bullish"
        if last["low"] >= prev["low"] and prev["low"] >= prev2["low"]:
            return False, "no_ll"
        if prev["close"] > prev["open"] and prev["body_ratio"] > 0.7:
            return False, "engulf_bull_prev"
        return True, "ok"


# ════════════════════════════════════════════════════
#  MACRO REFRESH
# ════════════════════════════════════════════════════
def refresh_macro():
    now = time.time()
    if now - _macro["last_fng"] > 300:
        try:
            d = requests.get("https://api.alternative.me/fng/?limit=1", timeout=5).json()["data"][0]
            _macro["fng"]       = int(d["value"])
            _macro["fng_label"] = d["value_classification"]
            _macro["last_fng"]  = now
        except: pass

    if now - _macro["last_btc"] > 5:
        try:
            df_1m  = get_ohlcv("BTCUSDT", Client.KLINE_INTERVAL_1MINUTE, 30)
            df_5m  = get_ohlcv("BTCUSDT", Client.KLINE_INTERVAL_5MINUTE, 60)
            df_15m = get_ohlcv("BTCUSDT", Client.KLINE_INTERVAL_15MINUTE, 60)
            df_1h  = get_ohlcv("BTCUSDT", Client.KLINE_INTERVAL_1HOUR, 60)
            _macro["btc_trend_1m"]  = _calc_trend(df_1m)
            _macro["btc_trend_5m"]  = _calc_trend(df_5m)
            _macro["btc_trend_15m"] = _calc_trend(df_15m)
            _macro["btc_trend_1h"]  = _calc_trend(df_1h)
            _macro["last_btc"]      = now
            t5m  = _macro["btc_trend_5m"]
            t15m = _macro["btc_trend_15m"]
            if t15m in ("BULL","BEAR") or t5m in ("BULL","BEAR"):
                _macro["scalp_mode"] = "TREND"
            else:
                _macro["scalp_mode"] = "MEAN_REV"
        except: pass

    if now - _macro["last_breadth"] > 30:
        try:
            bullish = 0
            sample  = SYMBOLS[:20]
            for sym in sample:
                df = get_ohlcv(sym, Client.KLINE_INTERVAL_5MINUTE, 10)
                if df is not None and len(df) >= 5:
                    c  = df["close"]
                    e9 = ta.trend.EMAIndicator(c, 9).ema_indicator().iloc[-1]
                    if c.iloc[-1] > e9: bullish += 1
            _macro["market_breadth"] = bullish / len(sample)
            _macro["last_breadth"]   = now
        except: pass

    if now - _macro.get("last_news", 0) > 120:
        try:
            data = requests.get(
                "https://cryptopanic.com/api/v1/posts/?auth_token=demo&public=true&currencies=BTC",
                timeout=5).json()
            neg_kw = ["crash","hack","ban","fraud","collapse","seized","scam","plunge"]
            pos_kw = ["institutional","ath","approved","record","bullish","rally","surge"]
            neg = pos = 0
            for post in data.get("results", [])[:8]:
                tl = post.get("title","").lower()
                if any(w in tl for w in neg_kw): neg += 1
                if any(w in tl for w in pos_kw): pos += 1
            score = pos - neg
            if score <= -3:   _macro["news"] = "strong_negative"
            elif score <= -1: _macro["news"] = "negative"
            elif score >= 3:  _macro["news"] = "strong_positive"
            else:             _macro["news"] = "neutral"
            _macro["last_news"] = now
        except: pass


def update_btc_price():
    try:
        px = get_price("BTCUSDT")
        if px > 0: _btc_price_history.append((time.time(), px))
    except: pass


def detect_flash_move():
    if len(_btc_price_history) < 2: return "none", 0.0
    cutoff  = time.time() - 120
    oldest  = next((px for ts, px in _btc_price_history if ts >= cutoff), None)
    if oldest is None: return "none", 0.0
    current = _btc_price_history[-1][1]
    pct = (current - oldest) / oldest * 100
    if pct <= -1.0: return "crash", abs(pct)
    if pct >= 1.0:  return "pump",  abs(pct)
    return "none", 0.0


# ════════════════════════════════════════════════════
#  ORDER BOOK IMBALANCE
# ════════════════════════════════════════════════════
def get_ob_imbalance(symbol):
    try:
        ob    = client.futures_order_book(symbol=symbol, limit=50)
        bid_w = sum(float(b[1]) * (1 / (i + 1)) for i, b in enumerate(ob["bids"][:20]))
        ask_w = sum(float(a[1]) * (1 / (i + 1)) for i, a in enumerate(ob["asks"][:20]))
        total = bid_w + ask_w
        return round((bid_w - ask_w) / total, 3) if total else 0.0
    except: return 0.0


# ════════════════════════════════════════════════════
#  ENTRY SCORE ENGINE v15
# ════════════════════════════════════════════════════
def get_entry_score(symbol, df_5m, direction):
    """
    Entry scoring v15.5 — lebih ketat, lebih bermakna.

    Perubahan dari v14/v15:
    - RSI confirmation: LONG butuh RSI 45-72, SHORT butuh RSI 28-55
      (hindari entry di ekstrem RSI yang sering reversal)
    - Setiap kategori harus punya kontribusi positif minimal
      (tidak bisa lolos hanya dari satu kategori saja)
    - Struktur harga: breakout harus real (tidak false breakout)
    - Volume harus seiring dengan arah momentum (bukan noise)
    """
    if df_5m is None or len(df_5m) < 35:
        return 0, []

    last  = df_5m.iloc[-1]
    prev  = df_5m.iloc[-2]
    prev2 = df_5m.iloc[-3]
    sigs  = []
    score = 0

    rsi      = last.get("rsi", 50)
    rsi_fast = last.get("rsi_fast", 50)
    p        = last["close"]

    # ── HARD GATE: RSI overextension ──────────────────────
    # Entry LONG saat RSI > 75 = beli di puncak, sering langsung balik
    # Entry SHORT saat RSI < 25 = jual di bottom, sering reversal
    if direction == "LONG"  and rsi > 75: return 0, []
    if direction == "SHORT" and rsi < 25: return 0, []

    # ── HARD GATE: RSI berlawanan arah ────────────────────
    # LONG butuh RSI minimal 40 (ada ruang naik)
    # SHORT butuh RSI maksimal 60 (ada ruang turun)
    if direction == "LONG"  and rsi < 40: return 0, []
    if direction == "SHORT" and rsi > 60: return 0, []

    # ── KATEGORI A: TREND (max 30) ────────────────────────
    e3, e5, e9, e21, e50 = (last["ema3"], last["ema5"], last["ema9"],
                             last["ema21"], last["ema50"])

    trend_score = 0
    trend_sig   = ""
    if direction == "LONG":
        if p > e3 > e5 > e9 > e21 > e50:
            trend_score = 30; trend_sig = "📐EMA_PERFECT↑"
        elif p > e3 > e5 > e9 > e21:
            trend_score = 25; trend_sig = "📐EMA_STACK↑"
        elif p > e5 > e9 > e21:
            trend_score = 18; trend_sig = "📐EMA↑"
        elif p > e9 > e21:
            trend_score = 10; trend_sig = "📐EMA_align↑"
    else:
        if p < e3 < e5 < e9 < e21 < e50:
            trend_score = 30; trend_sig = "📐EMA_PERFECT↓"
        elif p < e3 < e5 < e9 < e21:
            trend_score = 25; trend_sig = "📐EMA_STACK↓"
        elif p < e5 < e9 < e21:
            trend_score = 18; trend_sig = "📐EMA↓"
        elif p < e9 < e21:
            trend_score = 10; trend_sig = "📐EMA_align↓"

    # Bonus kalau RSI confirm arah
    if direction == "LONG"  and 50 <= rsi <= 72: trend_score += 3
    if direction == "SHORT" and 28 <= rsi <= 50: trend_score += 3

    score += trend_score
    if trend_sig: sigs.append(trend_sig)

    # ── KATEGORI B: MOMENTUM (max 25) ─────────────────────
    mom5    = last.get("mom5", 0)
    mom3    = last.get("mom3", 0)
    vol_rat = last["vol_ratio"]
    atr_now = last["atr"]
    atr_pv  = df_5m.iloc[-6]["atr"] if len(df_5m) > 6 else atr_now
    atr_exp = atr_now > atr_pv * 1.15

    # Pastikan momentum searah dengan direction
    mom_aligned = (mom5 > 0 and direction == "LONG") or (mom5 < 0 and direction == "SHORT")
    mom5_abs    = abs(mom5)
    mom3_abs    = abs(mom3)

    vol_score = 0
    vol_sig   = ""
    if mom_aligned:
        if mom5_abs >= 0.008 and atr_exp and vol_rat >= 2.0:
            vol_score = 25; vol_sig = f"🚀Mom{mom5_abs*100:.1f}%+ATR+Vol{vol_rat:.1f}x"
        elif mom5_abs >= 0.005 and vol_rat >= 2.0:
            vol_score = 20; vol_sig = f"📈Mom{mom5_abs*100:.1f}%+Vol{vol_rat:.1f}x"
        elif mom5_abs >= 0.003 and vol_rat >= 1.5:
            vol_score = 14; vol_sig = f"📈Mom{mom5_abs*100:.1f}%"
        elif mom5_abs >= 0.002 and vol_rat >= 3.0:
            vol_score = 12; vol_sig = f"🔥VolSurge{vol_rat:.1f}x"
        elif mom5_abs >= 0.0015:
            vol_score = 7
    else:
        # Momentum berlawanan = penalti besar
        vol_score = -10

    score += vol_score
    if vol_sig: sigs.append(vol_sig)

    # ── KATEGORI C: ORDER FLOW (max 25) ──────────────────
    h_now  = last["macd_hist"]
    h_prev = prev["macd_hist"]
    h_p2   = prev2["macd_hist"]
    br     = last["buy_ratio"]

    # MACD harus konsisten min 3 candle searah
    macd_trend_long  = h_now > h_prev and h_prev > h_p2 and h_now > 0
    macd_trend_short = h_now < h_prev and h_prev < h_p2 and h_now < 0
    macd_cross_up    = h_p2 < 0 and h_prev < 0 and h_now >= 0
    macd_cross_dn    = h_p2 > 0 and h_prev > 0 and h_now <= 0

    flow_score = 0
    flow_sig   = ""
    if direction == "LONG":
        if macd_trend_long and br > 0.58:
            flow_score = 25; flow_sig = f"✅MACD↑↑+Buy{br:.0%}"
        elif macd_trend_long:
            flow_score = 18; flow_sig = "✅MACD↑↑"
        elif macd_cross_up and br > 0.55:
            flow_score = 22; flow_sig = f"⚡MACD_X0↑+Buy{br:.0%}"
        elif macd_cross_up:
            flow_score = 15; flow_sig = "⚡MACD_X0↑"
        elif h_now > 0 and h_now > h_prev and br > 0.60:
            flow_score = 10; flow_sig = f"💧MACD+Buy{br:.0%}"
        elif br > 0.65:
            flow_score = 8; flow_sig = f"💧Buy{br:.0%}"
    else:
        if macd_trend_short and br < 0.42:
            flow_score = 25; flow_sig = f"✅MACD↓↓+Sell{1-br:.0%}"
        elif macd_trend_short:
            flow_score = 18; flow_sig = "✅MACD↓↓"
        elif macd_cross_dn and br < 0.45:
            flow_score = 22; flow_sig = f"⚡MACD_X0↓+Sell{1-br:.0%}"
        elif macd_cross_dn:
            flow_score = 15; flow_sig = "⚡MACD_X0↓"
        elif h_now < 0 and h_now < h_prev and br < 0.40:
            flow_score = 10; flow_sig = f"💧MACD+Sell{1-br:.0%}"
        elif br < 0.35:
            flow_score = 8; flow_sig = f"💧Sell{1-br:.0%}"

    score += flow_score
    if flow_sig: sigs.append(flow_sig)

    # ── KATEGORI D: MARKET STRUCTURE (max 20) ─────────────
    # Breakout harus dikonfirmasi volume dan body kuat
    # Hindari false breakout (breakout tanpa volume)
    recent_hi = df_5m.iloc[-8:-1]["high"].max()
    recent_lo = df_5m.iloc[-8:-1]["low"].min()
    candle_range = last["high"] - last["low"]
    is_strong_candle = last["body_ratio"] > 0.55 and last["vol_ratio"] > 1.5

    struct_score = 0
    struct_sig   = ""
    if direction == "LONG":
        confirmed_breakout = p > recent_hi and is_strong_candle
        engulf = (last["close"] > last["open"]
                  and last["close"] > prev["high"]
                  and last["body_ratio"] > 0.65
                  and last["vol_ratio"] > 1.3)
        if confirmed_breakout and last["vol_ratio"] > 2.0:
            struct_score = 20; struct_sig = "🚀BreakoutBull"
        elif confirmed_breakout:
            struct_score = 14; struct_sig = "📈Breakout↑"
        elif engulf:
            struct_score = 18; struct_sig = "🕯️Engulf↑"
        elif p > recent_hi and last["vol_ratio"] > 2.5:
            struct_score = 10; struct_sig = "📈HiBreak↑"
    else:
        confirmed_breakdown = p < recent_lo and is_strong_candle
        engulf = (last["close"] < last["open"]
                  and last["close"] < prev["low"]
                  and last["body_ratio"] > 0.65
                  and last["vol_ratio"] > 1.3)
        if confirmed_breakdown and last["vol_ratio"] > 2.0:
            struct_score = 20; struct_sig = "💥BreakdownBear"
        elif confirmed_breakdown:
            struct_score = 14; struct_sig = "📉Breakdown↓"
        elif engulf:
            struct_score = 18; struct_sig = "🕯️Engulf↓"
        elif p < recent_lo and last["vol_ratio"] > 2.5:
            struct_score = 10; struct_sig = "📉LoBreak↓"

    score += struct_score
    if struct_sig: sigs.append(struct_sig)

    # ── VETO: Kategori penting harus ada kontribusi ────────
    # Kalau tidak ada trend DAN tidak ada structure → score sangat rendah
    if trend_score == 0 and struct_score == 0:
        return 0, []   # tidak ada base, skip

    # Kalau MACD berlawanan kuat (negatif sementara entry LONG, atau sebaliknya)
    if direction == "LONG"  and h_now < 0 and h_prev < 0 and h_p2 < 0:
        score -= 15    # penalti: semua MACD negatif untuk LONG
    if direction == "SHORT" and h_now > 0 and h_prev > 0 and h_p2 > 0:
        score -= 15

    return max(0, min(100, score)), sigs


def determine_direction(df_5m, df_15m=None):
    """
    Tentukan arah entry. v15.5: lebih ketat.
    - Butuh minimal 8 poin (naik dari 6)
    - Keunggulan minimal 4 poin dari arah lawan (bukan hanya 1)
    - RSI harus mendukung arah
    """
    if df_5m is None or len(df_5m) < 20: return None
    last   = df_5m.iloc[-1]
    prev   = df_5m.iloc[-2]
    price  = last["close"]
    e3, e5, e9, e21 = last["ema3"], last["ema5"], last["ema9"], last["ema21"]
    rsi    = last.get("rsi", 50)
    long_pts = short_pts = 0

    # EMA stack — paling kuat
    if price > e3 > e5 > e9 > e21:  long_pts  += 5
    elif price < e3 < e5 < e9 < e21: short_pts += 5
    elif price > e5 > e9 > e21:     long_pts  += 3
    elif price < e5 < e9 < e21:     short_pts += 3
    elif price > e9 > e21:          long_pts  += 1
    elif price < e9 < e21:          short_pts += 1

    # Momentum 5 candle
    mom5 = last.get("mom5", 0)
    if mom5 > 0.003:    long_pts  += 3
    elif mom5 > 0.001:  long_pts  += 1
    elif mom5 < -0.003: short_pts += 3
    elif mom5 < -0.001: short_pts += 1

    # MACD histogram — harus 2 candle searah
    h_now  = last["macd_hist"]
    h_prev = prev["macd_hist"]
    if h_now > 0 and h_prev > 0 and h_now > h_prev:  long_pts  += 2
    elif h_now < 0 and h_prev < 0 and h_now < h_prev: short_pts += 2
    elif h_now > h_prev:                               long_pts  += 1
    else:                                              short_pts += 1

    # Buy/sell pressure + candle arah
    br = last.get("buy_ratio", 0.5)
    if br > 0.58 and last["close"] > last["open"]:   long_pts  += 2
    elif br < 0.42 and last["close"] < last["open"]: short_pts += 2

    # RSI confirmation
    if 48 <= rsi <= 68:   long_pts  += 2   # RSI zona bullish ideal
    elif 32 <= rsi <= 52: short_pts += 2   # RSI zona bearish ideal
    elif rsi > 72:        short_pts += 1   # RSI terlalu tinggi → bias SHORT
    elif rsi < 28:        long_pts  += 1   # RSI terlalu rendah → bias LONG

    # 15m higher timeframe
    if df_15m is not None and len(df_15m) >= 20:
        l15 = df_15m.iloc[-1]
        e9_15  = l15.get("ema9",  l15["close"])
        e21_15 = l15.get("ema21", l15["close"])
        if e9_15 > e21_15: long_pts  += 2
        else:              short_pts += 2

    # BTC macro
    btc_t = _macro.get("btc_trend_5m", "UNKNOWN")
    if btc_t in BULL_TRENDS:   long_pts  += 2
    elif btc_t in BEAR_TRENDS:  short_pts += 2

    # Keputusan: butuh ≥ 8 poin DAN keunggulan ≥ 4 dari lawan
    margin = 4
    if long_pts >= 8  and long_pts  - short_pts >= margin: return "LONG"
    if short_pts >= 8 and short_pts - long_pts  >= margin: return "SHORT"
    return None


def get_market_quality():
    """
    Market Quality score 0-100.
    v15.6: Fix breadth 5% + MILD_BEAR scoring terlalu tinggi (77).
    MEAN_REV mode = hard block entry.
    Breadth ekstrem hanya bonus kalau BTC juga confirm arah tsb.
    """
    btc_5m  = _macro.get("btc_trend_5m",  "UNKNOWN")
    btc_15m = _macro.get("btc_trend_15m", "UNKNOWN")
    btc_1h  = _macro.get("btc_trend_1h",  "UNKNOWN")
    breadth = _macro.get("market_breadth", 0.5)
    fng     = _macro.get("fng", 50)
    mode    = _macro.get("scalp_mode", "TREND")

    # Hard block: MEAN_REV = jangan entry apapun
    if mode == "MEAN_REV":
        return 20   # di bawah MQ_MIN_ENTRY = skip

    btc_trends = [btc_5m, btc_15m, btc_1h]
    bull_count = sum(1 for t in btc_trends if t in BULL_TRENDS)
    bear_count = sum(1 for t in btc_trends if t in BEAR_TRENDS)
    max_agree  = max(bull_count, bear_count)

    # Hard block: BTC tidak ada agreement
    if max_agree < 2:
        return 28

    # Hard block: breadth abu-abu
    if 0.35 <= breadth <= 0.65:
        return 35

    score = 50

    # BTC agreement
    if max_agree == 3:   score += 25
    elif max_agree == 2: score += 10

    # Breadth hanya bonus kalau selaras dengan BTC
    if breadth >= 0.75 and bull_count >= 2:
        score += 18   # bullish market + BTC bull
    elif breadth <= 0.15 and bear_count >= 2:
        score += 12   # bearish market + BTC bear (SHORT bisa valid)
    elif breadth >= 0.65 and bull_count >= 1:
        score += 8
    elif breadth <= 0.25 and bear_count >= 1:
        score += 5
    elif breadth <= 0.15:
        score -= 5    # breadth sangat rendah tapi BTC tidak bear = anomali

    # F&G
    if 28 <= fng <= 72:
        score += 5
    elif fng < 18 or fng > 88:
        score -= 12
    else:
        score -= 3

    return max(0, min(100, score))


# Threshold market quality untuk entry
MQ_MIN_ENTRY     = 48    # v15.6: naik dari 45 — lebih selektif
MQ_HIGH_QUALITY  = 70    # v15.6: naik dari 65


# ════════════════════════════════════════════════════
#  ENTRY FILTER v15.4
# ════════════════════════════════════════════════════
def should_enter(symbol):
    # ── Smart Coin Selector: blacklist check ──────────────
    if is_coin_blacklisted(symbol):
        _stats["skipped_blacklist"] += 1
        return None, "blacklist"

    killed, kill_reason = check_kill_switch()
    if killed:
        return None, f"kill:{kill_reason}"

    if is_symbol_cooling_down(symbol):
        return None, "cooldown"

    fng  = _macro["fng"]
    news = _macro["news"]
    if fng < MIN_FNG:             return None, f"F&G={fng}"
    if news == "strong_negative": return None, "bad_news"

    flash_dir, _ = detect_flash_move()
    if flash_dir != "none":       return None, f"flash_{flash_dir}"

    # ── Market Quality Gate ───────────────────────────────
    mq = get_market_quality()
    if mq < MQ_MIN_ENTRY:
        _stats["skipped_mq"] = _stats.get("skipped_mq", 0) + 1
        return None, f"mq_low({mq})"

    tickers = fetch_ticker24h_all()
    pct_24h = 0.0
    if symbol in tickers:
        t24 = tickers[symbol]
        if t24["vol24h"] < 500_000:
            return None, f"illiquid(${t24['vol24h']/1e6:.2f}M)"
        pct_24h = t24["pct"]

    # ── Candle timing filter (BARU) ───────────────────────
    candle_age = get_candle_age_pct(symbol, 300)  # 5m = 300s
    if candle_age > MAX_CANDLE_AGE_PCT:
        _stats["skipped_candle_age"] += 1
        return None, f"candle_old({candle_age*100:.0f}%)"

    df_5m  = get_ohlcv(symbol, Client.KLINE_INTERVAL_5MINUTE, 80)
    df_15m = get_ohlcv(symbol, Client.KLINE_INTERVAL_15MINUTE, 60)
    if df_5m is None or len(df_5m) < 30: return None, "no_data"

    df_5m = run_ta(df_5m.copy())
    if df_15m is not None and len(df_15m) >= 20:
        df_15m = run_ta(df_15m.copy())

    direction = determine_direction(df_5m, df_15m)
    if direction is None: return None, "no_direction"

    # ── Volatility sweet spot check (BARU) ────────────────
    last  = df_5m.iloc[-1]
    price = last["close"]
    atr   = last["atr"]
    atr_p = atr / price if price > 0 else 0
    if atr_p < MIN_ATR_PCT:
        return None, f"ATR_flat({atr_p*100:.3f}%)"
    if atr_p > MAX_ATR_PCT:
        return None, f"ATR_terlalu_besar({atr_p*100:.2f}%)"

    is_chop, chop_desc = is_chop_market(df_5m, direction)
    if is_chop:
        _stats["skipped_chop"] += 1
        return None, f"chop:{chop_desc}"

    mom_pass, mom_pct, mom_desc = check_momentum_strength(df_5m, direction)
    if not mom_pass:
        _stats["skipped_no_momentum"] += 1
        return None, f"no_mom:{mom_desc}"

    cont_pass, cont_desc = check_continuation(df_5m, direction)
    if not cont_pass:
        return None, f"no_cont:{cont_desc}"

    # ── Multi-timeframe alignment (BARU) ──────────────────
    if REQUIRE_MTF_ALIGN:
        mtf_agree, mtf_total, mtf_detail = get_mtf_alignment(symbol, direction)
        if mtf_total > 0 and mtf_agree < MTF_MIN_AGREE:
            _stats["skipped_mtf"] += 1
            return None, f"mtf_mis({mtf_agree}/{mtf_total}:{mtf_detail})"

    funding_bias, fr = get_funding_bias(symbol)
    if direction == "LONG"  and funding_bias == "bearish_bias" and fr > 0.001:
        return None, f"funding_bearish({fr*100:.3f}%)"
    if direction == "SHORT" and funding_bias == "bullish_bias" and fr < -0.001:
        return None, f"funding_bullish({fr*100:.3f}%)"

    scalp_mode  = _macro.get("scalp_mode", "TREND")
    breadth     = _macro.get("market_breadth", 0.5)
    btc_5m      = _macro["btc_trend_5m"]
    btc_15m     = _macro["btc_trend_15m"]
    btc_1h      = _macro.get("btc_trend_1h", "UNKNOWN")

    # Bear market = BTC BEAR (bukan MILD_BEAR) DAN breadth < 25% DAN 1h juga bear
    is_strong_bear = (btc_5m == "BEAR" and btc_15m in BEAR_TRENDS
                      and breadth < 0.25 and btc_1h in BEAR_TRENDS)

    # MEAN_REV: skip semua entry kecuali SHORT di strong bear market yang confirmed
    if scalp_mode == "MEAN_REV":
        if direction == "LONG":
            _stats["skipped_mean_rev"] += 1
            return None, "skip_MEAN_REV_LONG"
        if direction == "SHORT":
            # Untuk SHORT di MEAN_REV, butuh strong bear + BTC 5m harus BEAR (bukan MILD)
            if not is_strong_bear:
                _stats["skipped_mean_rev"] += 1
                return None, f"skip_MEAN_REV_SHORT(bear={is_strong_bear},BTC5m={btc_5m})"

    # Bear market: block LONG kalau kondisi benar-benar bearish
    # Pakai is_strong_bear (bukan MILD_BEAR + breadth 30%)
    if BEAR_BLOCK_LONG and is_strong_bear and direction == "LONG":
        return None, f"bear_mkt_block_LONG(breadth={breadth*100:.0f}%)"

    # BTC alignment check — harus jelas, MILD tidak cukup untuk block
    if direction == "LONG"  and btc_5m == "BEAR" and btc_15m == "BEAR":
        return None, f"skip_LONG:BTC_{btc_5m}"
    if direction == "SHORT" and btc_5m == "BULL" and btc_15m == "BULL":
        return None, f"skip_SHORT:BTC_{btc_5m}"
    if direction == "LONG"  and fng > MAX_FNG_LONG:
        return None, f"overbought:F&G={fng}"

    score, sigs = get_entry_score(symbol, df_5m, direction)

    # Bear market bonus: hanya kalau benar-benar strong bear
    if is_strong_bear and direction == "SHORT":
        score = min(100, score + BEAR_SHORT_SCORE_BONUS)

    min_score_now = get_session_min_score()

    # Market Quality adjustment:
    # Kalau market bagus (MQ >= 65): threshold turun 3 poin (lebih mudah entry)
    # Kalau market biasa (45-65): threshold normal
    # → mq sudah pasti >= MQ_MIN_ENTRY di sini karena sudah lolos gate di atas
    if mq >= MQ_HIGH_QUALITY:
        min_score_now = max(MIN_SCORE - 3, min_score_now - 3)
    elif mq < 55:
        min_score_now = min_score_now + 5   # market kurang bagus: threshold naik 5

    if is_strong_bear and direction == "SHORT":
        min_score_now = min(min_score_now, BEAR_MIN_SCORE_SHORT)

    if score < min_score_now:
        if min_score_now > MIN_SCORE:
            _stats["skipped_session"] += 1
        return None, f"score={score:.0f}<{min_score_now}(mq={mq})"

    if len(sigs) < MIN_ENTRY_SIGNALS: return None, f"signals={len(sigs)}"

    levels = calc_atr_levels(price, atr, direction)

    spread_ratio = get_spread_ratio(symbol, levels["tp1"], price)
    if spread_ratio > MAX_SPREAD_RATIO:
        _stats["skipped_spread"] += 1
        return None, f"spread_lebar({spread_ratio:.2f}x_TP1)"

    ob_imb = get_ob_imbalance(symbol)
    if direction == "LONG"  and ob_imb < -0.20: return None, f"OB_SHORT({ob_imb:.2f})"
    if direction == "SHORT" and ob_imb > 0.20:  return None, f"OB_LONG({ob_imb:.2f})"

    # ── Hitung priority score untuk smart ranking ─────────
    priority_score = calc_coin_priority_score(symbol, score, direction, df_5m)

    return direction, {
        "score":          score,
        "priority_score": priority_score,
        "signals":        sigs,
        "direction":      direction,
        "sl":             levels["sl"],
        "tp1":            levels["tp1"],
        "tp2":            levels["tp2"],
        "sl_pct":         levels["sl_pct"],
        "tp1_pct":        levels["tp1_pct"],
        "ob_imb":         ob_imb,
        "atr":            atr,
        "atr_pct":        levels["atr_pct"],
        "mom_pct":        mom_pct,
        "pct_24h":        pct_24h,
        "funding":        fr,
        "scalp_mode":     _macro["scalp_mode"],
        "btc_trend":      _macro["btc_trend_5m"],
        "instant_cut":    levels["instant_cut"],
        "perf_score":     get_coin_perf_score(symbol),
        "is_priority":    is_coin_priority(symbol),
        "mq":             mq,
    }


# ════════════════════════════════════════════════════
#  PARALLEL SCANNER
# ════════════════════════════════════════════════════
def scan_symbol_safe(symbol):
    try:
        time.sleep(SCAN_DELAY_MS)
        direction, info = should_enter(symbol)
        if direction: return symbol, direction, info
    except: pass
    return None


def scan_batch_parallel(symbols):
    candidates     = []
    symbols_to_scan = symbols[:20]
    futures = {_executor.submit(scan_symbol_safe, sym): sym for sym in symbols_to_scan}

    try:
        for future in as_completed(futures, timeout=12):
            try:
                result = future.result(timeout=2)
                if result:
                    candidates.append(result)
            except Exception:
                pass

    except TimeoutError:
        done_count    = sum(1 for f in futures if f.done())
        pending_count = len(futures) - done_count
        for future in futures:
            if future.done():
                try:
                    result = future.result(timeout=0)
                    if result:
                        candidates.append(result)
                except Exception:
                    pass
            else:
                future.cancel()
        if pending_count > 0:
            print(f"  ⚠️  Scan partial timeout: {done_count}/{len(futures)} selesai, "
                  f"{pending_count} di-cancel ({len(candidates)} kandidat)")

    except Exception as e:
        print(f"  ❌ Scan error tak terduga: {e}")

    return candidates


def sort_candidates_smart(candidates):
    """
    Sort kandidat berdasarkan priority_score (bukan hanya base score).
    Priority list coins naik, blacklisted tidak masuk sini.
    """
    def sort_key(item):
        sym, direction, info = item
        return info.get("priority_score", info.get("score", 0))

    return sorted(candidates, key=sort_key, reverse=True)


# ════════════════════════════════════════════════════
#  INSTANT RE-SCAN
# ════════════════════════════════════════════════════
def trigger_rescan(reason="", priority_symbol=None):
    if priority_symbol:
        _hot_symbols.appendleft(priority_symbol)
    _rescan_queue.put({"reason": reason, "ts": time.time()})


def instant_rescan_worker(symbols_active):
    while True:
        try:
            event = _rescan_queue.get(timeout=60)
            reason = event.get("reason", "")
            time.sleep(RE_SCAN_DELAY_SEC)

            slots_free = MAX_POSITIONS - len(open_positions)
            if slots_free <= 0: continue

            killed, _ = check_kill_switch()
            if killed: continue

            flash_dir, _ = detect_flash_move()
            if flash_dir != "none": continue
            if _macro["news"] == "strong_negative": continue

            hot  = [s for s in list(_hot_symbols) if s not in open_positions and not is_coin_blacklisted(s)]
            rest = [s for s in symbols_active if s not in open_positions and s not in hot and not is_coin_blacklisted(s)]
            scan_list = hot + rest

            _stats["rescans"] += 1
            print(f"\n  ⚡ RESCAN [{reason}] — {len(scan_list)} symbols, {slots_free} slot")

            try:
                candidates = scan_batch_parallel(scan_list[:40])
            except Exception as e:
                print(f"  ❌ Rescan error: {e}")
                candidates = []

            if candidates:
                candidates = sort_candidates_smart(candidates)
                print(f"  🎯 Rescan: {len(candidates)} kandidat")
                for sym, direction, info in candidates[:slots_free]:
                    if len(open_positions) >= MAX_POSITIONS: break
                    sig_str   = " | ".join(info.get("signals", [])[:3])
                    prio_tag  = "⭐" if info.get("is_priority") else "  "
                    print(f"  {prio_tag} {sym} {direction} Mom:{info.get('mom_pct',0)*100:+.2f}% PScore:{info.get('priority_score',0):.0f} | {sig_str}")
                    open_trade(sym, direction, info)
            else:
                print(f"  ⏳ Rescan: no setup")
        except queue.Empty:
            pass
        except Exception as e:
            print(f"  ❌ Rescan error: {e}")


# ════════════════════════════════════════════════════
#  TRADE EXECUTION — v15 PAPER TRADING
# ════════════════════════════════════════════════════
def open_trade(symbol, direction, info):
    with _lock:
        if symbol in open_positions: return
        if len(open_positions) >= MAX_POSITIONS: return
        open_positions[symbol] = {"_reserved": True}
        if len(open_positions) > MAX_POSITIONS:
            open_positions.pop(symbol, None)
            return

    try:
        set_leverage(symbol)
        price = get_price(symbol)
        if price == 0:
            with _lock: open_positions.pop(symbol, None)
            return
        qty = calc_qty(symbol, price)

        if PAPER_TRADING:
            paper_create_order(
                symbol=symbol,
                side=SIDE_BUY if direction == "LONG" else SIDE_SELL,
                order_type=ORDER_TYPE_MARKET,
                quantity=qty)
        else:
            client.futures_create_order(
                symbol=symbol,
                side=SIDE_BUY if direction == "LONG" else SIDE_SELL,
                type=ORDER_TYPE_MARKET,
                quantity=qty)

        entry  = get_price(symbol)
        atr    = info.get("atr", entry * 0.002)
        levels = calc_atr_levels(entry, atr, direction)
        sl     = levels["sl"]
        tp1    = levels["tp1"]
        tp2    = levels["tp2"]
        ic     = levels["instant_cut"]

        if direction == "LONG":
            trail_sl = entry * (1 - atr * ATR_TRAIL_MULT / entry)
            trail_sl = max(trail_sl, sl)
        else:
            trail_sl = entry * (1 + atr * ATR_TRAIL_MULT / entry)
            trail_sl = min(trail_sl, sl)

        open_positions[symbol] = {
            "side":             direction,
            "entry":            entry,
            "qty":              qty,
            "qty_remain":       qty,
            "sl":               sl,
            "tp1":              tp1,
            "tp2":              tp2,
            "peak":             entry,
            "trail_sl":         trail_sl,
            "trail_phase":      1,
            "trail_active":     False,
            "tp1_hit":          False,
            "be_active":        False,
            "open_time":        time.time(),
            "score":            info.get("score", 0),
            "priority_score":   info.get("priority_score", 0),
            "signals":          info.get("signals", []),
            "instant_cut":      ic,
            "instant_cut_done": False,
            "mom_pct":          info.get("mom_pct", 0),
            "entry_candle":     0,
            "atr":              atr,
            "scalp_mode":       info.get("scalp_mode", "TREND"),
            "paper":            PAPER_TRADING,
        }

        sl_p    = levels["sl_pct"]  * 100
        tp1_p   = levels["tp1_pct"] * 100
        tp2_p   = levels["tp2_pct"] * 100
        sig_str = " | ".join(info.get("signals", [])[:3])
        prio_tag = "⭐PRIORITY " if info.get("is_priority") else ""
        paper_tag = "[PAPER]" if PAPER_TRADING else ""

        print(f"\n  {'🟢' if direction=='LONG' else '🔴'} {paper_tag} {prio_tag}[{symbol}] {direction} @{entry:.5g}")
        print(f"     ATR:{atr:.5g}({levels['atr_pct']*100:.2f}%) SL:{sl_p:.2f}% TP1:{tp1_p:.2f}% TP2:{tp2_p:.2f}%")
        print(f"     PScore:{info.get('priority_score',0):.0f} PerfBonus:{info.get('perf_score',0):+.1f} Mom:{info.get('mom_pct',0)*100:+.2f}%")
        print(f"     {sig_str}")
        _stats["total_trades"] += 1

    except Exception as e:
        with _lock: open_positions.pop(symbol, None)
        print(f"  ❌ [{symbol}] Entry error: {e}")


def partial_close_tp1(symbol):
    pos = open_positions.get(symbol)
    if pos is None or pos.get("tp1_hit"): return
    try:
        if PAPER_TRADING:
            amt = paper_get_position_amt(symbol)
        else:
            amt = get_exchange_amt(symbol)

        if amt is None or amt == 0:
            pos["tp1_hit"] = True; return

        close_qty = round_step(abs(amt) * TP1_CLOSE_RATIO, get_sym_info(symbol)["step"])
        close_qty = max(close_qty, get_sym_info(symbol)["minQty"])
        if close_qty > abs(amt): close_qty = abs(amt)

        if PAPER_TRADING:
            paper_create_order(
                symbol=symbol,
                side=SIDE_SELL if amt > 0 else SIDE_BUY,
                order_type=ORDER_TYPE_MARKET,
                quantity=close_qty,
                reduce_only=True)
        else:
            client.futures_create_order(
                symbol=symbol,
                side=SIDE_SELL if amt > 0 else SIDE_BUY,
                type=ORDER_TYPE_MARKET,
                quantity=close_qty,
                reduceOnly=True)

        exit_p = get_price(symbol)
        side   = pos["side"]
        pnl    = (exit_p - pos["entry"]) * close_qty if side == "LONG" \
                 else (pos["entry"] - exit_p) * close_qty
        hold_s = time.time() - pos["open_time"]
        ptag   = "[PAPER]" if pos.get("paper") else ""
        print(f"  🎯 {ptag} [{symbol}] TP1 ({hold_s:.0f}s) PnL:{pnl:+.4f}U")

        pos["tp1_hit"]    = True
        pos["qty_remain"] = abs(amt) - close_qty
        pos["be_active"]  = True
        if side == "LONG":
            pos["sl"] = round(pos["entry"] * (1 + TRAIL_BE_PCT), 8)
        else:
            pos["sl"] = round(pos["entry"] * (1 - TRAIL_BE_PCT), 8)

        pos["trail_phase"]  = 2
        pos["trail_active"] = True
        pos["peak"]         = exit_p
        atr = pos.get("atr", exit_p * 0.002)

        if side == "LONG":
            pos["trail_sl"] = exit_p * (1 - atr * ATR_TRAIL_MULT / exit_p)
        else:
            pos["trail_sl"] = exit_p * (1 + atr * ATR_TRAIL_MULT / exit_p)

        _stats["tp1_hits"]  += 1
        _stats["wins"]      += 1
        _stats["total_pnl"] += pnl
        _stats["pnl_history"].append(pnl)
        update_kill_switch_after_trade(pnl)
        update_coin_perf(symbol, True, pnl)
        _perf[symbol]["wins"]   += 1
        _perf[symbol]["pnl"]    += pnl
        _perf[symbol]["trades"] += 1
        if pnl > _stats["best_trade"]: _stats["best_trade"] = pnl

        # Sync paper balance — partial TP1 juga harus masuk equity
        if PAPER_TRADING:
            _paper_balance["pnl_total"] += pnl
            _paper_balance["equity"]     = _paper_balance["initial_usdt"] + _paper_balance["pnl_total"]

        trade_log.append({
            "symbol": symbol, "side": side,
            "pnl": round(pnl, 4), "reason": "TP1 Partial",
            "hold_sec": int(hold_s), "paper": PAPER_TRADING,
        })
        _hot_symbols.appendleft(symbol)
        print_stats_inline()
    except Exception as e:
        print(f"  ❌ [{symbol}] TP1 error: {e}")
        pos["tp1_hit"] = True


def close_trade(symbol, reason=""):
    try:
        if PAPER_TRADING:
            amt = paper_get_position_amt(symbol)
        else:
            amt = get_exchange_amt(symbol)

        if amt is None: return False
        if amt == 0:
            with _lock: open_positions.pop(symbol, None)
            set_symbol_cooldown(symbol)
            trigger_rescan(f"close@{symbol}", priority_symbol=symbol)
            return True

        if PAPER_TRADING:
            paper_create_order(
                symbol=symbol,
                side=SIDE_SELL if amt > 0 else SIDE_BUY,
                order_type=ORDER_TYPE_MARKET,
                quantity=abs(amt),
                reduce_only=True)
        else:
            client.futures_create_order(
                symbol=symbol,
                side=SIDE_SELL if amt > 0 else SIDE_BUY,
                type=ORDER_TYPE_MARKET,
                quantity=abs(amt),
                reduceOnly=True)

        with _lock:
            pos = open_positions.pop(symbol, None)

        if pos:
            exit_p = get_price(symbol)
            qty_r  = pos.get("qty_remain", pos["qty"])
            side   = pos["side"]
            pnl    = (exit_p - pos["entry"]) * qty_r if side == "LONG" \
                     else (pos["entry"] - exit_p) * qty_r
            pct    = pnl / (pos["entry"] * qty_r) * 100 if qty_r > 0 else 0
            hold_s = time.time() - pos["open_time"]
            emoji  = "🟢" if pnl >= 0 else "🔴"
            be_tag = "[BE]" if pos.get("be_active") else ""
            ptag   = "[PAPER]" if pos.get("paper") else ""
            print(f"  {emoji} {ptag} [{symbol}] CLOSE — {reason}{be_tag} | {hold_s:.0f}s")
            print(f"     PnL: {pnl:+.4f}U ({pct:+.2f}%)")

            is_win = pnl >= 0
            trade_log.append({
                "symbol": symbol, "side": side,
                "pnl": round(pnl, 4), "reason": reason,
                "hold_sec": int(hold_s), "paper": PAPER_TRADING,
            })
            _stats["total_pnl"] += pnl
            _stats["pnl_history"].append(pnl)
            update_kill_switch_after_trade(pnl)
            update_coin_perf(symbol, is_win, pnl)

            _perf[symbol]["trades"] += 1
            _perf[symbol]["pnl"]    += pnl
            if is_win:
                _stats["wins"]   += 1
                _perf[symbol]["wins"] += 1
                if pnl > _stats["best_trade"]: _stats["best_trade"] = pnl
            else:
                _stats["losses"] += 1
                _perf[symbol]["losses"] += 1
                if pnl < _stats["worst_trade"]: _stats["worst_trade"] = pnl

            regime = pos.get("scalp_mode", "UNKNOWN")
            _perf_regime[regime]["pnl"] += pnl
            if is_win: _perf_regime[regime]["wins"] += 1
            else:      _perf_regime[regime]["losses"] += 1

            if "TP2"     in reason: _stats["tp2_hits"]     += 1
            if "SL"      in reason or "Stop" in reason: _stats["sl_hits"] += 1
            if "Force"       in reason: _stats["force_closes"] += 1
            if "Instant"     in reason: _stats["instant_cuts"] += 1
            if "SmartCut"    in reason: _stats["smart_cuts"]   += 1
            if "MomExit"     in reason: _stats["mom_exits"]    += 1
            if "StagExit"    in reason: _stats["stag_exits"]   += 1

            # Update paper balance
            if PAPER_TRADING:
                _paper_balance["pnl_total"] += pnl
                _paper_balance["equity"]     = _paper_balance["initial_usdt"] + _paper_balance["pnl_total"]

            print_stats_inline()
            set_symbol_cooldown(symbol)
            trigger_rescan(f"close@{symbol}({reason})", priority_symbol=symbol)

        return True
    except Exception as e:
        print(f"  ❌ [{symbol}] Close error: {e}")
        return False


# ════════════════════════════════════════════════════
#  POSITION MONITOR v15 — DELAYED TRAIL
# ════════════════════════════════════════════════════
def _get_live_momentum(symbol, side):
    """
    Cek momentum coin dari 1m chart — lebih robust, butuh 3 konfirmasi bukan 1.
    Return: (score -2..+2, description)
      +2 = momentum kuat searah
      +1 = momentum lemah searah
       0 = netral / tidak cukup sinyal
      -1 = mulai berbalik (waspada)
      -2 = berbalik kuat + confirmed (boleh pertimbangkan cut)

    PENTING: score -2 hanya kalau SEMUA sinyal agree berbalik.
    Satu candle bearish saja tidak cukup untuk -2.
    """
    try:
        df = get_ohlcv(symbol, Client.KLINE_INTERVAL_1MINUTE, 20)
        if df is None or len(df) < 10:
            return 0, "no_data"
        df = run_ta_lite(df.copy())

        last  = df.iloc[-1]
        prev  = df.iloc[-2]
        prev2 = df.iloc[-3]

        votes_for  = 0   # sinyal searah posisi
        votes_against = 0  # sinyal melawan posisi
        notes = []

        price = last["close"]
        e9    = last.get("ema9",  price)
        e21   = last.get("ema21", price)

        # ── Sinyal 1: EMA9 vs EMA21 alignment ────────────
        if side == "LONG":
            if e9 > e21:    votes_for  += 1; notes.append("ema9>21↑")
            elif e9 < e21:  votes_against += 1; notes.append("ema9<21↓")
        else:
            if e9 < e21:    votes_for  += 1; notes.append("ema9<21↓")
            elif e9 > e21:  votes_against += 1; notes.append("ema9>21↑")

        # ── Sinyal 2: Price vs EMA9 ───────────────────────
        if side == "LONG":
            if price > e9:  votes_for  += 1; notes.append("p>e9")
            else:           votes_against += 1; notes.append("p<e9")
        else:
            if price < e9:  votes_for  += 1; notes.append("p<e9")
            else:           votes_against += 1; notes.append("p>e9")

        # ── Sinyal 3: 3 candle trend ──────────────────────
        last3_bull = [df.iloc[i]["close"] > df.iloc[i]["open"] for i in [-3,-2,-1]]
        if side == "LONG":
            if sum(last3_bull) >= 2:  votes_for  += 1; notes.append("3c↑")
            elif sum(last3_bull) == 0: votes_against += 1; notes.append("3c↓")
        else:
            if sum(last3_bull) <= 1:  votes_for  += 1; notes.append("3c↓")
            elif sum(last3_bull) == 3: votes_against += 1; notes.append("3c↑")

        # Hitung score dari votes
        net = votes_for - votes_against
        if net >= 2:   score = 2
        elif net == 1: score = 1
        elif net == 0: score = 0
        elif net == -1: score = -1
        else:          score = -2   # semua 3 sinyal melawan

        return score, "|".join(notes)
    except:
        return 0, "err"


def calc_dynamic_hold_min(pos):
    """
    Hold time adaptif berdasarkan kondisi posisi.

    Prinsip:
    - Sudah TP1 hit → extend banyak, kejar TP2
    - Profit kecil → extend sedikit, beri waktu
    - Rugi kecil → default, tunggu SL atau momentum berbalik
    - Rugi besar + momentum semua melawan → cut lebih cepat
    """
    base       = MAX_HOLDING_MIN
    entry      = pos.get("entry", 1)
    price      = pos.get("_last_price", entry)
    side       = pos.get("side", "LONG")
    mom_score  = pos.get("_mom_score", 0)

    profit_pct = (price - entry) / entry if side == "LONG" else (entry - price) / entry

    if pos.get("tp1_hit"):
        return base * HOLD_MULT_TP1_HIT

    if profit_pct <= LOSS_BIG_THRESHOLD and mom_score <= -1:
        # Rugi besar dan momentum mulai berbalik — kurangi waktu tunggu
        return base * HOLD_MULT_LOSS_BIG

    if profit_pct < 0:
        return base * HOLD_MULT_LOSS_SMALL

    if profit_pct > TRAIL_ACTIVATE_PCT:
        return base * HOLD_MULT_PROFIT

    return base


def manage_positions():
    if not open_positions: return
    flash_dir, flash_pct = detect_flash_move()

    for symbol in list(open_positions.keys()):
        pos = open_positions.get(symbol)
        if pos is None: continue
        if pos.get("_reserved"): continue

        price = get_price(symbol)
        if price == 0: continue

        side  = pos["side"]
        entry = pos["entry"]
        atr   = pos.get("atr", entry * 0.002)

        # Simpan harga terakhir untuk calc_dynamic_hold_min
        pos["_last_price"] = price

        pos["entry_candle"] = pos.get("entry_candle", 0) + 1

        # ── Dynamic hold time ─────────────────────────────
        hold_min     = (time.time() - pos["open_time"]) / 60
        max_hold_now = calc_dynamic_hold_min(pos)

        if hold_min >= max_hold_now * 0.97:
            # Kalau posisi profit dan trail sudah aktif → jangan force close,
            # biarkan trail yang menutup. Force close hanya kalau:
            # 1. Posisi belum profit sama sekali, ATAU
            # 2. Trail belum aktif (belum proteksi profit)
            if side == "LONG":
                unrealized_pct = (price - entry) / entry
            else:
                unrealized_pct = (entry - price) / entry

            if pos.get("trail_active") and unrealized_pct > 0:
                # Trail aktif + masih profit → perketat trail, jangan force close
                if pos["trail_phase"] < 3:
                    pos["trail_phase"] = 3
                    print(f"     ⏰ [{symbol}] Hold limit → trail diperketat (bukan force close)")
                # Extend hold sedikit supaya trail punya waktu
                if hold_min < max_hold_now * 1.3:
                    pass   # Lanjut, jangan close
                else:
                    close_trade(symbol, f"⏰Force({hold_min:.1f}m/{max_hold_now:.1f}m)")
                    continue
            else:
                close_trade(symbol, f"⏰Force({hold_min:.1f}m/{max_hold_now:.1f}m)")
                continue

        # ── Flash move protection ─────────────────────────
        if flash_dir == "crash" and side == "LONG":
            close_trade(symbol, f"⚡FlashCrash-{flash_pct:.1f}%")
            continue
        elif flash_dir == "pump" and side == "SHORT":
            close_trade(symbol, f"⚡FlashPump+{flash_pct:.1f}%")
            continue

        # ── Smart momentum check ──────────────────────────
        # Cek momentum setiap 5 tick (~5 detik) — tidak setiap detik
        pos["_mom_tick"] = pos.get("_mom_tick", 0) + 1
        mom_score  = pos.get("_mom_score", 0)
        mom_detail = pos.get("_mom_detail", "")
        if pos["_mom_tick"] % 5 == 0:
            mom_score, mom_detail = _get_live_momentum(symbol, side)
            pos["_mom_score"]  = mom_score
            pos["_mom_detail"] = mom_detail

        # ── Instant cut (awal trade) ──────────────────────
        within_window = pos.get("entry_candle", 0) <= (INSTANT_CUT_WINDOW * 5)
        if not pos.get("instant_cut_done") and not pos.get("tp1_hit") and within_window:
            ic = pos["instant_cut"]
            if side == "LONG" and price <= ic:
                pos["instant_cut_done"] = True
                close_trade(symbol, "⚡InstCut")
                continue
            elif side == "SHORT" and price >= ic:
                pos["instant_cut_done"] = True
                close_trade(symbol, "⚡InstCut")
                continue
        elif not within_window:
            pos["instant_cut_done"] = True

        # ══════════════════════════════════════════════════
        #  LONG MANAGEMENT
        # ══════════════════════════════════════════════════
        if side == "LONG":
            profit_pct = (price - entry) / entry

            # TP1 hit
            if not pos["tp1_hit"] and price >= pos["tp1"]:
                partial_close_tp1(symbol)
                continue

            # Trail activation
            # Initial trail SL pakai 1.5x lebih lebar dari ATR_TRAIL_MULT —
            # beri ruang napas saat pertama aktif, baru ketatkan saat harga naik
            if not pos["trail_active"] and profit_pct >= TRAIL_ACTIVATE_PCT:
                pos["trail_active"] = True
                pos["sl"]       = round(entry * (1 + TRAIL_BE_PCT), 8)
                init_trail_mult = ATR_TRAIL_MULT * 1.5   # lebar saat pertama aktif
                pos["trail_sl"] = price * (1 - atr * init_trail_mult / price)
                pos["peak"]     = price
                print(f"     🔓 [{symbol}] Trail AKTIF @ {profit_pct*100:+.2f}% (init TSL:{pos['trail_sl']:.5g})")

            # Phase 3 tight trail
            if profit_pct >= TRAIL_TIGHT_PCT and pos["trail_phase"] < 3:
                pos["trail_phase"] = 3
                print(f"     ⬆️  [{symbol}] Phase3 trail ketat")

            # Update trail SL
            if pos["trail_active"] and price > pos["peak"]:
                pos["peak"] = price
                trail_mult  = ATR_TRAIL_TIGHT_MULT if pos["trail_phase"] >= 3 else ATR_TRAIL_MULT
                new_trail   = price * (1 - atr * trail_mult / price)
                pos["trail_sl"] = max(pos["trail_sl"], new_trail)

            # TP2
            if pos["tp1_hit"] and price >= pos["tp2"]:
                close_trade(symbol, "✨TP2")
                continue

            # Post-TP1 MomentumExit: kalau sudah TP1 tapi harga stagnan dan momentum berbalik,
            # keluar dari sisa posisi daripada nunggu TrailStop atau force close
            if (pos["tp1_hit"]
                    and mom_score <= -1
                    and time_ratio > 0.55
                    and profit_pct < pos.get("_peak_profit_pct", profit_pct) * 0.5):
                close_trade(symbol, f"🎯PostTP1MomExit(+{profit_pct*100:.2f}%)")
                continue

            # Track peak profit untuk post-TP1 exit
            if pos["tp1_hit"]:
                pos["_peak_profit_pct"] = max(pos.get("_peak_profit_pct", 0), profit_pct)

            # SmartCut — cut lebih awal dari force close kalau sudah jelas salah
            time_ratio = hold_min / max_hold_now
            if not pos["tp1_hit"] and profit_pct < 0:
                cond_a = (profit_pct <= LOSS_BIG_THRESHOLD
                          and mom_score <= -1
                          and time_ratio > 0.50)
                cond_b = (profit_pct < -0.002
                          and mom_score == -2
                          and time_ratio > 0.75)
                if cond_a or cond_b:
                    close_trade(symbol, f"🧠SmartCut({profit_pct*100:.2f}%,t={time_ratio:.0%})")
                    continue

            # ── MomentumExit LONG ─────────────────────────────
            # Keluar saat profit ada tapi momentum berbalik/berhenti.
            # Hindari force close dengan profit kecil — ambil sekarang.
            if (not pos["tp1_hit"]
                    and profit_pct >= MOM_EXIT_PROFIT_MIN
                    and time_ratio >= MOM_EXIT_TIME_MIN
                    and mom_score <= MOM_EXIT_SCORE_MAX):
                close_trade(symbol, f"🎯MomExit(+{profit_pct*100:.2f}%)")
                continue

            # ── StagnationExit LONG ───────────────────────────
            # Kondisi A: profit kecil + sudah 2.5m + momentum tidak positif
            stag_a = (hold_min >= STAG_CHECK_MIN
                      and abs(profit_pct) <= STAG_PROFIT_MAX
                      and mom_score <= STAG_MOM_MAX)
            # Kondisi B: profit ada tapi tidak bertambah (stuck) + sudah 55% waktu
            # Track high water mark profit untuk deteksi stuck
            if "profit_hwm" not in pos:
                pos["profit_hwm"]      = profit_pct
                pos["profit_hwm_time"] = hold_min
            elif profit_pct > pos["profit_hwm"]:
                pos["profit_hwm"]      = profit_pct
                pos["profit_hwm_time"] = hold_min
            stuck_duration = hold_min - pos["profit_hwm_time"]
            stag_b = (profit_pct > 0
                      and stuck_duration >= STAG_STUCK_MIN
                      and time_ratio >= STAG_STUCK_TIME_RATIO
                      and mom_score <= 0)
            if not pos["tp1_hit"] and (stag_a or stag_b):
                reason_tag = "A" if stag_a else f"B(stuck{stuck_duration:.1f}m)"
                close_trade(symbol, f"⏏️StagExit{reason_tag}({profit_pct*100:+.3f}%)")
                continue

            # Smart tighten trail: profit sudah ada + momentum berbalik confirmed
            if (pos["trail_active"]
                    and profit_pct > TRAIL_ACTIVATE_PCT
                    and mom_score <= -1
                    and pos["trail_phase"] < 3):
                pos["trail_phase"] = 3
                trail_mult = ATR_TRAIL_TIGHT_MULT
                new_trail  = price * (1 - atr * trail_mult / price)
                pos["trail_sl"] = max(pos["trail_sl"], new_trail)
                print(f"     ⚡ [{symbol}] Trail ketat ({mom_detail})")

            # Trail stop
            if pos["trail_active"] and price <= pos["trail_sl"]:
                tag = "🔒TrailBE" if pos.get("be_active") else "🔄TrailStop"
                close_trade(symbol, tag)
                continue

            # Hard SL
            if price <= pos["sl"]:
                close_trade(symbol, "🛑SL")
                continue

            # Status print
            pnl     = (price - entry) * pos.get("qty_remain", pos["qty"])
            phase   = pos.get("trail_phase", 1)
            act_tag = "✅" if pos["trail_active"] else "⏸️ "
            tsl     = f"TSL[{act_tag}P{phase}]:{pos['trail_sl']:.5g}"
            tp      = f"TP2:{pos['tp2']:.5g}" if pos["tp1_hit"] else f"TP1:{pos['tp1']:.5g}"
            ptag    = "📝" if pos.get("paper") else "📌"
            mom_tag = ["🔴🔴","🔴","⚪","🟢","🟢🟢"][mom_score + 2]
            print(f"  {ptag} [{symbol}] L@{entry:.5g}→{price:.5g} ({profit_pct*100:+.2f}%) | {pnl:+.3f}U | {hold_min:.1f}/{max_hold_now:.1f}m | {tsl} {tp} {mom_tag}")

        # ══════════════════════════════════════════════════
        #  SHORT MANAGEMENT
        # ══════════════════════════════════════════════════
        else:
            profit_pct = (entry - price) / entry

            # TP1 hit
            if not pos["tp1_hit"] and price <= pos["tp1"]:
                partial_close_tp1(symbol)
                continue

            # Trail activation SHORT — initial lebar, baru ketat saat price turun
            if not pos["trail_active"] and profit_pct >= TRAIL_ACTIVATE_PCT:
                pos["trail_active"] = True
                pos["sl"]       = round(entry * (1 - TRAIL_BE_PCT), 8)
                init_trail_mult = ATR_TRAIL_MULT * 1.5
                pos["trail_sl"] = price * (1 + atr * init_trail_mult / price)
                pos["peak"]     = price
                print(f"     🔓 [{symbol}] Trail AKTIF @ {profit_pct*100:+.2f}% (init TSL:{pos['trail_sl']:.5g})")

            # Phase 3
            if profit_pct >= TRAIL_TIGHT_PCT and pos["trail_phase"] < 3:
                pos["trail_phase"] = 3
                print(f"     ⬆️  [{symbol}] Phase3 trail ketat")

            # Update trail SL
            if pos["trail_active"] and price < pos["peak"]:
                pos["peak"] = price
                trail_mult  = ATR_TRAIL_TIGHT_MULT if pos["trail_phase"] >= 3 else ATR_TRAIL_MULT
                new_trail   = price * (1 + atr * trail_mult / price)
                pos["trail_sl"] = min(pos["trail_sl"], new_trail)

            # TP2
            if pos["tp1_hit"] and price <= pos["tp2"]:
                close_trade(symbol, "✨TP2")
                continue

            # Post-TP1 MomentumExit SHORT
            if (pos["tp1_hit"]
                    and mom_score <= -1
                    and time_ratio > 0.55
                    and profit_pct < pos.get("_peak_profit_pct", profit_pct) * 0.5):
                close_trade(symbol, f"🎯PostTP1MomExit(+{profit_pct*100:.2f}%)")
                continue

            if pos["tp1_hit"]:
                pos["_peak_profit_pct"] = max(pos.get("_peak_profit_pct", 0), profit_pct)

            # SmartCut SHORT
            time_ratio = hold_min / max_hold_now
            if not pos["tp1_hit"] and profit_pct < 0:
                cond_a = (profit_pct <= LOSS_BIG_THRESHOLD
                          and mom_score <= -1
                          and time_ratio > 0.50)
                cond_b = (profit_pct < -0.002
                          and mom_score == -2
                          and time_ratio > 0.75)
                if cond_a or cond_b:
                    close_trade(symbol, f"🧠SmartCut({profit_pct*100:.2f}%,t={time_ratio:.0%})")
                    continue

            # ── MomentumExit SHORT ────────────────────────────
            if (not pos["tp1_hit"]
                    and profit_pct >= MOM_EXIT_PROFIT_MIN
                    and time_ratio >= MOM_EXIT_TIME_MIN
                    and mom_score <= MOM_EXIT_SCORE_MAX):
                close_trade(symbol, f"🎯MomExit(+{profit_pct*100:.2f}%)")
                continue

            # ── StagnationExit SHORT ──────────────────────────
            stag_a = (hold_min >= STAG_CHECK_MIN
                      and abs(profit_pct) <= STAG_PROFIT_MAX
                      and mom_score <= STAG_MOM_MAX)
            if "profit_hwm" not in pos:
                pos["profit_hwm"]      = profit_pct
                pos["profit_hwm_time"] = hold_min
            elif profit_pct > pos["profit_hwm"]:
                pos["profit_hwm"]      = profit_pct
                pos["profit_hwm_time"] = hold_min
            stuck_duration = hold_min - pos["profit_hwm_time"]
            stag_b = (profit_pct > 0
                      and stuck_duration >= STAG_STUCK_MIN
                      and time_ratio >= STAG_STUCK_TIME_RATIO
                      and mom_score <= 0)
            if not pos["tp1_hit"] and (stag_a or stag_b):
                reason_tag = "A" if stag_a else f"B(stuck{stuck_duration:.1f}m)"
                close_trade(symbol, f"⏏️StagExit{reason_tag}({profit_pct*100:+.3f}%)")
                continue

            # Smart tighten trail SHORT
            if (pos["trail_active"]
                    and profit_pct > TRAIL_ACTIVATE_PCT
                    and mom_score <= -1
                    and pos["trail_phase"] < 3):
                pos["trail_phase"] = 3
                trail_mult = ATR_TRAIL_TIGHT_MULT
                new_trail  = price * (1 + atr * trail_mult / price)
                pos["trail_sl"] = min(pos["trail_sl"], new_trail)
                print(f"     ⚡ [{symbol}] Trail ketat ({mom_detail})")

            # Trail stop
            if pos["trail_active"] and price >= pos["trail_sl"]:
                tag = "🔒TrailBE" if pos.get("be_active") else "🔄TrailStop"
                close_trade(symbol, tag)
                continue

            # Hard SL
            if price >= pos["sl"]:
                close_trade(symbol, "🛑SL")
                continue

            # Status print
            pnl     = (entry - price) * pos.get("qty_remain", pos["qty"])
            phase   = pos.get("trail_phase", 1)
            act_tag = "✅" if pos["trail_active"] else "⏸️ "
            tsl     = f"TSL[{act_tag}P{phase}]:{pos['trail_sl']:.5g}"
            tp      = f"TP2:{pos['tp2']:.5g}" if pos["tp1_hit"] else f"TP1:{pos['tp1']:.5g}"
            ptag    = "📝" if pos.get("paper") else "📌"
            mom_tag = ["🔴🔴","🔴","⚪","🟢","🟢🟢"][mom_score + 2]
            print(f"  {ptag} [{symbol}] S@{entry:.5g}→{price:.5g} ({profit_pct*100:+.2f}%) | {pnl:+.3f}U | {hold_min:.1f}/{max_hold_now:.1f}m | {tsl} {tp} {mom_tag}")


# ════════════════════════════════════════════════════
#  PERFORMANCE ANALYTICS v15
# ════════════════════════════════════════════════════
def calc_expectancy():
    wins   = [t["pnl"] for t in trade_log if t["pnl"] > 0]
    losses = [t["pnl"] for t in trade_log if t["pnl"] < 0]
    if not wins and not losses: return 0.0
    wr     = len(wins) / (len(wins) + len(losses)) if (wins or losses) else 0
    avg_w  = sum(wins)   / len(wins)   if wins   else 0
    avg_l  = abs(sum(losses) / len(losses)) if losses else 0
    return round((wr * avg_w) - ((1 - wr) * avg_l), 5)


def calc_sharpe():
    pnls = list(_stats["pnl_history"])
    if len(pnls) < 5: return 0.0
    arr  = np.array(pnls)
    mean = float(np.mean(arr))
    std  = float(np.std(arr))
    if std == 0: return 0.0
    return round(mean / std, 3)


def calc_max_drawdown():
    pnls = list(_stats["pnl_history"])
    if len(pnls) < 2: return 0.0
    equity = np.cumsum(pnls)
    peak   = np.maximum.accumulate(equity)
    dd     = equity - peak
    return round(float(np.min(dd)), 4)


def print_stats_inline():
    n    = _stats["wins"] + _stats["losses"]
    wr   = _stats["wins"] / n * 100 if n else 0
    pnl  = _stats["total_pnl"]
    exp  = calc_expectancy()
    bar  = ("█" * _stats["wins"] + "░" * _stats["losses"])[-20:]
    emoji = "💚" if pnl >= 0 else "🔴"
    ptag = "[PAPER]" if PAPER_TRADING else ""
    print(f"     ┌─ 📊 {ptag} {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']} | {emoji}PnL:{pnl:+.4f}U | Exp:{exp:+.4f}U")
    print(f"     └─ TP1:{_stats['tp1_hits']} TP2:{_stats['tp2_hits']} SL:{_stats['sl_hits']} 🎯Mom:{_stats.get('mom_exits',0)} ⏏️Stag:{_stats.get('stag_exits',0)} 🧠:{_stats.get('smart_cuts',0)} ⚡Cut:{_stats['instant_cuts']} Force:{_stats['force_closes']} [{bar}]")


def print_stats():
    n    = _stats["wins"] + _stats["losses"]
    wr   = _stats["wins"] / n * 100 if n else 0
    sess = (time.time() - _stats["session_start"]) / 3600
    tph  = n / sess if sess > 0 else 0
    pnl  = _stats["total_pnl"]
    emoji = "💚" if pnl >= 0 else "🔴"
    exp  = calc_expectancy()
    sr   = calc_sharpe()
    mdd  = calc_max_drawdown()
    ks   = _kill_switch

    ptag = "⚠️  PAPER TRADING MODE — DATA UNTUK BACKTEST SAJA" if PAPER_TRADING else "🔴 LIVE TRADING"

    print(f"\n  {'─'*64}")
    print(f"  {ptag}")
    print(f"  📊 SESSION {sess*60:.0f}m | {tph:.1f} trades/jam | Rescans:{_stats['rescans']}")
    print(f"  🎯 {n} trades | WR:{wr:.0f}% | W:{_stats['wins']} L:{_stats['losses']}")
    print(f"  {emoji} Total P&L: {pnl:+.4f} USDT (virtual)")
    if PAPER_TRADING:
        print(f"  💰 Paper equity: {_paper_balance['equity']:.4f}U (modal {_paper_balance['initial_usdt']:.0f}U)")
    print(f"  📐 Expectancy:{exp:+.5f}U | Sharpe:{sr:.2f} | MaxDD:{mdd:.4f}U")
    print(f"  📈 Best:{_stats['best_trade']:+.4f}U │ 📉 Worst:{_stats['worst_trade']:+.4f}U")
    print(f"  🎯TP1:{_stats['tp1_hits']} ✨TP2:{_stats['tp2_hits']} 🛑SL:{_stats['sl_hits']} ⚡Cut:{_stats['instant_cuts']} 🧠Smart:{_stats.get('smart_cuts',0)} 🎯Mom:{_stats.get('mom_exits',0)} ⏏️Stag:{_stats.get('stag_exits',0)} ⏰Force:{_stats['force_closes']}")
    print(f"  🚫 Skip: Chop:{_stats['skipped_chop']} NoMom:{_stats['skipped_no_momentum']} MQ:{_stats['skipped_mq']} MeanRev:{_stats.get('skipped_mean_rev',0)}")
    print(f"       MTF:{_stats.get('skipped_mtf',0)} CandleAge:{_stats.get('skipped_candle_age',0)} Spread:{_stats['skipped_spread']} Blacklist:{_stats.get('skipped_blacklist',0)}")
    print(f"  🛡️  Kill switch: {'ACTIVE('+ks['reason']+')' if ks['active'] else 'OK'} | ConsecLoss:{ks['consec_losses']} | DailyPnL:{ks['daily_pnl']:+.2f}U | Lag:{ks['api_lag']*1000:.0f}ms")

    sym_sorted = sorted(_perf.items(), key=lambda x: x[1]["pnl"], reverse=True)
    if sym_sorted:
        print(f"  🏆 Top symbols:")
        for sym, data in sym_sorted[:5]:
            wr_s   = data["wins"] / data["trades"] * 100 if data["trades"] else 0
            cp     = _coin_perf[sym]
            prio   = "⭐" if is_coin_priority(sym) else "  "
            bl_tag = "🚫" if is_coin_blacklisted(sym) else "  "
            print(f"     {prio}{bl_tag}{sym:<14} {data['trades']}T WR:{wr_s:.0f}% PnL:{data['pnl']:+.4f}U SL-streak:{cp['sl_streak']} TP-streak:{cp['tp_streak']}")

    if _perf_regime:
        print(f"  📊 By regime:")
        for regime, data in _perf_regime.items():
            total_r = data["wins"] + data["losses"]
            wr_r    = data["wins"] / total_r * 100 if total_r else 0
            print(f"     {regime:<12} WR:{wr_r:.0f}% PnL:{data['pnl']:+.4f}U")

    if trade_log:
        print(f"  📋 Last 5:")
        for t in trade_log[-5:]:
            e    = "🟢" if t["pnl"] > 0 else "🔴"
            secs = t.get("hold_sec", 0)
            hold = f"{secs//60}m{secs%60}s"
            ptag = "[P]" if t.get("paper") else "   "
            print(f"     {e}{ptag} {t['symbol']:<14} {t['side']} {t['pnl']:+.4f}U ({hold}) — {t['reason'][:30]}")
    print(f"  {'─'*64}")


# ════════════════════════════════════════════════════
#  POSITION MONITOR THREAD
# ════════════════════════════════════════════════════
def position_monitor_thread():
    while True:
        try:
            if open_positions:
                manage_positions()
        except Exception as e:
            print(f"  ❌ Monitor error: {e}")
        time.sleep(POSITION_MONITOR_SEC)


# ════════════════════════════════════════════════════
#  MAIN LOOP — v15
# ════════════════════════════════════════════════════
def run_bot():
    mode_line = "PAPER TRADING (DRY RUN)" if PAPER_TRADING else "⚠️  LIVE TRADING"
    print("╔══════════════════════════════════════════════════════════════╗")
    print(f"║  🎯 BOT SCALPING v15.3 — SMART HOLD/CUT ENGINE              ║")
    print(f"║  Mode: {mode_line:<53}║")
    print("╠══════════════════════════════════════════════════════════════╣")
    print(f"║  Leverage:{LEVERAGE}x │ Per trade:${ORDER_USDT} │ Max posisi:{MAX_POSITIONS}              ║")
    print(f"║  SL:ATR×{ATR_SL_MULT} TP1:ATR×{ATR_TP1_MULT} TP2:ATR×{ATR_TP2_MULT}                    ║")
    print(f"║  Trail aktif > {TRAIL_ACTIVATE_PCT*100:.2f}% | Tight > {TRAIL_TIGHT_PCT*100:.2f}%                 ║")
    print(f"║  Smart Coin Selector: AKTIF                                 ║")
    print(f"║  Smart Hold/Cut:      AKTIF (momentum 1m realtime)          ║")
    print(f"║  Bear Market Mode:    AKTIF (block LONG breadth<{BEAR_MARKET_BREADTH*100:.0f}%)        ║")
    print(f"║  Multi-TF Alignment:  AKTIF ({MTF_MIN_AGREE}/3 TF harus agree)          ║")
    print(f"║  Candle age filter:   max {MAX_CANDLE_AGE_PCT*100:.0f}% dari candle           ║")
    print(f"║  Temp blacklist: {TEMP_BLACKLIST_SL} SL beruntun → {TEMP_BLACKLIST_MIN}m cooldown           ║")
    print("╚══════════════════════════════════════════════════════════════╝")

    if PAPER_TRADING:
        print(f"\n  ⚠️  PAPER TRADING AKTIF — tidak ada order ke exchange!")
        print(f"  Modal virtual: ${_paper_balance['initial_usdt']:.0f} USDT")

    print("\n  ⏳ Validasi symbols...")
    symbols_active = validate_symbols()
    print(f"  📊 {len(symbols_active)} symbols aktif")

    print(f"  📦 Pre-load symbol info...")
    with ThreadPoolExecutor(max_workers=15) as ex:
        list(ex.map(get_sym_info, symbols_active[:60]))

    print(f"  🌐 Refresh macro...")
    refresh_macro()
    update_btc_price()

    print(f"\n  ✅ BTC:{_macro['btc_trend_5m']} | Mode:{_macro['scalp_mode']} | F&G:{_macro['fng']}")
    print(f"  🚀 Start dalam 3 detik...\n")
    time.sleep(3)

    pm_thread = threading.Thread(target=position_monitor_thread, daemon=True)
    pm_thread.start()
    print("  🔧 Position monitor thread: START ✅")

    rs_thread = threading.Thread(target=instant_rescan_worker, args=(symbols_active,), daemon=True)
    rs_thread.start()
    print("  🔧 Re-scan thread: START ✅\n")

    global _scan_batch_idx
    cycle         = 0
    total_batches = math.ceil(len(symbols_active) / BATCH_SIZE)

    while True:
        cycle += 1
        refresh_macro()
        update_btc_price()

        if cycle % 30 == 0:
            check_api_latency()

        flash_dir, flash_pct = detect_flash_move()
        flash_info = f"⚡{flash_dir.upper()}:{flash_pct:.1f}%" if flash_dir != "none" else ""

        utc_h    = time.gmtime().tm_hour
        sess_tag = f"⚠️ JAM_JELEK(UTC{utc_h})" if utc_h in BAD_HOURS_UTC else ""

        print(f"\n{'═'*67}")
        print(f"  🔄 #{cycle} {time.strftime('%H:%M:%S')} {'[PAPER]' if PAPER_TRADING else ''} | F&G:{_macro['fng']} | "
              f"BTC1m:{_macro['btc_trend_1m']} 5m:{_macro['btc_trend_5m']} {flash_info} {sess_tag}")
        mq_now = get_market_quality()
        mq_tag = f"MQ:{mq_now}" + ("✅" if mq_now >= MQ_HIGH_QUALITY else "⚠️" if mq_now >= MQ_MIN_ENTRY else "🚫")
        print(f"  Mode:{_macro['scalp_mode']} | Breadth:{_macro['market_breadth']*100:.0f}% | {mq_tag} | "
              f"News:{_macro['news']} | Posisi({len(open_positions)}/{MAX_POSITIONS}): "
              f"{list(open_positions.keys()) or '—'}")
        if PAPER_TRADING:
            # Gunakan _stats["total_pnl"] sebagai single source of truth
            # _paper_balance["equity"] sekarang selalu sync (include TP1 partial)
            print(f"  💰 Paper equity: {_paper_balance['equity']:.4f}U | PnL: {_stats['total_pnl']:+.4f}U")

        slots_free = MAX_POSITIONS - len(open_positions)
        ks_active, ks_reason = check_kill_switch()

        if ks_active:
            resume_in = max(0, _kill_switch["resume_time"] - time.time())
            print(f"  🚨 KILL SWITCH AKTIF: {ks_reason} | Resume in: {resume_in/60:.1f}m")

        skip_reason = None
        if slots_free == 0:
            skip_reason = "posisi_penuh"
        elif _macro["news"] == "strong_negative":
            skip_reason = "bad_news"
        elif flash_dir != "none":
            skip_reason = f"flash_{flash_dir}"
        elif ks_active:
            skip_reason = f"kill:{ks_reason}"

        if not skip_reason:
            top_mv      = get_top_movers(symbols_active, n=40)
            # Filter top movers: skip yang di-blacklist, prioritaskan yang di priority list
            top_mv_syms = []
            for s, _, _ in top_mv:
                if s in open_positions: continue
                if is_coin_blacklisted(s): continue
                top_mv_syms.append(s)

            # Priority coins ke depan antrian
            priority_syms = [s for s in symbols_active
                             if is_coin_priority(s) and s not in open_positions
                             and not is_coin_blacklisted(s) and s not in top_mv_syms]

            batch_start   = _scan_batch_idx * BATCH_SIZE
            batch_regular = [s for s in symbols_active[batch_start:batch_start + BATCH_SIZE]
                             if s not in open_positions
                             and s not in top_mv_syms
                             and s not in priority_syms
                             and not is_coin_blacklisted(s)]
            _scan_batch_idx = (_scan_batch_idx + 1) % total_batches

            # Urutan: priority → top movers → regular
            scan_list = priority_syms[:5] + top_mv_syms[:15] + batch_regular[:10]

            top_display = [(s, pct) for s, pct, _ in top_mv[:5]]
            top_str     = " | ".join(f"{s}({pct:+.1f}%)" for s, pct in top_display)
            bl_count    = sum(1 for s in symbols_active if is_coin_blacklisted(s))
            pr_count    = sum(1 for s in symbols_active if is_coin_priority(s))
            print(f"  📊 TopMovers: {top_str}")
            print(f"  🔍 Scan {len(scan_list)} syms | Priority:{pr_count} Blacklist:{bl_count} | "
                  f"Chop:{_stats['skipped_chop']} MTF:{_stats.get('skipped_mtf',0)} CandleAge:{_stats.get('skipped_candle_age',0)}")

            try:
                candidates = scan_batch_parallel(scan_list)
            except Exception as e:
                print(f"  ❌ Scan loop error: {e}")
                candidates = []

            if candidates:
                # Sort menggunakan priority_score (smart ranking)
                candidates = sort_candidates_smart(candidates)
                print(f"  🎯 {len(candidates)} setup! Ambil top {min(len(candidates), slots_free)}")
                for sym, direction, info in candidates[:slots_free]:
                    if len(open_positions) >= MAX_POSITIONS: break
                    sig_str   = " | ".join(info.get("signals", [])[:3])
                    mom_str   = f"{info.get('mom_pct',0)*100:+.2f}%"
                    p24_str   = f"24h:{info.get('pct_24h',0):+.1f}%"
                    fr_str    = f"FR:{info.get('funding',0)*100:.3f}%"
                    prio_tag  = "⭐" if info.get("is_priority") else "  "
                    perf_str  = f"PerfBonus:{info.get('perf_score',0):+.1f}"
                    print(f"  {prio_tag} {sym} {direction} PScore:{info.get('priority_score',0):.0f} Mom:{mom_str} {p24_str} {fr_str} {perf_str}")
                    print(f"        {sig_str}")
                    open_trade(sym, direction, info)
            else:
                print(f"  ⏳ No setup found")
        else:
            print(f"  ⏸️  Skip: {skip_reason}")

        if cycle % 30 == 0:
            print_stats()

        print(f"  ⏱️  Next:{SCAN_INTERVAL}s | KS:{_kill_switch['consec_losses']}CL/{_kill_switch['daily_pnl']:+.2f}U | "
              f"Rescans:{_stats['rescans']} | Lag:{_kill_switch['api_lag']*1000:.0f}ms")
        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    run_bot()
