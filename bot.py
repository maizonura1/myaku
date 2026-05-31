"""
Bot Scalping v16 — CLAUDE AI MOMENTUM ENGINE 🤖  [INVERTED EDITION]
====================================================================

PERUBAHAN dari v16 original
────────────────────────────
✅ INVERT_DIRECTION = True  — eksekusi BERLAWANAN dari sinyal analisa
   • Analisa LONG  → eksekusi SHORT
   • Analisa SHORT → eksekusi LONG
✅ BTC filter dibalik sesuai logika invert
✅ AI bonus score disesuaikan dengan arah inverted
✅ Toggle mudah: ubah INVERT_DIRECTION = False untuk kembali normal

Semua fitur v16 lainnya tetap sama:
✅ FILTER USDC/BUSD     — hanya USDT pairs
✅ KILL SWITCH          — 8 loss beruntun
✅ CHOP FILTER          — threshold 62
✅ MIN_SCORE            — 35
✅ CLAUDE/GROQ API      — analisis coin
✅ MAX_HOLDING          — 6 menit
✅ SYMBOLS              — bersih dari duplikat dan USDC

PAPER TRADING = True  → hanya log, tidak masuk Binance
PAPER TRADING = False → live ke Binance
"""

import os, time, math, json, threading, queue, csv, re
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

# ══════════════════════════════════════════════════════
# ⚠️  PAPER TRADING MODE — SET False UNTUK LIVE ⚠️
PAPER_TRADING = True
# ══════════════════════════════════════════════════════

PAPER_LOG_FILE = "paper_trades_v16_inverted.csv"

# ════════════════════════════════════════════════════════
#  CONFIG v16 INVERTED
# ════════════════════════════════════════════════════════

# ── INVERT MODE ─────────────────────────────────────────
# True  = eksekusi BERLAWANAN dari sinyal (kontra-trend)
# False = eksekusi SAMA dengan sinyal (normal)
INVERT_DIRECTION = True
# ────────────────────────────────────────────────────────

LEVERAGE              = 20
ORDER_USDT            = 2
MAX_POSITIONS         = 3

ATR_SL_MULT           = 1.2
ATR_TP1_MULT          = 1.6
ATR_TP2_MULT          = 2.8
ATR_TRAIL_MULT        = 0.7
ATR_TRAIL_TIGHT_MULT  = 0.45

MIN_SL_PCT            = 0.0010
MAX_SL_PCT            = 0.0060
MIN_TP1_PCT           = 0.0018
MAX_TP2_PCT           = 0.0200

TRAIL_IMMEDIATE       = True
TRAIL_BE_PCT          = 0.0003
TRAIL_TIGHT_PCT       = 0.0025

TP1_CLOSE_RATIO       = 0.55
TP2_CLOSE_RATIO       = 0.45

INSTANT_CUT_MULT      = 0.5
INSTANT_CUT_WINDOW    = 3
INSTANT_CUT_MIN_PCT   = 0.0003

CHOP_INDEX_THRESHOLD  = 62.0
MIN_BB_WIDTH_PCT      = 0.003
MAX_EMA_CROSS_FREQ    = 4
MIN_ADX               = 15

MIN_MOMENTUM_PCT      = 0.0010
MIN_VOL_SURGE         = 1.2
MIN_TREND_CANDLES     = 2

SCAN_INTERVAL         = 2
POSITION_MONITOR_SEC  = 0.5
SCAN_DELAY_MS         = 0.025
BATCH_SIZE            = 25
MAX_HOLDING_MIN       = 6
SYMBOL_COOLDOWN_SEC   = 10
RE_SCAN_DELAY_SEC     = 0.3
ALWAYS_FILL_SLOTS     = True

# AI Config — Groq
AI_SCAN_INTERVAL      = 12
AI_TOP_PICKS          = 10
GROQ_API_KEY          = os.getenv("GROQ_API_KEY", "")
AI_MODEL              = "llama-3.3-70b-versatile"

# Session filter
BAD_HOURS_UTC         = {4, 5, 6}
BAD_HOURS_MIN_SCORE   = 50

# Kill switch
DAILY_LOSS_LIMIT      = -8.0
CONSEC_LOSS_MAX       = 8
CONSEC_LOSS_PAUSE_MIN = 10
MAX_API_LAG_SEC       = 3.0

# Cache TTL
OHLCV_CACHE_TTL_1M    = 2
OHLCV_CACHE_TTL_3M    = 4
OHLCV_CACHE_TTL_5M    = 5
OHLCV_CACHE_TTL_15M   = 30
OHLCV_CACHE_TTL_1H    = 1800
TICKER24H_TTL         = 6
FUNDING_TTL           = 30
TOP_MOVERS_TTL        = 6

# Filter entry
MIN_SCORE             = 35
MIN_ENTRY_SIGNALS     = 2
MIN_FNG               = 10
MAX_FNG_LONG          = 95
MAX_SL_ATR_PCT        = 0.012
MAX_SPREAD_RATIO      = 0.40

# ── SYMBOLS — hanya USDT, bersih dari duplikat dan USDC ──────
_RAW_SYMBOLS = [
    # Mega caps
    "BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT",
    "ADAUSDT","DOGEUSDT","AVAXUSDT","LINKUSDT","DOTUSDT",
    "MATICUSDT","LTCUSDT","ATOMUSDT","UNIUSDT","ETCUSDT",
    # Layer 2 & alt L1
    "NEARUSDT","APTUSDT","ARBUSDT","OPUSDT","INJUSDT",
    "SUIUSDT","TIAUSDT","STXUSDT","KASUSDT","TONUSDT",
    "TAOUSDT","ONDOUSDT","SEIUSDT","EIGENUSDT",
    # DeFi
    "AAVEUSDT","RUNEUSDT","CRVUSDT","MKRUSDT","COMPUSDT",
    "SUSHIUSDT","SNXUSDT","1INCHUSDT","BALUSDT","DYDXUSDT",
    "GMXUSDT","PENDLEUSDT","JTOUSDT","RAYUSDT","JUPUSDT",
    # AI & Data
    "FETUSDT","RENDERUSDT","WLDUSDT","OCEANUSDT","AGIXUSDT",
    "NMRUSDT","PHBUSDT","ARKMUSDT",
    # Meme
    "1000PEPEUSDT","WIFUSDT","1000BONKUSDT","SHIBUSDT",
    "FLOKIUSDT","MEMEUSDT","BOMEUSDT","DOGSUSDT",
    # Gaming & Metaverse
    "AXSUSDT","SANDUSDT","MANAUSDT","ENJUSDT","GALAUSDT",
    "IMXUSDT","BEAMXUSDT","ORDIUSDT","PIXELUSDT",
    # Infrastructure
    "FILUSDT","ARUSDT","STRKUSDT","ALTUSDT","DYMUSDT",
    "MANTAUSDT","ZETAUSDT","RONINUSDT","NOTUSDT","CATIUSDT",
    "PYTHUSDT","PORTALUSDT","XAIUSDT","VANRYUSDT",
    # Layer 1 alt
    "ALGOUSDT","ICPUSDT","FTMUSDT","HBARUSDT","FLOWUSDT",
    "EGLDUSDT","THETAUSDT","KAVAUSDT","BANDUSDT",
    "SKLUSDT","CELRUSDT","CTSIUSDT",
    # DeFi 2
    "BLURUSDT","MASKUSDT","HIGHUSDT",
    "PERPUSDT","LITUSDT","UNFIUSDT",
    # Misc
    "TRUUSDT","BLZUSDT","JASMYUSDT","CFXUSDT",
    "COMBOUSDT","AGLDUSDT","IDUSDT",
    "ENARUSDT","WUSDT","CKBUSDT",
    "HOOKUSDT","GLMRUSDT","AMBUSDT","RENUSDT",
    "DENTUSDT","HOTUSDT","IOSTUSDT","OGNUSDT",
    "LINAUSDT","SFPUSDT","BNTUSDT","FLMUSDT","TLMUSDT",
    "1000XECUSDT","ACEUSDT","JOEUSDT",
    # New listings 2024-2025
    "IOUSDT","ZKUSDT","LISTAUSDT","ZROUSDT","MYROUSDT",
    "NEIROUSDT","HMSTRUSDT","SCRUSDT","LUMIAUSDT",
    "COWUSDT","MOODENGUSDT","POPCATUSDT","ACTUSDT","PNUTUSDT",
    "KAIAUSDT","ORCAUSDT","MOVEUSDT","MEUSDT",
    "AIXBTUSDT","SONICUSDT","VIRTUALUSDT",
    "BIOUSDT","CGPTUSDT","ARCUSDT",
    "TRUMPUSDT","MELANIAUSDT",
    # Futures populer lainnya
    "DRIFTUSDT","PHAUSDT","STXUSDT","JUPUSDT",
    "WIFUSDT","1000BONKUSDT","TAOUSDT",
    "ETHFIUSDT","REZUSDT","BBUSDT",
]

def _clean_symbols(raw):
    seen = set()
    result = []
    for s in raw:
        s = s.upper().strip()
        if not s.endswith("USDT"):
            continue
        if s in seen:
            continue
        seen.add(s)
        result.append(s)
    return result

SYMBOLS = _clean_symbols(_RAW_SYMBOLS)

BULL_TRENDS = {"BULL", "MILD_BULL"}
BEAR_TRENDS = {"BEAR", "MILD_BEAR"}

# ════════════════════════════════════════════════════════
#  OUTPUT LOCK
# ════════════════════════════════════════════════════════
_print_lock = threading.Lock()

def safe_print(msg):
    with _print_lock:
        print(msg)


# ════════════════════════════════════════════════════════
#  HELPER INVERT DIRECTION
# ════════════════════════════════════════════════════════
def invert_dir(direction):
    """Membalik arah: LONG → SHORT, SHORT → LONG."""
    if direction == "LONG":
        return "SHORT"
    if direction == "SHORT":
        return "LONG"
    return direction


# ════════════════════════════════════════════════════════
#  STATE GLOBAL
# ════════════════════════════════════════════════════════
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
_hot_symbols        = deque(maxlen=50)
_ticker24h_cache    = {}
_ticker24h_ts       = 0
_funding_cache      = {}
_funding_ts         = 0
_top_movers         = []
_top_movers_ts      = 0
_ai_picks           = []
_ai_picks_ts        = 0
_ai_cycle_counter   = 0

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

_perf        = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0, "trades": 0})

_macro = {
    "fng": 50, "fng_label": "Neutral",
    "btc_trend_5m":  "UNKNOWN",
    "btc_trend_15m": "UNKNOWN",
    "btc_trend_1h":  "UNKNOWN",
    "market_breadth": 0.5,
    "news": "neutral",
    "scalp_mode": "TREND",
    "last_fng": 0, "last_btc": 0, "last_breadth": 0,
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
    "rescans": 0,
    "ai_scans": 0,
    "skipped_no_momentum": 0,
    "skipped_chop": 0,
    "skipped_spread": 0,
    "pnl_history": deque(maxlen=200),
    "session_start": time.time(),
}

_paper = {
    "balance":  1000.0,
    "trades":   [],
}


# ════════════════════════════════════════════════════════
#  PAPER TRADING ENGINE
# ════════════════════════════════════════════════════════
def paper_log_trade(symbol, side, action, qty, price, pnl=0.0, reason=""):
    row = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "symbol":    symbol,
        "side":      side,
        "action":    action,
        "qty":       qty,
        "price":     price,
        "pnl":       round(pnl, 4),
        "reason":    reason,
        "balance":   round(_paper["balance"], 2),
    }
    file_exists = os.path.isfile(PAPER_LOG_FILE)
    with open(PAPER_LOG_FILE, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=row.keys())
        if not file_exists:
            w.writeheader()
        w.writerow(row)

def paper_open_order(symbol, direction, qty, price):
    margin_used = (price * qty) / LEVERAGE
    _paper["balance"] -= margin_used
    paper_log_trade(symbol, direction, "OPEN", qty, price, reason="entry")
    safe_print(f"  📋 [PAPER] {symbol} {direction} OPEN @{price:.6g} qty={qty:.4f} margin=${margin_used:.2f}")
    return True

def paper_close_order(symbol, side, qty, entry, exit_price, reason=""):
    if side == "LONG":
        pnl = (exit_price - entry) * qty
    else:
        pnl = (entry - exit_price) * qty
    margin_returned = (entry * qty) / LEVERAGE
    _paper["balance"] += margin_returned + pnl
    paper_log_trade(symbol, side, "CLOSE", qty, exit_price, pnl=pnl, reason=reason)
    emoji = "🟢" if pnl >= 0 else "🔴"
    safe_print(f"  📋 [PAPER] {symbol} {side} CLOSE @{exit_price:.6g} PnL:{pnl:+.4f}U {emoji} [{reason}]")
    return pnl


# ════════════════════════════════════════════════════════
#  GROQ AI SCANNER
# ════════════════════════════════════════════════════════
def ask_groq_for_best_coins(tickers_data, macro_data):
    global _ai_picks, _ai_picks_ts

    if not GROQ_API_KEY:
        return []

    top_by_vol = sorted(
        [(sym, d) for sym, d in tickers_data.items()
         if d["vol24h"] > 1_500_000 and sym.endswith("USDT")],
        key=lambda x: abs(x[1]["pct"]),
        reverse=True
    )[:45]

    coin_summary = []
    for sym, d in top_by_vol:
        coin_summary.append(
            f"{sym}: {d['pct']:+.1f}% vol=${d['vol24h']/1e6:.1f}M p={d['price']}"
        )

    # Kalau invert aktif, kita minta AI rekomendasi normal,
    # lalu kita balik sendiri saat eksekusi
    prompt = f"""Expert crypto futures scalping analyst. Pick TOP 10 coins for scalping NEXT 5-10 MINUTES.

Market NOW:
- BTC 5m: {macro_data['btc_trend_5m']} | 15m: {macro_data['btc_trend_15m']}
- Fear & Greed: {macro_data['fng']} ({macro_data['fng_label']})
- Breadth: {macro_data['market_breadth']*100:.0f}% coins above EMA9
- Mode: {macro_data['scalp_mode']}

Top movers (vol filtered, USDT only):
{chr(10).join(coin_summary[:35])}

Criteria:
- Strong momentum + high volume surge
- BTC BULL → LONG preferred, BTC BEAR → SHORT preferred
- Avoid >15% 24h move already done
- Only USDT pairs

Return ONLY valid JSON array, no markdown, no text:
[{{"symbol":"SOLUSDT","direction":"LONG","reason":"..."}},...]
Exactly 10 items."""

    try:
        resp = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": AI_MODEL,
                "max_tokens": 900,
                "temperature": 0.25,
                "messages": [
                    {"role": "system",
                     "content": "Crypto scalping analyst. Output ONLY valid JSON array. No markdown. No explanation."},
                    {"role": "user", "content": prompt}
                ]
            },
            timeout=12
        )
        if resp.status_code == 200:
            data = resp.json()
            text = data["choices"][0]["message"]["content"].strip()
            text = re.sub(r"```json|```", "", text).strip()
            start = text.find("[")
            end   = text.rfind("]") + 1
            if start != -1 and end > start:
                text = text[start:end]
            picks = json.loads(text)
            picks = [p for p in picks if str(p.get("symbol","")).upper().endswith("USDT")]
            _ai_picks    = picks[:10]
            _ai_picks_ts = time.time()
            _stats["ai_scans"] += 1
            syms = [(p["symbol"], p.get("direction","?")) for p in _ai_picks[:5]]
            inv_tag = " [INVERTED]" if INVERT_DIRECTION else ""
            safe_print(f"\n  🤖 Groq AI picks{inv_tag}: {' | '.join(f'{s}({d})' for s,d in syms)}")
            return _ai_picks
        else:
            safe_print(f"  ⚠️ Groq {resp.status_code}: {resp.text[:80]}")
            return _ai_picks
    except json.JSONDecodeError as e:
        safe_print(f"  ⚠️ Groq JSON err: {e}")
        return _ai_picks
    except Exception as e:
        safe_print(f"  ⚠️ Groq err: {e}")
        return _ai_picks


def get_ai_direction_hint(symbol):
    """
    Mengembalikan arah AI hint.
    Catatan: hint ini adalah arah ANALISA (sebelum invert).
    Pembalikan dilakukan di get_entry_score dan should_enter.
    """
    for pick in _ai_picks:
        if pick.get("symbol") == symbol:
            return pick.get("direction"), pick.get("reason", "")
    return None, ""


# ════════════════════════════════════════════════════════
#  KILL SWITCH
# ════════════════════════════════════════════════════════
def check_kill_switch():
    ks  = _kill_switch
    now = time.time()

    if ks["active"] and now >= ks["resume_time"]:
        ks["active"]       = False
        ks["reason"]       = ""
        ks["consec_losses"] = 0
        safe_print(f"\n  ✅ Kill switch CLEAR — bot aktif kembali")

    if ks["active"]:
        return True, ks["reason"]

    day_start = now - (now % 86400)
    if day_start > ks["daily_reset_ts"]:
        ks["daily_pnl"]      = 0.0
        ks["daily_reset_ts"] = day_start

    if ks["daily_pnl"] <= DAILY_LOSS_LIMIT:
        ks["active"]      = True
        ks["reason"]      = f"daily_loss({ks['daily_pnl']:.2f})"
        ks["resume_time"] = day_start + 86400
        return True, ks["reason"]

    if ks["consec_losses"] >= CONSEC_LOSS_MAX:
        ks["active"]      = True
        ks["reason"]      = f"consec_loss({ks['consec_losses']})"
        ks["resume_time"] = now + (CONSEC_LOSS_PAUSE_MIN * 60)
        safe_print(f"\n  🚨 KILL SWITCH: {ks['consec_losses']} loss beruntun — pause {CONSEC_LOSS_PAUSE_MIN}m")
        return True, ks["reason"]

    return False, ""


def update_kill_switch_after_trade(pnl):
    ks = _kill_switch
    ks["daily_pnl"] += pnl
    if pnl < 0:
        ks["consec_losses"] += 1
    else:
        ks["consec_losses"] = 0


def check_api_latency():
    try:
        t0  = time.time()
        client.futures_ping()
        lag = time.time() - t0
        _kill_switch["api_lag"] = lag
        return lag <= MAX_API_LAG_SEC
    except:
        return False


# ════════════════════════════════════════════════════════
#  CHOP FILTER
# ════════════════════════════════════════════════════════
def calc_choppiness_index(df, period=14):
    if df is None or len(df) < period + 2:
        return 50.0
    try:
        high  = df["high"].values
        low   = df["low"].values
        close = df["close"].values
        tr_sum = 0.0
        for i in range(-period, 0):
            tr = max(high[i]-low[i], abs(high[i]-close[i-1]), abs(low[i]-close[i-1]))
            tr_sum += tr
        rng = max(high[-period:]) - min(low[-period:])
        if rng == 0 or tr_sum == 0:
            return 50.0
        return round(100 * math.log10(tr_sum / rng) / math.log10(period), 2)
    except:
        return 50.0


def is_chop_market(df_5m):
    if df_5m is None or len(df_5m) < 20:
        return False, "no_data"
    reasons = []
    ci = calc_choppiness_index(df_5m, 14)
    if ci > CHOP_INDEX_THRESHOLD:
        reasons.append(f"CI={ci:.1f}")
    last     = df_5m.iloc[-1]
    bb_width = last.get("bb_width", 0.01)
    if bb_width < MIN_BB_WIDTH_PCT:
        reasons.append("BB_narrow")
    hist   = df_5m["macd_hist"].values[-10:]
    h_std  = float(np.std(hist)) if len(hist) >= 5 else 0.001
    if h_std < 0.000008:
        reasons.append("MACD_flat")
    is_chop = len(reasons) >= 2
    return is_chop, "|".join(reasons) if reasons else "ok"


# ════════════════════════════════════════════════════════
#  UTILS
# ════════════════════════════════════════════════════════
def get_sym_info(symbol):
    if symbol in _sym_info:
        return _sym_info[symbol]
    try:
        for s in client.futures_exchange_info()["symbols"]:
            if s["symbol"] == symbol:
                for f in s["filters"]:
                    if f["filterType"] == "LOT_SIZE":
                        _sym_info[symbol] = {
                            "step":   float(f["stepSize"]),
                            "minQty": float(f["minQty"])
                        }
                        return _sym_info[symbol]
    except:
        pass
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
        return
    try:
        client.futures_change_leverage(symbol=symbol, leverage=LEVERAGE)
    except:
        pass


def get_price(symbol):
    try:
        return float(client.futures_symbol_ticker(symbol=symbol)["price"])
    except:
        return 0.0


def get_exchange_amt(symbol):
    if PAPER_TRADING:
        pos = open_positions.get(symbol)
        if pos and not pos.get("_reserved"):
            amt = pos.get("qty_remain", pos.get("qty", 0))
            return amt if pos["side"] == "LONG" else -amt
        return 0
    try:
        for p in client.futures_position_information(symbol=symbol):
            amt = float(p["positionAmt"])
            if amt != 0:
                return amt
        return 0
    except:
        return None


def is_symbol_cooling_down(symbol):
    if symbol not in _sym_cooldown:
        return False
    return (time.time() - _sym_cooldown[symbol]) < SYMBOL_COOLDOWN_SEC


def set_symbol_cooldown(symbol):
    _sym_cooldown[symbol] = time.time()


def validate_symbols():
    try:
        valid = {s["symbol"] for s in client.futures_exchange_info()["symbols"]
                 if s["status"] == "TRADING" and s["symbol"].endswith("USDT")}
        result = list(dict.fromkeys([s for s in SYMBOLS if s in valid]))
        safe_print(f"  ✅ {len(result)}/{len(SYMBOLS)} symbols valid (USDT only)")
        return result
    except:
        return list(dict.fromkeys(SYMBOLS))


# ════════════════════════════════════════════════════════
#  DATA SOURCES
# ════════════════════════════════════════════════════════
def fetch_ticker24h_all():
    global _ticker24h_cache, _ticker24h_ts
    now = time.time()
    if now - _ticker24h_ts < TICKER24H_TTL and _ticker24h_cache:
        return _ticker24h_cache
    try:
        tickers   = client.futures_ticker()
        new_cache = {}
        for t in tickers:
            sym = t["symbol"]
            if not sym.endswith("USDT"):
                continue
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
        premium   = client.futures_mark_price()
        new_cache = {}
        for p in premium:
            sym = p["symbol"]
            if not sym.endswith("USDT"):
                continue
            fr = float(p.get("lastFundingRate", 0))
            new_cache[sym] = fr
        _funding_cache = new_cache
        _funding_ts    = now
        return new_cache
    except:
        return _funding_cache


def get_top_movers(symbols_active, n=40):
    global _top_movers, _top_movers_ts
    now = time.time()
    if now - _top_movers_ts < TOP_MOVERS_TTL and _top_movers:
        return _top_movers
    try:
        tickers    = fetch_ticker24h_all()
        active_set = set(symbols_active)
        movers     = []
        for sym, data in tickers.items():
            if sym not in active_set:
                continue
            if not sym.endswith("USDT"):
                continue
            pct = data["pct"]
            vol = data["vol24h"]
            if vol < 800_000:
                continue
            movers.append((sym, pct, vol))
        movers.sort(key=lambda x: abs(x[1]), reverse=True)
        result = [(sym, pct, "LONG" if pct > 0 else "SHORT") for sym, pct, vol in movers[:n]]
        _top_movers    = result
        _top_movers_ts = now
        return result
    except:
        return _top_movers


def get_funding_bias(symbol):
    rates = fetch_funding_rates()
    fr    = rates.get(symbol, 0)
    if fr > 0.0005:  return "bearish_bias", fr
    if fr < -0.0005: return "bullish_bias", fr
    return "neutral", fr


# ════════════════════════════════════════════════════════
#  OHLCV CACHE
# ════════════════════════════════════════════════════════
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


# ════════════════════════════════════════════════════════
#  TECHNICAL ANALYSIS
# ════════════════════════════════════════════════════════
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
    df["mom5"]      = (c - c.shift(5)) / c.shift(5)
    df["mom3"]      = (c - c.shift(3)) / c.shift(3)
    return df


def _calc_trend(df):
    if df is None or len(df) < 25:
        return "UNKNOWN"
    c     = df["close"]
    price = c.iloc[-1]
    ema9  = ta.trend.EMAIndicator(c, 9).ema_indicator().iloc[-1]
    ema21 = ta.trend.EMAIndicator(c, 21).ema_indicator().iloc[-1]
    ema50 = ta.trend.EMAIndicator(c, 50).ema_indicator().iloc[-1]
    chg   = (price - c.iloc[-4]) / c.iloc[-4] * 100
    if price > ema9 > ema21 > ema50 and chg > 0:    return "BULL"
    elif price < ema9 < ema21 < ema50 and chg < 0:  return "BEAR"
    elif price > ema21 and chg > -0.2:              return "MILD_BULL"
    elif price < ema21 and chg < 0.2:               return "MILD_BEAR"
    return "SIDEWAYS"


# ════════════════════════════════════════════════════════
#  ATR LEVELS
# ════════════════════════════════════════════════════════
def calc_atr_levels(entry, atr, direction):
    raw_sl  = atr * ATR_SL_MULT
    raw_tp1 = atr * ATR_TP1_MULT
    raw_tp2 = atr * ATR_TP2_MULT
    raw_ic  = atr * INSTANT_CUT_MULT

    sl_dist  = max(entry * MIN_SL_PCT, min(raw_sl,  entry * MAX_SL_PCT))
    tp1_dist = max(entry * MIN_TP1_PCT, raw_tp1)
    tp2_dist = min(entry * MAX_TP2_PCT, raw_tp2)
    tp2_dist = max(tp2_dist, tp1_dist * 1.5)

    if direction == "LONG":
        sl          = round(entry - sl_dist,  8)
        tp1         = round(entry + tp1_dist, 8)
        tp2         = round(entry + tp2_dist, 8)
        instant_cut = round(entry - raw_ic, 8)
        trail_sl    = round(entry - atr * ATR_TRAIL_MULT, 8)
        trail_sl    = max(trail_sl, sl)
    else:
        sl          = round(entry + sl_dist,  8)
        tp1         = round(entry - tp1_dist, 8)
        tp2         = round(entry - tp2_dist, 8)
        instant_cut = round(entry + raw_ic, 8)
        trail_sl    = round(entry + atr * ATR_TRAIL_MULT, 8)
        trail_sl    = min(trail_sl, sl)

    return {
        "sl":          sl,
        "tp1":         tp1,
        "tp2":         tp2,
        "instant_cut": instant_cut,
        "trail_sl":    trail_sl,
        "sl_pct":      sl_dist  / entry,
        "tp1_pct":     tp1_dist / entry,
        "tp2_pct":     tp2_dist / entry,
        "atr":         atr,
        "atr_pct":     atr / entry,
    }


# ════════════════════════════════════════════════════════
#  MOMENTUM CHECK
# ════════════════════════════════════════════════════════
def check_momentum_strength(df, direction):
    """
    Cek momentum untuk arah yang akan DIEKSEKUSI (sudah setelah invert).
    Kalau INVERT_DIRECTION = True dan direction = SHORT (hasil invert dari LONG),
    kita tetap validasi momentum SHORT, bukan LONG.
    """
    if df is None or len(df) < 10:
        return False, 0, "no_data"

    last    = df.iloc[-1]
    recent  = df.iloc[-6:-1]
    p_now   = last["close"]
    p_5ago  = df.iloc[-6]["close"]
    mom_pct = (p_now - p_5ago) / p_5ago

    if direction == "LONG"  and mom_pct < MIN_MOMENTUM_PCT:
        return False, mom_pct, f"mom_weak({mom_pct*100:.2f}%)"
    if direction == "SHORT" and mom_pct > -MIN_MOMENTUM_PCT:
        return False, mom_pct, f"mom_weak({mom_pct*100:.2f}%)"

    vol_ratio = last["vol_ratio"]
    if vol_ratio < MIN_VOL_SURGE:
        return False, mom_pct, f"vol_low({vol_ratio:.1f}x)"

    if direction == "LONG":
        bullish = sum(1 for _, row in recent.iterrows() if row["close"] > row["open"])
        if bullish < MIN_TREND_CANDLES:
            return False, mom_pct, f"candles_weak({bullish}/5)"
    else:
        bearish = sum(1 for _, row in recent.iterrows() if row["close"] < row["open"])
        if bearish < MIN_TREND_CANDLES:
            return False, mom_pct, f"candles_weak({bearish}/5)"

    return True, mom_pct, f"mom={mom_pct*100:+.2f}% vol={vol_ratio:.1f}x"


# ════════════════════════════════════════════════════════
#  MACRO REFRESH
# ════════════════════════════════════════════════════════
def refresh_macro():
    now = time.time()
    if now - _macro["last_fng"] > 300:
        try:
            d = requests.get("https://api.alternative.me/fng/?limit=1", timeout=5).json()["data"][0]
            _macro["fng"]       = int(d["value"])
            _macro["fng_label"] = d["value_classification"]
            _macro["last_fng"]  = now
        except:
            pass

    if now - _macro["last_btc"] > 5:
        try:
            df_5m  = get_ohlcv("BTCUSDT", Client.KLINE_INTERVAL_5MINUTE, 60)
            df_15m = get_ohlcv("BTCUSDT", Client.KLINE_INTERVAL_15MINUTE, 60)
            _macro["btc_trend_5m"]  = _calc_trend(df_5m)
            _macro["btc_trend_15m"] = _calc_trend(df_15m)
            _macro["last_btc"]      = now
            t5  = _macro["btc_trend_5m"]
            t15 = _macro["btc_trend_15m"]
            _macro["scalp_mode"] = "TREND" if t15 in ("BULL","BEAR") or t5 in ("BULL","BEAR") else "RANGE"
        except:
            pass

    if now - _macro["last_breadth"] > 30:
        try:
            bullish = 0
            sample  = SYMBOLS[:20]
            for sym in sample:
                df = get_ohlcv(sym, Client.KLINE_INTERVAL_5MINUTE, 10)
                if df is not None and len(df) >= 5:
                    c  = df["close"]
                    e9 = ta.trend.EMAIndicator(c, 9).ema_indicator().iloc[-1]
                    if c.iloc[-1] > e9:
                        bullish += 1
            _macro["market_breadth"] = bullish / len(sample)
            _macro["last_breadth"]   = now
        except:
            pass


def update_btc_price():
    try:
        px = get_price("BTCUSDT")
        if px > 0:
            _btc_price_history.append((time.time(), px))
    except:
        pass


def detect_flash_move():
    if len(_btc_price_history) < 2:
        return "none", 0.0
    cutoff  = time.time() - 90
    oldest  = next((px for ts, px in _btc_price_history if ts >= cutoff), None)
    if oldest is None:
        return "none", 0.0
    current = _btc_price_history[-1][1]
    pct = (current - oldest) / oldest * 100
    if pct <= -1.2: return "crash", abs(pct)
    if pct >= 1.2:  return "pump",  abs(pct)
    return "none", 0.0


# ════════════════════════════════════════════════════════
#  ORDER BOOK IMBALANCE
# ════════════════════════════════════════════════════════
def get_ob_imbalance(symbol):
    try:
        ob    = client.futures_order_book(symbol=symbol, limit=50)
        bid_w = sum(float(b[1]) * (1/(i+1)) for i, b in enumerate(ob["bids"][:20]))
        ask_w = sum(float(a[1]) * (1/(i+1)) for i, a in enumerate(ob["asks"][:20]))
        total = bid_w + ask_w
        return round((bid_w - ask_w) / total, 3) if total else 0.0
    except:
        return 0.0


# ════════════════════════════════════════════════════════
#  ENTRY SCORE ENGINE v16 INVERTED
# ════════════════════════════════════════════════════════
def get_entry_score(symbol, df_5m, direction, ai_hint_direction=None):
    """
    Menghitung score untuk arah EKSEKUSI (sudah setelah invert).
    ai_hint_direction = arah dari AI (sebelum invert).
    Kalau INVERT_DIRECTION aktif, AI hint LONG cocok dengan eksekusi SHORT.
    """
    if df_5m is None or len(df_5m) < 30:
        return 0, []

    last  = df_5m.iloc[-1]
    prev  = df_5m.iloc[-2]
    prev2 = df_5m.iloc[-3]
    score = 0
    sigs  = []

    # ── AI BONUS — sesuaikan dengan invert ───────────────
    # Kalau invert aktif: AI LONG → kita eksekusi SHORT, jadi cocok saat direction=SHORT
    effective_ai = invert_dir(ai_hint_direction) if INVERT_DIRECTION and ai_hint_direction else ai_hint_direction
    if effective_ai == direction:
        score += 15
        inv_tag = "↩️" if INVERT_DIRECTION else ""
        sigs.append(f"🤖AI{inv_tag}={direction}")

    # ── A: TREND (max 25) ────────────────────────────────
    e3, e5, e9, e21 = last["ema3"], last["ema5"], last["ema9"], last["ema21"]
    p = last["close"]
    if direction == "LONG":
        if p > e3 > e5 > e9 > e21:
            score += 25; sigs.append("📐EMA_STACK↑")
        elif p > e5 > e9 > e21:
            score += 18; sigs.append("📐EMA↑")
        elif p > e9 > e21:
            score += 12; sigs.append("📐EMA_align↑")
        elif p > e9:
            score += 6;  sigs.append("📐E9↑")
    else:
        if p < e3 < e5 < e9 < e21:
            score += 25; sigs.append("📐EMA_STACK↓")
        elif p < e5 < e9 < e21:
            score += 18; sigs.append("📐EMA↓")
        elif p < e9 < e21:
            score += 12; sigs.append("📐EMA_align↓")
        elif p < e9:
            score += 6;  sigs.append("📐E9↓")

    # ── B: MOMENTUM/VOLATILITY (max 25) ──────────────────
    mom5    = abs(last.get("mom5", 0))
    vol_rat = last["vol_ratio"]
    atr_now = last["atr"]
    atr_p   = df_5m.iloc[-6]["atr"] if len(df_5m) > 6 else atr_now
    atr_exp = atr_now > atr_p * 1.1

    if mom5 >= 0.005 and atr_exp:
        score += 25; sigs.append(f"🚀Mom{mom5*100:.1f}%+ATR↑")
    elif mom5 >= 0.003 and vol_rat >= 1.8:
        score += 20; sigs.append(f"📈Mom{mom5*100:.1f}%+Vol{vol_rat:.1f}x")
    elif mom5 >= 0.0015:
        score += 13; sigs.append(f"📈Mom{mom5*100:.1f}%")
    elif vol_rat >= 2.5:
        score += 13; sigs.append(f"🔥Vol{vol_rat:.1f}x")
    elif vol_rat >= 1.5:
        score += 7

    # ── C: ORDER FLOW (max 25) ───────────────────────────
    h_now, h_prev, h_p2 = last["macd_hist"], prev["macd_hist"], prev2["macd_hist"]
    br = last["buy_ratio"]
    if direction == "LONG":
        if h_now > 0 and h_now > h_prev > h_p2 and br > 0.53:
            score += 25; sigs.append(f"✅MACD↑↑Buy{br:.0%}")
        elif h_now > 0 and h_now > h_prev:
            score += 17; sigs.append("✅MACD↑")
        elif h_prev < 0 and h_now >= 0:
            score += 20; sigs.append("⚡MACD_X↑")
        elif br > 0.58:
            score += 10; sigs.append(f"💧Buy{br:.0%}")
    else:
        if h_now < 0 and h_now < h_prev < h_p2 and br < 0.47:
            score += 25; sigs.append(f"✅MACD↓↓Sel{1-br:.0%}")
        elif h_now < 0 and h_now < h_prev:
            score += 17; sigs.append("✅MACD↓")
        elif h_prev > 0 and h_now <= 0:
            score += 20; sigs.append("⚡MACD_X↓")
        elif br < 0.42:
            score += 10; sigs.append(f"💧Sel{1-br:.0%}")

    # ── D: STRUCTURE (max 25) ────────────────────────────
    recent_hi = df_5m.iloc[-6:-1]["high"].max()
    recent_lo = df_5m.iloc[-6:-1]["low"].min()
    if direction == "LONG":
        if p > recent_hi and last["body_ratio"] > 0.55 and last["vol_ratio"] > 1.4:
            score += 25; sigs.append("🚀BreakBull")
        elif last["close"] > last["open"] and last["close"] > prev["high"] and last["body_ratio"] > 0.55:
            score += 20; sigs.append("🕯️Engulf↑")
        elif p > recent_hi:
            score += 12; sigs.append("📈Break↑")
    else:
        if p < recent_lo and last["body_ratio"] > 0.55 and last["vol_ratio"] > 1.4:
            score += 25; sigs.append("💥BreakBear")
        elif last["close"] < last["open"] and last["close"] < prev["low"] and last["body_ratio"] > 0.55:
            score += 20; sigs.append("🕯️Engulf↓")
        elif p < recent_lo:
            score += 12; sigs.append("📈Break↓")

    total = max(0, min(score, 100))
    return total, sigs


def determine_direction(df_5m, df_15m=None, ai_hint=None):
    """
    Menentukan arah ANALISA (sebelum invert).
    Hasil akan diinvert di should_enter sebelum eksekusi.
    """
    if df_5m is None or len(df_5m) < 20:
        return None
    last  = df_5m.iloc[-1]
    prev  = df_5m.iloc[-2]
    price = last["close"]
    e3, e5, e9 = last["ema3"], last["ema5"], last["ema9"]
    lp = sp = 0

    if price > e3 > e5 > e9:    lp += 4
    elif price < e3 < e5 < e9:  sp += 4
    elif price > e5 > e9:       lp += 2
    elif price < e5 < e9:       sp += 2

    mom5 = last.get("mom5", 0)
    if mom5 > 0.0015:  lp += 3
    elif mom5 < -0.0015: sp += 3

    if last["macd_hist"] > prev["macd_hist"]: lp += 2
    else:                                     sp += 2

    if last["buy_ratio"] > 0.53 and last["close"] > last["open"]:   lp += 2
    elif last["buy_ratio"] < 0.47 and last["close"] < last["open"]: sp += 2

    if df_15m is not None and len(df_15m) >= 20:
        l15 = df_15m.iloc[-1]
        if l15["ema9"] > l15["ema21"]: lp += 2
        else:                          sp += 2

    btc_t = _macro.get("btc_trend_5m", "UNKNOWN")
    if btc_t in BULL_TRENDS:  lp += 2
    elif btc_t in BEAR_TRENDS: sp += 2

    # AI hint sudah dalam bentuk arah ANALISA (asli dari Groq)
    if ai_hint == "LONG":    lp += 4
    elif ai_hint == "SHORT": sp += 4

    raw_direction = None
    if lp > sp and lp >= 4:  raw_direction = "LONG"
    elif sp > lp and sp >= 4: raw_direction = "SHORT"

    # ─── INVERT DI SINI ──────────────────────────────────
    if raw_direction is not None and INVERT_DIRECTION:
        return invert_dir(raw_direction)
    return raw_direction
    # ──────────────────────────────────────────────────────


# ════════════════════════════════════════════════════════
#  ENTRY FILTER v16 INVERTED
# ════════════════════════════════════════════════════════
def should_enter(symbol):
    if not symbol.endswith("USDT"):
        return None, "not_usdt"

    killed, kr = check_kill_switch()
    if killed:
        return None, f"kill:{kr}"

    if is_symbol_cooling_down(symbol):
        return None, "cooldown"

    fng = _macro["fng"]
    if fng < MIN_FNG:
        return None, f"F&G={fng}"
    if _macro["news"] == "strong_negative":
        return None, "bad_news"

    flash_dir, _ = detect_flash_move()
    if flash_dir != "none":
        return None, f"flash_{flash_dir}"

    tickers = fetch_ticker24h_all()
    pct_24h = 0.0
    if symbol in tickers:
        t24 = tickers[symbol]
        if t24["vol24h"] < 300_000:
            return None, "illiquid"
        pct_24h = t24["pct"]

    df_5m  = get_ohlcv(symbol, Client.KLINE_INTERVAL_5MINUTE, 80)
    df_15m = get_ohlcv(symbol, Client.KLINE_INTERVAL_15MINUTE, 60)
    if df_5m is None or len(df_5m) < 30:
        return None, "no_data"

    df_5m = run_ta(df_5m.copy())
    if df_15m is not None and len(df_15m) >= 20:
        df_15m = run_ta(df_15m.copy())

    # ai_hint = arah ANALISA asli dari Groq (LONG/SHORT sebelum invert)
    ai_hint, ai_reason = get_ai_direction_hint(symbol)

    # direction = arah EKSEKUSI (sudah diinvert di dalam determine_direction)
    direction = determine_direction(df_5m, df_15m, ai_hint=ai_hint)
    if direction is None:
        return None, "no_direction"

    is_chop, chop_desc = is_chop_market(df_5m)
    if is_chop:
        _stats["skipped_chop"] += 1
        return None, f"chop:{chop_desc}"

    mom_pass, mom_pct, mom_desc = check_momentum_strength(df_5m, direction)
    if not mom_pass:
        # AI override: hitung effective AI direction (setelah invert)
        effective_ai = invert_dir(ai_hint) if INVERT_DIRECTION and ai_hint else ai_hint
        if effective_ai == direction and abs(mom_pct) > MIN_MOMENTUM_PCT * 0.65:
            pass  # AI override, lanjut
        else:
            _stats["skipped_no_momentum"] += 1
            return None, f"no_mom:{mom_desc}"

    funding_bias, fr = get_funding_bias(symbol)
    if direction == "LONG"  and funding_bias == "bearish_bias" and fr > 0.001:
        return None, "funding_bear"
    if direction == "SHORT" and funding_bias == "bullish_bias" and fr < -0.001:
        return None, "funding_bull"

    btc_5m  = _macro["btc_trend_5m"]
    btc_15m = _macro["btc_trend_15m"]

    # ─── BTC FILTER DISESUAIKAN DENGAN INVERT ────────────
    # Karena direction sudah diinvert, filter harus mengikuti direction EKSEKUSI.
    # Contoh: direction=SHORT (hasil invert dari LONG analisa)
    #   → jangan SHORT kalau BTC sedang BEAR kuat (momentum turun terlalu deras untuk kontra)
    # Sama seperti logika normal, hanya sekarang "direction" sudah final.
    if direction == "LONG"  and btc_5m in BEAR_TRENDS and btc_15m in BEAR_TRENDS:
        if not INVERT_DIRECTION:
            return None, f"skip_LONG:BTC_{btc_5m}"
        # Kalau invert: LONG (arah eksekusi) saat BTC bear = kita analisa SHORT
        # Ini justru kondisi yang kita cari (kontra-trend), TAPI jika terlalu ekstrem tetap skip
        # Hanya skip kalau BTC crash sangat kuat (kedua timeframe BEAR solid)
        # → biarkan lewat, kontra-trend adalah tujuan kita
        pass

    if direction == "SHORT" and btc_5m in BULL_TRENDS and btc_15m in BULL_TRENDS:
        if not INVERT_DIRECTION:
            return None, f"skip_SHORT:BTC_{btc_5m}"
        # Sama: SHORT saat BTC bull = kontra-trend, biarkan lewat
        pass

    if direction == "LONG" and fng > MAX_FNG_LONG:
        return None, "overbought"

    score, sigs = get_entry_score(symbol, df_5m, direction, ai_hint)

    min_score_now = BAD_HOURS_MIN_SCORE if time.gmtime().tm_hour in BAD_HOURS_UTC else MIN_SCORE
    if score < min_score_now:
        return None, f"score={score:.0f}<{min_score_now}"

    if len(sigs) < MIN_ENTRY_SIGNALS:
        return None, f"signals={len(sigs)}"

    atr   = df_5m["atr"].iloc[-1]
    price = df_5m["close"].iloc[-1]

    if atr / price > MAX_SL_ATR_PCT:
        return None, "ATR_big"

    levels = calc_atr_levels(price, atr, direction)

    # Spread filter
    try:
        ob = client.futures_order_book(symbol=symbol, limit=5)
        best_bid = float(ob["bids"][0][0])
        best_ask = float(ob["asks"][0][0])
        spread   = best_ask - best_bid
        tp1_dist = abs(levels["tp1"] - price)
        if tp1_dist > 0 and (spread / tp1_dist) > MAX_SPREAD_RATIO:
            _stats["skipped_spread"] += 1
            return None, "spread_lebar"
    except:
        pass

    ob_imb = get_ob_imbalance(symbol)

    # ─── ORDER BOOK FILTER DISESUAIKAN DENGAN INVERT ─────
    # Kalau invert: kita masuk SHORT saat OB bias ke BUY (kontra imbalance)
    # Tetapi filter terlalu ketat bisa membunuh semua entry kontra-trend.
    # Solusi: longgarkan threshold OB saat invert, atau nonaktifkan.
    if not INVERT_DIRECTION:
        if direction == "LONG"  and ob_imb < -0.25: return None, "OB_SHORT"
        if direction == "SHORT" and ob_imb > 0.25:  return None, "OB_LONG"
    else:
        # Mode invert: OB berlawanan justru sinyal bagus (kontra-trend lebih ekstrem)
        # Hanya block kalau benar-benar netral atau sedikit searah eksekusi
        if direction == "LONG"  and ob_imb < -0.40: return None, "OB_too_short"
        if direction == "SHORT" and ob_imb > 0.40:  return None, "OB_too_long"

    return direction, {
        "score":       score,
        "signals":     sigs,
        "direction":   direction,
        "sl":          levels["sl"],
        "tp1":         levels["tp1"],
        "tp2":         levels["tp2"],
        "sl_pct":      levels["sl_pct"],
        "tp1_pct":     levels["tp1_pct"],
        "ob_imb":      ob_imb,
        "atr":         atr,
        "atr_pct":     levels["atr_pct"],
        "mom_pct":     mom_pct,
        "pct_24h":     pct_24h,
        "funding":     fr,
        "instant_cut": levels["instant_cut"],
        "trail_sl":    levels["trail_sl"],
        "ai_hint":     ai_hint,
        "ai_reason":   ai_reason,
    }


# ════════════════════════════════════════════════════════
#  PARALLEL SCANNER
# ════════════════════════════════════════════════════════
def scan_symbol_safe(symbol):
    try:
        time.sleep(SCAN_DELAY_MS)
        direction, info = should_enter(symbol)
        if direction:
            return symbol, direction, info
    except:
        pass
    return None


def scan_batch_parallel(symbols):
    candidates = []
    futures = {_executor.submit(scan_symbol_safe, sym): sym for sym in symbols[:30]}
    try:
        for future in as_completed(futures, timeout=14):
            try:
                result = future.result(timeout=2)
                if result:
                    candidates.append(result)
            except:
                pass
    except TimeoutError:
        for future in futures:
            if future.done():
                try:
                    result = future.result(timeout=0)
                    if result:
                        candidates.append(result)
                except:
                    pass
            else:
                future.cancel()
    except Exception as e:
        safe_print(f"  ❌ Scan batch err: {e}")
    return candidates


# ════════════════════════════════════════════════════════
#  INSTANT RESCAN
# ════════════════════════════════════════════════════════
def trigger_rescan(reason="", priority_symbol=None):
    if priority_symbol:
        _hot_symbols.appendleft(priority_symbol)
    _rescan_queue.put({"reason": reason, "ts": time.time()})


def instant_rescan_worker(symbols_active):
    while True:
        try:
            event = _rescan_queue.get(timeout=60)
            time.sleep(RE_SCAN_DELAY_SEC)
            slots_free = MAX_POSITIONS - len(open_positions)
            if slots_free <= 0:
                continue
            killed, _ = check_kill_switch()
            if killed:
                continue
            flash_dir, _ = detect_flash_move()
            if flash_dir != "none":
                continue

            ai_syms  = [p["symbol"] for p in _ai_picks if p["symbol"] not in open_positions
                        and p["symbol"] in set(symbols_active)]
            hot      = [s for s in list(_hot_symbols) if s not in open_positions]
            rest     = [s for s in symbols_active if s not in open_positions
                        and s not in hot and s not in ai_syms]
            scan_list = ai_syms[:12] + hot[:15] + rest[:15]

            _stats["rescans"] += 1
            try:
                candidates = scan_batch_parallel(scan_list)
            except Exception as e:
                candidates = []

            if candidates:
                candidates.sort(key=lambda x: x[2].get("score", 0), reverse=True)
                for sym, direction, info in candidates[:slots_free]:
                    if len(open_positions) >= MAX_POSITIONS:
                        break
                    open_trade(sym, direction, info)
        except queue.Empty:
            pass
        except Exception as e:
            safe_print(f"  ❌ Rescan worker err: {e}")


# ════════════════════════════════════════════════════════
#  TRADE EXECUTION
# ════════════════════════════════════════════════════════
def open_trade(symbol, direction, info):
    if not symbol.endswith("USDT"):
        return

    with _lock:
        if symbol in open_positions:
            return
        if len(open_positions) >= MAX_POSITIONS:
            return
        open_positions[symbol] = {"_reserved": True}

    try:
        set_leverage(symbol)
        price = get_price(symbol)
        if price == 0:
            with _lock: open_positions.pop(symbol, None)
            return
        qty = calc_qty(symbol, price)

        if PAPER_TRADING:
            paper_open_order(symbol, direction, qty, price)
        else:
            client.futures_create_order(
                symbol=symbol,
                side=SIDE_BUY if direction == "LONG" else SIDE_SELL,
                type=ORDER_TYPE_MARKET,
                quantity=qty)

        entry  = get_price(symbol)
        atr    = info.get("atr", entry * 0.002)
        levels = calc_atr_levels(entry, atr, direction)

        open_positions[symbol] = {
            "side":             direction,
            "entry":            entry,
            "qty":              qty,
            "qty_remain":       qty,
            "sl":               levels["sl"],
            "tp1":              levels["tp1"],
            "tp2":              levels["tp2"],
            "peak":             entry,
            "trail_sl":         levels["trail_sl"],
            "trail_active":     True,
            "trail_phase":      1,
            "tp1_hit":          False,
            "be_active":        False,
            "open_time":        time.time(),
            "score":            info.get("score", 0),
            "signals":          info.get("signals", []),
            "instant_cut":      levels["instant_cut"],
            "instant_cut_done": False,
            "entry_candle":     0,
            "atr":              atr,
            "ai_hint":          info.get("ai_hint", ""),
        }

        mode_tag = "📋PAPER" if PAPER_TRADING else "🔴LIVE"
        inv_tag  = "↩️KONTRA" if INVERT_DIRECTION else ""
        ai_raw   = info.get("ai_hint", "")
        ai_tag   = f"[🤖AI:{ai_raw}→{invert_dir(ai_raw) if INVERT_DIRECTION and ai_raw else ai_raw}]" if ai_raw else ""
        sig_str  = " | ".join(info.get("signals", [])[:3])
        safe_print(
            f"\n  {'🟢' if direction=='LONG' else '🔴'} {mode_tag} {inv_tag} {symbol} {direction} @{entry:.6g} {ai_tag}\n"
            f"     SL:{levels['sl_pct']*100:.2f}% TP1:{levels['tp1_pct']*100:.2f}% "
            f"Score:{info['score']:.0f} TrailSL:{levels['trail_sl']:.6g}\n"
            f"     {sig_str}"
        )
        _stats["total_trades"] += 1
    except Exception as e:
        with _lock: open_positions.pop(symbol, None)
        safe_print(f"  ❌ [{symbol}] Open err: {e}")


def partial_close_tp1(symbol):
    pos = open_positions.get(symbol)
    if pos is None or pos.get("tp1_hit"):
        return
    try:
        exit_p = get_price(symbol)
        side   = pos["side"]
        qty    = pos["qty"]
        info   = get_sym_info(symbol)
        c_qty  = max(round_step(qty * TP1_CLOSE_RATIO, info["step"]), info["minQty"])

        if PAPER_TRADING:
            pnl = paper_close_order(symbol, side, c_qty, pos["entry"], exit_p, "TP1")
        else:
            amt = get_exchange_amt(symbol)
            if amt is None or amt == 0:
                pos["tp1_hit"] = True; return
            c_qty = min(c_qty, abs(amt))
            client.futures_create_order(
                symbol=symbol,
                side=SIDE_SELL if amt > 0 else SIDE_BUY,
                type=ORDER_TYPE_MARKET,
                quantity=c_qty,
                reduceOnly=True)
            pnl = (exit_p - pos["entry"]) * c_qty if side == "LONG" \
                  else (pos["entry"] - exit_p) * c_qty

        hold_s = time.time() - pos["open_time"]
        safe_print(f"  🎯 [{symbol}] TP1 @{exit_p:.6g} ({hold_s:.0f}s) PnL:{pnl:+.4f}U")

        pos["tp1_hit"]    = True
        pos["qty_remain"] = qty - c_qty
        pos["be_active"]  = True
        atr               = pos.get("atr", exit_p * 0.002)
        if side == "LONG":
            pos["sl"]       = round(pos["entry"] * (1 + TRAIL_BE_PCT), 8)
            pos["trail_sl"] = max(pos["trail_sl"], pos["sl"])
        else:
            pos["sl"]       = round(pos["entry"] * (1 - TRAIL_BE_PCT), 8)
            pos["trail_sl"] = min(pos["trail_sl"], pos["sl"])
        pos["trail_phase"] = 2
        pos["peak"]        = exit_p

        _stats["tp1_hits"]  += 1
        _stats["wins"]      += 1
        _stats["total_pnl"] += pnl
        _stats["pnl_history"].append(pnl)
        update_kill_switch_after_trade(pnl)
        _perf[symbol]["wins"]   += 1
        _perf[symbol]["pnl"]    += pnl
        _perf[symbol]["trades"] += 1
        trade_log.append({"symbol": symbol, "side": side, "pnl": round(pnl,4),
                          "reason": "TP1", "hold_sec": int(hold_s)})
        _hot_symbols.appendleft(symbol)
    except Exception as e:
        safe_print(f"  ❌ [{symbol}] TP1 err: {e}")
        pos["tp1_hit"] = True


def close_trade(symbol, reason=""):
    try:
        pos = open_positions.get(symbol)
        if pos is None or pos.get("_reserved"):
            with _lock: open_positions.pop(symbol, None)
            return True

        exit_p = get_price(symbol)
        if exit_p == 0:
            return False

        qty_r = pos.get("qty_remain", pos["qty"])
        side  = pos["side"]

        if PAPER_TRADING:
            pnl = paper_close_order(symbol, side, qty_r, pos["entry"], exit_p, reason)
        else:
            amt = get_exchange_amt(symbol)
            if amt is None:
                return False
            if amt == 0:
                with _lock: open_positions.pop(symbol, None)
                set_symbol_cooldown(symbol)
                trigger_rescan(f"close@{symbol}", priority_symbol=symbol)
                return True
            client.futures_create_order(
                symbol=symbol,
                side=SIDE_SELL if amt > 0 else SIDE_BUY,
                type=ORDER_TYPE_MARKET,
                quantity=abs(amt),
                reduceOnly=True)
            pnl = (exit_p - pos["entry"]) * qty_r if side == "LONG" \
                  else (pos["entry"] - exit_p) * qty_r

        with _lock:
            open_positions.pop(symbol, None)

        pct    = pnl / (pos["entry"] * qty_r) * 100 if qty_r > 0 else 0
        hold_s = time.time() - pos["open_time"]
        emoji  = "🟢" if pnl >= 0 else "🔴"
        be_tag = "[BE]" if pos.get("be_active") else ""
        safe_print(
            f"  {emoji} [{symbol}] {side} CLOSE {reason}{be_tag} "
            f"@{exit_p:.6g} | {hold_s:.0f}s | PnL:{pnl:+.4f}U ({pct:+.2f}%)"
        )

        _stats["total_pnl"] += pnl
        _stats["pnl_history"].append(pnl)
        update_kill_switch_after_trade(pnl)
        _perf[symbol]["trades"] += 1
        _perf[symbol]["pnl"]    += pnl

        if pnl >= 0:
            _stats["wins"] += 1
            _perf[symbol]["wins"] += 1
            if pnl > _stats["best_trade"]: _stats["best_trade"] = pnl
        else:
            _stats["losses"] += 1
            _perf[symbol]["losses"] += 1
            if pnl < _stats["worst_trade"]: _stats["worst_trade"] = pnl

        if "TP2"     in reason: _stats["tp2_hits"]     += 1
        if "SL"      in reason: _stats["sl_hits"]      += 1
        if "Force"   in reason: _stats["force_closes"] += 1
        if "InstCut" in reason: _stats["instant_cuts"] += 1

        trade_log.append({"symbol": symbol, "side": side, "pnl": round(pnl,4),
                          "reason": reason, "hold_sec": int(hold_s)})
        set_symbol_cooldown(symbol)
        trigger_rescan(f"close@{symbol}", priority_symbol=symbol)
        return True
    except Exception as e:
        safe_print(f"  ❌ [{symbol}] Close err: {e}")
        return False


# ════════════════════════════════════════════════════════
#  POSITION MONITOR v16
# ════════════════════════════════════════════════════════
def manage_positions():
    if not open_positions:
        return
    flash_dir, flash_pct = detect_flash_move()

    for symbol in list(open_positions.keys()):
        pos = open_positions.get(symbol)
        if pos is None or pos.get("_reserved"):
            continue

        price = get_price(symbol)
        if price == 0:
            continue

        side  = pos["side"]
        entry = pos["entry"]
        atr   = pos.get("atr", entry * 0.002)
        pos["entry_candle"] = pos.get("entry_candle", 0) + 1
        hold_min = (time.time() - pos["open_time"]) / 60

        if hold_min >= MAX_HOLDING_MIN * 0.95:
            close_trade(symbol, f"⏰Force({hold_min:.1f}m)")
            continue

        if flash_dir == "crash" and side == "LONG":
            close_trade(symbol, f"⚡Flash-{flash_pct:.1f}%"); continue
        elif flash_dir == "pump" and side == "SHORT":
            close_trade(symbol, f"⚡Flash+{flash_pct:.1f}%"); continue

        within_window = pos.get("entry_candle", 0) <= (INSTANT_CUT_WINDOW * 6)
        if not pos.get("instant_cut_done") and not pos.get("tp1_hit") and within_window:
            ic = pos["instant_cut"]
            if (side == "LONG" and price <= ic) or (side == "SHORT" and price >= ic):
                pos["instant_cut_done"] = True
                close_trade(symbol, "⚡InstCut")
                continue
        elif not within_window:
            pos["instant_cut_done"] = True

        if side == "LONG":
            profit_pct = (price - entry) / entry

            if not pos["tp1_hit"] and price >= pos["tp1"]:
                partial_close_tp1(symbol)
                continue

            if price > pos["peak"]:
                pos["peak"] = price
                mult = ATR_TRAIL_TIGHT_MULT if pos["trail_phase"] >= 3 else ATR_TRAIL_MULT
                new_trail = price - atr * mult
                pos["trail_sl"] = max(pos["trail_sl"], new_trail)

            if profit_pct >= TRAIL_TIGHT_PCT and pos["trail_phase"] < 3:
                pos["trail_phase"] = 3

            if pos.get("be_active"):
                be_floor = pos["entry"] * (1 + TRAIL_BE_PCT)
                pos["trail_sl"] = max(pos["trail_sl"], be_floor)

            if pos["tp1_hit"] and price >= pos["tp2"]:
                close_trade(symbol, "✨TP2"); continue

            if price <= pos["trail_sl"]:
                tag = "🔒TrailBE" if pos.get("be_active") else "🔄Trail"
                close_trade(symbol, tag); continue

            if price <= pos["sl"]:
                close_trade(symbol, "🛑SL"); continue

            pnl = (price - entry) * pos.get("qty_remain", pos["qty"])
            tp_tag = f"TP2:{pos['tp2']:.6g}" if pos["tp1_hit"] else f"TP1:{pos['tp1']:.6g}"
            safe_print(
                f"  📌 [{symbol}] L@{entry:.6g}→{price:.6g} "
                f"({profit_pct*100:+.2f}%) {pnl:+.3f}U "
                f"{hold_min:.1f}m P{pos['trail_phase']} TSL:{pos['trail_sl']:.6g} {tp_tag}"
            )

        else:  # SHORT
            profit_pct = (entry - price) / entry

            if not pos["tp1_hit"] and price <= pos["tp1"]:
                partial_close_tp1(symbol)
                continue

            if price < pos["peak"]:
                pos["peak"] = price
                mult = ATR_TRAIL_TIGHT_MULT if pos["trail_phase"] >= 3 else ATR_TRAIL_MULT
                new_trail = price + atr * mult
                pos["trail_sl"] = min(pos["trail_sl"], new_trail)

            if profit_pct >= TRAIL_TIGHT_PCT and pos["trail_phase"] < 3:
                pos["trail_phase"] = 3

            if pos.get("be_active"):
                be_ceil = pos["entry"] * (1 - TRAIL_BE_PCT)
                pos["trail_sl"] = min(pos["trail_sl"], be_ceil)

            if pos["tp1_hit"] and price <= pos["tp2"]:
                close_trade(symbol, "✨TP2"); continue

            if price >= pos["trail_sl"]:
                tag = "🔒TrailBE" if pos.get("be_active") else "🔄Trail"
                close_trade(symbol, tag); continue

            if price >= pos["sl"]:
                close_trade(symbol, "🛑SL"); continue

            pnl = (entry - price) * pos.get("qty_remain", pos["qty"])
            tp_tag = f"TP2:{pos['tp2']:.6g}" if pos["tp1_hit"] else f"TP1:{pos['tp1']:.6g}"
            safe_print(
                f"  📌 [{symbol}] S@{entry:.6g}→{price:.6g} "
                f"({profit_pct*100:+.2f}%) {pnl:+.3f}U "
                f"{hold_min:.1f}m P{pos['trail_phase']} TSL:{pos['trail_sl']:.6g} {tp_tag}"
            )


def position_monitor_thread():
    while True:
        try:
            if open_positions:
                manage_positions()
        except Exception as e:
            safe_print(f"  ❌ Monitor err: {e}")
        time.sleep(POSITION_MONITOR_SEC)


# ════════════════════════════════════════════════════════
#  STATS
# ════════════════════════════════════════════════════════
def calc_expectancy():
    wins   = [t["pnl"] for t in trade_log if t["pnl"] > 0]
    losses = [t["pnl"] for t in trade_log if t["pnl"] < 0]
    if not wins and not losses:
        return 0.0
    wr    = len(wins) / (len(wins) + len(losses))
    avg_w = sum(wins)   / len(wins)   if wins   else 0
    avg_l = abs(sum(losses) / len(losses)) if losses else 0
    return round((wr * avg_w) - ((1 - wr) * avg_l), 5)


def calc_sharpe():
    pnls = list(_stats["pnl_history"])
    if len(pnls) < 5:
        return 0.0
    arr = np.array(pnls)
    std = float(np.std(arr))
    return round(float(np.mean(arr)) / std, 3) if std else 0.0


def print_stats():
    n    = _stats["wins"] + _stats["losses"]
    wr   = _stats["wins"] / n * 100 if n else 0
    sess = (time.time() - _stats["session_start"]) / 60
    pnl  = _stats["total_pnl"]
    emoji = "💚" if pnl >= 0 else "🔴"
    exp  = calc_expectancy()
    sr   = calc_sharpe()
    ks   = _kill_switch
    bal  = f" | Bal:${_paper['balance']:.2f}" if PAPER_TRADING else ""
    inv  = " [↩️INVERTED]" if INVERT_DIRECTION else ""

    with _print_lock:
        print(f"\n  {'─'*64}")
        print(f"  📋 PAPER TRADING{bal}{inv}")
        print(f"  ⏱  {sess:.0f}m | Trades:{n} WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']}")
        print(f"  {emoji} PnL:{pnl:+.4f}U | Exp:{exp:+.5f}U | Sharpe:{sr:.2f}")
        print(f"  🎯TP1:{_stats['tp1_hits']} ✨TP2:{_stats['tp2_hits']} "
              f"🛑SL:{_stats['sl_hits']} ⚡Cut:{_stats['instant_cuts']} ⏰Force:{_stats['force_closes']}")
        print(f"  🚫 Chop:{_stats['skipped_chop']} NoMom:{_stats['skipped_no_momentum']} "
              f"Spread:{_stats['skipped_spread']}")
        print(f"  🤖 AI picks (raw): {', '.join([p['symbol'] for p in _ai_picks[:5]])}")
        print(f"  🛡  KS:{ks['consec_losses']}CL DailyPnL:{ks['daily_pnl']:+.2f}U Lag:{ks['api_lag']*1000:.0f}ms")
        sym_sorted = sorted(_perf.items(), key=lambda x: x[1]["pnl"], reverse=True)
        if sym_sorted:
            print(f"  🏆 Top:")
            for sym, d in sym_sorted[:5]:
                w = d["wins"]/d["trades"]*100 if d["trades"] else 0
                print(f"     {sym:<16} {d['trades']}T WR:{w:.0f}% PnL:{d['pnl']:+.4f}U")
        if trade_log:
            print(f"  📋 Last 5:")
            for t in trade_log[-5:]:
                e = "🟢" if t["pnl"] > 0 else "🔴"
                s = t.get("hold_sec", 0)
                print(f"     {e} {t['symbol']:<16} {t['side']} {t['pnl']:+.4f}U "
                      f"({s//60}m{s%60}s) — {t['reason'][:20]}")
        print(f"  {'─'*64}\n")


# ════════════════════════════════════════════════════════
#  MAIN LOOP v16 INVERTED
# ════════════════════════════════════════════════════════
def run_bot():
    inv_label = "↩️ INVERT MODE AKTIF" if INVERT_DIRECTION else "➡️  MODE NORMAL"
    with _print_lock:
        print("╔══════════════════════════════════════════════════════════════╗")
        print("║  🤖 BOT SCALPING v16 — AI MOMENTUM ENGINE [INVERTED]        ║")
        print("╠══════════════════════════════════════════════════════════════╣")
        print(f"║  {inv_label:<60}║")
        print("║  📋 PAPER/BACKTEST MODE (USDT pairs only, no USDC)          ║")
        print(f"║  Leverage:{LEVERAGE}x  PerTrade:${ORDER_USDT}  MaxPos:{MAX_POSITIONS}  Hold:{MAX_HOLDING_MIN}m           ║")
        print(f"║  KillSwitch: {CONSEC_LOSS_MAX}CL / ${abs(DAILY_LOSS_LIMIT)} daily loss / pause {CONSEC_LOSS_PAUSE_MIN}m        ║")
        print(f"║  Groq AI: {'AKTIF ✅' if GROQ_API_KEY else 'OFF (set GROQ_API_KEY)'}                              ║")
        print("╚══════════════════════════════════════════════════════════════╝\n")

    safe_print("  ⏳ Validasi symbols (USDT only)...")
    symbols_active = validate_symbols()
    safe_print(f"  📊 {len(symbols_active)} symbols aktif")

    safe_print("  📦 Pre-load sym info...")
    with ThreadPoolExecutor(max_workers=15) as ex:
        list(ex.map(get_sym_info, symbols_active[:80]))

    safe_print("  🌐 Refresh macro...")
    refresh_macro()
    update_btc_price()
    safe_print(f"  ✅ BTC:{_macro['btc_trend_5m']} Mode:{_macro['scalp_mode']} F&G:{_macro['fng']}")
    safe_print("  🚀 Start dalam 3s...\n")
    time.sleep(3)

    threading.Thread(target=position_monitor_thread, daemon=True).start()
    threading.Thread(target=instant_rescan_worker, args=(symbols_active,), daemon=True).start()
    safe_print("  🔧 Monitor + Rescan threads: START ✅\n")

    global _scan_batch_idx, _ai_cycle_counter
    cycle         = 0
    total_batches = math.ceil(len(symbols_active) / BATCH_SIZE)

    while True:
        cycle += 1
        _ai_cycle_counter += 1
        refresh_macro()
        update_btc_price()

        if cycle % 30 == 0:
            check_api_latency()

        if _ai_cycle_counter >= AI_SCAN_INTERVAL and GROQ_API_KEY:
            _ai_cycle_counter = 0
            tickers = fetch_ticker24h_all()
            threading.Thread(
                target=ask_groq_for_best_coins, args=(tickers, _macro), daemon=True
            ).start()

        flash_dir, flash_pct = detect_flash_move()
        flash_str = f"⚡{flash_dir.upper()}:{flash_pct:.1f}%" if flash_dir != "none" else ""

        utc_h  = time.gmtime().tm_hour
        bad_h  = "⚠️BAD_HR" if utc_h in BAD_HOURS_UTC else ""
        bal_s  = f"${_paper['balance']:.0f}" if PAPER_TRADING else ""
        inv_s  = "↩️INVERT" if INVERT_DIRECTION else ""

        pos_list = list(open_positions.keys())
        with _print_lock:
            print(f"\n{'═'*65}")
            print(f"  #{cycle} {time.strftime('%H:%M:%S')} | F&G:{_macro['fng']} "
                  f"BTC:{_macro['btc_trend_5m']} {flash_str} {bad_h} {bal_s} {inv_s}")
            print(f"  Mode:{_macro['scalp_mode']} Breadth:{_macro['market_breadth']*100:.0f}% "
                  f"Pos({len(pos_list)}/{MAX_POSITIONS}): {pos_list or '—'}")
            if _ai_picks:
                ai_str = " | ".join(
                    f"{p['symbol']}({p.get('direction','?')}→{invert_dir(p.get('direction','?')) if INVERT_DIRECTION else p.get('direction','?')})"
                    for p in _ai_picks[:5]
                )
                print(f"  🤖 {ai_str}")

        slots_free = MAX_POSITIONS - len(open_positions)
        ks_active, ks_reason = check_kill_switch()

        if ks_active:
            resume_in = max(0, _kill_switch["resume_time"] - time.time())
            safe_print(f"  🚨 KS: {ks_reason} Resume:{resume_in/60:.1f}m")

        skip_reason = None
        if _macro["news"] == "strong_negative": skip_reason = "bad_news"
        elif flash_dir != "none":               skip_reason = f"flash_{flash_dir}"
        elif ks_active:                         skip_reason = f"kill:{ks_reason}"

        if slots_free > 0 and not skip_reason:
            active_set = set(symbols_active)
            ai_syms   = [p["symbol"] for p in _ai_picks
                         if p["symbol"] not in open_positions and p["symbol"] in active_set][:15]
            top_mv    = get_top_movers(symbols_active, n=40)
            top_syms  = [s for s, _, _ in top_mv
                         if s not in open_positions and s not in ai_syms][:20]

            batch_start = _scan_batch_idx * BATCH_SIZE
            reg_syms    = [s for s in symbols_active[batch_start:batch_start + BATCH_SIZE]
                           if s not in open_positions and s not in ai_syms and s not in top_syms]
            _scan_batch_idx = (_scan_batch_idx + 1) % total_batches

            scan_list = ai_syms + top_syms[:12] + reg_syms[:10]

            top_str = " | ".join(f"{s}({p:+.1f}%)" for s, p, _ in top_mv[:5])
            with _print_lock:
                print(f"  📊 Top:{top_str}")
                print(f"  🔍 {len(scan_list)} syms | AI:{len(ai_syms)} Movers:{len(top_syms[:12])} Reg:{len(reg_syms[:10])}")

            try:
                candidates = scan_batch_parallel(scan_list)
            except Exception as e:
                safe_print(f"  ❌ Scan err: {e}")
                candidates = []

            if candidates:
                candidates.sort(key=lambda x: x[2].get("score", 0), reverse=True)
                safe_print(f"  🎯 {len(candidates)} setup!")
                for sym, direction, info in candidates[:slots_free]:
                    if len(open_positions) >= MAX_POSITIONS:
                        break
                    ai_raw  = info.get("ai_hint", "")
                    ai_tag  = f"🤖{ai_raw}↩️{invert_dir(ai_raw)}" if ai_raw and INVERT_DIRECTION else (f"🤖{ai_raw}" if ai_raw else "")
                    sig_str = " | ".join(info.get("signals", [])[:3])
                    safe_print(f"     ⭐{ai_tag} {sym} {direction} "
                               f"Mom:{info.get('mom_pct',0)*100:+.2f}% Score:{info['score']:.0f} — {sig_str}")
                    open_trade(sym, direction, info)
            else:
                safe_print("  ⏳ No setup")

            if ALWAYS_FILL_SLOTS and len(open_positions) < MAX_POSITIONS:
                trigger_rescan("always_fill")
        else:
            if skip_reason:
                safe_print(f"  ⏸️  Skip: {skip_reason}")
            else:
                safe_print(f"  ✅ {MAX_POSITIONS} slot penuh")

        if cycle % 15 == 0:
            print_stats()

        ks = _kill_switch
        safe_print(
            f"  ⏱  Next:{SCAN_INTERVAL}s | KS:{ks['consec_losses']}CL/{ks['daily_pnl']:+.2f}U "
            f"| AI:{len(_ai_picks)}picks | Rescans:{_stats['rescans']}"
        )
        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    run_bot()
