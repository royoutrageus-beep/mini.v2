import math
import yfinance as yf
import pandas as pd
import streamlit as st
import time
import random
import requests
import numpy as np
import pytz
import pickle
import threading
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ════════════════════════════════════════════════════
#  CONFIG
# ════════════════════════════════════════════════════
import os
def _get_secret(*keys, default=""):
    """Try multiple secret key names, fallback to env vars."""
    for k in keys:
        try:
            v = st.secrets.get(k, "")
            if v: return v
        except: pass
        v = os.environ.get(k, "")
        if v: return v
    return default

TOKEN   = _get_secret("TELEGRAM_TOKEN", "TELEGRAM_BOT_TOKEN")
CHAT_ID = _get_secret("TELEGRAM_CHAT_ID", "CHAT_ID")
jakarta_tz = pytz.timezone('Asia/Jakarta')

try:
    DS_KEY = st.secrets.get("DATASECTORS_API_KEY", "")
except:
    DS_KEY = ""

DS_BASE = "https://api.datasectors.com"

# ════════════════════════════════════════════════════
#  DISK CACHE — thread-safe, persistent antar session
# ════════════════════════════════════════════════════
CACHE_DIR = Path("/tmp/hp_cache") if Path("/tmp").exists() else Path.home() / ".hp_cache"
CACHE_DIR.mkdir(exist_ok=True)
CACHE_TTL  = 300
_mem       = {}
_mem_lock  = threading.Lock()

def _ck(ticker, tf): return f"tt_{ticker}_{tf}"

def _disk_get(key):
    fp = CACHE_DIR / f"{key}.pkl"
    try:
        if fp.exists():
            d = pickle.loads(fp.read_bytes())
            if time.time() - d["ts"] < CACHE_TTL:
                return d["df"]
    except: pass
    return None

def _disk_set(key, df):
    try:
        fp = CACHE_DIR / f"{key}.pkl"
        fp.write_bytes(pickle.dumps({"ts": time.time(), "df": df}))
    except: pass

def _cache_get(ticker, tf):
    key = _ck(ticker, tf)
    with _mem_lock:
        if key in _mem:
            ts, df = _mem[key]
            if time.time() - ts < CACHE_TTL:
                return df
    df = _disk_get(key)
    if df is not None:
        with _mem_lock:
            _mem[key] = (time.time(), df)
    return df

def _cache_set(ticker, tf, df):
    key = _ck(ticker, tf)
    with _mem_lock:
        _mem[key] = (time.time(), df)
    _disk_set(key, df)

def _cache_age(ticker, tf):
    key = _ck(ticker, tf)
    with _mem_lock:
        if key in _mem:
            return time.time() - _mem[key][0]
    fp = CACHE_DIR / f"{key}.pkl"
    try:
        if fp.exists():
            d = pickle.loads(fp.read_bytes())
            return time.time() - d["ts"]
    except: pass
    return None

# ════════════════════════════════════════════════════
#  DATASECTORS FETCH — THREAD-SAFE
# ════════════════════════════════════════════════════
TF_MAP = {
    "1m":"1m","5m":"5m","15m":"15m","15":"15m",
    "30m":"30m","1h":"1h","4h":"4h",
    "1d":"daily","d":"daily","daily":"daily"
}

def _ds_headers():
    return {
        "X-API-Key": DS_KEY,
        "Accept": "*/*",
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
    }

def find_chartbit(obj, depth=0):
    if depth > 6: return None
    if isinstance(obj, dict):
        if "chartbit" in obj: return obj["chartbit"]
        for v in obj.values():
            r = find_chartbit(v, depth+1)
            if r: return r
    return None

def fetch_ds_ohlcv(ticker, interval="15m", limit=200, force_fresh=False):
    if not DS_KEY: return None
    if not force_fresh:
        cached = _cache_get(ticker, interval)
        if cached is not None:
            return cached
    t  = ticker.replace(".JK","").upper().strip()
    tf = TF_MAP.get(str(interval).lower(), "15m")
    ts_param = int(time.time())
    url = f"{DS_BASE}/api/chart-saham/{t}/{tf}/latest?_={ts_param}"
    try:
        r = requests.get(url, headers=_ds_headers(), timeout=12)
        if r.status_code != 200: return None
        rows = find_chartbit(r.json())
        if not rows: return None
        df = pd.DataFrame(rows)
        rename = {
            'open':'Open','high':'High','low':'Low','close':'Close',
            'volume':'Volume','datetime':'Datetime','date':'Date',
            'unix_timestamp':'UnixTs',
            'foreign_buy':'FBuy','foreign_sell':'FSell',
            'value':'Value','frequency':'Frequency',
        }
        df.rename(columns={k:v for k,v in rename.items() if k in df.columns}, inplace=True)
        for col in ["Open","High","Low","Close","Volume","FBuy","FSell","Value","Frequency"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        parsed = False
        for dc in ["Datetime","Date"]:
            if dc in df.columns:
                df["_dt"] = pd.to_datetime(df[dc], errors='coerce')
                if not df["_dt"].isna().all():
                    df = df.set_index("_dt"); parsed = True; break
        if not parsed and "UnixTs" in df.columns:
            df["_dt"] = pd.to_datetime(df["UnixTs"], unit='s', errors='coerce')
            df = df.set_index("_dt")
        df = df.dropna(subset=["Close"])
        df = df.sort_index()
        if len(df) < 20: return None
        _cache_set(ticker, interval, df)
        return df
    except:
        return None

# ════════════════════════════════════════════════════
#  YFINANCE ANTI-RATE-LIMIT ENGINE
#  Ticker().history() = chart API = no rate limit ✅
#  yf.download()      = bulk API  = rate limited ❌ (DIHAPUS)
#  random.shuffle + delay = sopan ke Yahoo ✅
# ════════════════════════════════════════════════════
def _normalize_yf_df(df):
    """Normalize DataFrame dari yfinance — handle berbagai versi & MultiIndex."""
    if df is None or df.empty: return None
    try:
        # Handle MultiIndex (yf.download multi-ticker)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        # Normalize column names
        df.columns = [str(c).strip().capitalize() for c in df.columns]
        rename_map = {"Adj close":"Close","Adj_close":"Close","Adjclose":"Close"}
        df = df.rename(columns=rename_map)
        req = ["Open","High","Low","Close","Volume"]
        missing = [c for c in req if c not in df.columns]
        if missing: return None
        df = df[req].copy()
        df["Close"]  = pd.to_numeric(df["Close"],  errors="coerce")
        df["Open"]   = pd.to_numeric(df["Open"],   errors="coerce").fillna(df["Close"])
        df["High"]   = pd.to_numeric(df["High"],   errors="coerce").fillna(df["Close"])
        df["Low"]    = pd.to_numeric(df["Low"],     errors="coerce").fillna(df["Close"])
        df["Volume"] = pd.to_numeric(df["Volume"],  errors="coerce").fillna(0)
        df = df.dropna(subset=["Close"])
        df = df[df["Close"] > 0]
        return df if len(df) >= 2 else None
    except: return None

def _fetch_yf_ticker(ticker_yf, period="7d", interval="15m"):
    """
    Single ticker fetch — 3-layer fallback untuk handle yfinance 1.x breaking changes.
    Layer 1: Ticker().history()           — chart API, fastest
    Layer 2: yf.download() single ticker  — bulk API fallback
    Layer 3: Ticker().history(proxy=None) — explicit no-proxy
    """
    # ── Layer 1: Ticker().history() ─────────────────
    try:
        t = yf.Ticker(ticker_yf)
        df = t.history(period=period, interval=interval, auto_adjust=True, actions=False)
        result = _normalize_yf_df(df)
        if result is not None: return result
    except Exception: pass

    # ── Layer 2: yf.download() single ticker ────────
    try:
        df = yf.download(
            ticker_yf, period=period, interval=interval,
            auto_adjust=True, progress=False, threads=False
        )
        result = _normalize_yf_df(df)
        if result is not None: return result
    except Exception: pass

    # ── Layer 3: Ticker tanpa auto_adjust ───────────
    try:
        t = yf.Ticker(ticker_yf)
        df = t.history(period=period, interval=interval, auto_adjust=False, actions=False)
        result = _normalize_yf_df(df)
        if result is not None: return result
    except Exception: pass

    return None

def _fetch_yf_parallel(tickers_yf, period="7d", interval="15m", workers=4, delay=(0.2, 0.5)):
    """Parallel fetch — workers=4 + shuffle + delay.
    Fewer workers = less chance of simultaneous rate-limit trigger.
    """
    results = {}; lock = threading.Lock()
    shuffled = list(tickers_yf); random.shuffle(shuffled)
    def _one(t):
        time.sleep(random.uniform(*delay))
        df = _fetch_yf_ticker(t, period, interval)
        if df is not None:
            with lock: results[t] = df
    with ThreadPoolExecutor(max_workers=workers) as exe:
        list(exe.map(_one, shuffled))
    return results

# ════════════════════════════════════════════════════
#  IDX QUANT — 151 TRADING STRATEGIES ENGINE
#  Kakushadze & Serur (2018)
#  Analisa DAILY untuk konfirmasi makro direction
#  Combined dengan TT 15m untuk timing entry presisi
# ════════════════════════════════════════════════════

def _iq_sma(prices, n):
    if len(prices) < n: return None
    return round(sum(prices[-n:]) / n, 2)

def _iq_ema(prices, n):
    if len(prices) < n: return None
    k = 2 / (n + 1); e = prices[-n]
    for x in prices[-n+1:]: e = x * k + e * (1 - k)
    return round(e, 2)

def _iq_rsi(prices, n=14):
    """Simple RSI untuk daily data — Strategy 3.14 ref."""
    if len(prices) < n + 1: return None
    deltas = [prices[i+1]-prices[i] for i in range(len(prices)-1)]
    gains = [d for d in deltas[-n:] if d > 0]
    losses = [-d for d in deltas[-n:] if d < 0]
    ag = sum(gains)/n if gains else 0
    al = sum(losses)/n if losses else 1e-9
    return round(100 - 100/(1 + ag/al), 1)

def _iq_momentum(prices):
    """Strategy 3.1 — Price Momentum: 1M/3M/6M returns."""
    n = len(prices); score = 50; signals = []
    if n >= 20:
        r = (prices[-1]-prices[-20])/prices[-20]*100
        score += 15 if r > 5 else -15 if r < -5 else 0
        signals.append(f"1M: {r:+.1f}%")
    if n >= 60:
        r = (prices[-1]-prices[-60])/prices[-60]*100
        score += 12 if r > 10 else -12 if r < -10 else 0
        signals.append(f"3M: {r:+.1f}%")
    if n >= 120:
        r = (prices[-1]-prices[-120])/prices[-120]*100
        score += 10 if r > 20 else -10 if r < -15 else 0
        signals.append(f"6M: {r:+.1f}%")
    return {"score": min(100, max(0, score)), "signals": signals}

def _iq_ma_signal(prices):
    """Strategy 3.11/3.12/3.13 — EMA5/13/34 + SMA20/50 stack."""
    e5=_iq_ema(prices,5); e13=_iq_ema(prices,13); e34=_iq_ema(prices,34)
    s20=_iq_sma(prices,20); s50=_iq_sma(prices,50)
    score=50; label="MIXED"; signals=[]
    if e5 and e13:
        if e5>e13: score+=15; signals.append("EMA5>13 ✓")
        else: score-=15; signals.append("EMA5<13 ✗")
    if e13 and e34:
        if e13>e34: score+=12; signals.append("EMA13>34 ✓")
        else: score-=12
    if s20 and s50:
        if s20>s50: score+=8; signals.append("SMA20>50 ✓")
        else: score-=8
    if e5 and e13 and e34:
        if e5>e13>e34:   label="BULLISH"
        elif e5<e13<e34: label="BEARISH"
    return {"score":min(100,max(0,score)),"label":label,"e5":e5,"e13":e13,"e34":e34,
            "s20":s20,"s50":s50,"signals":signals}

def _iq_bagger(momentum_score, ma_score, ma_label, vol_ann, prices):
    """Strategy 3.7 + 3.3 — Bagger: near 52W low + dry vol + momentum."""
    score=0; signals=[]
    score += momentum_score/100*40
    if momentum_score>=60: signals.append("✓ Momentum kuat (3.1)")
    score += ma_score/100*25
    if ma_label=="BULLISH": signals.append("✓ MA stack bullish (3.12/3.13)")
    if vol_ann:
        if 25<=vol_ann<=70: score+=20; signals.append(f"✓ Vol {vol_ann}% sweet spot")
        elif vol_ann<25: score+=10
        else: score+=5
    n=len(prices)
    if n>=60:
        h=max(prices[-min(252,n):]); l=min(prices[-min(252,n):]); cur=prices[-1]
        rng=h-l
        if rng>0:
            pct=(cur-l)/rng*100
            if pct<35:   score+=15; signals.append(f"✓ Near 52W low {pct:.0f}% (3.3)")
            elif pct<55: score+=8
    return {"score":round(min(100,score),1),"signals":signals}

def _iq_pivot_daily(df_d):
    """Strategy 3.14 — Daily Pivot Point (H+L+C)/3."""
    try:
        if df_d is None or len(df_d)<2: return {}
        prev=df_d.iloc[-2]
        h=float(prev["High"]); l=float(prev["Low"]); c=float(prev["Close"])
        pp=(h+l+c)/3; r1=2*pp-l; r2=pp+(h-l); s1=2*pp-h; s2=pp-(h-l)
        cur=float(df_d.iloc[-1]["Close"])
        return {"pp":round(pp),"r1":round(r1),"r2":round(r2),"s1":round(s1),"s2":round(s2),
                "above":cur>pp,"dist_r1":round((r1-cur)/cur*100,2)}
    except: return {}

def iq_analyze(df_daily):
    """
    Jalankan analisa IDX Quant 151 Strategies pada daily DataFrame.
    Returns dict dengan iq_score, iq_verdict, iq_ma, iq_bagger, dll.
    """
    empty = {"iq_score":0,"iq_verdict":"UNKNOWN","iq_mom":0,"iq_ma":"—",
             "iq_bagger":0,"iq_rsi":None,"iq_pivot":{},"iq_signals":[]}
    if df_daily is None or len(df_daily) < 10: return empty
    try:
        prices  = [float(x) for x in df_daily["Close"].tolist() if x>0]
        if len(prices) < 10: return empty

        # Volatility annualized
        vol_ann = None
        if len(prices) >= 21:
            rets = [(prices[i+1]-prices[i])/prices[i] for i in range(len(prices)-21,len(prices)-1)]
            mn = sum(rets)/len(rets)
            var = sum((r-mn)**2 for r in rets)/(len(rets)-1) if len(rets)>1 else 0
            vol_ann = round(math.sqrt(var)*math.sqrt(252)*100, 1) if var>0 else None

        mom = _iq_momentum(prices)
        ma  = _iq_ma_signal(prices)
        bag = _iq_bagger(mom["score"], ma["score"], ma["label"], vol_ann, prices)
        rsi = _iq_rsi(prices)
        pv  = _iq_pivot_daily(df_daily)

        # Composite score: Momentum 40% + MA 35% + Bagger 25%
        comp = round(mom["score"]*0.40 + ma["score"]*0.35 + bag["score"]*0.25, 1)
        verdict = "BUY" if comp>=65 else "HOLD" if comp>=45 else "WAIT"

        all_signals = mom["signals"] + ma["signals"][:2] + bag["signals"][:2]

        return {"iq_score":comp,"iq_verdict":verdict,"iq_mom":mom["score"],
                "iq_ma":ma["label"],"iq_bagger":bag["score"],"iq_rsi":rsi,
                "iq_pivot":pv,"iq_vol_ann":vol_ann,"iq_signals":all_signals,
                "iq_e5":ma["e5"],"iq_e13":ma["e13"],"iq_e34":ma["e34"]}
    except: return empty

def calc_mesin_grade(tt_score, tt_signal, iq_score, iq_verdict, iq_bagger):
    """
    ════ MESIN PRESISI GRADE ════
    Gabungan TT 15m (timing) + IQ daily (direction).
    Keduanya agree = high conviction signal.

    PRESISI 🎯  = TT GACOR/REVERSAL + IQ BUY        → strong entry NOW
    KUAT ⚡     = TT POTENSIAL + IQ BUY, atau TT GACOR + IQ HOLD
    BAGGER 💎   = TT BAGGER/KANDIDAT + IQ Bagger ≥ 60 → accumulation
    BANDAR 🔵   = TT BANDAR/HAKA/SUPER + IQ BUY      → smart money confirm
    MONITOR 👁️  = salah satu bullish, satunya netral
    WAIT ❌     = tidak ada alignment
    """
    is_tt_strong  = any(k in tt_signal for k in ["GACOR","REVERSAL"])
    is_tt_smart   = any(k in tt_signal for k in ["BANDAR","HAKA","SUPER"])
    is_tt_bag     = any(k in tt_signal for k in ["BAGGER","KANDIDAT"])
    is_tt_mod     = any(k in tt_signal for k in ["POTENSIAL","REBOUND","AKUM"])
    is_iq_buy     = iq_verdict == "BUY"
    is_iq_hold    = iq_verdict == "HOLD"

    if is_tt_smart and is_iq_buy:
        return "BANDAR 🔵", "#4da6ff", min(100, tt_score/6*50 + iq_score/100*50 + 22)
    if is_tt_strong and is_iq_buy:
        return "PRESISI 🎯", "#00ff88", min(100, tt_score/6*50 + iq_score/100*50 + 20)
    if is_tt_bag and iq_bagger >= 60:
        return "BAGGER 💎", "#bf5fff", min(100, tt_score/6*50 + iq_score/100*50 + 18)
    if is_tt_mod and is_iq_buy:
        return "KUAT ⚡", "#ffb700", min(100, tt_score/6*50 + iq_score/100*50 + 10)
    if is_tt_strong and is_iq_hold:
        return "KUAT ⚡", "#ffb700", min(100, tt_score/6*50 + iq_score/100*50 + 8)
    if is_iq_buy and (is_tt_strong or is_tt_mod):
        return "MONITOR 👁️", "#00e5ff", tt_score/6*50 + iq_score/100*50
    return "WAIT ❌", "#ff3d5a", max(0, tt_score/6*50 + iq_score/100*50 - 10)

# ════════════════════════════════════════════════════
#  SESSION STATE + DISK PERSISTENCE
# ════════════════════════════════════════════════════
_TT_RESULTS_FILE = CACHE_DIR / "tt_last_results.pkl"
_TT_RESULTS_TTL  = 1800

def _tt_save(results, ts):
    try: _TT_RESULTS_FILE.write_bytes(pickle.dumps({"results":results,"ts":ts}))
    except: pass

def _tt_load():
    try:
        if _TT_RESULTS_FILE.exists():
            d = pickle.loads(_TT_RESULTS_FILE.read_bytes())
            if time.time()-d["ts"] < _TT_RESULTS_TTL: return d
    except: pass
    return None

for _k, _v in [("tt_last_sent", set()), ("wl_results", []),
                ("wl_mode_used", ""), ("scan_results", []),
                ("data_dict", {}), ("last_scan_time", None),
                ("last_scan_mode", "Scalping ⚡"),
                ("bsjp_results", []), ("gapup_results", []),
                ("sector_data", {}), ("beta_data", [])]:
    if _k not in st.session_state: st.session_state[_k] = _v

if not st.session_state.scan_results:
    _tt_saved = _tt_load()
    if _tt_saved:
        st.session_state.scan_results = _tt_saved["results"]
        st.session_state.last_scan_time = _tt_saved["ts"]

st.set_page_config(layout="wide", page_title="Mesin Presisi v1.0", page_icon="⚡", initial_sidebar_state="collapsed")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;600;800&display=swap');
:root{--bg:#080c10;--surface:#0d1117;--border:#1c2533;--accent:#00e5ff;--green:#00ff88;--red:#ff3d5a;
      --amber:#ffb700;--purple:#bf5fff;--orange:#ff7b00;--muted:#4a5568;--text:#c9d1d9;--heading:#e6edf3;}
html,body,[data-testid="stAppViewContainer"]{background:var(--bg)!important;color:var(--text)!important;font-family:'Syne',sans-serif;}
#MainMenu,footer,header{visibility:hidden;}[data-testid="stSidebar"]{display:none!important;}
[data-testid="stExpander"]{background:var(--surface)!important;border:1px solid var(--border)!important;border-radius:8px!important;margin-bottom:12px!important;}
[data-testid="stExpander"] summary{font-family:'Space Mono',monospace!important;font-size:12px!important;color:var(--accent)!important;letter-spacing:1px!important;}
.settings-label{font-family:'Space Mono',monospace;font-size:10px;color:var(--muted);letter-spacing:2px;margin-bottom:10px;padding-bottom:6px;border-bottom:1px solid var(--border);}
.tt-header{display:flex;align-items:center;padding:16px 0 12px 0;border-bottom:1px solid var(--border);margin-bottom:16px;}
.tt-logo{font-family:'Space Mono',monospace;font-size:22px;font-weight:700;color:var(--orange);letter-spacing:-1px;}
.tt-sub{font-size:11px;color:var(--muted);letter-spacing:2px;text-transform:uppercase;}
.live-badge{display:inline-flex;align-items:center;gap:6px;padding:4px 12px;background:rgba(0,229,255,.08);border:1px solid rgba(0,229,255,.3);border-radius:20px;font-family:'Space Mono',monospace;font-size:10px;color:var(--accent);letter-spacing:1px;margin-left:auto;}
.live-dot{width:6px;height:6px;background:var(--green);border-radius:50%;animation:blink 1s infinite;}
@keyframes blink{0%,100%{opacity:1;}50%{opacity:.2;}}
.metric-row{display:flex;gap:10px;margin-bottom:18px;flex-wrap:wrap;}
.metric-card{flex:1;min-width:110px;background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:12px 14px;position:relative;overflow:hidden;}
.metric-card::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:var(--accent);}
.metric-card.green::before{background:var(--green);}.metric-card.red::before{background:var(--red);}
.metric-card.amber::before{background:var(--amber);}.metric-card.orange::before{background:var(--orange);}
.metric-card.purple::before{background:var(--purple);}
.metric-label{font-size:10px;color:var(--muted);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:4px;}
.metric-value{font-family:'Space Mono',monospace;font-size:24px;font-weight:700;color:var(--heading);line-height:1;}
.metric-sub{font-size:10px;color:var(--muted);margin-top:3px;}
.signal-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:12px;margin-bottom:20px;}
.signal-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px;position:relative;overflow:hidden;transition:border-color .2s;}
.signal-card.gacor{border-color:rgba(0,255,136,.4);background:rgba(0,255,136,.03);}
.signal-card.potensial{border-color:rgba(255,183,0,.3);background:rgba(255,183,0,.03);}
.signal-card.watch{border-color:rgba(0,229,255,.2);}
.signal-card::after{content:'';position:absolute;top:0;left:0;width:4px;height:100%;}
.signal-card.gacor::after{background:var(--green);}.signal-card.potensial::after{background:var(--amber);}
.signal-card.watch::after{background:var(--accent);}
.signal-card.bagger{border-color:rgba(191,95,255,.6);background:rgba(191,95,255,.05);box-shadow:0 0 20px rgba(191,95,255,.15);}
.signal-card.bagger::after{background:var(--purple);}.sc-bar.filled-purple{background:var(--purple);}
.bagger-alert-box{background:rgba(191,95,255,.06);border:1px solid rgba(191,95,255,.5);border-radius:8px;padding:14px 18px;margin-bottom:16px;animation:pulse-purple 2s infinite;}
@keyframes pulse-purple{0%,100%{border-color:rgba(191,95,255,.4);}50%{border-color:rgba(191,95,255,.9);}}
.bagger-title{color:var(--purple);font-family:'Space Mono',monospace;font-size:12px;font-weight:700;letter-spacing:2px;}
.sc-ticker{font-family:'Space Mono',monospace;font-size:18px;font-weight:700;color:var(--heading);}
.sc-price{font-family:'Space Mono',monospace;font-size:13px;color:var(--muted);}
.sc-signal{font-size:13px;font-weight:700;margin:6px 0;}
.sc-bars{display:flex;gap:3px;margin:8px 0;}
.sc-bar{height:16px;border-radius:2px;}.sc-bar.filled{background:var(--green);}.sc-bar.empty{background:var(--border);}
.sc-stats{display:flex;gap:12px;flex-wrap:wrap;margin-top:8px;}
.sc-stat{font-family:'Space Mono',monospace;font-size:10px;color:var(--muted);}.sc-stat span{color:var(--text);}
.alert-box{background:rgba(255,61,90,.06);border:1px solid rgba(255,61,90,.4);border-radius:8px;padding:14px 18px;margin-bottom:16px;animation:pulse-border 2s infinite;}
@keyframes pulse-border{0%,100%{border-color:rgba(255,61,90,.4);}50%{border-color:rgba(255,61,90,.9);}}
.alert-title{color:var(--red);font-family:'Space Mono',monospace;font-size:12px;font-weight:700;letter-spacing:2px;}
.tape-wrap{overflow:hidden;white-space:nowrap;border-top:1px solid var(--border);border-bottom:1px solid var(--border);padding:5px 0;margin-bottom:16px;background:var(--surface);}
.tape-inner{display:inline-block;animation:marquee 35s linear infinite;}
@keyframes marquee{0%{transform:translateX(0)}100%{transform:translateX(-50%)}}
.tape-item{display:inline-block;margin:0 18px;font-family:'Space Mono',monospace;font-size:10px;}
.tape-item.up{color:var(--green);}.tape-item.down{color:var(--red);}.tape-item.flat{color:var(--muted);}.tape-item.bagger{color:var(--purple);}
::-webkit-scrollbar{width:4px;height:4px;}::-webkit-scrollbar-track{background:var(--bg);}::-webkit-scrollbar-thumb{background:var(--border);border-radius:2px;}
[data-testid="stNumberInput"] input{background:var(--surface)!important;border:1px solid var(--border)!important;color:var(--heading)!important;font-family:'Space Mono',monospace!important;border-radius:6px!important;}
button[data-testid="baseButton-primary"]{background:var(--orange)!important;color:var(--bg)!important;font-family:'Space Mono',monospace!important;font-weight:700!important;border:none!important;}
.section-title{font-family:'Space Mono',monospace;font-size:11px;color:var(--muted);letter-spacing:2px;text-transform:uppercase;border-left:3px solid var(--orange);padding-left:10px;margin:20px 0 10px 0;}
.bt-result{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:20px;margin-top:12px;}
.bt-metric{display:inline-block;margin-right:24px;margin-bottom:8px;}
.bt-metric-val{font-family:'Space Mono',monospace;font-size:22px;font-weight:700;}
.bt-metric-lbl{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:1px;}
</style>""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════
#  STOCK LIST
# ════════════════════════════════════════════════════
raw_stocks = [
    "AADI","AALI","ABBA","ABDA","ABMM","ACES","ACRO","ACST","ADCP","ADES",
    "ADHI","ADMF","ADMG","ADMR","ADRO","AEGS","AGAR","AGII","AGRO","AGRS",
    "AHAP","AIMS","AISA","AKKU","AKPI","AKRA","AKSI","ALDO","ALII","ALKA",
    "ALMI","ALTO","AMAG","AMAN","AMAR","AMFG","AMIN","AMMN","AMMS","AMOR",
    "AMRT","ANDI","ANJT","ANTM","APEX","APIC","APII","APLI","APLN","ARCI",
    "AREA","ARGO","ARII","ARKA","ARKO","ARMY","ARNA","ARTA","ARTI","ARTO",
    "ASBI","ASDM","ASGR","ASHA","ASII","ASJT","ASLI","ASLC","ASMI","ASPI",
    "ASPR","ASRI","ASRM","ASSA","ATAP","ATIC","ATLA","AUTO","AVIA","AWAN",
    "AXIO","AYAM","AYLS","BABA","BABP","BABY","BACA","BAIK","BAJA","BALI",
    "BANK","BAPA","BAPI","BATA","BATR","BAUT","BAYU","BBCA","BBHI","BBKP",
    "BBLD","BBMD","BBNI","BBRI","BBRM","BBSI","BBSS","BBTN","BBYB","BCAP",
    "BCIC","BCIP","BDKR","BDMN","BEBS","BEEF","BEER","BEKS","BELI","BELL",
    "BESS","BEST","BFIN","BGTG","BHAT","BHIT","BIAS","BIKA","BIKE","BIMA",
    "BINA","BINO","BIPI","BIPP","BIRD","BISI","BIWA","BJBR","BJTM","BKDP",
    "BKSL","BKSW","BLES","BLOG","BLTA","BLTZ","BLUE","BMAS","BMBL","BMHS",
    "BMRI","BMSR","BMTR","BNBA","BNBR","BNGA","BNII","BNLI","BOAT","BOBA",
    "BOGA","BOLA","BOLT","BOSS","BPFI","BPII","BPTR","BRAM","BREN","BRIS",
    "BRMS","BRNA","BRPT","BRRC","BSBK","BSDE","BSIM","BSML","BSSR","BSWD",
    "BTEK","BTEL","BTON","BTPN","BTPS","BUAH","BUDI","BUKA","BUKK","BULL",
    "BUMI","BUVA","BVIC","BWPT","BYAN","CAKK","CAMP","CANI","CARE","CARS",
    "CASA","CASH","CASS","CBDK","CBPE","CBRE","CBUT","CBMF","CCSI","CDIA",
    "CEKA","CENT","CFIN","CGAS","CHEK","CHEM","CHIP","CINT","CITA","CITY",
    "CLAY","CLEO","CLPI","CMNP","CMNT","CMPP","CMRY","CNKO","CNMA","CNTX",
    "COAL","COCO","COIN","COWL","CPIN","CPRI","CPRO","CRAB","CRSN","CSAP",
    "CSIS","CSMI","CSRA","CTBN","CTRA","CTTH","CUAN","CYBR","DAAZ","DADA",
    "DART","DATA","DAYA","DCII","DEAL","DEFI","DEPO","DEWA","DEWI","DFAM",
    "DGNS","DGWG","DGIK","DIGI","DILD","DIVA","DKFT","DKHH","DLTA","DMAS",
    "DMMX","DMND","DNAR","DNET","DOID","DOOH","DOSS","DPNS","DPUM","DRMA",
    "DSFI","DSNG","DSSA","DUCK","DUTI","DVLA","DWGL","DYAN","EAST","ECII",
    "EDGE","EKAD","ELIT","ELPI","ELSA","ELTY","EMAS","EMDE","EMTK","ENAK",
    "ENRG","ENVY","ENZO","EPAC","EPMT","ERAL","ERAA","ERTX","ESIP","ESSA",
    "ESTA","ESTI","ETWA","EURO","EXCL","FAPA","FAST","FASW","FILM","FIMP",
    "FIRE","FISH","FITT","FLMC","FOLK","FOOD","FORE","FORU","FPNI","FUJI",
    "FUTR","FWCT","GAMA","GDST","GDYR","GEMA","GEMS","GGRP","GGRM","GHON",
    "GIAA","GJTL","GLOB","GLVA","GMFI","GMTD","GOLF","GOLD","GOLL","GOOD",
    "GOTO","GPRA","GPSO","GRIA","GRPH","GRPM","GRII","GSMF","GTBO","GTRA",
    "GTSI","GULA","GUNA","GWSA","GZCO","HADE","HAIS","HAJJ","HALO","HATM",
    "HBAT","HDFA","HDIT","HEAL","HELI","HERO","HEXA","HGII","HILL","HITS",
    "HKMU","HMSP","HOKI","HOME","HOMI","HOPE","HOTL","HRME","HRTA","HRUM",
    "HUMI","HYGN","IATA","IBFN","IBOS","IBST","ICBP","ICON","IDEA","IDPR",
    "IFII","IFSH","IGAR","IIKP","IKAI","IKAN","IKBI","IKPM","IMAS","IMJS",
    "IMPC","INAF","INAI","INCF","INCI","INCO","INDF","INDO","INDR","INDS",
    "INDX","INDY","INET","INKP","INOV","INPC","INPP","INPS","INRU","INTA",
    "INTD","INTP","IOTF","IPAC","IPCC","IPCM","IPOL","IPPE","IPTV","IRRA",
    "IRSX","ISAP","ISAT","ISEA","ISSP","ITIC","ITMA","ITMG","JAAS","JARR",
    "JAST","JATI","JAVA","JAYA","JECC","JGLE","JIHD","JKON","JMAS","JPFA",
    "JRPT","JSKY","JSMR","JSPT","JTPE","KAEF","KAQI","KARW","KARY","KAST",
    "KAYU","KBAG","KBLI","KBLM","KBLV","KBRI","KDSI","KDTN","KEEN","KEJU",
    "KETR","KIAS","KICI","KIJA","KING","KINO","KIOS","KJEN","KKES","KKGI",
    "KLAS","KLBF","KLIN","KMDS","KMTR","KOBX","KOCI","KOIN","KOKA","KONI",
    "KOPI","KOTA","KPIG","KRAH","KRAS","KREN","KSIX","KUAS","LABA","LABS",
    "LAJU","LAND","LAPD","LCGP","LCKM","LEAD","LFLO","LIFE","LINK","LION",
    "LIVE","LMAS","LMPI","LMSH","LOPI","LPCK","LPGI","LPIN","LPKR","LPLI",
    "LPPF","LPPS","LRNA","LSIP","LTLS","LUCK","LUCY","MAAS","MABA","MADA",
    "MAGP","MAHA","MAIN","MANG","MAPA","MAPB","MAPI","MARI","MARK","MASA",
    "MASB","MAYA","MBAP","MBMA","MBSS","MBTO","MCAS","MCOL","MCOR","MDIA",
    "MDKA","MDKI","MDLA","MDLN","MDRN","MEDC","MEDS","MEGA","MEJA","MENN",
    "MERI","MERK","META","MFMI","MGNA","MGRO","MHKI","MICE","MIDI","MIKA",
    "MINA","MINE","MIRA","MITI","MKAP","MKPI","MKTR","MLBI","MLIA","MLPL",
    "MLPT","MMLP","MMIX","MNCN","MOLI","MORA","MPOW","MPMX","MPPA","MPRO",
    "MPXL","MRAT","MREI","MSIE","MSIN","MSJA","MSKY","MSTI","MTDL","MTEL",
    "MTFN","MTLA","MTMH","MTPS","MTRA","MTRN","MTSM","MTWI","MUTU","MYOH",
    "MYOR","MYTX","NAIK","NANO","NASA","NASI","NATO","NAYZ","NCKL","NELY",
    "NEST","NETV","NICE","NICK","NICL","NIKL","NINE","NIRO","NISP","NOBU",
    "NPGF","NRCA","NSSS","NTBK","NUSA","NZIA","OASA","OBAT","OBMD","OCAP",
    "OILS","OKAS","OLIV","OMED","OMRE","OPMS","PACK","PADA","PADI","PALM",
    "PAMG","PANI","PANR","PANS","PART","PBID","PBSA","PBRX","PCAR","PDES",
    "PDPP","PEGE","PEHA","PELI","PENT","PERW","PEVE","PGAS","PGEO","PGJO",
    "PGLI","PGUN","PICO","PIPA","PJAA","PJHB","PKPK","PLAN","PLAS","PLIN",
    "PMJS","PMMP","PMUI","PNBN","PNBS","PNGO","PNIN","PNLF","PNSE","POLA",
    "POLI","POLL","POLU","POLY","POOL","PORT","POSA","POWR","PPGL","PPRI",
    "PPRE","PPRO","PRAY","PRDA","PRIM","PSAB","PSAT","PSDN","PSGO","PSKT",
    "PSSI","PTBA","PTDU","PTIS","PTMP","PTMR","PTPP","PTPS","PTPW","PTRO",
    "PTSN","PTSP","PUDP","PURA","PURE","PURI","PWON","PYFA","PZZA","RAAM",
    "RAFI","RAJA","RALS","RANC","RATU","RBMS","RCCC","RDTX","REAL","RELF",
    "RELI","REPP","RGAS","RICY","RIGS","RIMO","RISE","RLCO","RMBA","RMKE",
    "RMKO","RMLP","ROCK","RODA","ROLI","RONY","ROTI","RSCH","RSGK","RUIS",
    "RUNS","SAFE","SAGE","SAGI","SAME","SAMF","SAMR","SAMP","SANO","SAPX",
    "SATU","SBAT","SBMA","SCCO","SCMA","SCNP","SCPI","SDMU","SDPC","SDRA",
    "SEMA","SFAN","SGER","SGGH","SGJL","SGRO","SHID","SHIP","SICO","SIDO",
    "SIER","SILO","SIMA","SIMP","SINI","SIPD","SKBM","SKLT","SKRN","SKYB",
    "SLIS","SMAR","SMDM","SMDR","SMGA","SMGR","SMKM","SMKL","SMLE","SMMA",
    "SMMT","SMRA","SMRU","SMSM","SNLK","SOCI","SOFA","SOHO","SOLA","SONA",
    "SOSS","SOTS","SOUL","SPMA","SPRE","SPTO","SQMI","SRAJ","SREI","SRIL",
    "SRSN","SRTG","SSIA","SSMS","SSTM","STAA","STAR","STRK","STTP","SUGI",
    "SULI","SUNI","SUPA","SUPR","SURE","SWAT","SWID","SYAI","TALF","TAMA",
    "TAMU","TAPG","TARA","TAXI","TAYS","TBIG","TBLA","TBMS","TCID","TCPI",
    "TDPM","TEBE","TECH","TELE","TFAS","TFCO","TGKA","TGRA","TGUK","TIFA",
    "TINS","TIRA","TIRT","TKIM","TLDN","TLKM","TMAS","TMPO","TNCA","TOBA",
    "TOOL","TOPS","TOSK","TOTL","TOTO","TOWR","TOYS","TPAI","TPIA","TPMA",
    "TRAM","TRGU","TRIL","TRIM","TRIN","TRIO","TRIS","TRJA","TRON","TRST",
    "TRUE","TRUK","TRUS","TSPC","TUGU","TULT","TYRE","UANG","UCID","UDNG",
    "UFOE","ULTJ","UNIC","UNIQ","UNIT","UNSP","UNTR","UNVR","URBN","UVCR",
    "VAST","VATE","VCOK","VERN","VICI","VICO","VINS","VISA","VISI","VIVA",
    "VKTR","VOKS","VOSS","VRNA","VTNY","WAPO","WBSA","WEGE","WEHA","WGSH",
    "WICO","WIDI","WIFI","WIIM","WIKA","WINE","WINR","WINS","WIRG","WITA",
    "WMPP","WMUU","WOMF","WONS","WOOD","WOWS","WPOW","WSBP","WSKT","WTON",
    "YELO","YOII","YPAS","YULE","YUPI","ZATA","ZBRA","ZENI","ZINC","ZONE","ZYRX"
]
seen=set(); raw_stocks=[x for x in raw_stocks if not(x in seen or seen.add(x))]
stocks_yf=[s+".JK" for s in raw_stocks]
stock_map={s+".JK":s for s in raw_stocks}

# ════ MARKET REGIME — pakai Ticker().history() ════
@st.cache_data(ttl=300)
def get_market_regime():
    try:
        df = _fetch_yf_ticker("^JKSE", "60d", "1d")  # ← no yf.download!
        if df is None or len(df) < 10:
            return ("UNKNOWN", 0, 0, 0, "Data IHSG kurang", 0.0)
        close = df["Close"].dropna()
        if len(close) < 10: return ("UNKNOWN", 0, 0, 0, "Data close kurang", 0.0)
        ema20 = float(close.ewm(span=20, adjust=False).mean().iloc[-1])
        ema55 = float(close.ewm(span=min(55, len(close)-1), adjust=False).mean().iloc[-1])
        price = float(close.iloc[-1])
        chg   = float(((close.iloc[-1] - close.iloc[-2]) / close.iloc[-2]) * 100)
        band=0.012; pct_vs_e20=(price-ema20)/ema20*100
        above_e20_clear=price>ema20*(1+band); above_e20_any=price>ema20*(1-band)
        above_e55=price>ema55; recovering=chg>0.3
        bearish_confirm=chg<-0.3 and not above_e20_any
        if above_e20_clear and above_e55:
            return ("GREEN", price, ema20, ema55, f"IHSG {price:,.0f} > EMA20 & EMA55 → Bullish ✅ ({pct_vs_e20:+.1f}% vs EMA20)", chg)
        elif above_e20_any and above_e55:
            return ("GREEN", price, ema20, ema55, f"IHSG {price:,.0f} dekat EMA20 & di atas EMA55 → Bullish", chg)
        elif above_e20_any and not above_e55:
            return ("SIDEWAYS", price, ema20, ema55, f"IHSG {price:,.0f} > EMA20 tapi < EMA55({ema55:,.0f}) → Sideways", chg)
        elif not above_e20_any and recovering:
            return ("SIDEWAYS", price, ema20, ema55, f"IHSG {price:,.0f} recovery {chg:+.2f}% → Sideways", chg)
        elif bearish_confirm:
            return ("RED", price, ema20, ema55, f"IHSG {price:,.0f} < EMA20({ema20:,.0f}) {pct_vs_e20:+.1f}% → Bearish", chg)
        else:
            return ("SIDEWAYS", price, ema20, ema55, f"IHSG {price:,.0f} sedikit di bawah EMA20 → Sideways", chg)
    except Exception as e:
        return ("UNKNOWN", 0, 0, 0, f"IHSG error: {str(e)[:40]}", 0.0)

def get_regime_config(regime):
    # Threshold realistis — sebelumnya terlalu ketat (min_rvol 2.0 = 90% saham gak lolos)
    return {
        "RED":     {"mode":"Reversal 🎯","min_score":3,"min_rvol":1.2,"sl_mult":0.6,
                    "label":"🔴 MARKET MERAH — Reversal Only, Score ≥ 3","color":"#ff3d5a",
                    "desc":"Market bearish. Fokus reversal oversold, filter moderat."},
        "GREEN":   {"mode":"Bagger 💎","min_score":3,"min_rvol":1.3,"sl_mult":0.8,
                    "label":"🟢 MARKET HIJAU — Wyckoff Bagger Hunt, Score ≥ 3","color":"#00ff88",
                    "desc":"Market bullish. Cari akumulasi Wyckoff + breakout bagger."},
        "SIDEWAYS":{"mode":"Scalping ⚡","min_score":3,"min_rvol":1.3,"sl_mult":0.7,
                    "label":"🟡 MARKET SIDEWAYS — Semua Mode, RVOL ≥ 1.3x","color":"#ffb700",
                    "desc":"Market sideways. Cari momentum + volume confirmation."},
        "UNKNOWN": {"mode":"Scalping ⚡","min_score":2,"min_rvol":1.0,"sl_mult":0.8,
                    "label":"⚪ REGIME UNKNOWN — Manual Mode","color":"#4a5568","desc":""},
    }.get(regime,{"mode":"Scalping ⚡","min_score":2,"min_rvol":1.0,"sl_mult":0.8,"label":"⚪","color":"#4a5568","desc":""})

# ════ PIVOT — pakai Ticker().history() ════
@st.cache_data(ttl=3600)
def fetch_pivot_data(ticker_yf):
    try:
        df = _fetch_yf_ticker(ticker_yf, "5d", "1d")  # ← no yf.download!
        if df is None or len(df) < 2: return None
        prev=df.iloc[-2]; h=float(prev["High"]); l=float(prev["Low"]); c=float(prev["Close"])
        pp=(h+l+c)/3
        return {"PP":pp,"R1":2*pp-l,"R2":pp+(h-l),"S1":2*pp-h,"S2":pp-(h-l)}
    except: return None

def get_pivot_position(price, pivots):
    if pivots is None: return "Unknown","#4a5568"
    pp=pivots["PP"]
    if price>pivots.get("R2",pp*1.02):   return "Above R2 🔴","#ff3d5a"
    elif price>pivots.get("R1",pp*1.01): return "R1→R2 🟠","#ff7b00"
    elif price>pp:                        return "PP→R1 🟢","#00ff88"
    elif price>pivots.get("S1",pp*0.99): return "S1→PP 🟡","#ffb700"
    elif price>pivots.get("S2",pp*0.98): return "S2→S1 🔴","#ff3d5a"
    else:                                 return "Below S2 🔴","#ff3d5a"

# ════ SECTOR — pakai _fetch_yf_parallel ════
@st.cache_data(ttl=300)
def fetch_sector_rotation(sector_stocks):
    """Parallel Ticker().history() — no yf.download(), no rate limit."""
    tickers_yf = [s+".JK" for s in sector_stocks[:10]]
    raw = _fetch_yf_parallel(tickers_yf, "3d", "1d", workers=5)
    results = []
    for t, df in raw.items():
        try:
            if df is None or len(df) < 2: continue
            close=float(df["Close"].iloc[-1]); prev=float(df["Close"].iloc[-2])
            chg=(close-prev)/prev*100; vol=float(df["Volume"].iloc[-1])
            avgv=float(df["Volume"].mean()); rvol=vol/avgv if avgv>0 else 1.0
            results.append({"ticker":t.replace(".JK",""),"close":close,"chg":round(chg,2),"rvol":round(rvol,2)})
        except: continue
    return results

# ════ GAP UP — pakai _fetch_yf_parallel ════
@st.cache_data(ttl=300)
def scan_gap_up(tickers_yf, min_gap_pct=0.5):
    """Parallel Ticker().history() — no batched yf.download(), no rate limit."""
    raw = _fetch_yf_parallel(list(tickers_yf), "5d", "1d", workers=6)
    results = []
    for t, df in raw.items():
        tkr = t.replace(".JK","")
        try:
            if df is None or len(df) < 3: continue
            today=df.iloc[-1]; prev=df.iloc[-2]
            close=float(today["Close"]); high_t=float(today["High"]); low_t=float(today["Low"])
            high_p=float(prev["High"]); vol=float(today["Volume"])
            avg_vol=float(df["Volume"].mean()); rvol=vol/avg_vol if avg_vol>0 else 1.0
            gap_score=0; reasons=[]
            if close>high_p:
                gap_pct=(close-high_p)/high_p*100; gap_score+=3
                reasons.append(f"Gap {gap_pct:.1f}% above prev High ✦✦")
            close_ratio=(close-low_t)/max(high_t-low_t,1)
            if close_ratio>0.85:   gap_score+=2; reasons.append(f"Tutup dekat High {close_ratio:.0%}")
            elif close_ratio>0.70: gap_score+=1; reasons.append(f"Tutup kuat {close_ratio:.0%}")
            if rvol>3.0:   gap_score+=2; reasons.append(f"RVOL={rvol:.1f}x SURGE 🔥")
            elif rvol>2.0: gap_score+=1; reasons.append(f"RVOL={rvol:.1f}x")
            elif rvol>1.5: gap_score+=0.5
            if len(df)>=3:
                chg3=(close-float(df.iloc[-3]["Close"]))/float(df.iloc[-3]["Close"])*100
                if chg3>3: gap_score+=1; reasons.append(f"3D ROC +{chg3:.1f}%")
                elif chg3>1: gap_score+=0.5
            if gap_score<3: continue
            chg_today=(close-float(prev["Close"]))/float(prev["Close"])*100
            results.append({"Ticker":tkr,"Price":int(close),"Gap Score":round(gap_score,1),
                            "Chg %":round(chg_today,2),"Close Ratio":round(close_ratio,2),
                            "RVOL":round(rvol,2),"Prev High":int(high_p),
                            "Signal":"GAP UP 🚀" if gap_score>=4 else "POTENTIAL ⚡",
                            "Reasons":" · ".join(reasons[:3])})
        except: continue
    return sorted(results, key=lambda x:x["Gap Score"], reverse=True)

# ════ INDICATORS ════
def ema(s, n): return s.ewm(span=n, adjust=False).mean()

def apply_intraday_indicators(df):
    if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.droplevel(1)
    c = df["Close"]
    df["EMA9"]=ema(c,9); df["EMA21"]=ema(c,21); df["EMA50"]=ema(c,50); df["EMA200"]=ema(c,200)
    d=c.diff(); g=d.clip(lower=0).ewm(span=14,adjust=False).mean()
    l=(-d.clip(upper=0)).ewm(span=14,adjust=False).mean()
    rsi_raw=(100-100/(1+g/l.replace(0,np.nan))).fillna(50)
    df["RSI"]=rsi_raw; df["RSI_EMA"]=rsi_raw.ewm(span=14,adjust=False).mean()
    d5=c.diff(); g5=d5.clip(lower=0).ewm(span=5,adjust=False).mean()
    l5=(-d5.clip(upper=0)).ewm(span=5,adjust=False).mean()
    df["RSI5"]=(100-100/(1+g5/l5.replace(0,np.nan))).fillna(50)
    lo10=df["Low"].rolling(10).min(); hi10=df["High"].rolling(10).max()
    raw_k=(100*(c-lo10)/(hi10-lo10).replace(0,np.nan)).fillna(50)
    df["STOCH_K"]=raw_k.ewm(span=5,adjust=False).mean()
    df["STOCH_D"]=df["STOCH_K"].ewm(span=5,adjust=False).mean()
    df["STOCH_CROSS_UP"]=(df["STOCH_K"]>df["STOCH_D"])&(df["STOCH_K"].shift(1)<=df["STOCH_D"].shift(1))
    df["STOCH_CROSS_DOWN"]=(df["STOCH_K"]<df["STOCH_D"])&(df["STOCH_K"].shift(1)>=df["STOCH_D"].shift(1))
    ema12=c.ewm(span=12,adjust=False).mean(); ema26=c.ewm(span=26,adjust=False).mean()
    macd_line=ema12-ema26; signal_line=macd_line.ewm(span=9,adjust=False).mean()
    df["MACD"]=macd_line; df["MACD_Sig"]=signal_line
    df["MACD_Hist"]=(macd_line-signal_line).fillna(0)
    df["MACD_CROSS_UP"]=(macd_line>signal_line)&(macd_line.shift(1)<=signal_line.shift(1))
    df["MACD_CROSS_DOWN"]=(macd_line<signal_line)&(macd_line.shift(1)>=signal_line.shift(1))
    try:
        tp=(df["High"]+df["Low"]+df["Close"])/3
        df["VWAP"]=(tp*df["Volume"]).cumsum()/df["Volume"].cumsum()
        # FIX: kalau volume cumsum = 0 (semua bar vol=0) → VWAP NaN → fill dengan Close
        df["VWAP"] = df["VWAP"].fillna(c)
    except: df["VWAP"]=c
    df["BB_mid"]=c.rolling(20).mean(); df["BB_std"]=c.rolling(20).std()
    df["BB_upper"]=df["BB_mid"]+2*df["BB_std"]; df["BB_lower"]=df["BB_mid"]-2*df["BB_std"]
    df["BB_pct"]=(c-df["BB_lower"])/(df["BB_upper"]-df["BB_lower"])
    df["AvgVol"]=df["Volume"].rolling(20).mean()
    df["RVOL"]=df["Volume"]/df["AvgVol"].replace(0,np.nan)
    tr=pd.concat([df["High"]-df["Low"],(df["High"]-c.shift()).abs(),(df["Low"]-c.shift()).abs()],axis=1).max(axis=1)
    df["ATR"]=tr.rolling(14).mean()
    body_top=df[["Close","Open"]].max(axis=1); body_bot=df[["Close","Open"]].min(axis=1)
    hl=(df["High"]-df["Low"]).replace(0,np.nan)
    df["LWick"]=((body_bot-df["Low"])/hl*100).fillna(0)
    df["UWick"]=((df["High"]-body_top)/hl*100).fillna(0)
    df["Body"]=(body_top-body_bot)/hl*100
    df["BodyRatio"]=(body_top-body_bot)/hl.fillna(0)
    df["BullBar"]=(df["Close"]>df["Open"])&(df["BodyRatio"]>0.5)
    df["NetVol"]=np.where(c>=df["Open"],df["Volume"],-df["Volume"])
    df["NetVol3"]=pd.Series(df["NetVol"],index=df.index).rolling(3).sum()
    df["NetVol8"]=pd.Series(df["NetVol"],index=df.index).rolling(8).sum()
    df["VolSpike"]=df["RVOL"]>2.5
    df["ROC3"]=c.pct_change(3); df["ROC8"]=c.pct_change(8)
    df["HH"]=df["High"]>df["High"].shift(1); df["HL"]=df["Low"]>df["Low"].shift(1)
    df["LL"]=df["Low"]<df["Low"].shift(1);   df["LH"]=df["High"]<df["High"].shift(1)
    if "FBuy" in df.columns and "FSell" in df.columns:
        df["FNet"]=df["FBuy"]-df["FSell"]; df["FCum"]=df["FNet"].cumsum()
        df["FNet3"]=df["FNet"].rolling(3).sum(); df["FNet8"]=df["FNet"].rolling(8).sum()
        tot=df["FBuy"]+df["FSell"]
        df["FRatio"]=(df["FBuy"]/tot.replace(0,np.nan)).fillna(0.5)
    return df

# ════ SCORING ════
def score_scalping(r, p, p2):
    score=0; reasons=[]
    if r["EMA9"]>r["EMA21"]>r["EMA50"]:  score+=1.5; reasons.append("EMA stack ▲")
    elif r["EMA9"]>r["EMA21"]:            score+=0.8; reasons.append("EMA9>21")
    if r["Close"]>r["VWAP"]:             score+=1;   reasons.append("Above VWAP")
    if r["MACD_Hist"]>0 and r["MACD_Hist"]>float(p["MACD_Hist"]):
        score+=1.5; reasons.append("MACD hist expanding ✦")
        if p2 is not None and float(p["MACD_Hist"])>float(p2["MACD_Hist"]): score+=0.3
    elif r["MACD_Hist"]>0: score+=0.5; reasons.append("MACD hist +")
    rsi_e=float(r["RSI_EMA"])
    if 52<rsi_e<68:  score+=0.8; reasons.append(f"RSI-EMA={rsi_e:.1f}")
    elif rsi_e>=68:  score-=0.5
    rvol=float(r["RVOL"])
    if rvol>2.0:   score+=1;   reasons.append(f"RVOL={rvol:.1f}x surge")
    elif rvol>1.5: score+=0.6; reasons.append(f"RVOL={rvol:.1f}x")
    if bool(r["BullBar"]):    score+=0.5; reasons.append("Bullish bar")
    if float(r["NetVol3"])>0: score+=0.4; reasons.append("Net vol +")
    if r["Close"]<r["EMA200"]*0.98: score-=0.5
    return max(0,min(6,round(score,1))), reasons, {}

def score_momentum(r, p, p2):
    score=0; reasons=[]
    if bool(r["HH"]) and bool(r["HL"]): score+=1.5; reasons.append("HH+HL pattern ▲")
    elif bool(r["HH"]): score+=0.8
    rvol=float(r["RVOL"])
    if rvol>3.0:   score+=1.5; reasons.append(f"RVOL={rvol:.1f}x SURGE 🔥")
    elif rvol>2.0: score+=1.0; reasons.append(f"RVOL={rvol:.1f}x")
    elif rvol>1.5: score+=0.5
    roc=float(r["ROC3"])*100
    if roc>2.0:   score+=1.5; reasons.append(f"ROC3={roc:.1f}%")
    elif roc>1.0: score+=0.8; reasons.append(f"ROC3={roc:.1f}%")
    elif roc<0:   score-=0.5
    rsi_e=float(r["RSI_EMA"])
    if 55<rsi_e<75: score+=0.8; reasons.append(f"RSI-EMA={rsi_e:.1f}")
    if rsi_e>78:    score-=0.8; reasons.append("⚠️ RSI overbought")
    sk=float(r["STOCH_K"]); sd=float(r["STOCH_D"])
    if sk>60 and sk>sd: score+=0.8; reasons.append("STOCH K>D bullish")
    if r["MACD_Hist"]>0 and r["MACD_Hist"]>float(p["MACD_Hist"]): score+=0.8; reasons.append("MACD expanding")
    if r["Close"]>r["VWAP"]: score+=0.5; reasons.append("Above VWAP")
    return max(0,min(6,round(score,1))), reasons, {}

def score_reversal(r, p, p2):
    score=0; reasons=[]; os_count=0
    rsi_e=float(r["RSI_EMA"])
    if rsi_e<30:   os_count+=1; score+=1.5; reasons.append(f"RSI-EMA={rsi_e:.1f} OS extreme")
    elif rsi_e<40: os_count+=1; score+=0.8; reasons.append(f"RSI-EMA={rsi_e:.1f} OS")
    sk=float(r["STOCH_K"]); sd=float(r["STOCH_D"])
    if sk<20:   os_count+=1; score+=1;   reasons.append(f"STOCH={sk:.0f} extreme OS")
    elif sk<30: os_count+=1; score+=0.5
    bp=float(r["BB_pct"])
    if bp<0.05:   os_count+=1; score+=1;   reasons.append("BB lower touch")
    elif bp<0.15: os_count+=1; score+=0.5
    if os_count<1.5: return 0,[],{}
    rev=0; pk=float(p["STOCH_K"]); pd_=float(p["STOCH_D"])
    if sk<30 and sk>sd and pk<=pd_:   rev+=1; score+=2;   reasons.append("STOCH %K cross ↑ OS ✦✦")
    elif sk<25 and sk>sd:             rev+=1; score+=1.2; reasons.append("STOCH K>D extreme OS")
    rsi_p=float(p["RSI_EMA"])
    if rsi_e>rsi_p and rsi_e<42: rev+=1; score+=1.2; reasons.append("RSI-EMA pivot ↑")
    mh=float(r["MACD_Hist"]); mph=float(p["MACD_Hist"])
    if mh>mph and mh<0: rev+=1; score+=0.8; reasons.append("MACD hist diverge ↑")
    if rev==0: score*=0.3
    if bool(r["VolSpike"]) and float(r["Close"])<float(r["Open"]): score+=0.8; reasons.append("Volume climax sell")
    elif float(r["RVOL"])>1.5: score+=0.4
    if float(r["NetVol3"])>0: score+=0.5; reasons.append("Net vol turning +")
    if float(r["BodyRatio"])>0.75 and float(r["Close"])<float(r["Open"]): score-=0.8; reasons.append("⚠️ Bearish bar kuat")
    return max(0,min(6,round(score,1))), reasons, {}

def score_bagger(r, p, p2, df_full):
    def _sf(v, d=0.):
        try: x=float(v); return d if(np.isnan(x) or np.isinf(x)) else x
        except: return d
    score=0; reasons=[]
    try: close=float(r["Close"])
    except: close=0
    e9=_sf(r.get("EMA9",0)); e21=_sf(r.get("EMA21",0))
    e50=_sf(r.get("EMA50",0)); e200=_sf(r.get("EMA200",0))
    rvol=_sf(r.get("RVOL",1)); rsi_e=_sf(r.get("RSI_EMA",50))
    wyckoff_phase="SCANNING"; is_sideways=False
    range_high=close*1.05; range_low=close*0.95
    sideways_bars=min(20,len(df_full)-2)
    try:
        rh=float(df_full["High"].iloc[-sideways_bars-1:-1].max())
        rl=float(df_full["Low"].iloc[-sideways_bars-1:-1].min())
        rp=(rh-rl)/max(rl,0.01)*100; is_sideways=rp<8.0
        range_high=rh; range_low=rl
        if is_sideways:
            score+=1.0+max(0,(8.0-rp)/8.0)*0.5; reasons.append(f"Sideways {rp:.1f}% ({sideways_bars}B) ✦")
            wyckoff_phase="A-B"
    except: pass
    try:
        vm20=float(df_full["AvgVol"].iloc[-1]); vl5=float(df_full["Volume"].iloc[-6:-1].mean())
        dr=vl5/max(vm20,1)
        if dr<0.5 and is_sideways:   score+=2.0; reasons.append(f"Dry vol {dr:.2f}x stealth accum ✦✦"); wyckoff_phase="A-B AKUMULASI"
        elif dr<0.7 and is_sideways: score+=1.2; reasons.append(f"Vol drying {dr:.2f}x ✦"); wyckoff_phase="A-B AKUMULASI"
        elif dr<0.85 and is_sideways:score+=0.6; reasons.append(f"Vol below avg {dr:.2f}x")
    except: pass
    try:
        if "FBuy" in df_full.columns and "FSell" in df_full.columns:
            fb5=float(df_full["FBuy"].iloc[-6:-1].sum()); fs5=float(df_full["FSell"].iloc[-6:-1].sum())
            ft5=fb5+fs5
            if ft5>0:
                fr5=fb5/ft5
                if fr5>0.65 and is_sideways: score+=2.0; reasons.append(f"🔵 Asing stealth accum {fr5:.0%} buy ✦✦"); wyckoff_phase="A-B AKUMULASI"
                elif fr5>0.55: score+=1.0; reasons.append(f"Asing net buy {fr5:.0%} ✦")
        else:
            if len(df_full)>=12:
                nv=[float(df_full["NetVol"].iloc[i]) for i in range(-11,-1)]
                np_=sum(1 for v in nv if v>0); nr=np_/10
                if nr>=0.7 and is_sideways: score+=1.5; reasons.append(f"Stealth net buy {np_}/10 bars ✦✦")
                elif nr>=0.6: score+=0.8; reasons.append(f"Net buy {np_}/10 bars")
                elif nr>=0.5: score+=0.4
    except:
        nv3=_sf(r.get("NetVol3",0)); nv8=_sf(r.get("NetVol8",0))
        if nv3>0 and nv8>0: score+=0.8; reasons.append("Net buyer sustained ✦")
        elif nv3>0: score+=0.3
    try:
        bc=float(r["BB_std"]); ba=float(df_full["BB_std"].iloc[-11:-1].mean())
        sq=bc/max(ba,0.0001)
        if sq<0.7 and is_sideways: score+=1.5; reasons.append(f"BB squeeze extreme {sq:.2f}x ✦✦")
        elif sq<0.85: score+=0.8; reasons.append(f"BB squeeze {sq:.2f}x")
    except: pass
    spring_detected=False
    try:
        lb=min(15,len(df_full)-3); pl=df_full["Low"].iloc[-lb-2:-2]
        sup=float(pl.min()); bl=float(r["Low"]); bc2=float(r["Close"]); bh=float(r["High"])
        if bl<sup and bc2>sup:
            rc=(bc2-bl)/max(bh-bl,0.0001)
            if rc>0.7 and rvol>1.2: score+=3.0; reasons.append(f"🔥 SPRING! Support break → rebound {rc:.0%} ✦✦✦"); wyckoff_phase="SPRING ⚡"; spring_detected=True
            elif rc>0.5: score+=1.8; reasons.append(f"Spring pattern (recovery {rc:.0%}) ✦✦"); wyckoff_phase="SPRING"; spring_detected=True
        if float(p["Low"])<sup and float(p["Close"])>sup and bc2>float(p["Close"]) and not spring_detected:
            score+=2.0; reasons.append("Post-spring confirmation 🚀 ✦✦"); wyckoff_phase="POST-SPRING"; spring_detected=True
    except: pass
    try:
        ar=close>range_high*0.998; tb=_sf(r.get("BodyRatio",0))>0.55; bf=float(r["Close"])>float(r["Open"])
        if rvol>4.0 and ar and tb and bf:  score+=3.0; reasons.append(f"🚀 PHASE D! RVOL={rvol:.1f}x ✦✦✦"); wyckoff_phase="PHASE D 🚀"
        elif rvol>3.0 and ar and bf:       score+=2.2; reasons.append(f"Breakout confirmed RVOL={rvol:.1f}x ✦✦"); wyckoff_phase="BREAKOUT ✦"
        elif rvol>2.0 and ar:              score+=1.5; reasons.append(f"Breakout attempt RVOL={rvol:.1f}x")
        elif ar:                           score+=0.8; reasons.append("Above resistance (low vol)")
        else:
            if rvol>4.0:   score+=1.5; reasons.append(f"RVOL={rvol:.1f}x MASSIVE 🔥🔥")
            elif rvol>3.0: score+=1.0; reasons.append(f"RVOL={rvol:.1f}x SURGE 🔥")
            elif rvol>2.0: score+=0.5; reasons.append(f"RVOL={rvol:.1f}x")
            elif rvol<1.3 and wyckoff_phase not in ["A-B AKUMULASI","SPRING","POST-SPRING"]: score-=0.5
    except:
        if rvol>4.0:   score+=1.5; reasons.append(f"RVOL={rvol:.1f}x MASSIVE 🔥🔥")
        elif rvol>3.0: score+=1.0; reasons.append(f"RVOL={rvol:.1f}x SURGE 🔥")
        elif rvol>2.0: score+=0.5
    if e9>e21>e50>e200: score+=1.5; reasons.append("EMA golden stack ✦✦")
    elif e9>e21>e50:    score+=1.0; reasons.append("EMA stack ▲")
    elif e9>e21:        score+=0.4
    elif is_sideways and wyckoff_phase in ["A-B AKUMULASI","SPRING","POST-SPRING"]: score+=0.2
    if wyckoff_phase in ["A-B","A-B AKUMULASI","SPRING","POST-SPRING"]:
        if 25<=rsi_e<=52:  score+=1.0; reasons.append(f"RSI-EMA={rsi_e:.1f} accum zone ✓")
        elif rsi_e<25:     score+=0.6
        elif rsi_e>65:     score-=0.3
    else:
        if 52<rsi_e<72:   score+=1.0; reasons.append(f"RSI-EMA={rsi_e:.1f} momentum zone")
        elif rsi_e>=72:   score-=0.5; reasons.append(f"⚠️ RSI OB {rsi_e:.1f}")
        elif rsi_e<40:    score-=0.3
    if close>_sf(r.get("VWAP",close)): score+=0.5; reasons.append("Above VWAP")
    if e200>0 and close<e200*0.88:     score-=1.0
    try:
        if len(df_full)>=4:
            bcc=sum(1 for i in range(-3,0) if float(df_full["Close"].iloc[i])>float(df_full["Open"].iloc[i]))
            if bcc==3: score+=0.8; reasons.append("3x consecutive bull bars")
            elif bcc==2: score+=0.3
    except: pass
    if wyckoff_phase!="SCANNING": reasons.insert(0,f"⚙️ Wyckoff: {wyckoff_phase}")
    return max(0,min(6,round(score,1))),reasons,{"wyckoff_phase":wyckoff_phase}

def get_signal(score, mode):
    # Threshold diturunin biar low-score juga dapet label (gak WAIT)
    t={"Scalping ⚡":{5:"GACOR ⚡",   4:"POTENSIAL 🔥",3:"WATCH 👀",  2:"LEMAH 🟡",1:"MARGINAL 🔸"},
       "Momentum 🚀":{5:"GACOR 🚀",   4:"POTENSIAL 🔥",3:"WATCH 👀",  2:"LEMAH 🟡",1:"MARGINAL 🔸"},
       "Reversal 🎯":{5:"REVERSAL 🎯",4:"POTENSIAL 🔥",3:"WATCH 👀",  2:"LEMAH 🟡",1:"MARGINAL 🔸"},
       "Bagger 💎":  {5:"BAGGER 💎",  4:"KANDIDAT 🚀", 3:"WATCH 👀",  2:"LEMAH 🟡",1:"MARGINAL 🔸"}}.get(mode,{})
    for th in sorted(t.keys(),reverse=True):
        if score>=th: return t[th]
    return "WAIT"

def get_card_class(signal):
    if "BAGGER" in signal or "KANDIDAT" in signal: return "bagger"
    if "GACOR" in signal or "REVERSAL" in signal:  return "gacor"
    if "POTENSIAL" in signal:                      return "potensial"
    if "WATCH" in signal:                          return "watch"
    return ""

def score_bsjp(r, p, p2):
    score=0; reasons=[]
    hi_lo=float(r["High"])-float(r["Low"])
    close_pct=(float(r["Close"])-float(r["Low"]))/max(hi_lo,1)
    if close_pct>0.7:   score+=2;   reasons.append(f"Tutup dekat High ({close_pct:.0%})")
    elif close_pct>0.5: score+=1;   reasons.append(f"Tutup kuat ({close_pct:.0%})")
    rvol=float(r["RVOL"])
    if rvol>3.0:   score+=2;   reasons.append(f"RVOL={rvol:.1f}x SURGE 🔥")
    elif rvol>2.0: score+=1.5; reasons.append(f"RVOL={rvol:.1f}x kuat")
    elif rvol>1.5: score+=0.8; reasons.append(f"RVOL={rvol:.1f}x")
    if r["EMA9"]>r["EMA21"]>r["EMA50"]: score+=1.5; reasons.append("EMA stack ▲")
    elif r["EMA9"]>r["EMA21"]:           score+=0.8; reasons.append("EMA9>21")
    rsi_e=float(r["RSI_EMA"])
    if 45<rsi_e<70:  score+=1;   reasons.append(f"RSI-EMA={rsi_e:.1f} ✓")
    elif rsi_e>=70:  score-=1;   reasons.append(f"RSI-EMA={rsi_e:.1f} OB ⚠️")
    elif rsi_e<40:   score+=0.5; reasons.append(f"RSI-EMA={rsi_e:.1f} oversold")
    if float(r["MACD_Hist"])>0 and float(r["MACD_Hist"])>float(p["MACD_Hist"]):
        score+=1; reasons.append("MACD hist expanding ✦")
    elif float(r["MACD_Hist"])>0: score+=0.5; reasons.append("MACD +")
    if float(r["Close"])>float(r["VWAP"]): score+=0.5; reasons.append("Above VWAP")
    return max(0,min(6,round(score,1))),reasons,{}

def calc_trailing_stop(entry, current, atr, method="ATR", atr_mult=2.0, pct=3.0):
    if method=="ATR":      td=atr*atr_mult; sp=current-td
    elif method=="Persen": td=current*(pct/100); sp=current*(1-pct/100)
    else:                  td=atr*1.5; sp=current-td
    return{"stop":round(sp,0),"distance":round(td,0),
           "profit_float":round((current-entry)/entry*100,2),
           "profit_locked":round((sp-entry)/entry*100,2) if sp>entry else 0,
           "is_profitable":sp>entry}

def get_sinyal_v2(r, p, p2):
    def sf(v,d=0.):
        try: x=float(v); return d if(np.isnan(x) or np.isinf(x)) else x
        except: return d
    cl=sf(r.get("Close",0)); e9=sf(r.get("EMA9")); e21=sf(r.get("EMA21")); e50=sf(r.get("EMA50"))
    rsi_ema=sf(r.get("RSI_EMA",50)); rsi_ema_p=sf(p.get("RSI_EMA",50))
    sk=sf(r.get("STOCH_K",50)); sd=sf(r.get("STOCH_D",50))
    sk_p=sf(p.get("STOCH_K",50)); sd_p=sf(p.get("STOCH_D",50))
    mh=sf(r.get("MACD_Hist",0)); mh_p=sf(p.get("MACD_Hist",0))
    macd_v=sf(r.get("MACD",0)); sig_v=sf(r.get("MACD_Sig",0))
    macd_p=sf(p.get("MACD",0)); sig_p=sf(p.get("MACD_Sig",0))
    rv=sf(r.get("RVOL",1)); lw=sf(r.get("LWick",0)); uw=sf(r.get("UWick",0))
    body=sf(r.get("Body",50)); vwap_v=sf(r.get("VWAP",cl))
    score=0; flags=[]
    ema_bull=e9>e21>e50; ema_gc=e9>e21; ema_bear=e9<e21<e50
    p_e9=sf(p.get("EMA9")); p_e21=sf(p.get("EMA21"))
    gc_now=(e9>e21) and (p_e9<=p_e21)
    if ema_bull:   score+=15; flags.append("EMA▲")
    elif ema_gc:   score+=8;  flags.append("EMA GC")
    elif ema_bear: score-=12
    stoch_os=sk<20; stoch_ob=sk>80; stoch_cu=sk>sd and sk_p<=sd_p
    if stoch_os:
        score+=12; flags.append(f"STOCH OS {sk:.0f}")
        if stoch_cu: score+=8; flags.append("STOCH ↑")
    elif stoch_ob: score-=10
    elif stoch_cu and sk<60: score+=6; flags.append("STOCH ↑")
    rsi_os=rsi_ema<40; rsi_os2=rsi_ema<30; rsi_ob=rsi_ema>65
    rsi_cu=rsi_ema>rsi_ema_p and rsi_ema_p<40
    if rsi_os2:   score+=12; flags.append(f"RSI {rsi_ema:.0f} OS")
    elif rsi_os:  score+=7;  flags.append(f"RSI {rsi_ema:.0f}")
    elif 45<rsi_ema<65: score+=5
    elif rsi_ob:  score-=8
    if rsi_cu:    score+=6; flags.append("RSI ↑")
    macd_cu=macd_v>sig_v and macd_p<=sig_p; macd_cd=macd_v<sig_v and macd_p>=sig_p
    macd_exp=mh>0 and mh>mh_p
    if macd_cu:    score+=10; flags.append("MACD ↑")
    elif macd_exp: score+=7;  flags.append("MACD Exp")
    elif mh>0:     score+=3
    elif macd_cd:  score-=10; flags.append("MACD ↓")
    elif mh<0 and mh<mh_p: score-=5
    if rv>3:    score+=15; flags.append(f"RVOL {rv:.1f}x 🔥")
    elif rv>2:  score+=10; flags.append(f"RVOL {rv:.1f}x")
    elif rv>1.5:score+=5;  flags.append(f"RVOL {rv:.1f}x")
    elif rv<0.5:score-=5
    if lw>60:   score+=10; flags.append(f"LWick {lw:.0f}%")
    elif lw>40: score+=6
    elif lw>25: score+=3
    uw_sell=uw>50 and body<30
    if uw_sell: flags.append(f"UWick JUAL {uw:.0f}%")
    if cl>vwap_v:   score+=5; flags.append("VWAP▲")
    elif cl<vwap_v: score-=3
    fnet3=sf(r.get("FNet3",0)); fnet8=sf(r.get("FNet8",0)); fratio=sf(r.get("FRatio",0.5))
    if fnet3>0 and fnet8>0:
        score+=10; flags.append("🔵 Asing Akum")
        if fratio>0.7: score+=5; flags.append("Asing Dom")
    elif fnet3<0 and fnet8<0:
        score-=8; flags.append("🔴 Asing Jual")
    entry_kuat=((stoch_os or (sk<50 and ema_bear)) and (rsi_os or rsi_cu) and (macd_cu or macd_exp) and rv>=1.2)
    entry_mod=sum([stoch_os or stoch_cu, rsi_os or rsi_cu, macd_exp or macd_cu])>=2 and rv>=1.0
    is_bandar=(fnet3>0 and fnet8>0 and fratio>0.6 and rv>1.2 and ema_gc)
    is_haka=(ema_bull and rv>1.5 and macd_exp and rsi_ema>50 and sk>sd and cl>vwap_v)
    is_super=(entry_kuat and rv>2 and score>=35)
    is_rebound=(entry_kuat and (stoch_os or rsi_os2))
    is_jual=(uw_sell and (stoch_ob or rsi_ob) and rv>1.0)
    if is_jual:    return "JUAL ⬇️",    score," · ".join(flags[:3]),gc_now
    if is_bandar:  return "BANDAR 🔵",  score," · ".join(flags[:3]),gc_now
    if is_haka:    return "HAKA 🔨",    score," · ".join(flags[:3]),gc_now
    if is_super:   return "SUPER 🔥",   score," · ".join(flags[:3]),gc_now
    if is_rebound: return "REBOUND 🏀", score," · ".join(flags[:3]),gc_now
    if entry_mod and score>=20: return "AKUM 📦", score," · ".join(flags[:3]),gc_now
    if score>=15:  return "ON TRACK ✅",score," · ".join(flags[:3]),gc_now
    return "WAIT ❌",score," · ".join(flags[:3]),gc_now

def get_aksi_v2(sinyal, gc_now, score):
    if "BANDAR" in sinyal or (("HAKA" in sinyal or "SUPER" in sinyal) and score>=35): return "AT ENTRY 🎯"
    elif "REBOUND" in sinyal: return "WATCH REB 🏀"
    elif gc_now:              return "GC NOW ⚡"
    elif score>=25:           return "AT ENTRY 🎯"
    elif score>=15:           return "WAIT GC ⏳"
    else:                     return "WAIT ❌"

# ════ FETCH INTRADAY — DS primary, yFinance fallback (anti-rate-limit) ════
def fetch_intraday(tickers, interval="15m"):
    all_dfs={}; ticker_list=list(tickers)
    # Step 1: cache first
    need_fetch=[]
    for t in ticker_list:
        raw_t=t.replace(".JK","").upper()
        cached=_cache_get(raw_t, interval)
        if cached is not None: all_dfs[t]=cached; continue
        need_fetch.append(t)
    if not need_fetch: return all_dfs
    # Step 2: DS parallel (10 threads) — kalau DS_KEY ada
    if DS_KEY:
        def _fetch_one(t):
            raw_t=t.replace(".JK","").upper()
            df=fetch_ds_ohlcv(raw_t, interval, 200, True)
            return t, df
        with ThreadPoolExecutor(max_workers=10) as ex:
            futs={ex.submit(_fetch_one,t):t for t in need_fetch}
            for f in as_completed(futs):
                try:
                    t,df=f.result(timeout=20)
                    if df is not None and len(df)>=20:
                        all_dfs[t]=df
                except: pass
    # Step 3: yFinance fallback — Ticker().history() parallel, no rate limit!
    # Jalan untuk semua ticker yang masih missing (DS gagal atau DS_KEY kosong)
    missing_yf=[t for t in need_fetch if t not in all_dfs]
    if missing_yf:
        yf_data=_fetch_yf_parallel(missing_yf, "7d", interval, workers=5)
        for t_yf,df_yf in yf_data.items():
            if df_yf is not None and len(df_yf)>=20:
                all_dfs[t_yf]=df_yf
    return all_dfs

def send_telegram(results_top, source="Scanner"):
    if not TOKEN or not CHAT_ID: return False
    now=datetime.now(jakarta_tz); is_open=9<=now.hour<16; sep="━"*28
    # Bener-bener evaluate conditional di f-string (bukan {{}})
    market_status="🔴 MARKET OPEN" if is_open else "🌙 AFTER HOURS"
    alert_type="WATCHLIST" if source=="Watchlist" else "SCANNER"
    hdr=(f"{market_status}\n⚡ *MESIN PRESISI {alert_type}*\n"
         f"⏰ `{now.strftime('%H:%M:%S')} WIB` · `{now.strftime('%d %b %Y')}`\n{sep}\n")
    body=""  # INIT! Sebelumnya gak diinit → UnboundLocalError
    for r in results_top[:8]:
        sig=r.get("Signal","-")
        mg=r.get("Mesin_Grade","—")
        ms=r.get("Mesin_Score",0)
        em=("🎯" if "PRESISI" in mg else "🔵" if "BANDAR" in mg else "💎" if "BAGGER" in mg
            else "⚡" if "KUAT" in mg else "🏆" if("GACOR" in sig or "REVERSAL" in sig)
            else "🔥" if "POTENSIAL" in sig else "👀")
        te="📈" if "▲" in r.get("Trend","") else("📉" if "▼" in r.get("Trend","") else "➡️")
        iq_v=r.get("IQ_Verdict","—"); iq_s=r.get("IQ_Score",0)
        body+=(f"\n{em} *{r['Ticker']}* `[{mg}]` MS:`{ms:.0f}`\n"
               f"   💰 `{r['Price']:,}` {te} · TT:`{sig}`\n"
               f"   📊 IQ Daily: `{iq_v}` ({iq_s:.0f}/100)\n"
               f"   📈 RSI: `{r.get('RSI-EMA',0):.0f}` · RVOL: `{r.get('RVOL',0):.1f}x`\n"
               f"   🎯 TP: `{r['TP']:,}` 🛑 SL: `{r['SL']:,}` R:R `{r.get('R:R',0)}`\n"
               f"   💡 _{r.get('Reasons','')[:60]}_\n")
    footer=f"\n{sep}\n⚡ _Mesin Presisi v1.0 · TT 15M × IQ Daily_\n⚠️ _BUKAN saran investasi!_"
    try:
        resp=requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                          data={"chat_id":CHAT_ID,"text":hdr+body+footer,"parse_mode":"Markdown"},timeout=10)
        return resp.status_code==200
    except Exception as e: return False

SECTORS={
    "Energi & Mining":    ["ADRO","BYAN","ITMG","PTBA","HRUM","DOID","GEMS","PGAS","ELSA","MEDC","ESSA","AKRA","RIGS","DSSA","MBAP","KKGI","MYOH","SMMT","BSSR","INDY"],
    "Perbankan":          ["BBCA","BBRI","BMRI","BBNI","BBTN","BJBR","BJTM","BNGA","BDMN","MEGA","BBYB","ARTO","BRIS","AGRO","BBHI","NOBU","BACA","MAYA"],
    "Properti":           ["BSDE","CTRA","SMRA","LPKR","PWON","APLN","ASRI","DILD","DUTI","MDLN","MKPI","JRPT","KIJA","BEST","GPRA","DART","MTLA"],
    "Infrastruktur":      ["JSMR","TLKM","EXCL","ISAT","TBIG","TOWR","WIKA","ADHI","PTPP","WSKT","WTON","WEGE","BIRD","TMAS","SMDR","BBRM"],
    "Konsumer":           ["UNVR","ICBP","INDF","MYOR","KLBF","GGRM","HMSP","DLTA","ROTI","SKBM","GOOD","HOKI","CLEO","MIKA","HEAL","SILO","KAEF","DVLA"],
    "Industri & Otomotif":["ASII","AUTO","SMSM","HEXA","UNTR","SCCO","KBLI","VOKS","BRAM","GJTL","IMAS","INTP","SMGR","CPIN","JPFA","BRPT","TPIA"],
    "Teknologi":          ["GOTO","BUKA","EMTK","MNCN","SCMA","MTEL","MTDL","MLPT","CHIP","LUCK","NFCX","WIFI","DIGI","AWAN","INET","MCAS","TECH"],
    "Shipping & Logistik":["TMAS","SMDR","BBRM","NELY","SHIP","ELPI","BIRD","ASSA","WEHA","SAFE","ATLI","MIRA","RAJA","RIGS","MBSS"],
    "Petrokimia & Kimia": ["TPIA","BRPT","BUDI","EKAD","ETWA","ESSA","AKPI","CPRO","SRSN","UNIC"],
}
HORMUZ_SECTORS=["Energi & Mining","Shipping & Logistik","Petrokimia & Kimia"]

# ════ HEADER ════
regime, ihsg_price, ema20, ema55, regime_detail, ihsg_chg = get_market_regime()
rcfg=get_regime_config(regime); rcolor=rcfg["color"]
chg_col="#00ff88" if ihsg_chg>=0 else "#ff3d5a"; chg_sym="▲" if ihsg_chg>=0 else "▼"
now_jkt=datetime.now(jakarta_tz); is_open=9<=now_jkt.hour<16

st.markdown(f"""<div class="tt-header">
  <div><div class="tt-logo">⚡ MESIN PRESISI</div>
  <div class="tt-sub">TT 15M × IDX Quant Daily (151 Strategies) · v1.0 · Zero Rate Limit ✅</div></div>
  <div class="live-badge"><div class="live-dot"></div>
    {"⚡ DataSectors" if DS_KEY else "📊 yFinance"} · LIVE {now_jkt.strftime("%H:%M:%S")} WIB
  </div>
</div>""", unsafe_allow_html=True)

st.markdown(f"""<div style="background:rgba(0,0,0,.4);border:1px solid {rcolor}44;border-radius:8px;padding:12px 16px;margin-bottom:14px;border-left:4px solid {rcolor};">
  <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;">
    <div><div style="font-family:Space Mono,monospace;font-size:12px;font-weight:700;color:{rcolor};letter-spacing:1px;">{rcfg["label"]}</div>
         <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-top:3px;">{rcfg["desc"]}</div></div>
    <div style="text-align:right;font-family:Space Mono,monospace;">
      <div style="font-size:18px;font-weight:700;color:{rcolor};">{ihsg_price:,.0f} <span style="font-size:11px;color:{chg_col}">{chg_sym}{abs(ihsg_chg):.2f}%</span></div>
      <div style="font-size:9px;color:#4a5568;">EMA20 {ema20:,.0f} · EMA55 {ema55:,.0f}</div>
    </div>
  </div>
</div>""", unsafe_allow_html=True)

tab_scanner,tab_watchlist,tab_bsjp,tab_sector,tab_gapup,tab_trail,tab_backtest=st.tabs(
    ["🔥 Scanner","👁️ Watchlist","🌙 BSJP","🏭 Sektor","📈 Gap Up","🎯 Trailing Stop","📊 Backtest"])

# ════ TAB SCANNER ════
with tab_scanner:
    with st.expander("⚙️  Scanner Settings", expanded=False):
        sc1,sc2,sc3=st.columns(3)
        with sc1:
            st.markdown('<div class="settings-label">MODE SIGNAL</div>',unsafe_allow_html=True)
            auto_regime=st.toggle("🤖 Auto-Mode (Market Regime)",value=True,key="auto_reg")
            if auto_regime:
                scan_mode=rcfg["mode"]
                st.markdown(f'<div style="font-family:Space Mono,monospace;font-size:10px;padding:6px 10px;background:rgba(0,0,0,.3);border-radius:4px;color:{rcolor};">Auto: {scan_mode}</div>',unsafe_allow_html=True)
            else:
                scan_mode=st.radio("Mode",["Scalping ⚡","Momentum 🚀","Reversal 🎯","Bagger 💎"],label_visibility="collapsed",key="smr")
            tele_on=st.toggle("📡 Telegram Alert",value=True,key="tele_on")
            # Status TOKEN/CHAT_ID
            if tele_on:
                if TOKEN and CHAT_ID:
                    st.caption(f"✅ TG ready · Token: ...{TOKEN[-8:]} · Chat: {CHAT_ID}")
                    if st.button("🔔 Test TG",key="tg_test_btn",use_container_width=True):
                        ok=send_telegram([{"Ticker":"TEST","Price":1000,"Signal":"TEST 🔔",
                                          "Mesin_Grade":"PRESISI 🎯","Mesin_Score":99,
                                          "IQ_Verdict":"BUY","IQ_Score":80,
                                          "Trend":"▲ UP","RSI-EMA":55,"RVOL":2.1,
                                          "TP":1050,"SL":980,"R:R":2.0,
                                          "Reasons":"Test notif dari Mesin Presisi"}])
                        st.success("✅ Terkirim!") if ok else st.error("❌ Gagal — cek token & chat_id")
                else:
                    missing=[]
                    if not TOKEN: missing.append("TELEGRAM_TOKEN")
                    if not CHAT_ID: missing.append("TELEGRAM_CHAT_ID")
                    st.caption(f"⚠️ Missing: {', '.join(missing)} di secrets.toml")
        with sc2:
            st.markdown('<div class="settings-label">FILTER</div>',unsafe_allow_html=True)
            auto_thresh=st.toggle("🤖 Auto-Threshold",value=True,key="auto_thr")
            if auto_thresh:
                min_score=rcfg["min_score"]; vol_thresh=rcfg["min_rvol"]
                st.caption(f"Auto: Score≥{min_score} · RVOL≥{vol_thresh}x")
            else:
                min_score=st.slider("Min Score (0-6)",0,6,2,key="msc")
                vol_thresh=st.slider("Min RVOL Spike",1.0,5.0,1.5,0.1,key="vol")
            min_turn=st.number_input("Min Turnover (M Rp)",value=100,step=100,key="trn")*1_000_000
        with sc3:
            st.markdown('<div class="settings-label">TAMPILAN</div>',unsafe_allow_html=True)
            view_mode=st.radio("View",["Card View 🃏","Table View 📊"],label_visibility="collapsed",key="vm")
            quick_mode=st.toggle("⚡ Quick (200 saham)",value=False,key="quick_mode")
            st.caption(f"🎯 Regime: {regime} · Mode: {scan_mode}")

    _btn_c1, _btn_c2 = st.columns([3,1])
    with _btn_c1:
        do_scan_btn=st.button("🔥 MULAI SCAN SEKARANG",type="primary",use_container_width=True,key="btn_scan")
    with _btn_c2:
        do_diagnosa=st.button("🔧 Diagnosa",use_container_width=True,key="btn_diag")
    if do_diagnosa:
        with st.expander("🔧 DIAGNOSTIK yFinance", expanded=True):
            st.code(f"yfinance: {yf.__version__}")
            for _tk in ["BBCA.JK","TLKM.JK","GOTO.JK"]:
                _r = _fetch_yf_ticker(_tk, "5d", "1d")
                if _r is not None:
                    st.success(f"✅ {_tk} → {len(_r)} rows, Close[-1]={float(_r['Close'].iloc[-1]):,.0f}")
                else:
                    st.error(f"❌ {_tk} → GAGAL")
            # test 15m
            _r15 = _fetch_yf_ticker("BBCA.JK","5d","15m")
            if _r15 is not None:
                st.success(f"✅ BBCA 15m → {len(_r15)} rows ✓")
            else:
                st.error("❌ BBCA 15m → GAGAL — coba ganti requirements.txt: yfinance==0.2.61")
    _now_check=now_jkt.timestamp(); auto_trigger=False
    if st.session_state.last_scan_time and not do_scan_btn:
        if _now_check-st.session_state.last_scan_time>=300 and st.session_state.scan_results:
            auto_trigger=True

    if do_scan_btn or auto_trigger:
        scan_list=stocks_yf[:200] if quick_mode else stocks_yf
        n_scan=len(scan_list); prog_ph=st.empty(); pb=st.progress(0)
        prog_ph.markdown(f'<div style="color:#ff7b00;font-family:Space Mono,monospace;font-size:12px;">⚡ Fetching {n_scan} saham (DS parallel + yFinance anti-rate-limit)...</div>',unsafe_allow_html=True)
        pb.progress(0.1)
        data_dict={}
        try:
            ticker_list=list(scan_list)
            # ─ Step 1: DS cache check ─
            need_fetch=[]
            for t in ticker_list:
                raw_t=t.replace(".JK","").upper()
                cached=_cache_get(raw_t,"15m")
                if cached is not None: data_dict[t]=cached; continue
                need_fetch.append(t)
            n_cached=len(data_dict); n_need=len(need_fetch)
            prog_ph.markdown(f'<div style="color:#ff7b00;font-family:Space Mono,monospace;font-size:11px;">⚡ {n_cached} dari cache · {n_need} perlu fetch...</div>',unsafe_allow_html=True)

            # ─ Step 2: DS parallel fetch ─
            def _f(t):
                raw_t=t.replace(".JK","").upper()
                if DS_KEY:
                    df=fetch_ds_ohlcv(raw_t,"15m",200,True)
                    if df is not None: return t,df
                return t,None
            done_count=[0]
            if need_fetch:
                with ThreadPoolExecutor(max_workers=10) as ex:
                    futs={ex.submit(_f,t):t for t in need_fetch}
                    for fut in as_completed(futs):
                        try:
                            t,df=fut.result(timeout=15); done_count[0]+=1
                            if df is not None and len(df)>=20: data_dict[t]=df
                            if done_count[0]%30==0:
                                pct=0.10+(done_count[0]/max(n_need,1))*0.40
                                pb.progress(min(pct,0.50))
                        except: done_count[0]+=1

            # ─ Step 3: yFinance fallback — Ticker().history() PARALLEL, no rate limit ─
            missing_yf=[t for t in ticker_list if t not in data_dict]
            if missing_yf:
                prog_ph.markdown(f'<div style="color:#ffb700;font-family:Space Mono,monospace;font-size:11px;">📊 yFinance fallback: {len(missing_yf)} ticker → Ticker().history() parallel...</div>',unsafe_allow_html=True)
                yf_fetched=_fetch_yf_parallel(missing_yf,"5d","15m",workers=6)
                for t_yf,df_yf in yf_fetched.items():
                    if df_yf is not None and len(df_yf)>=20:
                        data_dict[t_yf]=df_yf
            pb.progress(0.76)

            st.session_state.data_dict=data_dict
            n_ds   = sum(1 for t in data_dict if hasattr(data_dict[t], "columns") and "FBuy" in data_dict[t].columns)
            n_yf   = len(data_dict) - n_ds
            src_lbl= f"DS:{n_ds} + yF:{n_yf}" if DS_KEY else f"yFinance:{n_yf}"
            status_color = "#00ff88" if len(data_dict)>0 else "#ff3d5a"
            prog_ph.markdown(f'<div style="color:{status_color};font-family:Space Mono,monospace;font-size:11px;">{"✅" if len(data_dict)>0 else "⚠️"} Data siap: <b>{len(data_dict)}</b> saham dari {len(ticker_list)} target [{src_lbl}]</div>',unsafe_allow_html=True)
            if len(data_dict)==0:
                st.error("⚠️ 0 saham berhasil di-fetch. Lihat diagnostik di bawah.")
                with st.expander("🔧 DIAGNOSTIK — klik untuk cek koneksi", expanded=True):
                    st.code(f"yfinance version: {yf.__version__}\nPython workers: {4}\nTickers target: {len(ticker_list)}\nContoh ticker: {ticker_list[:3] if ticker_list else 'KOSONG!'}")
                    st.caption("Test fetch BBCA.JK...")
                    _test_df = _fetch_yf_ticker("BBCA.JK", "5d", "1d")
                    if _test_df is not None:
                        st.success(f"✅ BBCA.JK OK — {len(_test_df)} rows, cols: {_test_df.columns.tolist()}")
                        st.caption("yFinance bisa fetch data. Kemungkinan masalah di paralel. Coba SCAN lagi.")
                    else:
                        st.error("❌ BBCA.JK juga gagal! Cek:\n- Internet Streamlit Cloud OK?\n- Coba tambahkan `yfinance==0.2.61` di requirements.txt")
                    # Test download() as last resort
                    try:
                        _td2 = yf.download("BBCA.JK","5d","1d",progress=False,threads=False)
                        if not _td2.empty:
                            st.success(f"✅ yf.download() OK — {len(_td2)} rows")
                        else:
                            st.warning("⚠️ yf.download() kosong juga")
                    except Exception as _te:
                        st.error(f"❌ yf.download() error: {type(_te).__name__}: {str(_te)[:100]}")
                st.stop()
            pb.progress(0.78)

            # ─ Step 4: Daily context — DS dulu, yFinance fallback parallel ─
            daily_dict={}
            if DS_KEY:
                def _fd(t):
                    raw_t=t.replace(".JK","").upper()
                    cached=_cache_get(raw_t,"daily")
                    if cached is not None: return t,cached
                    df=fetch_ds_ohlcv(raw_t,"daily",100,False)
                    return t,df
                with ThreadPoolExecutor(max_workers=8) as ex:
                    futs={ex.submit(_fd,t):t for t in list(data_dict.keys())}
                    for f in as_completed(futs):
                        try:
                            t,df=f.result(timeout=12)
                            if df is not None and len(df)>=2: daily_dict[t]=df
                        except: pass
            # yFinance daily fallback — Ticker().history() parallel, no rate limit!
            missing_daily=[t for t in list(data_dict.keys()) if t not in daily_dict]
            if missing_daily:
                yf_daily=_fetch_yf_parallel(missing_daily,"5d","1d",workers=6)
                for t_d,df_d in yf_daily.items():
                    if df_d is not None and len(df_d)>=2:
                        daily_dict[t_d]=df_d
            st.session_state.daily_dict=daily_dict

            # ─ Step 5: Process signals ─
            pb.progress(0.85)
            prog_ph.markdown(f'<div style="color:#00ff88;font-family:Space Mono,monospace;font-size:11px;">⚙️ Processing {len(data_dict)} saham...</div>',unsafe_allow_html=True)
            results=[]; tickers=list(data_dict.keys())
            daily_dict=st.session_state.get("daily_dict",{})
            # Track skip reasons untuk debug
            skip_reasons={"short":0,"price0":0,"turnover":0,"score":0,"error":0}
            last_err=[""]
            for i,ticker_yf in enumerate(tickers):
                pb.progress(0.85+(i+1)/max(len(tickers),1)*0.14)
                try:
                    df=data_dict[ticker_yf].copy()
                    if len(df)<30: skip_reasons["short"]+=1; continue
                    df=apply_intraday_indicators(df)
                    r=df.iloc[-1]; p=df.iloc[-2]; p2=df.iloc[-3] if len(df)>=3 else p
                    # NaN-safe conversion helpers — yFinance data sometimes has NaN!
                    def _si(v, d=0):
                        try:
                            x=float(v); return d if(np.isnan(x) or np.isinf(x)) else int(x)
                        except: return d
                    def _sff(v, d=0.0):
                        try:
                            x=float(v); return d if(np.isnan(x) or np.isinf(x)) else x
                        except: return d
                    close=_sff(r.get("Close",0)); vol=_sff(r.get("Volume",0))
                    if close<=0: skip_reasons["price0"]+=1; continue  # skip jika harga invalid
                    ticker_raw=ticker_yf.replace(".JK","").upper()
                    # FIX: gak boleh pakai `or` untuk DataFrame (ambiguous truth value)
                    df_d = daily_dict.get(ticker_yf)
                    if df_d is None: df_d = daily_dict.get(ticker_raw)
                    if df_d is not None and len(df_d)>=2:
                        c1=float(df_d.iloc[-1]["Close"]); c0=float(df_d.iloc[-2]["Close"])
                        gain_pct=(c1-c0)/max(c0,1)*100
                        d_vol=float(df_d.iloc[-1]["Volume"])
                        if d_vol > 0:
                            turnover=c1*d_vol  # daily volume normal
                        else:
                            # daily vol=0 (weekend/sebelum open) → 15m vol sum fallback
                            turnover=close*float(df["Volume"].fillna(0).sum())
                    else:
                        try:
                            # Fallback: sum semua 15m volume (aman tanpa timezone issue)
                            turnover=close*float(df["Volume"].fillna(0).sum())
                            gain_pct=float(r.get("ROC3",0))*100
                        except:
                            turnover=close*max(vol,0); gain_pct=float(r.get("ROC3",0))*100
                    rvol_raw=float(r["RVOL"]) if not np.isnan(float(r["RVOL"])) else 1.0
                    rvol=rvol_raw
                    if turnover<min_turn or rvol<vol_thresh: skip_reasons["turnover"]+=1; continue
                    if scan_mode=="Scalping ⚡":   sc,reasons,_=score_scalping(r,p,p2)
                    elif scan_mode=="Momentum 🚀": sc,reasons,_=score_momentum(r,p,p2)
                    elif scan_mode=="Bagger 💎":   sc,reasons,_=score_bagger(r,p,p2,df)
                    else:                          sc,reasons,_=score_reversal(r,p,p2)
                    if sc<min_score: skip_reasons["score"]+=1; continue
                    sig=get_signal(sc,scan_mode)
                    # NOTE: filter "if sig==WAIT continue" dihapus —
                    # min_score slider yang jadi sole filter (jadi rata kiri = score 0+ all show)
                    sig_v2,sc_v2,flags_v2,gc_now=get_sinyal_v2(r,p,p2)
                    aksi_v2=get_aksi_v2(sig_v2,gc_now,sc_v2)
                    atr=float(r["ATR"]) if not np.isnan(float(r["ATR"])) else close*0.01
                    if scan_mode=="Scalping ⚡":   tp=close+1.5*atr; sl=close-0.8*atr
                    elif scan_mode=="Momentum 🚀": tp=close+2.0*atr; sl=close-0.8*atr
                    elif scan_mode=="Bagger 💎":   tp=close+3.0*atr; sl=close-1.0*atr
                    else:                          tp=close+2.5*atr; sl=close-0.6*atr
                    rr=(tp-close)/max(close-sl,0.01)
                    e9=float(r["EMA9"]); e21=float(r["EMA21"]); e50=float(r["EMA50"])
                    trend="▲ UP" if e9>e21>e50 else("▼ DOWN" if e9<e21<e50 else "◆ SIDE")
                    def _sf(v,d=0.):
                        try: x=float(v); return d if(np.isnan(x) or np.isinf(x)) else x
                        except: return d
                    fnet3=_sf(r.get("FNet3",0)); fnet8=_sf(r.get("FNet8",0))
                    fratio=_sf(r.get("FRatio",0.5))
                    fbuy=_sf(r.get("FBuy",0)); fsell=_sf(r.get("FSell",0))
                    has_asing=(fbuy+fsell)>0
                    if not has_asing:         fdir="—";        fc_="#4a5568"
                    elif fnet3>0 and fnet8>0: fdir="🔵 BELI";  fc_="#4da6ff"
                    elif fnet3<0 and fnet8<0: fdir="🔴 JUAL";  fc_="#ff3d5a"
                    else:                     fdir="⚪ MIX";   fc_="#888888"
                    lwick=_sf(r.get("LWick",0))
                    vb=turnover/1e9; val_str=f"{vb:.1f}B" if vb>=1 else f"{round(vb*1000,0):.0f}M"

                    # ── IDX QUANT 151 STRATEGIES — daily analysis ──────────
                    iq = iq_analyze(df_d)  # df_d already fetched above
                    mg, mg_col, ms = calc_mesin_grade(
                        sc, sig+"|"+sig_v2, iq["iq_score"], iq["iq_verdict"], iq["iq_bagger"])

                    results.append({
                        "Ticker":stock_map.get(ticker_yf,ticker_raw),"Price":_si(close),"Score":sc,
                        "Signal":sig,"Sinyal_v2":sig_v2,"Aksi_v2":aksi_v2,
                        "Trend":trend,"RSI-EMA":round(_sff(r.get("RSI_EMA",50)),1),
                        "Stoch K":round(_sff(r.get("STOCH_K",50)),1),"Stoch D":round(_sff(r.get("STOCH_D",50)),1),
                        "MACD Hist":round(_sff(r.get("MACD_Hist",0)),4),"RVOL":round(rvol,2),
                        "BB%":round(_sff(r.get("BB_pct",0.5)),2),"ROC 3B%":round(gain_pct,2),
                        "Gain":round(gain_pct,1),
                        "VWAP":_si(r.get("VWAP",close),_si(close)),
                        "TP":_si(tp),"SL":_si(sl),"R:R":round(rr,1),
                        "Turnover(M)":round(turnover/1e6,1),"Val":val_str,
                        "Reasons":" · ".join(reasons),"_class":get_card_class(sig),
                        "LWick":round(lwick,1),"FDir":fdir,"FC":fc_,
                        "FNet3":_si(fnet3),"FNet8":_si(fnet8),"FRatio":round(fratio,2),
                        "sc_v2":sc_v2,"gc_now":gc_now,
                        # ── IDX QUANT fields ──
                        "IQ_Score":round(iq["iq_score"],1),
                        "IQ_Verdict":iq["iq_verdict"],
                        "IQ_MA":iq["iq_ma"],
                        "IQ_Mom":iq["iq_mom"],
                        "IQ_Bagger":iq["iq_bagger"],
                        "IQ_RSI":iq["iq_rsi"],
                        # ── MESIN PRESISI grade ──
                        "Mesin_Grade":mg,
                        "Mesin_Color":mg_col,
                        "Mesin_Score":round(ms,1),
                    })
                except Exception as _exc:
                    skip_reasons["error"]+=1
                    last_err[0]=f"{type(_exc).__name__}: {str(_exc)[:80]}"
                    continue
            pb.progress(1.0); prog_ph.empty(); pb.empty()
            st.session_state.scan_results=results
            st.session_state.last_scan_time=now_jkt.timestamp()
            _tt_save(results,st.session_state.last_scan_time)
            st.session_state.last_scan_mode=scan_mode
            # Debug info kalau hasil kosong
            if not results:
                n_data = len(data_dict)
                st.warning(
                    f"⚠️ Scan selesai tapi 0 sinyal lolos dari {n_data} saham yang di-fetch."
                )
                st.error(
                    f"**🔧 Breakdown skip reasons:**\n"
                    f"- Data terlalu pendek (<30 bars): **{skip_reasons['short']}**\n"
                    f"- Harga invalid (Close ≤ 0): **{skip_reasons['price0']}**\n"
                    f"- Turnover/RVOL di bawah threshold: **{skip_reasons['turnover']}**\n"
                    f"- Score di bawah min_score: **{skip_reasons['score']}**\n"
                    f"- Exception (crash): **{skip_reasons['error']}**\n\n"
                    f"**Last error:** `{last_err[0] or 'none'}`\n\n"
                    f"**Saran:** turunkan Min Score → 0, Min Turnover → 0, Min RVOL → 0 di sidebar."
                )
            else:
                st.success(f"✅ {len(results)} sinyal ditemukan dari {len(data_dict)} saham!")
            if tele_on and results:
                if "tt_last_sent" not in st.session_state: st.session_state.tt_last_sent=set()
                # Sort by Mesin_Score → kirim sinyal kualitas terbaik dulu
                dft=pd.DataFrame(results).sort_values("Mesin_Score",ascending=False)
                # Hanya kirim sinyal dengan Mesin Grade bermakna (skip WAIT/MARGINAL/LEMAH)
                quality_mask=dft["Mesin_Grade"].astype(str).str.contains(
                    "PRESISI|BANDAR|BAGGER|KUAT|MONITOR", na=False)
                dft_q=dft[quality_mask]
                cs=set(dft_q["Ticker"].tolist())
                na=cs-st.session_state.tt_last_sent  # new alerts only
                if na:
                    tn=dft_q[dft_q["Ticker"].isin(na)].head(8).to_dict("records")
                    if tn:
                        ok_send = send_telegram(tn)
                        if ok_send:
                            st.success(f"📡 Telegram: {len(tn)} sinyal terkirim ke Telegram!")
                        else:
                            st.warning(f"⚠️ Telegram gagal kirim — cek TOKEN & CHAT_ID di secrets.toml")
                    st.session_state.tt_last_sent.update(na)
                elif len(dft_q)>0:
                    st.info(f"📡 Telegram: {len(dft_q)} sinyal quality tapi sudah pernah dikirim (anti-spam)")
                else:
                    st.info(f"📡 Telegram: tidak ada sinyal kualitas (PRESISI/BANDAR/BAGGER/KUAT/MONITOR) untuk dikirim")
                st.session_state.tt_last_sent=st.session_state.tt_last_sent&cs
        except Exception as e:
            try: prog_ph.empty(); pb.empty()
            except: pass
            st.error(f"Scan error: {str(e)[:100]}")

    if st.session_state.last_scan_time:
        _nc=now_jkt.timestamp()
        _rem=max(0,300-(_nc-st.session_state.last_scan_time))
        _lt=datetime.fromtimestamp(st.session_state.last_scan_time,jakarta_tz).strftime("%H:%M:%S")
        _el=int(_nc-st.session_state.last_scan_time)
        st.caption(f"⏱️ Scan {_el//60}m {_el%60}s lalu · Refresh dalam: {int(_rem//60):02d}:{int(_rem%60):02d} · Last: {_lt} WIB")

    results=st.session_state.scan_results
    lm=st.session_state.get("last_scan_mode","")
    if not results and not do_scan_btn and not auto_trigger:
        st.markdown(f"""<div style="text-align:center;padding:48px;color:#4a5568;font-family:Space Mono,monospace;">
          <div style="font-size:36px;margin-bottom:12px;">⚡</div>
          <div style="font-size:13px;letter-spacing:2px;">KLIK SCAN UNTUK MULAI</div>
          <div style="font-size:10px;margin-top:8px;color:#2d3748;">
            {"⚡ Quick: 200 saham" if quick_mode else f"Full: {len(raw_stocks)} saham"} · Regime: {regime} · {rcfg["mode"]}
          </div>
          <div style="font-size:10px;margin-top:12px;color:#2d3748;padding:8px;background:#0d1117;border-radius:4px;border:1px solid #1c2533;">
            ⓘ Hasil scan tidak tersimpan setelah page reload. Klik SCAN lagi.
          </div>
          </div>
        </div>""", unsafe_allow_html=True)

    elif results:
        df_out=pd.DataFrame(results).sort_values("Mesin_Score",ascending=False).reset_index(drop=True)
        gacor  =df_out[df_out["Signal"].str.contains("GACOR|REVERSAL|HAKA|SUPER|BANDAR",na=False)]
        bagger =df_out[df_out["Signal"].str.contains("BAGGER|KANDIDAT",na=False)]
        potensi=df_out[df_out["Signal"].str.contains("POTENSIAL|REBOUND|AKUM",na=False)]
        avg_rsi=df_out["RSI-EMA"].mean()
        asing_b_list=df_out[df_out["FDir"]=="🔵 BELI"]["Ticker"].tolist() if "FDir" in df_out.columns else []
        asing_j_list=df_out[df_out["FDir"]=="🔴 JUAL"]["Ticker"].tolist() if "FDir" in df_out.columns else []
        bandar_list =df_out[df_out["Signal"].str.contains("BANDAR",na=False)]["Ticker"].tolist() if "Signal" in df_out.columns else []
        _mgcol = df_out["Mesin_Grade"].astype(str) if "Mesin_Grade" in df_out.columns else pd.Series([""] * len(df_out))
        presisi_list =df_out[_mgcol.str.contains("PRESISI",na=False)]["Ticker"].tolist()
        bandar_m_list=df_out[_mgcol.str.contains("BANDAR",na=False)]["Ticker"].tolist()
        kuat_list    =df_out[_mgcol.str.contains("KUAT",na=False)]["Ticker"].tolist()
        bagger_m_list=df_out[_mgcol.str.contains("BAGGER",na=False)]["Ticker"].tolist()
        iq_buy_cnt   =len(df_out[df_out["IQ_Verdict"]=="BUY"]) if "IQ_Verdict" in df_out.columns else 0
        bandar_cnt=len(bandar_list); asing_beli=len(asing_b_list); asing_jual=len(asing_j_list)

        rsi_color_avg = "#00ff88" if avg_rsi>50 else "#ffb700" if avg_rsi>35 else "#ff3d5a"
        st.markdown(f"""<div class="metric-row">
          <div class="metric-card" style="border-top-color:{rcolor}"><div class="metric-label">Regime</div>
            <div class="metric-value" style="font-size:16px;color:{rcolor}">{regime}</div>
            <div class="metric-sub">{ihsg_price:,.0f} {chg_sym}{abs(ihsg_chg):.2f}%</div></div>
          <div class="metric-card orange"><div class="metric-label">Mode</div>
            <div class="metric-value" style="font-size:13px;margin-top:4px;">{lm}</div></div>
          <div class="metric-card green"><div class="metric-label">Signal Lolos</div>
            <div class="metric-value">{len(df_out)}</div><div class="metric-sub">dari {len(raw_stocks)} emiten</div></div>
          <div class="metric-card" style="border-top-color:#bf5fff"><div class="metric-label">BAGGER 💎</div>
            <div class="metric-value" style="color:#bf5fff">{len(bagger)}</div></div>
          <div class="metric-card red"><div class="metric-label">GACOR 🔥</div>
            <div class="metric-value">{len(gacor)}</div></div>
          <div class="metric-card amber"><div class="metric-label">POTENSIAL</div>
            <div class="metric-value">{len(potensi)}</div></div>
          <div class="metric-card"><div class="metric-label">Avg RSI-EMA</div>
            <div class="metric-value" style="color:{rsi_color_avg}">{avg_rsi:.1f}</div></div>
        </div>""", unsafe_allow_html=True)

        presisi_str  = ", ".join(presisi_list[:4])  or "—"
        bandar_m_str = ", ".join(bandar_m_list[:4]) or "—"
        bagger_m_str = ", ".join(bagger_m_list[:4]) or "—"
        kuat_str     = ", ".join(kuat_list[:4])     or "—"
        st.markdown(f"""
<div style="background:linear-gradient(135deg,rgba(0,255,136,.04),rgba(191,95,255,.04));border:1px solid rgba(0,255,136,.25);border-radius:10px;padding:12px 16px;margin-bottom:12px;">
  <div style="font-family:Space Mono,monospace;font-size:11px;font-weight:700;color:#00ff88;letter-spacing:2px;margin-bottom:10px;">
    MESIN PRESISI v1.0 — 15M x DAILY ALIGNMENT (151 Strategies)
  </div>
  <div style="display:flex;gap:10px;flex-wrap:wrap;">
    <div style="background:rgba(0,255,136,.07);border:1px solid rgba(0,255,136,.3);border-radius:6px;padding:8px 14px;min-width:100px">
      <div style="font-family:Space Mono,monospace;font-size:9px;color:#00ff88;letter-spacing:1px">PRESISI</div>
      <div style="font-family:Space Mono,monospace;font-size:22px;font-weight:700;color:#00ff88">{len(presisi_list)}</div>
      <div style="font-size:9px;color:#4a5568">{presisi_str}</div>
    </div>
    <div style="background:rgba(77,166,255,.07);border:1px solid rgba(77,166,255,.3);border-radius:6px;padding:8px 14px;min-width:100px">
      <div style="font-family:Space Mono,monospace;font-size:9px;color:#4da6ff;letter-spacing:1px">BANDAR PRESISI</div>
      <div style="font-family:Space Mono,monospace;font-size:22px;font-weight:700;color:#4da6ff">{len(bandar_m_list)}</div>
      <div style="font-size:9px;color:#4a5568">{bandar_m_str}</div>
    </div>
    <div style="background:rgba(191,95,255,.07);border:1px solid rgba(191,95,255,.3);border-radius:6px;padding:8px 14px;min-width:100px">
      <div style="font-family:Space Mono,monospace;font-size:9px;color:#bf5fff;letter-spacing:1px">BAGGER PRESISI</div>
      <div style="font-family:Space Mono,monospace;font-size:22px;font-weight:700;color:#bf5fff">{len(bagger_m_list)}</div>
      <div style="font-size:9px;color:#4a5568">{bagger_m_str}</div>
    </div>
    <div style="background:rgba(255,183,0,.07);border:1px solid rgba(255,183,0,.3);border-radius:6px;padding:8px 14px;min-width:100px">
      <div style="font-family:Space Mono,monospace;font-size:9px;color:#ffb700;letter-spacing:1px">KUAT</div>
      <div style="font-family:Space Mono,monospace;font-size:22px;font-weight:700;color:#ffb700">{len(kuat_list)}</div>
      <div style="font-size:9px;color:#4a5568">{kuat_str}</div>
    </div>
    <div style="background:rgba(0,0,0,.2);border:1px solid #1c2533;border-radius:6px;padding:8px 14px;min-width:100px">
      <div style="font-family:Space Mono,monospace;font-size:9px;color:#4a5568;letter-spacing:1px">IQ BUY Daily</div>
      <div style="font-family:Space Mono,monospace;font-size:22px;font-weight:700;color:#2dd4bf">{iq_buy_cnt}</div>
      <div style="font-size:9px;color:#4a5568">151 Strategies</div>
    </div>
  </div>
</div>""", unsafe_allow_html=True)

        if DS_KEY:
            bandar_str  = ", ".join(bandar_list[:5])   or "—"
            asing_b_str = ", ".join(asing_b_list[:5])  or "—"
            asing_j_str = ", ".join(asing_j_list[:5])  or "—"
            st.markdown(f"""<div style="display:flex;gap:10px;margin-bottom:10px;flex-wrap:wrap">
  <div style="background:#0d1a2e;border:1px solid #4da6ff44;border-radius:8px;padding:8px 14px;flex:1;min-width:100px">
    <div style="font-family:Space Mono,monospace;font-size:9px;color:#4da6ff;letter-spacing:1px">BANDAR MASUK</div>
    <div style="font-family:Space Mono,monospace;font-size:18px;font-weight:700;color:#4da6ff">{bandar_cnt}</div>
    <div style="font-size:9px;color:#4a5568">{bandar_str}</div>
  </div>
  <div style="background:#0d2010;border:1px solid #00ff8844;border-radius:8px;padding:8px 14px;flex:1;min-width:100px">
    <div style="font-family:Space Mono,monospace;font-size:9px;color:#00ff88;letter-spacing:1px">ASING NET BUY</div>
    <div style="font-family:Space Mono,monospace;font-size:18px;font-weight:700;color:#00ff88">{asing_beli}</div>
    <div style="font-size:9px;color:#4a5568">{asing_b_str}</div>
  </div>
  <div style="background:#200d0d;border:1px solid #ff3d5a44;border-radius:8px;padding:8px 14px;flex:1;min-width:100px">
    <div style="font-family:Space Mono,monospace;font-size:9px;color:#ff3d5a;letter-spacing:1px">ASING NET SELL</div>
    <div style="font-family:Space Mono,monospace;font-size:18px;font-weight:700;color:#ff3d5a">{asing_jual}</div>
    <div style="font-size:9px;color:#4a5568">{asing_j_str}</div>
  </div>
  <div style="background:#0d1117;border:1px solid #1c2533;border-radius:8px;padding:8px 14px;flex:1;min-width:100px">
    <div style="font-family:Space Mono,monospace;font-size:9px;color:#4a5568;letter-spacing:1px">DATA SOURCE</div>
    <div style="font-family:Space Mono,monospace;font-size:14px;font-weight:700;color:#2dd4bf">DS + yFinance</div>
    <div style="font-size:9px;color:#4a5568">IDX FBuy+FSell+OHLCV</div>
  </div>
</div>""", unsafe_allow_html=True)

        th='<div class="tape-wrap"><div class="tape-inner">'
        for _,row in df_out.iterrows():
            roc=row["ROC 3B%"]; ib="BAGGER" in row["Signal"] or "KANDIDAT" in row["Signal"]
            mg2=str(row.get("Mesin_Grade","—"))
            cls="bagger" if ib else("up" if roc>0 else("down" if roc<0 else "flat"))
            sym="[BAG]" if "BAGGER" in mg2 else("[PRESISI]" if "PRESISI" in mg2 else("[BANDAR]" if "BANDAR" in mg2 else("[KUAT]" if "KUAT" in mg2 else("UP" if roc>0 else "DN"))))
            th+=f'<span class="tape-item {cls}">{row["Ticker"]} {int(row["Price"])} {sym} IQ:{row.get("IQ_Verdict","?")}</span>'
        th+=th.replace('tape-inner">',''); th+='</div></div>'
        st.markdown(th, unsafe_allow_html=True)

        n_mp = len(presisi_list)+len(bandar_m_list)
        if n_mp > 0:
            st.markdown(f'<div class="bagger-alert-box"><div class="bagger-title">MESIN PRESISI ALERT x{n_mp} — 15M + DAILY ALIGNED</div></div>', unsafe_allow_html=True)
        if not bagger.empty:
            st.markdown(f'<div class="bagger-alert-box"><div class="bagger-title">WYCKOFF BAGGER x{len(bagger)} KANDIDAT</div></div>', unsafe_allow_html=True)
        if not gacor.empty:
            st.markdown(f'<div class="alert-box"><div class="alert-title">GACOR ALERT x{len(gacor)} SAHAM — {lm}</div></div>', unsafe_allow_html=True)

        def tt_aksi_badge(a):
            a=str(a)
            if "AT ENTRY" in a: return f'<span style="background:#1a472a;color:#00ff88;padding:2px 8px;border-radius:4px;font-size:9px;font-weight:700">{a}</span>'
            elif "GC NOW" in a: return f'<span style="background:#0d2233;color:#00e5ff;padding:2px 8px;border-radius:4px;font-size:9px;font-weight:700">{a}</span>'
            elif "WATCH"  in a: return f'<span style="background:#251800;color:#ffb700;padding:2px 8px;border-radius:4px;font-size:9px;font-weight:700">{a}</span>'
            return f'<span style="background:#2a1a1a;color:#ff3d5a;padding:2px 8px;border-radius:4px;font-size:9px;font-weight:700">{a}</span>'

        def tt_sinyal_badge(s):
            s=str(s)
            M={"BANDAR":("#4da6ff","#0a1525"),"HAKA":("#00ff88","#0a2010"),"SUPER":("#bf5fff","#150a25"),
               "REBOUND":("#ffb700","#251800"),"JUAL":("#ff3d5a","#250a0d"),"AKUM":("#00e5ff","#0a1515"),
               "ON TRACK":("#00ff88","#0a1a0a"),"GACOR":("#00ff88","#0a2010"),"REVERSAL":("#bf5fff","#1a0d2e"),
               "POTENSIAL":("#ffb700","#1a1a0d"),"WATCH":("#00e5ff","#0a1a1a"),
               "LEMAH":("#888888","#1a1a1a"),"MARGINAL":("#666666","#151515"),
               "WAIT":("#4a5568","#0d0d0d")}
            for k,(c,bg) in M.items():
                if k in s: return f'<span style="background:{bg};color:{c};padding:2px 10px;border-radius:4px;font-size:9px;font-weight:700;border:1px solid {c}44">{s}</span>'
            return f'<span style="background:#1a1a1a;color:#4a5568;padding:2px 10px;border-radius:4px;font-size:9px;font-weight:700">{s}</span>'

        def mesin_badge(mg3):
            mg3=str(mg3)
            M2={"PRESISI":("#00ff88","#0a1a10","rgba(0,255,136,.4)"),
                "BANDAR": ("#4da6ff","#0a1525","rgba(77,166,255,.4)"),
                "BAGGER": ("#bf5fff","#150a25","rgba(191,95,255,.4)"),
                "KUAT":   ("#ffb700","#251800","rgba(255,183,0,.4)"),
                "MONITOR":("#00e5ff","#0a1515","rgba(0,229,255,.3)"),
                "WAIT":   ("#ff3d5a","#250a0d","rgba(255,61,90,.2)")}
            for k,(c,bg,brd) in M2.items():
                if k in mg3: return f'<span style="background:{bg};color:{c};padding:3px 10px;border-radius:4px;font-size:10px;font-weight:700;border:1px solid {brd}">{mg3}</span>'
            return f'<span style="background:#1a1a1a;color:#4a5568;padding:3px 10px;border-radius:4px;font-size:10px">{mg3}</span>'

        def iq_verdict_badge(v):
            v=str(v)
            M3={"BUY":("#22c55e","rgba(34,197,94,.15)","rgba(34,197,94,.4)"),
                "HOLD":("#8b5cf6","rgba(139,92,246,.15)","rgba(139,92,246,.35)"),
                "WAIT":("#ef4444","rgba(239,68,68,.1)","rgba(239,68,68,.3)")}
            c,bg,brd=M3.get(v,("#4a5568","rgba(255,255,255,.05)","rgba(255,255,255,.1)"))
            return f'<span style="background:{bg};color:{c};padding:2px 8px;border-radius:4px;font-size:9px;font-weight:700;border:1px solid {brd}">{v}</span>'

        if view_mode=="Card View 🃏":
            st.markdown('<div class="section-title">Signal Cards — sorted by Mesin Score</div>', unsafe_allow_html=True)
            ch='<div class="signal-grid">'
            for _,row in df_out.head(20).iterrows():
                si=int(row["Score"]); ib="BAGGER" in row["Signal"] or "KANDIDAT" in row["Signal"]
                bc="filled-purple" if ib else "filled"
                bars="".join([f'<div class="sc-bar {bc if i<si else "empty"}" style="width:24px"></div>' for i in range(6)])
                rc="#00ff88" if row["ROC 3B%"]>0 else "#ff3d5a"
                te="UP" if "UP" in row.get("Trend","") else("DN" if "DN" in row.get("Trend","") else "~")
                fd=row.get("FDir","—"); fc=row.get("FC","#4a5568")
                mg_v=str(row.get("Mesin_Grade","—")); mg_c=str(row.get("Mesin_Color","#4a5568"))
                ms_v=float(row.get("Mesin_Score",0))
                iq_v=str(row.get("IQ_Verdict","—")); iq_s=float(row.get("IQ_Score",0))
                iq_m=str(row.get("IQ_MA","—"))
                ch+=(f'<div class="signal-card {row["_class"]}">'
                     f'<div style="display:flex;justify-content:space-between;align-items:flex-start;">'
                     f'<div><div class="sc-ticker">{row["Ticker"]}</div>'
                     f'<div class="sc-price" style="color:{rc}">{int(row["Price"]):,} {te}</div></div>'
                     f'<div style="text-align:right;">'
                     f'<div style="font-family:Space Mono,monospace;font-size:9px;color:#4a5568">MESIN</div>'
                     f'<div style="font-family:Space Mono,monospace;font-size:18px;font-weight:700;color:{mg_c}">{ms_v:.0f}</div>'
                     f'</div></div>'
                     f'<div style="margin:5px 0">{mesin_badge(mg_v)}</div>'
                     f'<div class="sc-bars">{bars}</div>'
                     f'<div style="display:flex;gap:5px;margin:5px 0;flex-wrap:wrap">'
                     f'<div style="font-family:Space Mono,monospace;font-size:9px;padding:2px 7px;background:rgba(0,0,0,.3);border-radius:4px;color:#00e5ff">TT:{row["Signal"][:10]}</div>'
                     f'{iq_verdict_badge(iq_v)}'
                     f'<div style="font-family:Space Mono,monospace;font-size:9px;padding:2px 7px;background:rgba(0,0,0,.3);border-radius:4px;color:#4a5568">IQ:{iq_s:.0f} {iq_m[:4]}</div>'
                     f'</div>'
                     f'<div class="sc-stats">'
                     f'<div class="sc-stat">RSI <span>{row["RSI-EMA"]}</span></div>'
                     f'<div class="sc-stat">RVOL <span>{row["RVOL"]}x</span></div>'
                     f'<div class="sc-stat">ASING <span style="color:{fc}">{fd}</span></div>'
                     f'</div>'
                     f'<div class="sc-stats" style="margin-top:6px;">'
                     f'<div class="sc-stat">TP <span style="color:#00ff88">{int(row["TP"]):,}</span></div>'
                     f'<div class="sc-stat">SL <span style="color:#ff3d5a">{int(row["SL"]):,}</span></div>'
                     f'<div class="sc-stat">R:R <span>{row["R:R"]}</span></div>'
                     f'</div>'
                     f'<div style="margin-top:6px;font-size:9px;color:#4a5568;font-family:Space Mono,monospace">{row["Reasons"][:65]}</div>'
                     f'</div>')
            ch+='</div>'; st.markdown(ch, unsafe_allow_html=True)

        st.markdown('<div class="section-title">Full Signal Table — MESIN PRESISI x 151 STRATEGIES</div>', unsafe_allow_html=True)
        col_headers=["EMITEN","GRADE","MS","AKSI","SINYAL 15M","IQ DAILY","IQ SCORE","IQ MA","IQ BAG","RVOL","GAIN","NOW","TP","SL","PROFIT","RSI","TURNOVER","ASING"]
        header_html="".join(f'<th style="padding:7px 6px;color:#4a5568;font-family:Space Mono,monospace;font-size:9px;letter-spacing:1px;border-bottom:2px solid #1c2533;white-space:nowrap">{h}</th>' for h in col_headers)
        rows_html=""
        for _,row in df_out.head(50).iterrows():
            roc=row.get("ROC 3B%",0)
            gc="#00ff88" if roc>0 else "#ff3d5a"
            rsi_v=float(row.get("RSI-EMA",50))
            rsi_c="#00ff88" if rsi_v>60 else("#ff3d5a" if rsi_v<35 else("#ff7b00" if rsi_v<45 else "#4a5568"))
            rvol_s=f"{float(row.get('RVOL',1)):.1f}x"
            fd=str(row.get("FDir","—")); fc=str(row.get("FC","#4a5568"))
            sv2=str(row.get("Sinyal_v2",row.get("Signal","-"))); av2=str(row.get("Aksi_v2","-"))
            tp_v=row.get("TP",0); sl_v=row.get("SL",0); price=row.get("Price",0)
            pp=(tp_v-price)/max(price,1)*100 if price>0 else 0
            mg_v=str(row.get("Mesin_Grade","—")); mg_c2=str(row.get("Mesin_Color","#4a5568"))
            ms_v=float(row.get("Mesin_Score",0))
            iq_v=str(row.get("IQ_Verdict","—")); iq_s=float(row.get("IQ_Score",0))
            iq_m=str(row.get("IQ_MA","—")); iq_b=float(row.get("IQ_Bagger",0))
            iq_m_color="#00ff88" if iq_m=="BULLISH" else("#ff3d5a" if iq_m=="BEARISH" else "#5a6478")
            iq_b_color="#bf5fff" if iq_b>=65 else "#4a5568"
            iq_b_star="*" if iq_b>=65 else ""
            rows_html+=(f'<tr style="font-family:Space Mono,monospace;font-size:10px;">'
                f'<td style="padding:5px 6px;font-weight:700;color:#e6edf3;text-align:left;border-bottom:1px solid #1c2533;white-space:nowrap">{row["Ticker"]}</td>'
                f'<td style="padding:5px 6px;border-bottom:1px solid #1c2533;text-align:left">{mesin_badge(mg_v)}</td>'
                f'<td style="padding:5px 6px;color:{mg_c2};font-weight:700;border-bottom:1px solid #1c2533;text-align:center">{ms_v:.0f}</td>'
                f'<td style="padding:5px 6px;border-bottom:1px solid #1c2533;text-align:center">{tt_aksi_badge(av2)}</td>'
                f'<td style="padding:5px 6px;border-bottom:1px solid #1c2533;text-align:center">{tt_sinyal_badge(sv2)}</td>'
                f'<td style="padding:5px 6px;border-bottom:1px solid #1c2533;text-align:center">{iq_verdict_badge(iq_v)}</td>'
                f'<td style="padding:5px 6px;color:#2dd4bf;font-weight:700;border-bottom:1px solid #1c2533;text-align:center">{iq_s:.0f}</td>'
                f'<td style="padding:5px 6px;color:{iq_m_color};border-bottom:1px solid #1c2533;text-align:center;font-size:9px">{iq_m}</td>'
                f'<td style="padding:5px 6px;color:{iq_b_color};border-bottom:1px solid #1c2533;text-align:center">{iq_b:.0f}{iq_b_star}</td>'
                f'<td style="padding:5px 6px;color:#ff7b00;font-weight:700;border-bottom:1px solid #1c2533;text-align:center">{rvol_s}</td>'
                f'<td style="padding:5px 6px;color:{gc};font-weight:700;border-bottom:1px solid #1c2533;text-align:center">{roc:+.1f}%</td>'
                f'<td style="padding:5px 6px;color:#c9d1d9;border-bottom:1px solid #1c2533;text-align:center">{row["Price"]:,}</td>'
                f'<td style="padding:5px 6px;background:#0d2b0d;color:#00ff88;font-weight:700;border-bottom:1px solid #1c2533;text-align:center">{int(tp_v):,}</td>'
                f'<td style="padding:5px 6px;background:#2b0d0d;color:#ff3d5a;border-bottom:1px solid #1c2533;text-align:center">{int(sl_v):,}</td>'
                f'<td style="padding:5px 6px;color:#00ff88;border-bottom:1px solid #1c2533;text-align:center">{pp:.1f}%</td>'
                f'<td style="padding:5px 6px;color:{rsi_c};border-bottom:1px solid #1c2533;text-align:center">{rsi_v:.0f}</td>'
                f'<td style="padding:5px 6px;color:#4a5568;font-size:9px;border-bottom:1px solid #1c2533;text-align:center">{row.get("Turnover(M)",0):.0f}M</td>'
                f'<td style="padding:5px 6px;color:{fc};border-bottom:1px solid #1c2533;text-align:center;font-size:10px">{fd}</td>'
                f'</tr>')
        st.markdown(
            f'<div style="overflow-x:auto;border-radius:8px;border:1px solid #1c2533;max-height:70vh;overflow-y:auto;">'
            f'<table style="width:100%;border-collapse:collapse;">'
            f'<thead><tr style="background:#080c10;position:sticky;top:0;z-index:10;">{header_html}</tr></thead>'
            f'<tbody style="background:#0d1117">{rows_html}</tbody>'
            f'</table>'
            f'<div style="padding:6px 12px;background:#080c10;font-family:Space Mono,monospace;font-size:9px;color:#4a5568;border-top:1px solid #1c2533">'
            f'Mesin Presisi v1.0 — TT 15M x IDX Quant Daily (151 Strategies) — Zero Rate Limit</div>'
            f'</div>',
            unsafe_allow_html=True)

# ════ TAB WATCHLIST ════
with tab_watchlist:
    st.markdown('<div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-bottom:12px;padding:10px 14px;background:#0d1117;border-radius:6px;border-left:3px solid #ff7b00;">Analisa mendalam per saham. Input ticker IDX (tanpa .JK).</div>',unsafe_allow_html=True)
    wc1,wc2,wc3=st.columns([3,1,1])
    with wc1:
        wl_input=st.text_area("Ticker",placeholder="Contoh:\nBBCA\nARCI, ASSA, GOTO",height=120,label_visibility="collapsed",key="wl_input")
    with wc2:
        wl_mode=st.radio("Mode",["Scalping ⚡","Momentum 🚀","Reversal 🎯","Bagger 💎"],key="wl_mode")
        st.caption(f"Regime suggest: {rcfg['mode']}")
    with wc3:
        st.markdown("<br>",unsafe_allow_html=True)
        wl_force=st.toggle("🔄 Fresh",value=False,key="wl_fresh")
        wl_run=st.button("🔍 Analisa",use_container_width=True,key="wl_run")
        wl_tele=st.button("📡 Kirim Telegram",use_container_width=True,key="wl_tele")
    if wl_run and wl_input.strip():
        raw_wl=list(dict.fromkeys([t.strip().upper() for ln in wl_input.split("\n") for t in ln.split(",") if t.strip()]))
        if raw_wl:
            wl_res=[]; _pb_wl=st.progress(0)
            for i,t in enumerate(raw_wl):
                _pb_wl.progress((i+1)/len(raw_wl))
                df=None
                try:
                    if DS_KEY: df=fetch_ds_ohlcv(t,"15m",200,wl_force)
                    if df is None:
                        # yFinance fallback — Ticker().history(), no rate limit!
                        df=_fetch_yf_ticker(t+".JK","7d","15m")
                except: pass
                if df is None or len(df)<30:
                    wl_res.append({"Ticker":t,"Price":0,"Score":0,"Signal":"No data","Trend":"-",
                        "RSI-EMA":0,"Stoch K":0,"RVOL":0,"BB%":0,"ROC 3B%":0,
                        "VWAP":0,"TP":0,"SL":0,"R:R":0,"ATR":0,"Reasons":"No data","_class":"","MACD Hist":0}); continue
                try:
                    df2=apply_intraday_indicators(df.copy())
                    r=df2.iloc[-1]; p=df2.iloc[-2]; p2=df2.iloc[-3] if len(df2)>=3 else p
                    close=float(r["Close"]); atr=float(r["ATR"])
                    if wl_mode=="Scalping ⚡":   sc,reasons,_=score_scalping(r,p,p2);  tp=close+1.5*atr; sl=close-0.8*atr
                    elif wl_mode=="Momentum 🚀": sc,reasons,_=score_momentum(r,p,p2);  tp=close+2.0*atr; sl=close-0.8*atr
                    elif wl_mode=="Bagger 💎":   sc,reasons,_=score_bagger(r,p,p2,df2);tp=close+3.0*atr; sl=close-1.0*atr
                    else:                        sc,reasons,_=score_reversal(r,p,p2);  tp=close+2.5*atr; sl=close-0.6*atr
                    sig=get_signal(sc,wl_mode); sig_v2,sc_v2,_,gc_now=get_sinyal_v2(r,p,p2)
                    rr=(tp-close)/max(close-sl,0.01)
                    e9=float(r["EMA9"]); e21=float(r["EMA21"]); e50=float(r["EMA50"])
                    trend="▲ UP" if e9>e21>e50 else("▼ DOWN" if e9<e21<e50 else "◆ SIDE")
                    wl_res.append({"Ticker":t,"Price":int(close),"Score":sc,"Signal":sig,
                        "Trend":trend,"RSI-EMA":round(float(r["RSI_EMA"]),1),
                        "Stoch K":round(float(r["STOCH_K"]),1),"RVOL":round(float(r["RVOL"]),2),
                        "BB%":round(float(r["BB_pct"]),2),"ROC 3B%":round(float(r["ROC3"])*100,2),
                        "VWAP":int(float(r["VWAP"])),"TP":int(tp),"SL":int(sl),"R:R":round(rr,1),
                        "ATR":round(atr,0),"MACD Hist":round(float(r["MACD_Hist"]),4),
                        "Reasons":" · ".join(reasons),"_class":get_card_class(sig)})
                except Exception as ex:
                    wl_res.append({"Ticker":t,"Price":0,"Score":0,"Signal":f"Err:{str(ex)[:20]}",
                        "Trend":"-","RSI-EMA":0,"Stoch K":0,"RVOL":0,"BB%":0,"ROC 3B%":0,
                        "VWAP":0,"TP":0,"SL":0,"R:R":0,"ATR":0,"Reasons":"","_class":"","MACD Hist":0})
            _pb_wl.empty()
            st.session_state.wl_results=wl_res; st.session_state.wl_mode_used=wl_mode
    if wl_tele and st.session_state.wl_results:
        to_send=[r for r in st.session_state.wl_results if r["Price"]>0]
        if to_send: send_telegram(to_send[:5],source="Watchlist"); st.success("📡 Terkirim!")
    if st.session_state.wl_results:
        ok=[r for r in st.session_state.wl_results if r["Score"]>0]
        gcr=[r for r in ok if any(k in r.get("Signal","") for k in ["GACOR","REVERSAL","HAKA","BANDAR"])]
        pot=[r for r in ok if any(k in r.get("Signal","") for k in ["POTENSIAL","REBOUND"])]
        st.markdown(f"""<div class="metric-row" style="margin-top:12px;">
          <div class="metric-card orange"><div class="metric-label">Dipantau</div><div class="metric-value">{len(st.session_state.wl_results)}</div></div>
          <div class="metric-card green"><div class="metric-label">GACOR/BANDAR 🔥</div><div class="metric-value">{len(gcr)}</div></div>
          <div class="metric-card amber"><div class="metric-label">POTENSIAL</div><div class="metric-value">{len(pot)}</div></div>
          <div class="metric-card"><div class="metric-label">Data OK</div><div class="metric-value">{len(ok)}</div></div>
        </div>""",unsafe_allow_html=True)
        ch='<div class="signal-grid">'
        for row in sorted(st.session_state.wl_results,key=lambda x:x["Score"],reverse=True):
            if row["Price"]==0:
                ch+=f'<div class="signal-card"><div class="sc-ticker">{row["Ticker"]}</div><div style="font-size:11px;color:#4a5568;margin-top:6px;">{row.get("Signal","No data")}</div></div>'; continue
            si=int(row["Score"]); bars="".join([f'<div class="sc-bar {"filled" if i<si else "empty"}" style="width:26px"></div>' for i in range(6)])
            sig=row.get("Signal","-")
            sc2="#00ff88" if any(k in sig for k in ["GACOR","REVERSAL","HAKA","BANDAR"]) else("#ffb700" if "POTENSIAL" in sig else "#00e5ff" if "WATCH" in sig else "#4a5568")
            rv=row["RSI-EMA"]; rc="#ff3d5a" if rv<30 else("#ffb700" if rv<45 else "#00ff88" if rv>60 else "#c9d1d9")
            roc="#00ff88" if row.get("ROC 3B%",0)>0 else "#ff3d5a"
            te="📈" if "▲" in row["Trend"] else("📉" if "▼" in row["Trend"] else "➡️")
            ch+=f"""<div class="signal-card {row["_class"]}">
              <div style="display:flex;justify-content:space-between;">
                <div><div class="sc-ticker">{row["Ticker"]}</div>
                <div class="sc-price" style="color:{roc}">{row["Price"]:,} {te}</div></div>
                <div style="text-align:right"><div style="font-family:Space Mono,monospace;font-size:9px;color:#4a5568">SCORE</div>
                <div style="font-family:Space Mono,monospace;font-size:22px;font-weight:700;color:{sc2}">{row["Score"]}</div></div>
              </div>
              <div class="sc-signal" style="color:{sc2}">{sig}</div>
              <div class="sc-bars">{bars}</div>
              <div class="sc-stats">
                <div class="sc-stat">RSI-EMA <span style="color:{rc}">{rv}</span></div>
                <div class="sc-stat">RVOL <span>{row["RVOL"]}x</span></div>
                <div class="sc-stat">TP <span style="color:#00ff88">{int(row["TP"]):,}</span></div>
                <div class="sc-stat">SL <span style="color:#ff3d5a">{int(row["SL"]):,}</span></div>
              </div>
              <div style="margin-top:8px;font-size:10px;color:#4a5568;line-height:1.5;font-family:Space Mono,monospace">{row["Reasons"][:80]}</div>
            </div>"""
        ch+='</div>'; st.markdown(ch,unsafe_allow_html=True)
    elif not wl_run:
        st.markdown('<div style="text-align:center;padding:48px;color:#4a5568;font-family:Space Mono,monospace;"><div style="font-size:32px;margin-bottom:12px;">👁️</div><div>MASUKKAN TICKER DI ATAS</div></div>',unsafe_allow_html=True)

# ════ TAB BSJP ════
with tab_bsjp:
    nw=datetime.now(jakarta_tz)
    iet=(nw.hour==14 and nw.minute>=30) or (nw.hour==15 and nw.minute<=45)
    st.markdown(f"""<div style="background:rgba(191,95,255,.08);border:1px solid rgba(191,95,255,.3);border-radius:8px;padding:14px 18px;margin-bottom:16px;">
      <div style="font-family:Space Mono,monospace;font-size:13px;font-weight:700;color:#bf5fff;">🌙 BELI SORE JUAL PAGI</div>
      <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-top:4px;">
        Entry: <span style="color:#ffb700">14:30–15:45 WIB</span> · Exit: <span style="color:#00ff88">Besok 09:00–10:00 WIB</span> ·
        Status: <span style="color:{"#00ff88" if iet else "#4a5568"}>{"🟢 WAKTU ENTRY!" if iet else "⏳ Tunggu 14:30 WIB"}</span>
      </div></div>""",unsafe_allow_html=True)
    bc1,bc2=st.columns([2,1])
    with bc1:
        bsjp_min_score=st.slider("Min BSJP Score",0,6,4,key="bsjp_score")
        bsjp_min_rvol=st.slider("Min RVOL",1.0,5.0,1.5,0.1,key="bsjp_rvol")
    with bc2:
        bsjp_min_turn=st.number_input("Min Turnover (M Rp)",value=500,step=100,key="bsjp_turn")*1_000_000
    do_bsjp=st.button("🌙 SCAN BSJP SEKARANG",type="primary",use_container_width=True,key="btn_bsjp")
    if do_bsjp:
        bsjp_res=[]; scan_data=st.session_state.get("data_dict",{})
        if not scan_data:
            st.info("Fetching data untuk BSJP...")
            scan_data=fetch_intraday(tuple(stocks_yf[:200]))
        pb_bsjp=st.progress(0); tbs=list(scan_data.keys())
        for i,ty in enumerate(tbs):
            pb_bsjp.progress((i+1)/max(len(tbs),1))
            try:
                df=scan_data[ty].copy()
                if len(df)<30: continue
                df2=apply_intraday_indicators(df)
                r=df2.iloc[-1]; p=df2.iloc[-2]; p2=df2.iloc[-3] if len(df2)>=3 else p
                close=float(r["Close"]); vol=float(r["Volume"]); to=close*vol; rv=float(r["RVOL"])
                if to<bsjp_min_turn or rv<bsjp_min_rvol: continue
                sc,reasons,_=score_bsjp(r,p,p2)
                if sc<bsjp_min_score: continue
                bs="STRONG BUY 🌙" if sc>=5 else("BUY ⚡" if sc>=4 else "WATCH 👀")
                atr=float(r["ATR"]); tp=close+2.0*atr; sl=close-1.0*atr; rr=(tp-close)/max(close-sl,0.01)
                e9=float(r["EMA9"]); e21=float(r["EMA21"]); e50=float(r["EMA50"])
                trend="▲ UP" if e9>e21>e50 else("▼ DOWN" if e9<e21<e50 else "◆ SIDE")
                bsjp_res.append({"Ticker":stock_map.get(ty,ty.replace(".JK","")),"Price":int(close),
                    "Score":sc,"Signal":bs,"Trend":trend,"RSI-EMA":round(float(r["RSI_EMA"]),1),
                    "Stoch K":round(float(r["STOCH_K"]),1),"RVOL":round(rv,2),"TP":int(tp),"SL":int(sl),
                    "R:R":round(rr,1),"Turnover(M)":round(to/1e6,1),"Reasons":" · ".join(reasons),
                    "_class":"gacor" if sc>=5 else "potensial" if sc>=4 else "watch"})
            except: continue
        pb_bsjp.empty()
        st.session_state.bsjp_results=sorted(bsjp_res,key=lambda x:x["Score"],reverse=True)
    br=st.session_state.bsjp_results
    if br:
        strong=[r for r in br if "STRONG" in r.get("Signal","")]
        buy=[r for r in br if r.get("Signal","")=="BUY ⚡"]
        st.markdown(f"""<div class="metric-row">
          <div class="metric-card" style="border-top-color:#bf5fff"><div class="metric-label">Kandidat</div><div class="metric-value">{len(br)}</div></div>
          <div class="metric-card green"><div class="metric-label">Strong Buy 🌙</div><div class="metric-value">{len(strong)}</div></div>
          <div class="metric-card amber"><div class="metric-label">Buy ⚡</div><div class="metric-value">{len(buy)}</div></div>
        </div>""",unsafe_allow_html=True)
        bh='<div class="signal-grid">'
        for row in br[:12]:
            si=int(row["Score"]); bars="".join([f'<div class="sc-bar {"filled" if i<si else "empty"}" style="width:26px"></div>' for i in range(6)])
            sc2="#00ff88" if "STRONG" in row["Signal"] else "#ffb700"
            te="📈" if "▲" in row["Trend"] else("📉" if "▼" in row["Trend"] else "➡️")
            bh+=f"""<div class="signal-card {row["_class"]}">
              <div style="display:flex;justify-content:space-between;">
                <div><div class="sc-ticker">{row["Ticker"]}</div><div class="sc-price">{row["Price"]:,} {te}</div></div>
                <div style="text-align:right"><div style="font-family:Space Mono,monospace;font-size:9px;color:#4a5568">SCORE</div>
                  <div style="font-family:Space Mono,monospace;font-size:22px;font-weight:700;color:{sc2}">{row["Score"]}</div></div>
              </div>
              <div class="sc-signal" style="color:{sc2}">{row["Signal"]}</div>
              <div class="sc-bars">{bars}</div>
              <div class="sc-stats">
                <div class="sc-stat">RSI-EMA <span>{row["RSI-EMA"]}</span></div>
                <div class="sc-stat">RVOL <span>{row["RVOL"]}x</span></div>
                <div class="sc-stat">R:R <span>{row["R:R"]}</span></div>
              </div>
              <div class="sc-stats" style="margin-top:6px">
                <div class="sc-stat">TP <span style="color:#00ff88">{row["TP"]:,}</span></div>
                <div class="sc-stat">SL <span style="color:#ff3d5a">{row["SL"]:,}</span></div>
              </div>
              <div style="margin-top:4px;font-size:10px;color:#4a5568;font-family:Space Mono,monospace">{row["Reasons"][:70]}</div>
            </div>"""
        bh+='</div>'; st.markdown(bh,unsafe_allow_html=True)
    elif not do_bsjp:
        st.markdown('<div style="text-align:center;padding:48px;color:#4a5568;font-family:Space Mono,monospace;"><div style="font-size:32px;margin-bottom:12px;">🌙</div><div>KLIK SCAN BSJP</div></div>',unsafe_allow_html=True)

# ════ TAB SEKTOR ════
with tab_sector:
    st.markdown('<div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-bottom:14px;padding:10px 14px;background:#0d1117;border-radius:6px;border-left:3px solid #ff7b00;">Track pergerakan tiap sektor IDX. ⚡ Hormuz: Energi, Shipping, Petrokimia</div>',unsafe_allow_html=True)
    do_sector=st.button("🏭 REFRESH SEKTOR",type="primary",use_container_width=True,key="btn_sector")
    if do_sector:
        sec_data={}
        for sn,ss in SECTORS.items():
            res=fetch_sector_rotation(ss)
            if res:
                sec_data[sn]={"avg_chg":round(sum(r["chg"] for r in res)/len(res),2),
                    "avg_rvol":round(sum(r["rvol"] for r in res)/len(res),2),
                    "bullish":sum(1 for r in res if r["chg"]>0),"total":len(res),
                    "stocks":res,"is_hormuz":sn in HORMUZ_SECTORS}
        st.session_state.sector_data=sec_data
    if st.session_state.sector_data:
        ss=sorted(st.session_state.sector_data.items(),key=lambda x:x[1]["avg_chg"],reverse=True)
        cols_sec=st.columns(3)
        for idx,(sn,si) in enumerate(ss):
            chg=si["avg_chg"]; col="#00ff88" if chg>1 else("#ffb700" if chg>0 else "#ff3d5a")
            bg="rgba(0,255,136,.06)" if chg>1 else("rgba(255,183,0,.06)" if chg>0 else "rgba(255,61,90,.06)")
            hb=' <span style="color:#ffb700;font-size:9px">⚡HORMUZ</span>' if si["is_hormuz"] else ""
            bp=int(si["bullish"]/max(si["total"],1)*100)
            with cols_sec[idx%3]:
                st.markdown(f"""<div style="background:{bg};border:1px solid {col}44;border-radius:8px;padding:12px;margin-bottom:10px;">
                  <div style="font-family:Space Mono,monospace;font-size:10px;font-weight:700;color:#c9d1d9;">{sn}{hb}</div>
                  <div style="font-family:Space Mono,monospace;font-size:22px;font-weight:700;color:{col};margin:4px 0;">{chg:+.2f}%</div>
                  <div style="font-size:9px;color:#4a5568;">RVOL avg: {si["avg_rvol"]:.1f}x · Bullish: {si["bullish"]}/{si["total"]} ({bp}%)</div>
                  <div style="height:4px;background:#1c2533;border-radius:2px;margin-top:6px;overflow:hidden;">
                    <div style="width:{bp}%;height:100%;background:{col};border-radius:2px;"></div>
                  </div></div>""",unsafe_allow_html=True)
    else:
        st.markdown('<div style="text-align:center;padding:48px;color:#4a5568;font-family:Space Mono,monospace;"><div style="font-size:32px;margin-bottom:12px;">🏭</div><div>KLIK REFRESH SEKTOR</div></div>',unsafe_allow_html=True)

# ════ TAB GAP UP ════
with tab_gapup:
    st.markdown('<div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-bottom:14px;padding:10px 14px;background:#0d1117;border-radius:6px;border-left:3px solid #00ff88;">Deteksi saham <b style="color:#00ff88">Gap Up besok</b> — via yFinance Ticker().history() parallel ✅</div>',unsafe_allow_html=True)
    gc1,gc2=st.columns(2)
    with gc1: gms=st.slider("Min Gap Score",1,6,3,key="gu_score")
    with gc2: gq=st.toggle("⚡ Quick Scan (200)",value=True,key="gu_quick")
    do_gu=st.button("📈 SCAN GAP UP",type="primary",use_container_width=True,key="btn_gapup")
    if do_gu:
        gt=stocks_yf[:200] if gq else stocks_yf
        gr=scan_gap_up(tuple(gt))
        st.session_state.gapup_results=[r for r in gr if r["Gap Score"]>=gms]
    gr=st.session_state.gapup_results
    if gr:
        gc3=[r for r in gr if "GAP UP" in r.get("Signal","")]
        gp=[r for r in gr if "POTENTIAL" in r.get("Signal","")]
        st.markdown(f"""<div class="metric-row">
          <div class="metric-card green"><div class="metric-label">Gap Confirmed 🚀</div><div class="metric-value">{len(gc3)}</div></div>
          <div class="metric-card amber"><div class="metric-label">Potential ⚡</div><div class="metric-value">{len(gp)}</div></div>
          <div class="metric-card"><div class="metric-label">Total</div><div class="metric-value">{len(gr)}</div></div>
        </div>""",unsafe_allow_html=True)
        gh='<div class="signal-grid">'
        for row in gr[:20]:
            si=int(min(row["Gap Score"],6)); bars="".join([f'<div class="sc-bar {"filled" if i<si else "empty"}" style="width:26px"></div>' for i in range(6)])
            ig="GAP UP" in row.get("Signal",""); sc2="#00ff88" if ig else "#ffb700"
            cc="#00ff88" if row["Chg %"]>0 else "#ff3d5a"
            gh+=f"""<div class="signal-card {"gacor" if ig else "potensial"}">
              <div style="display:flex;justify-content:space-between;">
                <div><div class="sc-ticker">{row["Ticker"]}</div>
                <div class="sc-price" style="color:{cc}">{row["Price"]:,} ({row["Chg %"]:+.1f}%)</div></div>
                <div style="text-align:right"><div style="font-family:Space Mono,monospace;font-size:9px;color:#4a5568">GAP SCORE</div>
                  <div style="font-family:Space Mono,monospace;font-size:22px;font-weight:700;color:{sc2}">{row["Gap Score"]}</div></div>
              </div>
              <div class="sc-signal" style="color:{sc2}">{row["Signal"]}</div>
              <div class="sc-bars">{bars}</div>
              <div class="sc-stats">
                <div class="sc-stat">RVOL <span>{row["RVOL"]}x</span></div>
                <div class="sc-stat">Close% <span>{row["Close Ratio"]:.0%}</span></div>
                <div class="sc-stat">PrevHigh <span>{row["Prev High"]:,}</span></div>
              </div>
              <div style="margin-top:8px;font-size:10px;color:#4a5568;font-family:Space Mono,monospace">{row["Reasons"][:80]}</div>
            </div>"""
        gh+='</div>'; st.markdown(gh,unsafe_allow_html=True)
    elif not do_gu:
        st.markdown('<div style="text-align:center;padding:48px;color:#4a5568;font-family:Space Mono,monospace;"><div style="font-size:32px;margin-bottom:12px;">📈</div><div>KLIK SCAN GAP UP</div></div>',unsafe_allow_html=True)

# ════ TAB TRAILING STOP ════
with tab_trail:
    st.markdown('<div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-bottom:14px;padding:10px 14px;background:#0d1117;border-radius:6px;border-left:3px solid #bf5fff;">Lock profit. Trailing Stop otomatis ikut harga naik.</div>',unsafe_allow_html=True)
    tc1,tc2=st.columns(2)
    with tc1:
        st.markdown('<div class="settings-label">POSISI</div>',unsafe_allow_html=True)
        tr_t=st.text_input("Ticker",value="BBCA",key="tr_ticker").upper()
        tr_e=st.number_input("Harga Entry",value=9000,step=10,key="tr_entry")
        tr_q=st.number_input("Jumlah Lot",value=10,step=1,key="tr_qty")
    with tc2:
        st.markdown('<div class="settings-label">SETTING</div>',unsafe_allow_html=True)
        tr_m=st.radio("Metode",["ATR","Persen","Swing Low"],key="tr_method")
        if tr_m=="ATR": tr_am=st.slider("ATR Multiplier",1.0,5.0,2.0,0.5,key="tr_atr_m")
        elif tr_m=="Persen": tr_p=st.slider("Trailing %",1.0,10.0,3.0,0.5,key="tr_pct")
        tr_al=st.toggle("🔔 Alert Telegram",value=True,key="tr_alert")
    if st.button("🎯 HITUNG TRAILING STOP",type="primary",use_container_width=True,key="btn_trail"):
        try:
            df_tr=None
            if DS_KEY: df_tr=fetch_ds_ohlcv(tr_t,"15m",200)
            if df_tr is None:
                # yFinance fallback — Ticker().history(), no rate limit!
                df_tr=_fetch_yf_ticker(tr_t+".JK","7d","15m")
            if df_tr is not None and len(df_tr)>20:
                df_tr2=apply_intraday_indicators(df_tr.copy())
                cur=float(df_tr2["Close"].iloc[-1]); av=float(df_tr2["ATR"].iloc[-1])
                if tr_m=="ATR":      res=calc_trailing_stop(tr_e,cur,av,"ATR",tr_am)
                elif tr_m=="Persen": res=calc_trailing_stop(tr_e,cur,av,"Persen",pct=tr_p)
                else:                res=calc_trailing_stop(tr_e,cur,av,"Swing Low")
                stop=res["stop"]; pf=res["profit_float"]; pl=res["profit_locked"]; ip=res["is_profitable"]
                lv=tr_q*100; pr=(cur-tr_e)*lv; lr=max(0,(stop-tr_e)*lv)
                sc2="#00ff88" if ip else "#ff3d5a"; pc2="#00ff88" if pr>=0 else "#ff3d5a"
                st.markdown(f"""<div style="background:#0d1117;border:1px solid {sc2}44;border-radius:10px;padding:20px;margin-top:12px;">
                  <div class="metric-row">
                    <div class="metric-card"><div class="metric-label">Sekarang</div><div class="metric-value" style="color:#00e5ff">{int(cur):,}</div><div class="metric-sub">ATR: {int(av)}</div></div>
                    <div class="metric-card" style="border-top-color:{sc2}"><div class="metric-label">🎯 Trailing Stop</div><div class="metric-value" style="color:{sc2}">{int(stop):,}</div></div>
                    <div class="metric-card" style="border-top-color:{pc2}"><div class="metric-label">Profit Float</div><div class="metric-value" style="color:{pc2}">{pf:+.1f}%</div><div class="metric-sub">Rp {pr:,.0f}</div></div>
                    <div class="metric-card" style="border-top-color:#00ff88"><div class="metric-label">Terkunci 🔒</div><div class="metric-value" style="color:#00ff88">{pl:+.1f}%</div><div class="metric-sub">Rp {lr:,.0f}</div></div>
                  </div>
                  <div style="margin-top:10px;font-family:Space Mono,monospace;font-size:10px;color:#4a5568;">
                    {tr_t} · Entry {tr_e:,} → Now {int(cur):,} · {tr_q} lot ({lv:,} lembar) · {"✅ Profit terkunci!" if ip else "⚠️ Stop masih di bawah entry"}
                  </div></div>""",unsafe_allow_html=True)
                if tr_al and TOKEN and CHAT_ID:
                    mt=(f"🎯 *TRAILING STOP*\n{tr_t} · {tr_m}\n"
                        f"Entry: `{tr_e:,}` → Now: `{int(cur):,}`\n"
                        f"Stop: `{int(stop):,}` · Float: `{pf:+.1f}%`\n"
                        f"Locked: `{pl:+.1f}%` (Rp {lr:,.0f})")
                    try: requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",data={"chat_id":CHAT_ID,"text":mt,"parse_mode":"Markdown"},timeout=8)
                    except: pass
            else: st.error(f"Data {tr_t} tidak tersedia")
        except Exception as ex: st.error(f"Error: {str(ex)[:80]}")

# ════ TAB BACKTEST ════
with tab_backtest:
    st.markdown('<div class="section-title">Backtest Engine · 15M Intraday</div>',unsafe_allow_html=True)
    bt1,bt2,bt3,bt4=st.columns(4)
    bt_mode=bt1.selectbox("Mode",["Scalping ⚡","Momentum 🚀","Reversal 🎯","Bagger 💎"],key="bt_mode")
    bt_sc=bt2.slider("Min Score",0,6,4,key="bt_sc")
    bt_fwd=int(bt3.number_input("Hold (bars)",value=4,step=1,min_value=1,max_value=20))
    bt_sl=bt4.number_input("SL mult (x ATR)",value=0.8,step=0.1,min_value=0.1,max_value=3.0)
    st.caption(f"Hold {bt_fwd} bars × 15 min = ~{bt_fwd*15} menit per trade")
    if st.button("🚀 Run Backtest",type="primary",key="bt_run"):
        dd=st.session_state.get("data_dict",{})
        if not dd: st.warning("Jalankan Scanner dulu bro!")
        else:
            bt_r=[]; bpb=st.progress(0); sample=list(dd.keys())[:80]
            for bi,ty in enumerate(sample):
                bpb.progress((bi+1)/len(sample))
                try:
                    d=dd[ty].copy()
                    if len(d)<60: continue
                    d=apply_intraday_indicators(d)
                    for ii in range(50,len(d)-bt_fwd):
                        r0=d.iloc[ii]; r1=d.iloc[ii-1]; r2=d.iloc[ii-2]
                        if bt_mode=="Scalping ⚡":   sc,_,_=score_scalping(r0,r1,r2)
                        elif bt_mode=="Momentum 🚀": sc,_,_=score_momentum(r0,r1,r2)
                        elif bt_mode=="Bagger 💎":   sc,_,_=score_bagger(r0,r1,r2,d.iloc[:ii+1])
                        else:                         sc,_,_=score_reversal(r0,r1,r2)
                        if sc<bt_sc: continue
                        en=float(r0["Close"]); av=float(r0["ATR"]) if not np.isnan(float(r0["ATR"])) else en*0.005
                        if bt_mode=="Bagger 💎": tp2=en+3.0*av; sl2=en-1.0*av
                        else: tp2=en+2.0*av; sl2=en-bt_sl*av
                        ex=float(d.iloc[ii+bt_fwd]["Close"])
                        for fi in range(1,bt_fwd+1):
                            bar=d.iloc[ii+fi]
                            if float(bar["High"])>=tp2: ex=tp2; break
                            if float(bar["Low"])<=sl2:  ex=sl2; break
                        bt_r.append((ex-en)/en*100)
                except: continue
            bpb.empty()
            if not bt_r: st.warning("Tidak ada trades. Turunkan Min Score.")
            else:
                arr=np.array(bt_r); wr=len(arr[arr>0])/len(arr)*100
                avg=np.mean(arr); med=np.median(arr)
                pf=arr[arr>0].sum()/max(abs(arr[arr<0].sum()),0.01)
                mxdd=arr[arr<0].min() if len(arr[arr<0])>0 else 0
                st.markdown(f"""<div class="bt-result">
                  <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;letter-spacing:2px;margin-bottom:14px;">{len(arr)} TRADES · SCORE≥{bt_sc} · HOLD {bt_fwd} BARS (~{bt_fwd*15}M) · {bt_mode}</div>
                  <div style="display:flex;flex-wrap:wrap;">
                    <span class="bt-metric"><div class="bt-metric-val" style="color:{"#00ff88" if wr>=55 else "#ffb700" if wr>=50 else "#ff3d5a"}">{wr:.1f}%</div><div class="bt-metric-lbl">Win Rate</div></span>
                    <span class="bt-metric"><div class="bt-metric-val" style="color:{"#00ff88" if avg>0 else "#ff3d5a"}">{avg:+.2f}%</div><div class="bt-metric-lbl">Avg Return</div></span>
                    <span class="bt-metric"><div class="bt-metric-val" style="color:#00e5ff">{med:+.2f}%</div><div class="bt-metric-lbl">Median</div></span>
                    <span class="bt-metric"><div class="bt-metric-val" style="color:{"#00ff88" if pf>=1.5 else "#ffb700" if pf>=1 else "#ff3d5a"}">{pf:.2f}x</div><div class="bt-metric-lbl">Profit Factor</div></span>
                    <span class="bt-metric"><div class="bt-metric-val" style="color:#ff3d5a">{mxdd:.1f}%</div><div class="bt-metric-lbl">Max Loss</div></span>
                    <span class="bt-metric"><div class="bt-metric-val" style="color:#00ff88">{sum(1 for x in bt_r if x>0)}</div><div class="bt-metric-lbl">TP Hits</div></span>
                    <span class="bt-metric"><div class="bt-metric-val" style="color:#ff3d5a">{sum(1 for x in bt_r if x<0)}</div><div class="bt-metric-lbl">SL Hits</div></span>
                  </div></div>""",unsafe_allow_html=True)

# ════ FOOTER + AUTO-REFRESH ════
_nf=now_jkt.timestamp()
if st.session_state.last_scan_time:
    _r2=max(0,300-(_nf-st.session_state.last_scan_time)); m2=int(_r2//60); s2=int(_r2%60)
    _lt2=datetime.fromtimestamp(st.session_state.last_scan_time,jakarta_tz).strftime("%H:%M:%S")
    ti=f"⏱️ Next auto-scan: <span style='color:#ff7b00'>{m2:02d}:{s2:02d}</span> · Last: <span style='color:#2dd4bf'>{_lt2} WIB</span>"
else:
    ti="⏱️ Klik Scan untuk mulai"

st.markdown(f"""<div style="margin-top:28px;padding-top:14px;border-top:1px solid #1c2533;display:flex;justify-content:space-between;flex-wrap:wrap;gap:8px;">
  <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;">🔥 Mesin Presisi v1.0 · TT 15M × IDX Quant (151 Strategies) · Zero Rate Limit ✅</div>
  <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;">{ti}</div>
</div>""",unsafe_allow_html=True)

if st.session_state.last_scan_time:
    if _nf-st.session_state.last_scan_time>=295:
        time.sleep(5); st.rerun()
