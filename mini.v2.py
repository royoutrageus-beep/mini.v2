import yfinance as yf
import pandas as pd
import streamlit as st
import time
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
TOKEN   = st.secrets.get("TELEGRAM_TOKEN", "")
CHAT_ID = st.secrets.get("TELEGRAM_CHAT_ID", "")
jakarta_tz = pytz.timezone('Asia/Jakarta')

try:
    DS_KEY = st.secrets.get("DATASECTORS_API_KEY", "")
except:
    DS_KEY = ""

DS_BASE = "https://api.datasectors.com"

# ════════════════════════════════════════════════════
#  DISK CACHE — thread-safe, persistent antar session
#  FIX: @st.cache_data TIDAK thread-safe di ThreadPoolExecutor!
#  Solusi: pickle di ~/.hp_cache + memory dict + threading.Lock
# ════════════════════════════════════════════════════
CACHE_DIR = Path("/tmp/hp_cache") if Path("/tmp").exists() else Path.home() / ".hp_cache"
CACHE_DIR.mkdir(exist_ok=True)
CACHE_TTL  = 300   # 5 menit
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
    """
    Thread-safe DataSectors fetch.
    FIX: gak pakai @st.cache_data — gantikan dengan disk+memory cache.
    force_fresh=True → skip cache, paksa fetch API.
    """
    if not DS_KEY: return None

    if not force_fresh:
        cached = _cache_get(ticker, interval)
        if cached is not None:
            return cached

    t  = ticker.replace(".JK","").upper().strip()
    tf = TF_MAP.get(str(interval).lower(), "15m")
    # Cache-bust param — paksa CDN/server DS kirim data fresh
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
        df = df.sort_index()  # DS newest-first → sort asc
        if len(df) < 20: return None
        _cache_set(ticker, interval, df)
        return df
    except:
        return None

# ════════════════════════════════════════════════════
#  SESSION STATE
# ════════════════════════════════════════════════════
# ── Disk persistence for scan results ──
_TT_RESULTS_FILE = CACHE_DIR / "tt_last_results.pkl"
_TT_RESULTS_TTL  = 600

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

# Auto-restore dari disk setelah browser refresh
if not st.session_state.scan_results:
    _tt_saved = _tt_load()
    if _tt_saved:
        st.session_state.scan_results = _tt_saved["results"]
        st.session_state.last_scan_time = _tt_saved["ts"]

st.set_page_config(layout="wide", page_title="Theta Turbo v5", page_icon="🔥", initial_sidebar_state="collapsed")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;600;800&display=swap');
:root {
    --bg:#080c10; --surface:#0d1117; --border:#1c2533;
    --accent:#00e5ff; --green:#00ff88; --red:#ff3d5a;
    --amber:#ffb700; --purple:#bf5fff; --orange:#ff7b00;
    --muted:#4a5568; --text:#c9d1d9; --heading:#e6edf3;
}
html,body,[data-testid="stAppViewContainer"]{background:var(--bg)!important;color:var(--text)!important;font-family:'Syne',sans-serif;}
#MainMenu,footer,header{visibility:hidden;}
[data-testid="stSidebar"]{display:none!important;}
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
.metric-card.green::before{background:var(--green);}
.metric-card.red::before{background:var(--red);}
.metric-card.amber::before{background:var(--amber);}
.metric-card.orange::before{background:var(--orange);}
.metric-label{font-size:10px;color:var(--muted);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:4px;}
.metric-value{font-family:'Space Mono',monospace;font-size:24px;font-weight:700;color:var(--heading);line-height:1;}
.metric-sub{font-size:10px;color:var(--muted);margin-top:3px;}
.signal-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:12px;margin-bottom:20px;}
.signal-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px;position:relative;overflow:hidden;transition:border-color .2s;}
.signal-card.gacor{border-color:rgba(0,255,136,.4);background:rgba(0,255,136,.03);}
.signal-card.potensial{border-color:rgba(255,183,0,.3);background:rgba(255,183,0,.03);}
.signal-card.watch{border-color:rgba(0,229,255,.2);}
.signal-card::after{content:'';position:absolute;top:0;left:0;width:4px;height:100%;}
.signal-card.gacor::after{background:var(--green);}
.signal-card.potensial::after{background:var(--amber);}
.signal-card.watch::after{background:var(--accent);}
.sc-ticker{font-family:'Space Mono',monospace;font-size:18px;font-weight:700;color:var(--heading);}
.sc-price{font-family:'Space Mono',monospace;font-size:13px;color:var(--muted);}
.sc-signal{font-size:13px;font-weight:700;margin:6px 0;}
.sc-bars{display:flex;gap:3px;margin:8px 0;}
.sc-bar{height:16px;border-radius:2px;}
.sc-bar.filled{background:var(--green);}
.sc-bar.empty{background:var(--border);}
.sc-stats{display:flex;gap:12px;flex-wrap:wrap;margin-top:8px;}
.sc-stat{font-family:'Space Mono',monospace;font-size:10px;color:var(--muted);}
.sc-stat span{color:var(--text);}
.alert-box{background:rgba(255,61,90,.06);border:1px solid rgba(255,61,90,.4);border-radius:8px;padding:14px 18px;margin-bottom:16px;animation:pulse-border 2s infinite;}
@keyframes pulse-border{0%,100%{border-color:rgba(255,61,90,.4);}50%{border-color:rgba(255,61,90,.9);}}
.alert-title{color:var(--red);font-family:'Space Mono',monospace;font-size:12px;font-weight:700;letter-spacing:2px;}
.tape-wrap{overflow:hidden;white-space:nowrap;border-top:1px solid var(--border);border-bottom:1px solid var(--border);padding:5px 0;margin-bottom:16px;background:var(--surface);}
.tape-inner{display:inline-block;animation:marquee 35s linear infinite;}
@keyframes marquee{0%{transform:translateX(0)}100%{transform:translateX(-50%)}}
.tape-item{display:inline-block;margin:0 18px;font-family:'Space Mono',monospace;font-size:10px;}
.tape-item.up{color:var(--green);}.tape-item.down{color:var(--red);}.tape-item.flat{color:var(--muted);}
::-webkit-scrollbar{width:4px;height:4px;}::-webkit-scrollbar-track{background:var(--bg);}::-webkit-scrollbar-thumb{background:var(--border);border-radius:2px;}
[data-testid="stNumberInput"] input{background:var(--surface)!important;border:1px solid var(--border)!important;color:var(--heading)!important;font-family:'Space Mono',monospace!important;border-radius:6px!important;}
button[data-testid="baseButton-primary"]{background:var(--orange)!important;color:var(--bg)!important;font-family:'Space Mono',monospace!important;font-weight:700!important;border:none!important;}
.section-title{font-family:'Space Mono',monospace;font-size:11px;color:var(--muted);letter-spacing:2px;text-transform:uppercase;border-left:3px solid var(--orange);padding-left:10px;margin:20px 0 10px 0;}
.bt-result{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:20px;margin-top:12px;}
.bt-metric{display:inline-block;margin-right:24px;margin-bottom:8px;}
.bt-metric-val{font-family:'Space Mono',monospace;font-size:22px;font-weight:700;}
.bt-metric-lbl{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:1px;}
</style>
""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════
#  STOCK LIST
# ════════════════════════════════════════════════════
raw_stocks = [
    "AALI","ACES","ACST","ADES","ADHI","ADMF","ADMG","ADMR","ADRO","AGII","AGRO","AGRS",
    "AKPI","AKRA","AKSI","ALDO","ALKA","ALMI","AMAG","AMAR","AMFG","AMIN","AMMN","AMMS",
    "AMOR","AMRT","ANDI","ANJT","ANTM","APLN","ARCI","ARNA","ARTO","ASDM","ASGR","ASII",
    "ASRI","ASRM","ASSA","AUTO","AVIA","AWAN","AXIO","BACA","BBCA","BBHI","BBKP","BBLD",
    "BBMD","BBNI","BBRI","BBRM","BBSI","BBSS","BBTN","BBYB","BCAP","BCIC","BCIP","BDMN",
    "BEST","BFIN","BIRD","BISI","BJBR","BJTM","BLTZ","BLUE","BMBL","BMRI","BMTR","BNGA",
    "BNII","BNLI","BRAM","BRIS","BRNA","BRPT","BSDE","BSSR","BTON","BTPS","BUDI","BUKA",
    "BULL","BUMI","BYAN","CAMP","CASH","CASS","CBRE","CEKA","CINT","CITA","CITY","CLEO",
    "CMRY","COCO","CPIN","CPRO","CSAP","CSIS","CTBN","CTRA","CUAN","DART","DCII","DGNS",
    "DIGI","DILD","DLTA","DNET","DOID","DPNS","DSSA","DUTI","DVLA","EKAD","ELPI","ELSA",
    "EMAS","EMTK","EPMT","ERAA","ESSA","EXCL","FAST","FASW","FISH","GDST","GEMA","GEMS",
    "GGRM","GGRP","GIAA","GJTL","GOLD","GOOD","GOTO","GPRA","HEAL","HERO","HEXA","HITS",
    "HMSP","HOKI","HRTA","HRUM","ICBP","IMAS","IMPC","INAF","INAI","INCO","INDF","INET",
    "INFO","INPP","INTA","INTP","IPCC","IPCM","ISAT","ISSP","ITMG","JECC","JIHD","JKON",
    "JPFA","JRPT","JSMR","KAEF","KBLI","KBLM","KDSI","KEJU","KIJA","KING","KINO","KKGI",
    "KLBF","LPCK","LPGI","LPIN","LPKR","LPPF","LSIP","LTLS","LUCK","MAIN","MAPI","MARI",
    "MARK","MASA","MAYA","MBAP","MBMA","MBSS","MBTO","MDKA","MDLN","MEDC","MEGA","MIDI",
    "MIKA","MKPI","MLBI","MLIA","MLPT","MNCN","MTDL","MTEL","MTLA","MYOH","MYOR","NELY",
    "NFCX","NOBU","NRCA","PANI","PANR","PANS","PEHA","PGAS","PGEO","PGUN","PICO","PJAA",
    "PLIN","PNLF","POLU","PORT","POWR","PRDA","PRIM","PSSI","PTBA","PTRO","PWON","RAJA",
    "RALS","RICY","RIGS","RISE","RODA","ROTI","SAFE","SAME","SCCO","SCMA","SDRA","SGRO",
    "SHIP","SILO","SIMP","SKBM","SMAR","SMCB","SMDR","SMGR","SMMA","SMRA","SMSM","SOHO",
    "SPMA","SPTO","SRIL","SRTG","SSIA","SSMS","STAA","STTP","SUNU","SUPR","TBIG","TBLA",
    "TCID","TCPI","TECH","TELE","TGKA","TINS","TKIM","TLKM","TMAS","TOBA","TOWR","TRGU",
    "TRIM","TRIS","TRST","TRUE","TRUK","TSPC","TUGU","UNIC","UNIT","UNTR","UNVR","VOKS",
    "WEGE","WEHA","WICO","WIFI","WIKA","WINE","WINS","WITA","WOOD","WSKT","WTON","ZINC",
]
seen = set(); raw_stocks = [x for x in raw_stocks if not (x in seen or seen.add(x))]
stocks_yf  = [s + ".JK" for s in raw_stocks]
stock_map  = {s + ".JK": s for s in raw_stocks}

# ════════════════════════════════════════════════════
#  MARKET REGIME
# ════════════════════════════════════════════════════
@st.cache_data(ttl=600)
def get_market_regime():
    try:
        df = yf.download("^JKSE", period="60d", interval="1d",
                         progress=False, auto_adjust=True, timeout=8)
        if df is None or len(df) < 10:
            return ("UNKNOWN", 0, 0, 0, "Data kurang", 0.0)
        close = df["Close"].squeeze()
        ema20 = float(close.ewm(span=20, adjust=False).mean().iloc[-1])
        ema55 = float(close.ewm(span=min(55, len(close)-1), adjust=False).mean().iloc[-1])
        price = float(close.iloc[-1])
        chg   = float(((close.iloc[-1] - close.iloc[-2]) / close.iloc[-2]) * 100)
        if price < ema20:
            return ("RED",      price, ema20, ema55, f"IHSG {price:,.0f} < EMA20 → Bearish", chg)
        elif price > ema20 and price > ema55:
            return ("GREEN",    price, ema20, ema55, f"IHSG {price:,.0f} > EMA20 & EMA55 → Bullish", chg)
        else:
            return ("SIDEWAYS", price, ema20, ema55, f"IHSG {price:,.0f} antara EMA20-EMA55", chg)
    except:
        return ("UNKNOWN", 0, 0, 0, "IHSG tidak tersedia", 0.0)

def get_regime_config(regime):
    return {
        "RED":      {"mode":"Reversal 🎯","min_score":5,"min_rvol":2.0,"sl_mult":0.6,
                     "label":"🔴 MARKET MERAH — Reversal Only, Score ≥ 5","color":"#ff3d5a",
                     "desc":"Market bearish. Fokus reversal oversold, filter ketat."},
        "GREEN":    {"mode":"Scalping ⚡","min_score":4,"min_rvol":1.5,"sl_mult":0.8,
                     "label":"🟢 MARKET HIJAU — Scalping & Momentum, Score ≥ 4","color":"#00ff88",
                     "desc":"Market bullish. Scalping & Momentum optimal."},
        "SIDEWAYS": {"mode":"Scalping ⚡","min_score":4,"min_rvol":2.0,"sl_mult":0.7,
                     "label":"🟡 MARKET SIDEWAYS — Semua Mode, RVOL ≥ 2x","color":"#ffb700",
                     "desc":"Market sideways. RVOL harus lebih kuat."},
        "UNKNOWN":  {"mode":"Scalping ⚡","min_score":4,"min_rvol":1.5,"sl_mult":0.8,
                     "label":"⚪ REGIME UNKNOWN — Manual Mode","color":"#4a5568",
                     "desc":"Tidak bisa deteksi kondisi market."},
    }.get(regime, {"mode":"Scalping ⚡","min_score":4,"min_rvol":1.5,"sl_mult":0.8,
                   "label":"⚪ UNKNOWN","color":"#4a5568","desc":""})

# ════════════════════════════════════════════════════
#  INDICATORS
# ════════════════════════════════════════════════════
def ema(s, n): return s.ewm(span=n, adjust=False).mean()

def apply_intraday_indicators(df):
    if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.droplevel(1)
    c = df['Close']

    df['EMA9']  = ema(c,9);  df['EMA21'] = ema(c,21)
    df['EMA50'] = ema(c,50); df['EMA200']= ema(c,200)

    d  = c.diff(); g = d.clip(lower=0).ewm(span=14,adjust=False).mean()
    l  = (-d.clip(upper=0)).ewm(span=14,adjust=False).mean()
    rsi_raw = (100 - 100/(1+g/l.replace(0,np.nan))).fillna(50)
    df['RSI']     = rsi_raw
    df['RSI_EMA'] = rsi_raw.ewm(span=14,adjust=False).mean()
    d5 = c.diff(); g5 = d5.clip(lower=0).ewm(span=5,adjust=False).mean()
    l5 = (-d5.clip(upper=0)).ewm(span=5,adjust=False).mean()
    df['RSI5'] = (100-100/(1+g5/l5.replace(0,np.nan))).fillna(50)

    lo10 = df['Low'].rolling(10).min(); hi10 = df['High'].rolling(10).max()
    raw_k = (100*(c-lo10)/(hi10-lo10).replace(0,np.nan)).fillna(50)
    df['STOCH_K'] = raw_k.ewm(span=5,adjust=False).mean()
    df['STOCH_D'] = df['STOCH_K'].ewm(span=5,adjust=False).mean()
    df['STOCH_CROSS_UP']   = (df['STOCH_K']>df['STOCH_D']) & (df['STOCH_K'].shift(1)<=df['STOCH_D'].shift(1))
    df['STOCH_CROSS_DOWN'] = (df['STOCH_K']<df['STOCH_D']) & (df['STOCH_K'].shift(1)>=df['STOCH_D'].shift(1))

    ema12 = c.ewm(span=12,adjust=False).mean(); ema26 = c.ewm(span=26,adjust=False).mean()
    macd_line = ema12-ema26; signal_line = macd_line.ewm(span=9,adjust=False).mean()
    df['MACD']           = macd_line
    df['MACD_Sig']       = signal_line
    df['MACD_Hist']      = (macd_line-signal_line).fillna(0)
    df['MACD_CROSS_UP']  = (macd_line>signal_line)&(macd_line.shift(1)<=signal_line.shift(1))
    df['MACD_CROSS_DOWN']= (macd_line<signal_line)&(macd_line.shift(1)>=signal_line.shift(1))

    try:
        tp = (df['High']+df['Low']+df['Close'])/3
        df['VWAP'] = (tp*df['Volume']).cumsum()/df['Volume'].cumsum()
    except: df['VWAP'] = c

    df['BB_mid']  = c.rolling(20).mean(); df['BB_std'] = c.rolling(20).std()
    df['BB_upper']= df['BB_mid']+2*df['BB_std']; df['BB_lower']= df['BB_mid']-2*df['BB_std']
    df['BB_pct']  = (c-df['BB_lower'])/(df['BB_upper']-df['BB_lower'])

    df['AvgVol'] = df['Volume'].rolling(20).mean()
    df['RVOL']   = df['Volume']/df['AvgVol'].replace(0,np.nan)

    tr = pd.concat([df['High']-df['Low'],
                    (df['High']-c.shift()).abs(),
                    (df['Low'] -c.shift()).abs()],axis=1).max(axis=1)
    df['ATR'] = tr.rolling(14).mean()

    body_top = df[['Close','Open']].max(axis=1); body_bot = df[['Close','Open']].min(axis=1)
    hl = (df['High']-df['Low']).replace(0,np.nan)
    df['LWick']    = ((body_bot-df['Low'])/hl*100).fillna(0)
    df['UWick']    = ((df['High']-body_top)/hl*100).fillna(0)
    df['Body']     = (body_top-body_bot)/hl*100
    df['BodyRatio']= (body_top-body_bot)/hl.fillna(0)
    df['BullBar']  = (df['Close']>df['Open'])&(df['BodyRatio']>0.5)

    df['NetVol']  = np.where(c>=df['Open'],df['Volume'],-df['Volume'])
    df['NetVol3'] = pd.Series(df['NetVol'],index=df.index).rolling(3).sum()
    df['NetVol8'] = pd.Series(df['NetVol'],index=df.index).rolling(8).sum()
    df['VolSpike']= df['RVOL']>2.5
    df['ROC3']    = c.pct_change(3); df['ROC8'] = c.pct_change(8)
    df['HH'] = df['High']>df['High'].shift(1); df['HL'] = df['Low']>df['Low'].shift(1)
    df['LL'] = df['Low']<df['Low'].shift(1);   df['LH'] = df['High']<df['High'].shift(1)

    if 'FBuy' in df.columns and 'FSell' in df.columns:
        df['FNet']   = df['FBuy']-df['FSell']
        df['FCum']   = df['FNet'].cumsum()
        df['FNet3']  = df['FNet'].rolling(3).sum()
        df['FNet8']  = df['FNet'].rolling(8).sum()
        tot = df['FBuy']+df['FSell']
        df['FRatio'] = (df['FBuy']/tot.replace(0,np.nan)).fillna(0.5)
    return df

# ════════════════════════════════════════════════════
#  SCORING
# ════════════════════════════════════════════════════
def score_scalping(r, p, p2):
    score=0; reasons=[]
    if r['EMA9']>r['EMA21']>r['EMA50']:  score+=1.5; reasons.append("EMA stack ▲")
    elif r['EMA9']>r['EMA21']:            score+=0.8; reasons.append("EMA9>21")
    if r['Close']>r['VWAP']:             score+=1;   reasons.append("Above VWAP")
    if r['MACD_Hist']>0 and r['MACD_Hist']>float(p['MACD_Hist']):
        score+=1.5; reasons.append("MACD hist expanding ✦")
        if p2 is not None and float(p['MACD_Hist'])>float(p2['MACD_Hist']): score+=0.3
    elif r['MACD_Hist']>0: score+=0.5; reasons.append("MACD hist +")
    rsi_e=float(r['RSI_EMA'])
    if 52<rsi_e<68:  score+=0.8; reasons.append(f"RSI-EMA={rsi_e:.1f}")
    elif rsi_e>=68:  score-=0.5
    rvol=float(r['RVOL'])
    if rvol>2.0:   score+=1;   reasons.append(f"RVOL={rvol:.1f}x surge")
    elif rvol>1.5: score+=0.6; reasons.append(f"RVOL={rvol:.1f}x")
    if bool(r['BullBar']):    score+=0.5; reasons.append("Bullish bar")
    if float(r['NetVol3'])>0: score+=0.4; reasons.append("Net vol +")
    if r['Close']<r['EMA200']*0.98: score-=0.5
    return max(0,min(6,round(score,1))), reasons, {}

def score_momentum(r, p, p2):
    score=0; reasons=[]
    hh=bool(r['HH']); hl=bool(r['HL'])
    if hh and hl:  score+=1.5; reasons.append("HH+HL pattern ▲")
    elif hh:       score+=0.8
    rvol=float(r['RVOL'])
    if rvol>3.0:   score+=1.5; reasons.append(f"RVOL={rvol:.1f}x SURGE 🔥")
    elif rvol>2.0: score+=1.0; reasons.append(f"RVOL={rvol:.1f}x")
    elif rvol>1.5: score+=0.5
    roc=float(r['ROC3'])*100
    if roc>2.0:   score+=1.5; reasons.append(f"ROC3={roc:.1f}%")
    elif roc>1.0: score+=0.8; reasons.append(f"ROC3={roc:.1f}%")
    elif roc<0:   score-=0.5
    rsi_e=float(r['RSI_EMA'])
    if 55<rsi_e<75: score+=0.8; reasons.append(f"RSI-EMA={rsi_e:.1f}")
    if rsi_e>78:    score-=0.8; reasons.append("⚠️ RSI overbought")
    sk=float(r['STOCH_K']); sd=float(r['STOCH_D'])
    if sk>60 and sk>sd: score+=0.8; reasons.append("STOCH K>D bullish")
    if r['MACD_Hist']>0 and r['MACD_Hist']>float(p['MACD_Hist']): score+=0.8; reasons.append("MACD expanding")
    if r['Close']>r['VWAP']: score+=0.5; reasons.append("Above VWAP")
    return max(0,min(6,round(score,1))), reasons, {}

def score_reversal(r, p, p2):
    score=0; reasons=[]; os_count=0
    rsi_e=float(r['RSI_EMA'])
    if rsi_e<30:   os_count+=1; score+=1.5; reasons.append(f"RSI-EMA={rsi_e:.1f} OS extreme")
    elif rsi_e<40: os_count+=1; score+=0.8; reasons.append(f"RSI-EMA={rsi_e:.1f} OS")
    sk=float(r['STOCH_K']); sd=float(r['STOCH_D'])
    if sk<20:   os_count+=1; score+=1;   reasons.append(f"STOCH={sk:.0f} extreme OS")
    elif sk<30: os_count+=1; score+=0.5
    bp=float(r['BB_pct'])
    if bp<0.05:   os_count+=1; score+=1;   reasons.append("BB lower touch")
    elif bp<0.15: os_count+=1; score+=0.5
    if os_count<1.5: return 0,[],{}
    rev=0
    pk=float(p['STOCH_K']); pd_=float(p['STOCH_D'])
    if sk<30 and sk>sd and pk<=pd_:   rev+=1; score+=2;   reasons.append("STOCH %K cross ↑ OS ✦✦")
    elif sk<25 and sk>sd:             rev+=1; score+=1.2; reasons.append("STOCH K>D extreme OS")
    if p is not None:
        rsi_p=float(p['RSI_EMA'])
        if rsi_e>rsi_p and rsi_e<42: rev+=1; score+=1.2; reasons.append("RSI-EMA pivot ↑")
    mh=float(r['MACD_Hist']); mh_p=float(p['MACD_Hist'])
    if mh>mh_p and mh<0: rev+=1; score+=0.8; reasons.append("MACD hist diverge ↑")
    if rev==0: score*=0.3
    if bool(r['VolSpike']) and float(r['Close'])<float(r['Open']): score+=0.8; reasons.append("Volume climax sell")
    elif float(r['RVOL'])>1.5: score+=0.4
    if float(r['NetVol3'])>0: score+=0.5; reasons.append("Net vol turning +")
    if float(r['BodyRatio'])>0.75 and float(r['Close'])<float(r['Open']): score-=0.8; reasons.append("⚠️ Bearish bar kuat")
    return max(0,min(6,round(score,1))), reasons, {}

def get_signal(score, mode):
    t = {"Scalping ⚡":{5:"GACOR ⚡",4:"POTENSIAL 🔥",3:"WATCH 👀"},
         "Momentum 🚀":{5:"GACOR 🚀",4:"POTENSIAL 🔥",3:"WATCH 👀"},
         "Reversal 🎯":{5:"REVERSAL 🎯",4:"POTENSIAL 🔥",3:"WATCH 👀"}}.get(mode,{})
    for thresh in sorted(t.keys(), reverse=True):
        if score >= thresh: return t[thresh]
    return "WAIT"

def get_card_class(signal):
    if "GACOR" in signal or "REVERSAL" in signal: return "gacor"
    if "POTENSIAL" in signal: return "potensial"
    if "WATCH" in signal: return "watch"
    return ""

def get_sinyal_v2(r, p, p2):
    def sf(v,d=0.):
        try: x=float(v); return d if(np.isnan(x) or np.isinf(x)) else x
        except: return d
    cl     = sf(r.get('Close',0))
    e9     = sf(r.get('EMA9'));   e21=sf(r.get('EMA21')); e50=sf(r.get('EMA50'))
    rsi_ema= sf(r.get('RSI_EMA',50)); rsi_ema_p=sf(p.get('RSI_EMA',50))
    sk     = sf(r.get('STOCH_K',50)); sd=sf(r.get('STOCH_D',50))
    sk_p   = sf(p.get('STOCH_K',50)); sd_p=sf(p.get('STOCH_D',50))
    mh     = sf(r.get('MACD_Hist',0)); mh_p=sf(p.get('MACD_Hist',0))
    macd_v = sf(r.get('MACD',0));   sig_v=sf(r.get('MACD_Sig',0))
    macd_p = sf(p.get('MACD',0));   sig_p=sf(p.get('MACD_Sig',0))
    rv     = sf(r.get('RVOL',1))
    lw     = sf(r.get('LWick',0)); uw=sf(r.get('UWick',0))
    body   = sf(r.get('Body',50)); vwap_v=sf(r.get('VWAP',cl))
    score=0; flags=[]

    ema_bull=e9>e21>e50; ema_gc=e9>e21; ema_bear=e9<e21<e50
    p_e9=sf(p.get('EMA9')); p_e21=sf(p.get('EMA21'))
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
    if rsi_os2:  score+=12; flags.append(f"RSI {rsi_ema:.0f} OS")
    elif rsi_os: score+=7;  flags.append(f"RSI {rsi_ema:.0f}")
    elif 45<rsi_ema<65: score+=5
    elif rsi_ob: score-=8
    if rsi_cu:   score+=6; flags.append("RSI ↑")

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

    fnet3=sf(r.get('FNet3',0)); fnet8=sf(r.get('FNet8',0)); fratio=sf(r.get('FRatio',0.5))
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

    if is_jual:    return "JUAL ⬇️",    score, " · ".join(flags[:3]), gc_now
    if is_bandar:  return "BANDAR 🔵",  score, " · ".join(flags[:3]), gc_now
    if is_haka:    return "HAKA 🔨",    score, " · ".join(flags[:3]), gc_now
    if is_super:   return "SUPER 🔥",   score, " · ".join(flags[:3]), gc_now
    if is_rebound: return "REBOUND 🏀", score, " · ".join(flags[:3]), gc_now
    if entry_mod and score>=20: return "AKUM 📦", score, " · ".join(flags[:3]), gc_now
    if score>=15: return "ON TRACK ✅", score, " · ".join(flags[:3]), gc_now
    return "WAIT ❌", score, " · ".join(flags[:3]), gc_now

def get_aksi_v2(sinyal, gc_now, score):
    if "BANDAR" in sinyal or (("HAKA" in sinyal or "SUPER" in sinyal) and score>=35):
        return "AT ENTRY 🎯"
    elif "REBOUND" in sinyal: return "WATCH REB 🏀"
    elif gc_now:              return "GC NOW ⚡"
    elif score>=25:           return "AT ENTRY 🎯"
    elif score>=15:           return "WAIT GC ⏳"
    else:                     return "WAIT ❌"

# ════════════════════════════════════════════════════
#  FETCH INTRADAY — CONTROLLED PARALLEL (10 THREADS)
#
#  Sebelumnya stuck karena @st.cache_data dipanggil dari thread.
#  Sekarang cache sudah disk pickle (thread-safe) → parallel AMAN.
#
#  DS rate limit: 1000 req/menit = ~16 req/detik
#  10 threads × 1.5s/req = ~6.7 req/detik → aman!
#  778 tickers ÷ 10 threads = ~2 menit (vs 20 menit sequential)
# ════════════════════════════════════════════════════
def fetch_intraday(tickers, interval="15m", force_fresh=False):
    all_dfs = {}
    ticker_list = list(tickers)

    # Step 1: ambil dari cache dulu — instant
    need_fetch = []
    for t in ticker_list:
        raw_t = t.replace(".JK","").upper()
        if not force_fresh:
            cached = _cache_get(raw_t, interval)
            if cached is not None:
                all_dfs[t] = cached
                continue
        need_fetch.append(t)

    if not need_fetch:
        return all_dfs

    # Step 2: fetch yang belum cache — 10 threads, thread-safe
    def _fetch_one(t):
        raw_t = t.replace(".JK","").upper()
        df = fetch_ds_ohlcv(raw_t, interval, 200, True)
        return t, df

    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(_fetch_one, t): t for t in need_fetch}
        for f in as_completed(futs):
            try:
                t, df = f.result(timeout=15)
                if df is not None and len(df) >= 20:
                    all_dfs[t] = df
            except: pass

    return all_dfs

@st.cache_data(ttl=3600)
def fetch_pivot_data(ticker_yf):
    try:
        df = yf.download(ticker_yf, period="5d", interval="1d",
                         progress=False, auto_adjust=True, threads=False)
        if df is None or len(df) < 2: return None
        if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.droplevel(1)
        prev = df.iloc[-2]
        pp=(float(prev["High"])+float(prev["Low"])+float(prev["Close"]))/3
        r1=2*pp-float(prev["Low"]); r2=pp+(float(prev["High"])-float(prev["Low"]))
        s1=2*pp-float(prev["High"]); s2=pp-(float(prev["High"])-float(prev["Low"]))
        return {"PP":pp,"R1":r1,"R2":r2,"S1":s1,"S2":s2}
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

# ════════════════════════════════════════════════════
#  TELEGRAM
# ════════════════════════════════════════════════════
def send_telegram(results_top, source="Scanner"):
    if not TOKEN or not CHAT_ID: return
    now = datetime.now(jakarta_tz); is_open = 9<=now.hour<16
    sep = "━"*28
    hdr = (f"{'🔴 MARKET OPEN' if is_open else '🌙 AFTER HOURS'}\n"
           f"🔥 *THETA TURBO {'WATCHLIST' if source=='Watchlist' else 'ALERT'}*\n"
           f"⏰ `{now.strftime('%H:%M:%S')} WIB` · `{now.strftime('%d %b %Y')}`\n{sep}\n")
    body = ""
    for r in results_top[:5]:
        sig  = r.get('Signal','-')
        em   = "🏆" if ("GACOR" in sig or "REVERSAL" in sig) else ("🔥" if "POTENSIAL" in sig else "👀")
        te   = "📈" if "▲" in r.get('Trend','') else ("📉" if "▼" in r.get('Trend','') else "➡️")
        body += (f"\n{em} *{r['Ticker']}*  `{sig}`\n"
                 f"   💰 Price: `{r['Price']:,}` {te}\n"
                 f"   📈 RSI-EMA: `{r.get('RSI-EMA',0)}` | RVOL: `{r.get('RVOL',0)}x`\n"
                 f"   🎯 TP: `{r['TP']:,}` | 🛑 SL: `{r['SL']:,}`\n"
                 f"   💡 _{r.get('Reasons','')[:60]}_\n")
    footer = f"\n{sep}\n⚡ _Theta Turbo v5 · 15M_\n⚠️ _BUKAN saran investasi!_"
    try:
        requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                      data={"chat_id":CHAT_ID,"text":hdr+body+footer,"parse_mode":"Markdown"},
                      timeout=10)
    except: pass

# ════════════════════════════════════════════════════
#  SEKTOR
# ════════════════════════════════════════════════════
SECTORS = {
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
HORMUZ_SECTORS = ["Energi & Mining","Shipping & Logistik","Petrokimia & Kimia"]

@st.cache_data(ttl=300)
def fetch_sector_rotation(sector_stocks):
    results = []
    tickers_yf = [s+".JK" for s in sector_stocks[:10]]
    try:
        raw = yf.download(tickers_yf, period="3d", interval="1d",
                          group_by="ticker", progress=False, threads=True, auto_adjust=True)
        for t in tickers_yf:
            tkr = t.replace(".JK","")
            try:
                df = raw[t].dropna() if len(tickers_yf)>1 else raw.dropna()
                if isinstance(df.columns,pd.MultiIndex): df.columns=df.columns.droplevel(1)
                if len(df)<2: continue
                close=float(df["Close"].iloc[-1]); prev=float(df["Close"].iloc[-2])
                chg=(close-prev)/prev*100; vol=float(df["Volume"].iloc[-1])
                avg_v=float(df["Volume"].mean()); rvol=vol/avg_v if avg_v>0 else 1.0
                results.append({"ticker":tkr,"close":close,"chg":chg,"rvol":round(rvol,2)})
            except: pass
    except: pass
    return results

def calc_trailing_stop(entry, current, atr, method="ATR", atr_mult=2.0, pct=3.0):
    if method=="ATR":    trail_dist=atr*atr_mult; stop_price=current-trail_dist
    elif method=="Persen": trail_dist=current*(pct/100); stop_price=current*(1-pct/100)
    else:                trail_dist=atr*1.5; stop_price=current-trail_dist
    profit_pct=(current-entry)/entry*100
    locked_pct=(stop_price-entry)/entry*100 if stop_price>entry else 0
    return {"stop":round(stop_price,0),"distance":round(trail_dist,0),
            "profit_float":round(profit_pct,2),"profit_locked":round(locked_pct,2),
            "is_profitable":stop_price>entry}

def score_bsjp(r, p, p2):
    score=0; reasons=[]
    body=float(r["Close"])-float(r["Open"]); hi_lo=float(r["High"])-float(r["Low"])
    close_pct=(float(r["Close"])-float(r["Low"]))/max(hi_lo,1)
    if close_pct>0.7:  score+=2;   reasons.append(f"Tutup dekat High ({close_pct:.0%})")
    elif close_pct>0.5:score+=1;   reasons.append(f"Tutup kuat ({close_pct:.0%})")
    rvol=float(r["RVOL"])
    if rvol>3.0:   score+=2;   reasons.append(f"RVOL={rvol:.1f}x SURGE 🔥")
    elif rvol>2.0: score+=1.5; reasons.append(f"RVOL={rvol:.1f}x kuat")
    elif rvol>1.5: score+=0.8; reasons.append(f"RVOL={rvol:.1f}x")
    if r["EMA9"]>r["EMA21"]>r["EMA50"]:  score+=1.5; reasons.append("EMA stack ▲")
    elif r["EMA9"]>r["EMA21"]:            score+=0.8; reasons.append("EMA9>21")
    rsi_e=float(r["RSI_EMA"])
    if 45<rsi_e<70:  score+=1;   reasons.append(f"RSI-EMA={rsi_e:.1f} ✓")
    elif rsi_e>=70:  score-=1;   reasons.append(f"RSI-EMA={rsi_e:.1f} OB ⚠️")
    elif rsi_e<40:   score+=0.5; reasons.append(f"RSI-EMA={rsi_e:.1f} oversold")
    if float(r["MACD_Hist"])>0 and float(r["MACD_Hist"])>float(p["MACD_Hist"]):
        score+=1; reasons.append("MACD hist expanding ✦")
    elif float(r["MACD_Hist"])>0: score+=0.5; reasons.append("MACD +")
    if float(r["Close"])>float(r["VWAP"]): score+=0.5; reasons.append("Above VWAP")
    return max(0,min(6,round(score,1))), reasons, {}

@st.cache_data(ttl=300)
def scan_gap_up(tickers_yf, min_gap_pct=0.5):
    results = []
    for i in range(0, len(tickers_yf), 30):
        batch = tickers_yf[i:i+30]
        try:
            raw = yf.download(batch, period="5d", interval="1d",
                              group_by="ticker", progress=False, threads=True, auto_adjust=True)
            for t in batch:
                tkr = t.replace(".JK","")
                try:
                    if len(batch)>1: df=raw[t].dropna()
                    else:
                        df=raw.copy()
                        if isinstance(df.columns,pd.MultiIndex): df.columns=df.columns.droplevel(1)
                        df=df.dropna()
                    if len(df)<3: continue
                    today=df.iloc[-1]; prev=df.iloc[-2]
                    close=float(today["Close"]); high_t=float(today["High"]); low_t=float(today["Low"])
                    high_p=float(prev["High"]); vol=float(today["Volume"])
                    avg_vol=float(df["Volume"].mean()); rvol=vol/avg_vol if avg_vol>0 else 1.0
                    gap_score=0; reasons=[]
                    if close>high_p:
                        gap_pct=(close-high_p)/high_p*100; gap_score+=3
                        reasons.append(f"Gap {gap_pct:.1f}% above prev High ✦✦")
                    close_ratio=(close-low_t)/max(high_t-low_t,1)
                    if close_ratio>0.85: gap_score+=2; reasons.append(f"Tutup dekat High {close_ratio:.0%}")
                    elif close_ratio>0.70: gap_score+=1; reasons.append(f"Tutup kuat {close_ratio:.0%}")
                    if rvol>3.0: gap_score+=2; reasons.append(f"RVOL={rvol:.1f}x SURGE 🔥")
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
                except: pass
        except: pass
        time.sleep(0.2)
    return sorted(results, key=lambda x: x["Gap Score"], reverse=True)

# ════════════════════════════════════════════════════
#  HEADER
# ════════════════════════════════════════════════════
regime, ihsg_price, ema20, ema55, regime_detail, ihsg_chg = get_market_regime()
rcfg   = get_regime_config(regime)
rcolor = rcfg["color"]
chg_col= "#00ff88" if ihsg_chg>=0 else "#ff3d5a"
chg_sym= "▲" if ihsg_chg>=0 else "▼"
now_jkt= datetime.now(jakarta_tz)
is_open= 9<=now_jkt.hour<16

st.markdown(f"""
<div class="tt-header">
  <div>
    <div class="tt-logo">🔥 THETA TURBO</div>
    <div class="tt-sub">Intraday 15M Scanner · Auto Regime · v5.1 DS</div>
  </div>
  <div class="live-badge"><div class="live-dot"></div>
    {'⚡ DataSectors' if DS_KEY else '📊 yFinance'} · LIVE {now_jkt.strftime("%H:%M:%S")} WIB
  </div>
</div>""", unsafe_allow_html=True)

st.markdown(f"""
<div style="background:rgba(0,0,0,.4);border:1px solid {rcolor}44;border-radius:8px;
     padding:12px 16px;margin-bottom:14px;border-left:4px solid {rcolor};">
  <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;">
    <div>
      <div style="font-family:Space Mono,monospace;font-size:12px;font-weight:700;color:{rcolor};letter-spacing:1px;">{rcfg["label"]}</div>
      <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-top:3px;">{rcfg["desc"]}</div>
    </div>
    <div style="text-align:right;font-family:Space Mono,monospace;">
      <div style="font-size:18px;font-weight:700;color:{rcolor};">{ihsg_price:,.0f} <span style="font-size:11px;color:{chg_col}">{chg_sym}{abs(ihsg_chg):.2f}%</span></div>
      <div style="font-size:9px;color:#4a5568;">EMA20 {ema20:,.0f} · EMA55 {ema55:,.0f}</div>
    </div>
  </div>
</div>""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════
#  TABS
# ════════════════════════════════════════════════════
tab_scanner, tab_watchlist, tab_bsjp, tab_sector, tab_gapup, tab_trail, tab_backtest = st.tabs(
    ["🔥 Scanner","👁️ Watchlist","🌙 BSJP","🏭 Sektor","📈 Gap Up","🎯 Trailing Stop","📊 Backtest"])

# ════════════════════════════════════════════════════
#  TAB 1: SCANNER
# ════════════════════════════════════════════════════
with tab_scanner:
    with st.expander("⚙️  Scanner Settings", expanded=False):
        sc1, sc2, sc3 = st.columns(3)
        with sc1:
            st.markdown('<div class="settings-label">MODE SIGNAL</div>', unsafe_allow_html=True)
            auto_regime = st.toggle("🤖 Auto-Mode (Market Regime)", value=True, key="auto_reg")
            if auto_regime:
                scan_mode = rcfg["mode"]
                st.markdown(f'<div style="font-family:Space Mono,monospace;font-size:10px;padding:6px 10px;background:rgba(0,0,0,.3);border-radius:4px;color:{rcolor};">Auto: {scan_mode}</div>', unsafe_allow_html=True)
            else:
                scan_mode = st.radio("Mode", ["Scalping ⚡","Momentum 🚀","Reversal 🎯"], label_visibility="collapsed", key="scan_mode_radio")
            tele_on = st.toggle("📡 Telegram Alert", value=True, key="tele_on")
        with sc2:
            st.markdown('<div class="settings-label">FILTER</div>', unsafe_allow_html=True)
            auto_thresh = st.toggle("🤖 Auto-Threshold", value=True, key="auto_thr")
            if auto_thresh:
                min_score=rcfg["min_score"]; vol_thresh=rcfg["min_rvol"]
                st.caption(f"Auto: Score≥{min_score} · RVOL≥{vol_thresh}x")
            else:
                min_score  = st.slider("Min Score (0-6)", 0, 6, 4, key="msc")
                vol_thresh = st.slider("Min RVOL Spike", 1.0, 5.0, 1.5, 0.1, key="vol")
            min_turn = st.number_input("Min Turnover (M Rp)", value=500, step=100, key="trn") * 1_000_000
        with sc3:
            st.markdown('<div class="settings-label">TAMPILAN</div>', unsafe_allow_html=True)
            view_mode  = st.radio("View", ["Card View 🃏","Table View 📊"], label_visibility="collapsed", key="view_mode")
            quick_mode = st.toggle("⚡ Quick (200 saham)", value=False, key="quick_mode")
            force_fresh= st.toggle("🔄 Fresh Data", value=False, key="force_fresh",
                                   help="Skip cache, paksa fetch ulang dari API")
            st.caption(f"🎯 Regime: {regime} · Mode: {scan_mode}")

    # Scan button — INLINE, no st.rerun defer
    do_scan_btn = st.button("🔥 MULAI SCAN SEKARANG", type="primary", use_container_width=True, key="btn_scan")

    # Auto-refresh check (no sleep!)
    _now_check = now_jkt.timestamp()
    auto_trigger = False
    if st.session_state.last_scan_time and not do_scan_btn:
        _elapsed = _now_check - st.session_state.last_scan_time
        if _elapsed >= 300 and st.session_state.scan_results and is_open:
            auto_trigger = True

    if do_scan_btn or auto_trigger:
        scan_list = stocks_yf[:200] if quick_mode else stocks_yf
        n_scan    = len(scan_list)
        prog_ph   = st.empty()
        pb        = st.progress(0)

        # ── PHASE 1: Fetch data ──
        prog_ph.markdown(
            f'<div style="color:#ff7b00;font-family:Space Mono,monospace;font-size:12px;">'
            f'⚡ Phase 1/2: Fetching {n_scan} saham (10 threads parallel)...</div>',
            unsafe_allow_html=True)
        pb.progress(0.1)

        data_dict = {}
        try:
            ticker_list = list(scan_list)
            # Cache check dulu
            need_fetch = []
            for t in ticker_list:
                raw_t = t.replace(".JK","").upper()
                if not force_fresh:
                    cached = _cache_get(raw_t, "15m")
                    if cached is not None:
                        data_dict[t] = cached
                        continue
                need_fetch.append(t)

            n_cached = len(data_dict)
            n_need   = len(need_fetch)
            prog_ph.markdown(
                f'<div style="color:#ff7b00;font-family:Space Mono,monospace;font-size:11px;">'
                f'⚡ {n_cached} dari cache · {n_need} perlu fetch · 10 threads...</div>',
                unsafe_allow_html=True)

            # Parallel fetch 10 threads (thread-safe karena disk cache)
            def _f(t):
                raw_t = t.replace(".JK","").upper()
                df = fetch_ds_ohlcv(raw_t, "15m", 200, True)
                return t, df

            done_count = [0]
            with ThreadPoolExecutor(max_workers=10) as ex:
                futs = {ex.submit(_f, t): t for t in need_fetch}
                for fut in as_completed(futs):
                    try:
                        t, df = fut.result(timeout=15)
                        done_count[0] += 1
                        if df is not None and len(df) >= 20:
                            data_dict[t] = df
                        # Update progress setiap 20 ticker
                        if done_count[0] % 20 == 0:
                            pct = 0.1 + (done_count[0] / max(n_need, 1)) * 0.70
                            pb.progress(min(pct, 0.80))
                            prog_ph.markdown(
                                f'<div style="color:#ff7b00;font-family:Space Mono,monospace;font-size:11px;">'
                                f'⚡ Fetched {done_count[0]}/{n_need} · {len(data_dict)} berhasil...</div>',
                                unsafe_allow_html=True)
                    except: done_count[0] += 1

            st.session_state.data_dict = data_dict

            # ── Fetch daily data untuk GAIN + VAL yang akurat ──
            prog_ph.markdown(
                f'<div style="color:#00e5ff;font-family:Space Mono,monospace;font-size:11px;">'
                f'📅 Fetching daily context untuk Gain & Val...</div>',
                unsafe_allow_html=True)
            daily_dict = {}
            need_daily = [t for t in ticker_list]
            def _fd(t):
                raw_t = t.replace(".JK","").upper()
                # Try cache first
                cached = _cache_get(raw_t, "daily")
                if cached is not None: return t, cached
                df = fetch_ds_ohlcv(raw_t, "daily", 100, False)
                return t, df
            with ThreadPoolExecutor(max_workers=10) as ex:
                futs = {ex.submit(_fd, t): t for t in need_daily}
                for f in as_completed(futs):
                    try:
                        t, df = f.result(timeout=15)
                        if df is not None and len(df) >= 2:
                            daily_dict[t] = df
                    except: pass
            st.session_state.daily_dict = daily_dict

            # ── PHASE 2: Process ──
            pb.progress(0.85)
            prog_ph.markdown(
                f'<div style="color:#00ff88;font-family:Space Mono,monospace;font-size:11px;">'
                f'⚙️ Phase 2/2: Processing {len(data_dict)} saham...</div>',
                unsafe_allow_html=True)

            results = []; tickers = list(data_dict.keys())
            # Fetch daily data dict untuk gain + val yang akurat
            daily_dict = st.session_state.get("daily_dict", {})

            for i, ticker_yf in enumerate(tickers):
                pb.progress(0.85 + (i+1)/max(len(tickers),1)*0.14)
                try:
                    df = data_dict[ticker_yf].copy()
                    if len(df)<55: continue
                    df = apply_intraday_indicators(df)
                    r=df.iloc[-1]; p=df.iloc[-2]; p2=df.iloc[-3] if len(df)>=3 else p
                    close=float(r['Close']); vol=float(r['Volume'])

                    # FIX GAIN: pakai daily D1 kalau ada, bukan 15m ROC
                    ticker_raw = ticker_yf.replace(".JK","").upper()
                    df_d = daily_dict.get(ticker_yf) or daily_dict.get(ticker_raw)
                    if df_d is not None and len(df_d) >= 2:
                        c1 = float(df_d.iloc[-1]['Close'])
                        c0 = float(df_d.iloc[-2]['Close'])
                        gain_pct = (c1 - c0) / max(c0, 1) * 100
                        # FIX VAL: pakai volume harian
                        daily_vol = float(df_d.iloc[-1]['Volume'])
                        turnover = c1 * daily_vol
                    else:
                        # Fallback: sum volume 15m hari ini
                        try:
                            today = df.index[-1].date()
                            df_today = df[df.index.date == today]
                            turnover = close * df_today['Volume'].sum()
                            gain_pct = float(r.get('ROC3', 0)) * 100
                        except:
                            turnover = close * vol
                            gain_pct = float(r.get('ROC3', 0)) * 100

                    rvol=float(r['RVOL'])
                    if turnover < min_turn or rvol < vol_thresh: continue

                    sig,sc_v2,flags_v2,gc_now = get_sinyal_v2(r,p,p2)
                    aksi_v2 = get_aksi_v2(sig, gc_now, sc_v2)
                    reasons = flags_v2.split(" · ") if flags_v2 else []
                    sc = round(min(6,max(0,sc_v2/10)),1)
                    if "WAIT" in sig: continue
                    if sc_v2<10: continue

                    atr  = float(r['ATR']) if not np.isnan(float(r['ATR'])) else close*0.01
                    tp   = close+4.0*atr; sl=close-2.0*atr
                    rr   = (tp-close)/max(close-sl,0.01)
                    e9=float(r['EMA9']); e21=float(r['EMA21']); e50=float(r['EMA50'])
                    trend = "▲ UP" if e9>e21>e50 else("▼ DOWN" if e9<e21<e50 else"◆ SIDE")

                    # FIX ASING: cek FBuy+FSell > 0, bukan threshold 100jt
                    def _sf(v,d=0.):
                        try: x=float(v); return d if(np.isnan(x) or np.isinf(x)) else x
                        except: return d
                    fnet3=_sf(r.get('FNet3',0)); fnet8=_sf(r.get('FNet8',0))
                    fratio=_sf(r.get('FRatio',0.5))
                    fbuy=_sf(r.get('FBuy',0)); fsell=_sf(r.get('FSell',0))
                    has_asing = (fbuy + fsell) > 0
                    if not has_asing:          fdir="—";        fc_="#4a5568"
                    elif fnet3>0 and fnet8>0:  fdir="🔵 BELI";  fc_="#4da6ff"
                    elif fnet3<0 and fnet8<0:  fdir="🔴 JUAL";  fc_="#ff3d5a"
                    else:                      fdir="⚪ MIX";   fc_="#888888"
                    lwick=_sf(r.get('LWick',0))

                    # Val display dari daily turnover
                    vb = turnover / 1e9
                    val_str = f"{vb:.1f}B" if vb >= 1 else f"{round(vb*1000,0):.0f}M"

                    results.append({
                        "Ticker":stock_map.get(ticker_yf, ticker_raw),"Price":int(close),"Score":sc,
                        "Signal":sig,"Sinyal_v2":sig,"Aksi_v2":aksi_v2,
                        "Trend":trend,"RSI-EMA":round(float(r['RSI_EMA']),1),
                        "Stoch K":round(float(r['STOCH_K']),1),"Stoch D":round(float(r['STOCH_D']),1),
                        "MACD Hist":round(float(r['MACD_Hist']),4),"RVOL":round(rvol,2),
                        "BB%":round(float(r['BB_pct']),2),"ROC 3B%":round(gain_pct,2),
                        "Gain":round(gain_pct,1),
                        "VWAP":int(float(r['VWAP'])),"TP":int(tp),"SL":int(sl),"R:R":round(rr,1),
                        "Turnover(M)":round(turnover/1e6,1),"Val":val_str,
                        "Reasons":" · ".join(reasons),
                        "_class":get_card_class(sig),"LWick":round(lwick,1),
                        "FDir":fdir,"FC":fc_,
                        "FNet3":int(fnet3),"FNet8":int(fnet8),"FRatio":round(fratio,2),
                        "sc_v2":sc_v2,"gc_now":gc_now,
                    })
                except: continue

            pb.progress(1.0)
            prog_ph.empty(); pb.empty()
            st.session_state.scan_results = results
            st.session_state.last_scan_time = now_jkt.timestamp()
            _tt_save(results, st.session_state.last_scan_time)  # persist ke disk
            st.session_state.last_scan_mode = scan_mode

            if tele_on and results:
                if 'tt_last_sent' not in st.session_state: st.session_state.tt_last_sent=set()
                df_tmp=pd.DataFrame(results).sort_values("Score",ascending=False)
                cur_set=set(df_tmp['Ticker'].tolist()); new_alr=cur_set-st.session_state.tt_last_sent
                if new_alr:
                    top_new=df_tmp[df_tmp['Ticker'].isin(new_alr)].head(5).to_dict('records')
                    if top_new: send_telegram(top_new)
                    st.session_state.tt_last_sent.update(new_alr)
                st.session_state.tt_last_sent=st.session_state.tt_last_sent&cur_set
        except Exception as e:
            try: prog_ph.empty(); pb.empty()
            except: pass
            st.error(f"Scan error: {str(e)[:100]}")

    # Countdown timer display
    if st.session_state.last_scan_time:
        _now_cd  = now_jkt.timestamp()
        _rem_cd  = max(0, 300-(_now_cd-st.session_state.last_scan_time))
        _mnt_cd  = int(_rem_cd//60); _sec_cd=int(_rem_cd%60)
        _last_cd = datetime.fromtimestamp(st.session_state.last_scan_time,jakarta_tz).strftime("%H:%M:%S")
        _el      = int(_now_cd-st.session_state.last_scan_time)
        st.caption(f"⏱️ Scan {_el//60}m {_el%60}s lalu · Refresh dalam: {_mnt_cd:02d}:{_sec_cd:02d} · Last: {_last_cd} WIB · {'🔄 Fresh' if force_fresh else '💾 Cache'}")

    results = st.session_state.scan_results
    if not results and not do_scan_btn and not auto_trigger:
        st.markdown(f"""
        <div style="text-align:center;padding:48px;color:#4a5568;font-family:Space Mono,monospace;">
          <div style="font-size:36px;margin-bottom:12px;">🔥</div>
          <div style="font-size:13px;letter-spacing:2px;">KLIK SCAN UNTUK MULAI</div>
          <div style="font-size:10px;margin-top:8px;color:#2d3748;">
            {"⚡ Quick: 200 saham" if quick_mode else f"Full: {len(raw_stocks)} saham"} · Regime: {regime} · {rcfg['mode']}
          </div>
        </div>""", unsafe_allow_html=True)

    elif results:
        df_out = pd.DataFrame(results).sort_values("Score",ascending=False).reset_index(drop=True)
        gacor  = df_out[df_out["Signal"].str.contains("GACOR|REVERSAL|HAKA|SUPER|BANDAR",na=False)]
        potensi= df_out[df_out["Signal"].str.contains("POTENSIAL|REBOUND|AKUM",na=False)]
        avg_rsi= df_out['RSI-EMA'].mean()

        # FIX: define bandarmologi vars dulu sebelum dipakai
        asing_b_list = df_out[df_out["FDir"]=="🔵 BELI"]["Ticker"].tolist() if "FDir" in df_out.columns else []
        asing_j_list = df_out[df_out["FDir"]=="🔴 JUAL"]["Ticker"].tolist() if "FDir" in df_out.columns else []
        bandar_list  = df_out[df_out["Signal"].str.contains("BANDAR",na=False)]["Ticker"].tolist() if "Signal" in df_out.columns else []
        bandar_cnt   = len(bandar_list)  # ← FIX: variable was referenced but never defined
        asing_beli   = len(asing_b_list) # ← FIX
        asing_jual   = len(asing_j_list) # ← FIX

        st.markdown(f"""
        <div class="metric-row">
          <div class="metric-card" style="border-top-color:{rcolor}"><div class="metric-label">Regime</div>
            <div class="metric-value" style="font-size:16px;color:{rcolor}">{regime}</div>
            <div class="metric-sub">{ihsg_price:,.0f} {chg_sym}{abs(ihsg_chg):.2f}%</div></div>
          <div class="metric-card orange"><div class="metric-label">Mode</div>
            <div class="metric-value" style="font-size:13px;margin-top:4px;">{scan_mode}</div></div>
          <div class="metric-card green"><div class="metric-label">Signal Lolos</div>
            <div class="metric-value">{len(df_out)}</div><div class="metric-sub">dari {len(raw_stocks)} emiten</div></div>
          <div class="metric-card red"><div class="metric-label">GACOR/BANDAR 🔥</div>
            <div class="metric-value">{len(gacor)}</div></div>
          <div class="metric-card amber"><div class="metric-label">POTENSIAL</div>
            <div class="metric-value">{len(potensi)}</div></div>
          <div class="metric-card"><div class="metric-label">Avg RSI-EMA</div>
            <div class="metric-value" style="color:{'#00ff88' if avg_rsi>50 else '#ffb700' if avg_rsi>35 else '#ff3d5a'}">{avg_rsi:.1f}</div></div>
        </div>""", unsafe_allow_html=True)

        # Bandarmologi panel — FIX: pakai variabel yang sudah didefinisikan
        if DS_KEY:
            st.markdown(f"""
<div style="display:flex;gap:10px;margin-bottom:10px;flex-wrap:wrap">
  <div style="background:#0d1a2e;border:1px solid #4da6ff44;border-radius:8px;padding:8px 14px;flex:1;min-width:120px">
    <div style="font-family:Space Mono,monospace;font-size:9px;color:#4da6ff;letter-spacing:1px">🔵 BANDAR MASUK</div>
    <div style="font-family:Space Mono,monospace;font-size:18px;font-weight:700;color:#4da6ff">{bandar_cnt}</div>
    <div style="font-size:9px;color:#4a5568">{", ".join(bandar_list[:5]) or "—"}</div>
  </div>
  <div style="background:#0d2010;border:1px solid #00ff8844;border-radius:8px;padding:8px 14px;flex:1;min-width:120px">
    <div style="font-family:Space Mono,monospace;font-size:9px;color:#00ff88;letter-spacing:1px">🔵 ASING NET BUY</div>
    <div style="font-family:Space Mono,monospace;font-size:18px;font-weight:700;color:#00ff88">{asing_beli}</div>
    <div style="font-size:9px;color:#4a5568">{", ".join(asing_b_list[:5]) or "—"}</div>
  </div>
  <div style="background:#200d0d;border:1px solid #ff3d5a44;border-radius:8px;padding:8px 14px;flex:1;min-width:120px">
    <div style="font-family:Space Mono,monospace;font-size:9px;color:#ff3d5a;letter-spacing:1px">🔴 ASING NET SELL</div>
    <div style="font-family:Space Mono,monospace;font-size:18px;font-weight:700;color:#ff3d5a">{asing_jual}</div>
    <div style="font-size:9px;color:#4a5568">{", ".join(asing_j_list[:5]) or "—"}</div>
  </div>
  <div style="background:#0d1117;border:1px solid #1c2533;border-radius:8px;padding:8px 14px;flex:1;min-width:120px">
    <div style="font-family:Space Mono,monospace;font-size:9px;color:#4a5568;letter-spacing:1px">⚡ DATA SOURCE</div>
    <div style="font-family:Space Mono,monospace;font-size:14px;font-weight:700;color:#2dd4bf">DataSectors</div>
    <div style="font-size:9px;color:#4a5568">IDX · FBuy+FSell+OHLCV</div>
  </div>
</div>""", unsafe_allow_html=True)

        # Ticker tape
        th='<div class="tape-wrap"><div class="tape-inner">'
        for _,row in df_out.iterrows():
            roc=row['ROC 3B%']; cls='up' if roc>0 else('down' if roc<0 else'flat'); sym='▲' if roc>0 else('▼' if roc<0 else'─')
            th+=f'<span class="tape-item {cls}">{row["Ticker"]} {int(row["Price"])} {sym}{abs(roc):.1f}% [{row["Signal"]}]</span>'
        th+=th.replace('tape-inner">',''); th+='</div></div>'
        st.markdown(th, unsafe_allow_html=True)

        if not gacor.empty:
            st.markdown(f'<div class="alert-box"><div class="alert-title">🚨 GACOR ALERT · {len(gacor)} SAHAM · {scan_mode}</div></div>', unsafe_allow_html=True)

        # Badge helpers
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
               "POTENSIAL":("#ffb700","#1a1a0d")}
            for k,(c,bg) in M.items():
                if k in s: return f'<span style="background:{bg};color:{c};padding:2px 10px;border-radius:4px;font-size:9px;font-weight:700;border:1px solid {c}44">{s}</span>'
            return f'<span style="background:#1a1a1a;color:#4a5568;padding:2px 10px;border-radius:4px;font-size:9px;font-weight:700">{s}</span>'

        if view_mode=="Card View 🃏":
            st.markdown('<div class="section-title">Signal Cards</div>', unsafe_allow_html=True)
            card_html='<div class="signal-grid">'
            for _,row in df_out.head(20).iterrows():
                sc_int=int(row['Score'])
                bars=''.join([f'<div class="sc-bar {"filled" if i<sc_int else "empty"}" style="width:28px"></div>' for i in range(6)])
                roc_c='#00ff88' if row['ROC 3B%']>0 else'#ff3d5a'
                te="📈" if "▲" in row['Trend'] else("📉" if "▼" in row['Trend'] else"➡️")
                fd=row.get("FDir","—"); fc=row.get("FC","#4a5568")
                card_html+=f"""<div class="signal-card {row['_class']}">
                  <div style="display:flex;justify-content:space-between;align-items:flex-start;">
                    <div><div class="sc-ticker">{row['Ticker']}</div>
                    <div class="sc-price" style="color:{roc_c}">{int(row['Price']):,} {te}</div></div>
                    <div style="text-align:right;">
                      <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;">SCORE</div>
                      <div style="font-family:Space Mono,monospace;font-size:20px;font-weight:700;color:{'#00ff88' if sc_int>=5 else '#ffb700' if sc_int>=4 else '#00e5ff'}">{row['Score']}</div>
                    </div>
                  </div>
                  <div class="sc-signal" style="color:{'#00ff88' if 'GACOR' in row['Signal'] or 'HAKA' in row['Signal'] else '#ffb700' if 'POTENSIAL' in row['Signal'] or 'REBOUND' in row['Signal'] else '#4da6ff' if 'BANDAR' in row['Signal'] else '#00e5ff'}">{row['Signal']}</div>
                  <div class="sc-bars">{bars}</div>
                  <div class="sc-stats">
                    <div class="sc-stat">RSI-EMA <span>{row['RSI-EMA']}</span></div>
                    <div class="sc-stat">STOCH <span>{row['Stoch K']:.0f}</span></div>
                    <div class="sc-stat">RVOL <span>{row['RVOL']}x</span></div>
                    <div class="sc-stat">ASING <span style="color:{fc}">{fd}</span></div>
                  </div>
                  <div class="sc-stats" style="margin-top:6px;">
                    <div class="sc-stat">TP <span style="color:#00ff88">{int(row['TP']):,}</span></div>
                    <div class="sc-stat">SL <span style="color:#ff3d5a">{int(row['SL']):,}</span></div>
                    <div class="sc-stat">R:R <span>{row['R:R']}</span></div>
                  </div>
                  <div style="margin-top:8px;font-size:10px;color:#4a5568;line-height:1.4;font-family:Space Mono,monospace;">{row['Reasons'][:70]}</div>
                </div>"""
            card_html+='</div>'
            st.markdown(card_html, unsafe_allow_html=True)

        st.markdown('<div class="section-title">Full Signal Table</div>', unsafe_allow_html=True)

        rows_html=""
        for _,row in df_out.head(50).iterrows():
            gc  = "#00ff88" if row.get("ROC 3B%",0)>0 else "#ff3d5a"
            rsi_v=row.get("RSI-EMA",50)
            rsi_s="UP" if rsi_v>60 else("DEAD" if rsi_v<35 else("DOWN" if rsi_v<45 else "NEU"))
            rsi_c="#00ff88" if rsi_v>60 else"#ff3d5a" if rsi_v<35 else"#ff7b00" if rsi_v<45 else"#4a5568"
            rvol_v=row.get("RVOL",1); rvol_s=f"{rvol_v*100:.0f}%" if rvol_v<10 else f"{rvol_v:.1f}x"
            fd=row.get("FDir","—"); fc=row.get("FC","#4a5568")
            sinyal_v2=row.get("Sinyal_v2",row.get("Signal","-"))
            aksi_v2=row.get("Aksi_v2","-")
            tp_v=row.get("TP",0); sl_v=row.get("SL",0); price=row.get("Price",0)
            profit_pct=(tp_v-price)/max(price,1)*100 if price>0 else 0
            lwick=row.get("LWick",0)
            rows_html+=f"""<tr style="font-family:Space Mono,monospace;font-size:10px;">
<td style="padding:5px 6px;font-weight:700;color:#e6edf3;text-align:left;border-bottom:1px solid #1c2533;white-space:nowrap">{row['Ticker']}</td>
<td style="padding:5px 6px;color:{gc};font-weight:700;border-bottom:1px solid #1c2533;text-align:center">{row.get('ROC 3B%',0):+.1f}%</td>
<td style="padding:5px 6px;color:{"#00ff88" if lwick>30 else "#4a5568"};border-bottom:1px solid #1c2533;text-align:center">{lwick:.0f}%</td>
<td style="padding:5px 6px;border-bottom:1px solid #1c2533;text-align:center">{tt_aksi_badge(aksi_v2)}</td>
<td style="padding:5px 6px;border-bottom:1px solid #1c2533;text-align:center">{tt_sinyal_badge(sinyal_v2)}</td>
<td style="padding:5px 6px;color:#ff7b00;font-weight:700;border-bottom:1px solid #1c2533;text-align:center">{rvol_s}</td>
<td style="padding:5px 6px;color:#c9d1d9;border-bottom:1px solid #1c2533;text-align:center">{row["Price"]:,}</td>
<td style="padding:5px 6px;background:#0d2b0d;color:#00ff88;font-weight:700;border-bottom:1px solid #1c2533;text-align:center">{int(tp_v):,}</td>
<td style="padding:5px 6px;background:#2b0d0d;color:#ff3d5a;border-bottom:1px solid #1c2533;text-align:center">{int(sl_v):,}</td>
<td style="padding:5px 6px;color:#00ff88;border-bottom:1px solid #1c2533;text-align:center">{profit_pct:.1f}%</td>
<td style="padding:5px 6px;border-bottom:1px solid #1c2533;text-align:center"><span style="color:{rsi_c};font-weight:700">{rsi_s}</span></td>
<td style="padding:5px 6px;color:{rsi_c};border-bottom:1px solid #1c2533;text-align:center">{rsi_v:.0f}</td>
<td style="padding:5px 6px;color:#4a5568;font-size:9px;border-bottom:1px solid #1c2533;text-align:center">{row.get("Turnover(M)",0):.0f}M</td>
<td style="padding:5px 6px;color:{fc};border-bottom:1px solid #1c2533;text-align:center;font-size:10px">{fd}</td>
</tr>"""

        st.markdown(f"""
<div style="overflow-x:auto;border-radius:8px;border:1px solid #1c2533;max-height:70vh;overflow-y:auto;">
<table style="width:100%;border-collapse:collapse;">
<thead><tr style="background:#080c10;position:sticky;top:0;z-index:10;">
  {"".join(f'<th style="padding:7px 6px;color:#4a5568;font-family:Space Mono,monospace;font-size:9px;letter-spacing:1px;border-bottom:2px solid #1c2533">{h}</th>' for h in ["EMITEN","GAIN","WICK","AKSI","SINYAL","RVOL","NOW","TP","SL","PROFIT","RSI SIG","RSI","TURNOVER","ASING"])}
</tr></thead>
<tbody style="background:#0d1117">{rows_html}</tbody>
</table>
<div style="padding:6px 12px;background:#080c10;font-family:Space Mono,monospace;font-size:9px;color:#4a5568;border-top:1px solid #1c2533">
  TP=4xATR · SL=2xATR · Theta Turbo v5.1 ⚡ DataSectors
</div>
</div>""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════
#  TAB 2: WATCHLIST
# ════════════════════════════════════════════════════
with tab_watchlist:
    st.markdown('<div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-bottom:12px;padding:10px 14px;background:#0d1117;border-radius:6px;border-left:3px solid #ff7b00;">Analisa mendalam untuk saham pilihan lo. Input ticker IDX (tanpa .JK), pisah koma atau enter.</div>', unsafe_allow_html=True)
    wc1, wc2, wc3 = st.columns([3,1,1])
    with wc1:
        wl_input = st.text_area("Ticker", placeholder="Contoh:\nBBCA\nARCI, ASSA, GOTO", height=120, label_visibility="collapsed", key="wl_input")
    with wc2:
        wl_mode = st.radio("Mode", ["Scalping ⚡","Momentum 🚀","Reversal 🎯"], key="wl_mode")
        st.caption(f"Regime suggest: {rcfg['mode']}")
    with wc3:
        st.markdown("<br>", unsafe_allow_html=True)
        wl_force = st.toggle("🔄 Fresh", value=False, key="wl_fresh")
        wl_run   = st.button("🔍 Analisa", use_container_width=True, key="wl_run")
        wl_tele  = st.button("📡 Kirim Telegram", use_container_width=True, key="wl_tele")

    if wl_run and wl_input.strip():
        raw_wl = list(dict.fromkeys([t.strip().upper() for line in wl_input.split("\n")
                                     for t in line.split(",") if t.strip()]))
        if raw_wl:
            wl_res = []
            _pb_wl = st.progress(0)
            for i, t in enumerate(raw_wl):
                _pb_wl.progress((i+1)/len(raw_wl))
                df = None
                try:
                    if DS_KEY:
                        df = fetch_ds_ohlcv(t, "15m", 200, wl_force)
                    if df is None:
                        raw = yf.download(t+".JK", period="5d", interval="15m",
                                          progress=False, auto_adjust=True, threads=False)
                        if not raw.empty:
                            if isinstance(raw.columns,pd.MultiIndex): raw.columns=raw.columns.droplevel(1)
                            df = raw.dropna()
                            if len(df)<10: df=None
                except: pass
                if df is None or len(df)<55:
                    wl_res.append({"Ticker":t,"Price":0,"Score":0,"Signal":"No data","Trend":"-",
                        "RSI-EMA":0,"Stoch K":0,"RVOL":0,"BB%":0,"ROC 3B%":0,
                        "VWAP":0,"TP":0,"SL":0,"R:R":0,"ATR":0,"Reasons":"No data","_class":"","MACD Hist":0})
                    continue
                try:
                    df2 = apply_intraday_indicators(df.copy())
                    r=df2.iloc[-1]; p=df2.iloc[-2]; p2=df2.iloc[-3] if len(df2)>=3 else p
                    close=float(r['Close']); atr=float(r['ATR'])
                    sig,sc_v2,flags_v2,gc_now = get_sinyal_v2(r,p,p2)
                    sc=round(min(6,max(0,sc_v2/10)),1)
                    reasons=flags_v2.split(" · ") if flags_v2 else []
                    tp=close+4.0*atr; sl=close-2.0*atr; rr=(tp-close)/max(close-sl,0.01)
                    e9=float(r['EMA9']); e21=float(r['EMA21']); e50=float(r['EMA50'])
                    trend="▲ UP" if e9>e21>e50 else("▼ DOWN" if e9<e21<e50 else"◆ SIDE")
                    wl_res.append({"Ticker":t,"Price":int(close),"Score":sc,"Signal":sig,
                        "Trend":trend,"RSI-EMA":round(float(r['RSI_EMA']),1),
                        "Stoch K":round(float(r['STOCH_K']),1),"RVOL":round(float(r['RVOL']),2),
                        "BB%":round(float(r['BB_pct']),2),"ROC 3B%":round(float(r['ROC3'])*100,2),
                        "VWAP":int(float(r['VWAP'])),"TP":int(tp),"SL":int(sl),"R:R":round(rr,1),
                        "ATR":round(atr,0),"MACD Hist":round(float(r['MACD_Hist']),4),
                        "Reasons":" · ".join(reasons),"_class":get_card_class(sig)})
                except Exception as ex:
                    wl_res.append({"Ticker":t,"Price":0,"Score":0,"Signal":f"Err:{str(ex)[:20]}",
                        "Trend":"-","RSI-EMA":0,"Stoch K":0,"RVOL":0,"BB%":0,"ROC 3B%":0,
                        "VWAP":0,"TP":0,"SL":0,"R:R":0,"ATR":0,"Reasons":"","_class":"","MACD Hist":0})
            _pb_wl.empty()
            st.session_state.wl_results  = wl_res
            st.session_state.wl_mode_used = wl_mode

    if wl_tele and st.session_state.wl_results:
        to_send=[r for r in st.session_state.wl_results if r["Price"]>0]
        if to_send: send_telegram(to_send[:5],source="Watchlist"); st.success("📡 Terkirim!")

    if st.session_state.wl_results:
        ok=[r for r in st.session_state.wl_results if r["Score"]>0]
        gcr=[r for r in ok if "GACOR" in r.get("Signal","") or "REVERSAL" in r.get("Signal","") or "HAKA" in r.get("Signal","") or "BANDAR" in r.get("Signal","")]
        pot=[r for r in ok if "POTENSIAL" in r.get("Signal","") or "REBOUND" in r.get("Signal","")]
        st.markdown(f"""<div class="metric-row" style="margin-top:12px;">
          <div class="metric-card orange"><div class="metric-label">Dipantau</div><div class="metric-value">{len(st.session_state.wl_results)}</div></div>
          <div class="metric-card green"><div class="metric-label">GACOR/BANDAR 🔥</div><div class="metric-value">{len(gcr)}</div></div>
          <div class="metric-card amber"><div class="metric-label">POTENSIAL</div><div class="metric-value">{len(pot)}</div></div>
          <div class="metric-card"><div class="metric-label">Data OK</div><div class="metric-value">{len(ok)}</div></div>
        </div>""", unsafe_allow_html=True)

        ch='<div class="signal-grid">'
        for row in sorted(st.session_state.wl_results, key=lambda x: x["Score"], reverse=True):
            if row["Price"]==0:
                ch+=f'<div class="signal-card"><div class="sc-ticker">{row["Ticker"]}</div><div style="font-size:11px;color:#4a5568;margin-top:6px;">{row.get("Signal","No data")}</div></div>'
                continue
            sc_int=int(row["Score"]); bars=''.join([f'<div class="sc-bar {"filled" if i<sc_int else "empty"}" style="width:26px"></div>' for i in range(6)])
            sig=row.get("Signal","-")
            sc_col="#00ff88" if ("GACOR" in sig or "REVERSAL" in sig or "HAKA" in sig or "BANDAR" in sig) else("#ffb700" if "POTENSIAL" in sig else "#00e5ff" if "WATCH" in sig else "#4a5568")
            rsi_v=row["RSI-EMA"]; rsi_c="#ff3d5a" if rsi_v<30 else("#ffb700" if rsi_v<45 else "#00ff88" if rsi_v>60 else "#c9d1d9")
            roc_c="#00ff88" if row.get("ROC 3B%",0)>0 else "#ff3d5a"
            te="📈" if "▲" in row["Trend"] else("📉" if "▼" in row["Trend"] else "➡️")
            ch+=f"""<div class="signal-card {row['_class']}">
              <div style="display:flex;justify-content:space-between;">
                <div><div class="sc-ticker">{row['Ticker']}</div>
                <div class="sc-price" style="color:{roc_c}">{row['Price']:,} {te}</div></div>
                <div style="text-align:right">
                  <div style="font-family:Space Mono,monospace;font-size:9px;color:#4a5568">SCORE</div>
                  <div style="font-family:Space Mono,monospace;font-size:22px;font-weight:700;color:{'#00ff88' if sc_int>=5 else '#ffb700' if sc_int>=4 else '#00e5ff'}">{row['Score']}</div>
                </div>
              </div>
              <div class="sc-signal" style="color:{sc_col}">{sig}</div>
              <div class="sc-bars">{bars}</div>
              <div class="sc-stats">
                <div class="sc-stat">RSI-EMA <span style="color:{rsi_c}">{rsi_v}</span></div>
                <div class="sc-stat">STOCH <span>{row['Stoch K']:.0f}</span></div>
                <div class="sc-stat">RVOL <span>{row['RVOL']}x</span></div>
              </div>
              <div class="sc-stats" style="margin-top:6px">
                <div class="sc-stat">TP <span style="color:#00ff88">{int(row['TP']):,}</span></div>
                <div class="sc-stat">SL <span style="color:#ff3d5a">{int(row['SL']):,}</span></div>
                <div class="sc-stat">R:R <span>{row['R:R']}</span></div>
              </div>
              <div style="margin-top:8px;font-size:10px;color:#4a5568;line-height:1.5;font-family:Space Mono,monospace">{row['Reasons'][:80]}</div>
            </div>"""
        ch+='</div>'
        st.markdown(ch, unsafe_allow_html=True)
    elif not wl_run:
        st.markdown('<div style="text-align:center;padding:48px;color:#4a5568;font-family:Space Mono,monospace;"><div style="font-size:32px;margin-bottom:12px;">👁️</div><div>MASUKKAN TICKER DI ATAS</div></div>', unsafe_allow_html=True)

# ════════════════════════════════════════════════════
#  TAB 3: BSJP
# ════════════════════════════════════════════════════
with tab_bsjp:
    now_wib=datetime.now(jakarta_tz)
    is_entry_time=(now_wib.hour==14 and now_wib.minute>=30) or (now_wib.hour==15 and now_wib.minute<=45)
    st.markdown(f"""<div style="background:rgba(191,95,255,.08);border:1px solid rgba(191,95,255,.3);border-radius:8px;padding:14px 18px;margin-bottom:16px;">
      <div style="font-family:Space Mono,monospace;font-size:13px;font-weight:700;color:#bf5fff;">🌙 BELI SORE JUAL PAGI</div>
      <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-top:4px;">
        Entry: <span style="color:#ffb700">14:30–15:45 WIB</span> · Exit: <span style="color:#00ff88">Besok 09:00–10:00 WIB</span> ·
        Status: <span style="color:{'#00ff88' if is_entry_time else '#4a5568'}">{'🟢 WAKTU ENTRY!' if is_entry_time else '⏳ Tunggu 14:30 WIB'}</span>
      </div></div>""", unsafe_allow_html=True)

    bc1,bc2=st.columns([2,1])
    with bc1:
        bsjp_min_score=st.slider("Min BSJP Score",0,6,4,key="bsjp_score")
        bsjp_min_rvol =st.slider("Min RVOL",1.0,5.0,1.5,0.1,key="bsjp_rvol")
    with bc2:
        bsjp_min_turn=st.number_input("Min Turnover (M Rp)",value=500,step=100,key="bsjp_turn")*1_000_000
        bsjp_fresh=st.toggle("🔄 Fresh Data",value=False,key="bsjp_fresh")

    do_bsjp=st.button("🌙 SCAN BSJP SEKARANG",type="primary",use_container_width=True,key="btn_bsjp")

    if do_bsjp:
        bsjp_res=[]; scan_data=st.session_state.get("data_dict",{})
        if not scan_data:
            _pb2=st.progress(0)
            st.info("Fetching data untuk BSJP...")
            scan_data=fetch_intraday(tuple(stocks_yf[:200]), force_fresh=bsjp_fresh)
            _pb2.empty()
        pb_bsjp=st.progress(0); tickers_bsjp=list(scan_data.keys())
        for i,ticker_yf in enumerate(tickers_bsjp):
            pb_bsjp.progress((i+1)/max(len(tickers_bsjp),1))
            try:
                df=scan_data[ticker_yf].copy()
                if len(df)<55: continue
                df2=apply_intraday_indicators(df)
                r=df2.iloc[-1]; p=df2.iloc[-2]; p2=df2.iloc[-3] if len(df2)>=3 else p
                close=float(r['Close']); vol=float(r['Volume'])
                turnover=close*vol; rvol=float(r['RVOL'])
                if turnover<bsjp_min_turn or rvol<bsjp_min_rvol: continue
                sc,reasons,_=score_bsjp(r,p,p2)
                if sc<bsjp_min_score: continue
                bsjp_sig="STRONG BUY 🌙" if sc>=5 else("BUY ⚡" if sc>=4 else "WATCH 👀")
                atr=float(r['ATR']); tp=close+2.0*atr; sl=close-1.0*atr; rr=(tp-close)/max(close-sl,0.01)
                e9=float(r['EMA9']); e21=float(r['EMA21']); e50=float(r['EMA50'])
                trend="▲ UP" if e9>e21>e50 else("▼ DOWN" if e9<e21<e50 else"◆ SIDE")
                bsjp_res.append({"Ticker":stock_map.get(ticker_yf,ticker_yf.replace(".JK","")),"Price":int(close),
                    "Score":sc,"Signal":bsjp_sig,"Trend":trend,"RSI-EMA":round(float(r['RSI_EMA']),1),
                    "Stoch K":round(float(r['STOCH_K']),1),"RVOL":round(rvol,2),"TP":int(tp),"SL":int(sl),
                    "R:R":round(rr,1),"Turnover(M)":round(turnover/1e6,1),"Reasons":" · ".join(reasons),
                    "_class":"gacor" if sc>=5 else"potensial" if sc>=4 else"watch"})
            except: continue
        pb_bsjp.empty()
        st.session_state.bsjp_results=sorted(bsjp_res,key=lambda x:x["Score"],reverse=True)

    bsjp_results=st.session_state.bsjp_results
    if bsjp_results:
        strong=[r for r in bsjp_results if "STRONG" in r.get("Signal","")]
        buy   =[r for r in bsjp_results if r.get("Signal","")=="BUY ⚡"]
        st.markdown(f"""<div class="metric-row">
          <div class="metric-card" style="border-top-color:#bf5fff"><div class="metric-label">Kandidat</div><div class="metric-value">{len(bsjp_results)}</div></div>
          <div class="metric-card green"><div class="metric-label">Strong Buy 🌙</div><div class="metric-value">{len(strong)}</div></div>
          <div class="metric-card amber"><div class="metric-label">Buy ⚡</div><div class="metric-value">{len(buy)}</div></div>
        </div>""", unsafe_allow_html=True)
        bh='<div class="signal-grid">'
        for row in bsjp_results[:12]:
            sc_int=int(row["Score"]); bars=''.join([f'<div class="sc-bar {"filled" if i<sc_int else "empty"}" style="width:26px"></div>' for i in range(6)])
            sc_col="#00ff88" if "STRONG" in row["Signal"] else "#ffb700"
            te="📈" if "▲" in row["Trend"] else("📉" if "▼" in row["Trend"] else "➡️")
            bh+=f"""<div class="signal-card {row['_class']}">
              <div style="display:flex;justify-content:space-between;">
                <div><div class="sc-ticker">{row['Ticker']}</div><div class="sc-price">{row['Price']:,} {te}</div></div>
                <div style="text-align:right"><div style="font-family:Space Mono,monospace;font-size:9px;color:#4a5568">SCORE</div>
                  <div style="font-family:Space Mono,monospace;font-size:22px;font-weight:700;color:{sc_col}">{row['Score']}</div></div>
              </div>
              <div class="sc-signal" style="color:{sc_col}">{row['Signal']}</div>
              <div class="sc-bars">{bars}</div>
              <div class="sc-stats">
                <div class="sc-stat">RSI-EMA <span>{row['RSI-EMA']}</span></div>
                <div class="sc-stat">RVOL <span>{row['RVOL']}x</span></div>
                <div class="sc-stat">R:R <span>{row['R:R']}</span></div>
              </div>
              <div class="sc-stats" style="margin-top:6px">
                <div class="sc-stat">TP <span style="color:#00ff88">{row['TP']:,}</span></div>
                <div class="sc-stat">SL <span style="color:#ff3d5a">{row['SL']:,}</span></div>
              </div>
              <div style="margin-top:4px;font-size:10px;color:#4a5568;font-family:Space Mono,monospace">{row['Reasons'][:70]}</div>
            </div>"""
        bh+='</div>'
        st.markdown(bh, unsafe_allow_html=True)
    elif not do_bsjp:
        st.markdown('<div style="text-align:center;padding:48px;color:#4a5568;font-family:Space Mono,monospace;"><div style="font-size:32px;margin-bottom:12px;">🌙</div><div>KLIK SCAN BSJP</div><div style="font-size:10px;margin-top:8px;color:#2d3748;">Best jam 14:00–15:45 WIB</div></div>', unsafe_allow_html=True)

# ════════════════════════════════════════════════════
#  TAB 4: SEKTOR
# ════════════════════════════════════════════════════
with tab_sector:
    st.markdown('<div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-bottom:14px;padding:10px 14px;background:#0d1117;border-radius:6px;border-left:3px solid #ff7b00;">Track pergerakan tiap sektor IDX hari ini. ⚡ Hormuz-sensitive: Energi, Shipping, Petrokimia</div>', unsafe_allow_html=True)
    do_sector=st.button("🏭 REFRESH SEKTOR",type="primary",use_container_width=True,key="btn_sector")
    if do_sector:
        sec_data={}
        for sec_name,sec_stocks in SECTORS.items():
            res=fetch_sector_rotation(sec_stocks)
            if res:
                avg_chg=sum(r["chg"] for r in res)/len(res)
                avg_rvol=sum(r["rvol"] for r in res)/len(res)
                bullish=sum(1 for r in res if r["chg"]>0)
                sec_data[sec_name]={"avg_chg":round(avg_chg,2),"avg_rvol":round(avg_rvol,2),
                    "bullish":bullish,"total":len(res),"stocks":res,"is_hormuz":sec_name in HORMUZ_SECTORS}
        st.session_state.sector_data=sec_data

    if st.session_state.sector_data:
        sorted_secs=sorted(st.session_state.sector_data.items(),key=lambda x:x[1]["avg_chg"],reverse=True)
        st.markdown('<div class="section-title">Sektor Heatmap Hari Ini</div>', unsafe_allow_html=True)
        cols_sec=st.columns(3)
        for idx,(sec_name,sec_info) in enumerate(sorted_secs):
            chg=sec_info["avg_chg"]
            col="#00ff88" if chg>1 else("#ffb700" if chg>0 else "#ff3d5a")
            bg="rgba(0,255,136,.06)" if chg>1 else("rgba(255,183,0,.06)" if chg>0 else "rgba(255,61,90,.06)")
            hormuz_badge=' <span style="color:#ffb700;font-size:9px">⚡HORMUZ</span>' if sec_info["is_hormuz"] else ""
            bull_pct=int(sec_info["bullish"]/max(sec_info["total"],1)*100)
            with cols_sec[idx%3]:
                st.markdown(f"""<div style="background:{bg};border:1px solid {col}44;border-radius:8px;padding:12px;margin-bottom:10px;">
                  <div style="font-family:Space Mono,monospace;font-size:10px;font-weight:700;color:#c9d1d9;">{sec_name}{hormuz_badge}</div>
                  <div style="font-family:Space Mono,monospace;font-size:22px;font-weight:700;color:{col};margin:4px 0;">{chg:+.2f}%</div>
                  <div style="font-size:9px;color:#4a5568;">RVOL avg: {sec_info['avg_rvol']:.1f}x · Bullish: {sec_info['bullish']}/{sec_info['total']} ({bull_pct}%)</div>
                  <div style="height:4px;background:#1c2533;border-radius:2px;margin-top:6px;overflow:hidden;">
                    <div style="width:{bull_pct}%;height:100%;background:{col};border-radius:2px;"></div>
                  </div></div>""", unsafe_allow_html=True)
    else:
        st.markdown('<div style="text-align:center;padding:48px;color:#4a5568;font-family:Space Mono,monospace;"><div style="font-size:32px;margin-bottom:12px;">🏭</div><div>KLIK REFRESH SEKTOR</div></div>', unsafe_allow_html=True)

# ════════════════════════════════════════════════════
#  TAB 5: GAP UP
# ════════════════════════════════════════════════════
with tab_gapup:
    st.markdown('<div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-bottom:14px;padding:10px 14px;background:#0d1117;border-radius:6px;border-left:3px solid #00ff88;">Deteksi saham yang berpotensi <b style="color:#00ff88">Gap Up besok pagi</b>.</div>', unsafe_allow_html=True)
    gu_c1,gu_c2=st.columns(2)
    with gu_c1: gu_min_score=st.slider("Min Gap Score",1,6,3,key="gu_score")
    with gu_c2: gu_quick=st.toggle("⚡ Quick Scan (200)",value=True,key="gu_quick")
    do_gapup=st.button("📈 SCAN GAP UP",type="primary",use_container_width=True,key="btn_gapup")
    if do_gapup:
        scan_t=stocks_yf[:200] if gu_quick else stocks_yf
        gu_res=scan_gap_up(tuple(scan_t))
        st.session_state.gapup_results=[r for r in gu_res if r["Gap Score"]>=gu_min_score]
    gapup_res=st.session_state.gapup_results
    if gapup_res:
        gap_confirmed=[r for r in gapup_res if "GAP UP" in r.get("Signal","")]
        potential=[r for r in gapup_res if "POTENTIAL" in r.get("Signal","")]
        st.markdown(f"""<div class="metric-row">
          <div class="metric-card green"><div class="metric-label">Gap Confirmed 🚀</div><div class="metric-value">{len(gap_confirmed)}</div></div>
          <div class="metric-card amber"><div class="metric-label">Potential ⚡</div><div class="metric-value">{len(potential)}</div></div>
          <div class="metric-card"><div class="metric-label">Total</div><div class="metric-value">{len(gapup_res)}</div></div>
        </div>""", unsafe_allow_html=True)
        gu_html='<div class="signal-grid">'
        for row in gapup_res[:20]:
            sc_int=int(min(row["Gap Score"],6)); bars=''.join([f'<div class="sc-bar {"filled" if i<sc_int else "empty"}" style="width:26px"></div>' for i in range(6)])
            is_gap="GAP UP" in row.get("Signal",""); sc_col="#00ff88" if is_gap else "#ffb700"
            chg_c="#00ff88" if row["Chg %"]>0 else "#ff3d5a"
            gu_html+=f"""<div class="signal-card {'gacor' if is_gap else 'potensial'}">
              <div style="display:flex;justify-content:space-between;">
                <div><div class="sc-ticker">{row['Ticker']}</div>
                <div class="sc-price" style="color:{chg_c}">{row['Price']:,} ({row['Chg %']:+.1f}%)</div></div>
                <div style="text-align:right"><div style="font-family:Space Mono,monospace;font-size:9px;color:#4a5568">GAP SCORE</div>
                  <div style="font-family:Space Mono,monospace;font-size:22px;font-weight:700;color:{sc_col}">{row['Gap Score']}</div></div>
              </div>
              <div class="sc-signal" style="color:{sc_col}">{row['Signal']}</div>
              <div class="sc-bars">{bars}</div>
              <div class="sc-stats">
                <div class="sc-stat">RVOL <span>{row['RVOL']}x</span></div>
                <div class="sc-stat">Close% <span>{row['Close Ratio']:.0%}</span></div>
                <div class="sc-stat">PrevHigh <span>{row['Prev High']:,}</span></div>
              </div>
              <div style="margin-top:8px;font-size:10px;color:#4a5568;font-family:Space Mono,monospace">{row['Reasons'][:80]}</div>
            </div>"""
        gu_html+='</div>'
        st.markdown(gu_html, unsafe_allow_html=True)
    elif not do_gapup:
        st.markdown('<div style="text-align:center;padding:48px;color:#4a5568;font-family:Space Mono,monospace;"><div style="font-size:32px;margin-bottom:12px;">📈</div><div>KLIK SCAN GAP UP</div><div style="font-size:10px;margin-top:8px;color:#2d3748;">Best run: sore 14:00–16:00 WIB</div></div>', unsafe_allow_html=True)

# ════════════════════════════════════════════════════
#  TAB 6: TRAILING STOP
# ════════════════════════════════════════════════════
with tab_trail:
    st.markdown('<div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-bottom:14px;padding:10px 14px;background:#0d1117;border-radius:6px;border-left:3px solid #bf5fff;">Lock profit di market bullish. Trailing Stop otomatis ikut harga naik.</div>', unsafe_allow_html=True)
    tr_c1,tr_c2=st.columns(2)
    with tr_c1:
        st.markdown('<div class="settings-label">POSISI</div>', unsafe_allow_html=True)
        tr_ticker=st.text_input("Ticker",value="BBCA",key="tr_ticker").upper()
        tr_entry =st.number_input("Harga Entry",value=9000,step=10,key="tr_entry")
        tr_qty   =st.number_input("Jumlah Lot",value=10,step=1,key="tr_qty")
    with tr_c2:
        st.markdown('<div class="settings-label">SETTING</div>', unsafe_allow_html=True)
        tr_method=st.radio("Metode",["ATR","Persen","Swing Low"],key="tr_method")
        if tr_method=="ATR": tr_atr_mult=st.slider("ATR Multiplier",1.0,5.0,2.0,0.5,key="tr_atr_m")
        elif tr_method=="Persen": tr_pct=st.slider("Trailing %",1.0,10.0,3.0,0.5,key="tr_pct")
        tr_alert=st.toggle("🔔 Alert Telegram",value=True,key="tr_alert")

    if st.button("🎯 HITUNG TRAILING STOP",type="primary",use_container_width=True,key="btn_trail"):
        try:
            df_tr=None
            if DS_KEY: df_tr=fetch_ds_ohlcv(tr_ticker,"15m",200)
            if df_tr is None:
                raw_tr=yf.download(tr_ticker+".JK",period="5d",interval="15m",progress=False,auto_adjust=True,threads=False)
                if not raw_tr.empty:
                    if isinstance(raw_tr.columns,pd.MultiIndex): raw_tr.columns=raw_tr.columns.droplevel(1)
                    df_tr=raw_tr.dropna()
            if df_tr is not None and len(df_tr)>20:
                df_tr2=apply_intraday_indicators(df_tr.copy())
                current=float(df_tr2["Close"].iloc[-1]); atr_val=float(df_tr2["ATR"].iloc[-1])
                if tr_method=="ATR": trail_result=calc_trailing_stop(tr_entry,current,atr_val,"ATR",tr_atr_mult)
                elif tr_method=="Persen": trail_result=calc_trailing_stop(tr_entry,current,atr_val,"Persen",pct=tr_pct)
                else: trail_result=calc_trailing_stop(tr_entry,current,atr_val,"Swing Low")
                stop=trail_result["stop"]; p_float=trail_result["profit_float"]; p_locked=trail_result["profit_locked"]
                is_profit=trail_result["is_profitable"]
                lot_val=tr_qty*100; profit_rp=(current-tr_entry)*lot_val; locked_rp=max(0,(stop-tr_entry)*lot_val)
                stop_col="#00ff88" if is_profit else "#ff3d5a"
                profit_col="#00ff88" if profit_rp>=0 else "#ff3d5a"
                st.markdown(f"""<div style="background:#0d1117;border:1px solid {stop_col}44;border-radius:10px;padding:20px;margin-top:12px;">
                  <div class="metric-row">
                    <div class="metric-card"><div class="metric-label">Sekarang</div><div class="metric-value" style="color:#00e5ff">{int(current):,}</div><div class="metric-sub">ATR: {int(atr_val)}</div></div>
                    <div class="metric-card" style="border-top-color:{stop_col}"><div class="metric-label">🎯 Trailing Stop</div><div class="metric-value" style="color:{stop_col}">{int(stop):,}</div></div>
                    <div class="metric-card" style="border-top-color:{profit_col}"><div class="metric-label">Profit Float</div><div class="metric-value" style="color:{profit_col}">{p_float:+.1f}%</div><div class="metric-sub">Rp {profit_rp:,.0f}</div></div>
                    <div class="metric-card" style="border-top-color:#00ff88"><div class="metric-label">Terkunci 🔒</div><div class="metric-value" style="color:#00ff88">{p_locked:+.1f}%</div><div class="metric-sub">Rp {locked_rp:,.0f}</div></div>
                  </div>
                  <div style="margin-top:10px;font-family:Space Mono,monospace;font-size:10px;color:#4a5568;">
                    {tr_ticker} · Entry {tr_entry:,} → Now {int(current):,} · {tr_qty} lot ({lot_val:,} lembar) · {'✅ Profit terkunci!' if is_profit else '⚠️ Stop masih di bawah entry'}
                  </div></div>""", unsafe_allow_html=True)
                if tr_alert and TOKEN and CHAT_ID:
                    msg_tr=(f"🎯 *TRAILING STOP*\n{tr_ticker} · {tr_method}\n"
                            f"Entry: `{tr_entry:,}` → Now: `{int(current):,}`\n"
                            f"Stop: `{int(stop):,}` · Float: `{p_float:+.1f}%`\n"
                            f"Locked: `{p_locked:+.1f}%` (Rp {locked_rp:,.0f})")
                    try: requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",data={"chat_id":CHAT_ID,"text":msg_tr,"parse_mode":"Markdown"},timeout=8)
                    except: pass
            else: st.error(f"Data {tr_ticker} tidak tersedia")
        except Exception as ex: st.error(f"Error: {str(ex)[:80]}")

# ════════════════════════════════════════════════════
#  TAB 7: BACKTEST
# ════════════════════════════════════════════════════
with tab_backtest:
    st.markdown('<div class="section-title">Backtest Engine · 15M Intraday</div>', unsafe_allow_html=True)
    bt_c1,bt_c2,bt_c3,bt_c4=st.columns(4)
    bt_mode   =bt_c1.selectbox("Mode",["Scalping ⚡","Momentum 🚀","Reversal 🎯"],key="bt_mode")
    bt_sc     =bt_c2.slider("Min Score",0,6,4,key="bt_sc")
    bt_fwd    =int(bt_c3.number_input("Hold (bars)",value=4,step=1,min_value=1,max_value=20))
    bt_sl_mult=bt_c4.number_input("SL mult (x ATR)",value=0.8,step=0.1,min_value=0.1,max_value=3.0)

    if st.button("🚀 Run Backtest",type="primary",key="bt_run"):
        data_dict=st.session_state.get("data_dict",{})
        if not data_dict:
            st.warning("Jalankan Scanner dulu bro!")
        else:
            bt_results=[]; bt_pb=st.progress(0); sample=list(data_dict.keys())[:80]
            for bi,t_yf in enumerate(sample):
                bt_pb.progress((bi+1)/len(sample))
                try:
                    d=data_dict[t_yf].copy()
                    if len(d)<60: continue
                    d=apply_intraday_indicators(d)
                    for ii in range(50,len(d)-bt_fwd):
                        r0=d.iloc[ii]; r1=d.iloc[ii-1]; r2=d.iloc[ii-2]
                        if bt_mode=="Scalping ⚡":   sc,_,_=score_scalping(r0,r1,r2)
                        elif bt_mode=="Momentum 🚀": sc,_,_=score_momentum(r0,r1,r2)
                        else:                         sc,_,_=score_reversal(r0,r1,r2)
                        if sc<bt_sc: continue
                        entry=float(r0['Close']); atr_v=float(r0['ATR']) if not np.isnan(float(r0['ATR'])) else entry*0.005
                        tp_p=entry+2.0*atr_v; sl_p=entry-bt_sl_mult*atr_v
                        exit_price=float(d.iloc[ii+bt_fwd]['Close'])
                        for fwd_i in range(1,bt_fwd+1):
                            bar=d.iloc[ii+fwd_i]
                            if float(bar['High'])>=tp_p: exit_price=tp_p; break
                            if float(bar['Low'])<=sl_p:  exit_price=sl_p; break
                        bt_results.append((exit_price-entry)/entry*100)
                except: continue
            bt_pb.empty()
            if not bt_results:
                st.warning("Tidak ada trades yang match. Turunkan Min Score.")
            else:
                arr=np.array(bt_results); wr=len(arr[arr>0])/len(arr)*100
                avg=np.mean(arr); med=np.median(arr)
                pf=arr[arr>0].sum()/max(abs(arr[arr<0].sum()),0.01)
                mxdd=arr[arr<0].min() if len(arr[arr<0])>0 else 0
                st.markdown(f"""<div class="bt-result">
                  <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;letter-spacing:2px;margin-bottom:14px;">
                    {len(arr)} TRADES · SCORE≥{bt_sc} · HOLD {bt_fwd} BARS (~{bt_fwd*15}M) · {bt_mode}
                  </div>
                  <div style="display:flex;flex-wrap:wrap;">
                    <span class="bt-metric"><div class="bt-metric-val" style="color:{'#00ff88' if wr>=55 else '#ffb700' if wr>=50 else '#ff3d5a'}">{wr:.1f}%</div><div class="bt-metric-lbl">Win Rate</div></span>
                    <span class="bt-metric"><div class="bt-metric-val" style="color:{'#00ff88' if avg>0 else '#ff3d5a'}">{avg:+.2f}%</div><div class="bt-metric-lbl">Avg Return</div></span>
                    <span class="bt-metric"><div class="bt-metric-val" style="color:#00e5ff">{med:+.2f}%</div><div class="bt-metric-lbl">Median</div></span>
                    <span class="bt-metric"><div class="bt-metric-val" style="color:{'#00ff88' if pf>=1.5 else '#ffb700' if pf>=1 else '#ff3d5a'}">{pf:.2f}x</div><div class="bt-metric-lbl">Profit Factor</div></span>
                    <span class="bt-metric"><div class="bt-metric-val" style="color:#ff3d5a">{mxdd:.1f}%</div><div class="bt-metric-lbl">Max Loss</div></span>
                    <span class="bt-metric"><div class="bt-metric-val" style="color:#00ff88">{sum(1 for x in bt_results if x>0)}</div><div class="bt-metric-lbl">TP Hits</div></span>
                    <span class="bt-metric"><div class="bt-metric-val" style="color:#ff3d5a">{sum(1 for x in bt_results if x<0)}</div><div class="bt-metric-lbl">SL Hits</div></span>
                  </div></div>""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════
#  FOOTER + AUTO-REFRESH — JavaScript Timer
#  st.rerun() tidak jalan saat page idle (tidak ada interaksi).
#  JS timer jalan di browser → reload otomatis tiap 5 menit.
# ════════════════════════════════════════════════════
import streamlit.components.v1 as _components

_now_f      = now_jkt.timestamp()
is_open_now = 9 <= now_jkt.hour < 16

if st.session_state.last_scan_time:
    _rem2    = max(0, 480 - (_now_f - st.session_state.last_scan_time))
    mnt2     = int(_rem2 // 60); sec2 = int(_rem2 % 60)
    last_t2  = datetime.fromtimestamp(st.session_state.last_scan_time, jakarta_tz).strftime("%H:%M:%S")
    elapsed_s= int(_now_f - st.session_state.last_scan_time)
    time_info= f"⏱️ Next: <span style='color:#ff7b00'>{mnt2:02d}:{sec2:02d}</span> · Last: <span style='color:#2dd4bf'>{last_t2} WIB</span> · ⚡ DataSectors"
else:
    _rem2 = 300; elapsed_s = 0
    time_info = "⏱️ Klik Scan untuk mulai · ⚡ DataSectors"

st.markdown(f"""
<div style="margin-top:28px;padding-top:14px;border-top:1px solid #1c2533;
     display:flex;justify-content:space-between;flex-wrap:wrap;gap:8px;">
  <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;">
    🔥 Theta Turbo v5.2 · DataSectors ⚡ · Auto-refresh 5m
  </div>
  <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;">{time_info}</div>
</div>""", unsafe_allow_html=True)

# JS timer → reload browser otomatis, tidak butuh interaksi user
# st.rerun() DIHAPUS → bikin infinite loop di Streamlit Cloud!
if is_open_now and st.session_state.scan_results and st.session_state.last_scan_time:
    _rem_ms = max(10000, int(_rem2 * 1000))  # min 10 detik
    if elapsed_s < 600:  # max 10 menit
        _components.html(
            f"""<script>
            if(window._tt_timer) clearTimeout(window._tt_timer);
            window._tt_timer = setTimeout(function(){{
                window.parent.location.reload();
            }}, {_rem_ms});
            </script>""",
            height=0)
