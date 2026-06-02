"""
╔══════════════════════════════════════════════════════════════════╗
║   THETA KRAKEN v1.0  —  IDX Scanner                              ║
║   Merged: Theta Turbo (scoring) + HF Terminal (fetch strategy)   ║
║                                                                   ║
║   FETCH: 1-shot batch download semua ticker (HF style)           ║
║          @cache(ttl=120) — no rate limit, auto-refresh 2min      ║
║   SCORE: Theta indicators + HF sector Z-score + Kraken engine    ║
║          EMA·RSI·Stoch·MACD·VWAP·BB + anomaly detection         ║
║   SIGNAL: Combined 0-100 scoring — smart money + momentum        ║
╚══════════════════════════════════════════════════════════════════╝
"""

import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import requests
import time
import pytz
from datetime import datetime

# ── CONFIG ───────────────────────────────────────────────────────────────────
jakarta_tz = pytz.timezone("Asia/Jakarta")
TOKEN   = st.secrets.get("TELEGRAM_TOKEN", "")
CHAT_ID = st.secrets.get("TELEGRAM_CHAT_ID", "")
DS_KEY  = st.secrets.get("DATASECTORS_API_KEY", "")
DS_BASE = "https://api.datasectors.com"

for _k, _v in [
    ("scan_results", []), ("last_scan_time", None),
    ("sector_summary", {}), ("wl_results", []),
    ("bsjp_results", []), ("gapup_results", []),
    ("tt_last_sent", set()), ("first_scan_done", False),
]:
    if _k not in st.session_state: st.session_state[_k] = _v

st.set_page_config(layout="wide", page_title="Theta Kraken v1.0",
                   page_icon="🦑", initial_sidebar_state="collapsed")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;600;800&display=swap');
:root{--bg:#05080d;--surface:#090e15;--panel:#0c1220;--border:#162030;--border2:#1e3045;
      --accent:#2dd4bf;--gold:#f59e0b;--green:#10b981;--red:#ef4444;--amber:#f59e0b;
      --purple:#8b5cf6;--blue:#3b82f6;--muted:#3d5470;--text:#94a3b8;--heading:#e2e8f0;
      --orange:#ff7b00;}
html,body,[data-testid="stAppViewContainer"]{background:var(--bg)!important;color:var(--text)!important;font-family:'Syne',sans-serif;}
#MainMenu,footer,header{visibility:hidden;}
[data-testid="stSidebar"]{display:none!important;}
.metric-row{display:flex;gap:8px;margin-bottom:14px;flex-wrap:wrap;}
.metric-card{flex:1;min-width:100px;background:var(--panel);border:1px solid var(--border);border-radius:6px;padding:10px 14px;position:relative;}
.metric-card::before{content:'';position:absolute;top:0;left:0;right:0;height:1px;background:var(--accent);}
.metric-card.green::before{background:var(--green);}
.metric-card.red::before{background:var(--red);}
.metric-card.gold::before{background:var(--gold);}
.metric-card.purple::before{background:var(--purple);}
.metric-card.orange::before{background:var(--orange);}
.metric-lbl{font-family:'Space Mono',monospace;font-size:9px;color:var(--muted);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:4px;}
.metric-val{font-family:'Space Mono',monospace;font-size:22px;font-weight:700;color:var(--heading);line-height:1;}
.metric-sub{font-size:9px;color:var(--muted);margin-top:2px;}
.sector-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:8px;margin-bottom:16px;}
.sector-cell{background:var(--panel);border:1px solid var(--border);border-radius:6px;padding:10px 12px;position:relative;overflow:hidden;}
.sector-cell::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;}
.sector-cell.bull::before{background:var(--green);}
.sector-cell.bear::before{background:var(--red);}
.sector-cell.neut::before{background:var(--muted);}
.sector-name{font-family:'Space Mono',monospace;font-size:9px;color:var(--muted);letter-spacing:1px;text-transform:uppercase;margin-bottom:3px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.sector-ret{font-family:'Space Mono',monospace;font-size:18px;font-weight:700;line-height:1;}
.sector-stats{font-family:'Space Mono',monospace;font-size:9px;color:var(--muted);margin-top:3px;}
.signal-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(270px,1fr));gap:10px;margin-bottom:16px;}
.sig-card{background:var(--panel);border:1px solid var(--border);border-radius:8px;padding:14px;position:relative;overflow:hidden;}
.sig-card.buy{border-color:rgba(16,185,129,.4);background:rgba(16,185,129,.03);}
.sig-card.buy::after{content:'';position:absolute;top:0;left:0;width:3px;height:100%;background:var(--green);}
.sig-card.watch{border-color:rgba(245,158,11,.3);}
.sig-card.watch::after{content:'';position:absolute;top:0;left:0;width:3px;height:100%;background:var(--amber);}
.sig-card.strong{border-color:rgba(16,185,129,.7);background:rgba(16,185,129,.06);box-shadow:0 0 16px rgba(16,185,129,.12);}
.sig-card.strong::after{content:'';position:absolute;top:0;left:0;width:3px;height:100%;background:var(--green);}
.sc-ticker{font-family:'Space Mono',monospace;font-size:16px;font-weight:700;color:var(--heading);}
.sc-sector{font-family:'Space Mono',monospace;font-size:9px;color:var(--muted);margin-bottom:5px;}
.sc-bars{display:flex;gap:2px;margin:6px 0;}
.sc-bar{height:12px;border-radius:2px;}
.sc-bar.f{background:var(--green);}
.sc-bar.f-g{background:var(--gold);}
.sc-bar.e{background:var(--border);}
.sc-stats{display:flex;gap:8px;flex-wrap:wrap;margin-top:5px;}
.sc-stat{font-family:'Space Mono',monospace;font-size:9px;color:var(--muted);}
.sc-stat span{color:var(--text);}
.badge-sb{display:inline-block;padding:2px 8px;border-radius:3px;font-family:'Space Mono',monospace;font-size:9px;font-weight:700;background:rgba(16,185,129,.15);color:var(--green);border:1px solid rgba(16,185,129,.4);}
.badge-b{display:inline-block;padding:2px 8px;border-radius:3px;font-family:'Space Mono',monospace;font-size:9px;font-weight:700;background:rgba(16,185,129,.08);color:var(--green);border:1px solid rgba(16,185,129,.3);}
.badge-w{display:inline-block;padding:2px 8px;border-radius:3px;font-family:'Space Mono',monospace;font-size:9px;font-weight:700;background:rgba(245,158,11,.1);color:var(--amber);border:1px solid rgba(245,158,11,.3);}
.badge-s{display:inline-block;padding:2px 8px;border-radius:3px;font-family:'Space Mono',monospace;font-size:9px;font-weight:700;background:rgba(239,68,68,.1);color:var(--red);border:1px solid rgba(239,68,68,.3);}
.log-box{background:#030508;border:1px solid var(--border);border-radius:4px;padding:8px 12px;font-size:10px;color:var(--muted);max-height:180px;overflow-y:auto;font-family:'Space Mono',monospace;line-height:1.8;}
.hf-header{display:flex;align-items:center;padding:12px 0 10px 0;border-bottom:1px solid var(--border2);margin-bottom:14px;}
.hf-logo{font-family:'Space Mono',monospace;font-size:13px;font-weight:700;color:var(--accent);letter-spacing:3px;text-transform:uppercase;}
.live-badge{display:inline-flex;align-items:center;gap:5px;padding:3px 10px;background:rgba(45,212,191,.06);border:1px solid rgba(45,212,191,.3);border-radius:3px;font-family:'Space Mono',monospace;font-size:9px;color:var(--accent);letter-spacing:1px;margin-left:auto;}
.live-dot{width:5px;height:5px;background:var(--green);border-radius:50%;animation:blink 1s infinite;}
@keyframes blink{0%,100%{opacity:1;}50%{opacity:.2;}}
.section-title{font-family:'Space Mono',monospace;font-size:9px;color:var(--muted);letter-spacing:2px;text-transform:uppercase;border-left:2px solid var(--accent);padding-left:8px;margin:16px 0 8px 0;}
.tape-wrap{overflow:hidden;white-space:nowrap;border-top:1px solid var(--border);border-bottom:1px solid var(--border);padding:4px 0;margin-bottom:12px;background:var(--panel);}
.tape-inner{display:inline-block;animation:marquee 40s linear infinite;}
@keyframes marquee{0%{transform:translateX(0)}100%{transform:translateX(-50%)}}
.tape-item{display:inline-block;margin:0 14px;font-family:'Space Mono',monospace;font-size:9px;}
.tape-item.up{color:var(--green);}.tape-item.dn{color:var(--red);}.tape-item.fl{color:var(--muted);}
button[data-testid="baseButton-primary"]{background:var(--accent)!important;color:#05080d!important;font-family:'Space Mono',monospace!important;font-weight:700!important;border:none!important;}
::-webkit-scrollbar{width:3px;height:3px;}::-webkit-scrollbar-thumb{background:var(--border);border-radius:2px;}
@media(max-width:768px){.signal-grid{grid-template-columns:1fr;}.sector-grid{grid-template-columns:repeat(2,1fr);}}
</style>
""", unsafe_allow_html=True)

# ── SECTOR DATA (dari HF Terminal) ───────────────────────────────────────────
SECTOR_DATA = {
    "ENERGY":        ["ADRO","ITMG","PTBA","HRUM","INDY","MEDC","PGAS","AKRA","BUMI","ELSA","ENRG","DOID","KKGI","RMKE","GEMS","BOSS","DEWA","RAJA","BSSR","MBAP","MYOH","SMMT","AADI"],
    "MATERIALS":     ["ANTM","INCO","MDKA","TPIA","BRPT","SMGR","INTP","INKP","TKIM","MBMA","NCKL","TINS","ESSA","AMMN","ADMG","KRAS","NIKL","CUAN","MLIA","KBLI","VOKS","SCCO"],
    "FINANCIALS":    ["BBCA","BBRI","BMRI","BBNI","BRIS","ARTO","BTPS","BDMN","BNGA","BBTN","MEGA","AGRO","BBYB","MAYA","ADMF","CFIN","PNLF","BJBR","BJTM","NOBU","PNBN"],
    "CONSUMER_NC":   ["UNVR","ICBP","INDF","AMRT","CPIN","JPFA","MYOR","KLBF","SIDO","GGRM","HMSP","AALI","LSIP","CLEO","ROTI","STTP","ULTJ","ADES","BWPT","TAPG","CAMP"],
    "CONSUMER_CY":   ["ACES","MAPI","ERAA","ASII","AUTO","SMSM","FILM","SCMA","MAPA","LPPF","RALS","GJTL","KINO","ASSA","BIRD","GOTO","BUKA","EMTK"],
    "HEALTHCARE":    ["HEAL","MIKA","PRDA","SILO","DGNS","PEHA","TSPC","BMHS","IRRA","SAME","RSGK","DVLA","KAEF"],
    "INFRA_TECH":    ["TLKM","ISAT","EXCL","TOWR","TBIG","WIRG","BELI","MTDL","MLPT","MCAS","DCII","CHIP","LUCK","WIFI"],
    "PROPERTY":      ["BSDE","PWON","SMRA","CTRA","ASRI","PANI","DILD","BKSL","APLN","DUTI","JRPT","LPKR","MDLN","MKPI","MTLA","KIJA","BEST"],
    "INDUSTRY":      ["UNTR","ARNA","HEXA","BMTR","MARK","ALDO","SMSM","AMFG","LION","INTP","SMGR","WTON","SCCO"],
    "TRANSPORT":     ["JSMR","WIKA","PTPP","ADHI","SSIA","WEGE","TOTL","ACST","TMAS","SMDR","NELY","BULL","IPCC","PORT"],
}

# Full emiten list dari semua sektor (deduplicated)
_seen = set()
ALL_STOCKS_RAW = []
for _stocks in SECTOR_DATA.values():
    for _s in _stocks:
        if _s not in _seen:
            _seen.add(_s); ALL_STOCKS_RAW.append(_s)

ALL_STOCKS_YF  = [s + ".JK" for s in ALL_STOCKS_RAW]
STOCK_MAP_YF   = {s + ".JK": s for s in ALL_STOCKS_RAW}
TICKER_TO_SEC  = {s: sec for sec, lst in SECTOR_DATA.items() for s in lst}

# ── KRAKEN ENGINE (dari HedgeFundLibrary) ────────────────────────────────────
class KrakenEngine:
    @staticmethod
    def momentum_3_1(close, window=10):
        """Risk-adjusted momentum — return / std."""
        ret = close.pct_change()
        mom = ret.rolling(window).mean() / ret.rolling(window).std().replace(0, np.nan)
        return float(mom.fillna(0).iloc[-1])

    @staticmethod
    def zscore_sector(ret_1d, sector_avg, sector_std):
        """Z-score saham vs rata-rata sektornya — core HF anomaly detection."""
        if sector_std <= 0: return 0.0
        return (ret_1d - sector_avg) / sector_std

    @staticmethod
    def vol_imbalance(close, volume, window=10):
        """Volume imbalance — smart money proxy."""
        delta_p = close.diff()
        v_up   = pd.Series(np.where(delta_p > 0, volume, 0), index=close.index)
        v_down = pd.Series(np.where(delta_p < 0, volume, 0), index=close.index)
        su = v_up.rolling(window).sum()
        sd = v_down.rolling(window).sum()
        vi = ((su - sd) / (su + sd).replace(0, np.nan)).fillna(0)
        return float(vi.iloc[-1])

    @staticmethod
    def market_impact(close, volume, window=10):
        """Liquidity score — makin kecil makin liquid."""
        impact = (close.pct_change().abs() / (close * volume).replace(0, np.nan)).rolling(window).mean()
        return float((impact * 1e9).fillna(0).iloc[-1])

    @staticmethod
    def atr(high, low, close, window=14):
        tr = pd.concat([high - low,
                        (high - close.shift()).abs(),
                        (low  - close.shift()).abs()], axis=1).max(axis=1)
        return float(tr.rolling(window).mean().iloc[-1])

# ── DS FETCH (optional, untuk FBuy/FSell) ────────────────────────────────────
TF_MAP = {"15m":"15m","1d":"daily","daily":"daily"}

def _find_chartbit(obj, depth=0):
    if depth > 6: return None
    if isinstance(obj, dict):
        if "chartbit" in obj: return obj["chartbit"]
        for v in obj.values():
            r = _find_chartbit(v, depth+1)
            if r: return r
    return None

def fetch_ds_flow(ticker_raw, interval="daily"):
    """Fetch FBuy/FSell dari DataSectors — asing flow."""
    if not DS_KEY: return None, None, None
    t   = ticker_raw.upper().strip()
    tf  = TF_MAP.get(interval, "daily")
    url = f"{DS_BASE}/api/chart-saham/{t}/{tf}/latest?_={int(time.time())}"
    try:
        r = requests.get(url, headers={"X-API-Key": DS_KEY, "Accept": "*/*"}, timeout=10)
        if r.status_code != 200: return None, None, None
        rows = _find_chartbit(r.json())
        if not rows: return None, None, None
        df = pd.DataFrame(rows)
        ren = {'foreign_buy':'FBuy','foreign_sell':'FSell'}
        df.rename(columns={k:v for k,v in ren.items() if k in df.columns}, inplace=True)
        for col in ['FBuy','FSell']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        if 'FBuy' in df.columns and 'FSell' in df.columns:
            fbuy  = float(df['FBuy'].iloc[-1])
            fsell = float(df['FSell'].iloc[-1])
            fnet3 = float(df['FBuy'].tail(3).sum() - df['FSell'].tail(3).sum())
            return fbuy, fsell, fnet3
    except: pass
    return None, None, None

# ── IHSG REGIME ───────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def get_regime():
    try:
        df = yf.download("^JKSE", period="60d", interval="1d", progress=False, auto_adjust=True)
        if df is None or len(df) < 20: return "SIDEWAYS", 0, 0
        close = df["Close"].squeeze().dropna()
        ema20 = float(close.ewm(span=20, adjust=False).mean().iloc[-1])
        ema55 = float(close.ewm(span=55, adjust=False).mean().iloc[-1])
        price = float(close.iloc[-1])
        chg   = float((close.iloc[-1] - close.iloc[-2]) / close.iloc[-2] * 100)
        if   price > ema20 and price > ema55: regime = "GREEN"
        elif price < ema20:                   regime = "RED"
        else:                                 regime = "SIDEWAYS"
        return regime, price, chg
    except:
        return "SIDEWAYS", 0, 0.0

# ════════════════════════════════════════════════════════════════════════════
#  CORE FETCH ENGINE — HF Terminal Style
#  1 BATCH DOWNLOAD semua ticker sekaligus → @cache(ttl=120)
#  Ini kunci utama: tidak kena rate limit karena 1 request, bukan per-ticker
# ════════════════════════════════════════════════════════════════════════════
@st.cache_data(ttl=120)
def fetch_all_daily(tickers_tuple):
    """
    HF Terminal pattern: 1 shot download SEMUA ticker.
    period="30d" interval="1d" → ringan, Yahoo paling toleran.
    TTL 120s = auto-refresh tiap 2 menit tanpa rate limit.
    """
    try:
        raw = yf.download(
            list(tickers_tuple),
            period="30d",
            interval="1d",
            group_by="ticker",
            progress=False,
            auto_adjust=True,
            threads=True,   # HF Terminal pakai threads=True — OK untuk daily batch
        )
        return raw
    except Exception as e:
        st.warning(f"⚠️ Fetch error: {str(e)[:60]}")
        return None

def extract_ticker(raw, ticker_yf, n_total):
    """Extract single ticker dari batch result."""
    try:
        if raw is None or raw.empty: return None
        if n_total == 1:
            df = raw.copy()
            if isinstance(df.columns, pd.MultiIndex):
                l0 = df.columns.get_level_values(0).unique().tolist()
                _ohlcv = {'Open','High','Low','Close','Volume'}
                if any(x in _ohlcv for x in l0): df = df.droplevel(1, axis=1)
                else: df = df.droplevel(0, axis=1)
        else:
            if not isinstance(raw.columns, pd.MultiIndex): return None
            l0 = raw.columns.get_level_values(0).unique().tolist()
            l1 = raw.columns.get_level_values(1).unique().tolist()
            if ticker_yf in l0:   df = raw[ticker_yf].copy()
            elif ticker_yf in l1: df = raw.xs(ticker_yf, axis=1, level=1).copy()
            else: return None
            if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.droplevel(-1)
        rename = {c: c.capitalize() for c in df.columns if c.islower()}
        if rename: df = df.rename(columns=rename)
        required = ['Open','High','Low','Close','Volume']
        if any(c not in df.columns for c in required): return None
        df = df[required].dropna(subset=['Close'])
        df = df[df['Volume'] > 0]
        return df if len(df) >= 5 else None
    except: return None


# ════════════════════════════════════════════════════════════════════════════
#  THETA INDICATORS — diterapkan ke daily data
# ════════════════════════════════════════════════════════════════════════════
def apply_theta_indicators(df):
    """Full Theta Turbo indicators pada daily data."""
    c = df['Close']; v = df['Volume']; h = df['High']; l = df['Low']

    # EMA stack
    df['EMA9']  = c.ewm(span=9,  adjust=False).mean()
    df['EMA21'] = c.ewm(span=21, adjust=False).mean()
    df['EMA50'] = c.ewm(span=50, adjust=False).mean()
    df['EMA200']= c.ewm(span=200,adjust=False).mean()

    # RSI + smooth
    d = c.diff()
    g = d.clip(lower=0).ewm(span=14, adjust=False).mean()
    lo= (-d.clip(upper=0)).ewm(span=14, adjust=False).mean()
    rsi_raw = (100 - 100/(1 + g/lo.replace(0, np.nan))).fillna(50)
    df['RSI']     = rsi_raw
    df['RSI_EMA'] = rsi_raw.ewm(span=3, adjust=False).mean()

    # Stochastic
    lo14 = l.rolling(14).min(); hi14 = h.rolling(14).max()
    raw_k = (100*(c - lo14)/(hi14 - lo14).replace(0, np.nan)).fillna(50)
    df['STOCH_K'] = raw_k.ewm(span=5, adjust=False).mean()
    df['STOCH_D'] = df['STOCH_K'].ewm(span=3, adjust=False).mean()

    # MACD
    ema12 = c.ewm(span=12, adjust=False).mean()
    ema26 = c.ewm(span=26, adjust=False).mean()
    ml = ema12 - ema26
    ms = ml.ewm(span=9, adjust=False).mean()
    df['MACD_Hist'] = (ml - ms).fillna(0)

    # VWAP (rolling)
    tp = (h + l + c) / 3
    df['VWAP'] = (tp * v).rolling(14).sum() / v.rolling(14).sum()

    # Bollinger
    df['BB_mid'] = c.rolling(20).mean()
    df['BB_std'] = c.rolling(20).std()
    df['BB_up']  = df['BB_mid'] + 2*df['BB_std']
    df['BB_lo']  = df['BB_mid'] - 2*df['BB_std']
    df['BB_pct'] = ((c - df['BB_lo'])/(df['BB_up'] - df['BB_lo'])).clip(0, 1)

    # RVOL
    df['AvgVol'] = v.rolling(20).mean()
    df['RVOL']   = v / df['AvgVol'].replace(0, np.nan)

    # ATR
    tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    df['ATR'] = tr.rolling(14).mean()

    # Net volume
    df['NetVol']  = np.where(c >= c.shift(), v, -v)
    df['NetVol3'] = pd.Series(df['NetVol'], index=df.index).rolling(3).sum()
    df['NetVol8'] = pd.Series(df['NetVol'], index=df.index).rolling(8).sum()

    # Returns
    df['Ret1D'] = c.pct_change(1) * 100
    df['Ret5D'] = c.pct_change(5) * 100

    return df

# ════════════════════════════════════════════════════════════════════════════
#  COMBINED SCORING — Theta + Kraken + HF Z-score
#  Score 0–100 (HF style) dengan breakdown:
#  • Theta momentum   (0–30): EMA stack, MACD, VWAP, NetVol
#  • HF Z-score       (0–40): anomaly vs sektor → reversal detection
#  • Volume smart     (0–25): RVOL + vol imbalance
#  • RSI zone         (0–15): oversold/momentum
#  • Kraken signals   (0–10): momentum_3_1, market_impact bonus
# ════════════════════════════════════════════════════════════════════════════
def score_combined(df, r, p, p2, sector_avg, sector_std, scan_mode):
    """
    Hybrid scoring: Theta indicators + HF sector Z-score + Kraken engine.
    Returns score (0-100), signal, reasons.
    """
    score = 0; reasons = []
    close = float(r['Close'])

    # ── 1. HF Z-SCORE ANOMALY (0–40) — core dari HF Terminal ──
    ret1d = float(r.get('Ret1D', 0))
    z     = KrakenEngine.zscore_sector(ret1d, sector_avg, sector_std)

    if scan_mode in ["Reversal 🎯", "Anomaly 📡"]:
        # Z-score negatif = laggard vs sektor = reversal candidate
        if   z < -2.5: score += 40; reasons.append(f"Z-score {z:.1f} ⚡⚡ ekstrem laggard")
        elif z < -2.0: score += 32; reasons.append(f"Z-score {z:.1f} ⚡ anomali kuat")
        elif z < -1.5: score += 24; reasons.append(f"Z-score {z:.1f} reversal zone")
        elif z < -1.0: score += 16; reasons.append(f"Z-score {z:.1f} watch")
        elif z >  2.0: score -= 10; reasons.append(f"⚠️ Z-score {z:.1f} OB vs sektor")
    else:
        # Momentum mode: z positif = outperform sektor
        if   z >  2.0: score += 20; reasons.append(f"Z-score {z:.1f} outperform ✦")
        elif z >  1.0: score += 12; reasons.append(f"Z-score {z:.1f} above sector")
        elif z >  0.5: score += 6;  reasons.append(f"Z-score {z:.1f} slight outperform")
        elif z < -2.0: score -= 8

    # ── 2. THETA MOMENTUM (0–30) ──
    e9=float(r['EMA9']); e21=float(r['EMA21']); e50=float(r['EMA50'])
    mh=float(r['MACD_Hist']); mh_p=float(p['MACD_Hist'])

    if   e9>e21>e50:  score+=20; reasons.append("EMA stack ▲ ✦")
    elif e9>e21:      score+=12; reasons.append("EMA9>21")
    elif e9<e21<e50:  score-=8

    if mh > 0 and mh > mh_p:
        score += 10; reasons.append("MACD hist expanding ✦")
    elif mh > 0:
        score += 5; reasons.append("MACD +")
    elif mh < 0 and mh > mh_p:
        score += 3; reasons.append("MACD diverging ↑")

    if close > float(r['VWAP']): score += 5; reasons.append("Above VWAP")
    if float(r['NetVol3']) > 0:  score += 5; reasons.append("Net vol +")
    elif float(r['NetVol8']) > 0: score += 2

    # ── 3. VOLUME SMART MONEY (0–25) ──
    rvol = float(r['RVOL'])
    vi   = KrakenEngine.vol_imbalance(df['Close'], df['Volume'])

    if   rvol >= 5.0: score += 25; reasons.append(f"RVOL {rvol:.1f}x MASSIVE 🔥🔥")
    elif rvol >= 3.0: score += 20; reasons.append(f"RVOL {rvol:.1f}x SURGE 🔥")
    elif rvol >= 2.0: score += 14; reasons.append(f"RVOL {rvol:.1f}x kuat")
    elif rvol >= 1.5: score += 8;  reasons.append(f"RVOL {rvol:.1f}x")
    elif rvol >= 1.2: score += 3
    elif rvol < 0.7:  score -= 5

    if   vi > 0.4:  score += 5; reasons.append(f"Vol imbalance {vi:.2f} bullish ✦")
    elif vi > 0.2:  score += 3
    elif vi < -0.4: score -= 5; reasons.append(f"⚠️ Vol imbalance {vi:.2f} bearish")

    # ── 4. RSI ZONE (0–15) ──
    rsi_e = float(r['RSI_EMA'])
    if scan_mode in ["Reversal 🎯", "Anomaly 📡"]:
        if   rsi_e < 25:  score += 15; reasons.append(f"RSI-EMA {rsi_e:.0f} extreme OS ✦")
        elif rsi_e < 32:  score += 12; reasons.append(f"RSI-EMA {rsi_e:.0f} OS")
        elif rsi_e < 42:  score += 7;  reasons.append(f"RSI-EMA {rsi_e:.0f} watch zone")
        elif rsi_e > 72:  score -= 8;  reasons.append(f"⚠️ RSI-EMA {rsi_e:.0f} OB")
    else:
        if   40 <= rsi_e <= 60: score += 12; reasons.append(f"RSI-EMA {rsi_e:.0f} momentum ✓")
        elif 60 <  rsi_e <= 70: score += 8;  reasons.append(f"RSI-EMA {rsi_e:.0f} hot zone")
        elif rsi_e > 75:        score -= 8;  reasons.append(f"⚠️ RSI OB {rsi_e:.0f}")
        elif rsi_e < 35:        score += 5;  reasons.append(f"RSI-EMA {rsi_e:.0f} rebound zone")

    # ── 5. KRAKEN MOMENTUM ENGINE (0–10 bonus) ──
    kraken_m = KrakenEngine.momentum_3_1(df['Close'])
    if   kraken_m > 1.5: score += 10; reasons.append(f"Kraken mom {kraken_m:.2f} ✦✦")
    elif kraken_m > 0.8: score += 6;  reasons.append(f"Kraken mom {kraken_m:.2f} ✦")
    elif kraken_m > 0.3: score += 3
    elif kraken_m < -0.5: score -= 5

    # ── 6. BB ROOM ──
    bb_pct = float(r['BB_pct'])
    if   bb_pct < 0.2: score += 8; reasons.append("BB squeeze low — banyak ruang ✦")
    elif bb_pct < 0.4: score += 4
    elif bb_pct > 0.85: score -= 5; reasons.append("⚠️ BB near upper")

    # ── 7. SEKTOR CONTEXT BONUS ──
    if sector_avg > 0.5:
        score += 8; reasons.append(f"Sektor bullish {sector_avg:.1f}%")
    elif sector_avg > 0:
        score += 4
    elif sector_avg < -1:
        score -= 5; reasons.append(f"⚠️ Sektor bearish {sector_avg:.1f}%")

    score = int(max(0, min(100, score)))

    # ── SIGNAL MAPPING ──
    if scan_mode in ["Reversal 🎯", "Anomaly 📡"]:
        if score >= 70 and rvol >= 2.0: signal = "STRONG BUY 🔥"; cls = "strong"
        elif score >= 55:                signal = "BUY ⚡";        cls = "buy"
        elif score >= 40:                signal = "WATCH 👀";      cls = "watch"
        else:                            signal = "AVOID ❄️";      cls = "avoid"
    elif scan_mode == "Bagger 💎":
        if score >= 70 and rvol >= 1.5: signal = "BAGGER 💎";    cls = "strong"
        elif score >= 55:                signal = "KANDIDAT 🚀";  cls = "buy"
        elif score >= 40:                signal = "WATCH 👀";     cls = "watch"
        else:                            signal = "AVOID ❄️";     cls = "avoid"
    else:  # Scalping / Momentum
        if score >= 75 and rvol >= 2.0: signal = "STRONG BUY 🔥"; cls = "strong"
        elif score >= 55:                signal = "BUY ⚡";        cls = "buy"
        elif score >= 40:                signal = "WATCH 👀";      cls = "watch"
        else:                            signal = "AVOID ❄️";      cls = "avoid"

    return score, signal, cls, reasons, z, kraken_m, vi


# ════════════════════════════════════════════════════════════════════════════
#  MAIN ANALYSIS ENGINE — HF batch fetch + Theta + Kraken scoring
# ════════════════════════════════════════════════════════════════════════════
def run_analysis(raw_data, tickers_yf, scan_mode, min_score, min_rvol, min_turn):
    """
    Analyze dari cached batch data.
    raw_data = hasil yf.download(semua_ticker) — sudah di-cache TTL 120s.
    """
    n_total = len(tickers_yf)

    # ── Step 1: Build per-sector stats (HF Z-score calculation) ──
    sector_stats = {}  # {sector: {avg, std, stocks_data}}
    per_ticker_data = {}

    log_lines = []; log_ph = st.empty()
    pb_ph = st.progress(0)
    status_ph = st.empty()
    status_ph.markdown(
        '<div style="font-family:Space Mono,monospace;font-size:10px;color:#2dd4bf;">⬡ Processing sector Z-scores...</div>',
        unsafe_allow_html=True
    )

    for i, t_yf in enumerate(tickers_yf):
        pb_ph.progress((i+1) / n_total)
        t_raw = STOCK_MAP_YF.get(t_yf, t_yf.replace(".JK",""))
        sector = TICKER_TO_SEC.get(t_raw, "OTHER")

        df = extract_ticker(raw_data, t_yf, n_total)
        if df is None or len(df) < 10: continue

        try:
            df = apply_theta_indicators(df)
            r   = df.iloc[-1]
            ret1d = float(r.get('Ret1D', 0))
            per_ticker_data[t_yf] = {"df": df, "sector": sector, "ret1d": ret1d}

            if sector not in sector_stats:
                sector_stats[sector] = []
            sector_stats[sector].append(ret1d)
        except: continue

    # Calculate sector avg/std
    sector_params = {}
    for sec, rets in sector_stats.items():
        arr = np.array(rets)
        avg = float(np.mean(arr)) if len(arr) > 0 else 0
        std = float(np.std(arr))  if len(arr) > 1 else 0.001
        med = float(np.median(arr)) if len(arr) > 0 else 0
        bull = sum(1 for r in arr if r > 0)
        bear = sum(1 for r in arr if r < 0)
        sector_params[sec] = {"avg":avg,"std":std,"med":med,"bull":bull,"bear":bear,"total":len(arr)}

    # ── Step 2: Score each ticker ──
    status_ph.markdown(
        '<div style="font-family:Space Mono,monospace;font-size:10px;color:#2dd4bf;">⬡ Running Theta+Kraken scoring engine...</div>',
        unsafe_allow_html=True
    )

    results = []; n_proc = len(per_ticker_data)
    for i, (t_yf, td) in enumerate(per_ticker_data.items()):
        pb_ph.progress((i+1) / max(n_proc, 1))
        t_raw  = STOCK_MAP_YF.get(t_yf, t_yf.replace(".JK",""))
        sector = td["sector"]
        df     = td["df"]
        sp     = sector_params.get(sector, {"avg":0,"std":0.001})

        try:
            r  = df.iloc[-1]; p = df.iloc[-2]; p2 = df.iloc[-3] if len(df)>=3 else p
            close   = float(r['Close'])
            vol     = float(r['Volume'])
            rvol    = float(r['RVOL'])
            turnover= close * vol

            if rvol < min_rvol or turnover < min_turn: continue

            score, signal, cls, reasons, z, kraken_m, vi = score_combined(
                df, r, p, p2, sp["avg"], sp["std"], scan_mode
            )

            if score < min_score or "AVOID" in signal: continue

            # ATR TP/SL
            atr_v = float(r['ATR']) if not np.isnan(float(r['ATR'])) else close * 0.02
            if scan_mode == "Reversal 🎯":
                tp = close + 3.0*atr_v; sl = close - 1.2*atr_v
            elif scan_mode == "Bagger 💎":
                tp = close + 4.0*atr_v; sl = close - 1.5*atr_v
            else:
                tp = close + 2.5*atr_v; sl = close - 1.2*atr_v
            rr = (tp-close) / max(close-sl, 0.01)

            e9=float(r['EMA9']); e21=float(r['EMA21']); e50=float(r['EMA50'])
            trend = "▲ UP" if e9>e21>e50 else ("▼ DOWN" if e9<e21<e50 else "◆ SIDE")

            # DS asing flow (optional — jika DS_KEY ada)
            fdir = "—"; fc = "#3d5470"
            if DS_KEY:
                try:
                    fb, fs, fn3 = fetch_ds_flow(t_raw, "daily")
                    if fb is not None:
                        if fn3 > 0:  fdir = "🔵 BELI"; fc = "#4da6ff"
                        elif fn3 < 0: fdir = "🔴 JUAL"; fc = "#ef4444"
                        else:          fdir = "⚪ MIX";  fc = "#6b7280"
                except: pass

            # Log
            icon = "🔥" if "STRONG" in signal else ("⚡" if "BUY" in signal else "👀")
            log_lines.append(
                f'<span style="color:{"#10b981" if "STRONG" in signal else "#f59e0b" if "BUY" in signal else "#3d5470"}">'
                f'[{datetime.now(jakarta_tz).strftime("%H:%M:%S")}] {icon} {t_raw} | '
                f'score:{score} | z:{z:.1f} | rvol:{rvol:.1f}x | {signal}</span>'
            )
            log_ph.markdown(
                f'<div class="log-box">{"<br>".join(log_lines[-10:])}</div>',
                unsafe_allow_html=True
            )

            results.append({
                "Ticker":     t_raw,
                "Sektor":     sector,
                "Price":      int(close),
                "Score":      score,
                "Signal":     signal,
                "_cls":       cls,
                "Trend":      trend,
                "Z-Score":    round(z, 2),
                "1D%":        round(float(r.get('Ret1D',0)), 2),
                "5D%":        round(float(r.get('Ret5D',0)), 2),
                "RSI-EMA":    round(float(r['RSI_EMA']), 1),
                "Stoch K":    round(float(r['STOCH_K']), 1),
                "MACD Hist":  round(float(r['MACD_Hist']), 4),
                "RVOL":       round(rvol, 2),
                "Vol Imbal":  round(vi, 3),
                "BB%":        round(float(r['BB_pct']), 2),
                "Kraken Mom": round(kraken_m, 3),
                "VWAP":       int(float(r['VWAP'])),
                "TP":         int(tp),
                "SL":         int(sl),
                "R:R":        round(rr, 1),
                "Turnover(B)":round(turnover/1e9, 3),
                "FDir":       fdir,
                "FC":         fc,
                "Sector Avg": round(sp["avg"], 2),
            })
        except: continue

    pb_ph.empty(); status_ph.empty(); log_ph.empty()
    return results, sector_params

# ── TELEGRAM ─────────────────────────────────────────────────────────────────
def send_tele(results_top):
    if not TOKEN or not CHAT_ID or not results_top: return
    now=datetime.now(jakarta_tz); sep="━"*26
    msg=(f"🦑 *THETA KRAKEN — AUTO SCAN*\n"
         f"⏰ `{now.strftime('%H:%M:%S')} WIB · {now.strftime('%d %b %Y')}`\n{sep}\n")
    for i,r in enumerate(results_top[:5],1):
        sig=r['Signal']; bar="█"*int(r['Score']//10)+"░"*(10-int(r['Score']//10))
        msg+=(f"\n{'🔥' if 'STRONG' in sig else '⚡'} *#{i} {r['Ticker']}* `{sig}`\n"
              f"   💰 `Rp{r['Price']:,}` | Z:`{r['Z-Score']:+.1f}` | Sector:`{r['Sektor']}`\n"
              f"   📊 `[{bar}] {r['Score']}/100`\n"
              f"   RVOL:`{r['RVOL']}x` | RSI:`{r['RSI-EMA']}` | 1D:`{r['1D%']:+.1f}%`\n"
              f"   🎯 TP:`{r['TP']:,}` | SL:`{r['SL']:,}` | R:R:`{r['R:R']}`\n"
              f"   Asing: {r.get('FDir','—')}\n")
    msg+=f"\n{sep}\n⚠️ _Bukan saran investasi. DYOR!_"
    try:
        requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                      data={"chat_id":CHAT_ID,"text":msg,"parse_mode":"Markdown"},timeout=10)
    except: pass


# ════════════════════════════════════════════════════════════════════════════
#  HEADER + CONTROLS
# ════════════════════════════════════════════════════════════════════════════
regime, ihsg_price, ihsg_chg = get_regime()
now_jkt  = datetime.now(jakarta_tz)
rcolor   = {"GREEN":"#10b981","RED":"#ef4444","SIDEWAYS":"#f59e0b"}.get(regime,"#3d5470")
chg_col  = "#10b981" if ihsg_chg >= 0 else "#ef4444"
chg_sym  = "▲" if ihsg_chg >= 0 else "▼"

st.markdown(f"""
<div class="hf-header">
  <div>
    <div class="hf-logo">🦑 THETA KRAKEN v1.0</div>
    <div style="font-family:Space Mono,monospace;font-size:9px;color:#3d5470;letter-spacing:2px;text-transform:uppercase;margin-top:2px;">
      HF Fetch · Theta Indicators · Kraken Engine · Sector Z-Score · Auto-Refresh 2min
    </div>
  </div>
  <div class="live-badge">
    <div class="live-dot"></div>
    {'DS+yF' if DS_KEY else 'yF'} · LIVE {now_jkt.strftime("%H:%M:%S")} WIB
  </div>
</div>""", unsafe_allow_html=True)

# IHSG regime bar
st.markdown(f"""
<div style="background:rgba(0,0,0,.3);border:1px solid {rcolor}44;border-radius:6px;
     padding:8px 14px;margin-bottom:12px;border-left:3px solid {rcolor};display:flex;
     justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;">
  <div>
    <div style="font-family:Space Mono,monospace;font-size:10px;font-weight:700;color:{rcolor};">
      {'🟢' if regime=='GREEN' else '🔴' if regime=='RED' else '🟡'} IHSG REGIME: {regime}
    </div>
    <div style="font-size:9px;color:#3d5470;margin-top:2px;">
      Fetch: 1-shot batch daily · @cache ttl=120s · No rate limit
    </div>
  </div>
  <div style="font-family:Space Mono,monospace;font-size:15px;font-weight:700;color:{rcolor};">
    {ihsg_price:,.0f} <span style="font-size:10px;color:{chg_col}">{chg_sym}{abs(ihsg_chg):.2f}%</span>
  </div>
</div>""", unsafe_allow_html=True)

# ── CONTROLS ──────────────────────────────────────────────────────────────────
c1,c2,c3,c4 = st.columns([2,2,2,1])
with c1:
    scan_mode = st.radio(
        "Mode", ["Scalping/Momentum ⚡","Reversal 🎯","Anomaly 📡","Bagger 💎"],
        index={"GREEN":0,"RED":1,"SIDEWAYS":0}.get(regime,0),
        horizontal=True, key="scan_mode"
    )
    # Recommend based on regime
    rec = {"GREEN":"Scalping/Momentum ⚡","RED":"Reversal 🎯","SIDEWAYS":"Anomaly 📡"}.get(regime,"Scalping/Momentum ⚡")
    st.caption(f"🤖 Regime suggest: {rec}")
with c2:
    min_score = st.slider("Min Score (0-100)", 0, 100,
                          {"GREEN":45,"RED":55,"SIDEWAYS":40}.get(regime,40), 5, key="min_score")
    min_rvol  = st.slider("Min RVOL", 0.5, 5.0, 1.2, 0.1, key="min_rvol")
with c3:
    min_turn  = st.number_input("Min Turnover (B Rp)", 0.0, 100.0, 0.5, 0.1, key="min_turn") * 1e9
    scan_size = st.radio("Pool", ["LQ45 ⚡","Sektor 🏭","Full 🦅"], index=1, horizontal=True, key="scan_size")
    tele_auto = st.toggle("📡 Auto Telegram", value=False, key="tele_auto")
with c4:
    st.markdown("<br><br>", unsafe_allow_html=True)
    rescan = st.button("🔄 RE-SCAN", type="primary", use_container_width=True, key="btn_rescan")

# ── SECTOR FILTER ─────────────────────────────────────────────────────────────
LQ45 = ["BBCA","BBRI","BMRI","TLKM","ASII","GOTO","BYAN","MDKA","UNVR","ICBP",
         "INDF","KLBF","SIDO","MNCN","EXCL","TOWR","PGAS","PTBA","ADRO","ITMG",
         "INCO","HRUM","JSMR","SMGR","WIKA","WSKT","ANTM","PTPP","TBIG","BRIS",
         "AMMN","MBMA","CUAN","BBNI","BBTN","BMTR","INKP","BRPT","TPIA","BREN",
         "EMTK","PANI","DSSA","ISAT","BBCA"]

if "LQ45" in scan_size:
    pool_raw = LQ45
elif "Sektor" in scan_size:
    active_secs = st.multiselect(
        "Filter Sektor", list(SECTOR_DATA.keys()),
        default=list(SECTOR_DATA.keys()), key="sec_filter",
        label_visibility="collapsed"
    )
    pool_raw = list(dict.fromkeys(s for sec in active_secs for s in SECTOR_DATA.get(sec,[])))
else:
    pool_raw = ALL_STOCKS_RAW

pool_yf  = [s+".JK" for s in pool_raw]
pool_key = f"{scan_size}_{scan_mode}_{len(pool_raw)}"

# ════════════════════════════════════════════════════════════════════════════
#  FETCH — HF Terminal pattern
#  1 shot batch download → @cache(ttl=120) → NO rate limit
# ════════════════════════════════════════════════════════════════════════════
_first_run = not st.session_state.first_scan_done
_pool_changed = st.session_state.get("last_pool_key","") != pool_key

fetch_ph = st.empty()
if _first_run or rescan or _pool_changed:
    fetch_ph.markdown(
        f'<div style="font-family:Space Mono,monospace;font-size:10px;color:#2dd4bf;">'
        f'⬡ Fetching {len(pool_yf)} emiten — 1 batch download (HF style)...</div>',
        unsafe_allow_html=True
    )

raw_data = fetch_all_daily(tuple(pool_yf))
fetch_ph.empty()

if raw_data is None:
    st.error("⚠️ Fetch gagal. Cek koneksi internet.")
    st.stop()

# ════════════════════════════════════════════════════════════════════════════
#  AUTO-RUN — IDX Life + HF Terminal pattern
# ════════════════════════════════════════════════════════════════════════════
if _first_run or rescan or _pool_changed:
    st.session_state.first_scan_done = True
    st.session_state.last_pool_key   = pool_key
    st.markdown(
        f'<div style="font-family:Space Mono,monospace;font-size:10px;color:#f59e0b;'
        f'padding:6px 10px;background:#090e15;border-radius:4px;margin-bottom:8px;">'
        f'⬡ AUTO-ANALYSIS {len(pool_yf)} emiten · {scan_mode} · '
        f'{"Pertama buka app" if _first_run else "Re-scan"}</div>',
        unsafe_allow_html=True
    )
    results, sector_params = run_analysis(
        raw_data, pool_yf, scan_mode, min_score, min_rvol, min_turn
    )
    st.session_state.scan_results    = results
    st.session_state.sector_summary  = sector_params
    st.session_state.last_scan_time  = now_jkt.strftime("%H:%M:%S")

    if tele_auto and results:
        top5 = [r for r in results if "STRONG" in r.get("Signal","") or "BUY" in r.get("Signal","")]
        if top5: send_tele(top5[:5])

results       = st.session_state.scan_results
sector_params = st.session_state.sector_summary

if not results:
    st.info("⬡ Tidak ada signal yang lolos filter. Turunkan Min Score atau Min RVOL.")
    st.stop()

# ════════════════════════════════════════════════════════════════════════════
#  DISPLAY
# ════════════════════════════════════════════════════════════════════════════
df_res = pd.DataFrame(results)
strong = df_res[df_res["Signal"].str.contains("STRONG", na=False)]
buys   = df_res[df_res["Signal"].str.contains("BUY",    na=False)]
watch  = df_res[df_res["Signal"].str.contains("WATCH",  na=False)]
avg_z  = df_res["Z-Score"].mean()
avg_rsi= df_res["RSI-EMA"].mean()

# ── Metrics ──────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="metric-row">
  <div class="metric-card" style="border-top-color:{rcolor}">
    <div class="metric-lbl">Regime</div>
    <div class="metric-val" style="font-size:15px;color:{rcolor}">{regime}</div>
    <div class="metric-sub">IHSG {ihsg_price:,.0f} {chg_sym}{abs(ihsg_chg):.2f}%</div>
  </div>
  <div class="metric-card green">
    <div class="metric-lbl">STRONG BUY 🔥</div>
    <div class="metric-val">{len(strong)}</div>
    <div class="metric-sub">score≥70 + rvol≥2x</div>
  </div>
  <div class="metric-card" style="border-top-color:#10b981">
    <div class="metric-lbl">BUY ⚡</div>
    <div class="metric-val">{len(buys)}</div>
  </div>
  <div class="metric-card" style="border-top-color:#f59e0b">
    <div class="metric-lbl">WATCH 👀</div>
    <div class="metric-val">{len(watch)}</div>
  </div>
  <div class="metric-card orange">
    <div class="metric-lbl">Dipindai</div>
    <div class="metric-val">{len(df_res)}</div>
    <div class="metric-sub">dari {len(pool_raw)} emiten</div>
  </div>
  <div class="metric-card" style="border-top-color:#8b5cf6">
    <div class="metric-lbl">Avg Z-Score</div>
    <div class="metric-val" style="color:{'#10b981' if avg_z>0 else '#ef4444'}">{avg_z:+.2f}</div>
    <div class="metric-sub">vs sektor</div>
  </div>
  <div class="metric-card">
    <div class="metric-lbl">Last Scan</div>
    <div class="metric-val" style="font-size:13px;color:#2dd4bf">{st.session_state.get('last_scan_time','--')}</div>
    <div class="metric-sub">WIB · TTL 120s</div>
  </div>
</div>""", unsafe_allow_html=True)

# ── Sector Heatmap (HF Terminal style) ───────────────────────────────────────
if sector_params:
    st.markdown('<div class="section-title">Sector Heatmap — Rotation Monitor</div>', unsafe_allow_html=True)
    heat = '<div class="sector-grid">'
    for sec, sp in sorted(sector_params.items(), key=lambda x: x[1]["avg"], reverse=True):
        avg=sp["avg"]; bull=sp["bull"]; bear=sp["bear"]; tot=sp["total"]
        cls="bull" if avg>0.2 else("bear" if avg<-0.2 else"neut")
        col="#10b981" if avg>0.2 else("#ef4444" if avg<-0.2 else"#3d5470")
        sym="▲" if avg>0 else "▼"
        bp=int(bull/tot*100) if tot>0 else 0
        heat+=f"""<div class="sector-cell {cls}">
          <div class="sector-name">{sec.replace('_',' ')}</div>
          <div class="sector-ret" style="color:{col}">{sym}{abs(avg):.2f}%</div>
          <div style="height:3px;background:#162030;border-radius:2px;overflow:hidden;margin:4px 0;">
            <div style="width:{bp}%;height:100%;background:{col};border-radius:2px;display:inline-block;"></div>
          </div>
          <div class="sector-stats">▲{bull} ▼{bear} of {tot}</div>
        </div>"""
    heat += '</div>'
    st.markdown(heat, unsafe_allow_html=True)

# ── Ticker Tape ───────────────────────────────────────────────────────────────
tape = '<div class="tape-wrap"><div class="tape-inner">'
for _, row in df_res.iterrows():
    chg=row["1D%"]; cls="up" if chg>0 else("dn" if chg<0 else"fl")
    sym="🔥" if "STRONG" in row["Signal"] else("⚡" if "BUY" in row["Signal"] else "👀")
    tape+=f'<span class="tape-item {cls}">{row["Ticker"]} {row["Price"]:,} {sym}{abs(chg):.1f}% z:{row["Z-Score"]:+.1f} [{row["Score"]}]</span>'
tape = tape*2 + '</div></div>'
st.markdown(tape, unsafe_allow_html=True)

# ── Signal Cards ──────────────────────────────────────────────────────────────
st.markdown('<div class="section-title">🏆 Top Signal Cards</div>', unsafe_allow_html=True)
card_html = '<div class="signal-grid">'
for _, row in df_res.sort_values("Score", ascending=False).head(20).iterrows():
    sc=row["Score"]; filled=int(sc//10)
    # Score bar: hijau kalau score>=70, gold kalau 40-70
    bar_cls="f" if sc>=70 else "f-g"
    bars=''.join([f'<div class="sc-bar {bar_cls if i<filled else "e"}" style="width:22px"></div>' for i in range(10)])
    chg_c="#10b981" if row["1D%"]>0 else "#ef4444"
    z_c  ="#10b981" if row["Z-Score"]<-1 else("#ef4444" if row["Z-Score"]>1.5 else "#6b7280")
    fc   = row.get("FC","#3d5470")
    fdir = row.get("FDir","—")
    sig  = row["Signal"]
    badge_cls="badge-sb" if "STRONG" in sig else("badge-b" if "BUY" in sig else "badge-w")
    card_html+=f"""<div class="sig-card {row['_cls']}">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;">
        <div>
          <div class="sc-ticker">{row['Ticker']}</div>
          <div class="sc-sector">{row['Sektor'].replace('_',' ')} · {row['Trend']}</div>
        </div>
        <div style="text-align:right;">
          <div style="font-family:Space Mono,monospace;font-size:8px;color:#3d5470">SCORE</div>
          <div style="font-family:Space Mono,monospace;font-size:20px;font-weight:700;color:{'#10b981' if sc>=70 else '#f59e0b' if sc>=40 else '#6b7280'}">{sc}</div>
        </div>
      </div>
      <div style="margin:5px 0"><span class="{badge_cls}">{sig}</span></div>
      <div style="font-family:Space Mono,monospace;font-size:11px;color:{chg_c};margin-bottom:4px;">Rp{row['Price']:,} {'+' if row['1D%']>0 else ''}{row['1D%']:.1f}% (1D)</div>
      <div class="sc-bars">{bars}</div>
      <div class="sc-stats">
        <div class="sc-stat">Z <span style="color:{z_c}">{row['Z-Score']:+.1f}</span></div>
        <div class="sc-stat">RVOL <span>{row['RVOL']}x</span></div>
        <div class="sc-stat">RSI <span>{row['RSI-EMA']}</span></div>
        <div class="sc-stat">BB% <span>{row['BB%']:.2f}</span></div>
      </div>
      <div class="sc-stats" style="margin-top:4px;">
        <div class="sc-stat">TP <span style="color:#10b981">Rp{row['TP']:,}</span></div>
        <div class="sc-stat">SL <span style="color:#ef4444">Rp{row['SL']:,}</span></div>
        <div class="sc-stat">R:R <span>{row['R:R']}</span></div>
        <div class="sc-stat">Asing <span style="color:{fc}">{fdir}</span></div>
      </div>
      <div style="margin-top:5px;font-size:9px;color:#3d5470;font-family:Space Mono,monospace;">
        Kraken Mom: {row.get('Kraken Mom',0):.3f} · Vol Imbal: {row.get('Vol Imbal',0):.2f} · Sec Avg: {row.get('Sector Avg',0):+.2f}%
      </div>
    </div>"""
card_html += '</div>'
st.markdown(card_html, unsafe_allow_html=True)

# ── Full Table ────────────────────────────────────────────────────────────────
st.markdown('<div class="section-title">Full Signal Table</div>', unsafe_allow_html=True)
show_cols = ["Ticker","Sektor","Price","Score","Signal","Trend","1D%","5D%","Z-Score",
             "RSI-EMA","Stoch K","MACD Hist","RVOL","Vol Imbal","BB%","Kraken Mom",
             "VWAP","TP","SL","R:R","Turnover(B)","FDir","Sector Avg"]
show_cols = [c for c in show_cols if c in df_res.columns]
st.dataframe(
    df_res[show_cols].sort_values("Score", ascending=False),
    width="stretch", hide_index=True,
    column_config={
        "Score":      st.column_config.ProgressColumn("Score", min_value=0, max_value=100, format="%.0f"),
        "1D%":        st.column_config.NumberColumn("1D%",     format="%.2f%%"),
        "5D%":        st.column_config.NumberColumn("5D%",     format="%.2f%%"),
        "Z-Score":    st.column_config.NumberColumn("Z-Score", format="%.2f"),
        "RVOL":       st.column_config.NumberColumn("RVOL",    format="%.2fx"),
        "Turnover(B)":st.column_config.NumberColumn("Turnover(B)", format="Rp%.2fB"),
        "FDir":       st.column_config.TextColumn("Asing Flow"),
        "Sector Avg": st.column_config.NumberColumn("Sec Avg%", format="%.2f%%"),
    }
)

# ── Telegram manual ───────────────────────────────────────────────────────────
st.markdown("---")
tc1, tc2 = st.columns([4,1])
with tc1:
    st.markdown(
        f'<div style="font-family:Space Mono,monospace;font-size:9px;color:#3d5470;">'
        f'⬡ {len(strong)} STRONG · {len(buys)} BUY · {len(watch)} WATCH · '
        f'Telegram: {"✅ active" if TOKEN else "❌ set secrets"} · '
        f'DS: {"✅ active" if DS_KEY else "❌ yF only"}</div>',
        unsafe_allow_html=True
    )
with tc2:
    if st.button("📲 Kirim Telegram", use_container_width=True, key="btn_tele"):
        top = df_res[df_res["Signal"].str.contains("STRONG|BUY",na=False)].head(5).to_dict("records")
        if top: send_tele(top); st.success("✅ Sent!")

# ── Watchlist Quick Analyze ───────────────────────────────────────────────────
st.markdown("---")
st.markdown('<div class="section-title">👁️ Watchlist Deep Dive</div>', unsafe_allow_html=True)
wl_in = st.text_input("Ticker (pisah koma)", placeholder="BBCA, ARCI, ASSA", key="wl_in")
if wl_in.strip():
    wl_tickers = [t.strip().upper()+".JK" for t in wl_in.split(",") if t.strip()]
    with st.spinner(f"Fetching {len(wl_tickers)} ticker..."):
        wl_raw = fetch_all_daily(tuple(wl_tickers))
    if wl_raw is not None:
        wl_res = []
        for t_yf in wl_tickers:
            t_raw = t_yf.replace(".JK","")
            sector = TICKER_TO_SEC.get(t_raw, "OTHER")
            sp = sector_params.get(sector, {"avg":0,"std":0.001})
            df = extract_ticker(wl_raw, t_yf, len(wl_tickers))
            if df is None or len(df) < 10: continue
            try:
                df = apply_theta_indicators(df)
                r  = df.iloc[-1]; p = df.iloc[-2]; p2 = df.iloc[-3] if len(df)>=3 else p
                close  = float(r['Close'])
                rvol   = float(r['RVOL'])
                turn   = close * float(r['Volume'])
                score, signal, cls, reasons, z, km, vi = score_combined(
                    df, r, p, p2, sp["avg"], sp["std"], scan_mode
                )
                atr_v = float(r['ATR']) if not np.isnan(float(r['ATR'])) else close*0.02
                tp = close + 2.5*atr_v; sl = close - 1.2*atr_v
                rr = (tp-close)/max(close-sl,0.01)
                e9=float(r['EMA9']); e21=float(r['EMA21']); e50=float(r['EMA50'])
                trend="▲ UP" if e9>e21>e50 else("▼ DOWN" if e9<e21<e50 else"◆ SIDE")
                wl_res.append({
                    "Ticker":t_raw,"Sektor":sector,"Price":int(close),
                    "Score":score,"Signal":signal,"Trend":trend,
                    "Z-Score":round(z,2),"1D%":round(float(r.get('Ret1D',0)),2),
                    "RSI-EMA":round(float(r['RSI_EMA']),1),"RVOL":round(rvol,2),
                    "MACD Hist":round(float(r['MACD_Hist']),4),
                    "BB%":round(float(r['BB_pct']),2),"Kraken Mom":round(km,3),
                    "TP":int(tp),"SL":int(sl),"R:R":round(rr,1),
                    "Reasons":" · ".join(reasons[:3])
                })
            except: continue
        if wl_res:
            df_wl = pd.DataFrame(wl_res).sort_values("Score", ascending=False)
            st.dataframe(df_wl, width="stretch", hide_index=True)

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style="margin-top:20px;padding-top:10px;border-top:1px solid #162030;
     display:flex;justify-content:space-between;flex-wrap:wrap;gap:6px;">
  <div style="font-family:Space Mono,monospace;font-size:9px;color:#3d5470;">
    🦑 Theta Kraken v1.0 · HF Fetch (no rate limit) · Theta Indicators · Kraken Engine · Sector Z-Score
  </div>
  <div style="font-family:Space Mono,monospace;font-size:9px;color:#3d5470;">
    Last: {st.session_state.get('last_scan_time','--')} WIB · {len(pool_raw)} emiten · @cache ttl=120s
  </div>
</div>
<div style="font-family:Space Mono,monospace;font-size:8px;color:#1e3045;text-align:center;margin-top:6px;">
  ⚠️ BUKAN saran investasi · Daily data (delayed) · DYOR selalu
</div>""", unsafe_allow_html=True)
