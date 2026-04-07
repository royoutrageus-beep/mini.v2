import yfinance as yf
import pandas as pd
import streamlit as st
import time
import requests
import numpy as np
import pytz
from datetime import datetime

# ─────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────
TOKEN      = st.secrets["TELEGRAM_TOKEN"]
CHAT_ID    = st.secrets["TELEGRAM_CHAT_ID"]
jakarta_tz = pytz.timezone('Asia/Jakarta')

st.set_page_config(
    layout="wide",
    page_title="Theta Turbo v4",
    page_icon="🔥",
    initial_sidebar_state="collapsed",
)

# ─────────────────────────────────────────────
#  CSS — sama tema QuantEdge
# ─────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;600;800&display=swap');
:root {
    --bg:#080c10; --surface:#0d1117; --border:#1c2533;
    --accent:#00e5ff; --green:#00ff88; --red:#ff3d5a;
    --amber:#ffb700; --purple:#bf5fff; --orange:#ff7b00;
    --muted:#4a5568; --text:#c9d1d9; --heading:#e6edf3;
}
html,body,[data-testid="stAppViewContainer"]{
    background:var(--bg)!important; color:var(--text)!important;
    font-family:'Syne',sans-serif;
}
#MainMenu,footer,header{visibility:hidden;}
[data-testid="stSidebar"]{display:none!important;}

[data-testid="stExpander"]{
    background:var(--surface)!important; border:1px solid var(--border)!important;
    border-radius:8px!important; margin-bottom:12px!important;
}
[data-testid="stExpander"] summary{
    font-family:'Space Mono',monospace!important; font-size:12px!important;
    color:var(--accent)!important; letter-spacing:1px!important;
}
.settings-label{
    font-family:'Space Mono',monospace; font-size:10px; color:var(--muted);
    letter-spacing:2px; margin-bottom:10px; padding-bottom:6px;
    border-bottom:1px solid var(--border);
}

/* Header */
.tt-header{
    display:flex; align-items:center; padding:16px 0 12px 0;
    border-bottom:1px solid var(--border); margin-bottom:16px;
}
.tt-logo{font-family:'Space Mono',monospace; font-size:22px; font-weight:700; color:var(--orange); letter-spacing:-1px;}
.tt-sub{font-size:11px; color:var(--muted); letter-spacing:2px; text-transform:uppercase;}

/* Live badge */
.live-badge{
    display:inline-flex; align-items:center; gap:6px;
    padding:4px 12px; background:rgba(0,229,255,.08);
    border:1px solid rgba(0,229,255,.3); border-radius:20px;
    font-family:'Space Mono',monospace; font-size:10px; color:var(--accent);
    letter-spacing:1px; margin-left:auto;
}
.live-dot{
    width:6px; height:6px; background:var(--green); border-radius:50%;
    animation:blink 1s infinite;
}
@keyframes blink{0%,100%{opacity:1;}50%{opacity:.2;}}

/* Metric cards */
.metric-row{display:flex; gap:10px; margin-bottom:18px; flex-wrap:wrap;}
.metric-card{
    flex:1; min-width:110px; background:var(--surface);
    border:1px solid var(--border); border-radius:8px;
    padding:12px 14px; position:relative; overflow:hidden;
}
.metric-card::before{content:''; position:absolute; top:0; left:0; right:0; height:2px; background:var(--accent);}
.metric-card.green::before{background:var(--green);}
.metric-card.red::before{background:var(--red);}
.metric-card.amber::before{background:var(--amber);}
.metric-card.orange::before{background:var(--orange);}
.metric-label{font-size:10px; color:var(--muted); letter-spacing:1.5px; text-transform:uppercase; margin-bottom:4px;}
.metric-value{font-family:'Space Mono',monospace; font-size:24px; font-weight:700; color:var(--heading); line-height:1;}
.metric-sub{font-size:10px; color:var(--muted); margin-top:3px;}

/* Signal cards */
.signal-grid{display:grid; grid-template-columns:repeat(auto-fill,minmax(280px,1fr)); gap:12px; margin-bottom:20px;}
.signal-card{
    background:var(--surface); border:1px solid var(--border);
    border-radius:10px; padding:16px; position:relative; overflow:hidden;
    transition:border-color .2s;
}
.signal-card.gacor{border-color:rgba(0,255,136,.4); background:rgba(0,255,136,.03);}
.signal-card.potensial{border-color:rgba(255,183,0,.3); background:rgba(255,183,0,.03);}
.signal-card.watch{border-color:rgba(0,229,255,.2);}
.signal-card::after{
    content:''; position:absolute; top:0; left:0; width:4px; height:100%;
}
.signal-card.gacor::after{background:var(--green);}
.signal-card.potensial::after{background:var(--amber);}
.signal-card.watch::after{background:var(--accent);}

.sc-ticker{font-family:'Space Mono',monospace; font-size:18px; font-weight:700; color:var(--heading);}
.sc-price{font-family:'Space Mono',monospace; font-size:13px; color:var(--muted);}
.sc-signal{font-size:13px; font-weight:700; margin:6px 0;}
.sc-bars{display:flex; gap:3px; margin:8px 0;}
.sc-bar{height:16px; border-radius:2px; transition:width .3s;}
.sc-bar.filled{background:var(--green);}
.sc-bar.empty{background:var(--border);}
.sc-bar.amber{background:var(--amber);}
.sc-stats{display:flex; gap:12px; flex-wrap:wrap; margin-top:8px;}
.sc-stat{font-family:'Space Mono',monospace; font-size:10px; color:var(--muted);}
.sc-stat span{color:var(--text);}

/* Alert box */
.alert-box{
    background:rgba(255,61,90,.06); border:1px solid rgba(255,61,90,.4);
    border-radius:8px; padding:14px 18px; margin-bottom:16px;
    animation:pulse-border 2s infinite;
}
@keyframes pulse-border{0%,100%{border-color:rgba(255,61,90,.4);}50%{border-color:rgba(255,61,90,.9);}}
.alert-title{color:var(--red); font-family:'Space Mono',monospace; font-size:12px; font-weight:700; letter-spacing:2px;}

/* Ticker tape */
.tape-wrap{
    overflow:hidden; white-space:nowrap; border-top:1px solid var(--border);
    border-bottom:1px solid var(--border); padding:5px 0; margin-bottom:16px;
    background:var(--surface);
}
.tape-inner{display:inline-block; animation:marquee 35s linear infinite;}
@keyframes marquee{0%{transform:translateX(0)}100%{transform:translateX(-50%)}}
.tape-item{display:inline-block; margin:0 18px; font-family:'Space Mono',monospace; font-size:10px;}
.tape-item.up{color:var(--green);} .tape-item.down{color:var(--red);} .tape-item.flat{color:var(--muted);}

/* Table */
[data-testid="stDataFrame"]{border:1px solid var(--border)!important; border-radius:8px!important;}
[data-testid="stDataFrame"] thead th{
    background:var(--surface)!important; color:var(--muted)!important;
    font-family:'Space Mono',monospace!important; font-size:11px!important;
    letter-spacing:1px!important; text-transform:uppercase!important;
}
::-webkit-scrollbar{width:4px; height:4px;}
::-webkit-scrollbar-track{background:var(--bg);}
::-webkit-scrollbar-thumb{background:var(--border); border-radius:2px;}
[data-testid="stNumberInput"] input{
    background:var(--surface)!important; border:1px solid var(--border)!important;
    color:var(--heading)!important; font-family:'Space Mono',monospace!important; border-radius:6px!important;
}
button[data-testid="baseButton-primary"]{
    background:var(--orange)!important; color:var(--bg)!important;
    font-family:'Space Mono',monospace!important; font-weight:700!important; border:none!important;
}
.section-title{
    font-family:'Space Mono',monospace; font-size:11px; color:var(--muted);
    letter-spacing:2px; text-transform:uppercase; border-left:3px solid var(--orange);
    padding-left:10px; margin:20px 0 10px 0;
}
@media(max-width:768px){
    .main .block-container{padding-left:.75rem!important; padding-right:.75rem!important;}
    .signal-grid{grid-template-columns:1fr;}
}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
#  STOCK LIST
# ─────────────────────────────────────────────
raw_stocks = [
    "AALI","ABBA","ABDA","ABMM","ACES","ACST","ADCP","ADES","ADHI","ADMF","ADMG","ADMR","ADRO","AGII","AGRO","AGRS",
    "AHAP","AIMS","AISA","AKPI","AKRA","AKSI","ALDO","ALKA","ALMI","ALRE","AMAG","AMAR","AMFG","AMIN","AMMS","AMOR",
    "AMRT","ANDI","ANJT","ANTM","APEX","APIC","APLI","APLN","ARCI","ARGO","ARII","ARKA","ARKO","ARMY","ARNA","ARTA",
    "ARTI","ARTO","ASBI","ASCL","ASDM","ASGR","ASII","ASJT","ASLC","ASMI","ASPI","ASRI","ASRM","ASSA","ATAP","ATIC",
    "ATLI","AUTO","AVIA","AWAN","AXIO","AYLS","BABP","BACA","BAIC","BAPA","BAPI","BARI","BATA","BATU","BAYU","BBCA",
    "BBHI","BBKP","BBLD","BBMD","BBNI","BBRI","BBRM","BBSI","BBSS","BBTN","BBYB","BCAP","BCIC","BCIP","BDMN","BEBS",
    "BEEF","BEER","BELI","BESS","BEST","BFIN","BGTG","BHIT","BIAS","BVIC","BIKA","BIMA","BINA","BIPI","BIPP","BIRD",
    "BISI","BJBR","BJTM","BKDP","BKSL","BKSW","BLTA","BLTZ","BLUE","BMAS","BMBL","BMRI","BMTR","BNBA","BNGA","BNII",
    "BNLI","BOBA","BOGA","BOKE","BOLA","BORO","BOSS","BPFI","BPII","BPTR","BRAM","BRIS","BRMS","BRNA","BRPT","BSDE",
    "BSIM","BSML","BSSR","BSWD","BTEK","BTEL","BTON","BTPS","BUDI","BUKK","BULL","BUMI","BUVA","BWPT","BYAN","CAKK",
    "CAMP","CARS","CASH","CASS","CASY","CBRE","CEKA","CENT","CERE","CESS","CFIN","CHIP","CINT","CITA","CITY","CLAY",
    "CLEO","CLPI","CMNT","CMPP","CMRY","CNKO","CNMA","CNTX","COAL","COCO","CPIN","CPRI","CPRO","CSAP","CSIS","CSMI",
    "CSRA","CTBN","CTRA","CTRP","CTRS","CTTH","CUAN","DADA","DAJK","DART","DAYA","DEAL","DEFI","DEIT","DEWA","DFAM",
    "DGIK","DGNS","DIGI","DILD","DIVA","DKFT","DLTA","DMMX","DMND","DMSX","DMTX","DNAR","DNET","DOID","DPNS","DPUM",
    "DRMA","DSSA","DSST","DUCK","DUTI","DVLA","DWGL","DYAN","EAST","ECII","EDII","EKAD","ELIT","ELPI","ELSA","ELTY",
    "EMAS","EMTK","ENRG","EPAC","EPMT","ERAA","ERTX","ESIP","ESSA","ESTA","ESTI","ETWA","EURO","EVIT","EXCL","FAPA",
    "FAST","FASW","FEST","FIFA","FIMP","FIRE","FISH","FITT","FLMC","FMII","FORU","FORZ","FPNI","FREN","FUAD","FWCT",
    "GAMA","GDST","GDYR","GEAS","GEMA","GEMS","GGRM","GGRP","GHON","GIAA","GJTL","GLOB","GLVA","GMCU","GMTD","GOLD",
    "GOOD","GOTO","GPRA","GPSO","GRIA","GRPM","GSMF","GTBO","GWSA","GZCO","HADE","HAIS","HALO","HATM","HDFA","HDIT",
    "HEAL","HELI","HERO","HEXA","HHPW","HIAM","HITS","HKMU","HMSP","HOKI","HOME","HOPE","HOTL","HRTA","HRUM","IATA",
    "IBFN","IBOS","ICBP","ICON","IDPR","IFII","IFSH","IGAR","IIKP","IKAI","IKAN","IKBI","IMAS","IMJS","IMPC","INAF",
    "INAI","INCF","INCO","INDF","INDO","INET","INFN","INFO","INPC","INPP","INPS","INRU","INSG","INTA","INTD","INTP",
    "IPAC","IPCC","IPCM","IPPE","IPTV","IRRA","ISAP","ISAT","ISIG","ISSP","ITIC","ITMA","ITMG","JAST","JATI","JAVA",
    "JECC","JGLE","JIHD","JKON","JKSW","JMAS","JPFA","JRPT","JSMR","JSPT","JTPE","KAEF","KARY","KAYU","KBAG","KBLI",
    "KBLM","KBLV","KBMD","KDSI","KEEN","KEJU","KIAS","KICI","KIJA","KING","KINO","KIOS","KJEN","KKGI","KLAS","KLBF",
    "KMDS","KMTR","KOBX","KOIN","KOKA","KOKI","KONI","KOPI","KOTA","KPAS","KPIG","KRAH","KRAS","KREN","LAAW","LABA",
    "LAND","LAPD","LCGP","LCKM","LEAD","LIFE","LION","LPCK","LPGI","LPIN","LPKR","LPLI","LPPS","LPPF","LRNA","LSIP",
    "LTLS","LUCK","LUCY","MABA","MAGP","MAHA","MAIN","MAMI","MAPA","MAPB","MAPI","MARI","MARK","MASA","MAYA","MBAP",
    "MBMA","MBSS","MBTO","MCAS","MCOL","MCOR","MDIA","MDKA","MDKI","MDLN","MDPP","MEDC","MEGA","MENN","METI","METR",
    "METS","MFIN","MFMI","MGNA","MICE","MIDI","MIKA","MINA","MIRA","MITI","MKAP","MKNT","MKPI","MLBI","MLIA","MLMS",
    "MLPL","MLPT","MMIX","MNCN","MPMX","MPPA","MPRO","MRAT","MREI","MSIN","MSKY","MTDL","MTEL","MTFN","MTLA","MTMH",
    "MTPS","MTRA","MTSM","MYOH","MYOR","MYPZ","MYRX","MYTX","NANO","NASA","NARE","NATO","NELY","NETV","NFCX","NICK",
    "NICL","NIRO","NISM","NKEF","NKIT","NLMS","NOBU","NPGF","NRCA","NSSS","NTBK","NUSA","NVAM","NZIA","OASA","OBMD",
    "OCAP","OCAS","OCDM","OKAS","OLIV","OMRE","OPMS","PADI","PAFI","PAMG","PANI","PANR","PANS","PANT","PARD","PARE",
    "PBID","PBRX","PBSA","PCAR","PDES","PEGE","PEHA","PELI","PESS","PGAS","PGEO","PGUN","PICO","PJAA","PKPK","PLIN",
    "PLNB","PLSN","PMJS","PMMP","PNBS","PNIN","PNLF","PNSE","POLA","POLL","POLU","POLY","POOL","PORT","POWR","PPGL",
    "PPRE","PPRO","PRAS","PRDA","PRIM","PSAB","PSDN","PSGO","PSKT","PSSI","PTBA","PTDU","PTIS","PTPW","PTRO","PTSN",
    "PTSP","PUDP","PURA","PURE","PURI","PWON","PYFA","RAAM","RACY","RAJA","RALS","RANC","RBMS","RCCC","RELI","REMA",
    "RGAS","RICY","RIGS","RIMO","RISE","RMKE","RMKO","RODA","RONI","ROTI","SAFE","SAME","SAMF","SAMI","SANK","SANT",
    "SAPX","SBAT","SBMA","SCCO","SCMA","SCNP","SCRB","SDMU","SDPC","SDRA","SEMA","SGER","SGRO","SHID","SHIP","SIAP",
    "SILO","SIMA","SIMP","SINI","SIPD","SKBM","SKLT","SKYB","SLIS","SMAR","SMBR","SMCB","SMDM","SMDR","SMGR","SMMA",
    "SMMT","SMRA","SMRU","SMSM","SNLK","SOFA","SOHO","SONA","SOSS","SOTS","SPMA","SPTO","SQMI","SRAJ","SRIL","SRTG",
    "SSIA","SSMS","SSTM","STAA","STTP","SUGI","SULI","SUMI","SUNU","SUPR","SURE","SURV","SUTI","SWAT","SWID","TAMA",
    "TAMU","TARA","TAXI","TBIG","TBLA","TBMS","TCID","TCOA","TCPI","TEBE","TECC","TECH","TELE","TFAS","TFCO","TGKA",
    "TGUK","TIFA","TINS","TIRA","TIRT","TKIM","TLDN","TLKM","TMAS","TMPO","TNCA","TOBA","TOOL","TOTA","TOWR","TPAI",
    "TPMA","TRGU","TRIL","TRIM","TRIN","TRIS","TRJA","TRST","TRUE","TRUK","TRUS","TSPC","TUGU","TULI","TYRE","UANG",
    "UCID","UNIC","UNIT","UNSP","UNTR","UNVR","URBN","UVCR","VICI","VICO","VINS","VIPT","VIVA","VOKS","VOMR","VTNY",
    "WAPO","WEGE","WEHA","WICO","WIDI","WIFI","WIGL","WIKA","WIKI","WIMM","WINE","WINS","WIRG","WITA","WMUU","WOOD",
    "WOWS","WSBP","WSKT","WTON","YELO","YPAS","YULE","ZATA","ZBRA","ZINC"
]
seen = set(); raw_stocks = [x for x in raw_stocks if not (x in seen or seen.add(x))]
stocks_yf = [s + ".JK" for s in raw_stocks]
stock_map  = {s + ".JK": s for s in raw_stocks}

# ─────────────────────────────────────────────
#  HEADER
# ─────────────────────────────────────────────
now_jkt = datetime.now(jakarta_tz)
st.markdown(f"""
<div class="tt-header">
  <div>
    <div class="tt-logo">🔥 THETA TURBO</div>
    <div class="tt-sub">Intraday 15M Scanner · Scalping & Momentum Engine v4.0</div>
  </div>
  <div class="live-badge">
    <div class="live-dot"></div>
    LIVE {now_jkt.strftime("%H:%M:%S")} WIB
  </div>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
#  SETTINGS
# ─────────────────────────────────────────────
with st.expander("⚙️  Scanner Settings", expanded=False):
    sc1, sc2, sc3 = st.columns(3)
    with sc1:
        st.markdown('<div class="settings-label">MODE SIGNAL</div>', unsafe_allow_html=True)
        scan_mode    = st.radio("Mode", ["Scalping ⚡","Momentum 🚀","Reversal 🎯"], label_visibility="collapsed")
        tele_on      = st.toggle("📡 Telegram Alert", value=True)
    with sc2:
        st.markdown('<div class="settings-label">FILTER</div>', unsafe_allow_html=True)
        min_score    = st.slider("Min Score (0-6)", 0, 6, 4, key="msc")
        vol_thresh   = st.slider("Min RVOL Spike", 1.0, 5.0, 1.5, 0.1, key="vol")
        min_turn     = st.number_input("Min Turnover (M Rp)", value=500, step=100, key="trn") * 1_000_000
    with sc3:
        st.markdown('<div class="settings-label">TAMPILAN</div>', unsafe_allow_html=True)
        view_mode    = st.radio("View", ["Card View 🃏","Table View 📊"], label_visibility="collapsed")
        st.caption("🔄 Auto-refresh tiap 300 detik")
        st.caption(f"📊 {len(raw_stocks)} emiten dipantau")
        st.caption("⏱️ Data interval: 15 menit")

# ─────────────────────────────────────────────
#  INDICATOR ENGINE (dioptimasi untuk 15M)
# ─────────────────────────────────────────────
def ema(s, n): return s.ewm(span=n, adjust=False).mean()

def rsi_smooth(s, p=14, smooth=3):
    """RSI di-smooth EMA — kurangi noise di timeframe pendek"""
    delta = s.diff()
    gain  = delta.clip(lower=0).rolling(p).mean()
    loss  = (-delta.clip(upper=0)).rolling(p).mean()
    rs    = gain / loss.replace(0, np.nan)
    raw   = 100 - 100/(1+rs)
    return raw, ema(raw, smooth)  # raw + smooth version

def stochastic(h, l, c, k=14, d=3):
    """Stochastic %K/%D — deteksi reversal dari oversold/overbought"""
    ll = l.rolling(k).min()
    hh = h.rolling(k).max()
    K  = 100*(c-ll)/(hh-ll).replace(0,np.nan)
    D  = K.rolling(d).mean()
    return K.fillna(50), D.fillna(50)

def macd(s, f=12, sl=26, sg=9):
    ml  = ema(s,f) - ema(s,sl)
    sig = ema(ml,sg)
    return ml, sig, ml-sig

def vwap(df):
    """VWAP — benchmark price intraday, kunci support/resistance"""
    tp  = (df['High'] + df['Low'] + df['Close']) / 3
    return (tp * df['Volume']).cumsum() / df['Volume'].cumsum()

def apply_intraday_indicators(df):
    # EMA pendek untuk scalping
    df['EMA9']   = ema(df['Close'], 9)
    df['EMA21']  = ema(df['Close'], 21)
    df['EMA50']  = ema(df['Close'], 50)
    df['EMA200'] = ema(df['Close'], 200)

    # RSI + RSI-EMA
    df['RSI'], df['RSI_EMA'] = rsi_smooth(df['Close'], 14, 3)

    # Stochastic
    df['STOCH_K'], df['STOCH_D'] = stochastic(df['High'], df['Low'], df['Close'], 14, 3)

    # MACD
    df['MACD'], df['MACD_Sig'], df['MACD_Hist'] = macd(df['Close'])

    # VWAP
    try:
        df['VWAP'] = vwap(df)
    except:
        df['VWAP'] = df['Close']

    # Bollinger Bands
    df['BB_mid']   = df['Close'].rolling(20).mean()
    df['BB_std']   = df['Close'].rolling(20).std()
    df['BB_upper'] = df['BB_mid'] + 2*df['BB_std']
    df['BB_lower'] = df['BB_mid'] - 2*df['BB_std']
    df['BB_pct']   = (df['Close']-df['BB_lower'])/(df['BB_upper']-df['BB_lower'])

    # Volume
    df['AvgVol']   = df['Volume'].rolling(20).mean()
    df['RVOL']     = df['Volume'] / df['AvgVol']

    # Net Volume Flow (15M)
    df['NetVol']   = np.where(df['Close'] >= df['Open'], df['Volume'], -df['Volume'])
    df['NetVol3']  = pd.Series(df['NetVol'], index=df.index).rolling(3).sum()
    df['NetVol8']  = pd.Series(df['NetVol'], index=df.index).rolling(8).sum()

    # Volume climax — lonjakan tiba-tiba
    df['VolSpike'] = df['RVOL'] > 2.5

    # Candle
    df['Body']      = (df['Close'] - df['Open']).abs()
    df['BodyRatio'] = df['Body'] / (df['High'] - df['Low']).replace(0, np.nan)
    df['BullBar']   = (df['Close'] > df['Open']) & (df['BodyRatio'] > 0.5)

    # Price momentum
    df['ROC3']  = df['Close'].pct_change(3)
    df['ROC8']  = df['Close'].pct_change(8)

    # Higher Highs / Higher Lows (trend intraday)
    df['HH'] = df['High'] > df['High'].shift(1)
    df['HL'] = df['Low']  > df['Low'].shift(1)
    df['LL'] = df['Low']  < df['Low'].shift(1)
    df['LH'] = df['High'] < df['High'].shift(1)

    return df

# ─────────────────────────────────────────────
#  SCORING ENGINE (per mode)
# ─────────────────────────────────────────────
def score_scalping(r, p, p2):
    """
    SCALPING — sinyal cepat 15M
    Focus: EMA9/21 stack + MACD hist + volume spike + momentum candle
    Max score: 6
    """
    score=0; reasons=[]; details={}

    # 1. EMA stack (tren mikro)
    if r['EMA9']>r['EMA21']>r['EMA50']:
        score+=1.5; reasons.append("EMA stack ▲"); details['ema']='bullish'
    elif r['EMA9']>r['EMA21']:
        score+=0.8; reasons.append("EMA9>21"); details['ema']='partial'
    else:
        details['ema']='bearish'

    # 2. Price di atas VWAP (intraday bias)
    if r['Close']>r['VWAP']:
        score+=1; reasons.append("Above VWAP"); details['vwap']='above'
    else:
        details['vwap']='below'

    # 3. MACD histogram expanding positive
    if r['MACD_Hist']>0 and r['MACD_Hist']>float(p['MACD_Hist']):
        score+=1.5; reasons.append("MACD hist expanding ✦")
        if p2 is not None and float(p['MACD_Hist'])>float(p2['MACD_Hist']):
            score+=0.3; reasons.append("MACD 3 bar rising")
    elif r['MACD_Hist']>0:
        score+=0.5; reasons.append("MACD hist +")

    # 4. RSI-EMA zona momentum (50-70, bukan overbought)
    rsi_e = float(r['RSI_EMA'])
    if 52<rsi_e<68:
        score+=0.8; reasons.append(f"RSI-EMA={rsi_e:.1f}"); details['rsi']='momentum'
    elif rsi_e>=68:
        score-=0.5; details['rsi']='overbought'
    else:
        details['rsi']='weak'

    # 5. Volume spike konfirmasi
    rvol = float(r['RVOL'])
    if rvol>2.0:
        score+=1; reasons.append(f"RVOL={rvol:.1f}x surge")
    elif rvol>1.5:
        score+=0.6; reasons.append(f"RVOL={rvol:.1f}x")

    # 6. Bullish candle body
    if bool(r['BullBar']):
        score+=0.5; reasons.append("Bullish bar")

    # 7. Net volume positif
    if float(r['NetVol3'])>0:
        score+=0.4; reasons.append("Net vol +")

    # Disqualifier: harga di bawah EMA200 di 15M
    if r['Close']<r['EMA200']*0.98:
        score-=0.5

    return max(0,min(6,round(score,1))), reasons, details


def score_momentum(r, p, p2):
    """
    MOMENTUM — breakout dan trend continuation
    Focus: Breakout high, volume, stochastic, ROC
    Max score: 6
    """
    score=0; reasons=[]; details={}

    # 1. Breakout: Higher High + Higher Low (3 bar)
    hh = bool(r['HH']); hl = bool(r['HL'])
    if hh and hl:
        score+=1.5; reasons.append("HH+HL pattern ▲"); details['trend']='strong'
    elif hh:
        score+=0.8; details['trend']='moderate'
    else:
        details['trend']='weak'

    # 2. Volume surge besar
    rvol=float(r['RVOL'])
    if rvol>3.0:   score+=1.5; reasons.append(f"RVOL={rvol:.1f}x SURGE 🔥")
    elif rvol>2.0: score+=1.0; reasons.append(f"RVOL={rvol:.1f}x")
    elif rvol>1.5: score+=0.5

    # 3. ROC kuat (accelerasi harga)
    roc = float(r['ROC3'])*100
    if roc>2.0:   score+=1.5; reasons.append(f"ROC3={roc:.1f}%")
    elif roc>1.0: score+=0.8; reasons.append(f"ROC3={roc:.1f}%")
    elif roc<0:   score-=0.5

    # 4. RSI-EMA zone momentum
    rsi_e=float(r['RSI_EMA'])
    if 55<rsi_e<75: score+=0.8; reasons.append(f"RSI-EMA={rsi_e:.1f}")
    if rsi_e>78: score-=0.8; reasons.append("⚠️ RSI overbought")
    details['rsi_e']=rsi_e

    # 5. Stochastic momentum zone
    sk=float(r['STOCH_K']); sd=float(r['STOCH_D'])
    if sk>60 and sk>sd:
        score+=0.8; reasons.append("STOCH K>D bullish")

    # 6. MACD
    if r['MACD_Hist']>0 and r['MACD_Hist']>float(p['MACD_Hist']):
        score+=0.8; reasons.append("MACD expanding")

    # 7. Above VWAP
    if r['Close']>r['VWAP']:
        score+=0.5; reasons.append("Above VWAP")

    return max(0,min(6,round(score,1))), reasons, details


def score_reversal(r, p, p2):
    """
    REVERSAL — tangkap pembalikan dari oversold intraday
    Focus: Stochastic cross + RSI-EMA pivot + volume climax + BB lower
    Max score: 6
    """
    score=0; reasons=[]; details={}

    # 1. Oversold multi-konfirmasi
    os_count=0
    rsi_e=float(r['RSI_EMA'])
    if rsi_e<30:   os_count+=1; score+=1.5; reasons.append(f"RSI-EMA={rsi_e:.1f} OS extreme")
    elif rsi_e<40: os_count+=1; score+=0.8; reasons.append(f"RSI-EMA={rsi_e:.1f} OS")

    sk=float(r['STOCH_K']); sd=float(r['STOCH_D'])
    if sk<20:  os_count+=1; score+=1; reasons.append(f"STOCH={sk:.0f} extreme OS")
    elif sk<30: os_count+=1; score+=0.5

    bp=float(r['BB_pct'])
    if bp<0.05:  os_count+=1; score+=1; reasons.append("BB lower touch")
    elif bp<0.15: os_count+=1; score+=0.5

    # Gate: belum oversold cukup
    if os_count<1.5: return 0,[],{}

    # 2. Reversal konfirmasi — WAJIB ada
    rev=0
    pk=float(p['STOCH_K']); pd_=float(p['STOCH_D'])
    if sk<30 and sk>sd and pk<=pd_:
        rev+=1; score+=2; reasons.append("STOCH %K cross ↑ OS ✦✦")
    elif sk<25 and sk>sd:
        rev+=1; score+=1.2; reasons.append("STOCH K>D extreme OS")

    if p is not None:
        rsi_p=float(p['RSI_EMA'])
        if rsi_e>rsi_p and rsi_e<42:
            rev+=1; score+=1.2; reasons.append("RSI-EMA pivot ↑")

    mh=float(r['MACD_Hist']); mh_p=float(p['MACD_Hist'])
    if mh>mh_p and mh<0:
        rev+=1; score+=0.8; reasons.append("MACD hist diverge ↑")

    # Gate: tidak ada reversal → penalti
    if rev==0: score*=0.3

    # 3. Volume exhaustion
    if bool(r['VolSpike']) and float(r['Close'])<float(r['Open']):
        score+=0.8; reasons.append("Volume climax sell")
    elif float(r['RVOL'])>1.5:
        score+=0.4

    # 4. Net volume turning
    if float(r['NetVol3'])>0:
        score+=0.5; reasons.append("Net vol turning +")

    # Bearish candle kuat = belum waktunya
    if float(r['BodyRatio'])>0.75 and float(r['Close'])<float(r['Open']):
        score-=0.8; reasons.append("⚠️ Bearish bar kuat")

    details['os_count']=os_count; details['rev']=rev
    return max(0,min(6,round(score,1))), reasons, details


def get_signal(score, mode):
    thresholds = {
        "Scalping ⚡":  {5:"GACOR ⚡",4:"POTENSIAL 🔥",3:"WATCH 👀"},
        "Momentum 🚀":  {5:"GACOR 🚀",4:"POTENSIAL 🔥",3:"WATCH 👀"},
        "Reversal 🎯":  {5:"REVERSAL 🎯",4:"POTENSIAL 🔥",3:"WATCH 👀"},
    }
    t = thresholds.get(mode, {})
    for thresh in sorted(t.keys(), reverse=True):
        if score >= thresh: return t[thresh]
    return "WAIT"

def get_card_class(signal):
    if "GACOR" in signal or "REVERSAL" in signal: return "gacor"
    if "POTENSIAL" in signal: return "potensial"
    if "WATCH" in signal: return "watch"
    return ""

# ─────────────────────────────────────────────
#  TELEGRAM — format eye-catching
# ─────────────────────────────────────────────
def send_telegram_alert(results_top):
    now = datetime.now(jakarta_tz)
    is_open = 9 <= now.hour < 16

    header = (
        f"{'🔴 MARKET OPEN' if is_open else '🌙 AFTER HOURS'}\n"
        f"🔥 *THETA TURBO ALERT*\n"
        f"⏰ `{now.strftime('%H:%M:%S')} WIB` · `{now.strftime('%d %b %Y')}`\n"
        f"{'━'*28}\n"
    )

    body = ""
    for r in results_top[:5]:  # max 5 per blast
        sig = r['Signal']
        emoji = "🏆" if "GACOR" in sig else ("🔥" if "POTENSIAL" in sig else "👀")
        trend_e = "📈" if "▲" in r.get('Trend','') else ("📉" if "▼" in r.get('Trend','') else "➡️")

        # Score bar visual
        score_int = int(r['Score'])
        bar = "█"*score_int + "░"*(6-score_int)

        body += (
            f"\n{emoji} *{r['Ticker']}*  `{sig}`\n"
            f"   💰 Price: `{r['Price']:,}` {trend_e}\n"
            f"   📊 Score: `[{bar}] {r['Score']}/6`\n"
            f"   📈 RSI-EMA: `{r['RSI-EMA']}` | STOCH: `{r['Stoch K']}`\n"
            f"   🌊 RVOL: `{r['RVOL']}x` | MACD: `{r['MACD Hist']}`\n"
            f"   🎯 TP: `{r['TP']:,}` | 🛑 SL: `{r['SL']:,}` | R:R `{r['R:R']}`\n"
            f"   💡 _{r['Reasons'][:60]}_\n"
        )

    footer = (
        f"\n{'━'*28}\n"
        f"⚡ _Theta Turbo v4 · 15M Intraday_\n"
        f"⚠️ _BUKAN saran investasi. DYOR!_"
    )

    full_msg = header + body + footer
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": CHAT_ID, "text": full_msg, "parse_mode": "Markdown"}, timeout=10)
    except Exception as e:
        st.error(f"Telegram error: {e}")


def send_telegram_gacor(row):
    """Alert khusus score = 6 — kirim langsung, high priority"""
    now  = datetime.now(jakarta_tz)
    score_bar = "█"*6

    msg = (
        f"🚨🚨 *SCORE PENUH — {row['Ticker']}* 🚨🚨\n"
        f"{'━'*28}\n"
        f"💰 Price: `{row['Price']:,}`\n"
        f"📊 Score: `[{score_bar}] 6/6` ← MAXIMUM\n"
        f"📈 RSI-EMA: `{row['RSI-EMA']}` | Stoch K: `{row['Stoch K']}`\n"
        f"🌊 RVOL: `{row['RVOL']}x` — Volume surge!\n"
        f"📉 MACD Hist: `{row['MACD Hist']}`\n"
        f"{'━'*28}\n"
        f"🎯 TP: `{row['TP']:,}`\n"
        f"🛑 SL: `{row['SL']:,}`\n"
        f"⚖️ R:R = `{row['R:R']}`\n"
        f"{'━'*28}\n"
        f"💡 _{row['Reasons'][:80]}_\n\n"
        f"⏰ `{now.strftime('%H:%M:%S')} WIB`\n"
        f"⚡ _Theta Turbo v4 · 15M_\n"
        f"⚠️ _BUKAN saran investasi!_"
    )
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"}, timeout=10)
    except: pass


# ─────────────────────────────────────────────
#  DATA FETCH — chunked buat hindari rate limit
# ─────────────────────────────────────────────
@st.cache_data(ttl=300)
def fetch_intraday(tickers, chunk=25):
    all_dfs = {}
    for i in range(0, len(tickers), chunk):
        batch = tickers[i:i+chunk]
        try:
            raw = yf.download(
                batch, period="5d", interval="15m",
                group_by='ticker', progress=False,
                threads=True, auto_adjust=True
            )
            for t in batch:
                try:
                    df = raw[t].dropna() if len(batch)>1 else raw.dropna()
                    if len(df) >= 50:
                        all_dfs[t] = df
                except: pass
        except: pass
        time.sleep(0.5)  # napas biar ga di-ban
    return all_dfs

# ─────────────────────────────────────────────
#  MAIN SCAN
# ─────────────────────────────────────────────
prog_ph = st.empty()
with prog_ph.container():
    st.markdown('<div style="color:#ff7b00;font-family:Space Mono,monospace;font-size:12px;letter-spacing:1px;">🔥 Scanning intraday 15M data...</div>', unsafe_allow_html=True)
    pb = st.progress(0)

try:
    data_dict = fetch_intraday(stocks_yf)
    results   = []
    tickers   = list(data_dict.keys())

    for i, ticker_yf in enumerate(tickers):
        pb.progress((i+1)/max(len(tickers),1))
        try:
            df = data_dict[ticker_yf].copy()
            if len(df) < 55: continue

            df = apply_intraday_indicators(df)
            r  = df.iloc[-1]; p = df.iloc[-2]
            p2 = df.iloc[-3] if len(df)>=3 else p

            close    = float(r['Close'])
            vol      = float(r['Volume'])
            turnover = close * vol
            rvol     = float(r['RVOL'])

            if turnover < min_turn: continue
            if rvol < vol_thresh:   continue

            # Pilih scoring sesuai mode
            if scan_mode == "Scalping ⚡":
                sc, reasons, det = score_scalping(r, p, p2)
            elif scan_mode == "Momentum 🚀":
                sc, reasons, det = score_momentum(r, p, p2)
            else:
                sc, reasons, det = score_reversal(r, p, p2)

            if sc < min_score: continue

            sig = get_signal(sc, scan_mode)
            if sig == "WAIT": continue

            # TP / SL (berbasis ATR intraday)
            tr_arr  = pd.concat([
                df['High']-df['Low'],
                (df['High']-df['Close'].shift()).abs(),
                (df['Low']-df['Close'].shift()).abs()
            ], axis=1).max(axis=1)
            atr     = float(tr_arr.rolling(14).mean().iloc[-1])

            if scan_mode == "Scalping ⚡":
                tp = close + 1.5*atr; sl = close - 0.8*atr
            elif scan_mode == "Momentum 🚀":
                tp = close + 2.0*atr; sl = close - 1.0*atr
            else:  # Reversal
                tp = close + 2.5*atr; sl = close - 0.8*atr

            rr = (tp-close)/max(close-sl, 0.01)

            # Trend
            e9=float(r['EMA9']); e21=float(r['EMA21']); e50=float(r['EMA50'])
            trend = "▲ UP" if e9>e21>e50 else ("▼ DOWN" if e9<e21<e50 else "◆ SIDE")
            roc3  = float(r['ROC3'])*100

            results.append({
                "Ticker":    stock_map[ticker_yf],
                "Price":     int(close),
                "Score":     sc,
                "Signal":    sig,
                "Trend":     trend,
                "RSI-EMA":   round(float(r['RSI_EMA']),1),
                "Stoch K":   round(float(r['STOCH_K']),1),
                "Stoch D":   round(float(r['STOCH_D']),1),
                "MACD Hist": round(float(r['MACD_Hist']),4),
                "RVOL":      round(rvol,2),
                "BB%":       round(float(r['BB_pct']),2),
                "ROC 3B%":   round(roc3,2),
                "VWAP":      int(float(r['VWAP'])),
                "TP":        int(tp),
                "SL":        int(sl),
                "R:R":       round(rr,1),
                "Turnover(M)": round(turnover/1e6,1),
                "Reasons":   " · ".join(reasons),
                "_class":    get_card_class(sig),
            })
        except Exception: continue

    prog_ph.empty()

    # ─────────────────────────────────────────
    #  DISPLAY
    # ─────────────────────────────────────────
    if not results:
        st.markdown("""
        <div style="text-align:center;padding:60px;color:#4a5568;font-family:Space Mono,monospace;">
          <div style="font-size:36px;margin-bottom:12px;">📭</div>
          <div style="font-size:13px;letter-spacing:2px;">BELUM ADA SIGNAL VALID</div>
          <div style="font-size:11px;margin-top:8px;">Turunkan Min Score atau Min RVOL</div>
        </div>""", unsafe_allow_html=True)
    else:
        df_out  = pd.DataFrame(results).sort_values("Score", ascending=False).reset_index(drop=True)
        gacor   = df_out[df_out["Signal"].str.contains("GACOR|REVERSAL", na=False)]
        potensi = df_out[df_out["Signal"].str.contains("POTENSIAL", na=False)]
        avg_rsi = df_out['RSI-EMA'].mean()

        # Metrics
        st.markdown(f"""
        <div class="metric-row">
          <div class="metric-card orange"><div class="metric-label">Mode</div>
            <div class="metric-value" style="font-size:13px;margin-top:4px;">{scan_mode}</div></div>
          <div class="metric-card green"><div class="metric-label">Signal Lolos</div>
            <div class="metric-value">{len(df_out)}</div>
            <div class="metric-sub">dari {len(raw_stocks)} emiten</div></div>
          <div class="metric-card red"><div class="metric-label">GACOR 🔥</div>
            <div class="metric-value">{len(gacor)}</div>
            <div class="metric-sub">score ≥ 5</div></div>
          <div class="metric-card amber"><div class="metric-label">POTENSIAL</div>
            <div class="metric-value">{len(potensi)}</div>
            <div class="metric-sub">score = 4</div></div>
          <div class="metric-card"><div class="metric-label">Avg RSI-EMA</div>
            <div class="metric-value" style="color:{'#00ff88' if avg_rsi>50 else '#ffb700' if avg_rsi>35 else '#ff3d5a'}">{avg_rsi:.1f}</div>
            <div class="metric-sub">{'Bullish' if avg_rsi>50 else 'Neutral' if avg_rsi>35 else 'Oversold'}</div></div>
        </div>
        """, unsafe_allow_html=True)

        # Ticker tape
        th = '<div class="tape-wrap"><div class="tape-inner">'
        for _, row in df_out.iterrows():
            roc=row['ROC 3B%']; cls='up' if roc>0 else('down' if roc<0 else'flat')
            sym='▲' if roc>0 else('▼' if roc<0 else'─')
            th += f'<span class="tape-item {cls}">{row["Ticker"]} {int(row["Price"])} {sym}{abs(roc):.1f}% [{row["Signal"]}]</span>'
        th += th.replace('tape-inner">',''); th += '</div></div>'
        st.markdown(th, unsafe_allow_html=True)

        # Alert GACOR
        if not gacor.empty:
            st.markdown(f"""
            <div class="alert-box">
              <div class="alert-title">🚨 GACOR ALERT · {len(gacor)} SAHAM · {scan_mode}</div>
              <div style="font-size:11px;color:#4a5568;margin-top:4px;">
                Score ≥ 5 · Konfirmasi multi-indikator 15M · R:R optimal
              </div>
            </div>""", unsafe_allow_html=True)

        # Telegram alert
        if tele_on and results:
            if 'tt_last_sent' not in st.session_state: st.session_state.tt_last_sent=set()
            cur_set = set(df_out['Ticker'].tolist())
            new_alr = cur_set - st.session_state.tt_last_sent

            if new_alr:
                top_new = df_out[df_out['Ticker'].isin(new_alr)].head(5).to_dict('records')
                if top_new:
                    send_telegram_alert(top_new)

                # Extra alert untuk score = 6
                perfect = df_out[(df_out['Ticker'].isin(new_alr)) & (df_out['Score']==6)]
                for _, rw in perfect.iterrows():
                    send_telegram_gacor(rw.to_dict())

                st.session_state.tt_last_sent.update(new_alr)
            st.session_state.tt_last_sent = st.session_state.tt_last_sent & cur_set

        # CARD VIEW
        if view_mode == "Card View 🃏":
            st.markdown('<div class="section-title">Signal Cards</div>', unsafe_allow_html=True)
            card_html = '<div class="signal-grid">'
            for _, row in df_out.head(20).iterrows():
                sc_int  = int(row['Score'])
                bars    = ''.join([
                    f'<div class="sc-bar {"filled" if i < sc_int else "empty"}" style="width:28px"></div>'
                    for i in range(6)
                ])
                roc_c   = '#00ff88' if row['ROC 3B%']>0 else '#ff3d5a'
                price_c = '#00ff88' if row['ROC 3B%']>0 else '#ff3d5a'
                trend_e = "📈" if "▲" in row['Trend'] else ("📉" if "▼" in row['Trend'] else "➡️")

                card_html += f"""
                <div class="signal-card {row['_class']}">
                  <div style="display:flex;justify-content:space-between;align-items:flex-start;">
                    <div>
                      <div class="sc-ticker">{row['Ticker']}</div>
                      <div class="sc-price" style="color:{price_c}">{int(row['Price']):,} {trend_e}</div>
                    </div>
                    <div style="text-align:right;">
                      <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;">SCORE</div>
                      <div style="font-family:Space Mono,monospace;font-size:20px;font-weight:700;color:{'#00ff88' if sc_int>=5 else '#ffb700' if sc_int>=4 else '#00e5ff'}">{row['Score']}</div>
                    </div>
                  </div>
                  <div class="sc-signal" style="color:{'#00ff88' if 'GACOR' in row['Signal'] or 'REVERSAL' in row['Signal'] else '#ffb700' if 'POTENSIAL' in row['Signal'] else '#00e5ff'}">{row['Signal']}</div>
                  <div class="sc-bars">{bars}</div>
                  <div class="sc-stats">
                    <div class="sc-stat">RSI-EMA <span>{row['RSI-EMA']}</span></div>
                    <div class="sc-stat">STOCH <span>{row['Stoch K']:.0f}</span></div>
                    <div class="sc-stat">RVOL <span>{row['RVOL']}x</span></div>
                    <div class="sc-stat">ROC <span style="color:{roc_c}">{row['ROC 3B%']:+.1f}%</span></div>
                  </div>
                  <div class="sc-stats" style="margin-top:6px;">
                    <div class="sc-stat">TP <span style="color:#00ff88">{int(row['TP']):,}</span></div>
                    <div class="sc-stat">SL <span style="color:#ff3d5a">{int(row['SL']):,}</span></div>
                    <div class="sc-stat">R:R <span>{row['R:R']}</span></div>
                  </div>
                  <div style="margin-top:8px;font-size:10px;color:#4a5568;line-height:1.4;font-family:Space Mono,monospace;">{row['Reasons'][:70]}</div>
                </div>"""
            card_html += '</div>'
            st.markdown(card_html, unsafe_allow_html=True)

        # TABLE VIEW
        st.markdown('<div class="section-title">Full Signal Table</div>', unsafe_allow_html=True)
        display_cols = ["Ticker","Price","Score","Signal","Trend","RSI-EMA","Stoch K","Stoch D",
                        "MACD Hist","RVOL","BB%","ROC 3B%","VWAP","TP","SL","R:R","Turnover(M)","Reasons"]
        st.dataframe(
            df_out[display_cols], width='stretch', hide_index=True,
            column_config={
                "Score":      st.column_config.ProgressColumn("Score", min_value=0, max_value=6, format="%.1f"),
                "RSI-EMA":    st.column_config.NumberColumn("RSI-EMA", format="%.1f"),
                "Stoch K":    st.column_config.NumberColumn("Stoch K", format="%.1f"),
                "RVOL":       st.column_config.NumberColumn("RVOL", format="%.1fx"),
                "ROC 3B%":    st.column_config.NumberColumn("ROC 3B%", format="%.2f%%"),
                "Turnover(M)":st.column_config.NumberColumn("Turnover(M)", format="Rp%.0fM"),
            }
        )

except Exception as e:
    st.markdown(f"""
    <div style="background:rgba(255,61,90,.1);border:1px solid #ff3d5a;border-radius:8px;padding:20px;font-family:Space Mono,monospace;">
      <div style="color:#ff3d5a;font-weight:700;">⚠️ ERROR</div>
      <div style="color:#c9d1d9;font-size:12px;margin-top:8px;">{str(e)}</div>
    </div>""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
#  FOOTER
# ─────────────────────────────────────────────
st.markdown(f"""
<div style="margin-top:28px;padding-top:14px;border-top:1px solid #1c2533;
     display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;">
  <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;">
    🔥 Theta Turbo v4.0 · Intraday 15M Scanner · Built for Speed
  </div>
  <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;">
    <span style="color:#ff7b00">{datetime.now(jakarta_tz).strftime('%H:%M:%S')} WIB</span>
    · Next refresh 300s
  </div>
</div>""", unsafe_allow_html=True)

time.sleep(300)
st.rerun()
