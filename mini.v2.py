import yfinance as yf
import pandas as pd
import streamlit as st
import time
import requests
import numpy as np
import pytz
from datetime import datetime

# ════════════════════════════════════════════════════
#  CONFIG — isi di Streamlit Secrets
#  TELEGRAM_TOKEN = "xxx"
#  TELEGRAM_CHAT_ID = "xxx"
# ════════════════════════════════════════════════════
TOKEN   = st.secrets.get("TELEGRAM_TOKEN", "")
CHAT_ID = st.secrets.get("TELEGRAM_CHAT_ID", "")
jakarta_tz = pytz.timezone('Asia/Jakarta')

# Session state
for _k, _v in [("tt_last_sent", set()), ("wl_results", []),
                ("wl_mode_used", ""), ("scan_results", []),
                ("data_dict", {}), ("last_scan_time", None),
                ("last_scan_mode", "Scalping ⚡")]:
    if _k not in st.session_state: st.session_state[_k] = _v

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
[data-testid="stDataFrame"]{border:1px solid var(--border)!important;border-radius:8px!important;}
[data-testid="stDataFrame"] thead th{background:var(--surface)!important;color:var(--muted)!important;font-family:'Space Mono',monospace!important;font-size:11px!important;letter-spacing:1px!important;text-transform:uppercase!important;}
::-webkit-scrollbar{width:4px;height:4px;}::-webkit-scrollbar-track{background:var(--bg);}::-webkit-scrollbar-thumb{background:var(--border);border-radius:2px;}
[data-testid="stNumberInput"] input{background:var(--surface)!important;border:1px solid var(--border)!important;color:var(--heading)!important;font-family:'Space Mono',monospace!important;border-radius:6px!important;}
button[data-testid="baseButton-primary"]{background:var(--orange)!important;color:var(--bg)!important;font-family:'Space Mono',monospace!important;font-weight:700!important;border:none!important;}
.section-title{font-family:'Space Mono',monospace;font-size:11px;color:var(--muted);letter-spacing:2px;text-transform:uppercase;border-left:3px solid var(--orange);padding-left:10px;margin:20px 0 10px 0;}
.bt-result{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:20px;margin-top:12px;}
.bt-metric{display:inline-block;margin-right:24px;margin-bottom:8px;}
.bt-metric-val{font-family:'Space Mono',monospace;font-size:22px;font-weight:700;}
.bt-metric-lbl{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:1px;}
@media(max-width:768px){.main .block-container{padding-left:.75rem!important;padding-right:.75rem!important;}.signal-grid{grid-template-columns:1fr;}}
</style>
""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════
#  STOCK LIST — FULL IDX
# ════════════════════════════════════════════════════
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

# ════════════════════════════════════════════════════
#  MARKET REGIME DETECTOR
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
            return ("RED",     price, ema20, ema55, f"IHSG {price:,.0f} < EMA20 → Bearish", chg)
        elif price > ema20 and price > ema55:
            return ("GREEN",   price, ema20, ema55, f"IHSG {price:,.0f} > EMA20 & EMA55 → Bullish", chg)
        else:
            return ("SIDEWAYS",price, ema20, ema55, f"IHSG {price:,.0f} antara EMA20-EMA55", chg)
    except:
        return ("UNKNOWN", 0, 0, 0, "IHSG tidak tersedia — manual mode", 0.0)

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

def rsi_smooth(s, p=14, smooth=3):
    delta = s.diff()
    gain  = delta.clip(lower=0).rolling(p).mean()
    loss  = (-delta.clip(upper=0)).rolling(p).mean()
    rs    = gain / loss.replace(0, np.nan)
    raw   = 100 - 100/(1+rs)
    return raw, ema(raw, smooth)

def stochastic(h, l, c, k=14, d=3):
    ll = l.rolling(k).min(); hh = h.rolling(k).max()
    K  = 100*(c-ll)/(hh-ll).replace(0,np.nan)
    D  = K.rolling(d).mean()
    return K.fillna(50), D.fillna(50)

def macd(s, f=12, sl=26, sg=9):
    ml = ema(s,f)-ema(s,sl); sig = ema(ml,sg)
    return ml, sig, ml-sig

def vwap(df):
    tp = (df['High']+df['Low']+df['Close'])/3
    return (tp*df['Volume']).cumsum()/df['Volume'].cumsum()

def apply_intraday_indicators(df):
    if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.droplevel(1)
    df['EMA9']  = ema(df['Close'],9);  df['EMA21'] = ema(df['Close'],21)
    df['EMA50'] = ema(df['Close'],50); df['EMA200']= ema(df['Close'],200)
    df['RSI'], df['RSI_EMA'] = rsi_smooth(df['Close'],14,3)
    df['STOCH_K'], df['STOCH_D'] = stochastic(df['High'],df['Low'],df['Close'],14,3)
    df['MACD'], df['MACD_Sig'], df['MACD_Hist'] = macd(df['Close'])
    try:    df['VWAP'] = vwap(df)
    except: df['VWAP'] = df['Close']
    df['BB_mid']  = df['Close'].rolling(20).mean()
    df['BB_std']  = df['Close'].rolling(20).std()
    df['BB_upper']= df['BB_mid']+2*df['BB_std']; df['BB_lower']= df['BB_mid']-2*df['BB_std']
    df['BB_pct']  = (df['Close']-df['BB_lower'])/(df['BB_upper']-df['BB_lower'])
    df['AvgVol']  = df['Volume'].rolling(20).mean()
    df['RVOL']    = df['Volume']/df['AvgVol']
    df['NetVol']  = np.where(df['Close']>=df['Open'],df['Volume'],-df['Volume'])
    df['NetVol3'] = pd.Series(df['NetVol'],index=df.index).rolling(3).sum()
    df['NetVol8'] = pd.Series(df['NetVol'],index=df.index).rolling(8).sum()
    df['VolSpike']= df['RVOL']>2.5
    df['Body']    = (df['Close']-df['Open']).abs()
    df['BodyRatio']= df['Body']/(df['High']-df['Low']).replace(0,np.nan)
    df['BullBar'] = (df['Close']>df['Open'])&(df['BodyRatio']>0.5)
    df['ROC3']    = df['Close'].pct_change(3); df['ROC8'] = df['Close'].pct_change(8)
    df['HH']= df['High']>df['High'].shift(1);  df['HL']= df['Low']>df['Low'].shift(1)
    df['LL']= df['Low']<df['Low'].shift(1);    df['LH']= df['High']<df['High'].shift(1)
    tr = pd.concat([df['High']-df['Low'],(df['High']-df['Close'].shift()).abs(),(df['Low']-df['Close'].shift()).abs()],axis=1).max(axis=1)
    df['ATR'] = tr.rolling(14).mean()
    return df

# ════════════════════════════════════════════════════
#  SCORING
# ════════════════════════════════════════════════════
def score_scalping(r, p, p2):
    score=0; reasons=[]
    if r['EMA9']>r['EMA21']>r['EMA50']:   score+=1.5; reasons.append("EMA stack ▲")
    elif r['EMA9']>r['EMA21']:             score+=0.8; reasons.append("EMA9>21")
    if r['Close']>r['VWAP']:              score+=1;   reasons.append("Above VWAP")
    if r['MACD_Hist']>0 and r['MACD_Hist']>float(p['MACD_Hist']):
        score+=1.5; reasons.append("MACD hist expanding ✦")
        if p2 is not None and float(p['MACD_Hist'])>float(p2['MACD_Hist']): score+=0.3; reasons.append("MACD 3 bar rising")
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

# ════════════════════════════════════════════════════
#  TELEGRAM — format detail
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
        bar  = "█"*int(r['Score'])+"░"*(6-int(r['Score']))
        body += (f"\n{em} *{r['Ticker']}*  `{sig}`\n"
                 f"   💰 Price: `{r['Price']:,}` {te}\n"
                 f"   📊 Score: `[{bar}] {r['Score']}/6`\n"
                 f"   📈 RSI-EMA: `{r.get('RSI-EMA',0)}` | STOCH: `{r.get('Stoch K',0)}`\n"
                 f"   🌊 RVOL: `{r.get('RVOL',0)}x` | MACD: `{r.get('MACD Hist',0)}`\n"
                 f"   🎯 TP: `{r['TP']:,}` | 🛑 SL: `{r['SL']:,}` | R:R `{r['R:R']}`\n"
                 f"   💡 _{r.get('Reasons','')[:60]}_\n")
    footer = f"\n{sep}\n⚡ _Theta Turbo v5 · 15M Intraday_\n⚠️ _BUKAN saran investasi. DYOR!_"
    try:
        requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                      data={"chat_id":CHAT_ID,"text":hdr+body+footer,"parse_mode":"Markdown"},
                      timeout=10)
    except: pass

# ════════════════════════════════════════════════════
#  DATA FETCH — cache 15 menit
# ════════════════════════════════════════════════════
@st.cache_data(ttl=360)
def fetch_intraday(tickers, chunk=25):
    all_dfs = {}
    for i in range(0, len(tickers), chunk):
        batch = tickers[i:i+chunk]
        try:
            raw = yf.download(batch, period="5d", interval="15m",
                              group_by='ticker', progress=False,
                              threads=True, auto_adjust=True)
            for t in batch:
                try:
                    df = raw[t].dropna() if len(batch)>1 else raw.dropna()
                    if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.droplevel(1)
                    if len(df) >= 50: all_dfs[t] = df
                except: pass
        except: pass
        time.sleep(0.5)
    return all_dfs

# ════════════════════════════════════════════════════
#  HEADER
# ════════════════════════════════════════════════════
regime, ihsg_price, ema20, ema55, regime_detail, ihsg_chg = get_market_regime()
rcfg    = get_regime_config(regime)
rcolor  = rcfg["color"]
chg_col = "#00ff88" if ihsg_chg >= 0 else "#ff3d5a"
chg_sym = "▲" if ihsg_chg >= 0 else "▼"

now_jkt = datetime.now(jakarta_tz)
st.markdown(f"""
<div class="tt-header">
  <div>
    <div class="tt-logo">🔥 THETA TURBO</div>
    <div class="tt-sub">Intraday 15M Scanner · Auto Regime · v5.0</div>
  </div>
  <div class="live-badge"><div class="live-dot"></div>LIVE {now_jkt.strftime("%H:%M:%S")} WIB</div>
</div>""", unsafe_allow_html=True)

# Regime Panel
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

# ════════════════════════════════════════════════════
#  PIVOT POINTS — Classic Floor Trader Formula
# ════════════════════════════════════════════════════
def calc_pivot_points(high, low, close):
    """Hitung pivot points harian dari H/L/C kemarin."""
    pp = (high + low + close) / 3
    r1 = 2*pp - low
    r2 = pp + (high - low)
    r3 = high + 2*(pp - low)
    s1 = 2*pp - high
    s2 = pp - (high - low)
    s3 = low - 2*(high - pp)
    return {"PP":pp,"R1":r1,"R2":r2,"R3":r3,"S1":s1,"S2":s2,"S3":s3}

@st.cache_data(ttl=3600)
def fetch_pivot_data(ticker_yf):
    """Fetch daily data untuk pivot point calculation."""
    try:
        df = yf.download(ticker_yf, period="5d", interval="1d",
                         progress=False, auto_adjust=True, threads=False)
        if df is None or len(df) < 2: return None
        if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.droplevel(1)
        prev = df.iloc[-2]  # kemarin
        return calc_pivot_points(float(prev["High"]), float(prev["Low"]), float(prev["Close"]))
    except: return None

def get_pivot_position(price, pivots):
    """Tentukan posisi price relatif terhadap pivot."""
    if pivots is None: return "Unknown", "#4a5568"
    pp = pivots["PP"]
    if price > pivots["R2"]:   return "Above R2 🔴", "#ff3d5a"
    elif price > pivots["R1"]: return "R1→R2 🟠",   "#ff7b00"
    elif price > pp:           return "PP→R1 🟢",   "#00ff88"
    elif price > pivots["S1"]: return "S1→PP 🟡",   "#ffb700"
    elif price > pivots["S2"]: return "S2→S1 🔴",   "#ff3d5a"
    else:                      return "Below S2 🔴", "#ff3d5a"

# ════════════════════════════════════════════════════
#  MULTI-TIMEFRAME SCORE
# ════════════════════════════════════════════════════
@st.cache_data(ttl=360)
def fetch_mtf_data(ticker_yf):
    """Fetch 15M + 1H + 1D untuk MTF analysis."""
    result = {}
    for interval, period, key in [("15m","3d","M15"), ("1h","10d","H1"), ("1d","60d","D1")]:
        try:
            df = yf.download(ticker_yf, period=period, interval=interval,
                             progress=False, auto_adjust=True, threads=False)
            if df is not None and not df.empty:
                if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.droplevel(1)
                df = df.dropna()
                if len(df) >= 20: result[key] = df
        except: pass
    return result

def score_mtf(ticker_yf, mode="Scalping ⚡"):
    """Hitung score per timeframe dan gabungkan."""
    mtf = fetch_mtf_data(ticker_yf)
    scores = {}
    for tf_key, df in mtf.items():
        try:
            df = apply_intraday_indicators(df.copy())
            if len(df) < 3: continue
            r=df.iloc[-1]; p=df.iloc[-2]; p2=df.iloc[-3]
            if mode=="Scalping ⚡":   sc,_,_=score_scalping(r,p,p2)
            elif mode=="Momentum 🚀": sc,_,_=score_momentum(r,p,p2)
            else:                     sc,_,_=score_reversal(r,p,p2)
            scores[tf_key] = round(sc, 1)
        except: scores[tf_key] = 0
    return scores

def mtf_alignment(scores):
    """Cek apakah semua TF align bullish."""
    if not scores: return "No Data", "#4a5568", 0
    vals = list(scores.values())
    avg  = sum(vals)/len(vals)
    bullish_count = sum(1 for v in vals if v >= 4)
    if bullish_count == len(vals):  return "FULL ALIGN 🔥", "#00ff88", avg
    elif bullish_count >= 2:        return "PARTIAL ⚡",    "#ffb700", avg
    elif bullish_count == 1:        return "MIXED ⚠️",      "#ff7b00", avg
    else:                           return "NO ALIGN ❌",   "#ff3d5a", avg

# ════════════════════════════════════════════════════
#  BSJP — BELI SORE JUAL PAGI
#  Entry: 14:30–15:45 WIB | Exit: besok 09:00–10:00
# ════════════════════════════════════════════════════
def score_bsjp(r, p, p2):
    """
    Scoring khusus BSJP — fokus momentum closing + overnight gap potential.
    Beda dari scalping: lebih weight ke trend harian & volume surge closing.
    """
    score=0; reasons=[]
    # 1. Closing strength — tutup mendekati high harian
    body   = float(r["Close"]) - float(r["Open"])
    hi_lo  = float(r["High"])  - float(r["Low"])
    close_pct = (float(r["Close"]) - float(r["Low"])) / max(hi_lo, 1)
    if close_pct > 0.7:  score+=2;   reasons.append(f"Tutup dekat High ({close_pct:.0%})")
    elif close_pct > 0.5: score+=1;  reasons.append(f"Tutup kuat ({close_pct:.0%})")

    # 2. Volume surge sore hari = sinyal akumulasi
    rvol = float(r["RVOL"])
    if rvol > 3.0:   score+=2;   reasons.append(f"RVOL={rvol:.1f}x SURGE 🔥")
    elif rvol > 2.0: score+=1.5; reasons.append(f"RVOL={rvol:.1f}x kuat")
    elif rvol > 1.5: score+=0.8; reasons.append(f"RVOL={rvol:.1f}x")

    # 3. EMA trend alignment
    if r["EMA9"]>r["EMA21"]>r["EMA50"]:  score+=1.5; reasons.append("EMA stack ▲")
    elif r["EMA9"]>r["EMA21"]:            score+=0.8; reasons.append("EMA9>21")

    # 4. RSI tidak overbought (jangan beli yang udah tinggi banget)
    rsi_e = float(r["RSI_EMA"])
    if 45<rsi_e<70:  score+=1;   reasons.append(f"RSI-EMA={rsi_e:.1f} ✓")
    elif rsi_e>=70:  score-=1;   reasons.append(f"RSI-EMA={rsi_e:.1f} OB ⚠️")
    elif rsi_e<40:   score+=0.5; reasons.append(f"RSI-EMA={rsi_e:.1f} oversold")

    # 5. MACD positif
    if float(r["MACD_Hist"])>0 and float(r["MACD_Hist"])>float(p["MACD_Hist"]):
        score+=1; reasons.append("MACD hist expanding ✦")
    elif float(r["MACD_Hist"])>0:
        score+=0.5; reasons.append("MACD +")

    # 6. Above VWAP = bandar masih akumulasi
    if float(r["Close"])>float(r["VWAP"]): score+=0.5; reasons.append("Above VWAP")

    return max(0,min(6,round(score,1))), reasons, {}


# ════════════════════════════════════════════════════
#  SEKTOR IDX — MAPPING & ROTATION
# ════════════════════════════════════════════════════
SECTORS = {
    "Energi & Mining":    ["ADRO","BYAN","ITMG","PTBA","HRUM","DOID","GEMS","PGAS","ELSA","MEDC","ESSA","AKRA","RIGS","DSSA","MBAP","KKGI","MYOH","SMMT","BSSR","INDY"],
    "Perbankan":          ["BBCA","BBRI","BMRI","BBNI","BBTN","BJBR","BJTM","BNGA","BDMN","NISP","MEGA","BBYB","ARTO","BRIS","AGRO","BBHI","NOBU","PNBN","BACA","MAYA"],
    "Properti":           ["BSDE","CTRA","SMRA","LPKR","PWON","APLN","ASRI","DILD","DUTI","MDLN","MKPI","JRPT","KIJA","BEST","GPRA","NUSA","DART","CITY","BKSL","MTLA"],
    "Infrastruktur":      ["JSMR","TLKM","EXCL","ISAT","TBIG","TOWR","WIKA","ADHI","PTPP","WSKT","WTON","WEGE","ACST","DGIK","TRUK","BIRD","GIAA","TMAS","SMDR","BBRM"],
    "Konsumer":           ["UNVR","ICBP","INDF","MYOR","KLBF","SIDO","GGRM","HMSP","ULTJ","DLTA","ROTI","SKBM","GOOD","HOKI","CLEO","MIKA","HEAL","SILO","KAEF","DVLA"],
    "Industri & Otomotif":["ASII","AUTO","SMSM","HEXA","UNTR","SCCO","KBLI","VOKS","BRAM","GJTL","IMAS","INTP","SMGR","AMFG","LION","CPIN","JPFA","MAIN","BRPT","TPIA"],
    "Teknologi":          ["GOTO","BUKA","EMTK","MNCN","SCMA","MTEL","MTDL","MLPT","CHIP","LUCK","NFCX","DCII","WIFI","DIGI","AWAN","AXIO","INET","MCAS","WIRG","TECH"],
    "Shipping & Logistik":["TMAS","SMDR","BBRM","NELY","AKSI","SHIP","ELPI","BIRD","GIAA","TAXI","ASSA","WEHA","SAFE","ATLI","MIRA","HEXA","RAJA","RIGS","MBSS","IATA"],
    "Petrokimia & Kimia": ["TPIA","BRPT","BUDI","EKAD","INCI","DPNS","ETWA","MDKI","ESSA","AKPI","ADMG","CPRO","SRSN","MOLI","PURA","CEKA","KBLM","JPFA","CPIN","UNIC"],
}

# Hormuz-sensitive sectors (benefited dari Hormuz open)
HORMUZ_SECTORS = ["Energi & Mining", "Shipping & Logistik", "Petrokimia & Kimia"]

@st.cache_data(ttl=300)
def fetch_sector_rotation(sector_stocks):
    """Fetch daily data untuk sektor rotation — perubahan % hari ini."""
    results = []
    tickers_yf = [s+".JK" for s in sector_stocks[:10]]  # top 10 per sektor
    try:
        raw = yf.download(tickers_yf, period="3d", interval="1d",
                          group_by="ticker", progress=False,
                          threads=True, auto_adjust=True)
        for t in tickers_yf:
            tkr = t.replace(".JK","")
            try:
                if len(tickers_yf) > 1:
                    df = raw[t].dropna()
                else:
                    df = raw.copy()
                    if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.droplevel(1)
                    df = df.dropna()
                if len(df) < 2: continue
                close  = float(df["Close"].iloc[-1])
                prev   = float(df["Close"].iloc[-2])
                chg    = (close - prev) / prev * 100
                vol    = float(df["Volume"].iloc[-1])
                avg_v  = float(df["Volume"].mean())
                rvol   = vol / avg_v if avg_v > 0 else 1.0
                results.append({"ticker":tkr,"close":close,"chg":chg,"rvol":round(rvol,2)})
            except: pass
    except: pass
    return results

# ════════════════════════════════════════════════════
#  SECTOR BETA & RELATIVE STRENGTH vs IHSG
# ════════════════════════════════════════════════════
@st.cache_data(ttl=3600)
def calc_sector_beta(sector_name, sector_stocks, lookback=20):
    """
    Hitung beta sektor vs IHSG dan relative strength.
    Beta > 1 = lebih volatile dari market
    Beta < 1 = defensive
    RS = return sektor - return IHSG (positif = outperform)
    """
    try:
        # Fetch IHSG
        ihsg = yf.download("^JKSE", period="60d", interval="1d",
                           progress=False, auto_adjust=True)
        if ihsg is None or len(ihsg) < lookback: return None
        if isinstance(ihsg.columns, pd.MultiIndex): ihsg.columns = ihsg.columns.droplevel(1)
        ihsg_ret = ihsg["Close"].pct_change().dropna()

        # Fetch sektor (rata-rata return saham dalam sektor)
        tickers_yf = [s+".JK" for s in sector_stocks[:8]]
        raw = yf.download(tickers_yf, period="60d", interval="1d",
                          group_by="ticker", progress=False,
                          threads=True, auto_adjust=True)

        sec_rets = []
        for t in tickers_yf:
            try:
                if len(tickers_yf) > 1:
                    df = raw[t]["Close"].dropna()
                else:
                    df = raw["Close"].dropna()
                ret = df.pct_change().dropna()
                sec_rets.append(ret)
            except: pass

        if not sec_rets: return None

        # Align semua ke index yang sama
        sec_avg = pd.concat(sec_rets, axis=1).mean(axis=1)
        aligned  = pd.concat([ihsg_ret, sec_avg], axis=1).dropna()
        aligned.columns = ["IHSG","Sektor"]

        if len(aligned) < 10: return None

        # Beta = Cov(Sektor, IHSG) / Var(IHSG)
        cov    = aligned["Sektor"].cov(aligned["IHSG"])
        var    = aligned["IHSG"].var()
        beta   = round(cov / var, 2) if var > 0 else 1.0

        # Correlation
        corr   = round(aligned["Sektor"].corr(aligned["IHSG"]), 2)

        # Relative Strength — 5 hari terakhir
        rs5    = round((aligned["Sektor"].tail(5).sum() - aligned["IHSG"].tail(5).sum()) * 100, 2)

        # Return 1 bulan
        ret_1m_sec  = round(aligned["Sektor"].tail(20).sum() * 100, 2)
        ret_1m_ihsg = round(aligned["IHSG"].tail(20).sum() * 100, 2)

        # Max Drawdown sektor saat IHSG turun
        down_days = aligned[aligned["IHSG"] < -0.005]
        avg_down  = round(down_days["Sektor"].mean() * 100, 2) if len(down_days) > 0 else 0.0

        return {
            "sector": sector_name,
            "beta": beta,
            "corr": corr,
            "rs5": rs5,
            "ret_1m_sec": ret_1m_sec,
            "ret_1m_ihsg": ret_1m_ihsg,
            "avg_down": avg_down,  # rata-rata return saat IHSG turun
            "defensive": beta < 0.8 and corr < 0.7,
        }
    except: return None

def get_beta_label(beta):
    if beta < 0.6:   return "🛡️ Very Defensive", "#00ff88"
    elif beta < 0.8: return "🟢 Defensive",      "#00ff88"
    elif beta < 1.0: return "🟡 Moderate",       "#ffb700"
    elif beta < 1.3: return "🟠 Aggressive",     "#ff7b00"
    else:            return "🔴 High Risk",       "#ff3d5a"

# ════════════════════════════════════════════════════
#  GAP UP SCANNER
# ════════════════════════════════════════════════════
@st.cache_data(ttl=300)
def scan_gap_up(tickers_yf, min_gap_pct=0.5):
    """
    Detect kandidat Gap Up besok pagi:
    - Close hari ini > High kemarin (gap confirmed)
    - ATAU Close mendekati High hari ini (potential gap up)
    - Volume surge sore hari
    """
    results = []
    for i in range(0, len(tickers_yf), 30):
        batch = tickers_yf[i:i+30]
        try:
            raw = yf.download(batch, period="5d", interval="1d",
                              group_by="ticker", progress=False,
                              threads=True, auto_adjust=True)
            for t in batch:
                tkr = t.replace(".JK","")
                try:
                    if len(batch) > 1:
                        df = raw[t].dropna()
                    else:
                        df = raw.copy()
                        if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.droplevel(1)
                        df = df.dropna()
                    if len(df) < 3: continue

                    today  = df.iloc[-1]
                    prev   = df.iloc[-2]

                    close   = float(today["Close"])
                    high_t  = float(today["High"])
                    low_t   = float(today["Low"])
                    high_p  = float(prev["High"])
                    vol     = float(today["Volume"])
                    avg_vol = float(df["Volume"].mean())
                    rvol    = vol / avg_vol if avg_vol > 0 else 1.0

                    # Gap score
                    gap_score = 0
                    reasons   = []

                    # 1. Close di atas High kemarin = gap confirmed
                    if close > high_p:
                        gap_pct = (close - high_p) / high_p * 100
                        gap_score += 3
                        reasons.append(f"Gap {gap_pct:.1f}% above prev High ✦✦")

                    # 2. Close mendekati High hari ini (>85%) = potential gap
                    close_ratio = (close - low_t) / max(high_t - low_t, 1)
                    if close_ratio > 0.85:
                        gap_score += 2
                        reasons.append(f"Tutup dekat High {close_ratio:.0%}")
                    elif close_ratio > 0.70:
                        gap_score += 1
                        reasons.append(f"Tutup kuat {close_ratio:.0%}")

                    # 3. Volume surge
                    if rvol > 3.0:   gap_score += 2; reasons.append(f"RVOL={rvol:.1f}x SURGE 🔥")
                    elif rvol > 2.0: gap_score += 1; reasons.append(f"RVOL={rvol:.1f}x")
                    elif rvol > 1.5: gap_score += 0.5

                    # 4. Trend harian naik
                    if len(df) >= 3:
                        chg3 = (close - float(df.iloc[-3]["Close"])) / float(df.iloc[-3]["Close"]) * 100
                        if chg3 > 3:    gap_score += 1; reasons.append(f"3D ROC +{chg3:.1f}%")
                        elif chg3 > 1:  gap_score += 0.5

                    if gap_score < 3: continue

                    chg_today = (close - float(prev["Close"])) / float(prev["Close"]) * 100
                    results.append({
                        "Ticker": tkr, "Price": int(close),
                        "Gap Score": round(gap_score,1),
                        "Chg %": round(chg_today,2),
                        "Close Ratio": round(close_ratio,2),
                        "RVOL": round(rvol,2),
                        "Prev High": int(high_p),
                        "Signal": "GAP UP 🚀" if gap_score>=4 else "POTENTIAL ⚡",
                        "Reasons": " · ".join(reasons[:3])
                    })
                except: pass
        except: pass
        time.sleep(0.3)
    return sorted(results, key=lambda x: x["Gap Score"], reverse=True)

# ════════════════════════════════════════════════════
#  TRAILING STOP ENGINE
# ════════════════════════════════════════════════════
def calc_trailing_stop(entry, current, atr, method="ATR", atr_mult=2.0, pct=3.0):
    """
    Hitung trailing stop berdasarkan metode pilihan.
    Returns: stop_price, profit_locked, trail_distance
    """
    if method == "ATR":
        trail_dist  = atr * atr_mult
        stop_price  = current - trail_dist
    elif method == "Persen":
        trail_dist  = current * (pct/100)
        stop_price  = current * (1 - pct/100)
    else:  # Swing Low
        trail_dist  = atr * 1.5
        stop_price  = current - trail_dist

    profit_locked = max(0, stop_price - entry)
    profit_pct    = (current - entry) / entry * 100
    locked_pct    = (stop_price - entry) / entry * 100 if stop_price > entry else 0

    return {
        "stop":     round(stop_price, 0),
        "distance": round(trail_dist, 0),
        "profit_float": round(profit_pct, 2),
        "profit_locked": round(locked_pct, 2),
        "is_profitable": stop_price > entry
    }

tab_scanner, tab_watchlist, tab_bsjp, tab_sector, tab_gapup, tab_trail, tab_backtest = st.tabs(["🔥 Scanner","👁️ Watchlist","🌙 BSJP","🏭 Sektor","📈 Gap Up","🎯 Trailing Stop","📊 Backtest"])

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
                min_score  = rcfg["min_score"]
                vol_thresh = rcfg["min_rvol"]
                st.caption(f"Auto: Score≥{min_score} · RVOL≥{vol_thresh}x")
            else:
                min_score  = st.slider("Min Score (0-6)", 0, 6, 4, key="msc")
                vol_thresh = st.slider("Min RVOL Spike", 1.0, 5.0, 1.5, 0.1, key="vol")
            min_turn = st.number_input("Min Turnover (M Rp)", value=500, step=100, key="trn") * 1_000_000
        with sc3:
            st.markdown('<div class="settings-label">TAMPILAN</div>', unsafe_allow_html=True)
            view_mode  = st.radio("View", ["Card View 🃏","Table View 📊"], label_visibility="collapsed", key="view_mode")
            quick_mode = st.toggle("⚡ Quick (200 saham)", value=False, key="quick_mode")
            st.caption(f"🎯 Regime: {regime} · Mode: {scan_mode}")
            st.caption(f"📊 {len(raw_stocks)} emiten tersedia")

    # Scan button
    do_scan = st.button("🔥 MULAI SCAN SEKARANG", type="primary", use_container_width=True, key="btn_scan")

    # Auto-refresh trigger — fresh timestamp setiap check
    _now_check = datetime.now(jakarta_tz).timestamp()
    if st.session_state.last_scan_time and not do_scan:
        _elapsed = _now_check - st.session_state.last_scan_time
        if _elapsed >= 300 and st.session_state.scan_results:
            do_scan = True  # auto trigger setiap 5 menit

    if do_scan:
        scan_list = stocks_yf[:200] if quick_mode else stocks_yf
        prog_ph = st.empty()
        with prog_ph.container():
            st.markdown(f'<div style="color:#ff7b00;font-family:Space Mono,monospace;font-size:12px;letter-spacing:1px;">🔥 Scanning {len(scan_list)} saham ({scan_mode})...</div>', unsafe_allow_html=True)
            pb = st.progress(0)
        try:
            data_dict = fetch_intraday(tuple(scan_list))
            st.session_state.data_dict = data_dict
            results = []; tickers = list(data_dict.keys())
            for i, ticker_yf in enumerate(tickers):
                pb.progress((i+1)/max(len(tickers),1))
                try:
                    df = data_dict[ticker_yf].copy()
                    if len(df) < 55: continue
                    df = apply_intraday_indicators(df)
                    r=df.iloc[-1]; p=df.iloc[-2]; p2=df.iloc[-3] if len(df)>=3 else p
                    close=float(r['Close']); vol=float(r['Volume']); turnover=close*vol; rvol=float(r['RVOL'])
                    if turnover<min_turn or rvol<vol_thresh: continue
                    if scan_mode=="Scalping ⚡":   sc,reasons,_=score_scalping(r,p,p2)
                    elif scan_mode=="Momentum 🚀": sc,reasons,_=score_momentum(r,p,p2)
                    else:                          sc,reasons,_=score_reversal(r,p,p2)
                    if sc<min_score: continue
                    sig=get_signal(sc,scan_mode)
                    if sig=="WAIT": continue
                    atr=float(r['ATR']); slm=rcfg.get("sl_mult",0.8)
                    if scan_mode=="Scalping ⚡":   tp=close+1.5*atr; sl=close-slm*atr
                    elif scan_mode=="Momentum 🚀": tp=close+2.0*atr; sl=close-slm*atr
                    else:                          tp=close+2.5*atr; sl=close-slm*atr
                    rr=(tp-close)/max(close-sl,0.01)
                    e9=float(r['EMA9']); e21=float(r['EMA21']); e50=float(r['EMA50'])
                    trend="▲ UP" if e9>e21>e50 else("▼ DOWN" if e9<e21<e50 else"◆ SIDE")
                    results.append({"Ticker":stock_map[ticker_yf],"Price":int(close),"Score":sc,"Signal":sig,"Trend":trend,
                        "RSI-EMA":round(float(r['RSI_EMA']),1),"Stoch K":round(float(r['STOCH_K']),1),"Stoch D":round(float(r['STOCH_D']),1),
                        "MACD Hist":round(float(r['MACD_Hist']),4),"RVOL":round(rvol,2),"BB%":round(float(r['BB_pct']),2),
                        "ROC 3B%":round(float(r['ROC3'])*100,2),"VWAP":int(float(r['VWAP'])),"TP":int(tp),"SL":int(sl),
                        "R:R":round(rr,1),"Turnover(M)":round(turnover/1e6,1),"Reasons":" · ".join(reasons),"_class":get_card_class(sig)})
                except: continue
            prog_ph.empty()
            st.session_state.scan_results = results
            st.session_state.last_scan_time = datetime.now(jakarta_tz).timestamp()  # fresh timestamp
            st.session_state.last_scan_mode = scan_mode
            # Telegram alert
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
            prog_ph.empty()
            st.error(f"Scan error: {str(e)[:100]}")

    # Display countdown
    if st.session_state.last_scan_time:
        _now_cd   = datetime.now(jakarta_tz).timestamp()
        _rem_cd   = max(0, 300 - (_now_cd - st.session_state.last_scan_time))
        _mnt_cd   = int(_rem_cd//60); _sec_cd = int(_rem_cd%60)
        _last_cd  = datetime.fromtimestamp(st.session_state.last_scan_time, jakarta_tz).strftime("%H:%M:%S")
        st.caption(f"⏱️ Next auto-scan: {_mnt_cd:02d}:{_sec_cd:02d} · Last: {_last_cd} WIB")

    # Show results
    results = st.session_state.scan_results
    if not results and not do_scan:
        st.markdown(f"""
        <div style="text-align:center;padding:48px;color:#4a5568;font-family:Space Mono,monospace;">
          <div style="font-size:36px;margin-bottom:12px;">🔥</div>
          <div style="font-size:13px;letter-spacing:2px;">KLIK SCAN UNTUK MULAI</div>
          <div style="font-size:10px;margin-top:8px;color:#2d3748;">
            {"⚡ Quick: 200 saham" if quick_mode else f"Full: {len(raw_stocks)} saham"} · Regime: {regime} · Auto mode: {rcfg["mode"]}
          </div>
        </div>""", unsafe_allow_html=True)
    elif results:
        df_out=pd.DataFrame(results).sort_values("Score",ascending=False).reset_index(drop=True)
        gacor=df_out[df_out["Signal"].str.contains("GACOR|REVERSAL",na=False)]
        potensi=df_out[df_out["Signal"].str.contains("POTENSIAL",na=False)]
        avg_rsi=df_out['RSI-EMA'].mean()
        st.markdown(f"""
        <div class="metric-row">
          <div class="metric-card" style="border-top-color:{rcolor}"><div class="metric-label">Regime</div>
            <div class="metric-value" style="font-size:16px;color:{rcolor}">{regime}</div>
            <div class="metric-sub">{ihsg_price:,.0f} {chg_sym}{abs(ihsg_chg):.2f}%</div></div>
          <div class="metric-card orange"><div class="metric-label">Mode</div>
            <div class="metric-value" style="font-size:13px;margin-top:4px;">{scan_mode}</div></div>
          <div class="metric-card green"><div class="metric-label">Signal Lolos</div>
            <div class="metric-value">{len(df_out)}</div><div class="metric-sub">dari {len(raw_stocks)} emiten</div></div>
          <div class="metric-card red"><div class="metric-label">GACOR 🔥</div>
            <div class="metric-value">{len(gacor)}</div><div class="metric-sub">score ≥ 5</div></div>
          <div class="metric-card amber"><div class="metric-label">POTENSIAL</div>
            <div class="metric-value">{len(potensi)}</div></div>
          <div class="metric-card"><div class="metric-label">Avg RSI-EMA</div>
            <div class="metric-value" style="color:{'#00ff88' if avg_rsi>50 else '#ffb700' if avg_rsi>35 else '#ff3d5a'}">{avg_rsi:.1f}</div>
            <div class="metric-sub">{'Bullish' if avg_rsi>50 else 'Neutral' if avg_rsi>35 else 'Oversold'}</div></div>
        </div>""", unsafe_allow_html=True)

        th='<div class="tape-wrap"><div class="tape-inner">'
        for _,row in df_out.iterrows():
            roc=row['ROC 3B%']; cls='up' if roc>0 else('down' if roc<0 else'flat'); sym='▲' if roc>0 else('▼' if roc<0 else'─')
            th+=f'<span class="tape-item {cls}">{row["Ticker"]} {int(row["Price"])} {sym}{abs(roc):.1f}% [{row["Signal"]}]</span>'
        th+=th.replace('tape-inner">',''); th+='</div></div>'
        st.markdown(th, unsafe_allow_html=True)

        if not gacor.empty:
            st.markdown(f'<div class="alert-box"><div class="alert-title">🚨 GACOR ALERT · {len(gacor)} SAHAM · {scan_mode}</div><div style="font-size:11px;color:#4a5568;margin-top:4px;">Score ≥ 5 · Konfirmasi multi-indikator 15M · R:R optimal</div></div>', unsafe_allow_html=True)

        if view_mode=="Card View 🃏":
            st.markdown('<div class="section-title">Signal Cards</div>', unsafe_allow_html=True)
            card_html='<div class="signal-grid">'
            for _,row in df_out.head(20).iterrows():
                sc_int=int(row['Score'])
                bars=''.join([f'<div class="sc-bar {"filled" if i<sc_int else "empty"}" style="width:28px"></div>' for i in range(6)])
                roc_c='#00ff88' if row['ROC 3B%']>0 else'#ff3d5a'
                trend_e="📈" if "▲" in row['Trend'] else("📉" if "▼" in row['Trend'] else"➡️")
                card_html+=f"""<div class="signal-card {row['_class']}">
                  <div style="display:flex;justify-content:space-between;align-items:flex-start;">
                    <div><div class="sc-ticker">{row['Ticker']}</div><div class="sc-price" style="color:{roc_c}">{int(row['Price']):,} {trend_e}</div></div>
                    <div style="text-align:right;"><div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;">SCORE</div>
                    <div style="font-family:Space Mono,monospace;font-size:20px;font-weight:700;color:{'#00ff88' if sc_int>=5 else '#ffb700' if sc_int>=4 else '#00e5ff'}">{row['Score']}</div></div>
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
                  <div style="margin-top:6px;display:flex;gap:8px;flex-wrap:wrap;">
                    <div style="font-family:Space Mono,monospace;font-size:9px;padding:2px 8px;border-radius:10px;background:rgba(0,0,0,.3);color:#4a5568;">
                      📍 {row.get('Pivot Pos','-')}
                    </div>
                    <div style="font-family:Space Mono,monospace;font-size:9px;padding:2px 8px;border-radius:10px;background:rgba(0,0,0,.3);color:#4a5568;">
                      PP {row.get('PP',0):,} · R1 {row.get('R1',0):,} · S1 {row.get('S1',0):,}
                    </div>
                  </div>
                </div>"""
            card_html+='</div>'
            st.markdown(card_html, unsafe_allow_html=True)

        st.markdown('<div class="section-title">Full Signal Table</div>', unsafe_allow_html=True)
        display_cols=["Ticker","Price","Score","Signal","Trend","RSI-EMA","Stoch K","Stoch D","MACD Hist","RVOL","BB%","ROC 3B%","VWAP","TP","SL","R:R","Turnover(M)","Reasons"]
        st.dataframe(df_out[display_cols], width='stretch', hide_index=True, column_config={
            "Score":      st.column_config.ProgressColumn("Score",min_value=0,max_value=6,format="%.1f"),
            "RSI-EMA":    st.column_config.NumberColumn("RSI-EMA",format="%.1f"),
            "Stoch K":    st.column_config.NumberColumn("Stoch K",format="%.1f"),
            "RVOL":       st.column_config.NumberColumn("RVOL",format="%.1fx"),
            "ROC 3B%":    st.column_config.NumberColumn("ROC 3B%",format="%.2f%%"),
            "Turnover(M)":st.column_config.NumberColumn("Turnover(M)",format="Rp%.0fM"),
        })

# ════════════════════════════════════════════════════
#  TAB 2: WATCHLIST ANALYZER
# ════════════════════════════════════════════════════
with tab_watchlist:
    st.markdown("""
    <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-bottom:12px;
         padding:10px 14px;background:#0d1117;border-radius:6px;border-left:3px solid #ff7b00;">
      Analisa mendalam untuk saham pilihan lo &amp; grup. Input ticker IDX (tanpa .JK), pisah koma atau enter.
    </div>""", unsafe_allow_html=True)

    wc1, wc2, wc3 = st.columns([3,1,1])
    with wc1:
        wl_input = st.text_area("Ticker", placeholder="Contoh:\nBBCA\nARCI, ASSA, GOTO\nBBRI, BMRI",
                                height=120, label_visibility="collapsed", key="wl_input")
    with wc2:
        wl_mode = st.radio("Mode", ["Scalping ⚡","Momentum 🚀","Reversal 🎯"], key="wl_mode")
        st.caption(f"Regime suggest: {rcfg['mode']}")
    with wc3:
        st.markdown("<br>", unsafe_allow_html=True)
        wl_run   = st.button("🔍 Analisa", use_container_width=True, key="wl_run")
        wl_tele  = st.button("📡 Kirim Telegram", use_container_width=True, key="wl_tele")
        wl_share = st.button("📋 Copy Hasil", use_container_width=True, key="wl_share")

    if wl_run and wl_input.strip():
        raw_wl = list(dict.fromkeys([t.strip().upper() for line in wl_input.split("\n")
                                     for t in line.split(",") if t.strip()]))
        if raw_wl:
            with st.spinner(f"Menganalisa {len(raw_wl)} saham..."):
                wl_res = []
                for t in raw_wl:
                    df = None
                    try:
                        raw = yf.download(t+".JK", period="5d", interval="15m",
                                          progress=False, auto_adjust=True, threads=False)
                        if not raw.empty:
                            if isinstance(raw.columns, pd.MultiIndex): raw.columns = raw.columns.droplevel(1)
                            df = raw.dropna()
                            if len(df) < 10: df = None
                    except: pass
                    if df is None or len(df) < 55:
                        wl_res.append({"Ticker":t,"Price":0,"Score":0,"Signal":"No data",
                            "RSI-EMA":0,"Stoch K":0,"RVOL":0,"BB%":0,"Trend":"-",
                            "TP":0,"SL":0,"R:R":0,"ROC 3B%":0,"VWAP":0,"ATR":0,
                            "Reasons":"No data","_class":"","MACD Hist":0}); continue
                    try:
                        df = apply_intraday_indicators(df)
                        r=df.iloc[-1]; p=df.iloc[-2]; p2=df.iloc[-3] if len(df)>=3 else p
                        close=float(r['Close']); atr=float(r['ATR'])
                        slm = rcfg.get("sl_mult", 0.8)
                        if wl_mode=="Scalping ⚡":   sc,reasons,_=score_scalping(r,p,p2);  tp=close+1.5*atr; sl=close-slm*atr
                        elif wl_mode=="Momentum 🚀": sc,reasons,_=score_momentum(r,p,p2);  tp=close+2.0*atr; sl=close-slm*atr
                        else:                        sc,reasons,_=score_reversal(r,p,p2);  tp=close+2.5*atr; sl=close-slm*atr
                        sig=get_signal(sc,wl_mode); rr=(tp-close)/max(close-sl,0.01)
                        e9=float(r['EMA9']); e21=float(r['EMA21']); e50=float(r['EMA50'])
                        trend="▲ UP" if e9>e21>e50 else("▼ DOWN" if e9<e21<e50 else "◆ SIDE")
                        # Pivot + MTF untuk watchlist
                        _wl_pvt = fetch_pivot_data(t+".JK")
                        _wl_pvt_pos = get_pivot_position(close, _wl_pvt)[0] if _wl_pvt else "-"
                        _wl_mtf = score_mtf(t+".JK", mode=wl_mode)
                        _wl_align, _wl_align_col, _wl_avg = mtf_alignment(_wl_mtf)
                        wl_res.append({"Ticker":t,"Price":int(close),"Score":sc,"Signal":sig,
                            "Trend":trend,"RSI-EMA":round(float(r['RSI_EMA']),1),
                            "Stoch K":round(float(r['STOCH_K']),1),"RVOL":round(float(r['RVOL']),2),
                            "BB%":round(float(r['BB_pct']),2),"ROC 3B%":round(float(r['ROC3'])*100,2),
                            "VWAP":int(float(r['VWAP'])),"TP":int(tp),"SL":int(sl),"R:R":round(rr,1),
                            "ATR":round(atr,0),"MACD Hist":round(float(r['MACD_Hist']),4),
                            "Reasons":" · ".join(reasons),"_class":get_card_class(sig),
                            "Pivot Pos":_wl_pvt_pos,
                            "PP":int(_wl_pvt["PP"]) if _wl_pvt else 0,
                            "R1":int(_wl_pvt["R1"]) if _wl_pvt else 0,
                            "S1":int(_wl_pvt["S1"]) if _wl_pvt else 0,
                            "MTF Align":_wl_align,
                            "M15":_wl_mtf.get("M15",0),"H1":_wl_mtf.get("H1",0),"D1":_wl_mtf.get("D1",0)})
                    except Exception as ex:
                        wl_res.append({"Ticker":t,"Price":0,"Score":0,"Signal":f"Err:{str(ex)[:20]}",
                            "RSI-EMA":0,"Stoch K":0,"RVOL":0,"BB%":0,"Trend":"-",
                            "TP":0,"SL":0,"R:R":0,"ROC 3B%":0,"VWAP":0,"ATR":0,
                            "Reasons":"","_class":"","MACD Hist":0})

            st.session_state.wl_results  = wl_res
            st.session_state.wl_mode_used = wl_mode

            # Auto kirim Telegram kalau ada signal bagus
            wl_top = [r for r in wl_res if r["Price"]>0 and
                      ("GACOR" in r.get("Signal","") or "REVERSAL" in r.get("Signal","") or "POTENSIAL" in r.get("Signal",""))]
            if wl_top:
                send_telegram(wl_top[:5], source="Watchlist")
                st.success(f"📡 Alert terkirim ke Telegram: {len(wl_top)} signal!")

            # Summary metrics
            ok  = [r for r in wl_res if r["Score"]>0]
            gcr = [r for r in ok if "GACOR" in r.get("Signal","") or "REVERSAL" in r.get("Signal","")]
            pot = [r for r in ok if "POTENSIAL" in r.get("Signal","")]
            st.markdown(f"""
            <div class="metric-row" style="margin-top:16px;">
              <div class="metric-card orange"><div class="metric-label">Dipantau</div><div class="metric-value">{len(raw_wl)}</div></div>
              <div class="metric-card green"><div class="metric-label">GACOR 🔥</div><div class="metric-value">{len(gcr)}</div></div>
              <div class="metric-card amber"><div class="metric-label">POTENSIAL</div><div class="metric-value">{len(pot)}</div></div>
              <div class="metric-card"><div class="metric-label">Data OK</div><div class="metric-value">{len(ok)}</div></div>
            </div>""", unsafe_allow_html=True)

            # Cards
            ch = '<div class="signal-grid">'
            for row in sorted(wl_res, key=lambda x: x["Score"], reverse=True):
                if row["Price"]==0:
                    ch += f'<div class="signal-card"><div class="sc-ticker">{row["Ticker"]}</div><div style="font-size:11px;color:#4a5568;margin-top:6px;">{row.get("Signal","No data")}</div></div>'
                    continue
                sc_int=int(row["Score"]); bars=''.join([f'<div class="sc-bar {"filled" if i<sc_int else "empty"}" style="width:26px"></div>' for i in range(6)])
                sig=row.get("Signal","-")
                sc_col="#00ff88" if ("GACOR" in sig or "REVERSAL" in sig) else("#ffb700" if "POTENSIAL" in sig else "#00e5ff" if "WATCH" in sig else "#4a5568")
                rsi_v=row["RSI-EMA"]; rsi_c="#ff3d5a" if rsi_v<30 else("#ffb700" if rsi_v<45 else "#00ff88" if rsi_v>60 else "#c9d1d9")
                roc_c="#00ff88" if row.get("ROC 3B%",0)>0 else "#ff3d5a"
                te="📈" if "▲" in row["Trend"] else("📉" if "▼" in row["Trend"] else "➡️")
                ch += f"""<div class="signal-card {row['_class']}">
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
                  <div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap;">
                    <div style="font-family:Space Mono,monospace;font-size:9px;padding:2px 7px;border-radius:10px;background:rgba(0,0,0,.3);color:#4a5568;">📍 {row.get('Pivot Pos','-')}</div>
                    <div style="font-family:Space Mono,monospace;font-size:9px;padding:2px 7px;border-radius:10px;background:rgba(0,0,0,.3);color:#4a5568;">MTF: {row.get('MTF Align','-')}</div>
                  </div>
                  <div style="font-family:Space Mono,monospace;font-size:9px;color:#4a5568;margin-top:4px;">
                    M15:{row.get('M15',0)} · H1:{row.get('H1',0)} · D1:{row.get('D1',0)} &nbsp;|&nbsp; PP:{row.get('PP',0):,} · R1:{row.get('R1',0):,} · S1:{row.get('S1',0):,}
                  </div>
                </div>"""
            ch += '</div>'
            st.markdown(ch, unsafe_allow_html=True)

            # Table
            df_wl = pd.DataFrame([r for r in wl_res if r["Price"]>0])
            if not df_wl.empty:
                show = ["Ticker","Price","Score","Signal","Trend","RSI-EMA","Stoch K","RVOL","BB%","ROC 3B%","VWAP","TP","SL","R:R","MTF Align","M15","H1","D1","Pivot Pos","PP","R1","S1","ATR","Reasons"]
                show = [c for c in show if c in df_wl.columns]
                st.dataframe(df_wl[show], width='stretch', hide_index=True, column_config={
                    "Score":   st.column_config.ProgressColumn("Score",min_value=0,max_value=6,format="%.1f"),
                    "RSI-EMA": st.column_config.NumberColumn("RSI-EMA",format="%.1f"),
                    "RVOL":    st.column_config.NumberColumn("RVOL",format="%.2fx"),
                    "ROC 3B%": st.column_config.NumberColumn("ROC 3B%",format="%.2f%%"),
                })

    if wl_tele and st.session_state.wl_results:
        to_send = [r for r in st.session_state.wl_results if r["Price"]>0]
        if to_send:
            send_telegram(to_send[:5], source="Watchlist")
            st.success(f"📡 Terkirim: {min(5,len(to_send))} teratas!")

    if wl_share and st.session_state.wl_results:
        now_str = datetime.now(jakarta_tz).strftime("%d %b %Y %H:%M")
        txt = f"🔥 THETA TURBO WATCHLIST\n⏰ {now_str} WIB\n📊 Mode: {st.session_state.get('wl_mode_used','')} | Regime: {regime}\n"+"─"*28+"\n"
        for r in sorted(st.session_state.wl_results, key=lambda x: x["Score"], reverse=True):
            if r["Price"]==0: continue
            sig=r.get("Signal","-")
            em="🔥" if ("GACOR" in sig or "REVERSAL" in sig) else("⚡" if "POTENSIAL" in sig else "👀")
            txt+=f"{em} {r['Ticker']} | {r['Price']:,} | Score:{r['Score']} | RSI:{r['RSI-EMA']} | {sig}\n"
            if r.get("Reasons"): txt+=f"   → {r['Reasons'][:60]}\n"
        txt+="─"*28+"\nby Theta Turbo v5 🚀"
        st.text_area("Copy untuk grup:", txt, height=280, key="share_out")

    if not st.session_state.wl_results and not wl_run:
        st.markdown("""
        <div style="text-align:center;padding:48px;color:#4a5568;font-family:Space Mono,monospace;">
          <div style="font-size:32px;margin-bottom:12px;">👁️</div>
          <div style="font-size:12px;letter-spacing:2px;">MASUKKAN TICKER DI ATAS</div>
          <div style="font-size:10px;margin-top:8px;color:#2d3748;">
            Bisa 1 atau banyak · Pisah koma atau enter<br>Contoh: BBCA, ARCI, ASSA, GOTO
          </div>
        </div>""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════
#  TAB 3: BSJP — BELI SORE JUAL PAGI
# ════════════════════════════════════════════════════
with tab_bsjp:
    now_wib = datetime.now(jakarta_tz)
    is_entry_time = (now_wib.hour == 14 and now_wib.minute >= 30) or                     (now_wib.hour == 15 and now_wib.minute <= 45)
    is_exit_time  = (now_wib.hour == 9) or (now_wib.hour == 10 and now_wib.minute == 0)

    # Header BSJP
    st.markdown(f"""
    <div style="background:rgba(191,95,255,.08);border:1px solid rgba(191,95,255,.3);
         border-radius:8px;padding:14px 18px;margin-bottom:16px;">
      <div style="font-family:Space Mono,monospace;font-size:13px;font-weight:700;
                  color:#bf5fff;letter-spacing:1px;">🌙 BELI SORE JUAL PAGI</div>
      <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-top:4px;">
        Entry: <span style="color:#ffb700">14:30 – 15:45 WIB</span> &nbsp;·&nbsp;
        Exit: <span style="color:#00ff88">Besok 09:00 – 10:00 WIB</span> &nbsp;·&nbsp;
        Status: <span style="color:{'#00ff88' if is_entry_time else '#ffb700' if is_exit_time else '#4a5568'}">
          {'🟢 WAKTU ENTRY!' if is_entry_time else '🟡 WAKTU EXIT!' if is_exit_time else '⏳ Tunggu 14:30 WIB'}
        </span>
      </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;
         padding:10px 14px;background:#0d1117;border-radius:6px;border-left:3px solid #bf5fff;margin-bottom:16px;">
      💡 <b style="color:#c9d1d9">Strategi:</b> Beli saham dengan momentum kuat di sore hari (14:30-15:45),
      jual pagi hari berikutnya (09:00-10:00) saat gap up opening.<br>
      Cocok untuk overnight hold dengan risiko terukur.
    </div>
    """, unsafe_allow_html=True)

    bsjp_c1, bsjp_c2 = st.columns([2,1])
    with bsjp_c1:
        bsjp_min_score = st.slider("Min BSJP Score", 0, 6, 4, key="bsjp_score")
        bsjp_min_rvol  = st.slider("Min RVOL", 1.0, 5.0, 1.5, 0.1, key="bsjp_rvol")
    with bsjp_c2:
        bsjp_min_turn  = st.number_input("Min Turnover (M Rp)", value=500, step=100, key="bsjp_turn") * 1_000_000
        bsjp_tele      = st.toggle("📡 Telegram Alert", value=True, key="bsjp_tele")

    do_bsjp = st.button("🌙 SCAN BSJP SEKARANG", type="primary", use_container_width=True, key="btn_bsjp")

    if "bsjp_results" not in st.session_state: st.session_state.bsjp_results = []

    if do_bsjp:
        bsjp_prog = st.empty()
        bsjp_prog.info("🌙 Scanning BSJP candidates...")
        bsjp_res = []
        scan_data = st.session_state.get("data_dict", {})

        # Kalau data_dict kosong, fetch dulu
        if not scan_data:
            bsjp_prog.warning("⚠️ Jalankan Scanner Intraday dulu, atau scan khusus BSJP di bawah...")
            try:
                scan_data = fetch_intraday(tuple(stocks_yf[:200]))  # quick 200 untuk BSJP
            except: pass

        pb_bsjp = st.progress(0)
        tickers_bsjp = list(scan_data.keys())

        for i, ticker_yf in enumerate(tickers_bsjp):
            pb_bsjp.progress((i+1)/max(len(tickers_bsjp),1))
            try:
                df = scan_data[ticker_yf].copy()
                if len(df) < 55: continue

                # Filter hanya data sore (13:00-16:00 WIB = 06:00-09:00 UTC)
                df_copy = apply_intraday_indicators(df)

                # Gunakan bar terakhir (sore hari)
                r=df_copy.iloc[-1]; p=df_copy.iloc[-2]; p2=df_copy.iloc[-3] if len(df_copy)>=3 else p
                close=float(r['Close']); vol=float(r['Volume'])
                turnover=close*vol; rvol=float(r['RVOL'])

                if turnover < bsjp_min_turn or rvol < bsjp_min_rvol: continue

                sc, reasons, _ = score_bsjp(r, p, p2)
                if sc < bsjp_min_score: continue

                # Signal label
                if sc >= 5:   bsjp_sig = "STRONG BUY 🌙"
                elif sc >= 4: bsjp_sig = "BUY ⚡"
                else:         bsjp_sig = "WATCH 👀"

                # TP/SL untuk overnight
                atr = float(r['ATR'])
                tp  = close + 2.0*atr   # overnight TP lebih lebar
                sl  = close - 1.0*atr
                rr  = (tp-close)/max(close-sl,0.01)

                # Pivot points
                pvt = fetch_pivot_data(ticker_yf)
                pvt_pos, pvt_col = get_pivot_position(close, pvt)[:2] if pvt else ("-","#4a5568")

                e9=float(r['EMA9']); e21=float(r['EMA21']); e50=float(r['EMA50'])
                trend="▲ UP" if e9>e21>e50 else("▼ DOWN" if e9<e21<e50 else"◆ SIDE")

                bsjp_res.append({
                    "Ticker":stock_map.get(ticker_yf, ticker_yf.replace(".JK","")),
                    "Price":int(close),"Score":sc,"Signal":bsjp_sig,"Trend":trend,
                    "RSI-EMA":round(float(r['RSI_EMA']),1),"Stoch K":round(float(r['STOCH_K']),1),
                    "RVOL":round(rvol,2),"TP":int(tp),"SL":int(sl),"R:R":round(rr,1),
                    "Turnover(M)":round(turnover/1e6,1),"Pivot Pos":pvt_pos,
                    "PP":int(pvt["PP"]) if pvt else 0,
                    "R1":int(pvt["R1"]) if pvt else 0,
                    "S1":int(pvt["S1"]) if pvt else 0,
                    "Reasons":" · ".join(reasons),
                    "_class":"gacor" if sc>=5 else "potensial" if sc>=4 else "watch"
                })
            except: continue

        pb_bsjp.empty()
        bsjp_prog.empty()
        bsjp_res = sorted(bsjp_res, key=lambda x: x["Score"], reverse=True)
        st.session_state.bsjp_results = bsjp_res

        # Telegram
        if bsjp_tele and bsjp_res:
            now_b = datetime.now(jakarta_tz)
            sep = "━"*28
            msg = (f"🌙 *BSJP ALERT — BELI SORE JUAL PAGI*\n"
                   f"⏰ `{now_b.strftime('%H:%M:%S')} WIB` · `{now_b.strftime('%d %b %Y')}`\n{sep}\n")
            for r in bsjp_res[:5]:
                bar = "█"*int(r['Score'])+"░"*(6-int(r['Score']))
                msg += (f"\n🌙 *{r['Ticker']}* `{r['Signal']}`\n"
                        f"   💰 Price: `{r['Price']:,}` {('📈' if '▲' in r['Trend'] else '📉' if '▼' in r['Trend'] else '➡️')}\n"
                        f"   📊 Score: `[{bar}] {r['Score']}/6`\n"
                        f"   📈 RSI-EMA: `{r['RSI-EMA']}` | RVOL: `{r['RVOL']}x`\n"
                        f"   🎯 TP: `{r['TP']:,}` | 🛑 SL: `{r['SL']:,}` | R:R `{r['R:R']}`\n"
                        f"   📍 Pivot: `{r['Pivot Pos']}`\n"
                        f"   💡 _{r['Reasons'][:50]}_\n")
            msg += f"\n{sep}\n🌙 _Entry 14:30-15:45 · Exit besok 09:00-10:00_\n⚠️ _BUKAN saran investasi!_"
            try:
                requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                              data={"chat_id":CHAT_ID,"text":msg,"parse_mode":"Markdown"}, timeout=10)
            except: pass

    # Display BSJP results
    bsjp_results = st.session_state.bsjp_results
    if bsjp_results:
        strong = [r for r in bsjp_results if "STRONG" in r.get("Signal","")]
        buy    = [r for r in bsjp_results if r.get("Signal","")=="BUY ⚡"]

        st.markdown(f"""
        <div class="metric-row">
          <div class="metric-card" style="border-top-color:#bf5fff"><div class="metric-label">Dipindai</div>
            <div class="metric-value">{len(bsjp_results)}</div></div>
          <div class="metric-card green"><div class="metric-label">Strong Buy 🌙</div>
            <div class="metric-value">{len(strong)}</div></div>
          <div class="metric-card amber"><div class="metric-label">Buy ⚡</div>
            <div class="metric-value">{len(buy)}</div></div>
          <div class="metric-card"><div class="metric-label">Entry</div>
            <div class="metric-value" style="font-size:13px;color:#ffb700">14:30</div>
            <div class="metric-sub">sampai 15:45 WIB</div></div>
          <div class="metric-card"><div class="metric-label">Exit</div>
            <div class="metric-value" style="font-size:13px;color:#00ff88">09:00</div>
            <div class="metric-sub">besok pagi WIB</div></div>
        </div>""", unsafe_allow_html=True)

        # Top 3 highlight
        if len(bsjp_results) >= 1:
            medals = ["🥇","🥈","🥉"]
            cols_top = st.columns(min(3, len(bsjp_results)))
            for idx, col in enumerate(cols_top):
                if idx >= len(bsjp_results): break
                row = bsjp_results[idx]
                sig_col = "#00ff88" if "STRONG" in row["Signal"] else "#ffb700"
                with col:
                    st.markdown(f"""
                    <div style="background:#0d1117;border:1px solid {sig_col}44;border-radius:10px;
                         padding:16px;text-align:center;border-top:3px solid {sig_col};">
                      <div style="font-size:24px">{medals[idx]}</div>
                      <div style="font-family:Space Mono,monospace;font-size:18px;font-weight:700;color:#e6edf3;">{row['Ticker']}</div>
                      <div style="font-family:Space Mono,monospace;font-size:28px;font-weight:700;color:{sig_col};">{row['Score']}</div>
                      <div style="font-size:11px;font-weight:700;color:{sig_col};">{row['Signal']}</div>
                      <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-top:6px;">
                        RVOL {row['RVOL']}x · RSI {row['RSI-EMA']}
                      </div>
                      <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;">
                        TP {row['TP']:,} · SL {row['SL']:,}
                      </div>
                    </div>""", unsafe_allow_html=True)

        # Full cards
        st.markdown('<div class="section-title">Semua Kandidat BSJP</div>', unsafe_allow_html=True)
        bsjp_html = '<div class="signal-grid">'
        for row in bsjp_results:
            sc_int = int(row["Score"])
            bars   = ''.join([f'<div class="sc-bar {"filled" if i<sc_int else "empty"}" style="width:26px"></div>' for i in range(6)])
            sig    = row.get("Signal","-")
            sc_col = "#00ff88" if "STRONG" in sig else "#ffb700" if "BUY" in sig else "#00e5ff"
            te     = "📈" if "▲" in row["Trend"] else ("📉" if "▼" in row["Trend"] else "➡️")
            bsjp_html += f"""<div class="signal-card {row['_class']}">
              <div style="display:flex;justify-content:space-between;">
                <div><div class="sc-ticker">{row['Ticker']}</div>
                <div class="sc-price">{row['Price']:,} {te}</div></div>
                <div style="text-align:right">
                  <div style="font-family:Space Mono,monospace;font-size:9px;color:#4a5568">SCORE</div>
                  <div style="font-family:Space Mono,monospace;font-size:22px;font-weight:700;color:{sc_col}">{row['Score']}</div>
                </div>
              </div>
              <div class="sc-signal" style="color:{sc_col}">{sig}</div>
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
              <div style="margin-top:6px;font-family:Space Mono,monospace;font-size:9px;color:#4a5568;">
                📍 {row['Pivot Pos']} · PP {row['PP']:,}
              </div>
              <div style="margin-top:4px;font-size:10px;color:#4a5568;line-height:1.4;font-family:Space Mono,monospace">{row['Reasons'][:70]}</div>
            </div>"""
        bsjp_html += '</div>'
        st.markdown(bsjp_html, unsafe_allow_html=True)

        # Table
        df_bsjp = pd.DataFrame(bsjp_results)
        show_cols = ["Ticker","Price","Score","Signal","Trend","RSI-EMA","Stoch K","RVOL","TP","SL","R:R","Pivot Pos","PP","R1","S1","Turnover(M)","Reasons"]
        show_cols = [c for c in show_cols if c in df_bsjp.columns]
        st.dataframe(df_bsjp[show_cols], width='stretch', hide_index=True, column_config={
            "Score": st.column_config.ProgressColumn("Score",min_value=0,max_value=6,format="%.1f"),
            "RVOL":  st.column_config.NumberColumn("RVOL",format="%.2fx"),
        })

    elif not do_bsjp:
        st.markdown("""
        <div style="text-align:center;padding:48px;color:#4a5568;font-family:Space Mono,monospace;">
          <div style="font-size:32px;margin-bottom:12px;">🌙</div>
          <div style="font-size:12px;letter-spacing:2px;">KLIK SCAN BSJP</div>
          <div style="font-size:10px;margin-top:8px;color:#2d3748;">
            Best digunakan jam 14:00–15:45 WIB<br>
            Entry sore → jual besok pagi gap up 🚀
          </div>
        </div>""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════
#  TAB 4: SEKTOR ROTATION
# ════════════════════════════════════════════════════
with tab_sector:
    st.markdown("""
    <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-bottom:14px;
         padding:10px 14px;background:#0d1117;border-radius:6px;border-left:3px solid #ff7b00;">
      Track pergerakan tiap sektor IDX hari ini. Sektor merah = hindari, hijau = fokus di sana.
      <br>⚡ <b style="color:#ffb700">Hormuz-sensitive:</b> Energi, Shipping, Petrokimia
    </div>""", unsafe_allow_html=True)

    do_sector = st.button("🏭 REFRESH SEKTOR", type="primary", use_container_width=True, key="btn_sector")

    if "sector_data" not in st.session_state: st.session_state.sector_data = {}

    if do_sector:
        with st.spinner("Mengambil data sektor..."):
            sec_data = {}
            for sec_name, sec_stocks in SECTORS.items():
                results = fetch_sector_rotation(sec_stocks)
                if results:
                    avg_chg  = sum(r["chg"]  for r in results) / len(results)
                    avg_rvol = sum(r["rvol"] for r in results) / len(results)
                    bullish  = sum(1 for r in results if r["chg"] > 0)
                    sec_data[sec_name] = {
                        "avg_chg": round(avg_chg,2), "avg_rvol": round(avg_rvol,2),
                        "bullish": bullish, "total": len(results),
                        "stocks": results, "is_hormuz": sec_name in HORMUZ_SECTORS
                    }
            st.session_state.sector_data = sec_data

    if st.session_state.sector_data:
        # Sort by avg_chg
        sorted_secs = sorted(st.session_state.sector_data.items(),
                             key=lambda x: x[1]["avg_chg"], reverse=True)

        # Summary heatmap
        st.markdown('<div class="section-title">Sektor Heatmap Hari Ini</div>', unsafe_allow_html=True)
        cols_sec = st.columns(3)
        for idx, (sec_name, sec_info) in enumerate(sorted_secs):
            chg  = sec_info["avg_chg"]
            col  = "#00ff88" if chg > 1 else "#ffb700" if chg > 0 else "#ff3d5a"
            bg   = "rgba(0,255,136,.06)" if chg>1 else "rgba(255,183,0,.06)" if chg>0 else "rgba(255,61,90,.06)"
            bdr  = col+"44"
            hormuz_badge = ' <span style="color:#ffb700;font-size:9px">⚡HORMUZ</span>' if sec_info["is_hormuz"] else ""
            bull_pct = int(sec_info["bullish"]/max(sec_info["total"],1)*100)
            with cols_sec[idx % 3]:
                st.markdown(f"""
                <div style="background:{bg};border:1px solid {bdr};border-radius:8px;
                     padding:12px;margin-bottom:10px;">
                  <div style="font-family:Space Mono,monospace;font-size:10px;font-weight:700;
                               color:#c9d1d9;">{sec_name}{hormuz_badge}</div>
                  <div style="font-family:Space Mono,monospace;font-size:22px;font-weight:700;
                               color:{col};margin:4px 0;">{chg:+.2f}%</div>
                  <div style="font-size:9px;color:#4a5568;">
                    RVOL avg: {sec_info['avg_rvol']:.1f}x &nbsp;·&nbsp;
                    Bullish: {sec_info['bullish']}/{sec_info['total']} ({bull_pct}%)
                  </div>
                  <div style="height:4px;background:#1c2533;border-radius:2px;margin-top:6px;overflow:hidden;">
                    <div style="width:{bull_pct}%;height:100%;background:{col};border-radius:2px;"></div>
                  </div>
                </div>""", unsafe_allow_html=True)

        # Top picks per sektor terbaik
        st.markdown('<div class="section-title">Top Saham Per Sektor Terkuat</div>', unsafe_allow_html=True)
        top3_secs = sorted_secs[:3]
        cols_top = st.columns(3)
        for cidx, (sec_name, sec_info) in enumerate(top3_secs):
            with cols_top[cidx]:
                chg   = sec_info["avg_chg"]
                col   = "#00ff88" if chg > 0 else "#ff3d5a"
                st.markdown(f'<div style="font-family:Space Mono,monospace;font-size:11px;color:{col};font-weight:700;margin-bottom:8px;">{sec_name}</div>', unsafe_allow_html=True)
                for stk in sorted(sec_info["stocks"], key=lambda x: x["chg"], reverse=True)[:5]:
                    sc = "#00ff88" if stk["chg"]>0 else "#ff3d5a"
                    st.markdown(f"""
                    <div style="display:flex;justify-content:space-between;padding:5px 0;
                         border-bottom:1px solid #1c2533;font-family:Space Mono,monospace;font-size:10px;">
                      <span style="color:#c9d1d9;">{stk['ticker']}</span>
                      <span style="color:{sc}">{stk['chg']:+.1f}%</span>
                      <span style="color:#4a5568;">RVOL {stk['rvol']}x</span>
                    </div>""", unsafe_allow_html=True)
    # ── BETA ANALYSIS SECTION ──
    st.markdown('<div class="section-title" style="margin-top:24px;">Beta & Relative Strength vs IHSG</div>', unsafe_allow_html=True)
    st.markdown("""
    <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-bottom:12px;
         padding:8px 12px;background:#0d1117;border-radius:6px;">
      Beta mengukur seberapa besar sektor ikut jatuh/naik saat IHSG bergerak.<br>
      🛡️ Beta &lt; 0.8 = Defensive (tahan banting) &nbsp;·&nbsp; 🔴 Beta &gt; 1.2 = Amplifier (kena hajar duluan)
    </div>""", unsafe_allow_html=True)

    do_beta = st.button("🔬 Hitung Beta Semua Sektor", use_container_width=True, key="btn_beta")
    if "beta_data" not in st.session_state: st.session_state.beta_data = []

    if do_beta:
        beta_res = []
        bp = st.progress(0)
        secs = list(SECTORS.items())
        for i, (sec_name, sec_stocks) in enumerate(secs):
            bp.progress((i+1)/len(secs))
            res = calc_sector_beta(sec_name, sec_stocks)
            if res: beta_res.append(res)
        bp.empty()
        beta_res = sorted(beta_res, key=lambda x: x["beta"])
        st.session_state.beta_data = beta_res

    if st.session_state.beta_data:
        beta_data = st.session_state.beta_data

        # Summary tiles — defensive to aggressive
        st.markdown("**Ranking: Paling Defensive → Paling Agresif**", unsafe_allow_html=False)
        for b in beta_data:
            beta_lbl, beta_col = get_beta_label(b["beta"])
            rs_col   = "#00ff88" if b["rs5"]>0 else "#ff3d5a"
            down_col = "#00ff88" if b["avg_down"]>0 else "#ff3d5a"
            hormuz   = " ⚡" if b["sector"] in HORMUZ_SECTORS else ""
            width    = min(100, int(abs(b["beta"])*50))

            st.markdown(f"""
            <div style="background:#0d1117;border:1px solid #1c2533;border-radius:8px;
                 padding:12px 16px;margin-bottom:8px;border-left:4px solid {beta_col};">
              <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;">
                <div style="flex:2;">
                  <div style="font-family:Space Mono,monospace;font-size:11px;font-weight:700;color:#c9d1d9;">
                    {b['sector']}{hormuz}
                  </div>
                  <div style="font-family:Space Mono,monospace;font-size:9px;color:#4a5568;margin-top:2px;">
                    Korrelasi IHSG: {b['corr']} &nbsp;·&nbsp; 1M Return: {b['ret_1m_sec']:+.1f}%
                  </div>
                </div>
                <div style="text-align:center;min-width:80px;">
                  <div style="font-family:Space Mono,monospace;font-size:20px;font-weight:700;color:{beta_col};">{b['beta']}</div>
                  <div style="font-size:9px;color:{beta_col};">{beta_lbl}</div>
                </div>
                <div style="text-align:center;min-width:80px;">
                  <div style="font-family:Space Mono,monospace;font-size:14px;font-weight:700;color:{rs_col};">{b['rs5']:+.1f}%</div>
                  <div style="font-size:9px;color:#4a5568;">RS 5 Hari</div>
                </div>
                <div style="text-align:center;min-width:80px;">
                  <div style="font-family:Space Mono,monospace;font-size:14px;font-weight:700;color:{down_col};">{b['avg_down']:+.2f}%</div>
                  <div style="font-size:9px;color:#4a5568;">Avg saat IHSG ↓</div>
                </div>
              </div>
              <div style="height:4px;background:#1c2533;border-radius:2px;margin-top:10px;overflow:hidden;">
                <div style="width:{width}%;height:100%;background:{beta_col};border-radius:2px;transition:width .3s;"></div>
              </div>
            </div>""", unsafe_allow_html=True)

        # Insight box
        defensive = [b for b in beta_data if b["beta"] < 0.8]
        aggressive = [b for b in beta_data if b["beta"] > 1.2]
        st.markdown(f"""
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:16px;">
          <div style="background:rgba(0,255,136,.06);border:1px solid rgba(0,255,136,.2);border-radius:8px;padding:14px;">
            <div style="font-family:Space Mono,monospace;font-size:10px;font-weight:700;color:#00ff88;margin-bottom:8px;">
              🛡️ SEKTOR DEFENSIVE — AMAN SAAT IHSG MERAH
            </div>
            {"".join(f'<div style="font-family:Space Mono,monospace;font-size:10px;color:#c9d1d9;margin-bottom:3px;">• {b["sector"]} (β={b["beta"]})</div>' for b in defensive) or '<div style="color:#4a5568;font-size:10px;">Tidak ada sektor yang sangat defensive</div>'}
          </div>
          <div style="background:rgba(255,61,90,.06);border:1px solid rgba(255,61,90,.2);border-radius:8px;padding:14px;">
            <div style="font-family:Space Mono,monospace;font-size:10px;font-weight:700;color:#ff3d5a;margin-bottom:8px;">
              🔴 SEKTOR AGRESIF — KENA HAJAR DULUAN
            </div>
            {"".join(f'<div style="font-family:Space Mono,monospace;font-size:10px;color:#c9d1d9;margin-bottom:3px;">• {b["sector"]} (β={b["beta"]})</div>' for b in aggressive) or '<div style="color:#4a5568;font-size:10px;">Tidak ada sektor yang ekstrem agresif</div>'}
          </div>
        </div>""", unsafe_allow_html=True)

    else:
        st.markdown("""
        <div style="text-align:center;padding:48px;color:#4a5568;font-family:Space Mono,monospace;">
          <div style="font-size:32px;margin-bottom:12px;">🏭</div>
          <div style="font-size:12px;letter-spacing:2px;">KLIK REFRESH SEKTOR atau HITUNG BETA</div>
          <div style="font-size:10px;margin-top:8px;color:#2d3748;">
            Track sektor mana yang paling hot hari ini<br>
            ⚡ Hormuz open → Energi, Shipping, Petrokimia<br>
            🛡️ Beta analysis → sektor mana yang tahan banting
          </div>
        </div>""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════
#  TAB 5: GAP UP SCANNER
# ════════════════════════════════════════════════════
with tab_gapup:
    st.markdown("""
    <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-bottom:14px;
         padding:10px 14px;background:#0d1117;border-radius:6px;border-left:3px solid #00ff88;">
      Deteksi saham yang berpotensi <b style="color:#00ff88">Gap Up besok pagi</b> (09:00-10:00 WIB).<br>
      Entry terbaik: sore hari sebelum close, atau esok pagi sebelum gap terkonfirmasi.
    </div>""", unsafe_allow_html=True)

    gu_c1, gu_c2 = st.columns(2)
    with gu_c1:
        gu_min_score = st.slider("Min Gap Score", 1, 6, 3, key="gu_score")
    with gu_c2:
        gu_quick = st.toggle("⚡ Quick Scan (200)", value=True, key="gu_quick")

    do_gapup = st.button("📈 SCAN GAP UP SEKARANG", type="primary", use_container_width=True, key="btn_gapup")

    if "gapup_results" not in st.session_state: st.session_state.gapup_results = []

    if do_gapup:
        scan_tickers = stocks_yf[:200] if gu_quick else stocks_yf
        with st.spinner(f"Scanning {len(scan_tickers)} saham untuk Gap Up..."):
            gu_res = scan_gap_up(scan_tickers)
            gu_res = [r for r in gu_res if r["Gap Score"] >= gu_min_score]
            st.session_state.gapup_results = gu_res

        if gu_res and TOKEN and CHAT_ID:
            now_g = datetime.now(jakarta_tz)
            sep = "━"*28
            msg = f"📈 *GAP UP SCANNER*\n⏰ `{now_g.strftime('%H:%M:%S')} WIB`\n{sep}\n"
            for r in gu_res[:5]:
                msg += (f"\n🚀 *{r['Ticker']}* `{r['Signal']}`\n"
                        f"   💰 Price: `{r['Price']:,}` ({r['Chg %']:+.1f}%)\n"
                        f"   📊 Gap Score: `{r['Gap Score']}/6`\n"
                        f"   🌊 RVOL: `{r['RVOL']}x` | Prev High: `{r['Prev High']:,}`\n"
                        f"   💡 _{r['Reasons'][:50]}_\n")
            msg += f"\n{sep}\n📈 _Gap Up Scanner · Entry besok pagi_\n⚠️ _BUKAN saran investasi!_"
            try:
                requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                              data={"chat_id":CHAT_ID,"text":msg,"parse_mode":"Markdown"}, timeout=10)
                st.success("📡 Gap Up alert terkirim ke Telegram!")
            except: pass

    gapup_res = st.session_state.gapup_results
    if gapup_res:
        gap_confirmed = [r for r in gapup_res if "GAP UP" in r.get("Signal","")]
        potential     = [r for r in gapup_res if "POTENTIAL" in r.get("Signal","")]

        st.markdown(f"""
        <div class="metric-row">
          <div class="metric-card green"><div class="metric-label">Gap Confirmed 🚀</div>
            <div class="metric-value">{len(gap_confirmed)}</div></div>
          <div class="metric-card amber"><div class="metric-label">Potential ⚡</div>
            <div class="metric-value">{len(potential)}</div></div>
          <div class="metric-card"><div class="metric-label">Total</div>
            <div class="metric-value">{len(gapup_res)}</div></div>
        </div>""", unsafe_allow_html=True)

        # Cards
        gu_html = '<div class="signal-grid">'
        for row in gapup_res[:20]:
            sc_int = int(min(row["Gap Score"],6))
            bars   = ''.join([f'<div class="sc-bar {"filled" if i<sc_int else "empty"}" style="width:26px"></div>' for i in range(6)])
            is_gap = "GAP UP" in row.get("Signal","")
            sc_col = "#00ff88" if is_gap else "#ffb700"
            chg_c  = "#00ff88" if row["Chg %"]>0 else "#ff3d5a"
            gu_html += f"""<div class="signal-card {'gacor' if is_gap else 'potensial'}">
              <div style="display:flex;justify-content:space-between;">
                <div>
                  <div class="sc-ticker">{row['Ticker']}</div>
                  <div class="sc-price" style="color:{chg_c}">{row['Price']:,} ({row['Chg %']:+.1f}%)</div>
                </div>
                <div style="text-align:right">
                  <div style="font-family:Space Mono,monospace;font-size:9px;color:#4a5568">GAP SCORE</div>
                  <div style="font-family:Space Mono,monospace;font-size:22px;font-weight:700;color:{sc_col}">{row['Gap Score']}</div>
                </div>
              </div>
              <div class="sc-signal" style="color:{sc_col}">{row['Signal']}</div>
              <div class="sc-bars">{bars}</div>
              <div class="sc-stats">
                <div class="sc-stat">RVOL <span>{row['RVOL']}x</span></div>
                <div class="sc-stat">Close% <span>{row['Close Ratio']:.0%}</span></div>
                <div class="sc-stat">PrevHigh <span>{row['Prev High']:,}</span></div>
              </div>
              <div style="margin-top:8px;font-size:10px;color:#4a5568;font-family:Space Mono,monospace;">{row['Reasons'][:80]}</div>
            </div>"""
        gu_html += '</div>'
        st.markdown(gu_html, unsafe_allow_html=True)

        df_gu = pd.DataFrame(gapup_res)
        st.dataframe(df_gu, width='stretch', hide_index=True,
                     column_config={"Gap Score": st.column_config.ProgressColumn("Gap Score",min_value=0,max_value=6,format="%.1f"),
                                    "RVOL": st.column_config.NumberColumn("RVOL",format="%.2fx"),
                                    "Chg %": st.column_config.NumberColumn("Chg %",format="%.2f%%")})
    elif not do_gapup:
        st.markdown("""
        <div style="text-align:center;padding:48px;color:#4a5568;font-family:Space Mono,monospace;">
          <div style="font-size:32px;margin-bottom:12px;">📈</div>
          <div style="font-size:12px;letter-spacing:2px;">KLIK SCAN GAP UP</div>
          <div style="font-size:10px;margin-top:8px;color:#2d3748;">
            Best run: sore hari 14:00–16:00 WIB<br>
            Hasil = kandidat gap up besok pagi 🚀
          </div>
        </div>""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════
#  TAB 6: TRAILING STOP ENGINE
# ════════════════════════════════════════════════════
with tab_trail:
    st.markdown("""
    <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-bottom:14px;
         padding:10px 14px;background:#0d1117;border-radius:6px;border-left:3px solid #bf5fff;">
      Lock profit di market bullish. Trailing Stop otomatis ikut harga naik — tidak turun.<br>
      <b style="color:#ffb700">Tips:</b> Pakai ATR 2x untuk scalping, 3x untuk swing/BSJP.
    </div>""", unsafe_allow_html=True)

    tr_c1, tr_c2 = st.columns(2)
    with tr_c1:
        st.markdown('<div class="settings-label">POSISI LO</div>', unsafe_allow_html=True)
        tr_ticker  = st.text_input("Ticker (tanpa .JK)", value="BBCA", key="tr_ticker").upper()
        tr_entry   = st.number_input("Harga Entry (Rp)", value=9000, step=10, key="tr_entry")
        tr_qty     = st.number_input("Jumlah Lot", value=10, step=1, key="tr_qty")

    with tr_c2:
        st.markdown('<div class="settings-label">SETTING TRAILING</div>', unsafe_allow_html=True)
        tr_method  = st.radio("Metode", ["ATR","Persen","Swing Low"], key="tr_method")
        if tr_method == "ATR":
            tr_atr_mult = st.slider("ATR Multiplier", 1.0, 5.0, 2.0, 0.5, key="tr_atr_m")
        elif tr_method == "Persen":
            tr_pct = st.slider("Trailing %", 1.0, 10.0, 3.0, 0.5, key="tr_pct")
        tr_alert = st.toggle("🔔 Alert Telegram saat kena Stop", value=True, key="tr_alert")

    if st.button("🎯 HITUNG TRAILING STOP", type="primary", use_container_width=True, key="btn_trail"):
        with st.spinner(f"Fetch data {tr_ticker}..."):
            try:
                raw_tr = yf.download(tr_ticker+".JK", period="5d", interval="15m",
                                     progress=False, auto_adjust=True, threads=False)
                if not raw_tr.empty:
                    if isinstance(raw_tr.columns, pd.MultiIndex): raw_tr.columns = raw_tr.columns.droplevel(1)
                    df_tr = apply_intraday_indicators(raw_tr.dropna())
                    current = float(df_tr["Close"].iloc[-1])
                    atr_val = float(df_tr["ATR"].iloc[-1])

                    if tr_method == "ATR":
                        trail_result = calc_trailing_stop(tr_entry, current, atr_val, "ATR", tr_atr_mult)
                    elif tr_method == "Persen":
                        trail_result = calc_trailing_stop(tr_entry, current, atr_val, "Persen", pct=tr_pct)
                    else:
                        trail_result = calc_trailing_stop(tr_entry, current, atr_val, "Swing Low")

                    stop      = trail_result["stop"]
                    dist      = trail_result["distance"]
                    p_float   = trail_result["profit_float"]
                    p_locked  = trail_result["profit_locked"]
                    is_profit = trail_result["is_profitable"]

                    lot_val   = tr_qty * 100  # 1 lot = 100 lembar
                    profit_rp = (current - tr_entry) * lot_val
                    locked_rp = max(0, (stop - tr_entry) * lot_val)

                    stop_col   = "#00ff88" if is_profit else "#ff3d5a"
                    profit_col = "#00ff88" if profit_rp >= 0 else "#ff3d5a"

                    st.markdown(f"""
                    <div style="background:#0d1117;border:1px solid {stop_col}44;border-radius:10px;padding:20px;margin-top:12px;">
                      <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;letter-spacing:2px;margin-bottom:16px;">
                        {tr_ticker} · {tr_method} · Entry {tr_entry:,}
                      </div>
                      <div class="metric-row">
                        <div class="metric-card"><div class="metric-label">Harga Sekarang</div>
                          <div class="metric-value" style="color:#00e5ff">{int(current):,}</div>
                          <div class="metric-sub">ATR: {int(atr_val)}</div></div>
                        <div class="metric-card" style="border-top-color:{stop_col}">
                          <div class="metric-label">🎯 Trailing Stop</div>
                          <div class="metric-value" style="color:{stop_col}">{int(stop):,}</div>
                          <div class="metric-sub">Jarak: {int(dist):,}</div></div>
                        <div class="metric-card" style="border-top-color:{profit_col}">
                          <div class="metric-label">Profit Float</div>
                          <div class="metric-value" style="color:{profit_col}">{p_float:+.1f}%</div>
                          <div class="metric-sub">Rp {profit_rp:,.0f}</div></div>
                        <div class="metric-card" style="border-top-color:#00ff88">
                          <div class="metric-label">Profit Terkunci 🔒</div>
                          <div class="metric-value" style="color:#00ff88">{p_locked:+.1f}%</div>
                          <div class="metric-sub">Rp {locked_rp:,.0f}</div></div>
                      </div>
                      <div style="margin-top:14px;padding:12px;background:rgba(0,0,0,.3);border-radius:6px;">
                        <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-bottom:8px;">PRICE MAP</div>
                        <div style="position:relative;height:16px;background:#1c2533;border-radius:8px;overflow:hidden;">
                          {"" if current == tr_entry else f'<div style="position:absolute;left:{min(99,max(1,int((tr_entry-stop)/(current-stop)*100 if current!=stop else 50)))}%;width:2px;height:100%;background:#4a5568;"></div>'}
                          <div style="position:absolute;left:{min(99,max(1,int((stop/(max(current,stop+1)*1.05))*100)))}%;width:3px;height:100%;background:{stop_col};"></div>
                          <div style="width:{min(100,int((current/max(current*1.05,1))*100))}%;height:100%;background:linear-gradient(90deg,{stop_col},{profit_col});border-radius:8px;opacity:.6;"></div>
                        </div>
                        <div style="display:flex;justify-content:space-between;font-family:Space Mono,monospace;font-size:9px;color:#4a5568;margin-top:4px;">
                          <span>Stop {int(stop):,}</span>
                          <span>Entry {tr_entry:,}</span>
                          <span>Now {int(current):,}</span>
                        </div>
                      </div>
                      <div style="margin-top:12px;font-family:Space Mono,monospace;font-size:10px;color:#4a5568;line-height:1.8;">
                        💼 {tr_qty} lot ({lot_val:,} lembar) &nbsp;·&nbsp;
                        {'✅ Profit sudah terkunci!' if is_profit else '⚠️ Stop masih di bawah entry'}
                      </div>
                    </div>""", unsafe_allow_html=True)

                    # Save state for alert
                    st.session_state["trail_stop"] = {"ticker":tr_ticker,"stop":stop,"current":current,"entry":tr_entry}

                    if tr_alert and TOKEN and CHAT_ID:
                        now_tr = datetime.now(jakarta_tz)
                        msg_tr = (f"🎯 *TRAILING STOP UPDATE*\n"
                                  f"⏰ `{now_tr.strftime('%H:%M:%S')} WIB`\n{'━'*28}\n"
                                  f"📌 *{tr_ticker}* | Metode: {tr_method}\n"
                                  f"💰 Entry: `{tr_entry:,}` → Now: `{int(current):,}`\n"
                                  f"🎯 Trailing Stop: `{int(stop):,}`\n"
                                  f"🔒 Profit terkunci: `{p_locked:+.1f}%` (Rp {locked_rp:,.0f})\n"
                                  f"📊 Float P&L: `{p_float:+.1f}%` (Rp {profit_rp:,.0f})\n"
                                  f"{'━'*28}\n⚠️ _BUKAN saran investasi!_")
                        try:
                            requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                                          data={"chat_id":CHAT_ID,"text":msg_tr,"parse_mode":"Markdown"}, timeout=10)
                            st.success("📡 Trailing stop terkirim ke Telegram!")
                        except: pass
                else:
                    st.error(f"Data {tr_ticker} tidak tersedia")
            except Exception as ex:
                st.error(f"Error: {str(ex)[:80]}")

    # Trailing Stop guide
    with st.expander("📖 Cara Pakai Trailing Stop", expanded=False):
        st.markdown("""
        <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;line-height:2;">
          <b style="color:#c9d1d9">ATR 2x</b> → Scalping 15M, tight trailing<br>
          <b style="color:#c9d1d9">ATR 3x</b> → Swing / BSJP, lebih longgar<br>
          <b style="color:#c9d1d9">Persen 3%</b> → Simple, mudah dipahami<br>
          <b style="color:#c9d1d9">Swing Low</b> → Berdasarkan struktur harga<br>
          <br>
          <b style="color:#ffb700">Tips Market Bullish:</b><br>
          • Biarkan profit berjalan, geser stop seiring naik<br>
          • Jangan close profit terlalu cepat di trend naik<br>
          • Lock 50% posisi di TP1, biarkan 50% lanjut<br>
          • ATR multiplier lebih besar = stop lebih longgar
        </div>""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════
#  TAB 7: BACKTEST
# ════════════════════════════════════════════════════
with tab_backtest:
    st.markdown('<div class="section-title">Backtest Engine · 15M Intraday</div>', unsafe_allow_html=True)
    st.markdown("""
    <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;line-height:1.9;margin-bottom:14px;">
    ℹ️  Entry = bar saat signal terpenuhi &nbsp;·&nbsp; Exit = kena TP / SL / atau N bar ke depan<br>
    ⏱️  1 bar = 15 menit &nbsp;·&nbsp; Jalankan Scanner dulu agar data tersedia
    </div>""", unsafe_allow_html=True)

    bt_c1, bt_c2, bt_c3, bt_c4 = st.columns(4)
    bt_mode    = bt_c1.selectbox("Mode Backtest", ["Scalping ⚡","Momentum 🚀","Reversal 🎯"], key="bt_mode")
    bt_sc      = bt_c2.slider("Min Score Entry", 0, 6, 4, key="bt_sc")
    bt_fwd     = int(bt_c3.number_input("Hold (bars)", value=4, step=1, min_value=1, max_value=20))
    bt_sl_mult = bt_c4.number_input("SL mult (x ATR)", value=0.8, step=0.1, min_value=0.1, max_value=3.0)
    st.caption(f"Hold {bt_fwd} bars × 15 menit = ~{bt_fwd*15} menit per trade")

    if st.button("🚀 Run Backtest", type="primary", key="bt_run"):
        data_dict = st.session_state.get("data_dict", {})
        if not data_dict:
            st.warning("Jalankan Scanner dulu bro! (Tab Scanner → Klik Scan)")
        else:
            bt_results=[]; bt_by_trend={"▲ UP":[],"▼ DOWN":[],"◆ SIDE":[]}
            bt_by_session={"Pagi 09-11":[],"Siang 11-14":[],"Sore 14-16":[]}; bt_by_score={4:[],5:[],6:[]}
            bt_pb=st.progress(0); sample=list(data_dict.keys())[:80]
            for bi, t_yf in enumerate(sample):
                bt_pb.progress((bi+1)/len(sample))
                try:
                    d=data_dict[t_yf].copy()
                    if len(d)<60: continue
                    d=apply_intraday_indicators(d)
                    for ii in range(50, len(d)-bt_fwd):
                        r0=d.iloc[ii]; r1=d.iloc[ii-1]; r2=d.iloc[ii-2]
                        if bt_mode=="Scalping ⚡":   sc,_,_=score_scalping(r0,r1,r2)
                        elif bt_mode=="Momentum 🚀": sc,_,_=score_momentum(r0,r1,r2)
                        else:                         sc,_,_=score_reversal(r0,r1,r2)
                        if sc<bt_sc: continue
                        entry=float(r0['Close']); atr_v=float(r0['ATR']) if not np.isnan(float(r0['ATR'])) else entry*0.005
                        if bt_mode=="Scalping ⚡":   tp_p=entry+1.5*atr_v; sl_p=entry-bt_sl_mult*atr_v
                        elif bt_mode=="Momentum 🚀": tp_p=entry+2.0*atr_v; sl_p=entry-bt_sl_mult*atr_v
                        else:                         tp_p=entry+2.5*atr_v; sl_p=entry-bt_sl_mult*atr_v
                        exit_price=float(d.iloc[ii+bt_fwd]['Close'])
                        for fwd_i in range(1, bt_fwd+1):
                            bar=d.iloc[ii+fwd_i]
                            if float(bar['High'])>=tp_p: exit_price=tp_p; break
                            if float(bar['Low'])<=sl_p:  exit_price=sl_p; break
                        ret=(exit_price-entry)/entry*100; bt_results.append(ret)
                        e9=float(r0['EMA9']); e21=float(r0['EMA21']); e50=float(r0['EMA50'])
                        tr="▲ UP" if e9>e21>e50 else("▼ DOWN" if e9<e21<e50 else "◆ SIDE")
                        bt_by_trend[tr].append(ret)
                        try:
                            hr=d.index[ii].hour
                            if 9<=hr<11: bt_by_session["Pagi 09-11"].append(ret)
                            elif 11<=hr<14: bt_by_session["Siang 11-14"].append(ret)
                            elif 14<=hr<16: bt_by_session["Sore 14-16"].append(ret)
                        except: pass
                        sc_int=int(sc)
                        if sc_int in bt_by_score: bt_by_score[sc_int].append(ret)
                except: continue
            bt_pb.empty()
            if not bt_results:
                st.warning("Tidak ada trades yang match. Turunkan Min Score.")
            else:
                arr=np.array(bt_results); wr=len(arr[arr>0])/len(arr)*100
                avg=np.mean(arr); med=np.median(arr)
                pf=arr[arr>0].sum()/max(abs(arr[arr<0].sum()),0.01)
                mxdd=arr[arr<0].min() if len(arr[arr<0])>0 else 0
                st.markdown(f"""
                <div class="bt-result">
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
                  </div>
                </div>""", unsafe_allow_html=True)
                tab_tr,tab_ses,tab_sc=st.tabs(["📈 Per Trend","⏰ Per Sesi","🎯 Per Score"])
                with tab_tr:
                    for tr_name,vals in bt_by_trend.items():
                        if not vals: continue
                        a=np.array(vals); wr_t=len(a[a>0])/len(a)*100; avg_t=np.mean(a)
                        col="#00ff88" if wr_t>=55 else("#ffb700" if wr_t>=50 else "#ff3d5a")
                        st.markdown(f'<div style="margin-bottom:10px;"><div style="display:flex;justify-content:space-between;"><span style="font-family:Space Mono,monospace;font-size:12px;color:#c9d1d9;">{tr_name}</span><span style="font-family:Space Mono,monospace;font-size:11px;color:{col};">{wr_t:.1f}% WR · avg {avg_t:+.2f}% · {len(a)} trades</span></div><div style="height:8px;background:var(--border);border-radius:4px;overflow:hidden;margin-top:4px;"><div style="width:{int(wr_t)}%;height:100%;background:{col};border-radius:4px;"></div></div></div>', unsafe_allow_html=True)
                with tab_ses:
                    for sname,vals in bt_by_session.items():
                        if not vals: continue
                        a=np.array(vals); wr_s=len(a[a>0])/len(a)*100; avg_s=np.mean(a)
                        col="#00ff88" if wr_s>=55 else("#ffb700" if wr_s>=50 else "#ff3d5a")
                        st.markdown(f'<div style="margin-bottom:10px;"><div style="display:flex;justify-content:space-between;"><span style="font-family:Space Mono,monospace;font-size:12px;color:#c9d1d9;">⏰ {sname}</span><span style="font-family:Space Mono,monospace;font-size:11px;color:{col};">{wr_s:.1f}% WR · avg {avg_s:+.2f}% · {len(a)} trades</span></div><div style="height:8px;background:var(--border);border-radius:4px;overflow:hidden;margin-top:4px;"><div style="width:{int(wr_s)}%;height:100%;background:{col};border-radius:4px;"></div></div></div>', unsafe_allow_html=True)
                with tab_sc:
                    for sc_lv in [4,5,6]:
                        vals=bt_by_score.get(sc_lv,[])
                        if not vals: continue
                        a=np.array(vals); wr_v=len(a[a>0])/len(a)*100; avg_v=np.mean(a)
                        col="#00ff88" if wr_v>=55 else("#ffb700" if wr_v>=50 else "#ff3d5a")
                        st.markdown(f'<div style="margin-bottom:10px;"><div style="display:flex;justify-content:space-between;"><span style="font-family:Space Mono,monospace;font-size:12px;color:#c9d1d9;">Score {sc_lv} [{"█"*sc_lv+"░"*(6-sc_lv)}]</span><span style="font-family:Space Mono,monospace;font-size:11px;color:{col};">{wr_v:.1f}% WR · avg {avg_v:+.2f}% · {len(a)} trades</span></div><div style="height:8px;background:var(--border);border-radius:4px;overflow:hidden;margin-top:4px;"><div style="width:{int(wr_v)}%;height:100%;background:{col};border-radius:4px;"></div></div></div>', unsafe_allow_html=True)

# ════════════════════════════════════════════════════
#  FOOTER + AUTO REFRESH 15 MENIT
# ════════════════════════════════════════════════════
_now_f = datetime.now(jakarta_tz).timestamp()
if st.session_state.last_scan_time:
    _rem2 = max(0, 300 - (_now_f - st.session_state.last_scan_time))
    mnt2 = int(_rem2//60); sec2 = int(_rem2%60)
    last_t2 = datetime.fromtimestamp(st.session_state.last_scan_time, jakarta_tz).strftime("%H:%M:%S")
    time_info = f"⏱️ Next auto-scan: <span style='color:#ff7b00'>{mnt2:02d}:{sec2:02d}</span> · Last: <span style='color:#2dd4bf'>{last_t2} WIB</span>"
else:
    time_info = "⏱️ Klik Scan untuk mulai"
st.markdown(f"""
<div style="margin-top:28px;padding-top:14px;border-top:1px solid #1c2533;
     display:flex;justify-content:space-between;flex-wrap:wrap;gap:8px;">
  <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;">🔥 Theta Turbo v5.0 · yFinance · Auto Regime</div>
  <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;">{time_info}</div>
</div>""", unsafe_allow_html=True)

# Auto-refresh countdown — hanya aktif setelah scan pertama
# Tidak sleep kalau belum pernah scan (biar tombol langsung respond)
if st.session_state.last_scan_time:
    time.sleep(30)
    st.rerun()
