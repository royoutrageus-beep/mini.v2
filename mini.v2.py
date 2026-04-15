import yfinance as yf
import pandas as pd
import streamlit as st
import time
import requests
import numpy as np
import pytz
from datetime import datetime, timedelta

# ════════════════════════════════════════════════════════
#  ⚙️  CONFIG — SEMUA KEY DI STREAMLIT SECRETS
# ════════════════════════════════════════════════════════
DATASECTORS_API_KEY = st.secrets["DATASECTORS_API_KEY"]
DS_DAILY_QUOTA      = int(st.secrets.get("DS_DAILY_QUOTA", 5000))
TOKEN               = st.secrets.get("TELEGRAM_TOKEN", "")
CHAT_ID             = st.secrets.get("TELEGRAM_CHAT_ID", "")
# ════════════════════════════════════════════════════════

jakarta_tz = pytz.timezone("Asia/Jakarta")
DS_BASE    = "https://api.datasectors.com/api"
DS_HEADERS = {"X-API-Key": DATASECTORS_API_KEY, "Content-Type": "application/json"}

# ── SESSION STATE ──
for _k, _v in [("ds_calls_today",0),("ds_date",None),("data_source","Belum scan"),
                ("wl_results",[]),("wl_mode_used",""),("tt_last_sent",set())]:
    if _k not in st.session_state: st.session_state[_k] = _v
_today = datetime.now(jakarta_tz).date()
if st.session_state.ds_date != _today:
    st.session_state.ds_calls_today = 0
    st.session_state.ds_date = _today

# ── PAGE CONFIG ──
st.set_page_config(layout="wide", page_title="Theta Turbo v5", page_icon="🔥",
                   initial_sidebar_state="collapsed")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;600;800&display=swap');
:root {
  --bg:#080c10;--surface:#0d1117;--border:#1c2533;
  --accent:#00e5ff;--green:#00ff88;--red:#ff3d5a;
  --amber:#ffb700;--orange:#ff7b00;--muted:#4a5568;
  --text:#c9d1d9;--heading:#e6edf3;
}
html,body,[data-testid="stAppViewContainer"]{background:var(--bg)!important;color:var(--text)!important;font-family:'Syne',sans-serif;}
#MainMenu,footer,header{visibility:hidden;}
[data-testid="stSidebar"]{display:none!important;}
[data-testid="stExpander"]{background:var(--surface)!important;border:1px solid var(--border)!important;border-radius:8px!important;margin-bottom:12px!important;}
[data-testid="stExpander"] summary{font-family:'Space Mono',monospace!important;font-size:12px!important;color:var(--accent)!important;letter-spacing:1px!important;}
.settings-label{font-family:'Space Mono',monospace;font-size:10px;color:var(--muted);letter-spacing:2px;margin-bottom:10px;padding-bottom:6px;border-bottom:1px solid var(--border);}
.tt-header{display:flex;align-items:center;padding:16px 0 12px;border-bottom:1px solid var(--border);margin-bottom:16px;}
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
#  STOCK LIST
# ════════════════════════════════════════════════════
raw_stocks = [
    "AALI","ABBA","ABDA","ABMM","ACES","ACST","ADCP","ADES","ADHI","ADMF","ADMG","ADMR","ADRO","AGII","AGRO","AGRS",
    "AIMS","AISA","AKPI","AKRA","AKSI","ALDO","ALKA","ALMI","ALRE","AMAG","AMAR","AMFG","AMIN","AMRT","ANDI","ANJT",
    "ANTM","APEX","APLI","APLN","ARCI","ARGO","ARII","ARKA","ARKO","ARNA","ARTI","ARTO","ASBI","ASDM","ASGR","ASII",
    "ASJT","ASLC","ASRI","ASRM","ASSA","ATIC","AUTO","AVIA","AWAN","AYLS","BABP","BACA","BAIC","BAPA","BAPI","BARI",
    "BBCA","BBHI","BBKP","BBMD","BBNI","BBRI","BBRM","BBSS","BBTN","BBYB","BCAP","BCIC","BDMN","BEEF","BEER","BELI",
    "BEST","BFIN","BGTG","BHIT","BINA","BIPI","BIRD","BISI","BJBR","BJTM","BKDP","BKSL","BLTZ","BLUE","BMAS","BMBL",
    "BMRI","BNBA","BNGA","BNII","BNLI","BOSS","BPFI","BPII","BPTR","BRAM","BRIS","BRMS","BRPT","BSDE","BSIM","BSSR",
    "BSWD","BTEK","BTEL","BTON","BTPS","BUDI","BUKK","BUMI","BYAN","CAKK","CAMP","CARS","CASS","CEKA","CENT","CESS",
    "CFIN","CHIP","CINT","CITA","CITY","CLEO","CLPI","CMRY","CNTX","COAL","CPIN","CSRA","CTBN","CTRA","CUAN","DADA",
    "DART","DEAL","DEFI","DEIT","DEWA","DFAM","DGIK","DGNS","DIGI","DILD","DIVA","DKFT","DLTA","DMMX","DNAR","DNET",
    "DOID","DPNS","DRMA","DSSA","DUCK","DUTI","DVLA","DYAN","EAST","ECII","EKAD","ELIT","ELSA","EMAS","EMTK","ENRG",
    "ERAA","ERTX","ESIP","ESSA","ESTI","ETWA","EURO","EXCL","FAPA","FAST","FASW","FEST","FIMP","FIRE","FORU","FORZ",
    "FPNI","FREN","GAMA","GDST","GDYR","GEMA","GEMS","GGRM","GHON","GIAA","GJTL","GLOB","GLVA","GMTD","GOLD","GOOD",
    "GOTO","GPRA","GPSO","GRIA","GRPM","GSMF","GTBO","GWSA","HAIS","HEAL","HELI","HERO","HEXA","HMSP","HOKI","HOME",
    "HOPE","HOTL","HRTA","HRUM","IATA","IBFN","ICBP","ICON","IDPR","IFII","IFSH","IGAR","IKAI","IKAN","IKBI","IMAS",
    "IMJS","IMPC","INAF","INAI","INCF","INCO","INDF","INET","INPC","INPP","INPS","INRU","INTP","IPCC","IPCM","IPPE",
    "IPTV","IRRA","ISAT","ISSP","ITMG","JAVA","JECC","JGLE","JIHD","JKON","JMAS","JPFA","JRPT","JSMR","JSPT","JTPE",
    "KAEF","KARY","KAYU","KBAG","KBLI","KBLM","KBLV","KBMD","KDSI","KEEN","KEJU","KIAS","KIJA","KINO","KKGI","KLBF",
    "KMDS","KMTR","KOBX","KOKA","KOPI","KOTA","KPAS","KPIG","KRAH","KRAS","KREN","LAAW","LABA","LAND","LAPD","LCGP",
    "LEAD","LIFE","LION","LPCK","LPGI","LPKR","LPPS","LPPF","LSIP","LTLS","LUCK","LUCY","MABA","MAHA","MAIN","MAPA",
    "MAPB","MAPI","MARK","MASA","MAYA","MBAP","MBSS","MBTO","MCAS","MCOL","MCOR","MDIA","MDKA","MDLN","MEDC","MEGA",
    "METI","MFIN","MFMI","MICE","MIDI","MIKA","MINA","MITI","MKPI","MLBI","MLIA","MLMS","MLPT","MNCN","MPMX","MPPA",
    "MREI","MSIN","MSKY","MTDL","MTEL","MTFN","MTLA","MTPS","MTRA","MTSM","MYOH","MYOR","MYTX","NANO","NASA","NELY",
    "NETV","NFCX","NICK","NICL","NIRO","NISM","NKEF","NKIT","NOBU","NPGF","NRCA","NSSS","NTBK","NUSA","NVAM","NZIA",
    "OASA","OBMD","OCAP","OKAS","OMRE","PADI","PAFI","PAMG","PANI","PANR","PANS","PANT","PBID","PBRX","PBSA","PCAR",
    "PEGE","PEHA","PGAS","PGEO","PICO","PJAA","PKPK","PLIN","PLNB","PMJS","PMMP","PNBS","PNIN","PNLF","PNSE","POLL",
    "PORT","POWR","PPRE","PPRO","PRAS","PRDA","PRIM","PSAB","PSDN","PSGO","PSKT","PSSI","PTBA","PTDU","PTIS","PTRO",
    "PTSN","PTSP","PUDP","PURA","PURE","PWON","PYFA","RAAM","RACY","RAJA","RALS","RANC","RBMS","RCCC","RELI","REMA",
    "RGAS","RICY","RIGS","RIMO","RISE","RMKE","RMKO","RODA","RONI","ROTI","SAFE","SAME","SAMF","SAMI","SANK","SAPX",
    "SBMA","SCCO","SCMA","SCNP","SDMU","SDPC","SDRA","SEMA","SGER","SGRO","SHID","SHIP","SILO","SIMA","SIMP","SINI",
    "SIPD","SKBM","SKLT","SKYB","SLIS","SMAR","SMBR","SMCB","SMDM","SMDR","SMGR","SMMT","SMRA","SMSM","SOHO","SONA",
    "SPMA","SPTO","SRAJ","SRIL","SRTG","SSIA","STAA","STTP","SULI","SUPR","SURE","TAMA","TARA","TAXI","TBIG","TBLA",
    "TBMS","TCID","TCOA","TEBE","TECH","TELE","TFAS","TINS","TIRA","TIRT","TKIM","TLDN","TLKM","TMAS","TNCA","TOBA",
    "TOOL","TOTA","TOWR","TPMA","TRGU","TRIL","TRIM","TRIN","TRIS","TRJA","TRST","TRUE","TRUK","TSPC","TUGU","TYRE",
    "UNIC","UNIT","UNSP","UNTR","UNVR","URBN","UVCR","VICI","VICO","VINS","VIPT","VIVA","VOKS","VOMR","VTNY","WAPO",
    "WEGE","WEHA","WICO","WIDI","WIFI","WIGL","WIKA","WIKI","WIMM","WINE","WINS","WIRG","WMUU","WOOD","WOWS","WSBP",
    "WSKT","WTON","YELO","YPAS","YULE","ZATA","ZBRA","ZINC"
]
seen = set(); raw_stocks = [x for x in raw_stocks if not (x in seen or seen.add(x))]
stocks_yf = [s + ".JK" for s in raw_stocks]
stock_map  = {s + ".JK": s for s in raw_stocks}

# ════════════════════════════════════════════════════
#  INDICATOR FUNCTIONS
# ════════════════════════════════════════════════════
def ema(s, n): return s.ewm(span=n, adjust=False).mean()

def rsi_smooth(s, p=14, smooth=3):
    d=s.diff(); g=d.clip(lower=0).rolling(p).mean(); l=(-d.clip(upper=0)).rolling(p).mean()
    rs=g/l.replace(0,np.nan); raw=100-100/(1+rs)
    return raw, ema(raw, smooth)

def stochastic(h, l, c, k=14, d=3):
    ll=l.rolling(k).min(); hh=h.rolling(k).max()
    K=100*(c-ll)/(hh-ll).replace(0,np.nan); D=K.rolling(d).mean()
    return K.fillna(50), D.fillna(50)

def macd(s, f=12, sl=26, sg=9):
    ml=ema(s,f)-ema(s,sl); sig=ema(ml,sg); return ml, sig, ml-sig

def vwap(df):
    tp=(df["High"]+df["Low"]+df["Close"])/3
    return (tp*df["Volume"]).cumsum()/df["Volume"].cumsum()

def apply_intraday_indicators(df):
    # Safety: flatten MultiIndex columns kalau ada
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.droplevel(1)
    df["EMA9"]=ema(df["Close"],9);  df["EMA21"]=ema(df["Close"],21)
    df["EMA50"]=ema(df["Close"],50); df["EMA200"]=ema(df["Close"],200)
    df["RSI"],df["RSI_EMA"]=rsi_smooth(df["Close"],14,3)
    df["STOCH_K"],df["STOCH_D"]=stochastic(df["High"],df["Low"],df["Close"],14,3)
    df["MACD"],df["MACD_Sig"],df["MACD_Hist"]=macd(df["Close"])
    try:    df["VWAP"]=vwap(df)
    except: df["VWAP"]=df["Close"]
    df["BB_mid"]=df["Close"].rolling(20).mean(); df["BB_std"]=df["Close"].rolling(20).std()
    df["BB_upper"]=df["BB_mid"]+2*df["BB_std"]; df["BB_lower"]=df["BB_mid"]-2*df["BB_std"]
    df["BB_pct"]=(df["Close"]-df["BB_lower"])/(df["BB_upper"]-df["BB_lower"])
    df["AvgVol"]=df["Volume"].rolling(20).mean()
    df["RVOL"]=df["Volume"]/df["AvgVol"]
    df["NetVol"]=np.where(df["Close"]>=df["Open"],df["Volume"],-df["Volume"])
    df["NetVol3"]=pd.Series(df["NetVol"],index=df.index).rolling(3).sum()
    df["NetVol8"]=pd.Series(df["NetVol"],index=df.index).rolling(8).sum()
    df["VolSpike"]=df["RVOL"]>2.5
    df["Body"]=(df["Close"]-df["Open"]).abs()
    df["BodyRatio"]=df["Body"]/(df["High"]-df["Low"]).replace(0,np.nan)
    df["BullBar"]=(df["Close"]>df["Open"])&(df["BodyRatio"]>0.5)
    df["ROC3"]=df["Close"].pct_change(3); df["ROC8"]=df["Close"].pct_change(8)
    df["HH"]=df["High"]>df["High"].shift(1); df["HL"]=df["Low"]>df["Low"].shift(1)
    df["LL"]=df["Low"]<df["Low"].shift(1);   df["LH"]=df["High"]<df["High"].shift(1)
    tr=pd.concat([df["High"]-df["Low"],(df["High"]-df["Close"].shift()).abs(),(df["Low"]-df["Close"].shift()).abs()],axis=1).max(axis=1)
    df["ATR"]=tr.rolling(14).mean()
    return df

# ════════════════════════════════════════════════════
#  SCORING FUNCTIONS
# ════════════════════════════════════════════════════
def score_scalping(r, p, p2):
    score=0; reasons=[]
    if r["EMA9"]>r["EMA21"]>r["EMA50"]:   score+=1.5; reasons.append("EMA stack ▲")
    elif r["EMA9"]>r["EMA21"]:             score+=0.8; reasons.append("EMA9>21")
    if r["Close"]>r["VWAP"]:              score+=1;   reasons.append("Above VWAP")
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
    hh=bool(r["HH"]); hl=bool(r["HL"])
    if hh and hl:  score+=1.5; reasons.append("HH+HL pattern ▲")
    elif hh:       score+=0.8
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
    if rsi_e>78:    score-=0.8
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
    rev=0
    pk=float(p["STOCH_K"]); pd_=float(p["STOCH_D"])
    if sk<30 and sk>sd and pk<=pd_:   rev+=1; score+=2;   reasons.append("STOCH %K cross ↑ OS ✦✦")
    elif sk<25 and sk>sd:             rev+=1; score+=1.2; reasons.append("STOCH K>D extreme OS")
    if p is not None:
        rsi_p=float(p["RSI_EMA"])
        if rsi_e>rsi_p and rsi_e<42: rev+=1; score+=1.2; reasons.append("RSI-EMA pivot ↑")
    mh=float(r["MACD_Hist"]); mh_p=float(p["MACD_Hist"])
    if mh>mh_p and mh<0: rev+=1; score+=0.8; reasons.append("MACD hist diverge ↑")
    if rev==0: score*=0.3
    if bool(r["VolSpike"]) and float(r["Close"])<float(r["Open"]): score+=0.8; reasons.append("Volume climax sell")
    elif float(r["RVOL"])>1.5: score+=0.4
    if float(r["NetVol3"])>0: score+=0.5; reasons.append("Net vol turning +")
    if float(r["BodyRatio"])>0.75 and float(r["Close"])<float(r["Open"]): score-=0.8
    return max(0,min(6,round(score,1))), reasons, {}

def get_signal(score, mode):
    t={"Scalping ⚡":{5:"GACOR ⚡",4:"POTENSIAL 🔥",3:"WATCH 👀"},
       "Momentum 🚀":{5:"GACOR 🚀",4:"POTENSIAL 🔥",3:"WATCH 👀"},
       "Reversal 🎯":{5:"REVERSAL 🎯",4:"POTENSIAL 🔥",3:"WATCH 👀"}}.get(mode,{})
    for thresh in sorted(t.keys(), reverse=True):
        if score>=thresh: return t[thresh]
    return "WAIT"

def get_card_class(signal):
    if "GACOR" in signal or "REVERSAL" in signal: return "gacor"
    if "POTENSIAL" in signal: return "potensial"
    if "WATCH" in signal: return "watch"
    return ""

# ════════════════════════════════════════════════════
#  TELEGRAM
# ════════════════════════════════════════════════════
def send_telegram_alert(results_top, source="Scanner", mode=""):
    """Kirim alert detail ke Telegram. Satu pesan, semua info."""
    if not TOKEN or not CHAT_ID: return
    now = datetime.now(jakarta_tz)
    is_open = 9 <= now.hour < 16
    sep = "━" * 28

    header = (
        f"{'🔴 MARKET OPEN' if is_open else '🌙 AFTER HOURS'}\n"
        f"🔥 *THETA TURBO {'WATCHLIST' if source=='Watchlist' else 'ALERT'}*\n"
        f"⏰ `{now.strftime('%H:%M:%S')} WIB` · `{now.strftime('%d %b %Y')}`\n"
        f"{sep}\n"
    )

    body = ""
    for r in results_top[:5]:
        sig  = r.get("Signal", "-")
        em   = "🏆" if ("GACOR" in sig or "REVERSAL" in sig) else ("🔥" if "POTENSIAL" in sig else "👀")
        te   = "📈" if "▲" in r.get("Trend","") else ("📉" if "▼" in r.get("Trend","") else "➡️")
        bar  = "█" * int(r["Score"]) + "░" * (6 - int(r["Score"]))
        rsn  = r.get("Reasons","")[:60]
        body += (
            f"\n{em} *{r['Ticker']}*  `{sig}`\n"
            f"   💰 Price: `{r['Price']:,}` {te}\n"
            f"   📊 Score: `[{bar}] {r['Score']}/6`\n"
            f"   📈 RSI-EMA: `{r.get('RSI-EMA',0)}` | STOCH: `{r.get('Stoch K',0)}`\n"
            f"   🌊 RVOL: `{r.get('RVOL',0)}x` | MACD: `{r.get('MACD Hist',0)}`\n"
            f"   🎯 TP: `{r['TP']:,}` | 🛑 SL: `{r['SL']:,}` | R:R `{r['R:R']}`\n"
            f"   💡 _{rsn}_\n"
        )

    footer = (
        f"\n{sep}\n"
        f"⚡ _Theta Turbo v5 · 15M Intraday_\n"
        f"⚠️ _BUKAN saran investasi. DYOR!_"
    )

    try:
        requests.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": header+body+footer, "parse_mode": "Markdown"},
            timeout=10
        )
    except: pass

# ════════════════════════════════════════════════════
#  DATA ENGINE — HYBRID DATASECTORS + YFINANCE
# ════════════════════════════════════════════════════
def ds_ok(n=1):
    return bool(DATASECTORS_API_KEY) and (st.session_state.ds_calls_today+n)<=DS_DAILY_QUOTA

def ds_bump(n=1): st.session_state.ds_calls_today += n

def fetch_ohlcv_ds(ticker, interval="15m", limit=120):
    try:
        r=requests.post(f"{DS_BASE}/chart/ohlcv",
            json={"symbol":ticker,"interval":interval,"limit":limit},
            headers=DS_HEADERS, timeout=10)
        ds_bump()
        if r.status_code!=200: return None
        d=r.json()
        if not d.get("success"): return None
        rows=d.get("data",[])
        if not rows: return None
        df=pd.DataFrame(rows)
        df.columns=[c.title() for c in df.columns]
        if "Timestamp" in df.columns:
            df["Datetime"]=pd.to_datetime(df["Timestamp"],unit="s",errors="coerce")
            df=df.set_index("Datetime")
        for col in ["Open","High","Low","Close","Volume"]:
            if col in df.columns: df[col]=pd.to_numeric(df[col],errors="coerce")
        df=df.dropna(subset=["Close"])
        return df if len(df)>=20 else None
    except: return None

# ✅ FIX: cache_data nempel LANGSUNG ke fungsi, bukan ke comment!
@st.cache_data(ttl=300)
def fetch_intraday_yf_cached(tickers_tuple, chunk=25):
    """yFinance fetch dengan caching 5 menit — JAUH lebih cepat di re-run."""
    tickers = list(tickers_tuple)
    all_dfs = {}
    for i in range(0, len(tickers), chunk):
        batch = tickers[i:i+chunk]
        try:
            raw = yf.download(
                batch, period="5d", interval="15m",
                group_by="ticker", progress=False,
                threads=False,  # threads=False lebih stabil, hindari rate limit
                auto_adjust=True, timeout=20
            )
            for t in batch:
                try:
                    if len(batch) > 1:
                        df = raw[t].dropna()
                    else:
                        # Single ticker — handle MultiIndex
                        df = raw.copy()
                        if isinstance(df.columns, pd.MultiIndex):
                            df.columns = df.columns.droplevel(1)
                        df = df.dropna()
                    if len(df) >= 50: all_dfs[t] = df
                except: pass
        except Exception as ex:
            err = str(ex)
            if "Rate" in err or "429" in err or "Too Many" in err:
                time.sleep(5)  # backoff kalau rate limited
            else:
                time.sleep(0.5)
        time.sleep(0.5)  # throttle antar batch
    return all_dfs

def fetch_intraday(tickers_yf):
    """Hybrid: DataSectors → yFinance fallback (dengan cache)."""
    tickers_raw=[t.replace(".JK","") for t in tickers_yf]
    if ds_ok(1):
        result={}
        for t in tickers_raw:
            if not ds_ok(): break
            df=fetch_ohlcv_ds(t,"15m",120)
            if df is not None and len(df)>=50:
                result[t+".JK"]=df
            time.sleep(0.12)
        if result:
            st.session_state.data_source="DataSectors 🟢"
            return result
        st.session_state.data_source="yFinance (DS gagal) 🟡"
    else:
        rem=DS_DAILY_QUOTA-st.session_state.ds_calls_today
        st.session_state.data_source=("yFinance (API key belum diisi) ⚪"
            if not bool(DATASECTORS_API_KEY)
            else f"yFinance (Sisa quota: {rem}) 🔴")
    # yFinance dengan cache — pakai tuple supaya bisa di-hash
    return fetch_intraday_yf_cached(tuple(tickers_yf))

# ════════════════════════════════════════════════════
#  MARKET REGIME DETECTOR
# ════════════════════════════════════════════════════
@st.cache_data(ttl=600)
def get_market_regime():
    """Fetch IHSG regime. Instant fallback kalau rate limited — tidak retry."""
    try:
        df = yf.download("^JKSE", period="60d", interval="1d",
                         progress=False, auto_adjust=True, timeout=8)
        if df is not None and len(df) >= 10:
            close = df["Close"].squeeze()
            ema20 = float(close.ewm(span=20, adjust=False).mean().iloc[-1])
            ema55 = float(close.ewm(span=min(55,len(close)-1), adjust=False).mean().iloc[-1])
            price = float(close.iloc[-1])
            chg   = float(((close.iloc[-1]-close.iloc[-2])/close.iloc[-2])*100)
            if price < ema20:                        regime,detail = "RED",   f"IHSG {price:,.0f} < EMA20 → Bearish"
            elif price > ema20 and price > ema55:    regime,detail = "GREEN", f"IHSG {price:,.0f} > EMA20 & EMA55 → Bullish"
            else:                                    regime,detail = "SIDEWAYS",f"IHSG {price:,.0f} antara EMA20-EMA55"
            return (regime, price, ema20, ema55, detail, chg)
    except: pass
    return ("UNKNOWN", 0, 0, 0, "IHSG unavailable — pakai manual mode", 0.0)

def get_regime_config(regime):
    cfgs={
        "RED":      {"mode":"Reversal 🎯","min_score":5,"min_rvol":2.0,"sl_mult":0.6,
                     "label":"🔴 MARKET MERAH — Reversal Only, Score ≥ 5","color":"#ff3d5a",
                     "desc":"Market bearish. Fokus reversal oversold, filter sangat ketat."},
        "GREEN":    {"mode":"Scalping ⚡","min_score":4,"min_rvol":1.5,"sl_mult":0.8,
                     "label":"🟢 MARKET HIJAU — Scalping & Momentum, Score ≥ 4","color":"#00ff88",
                     "desc":"Market bullish. Scalping & Momentum optimal, filter normal."},
        "SIDEWAYS": {"mode":"Scalping ⚡","min_score":4,"min_rvol":2.0,"sl_mult":0.7,
                     "label":"🟡 MARKET SIDEWAYS — Semua Mode, RVOL ≥ 2x","color":"#ffb700",
                     "desc":"Market sideways. Semua mode boleh, RVOL harus lebih kuat."},
        "UNKNOWN":  {"mode":"Scalping ⚡","min_score":4,"min_rvol":1.5,"sl_mult":0.8,
                     "label":"⚪ REGIME UNKNOWN — Pakai Setting Manual","color":"#4a5568",
                     "desc":"Tidak bisa deteksi kondisi market."},
    }
    return cfgs.get(regime, cfgs["UNKNOWN"])

# ════════════════════════════════════════════════════
#  WATCHLIST ANALYZER
# ════════════════════════════════════════════════════
def analyze_watchlist(tickers_raw, mode="Reversal 🎯"):
    results=[]
    for t in tickers_raw:
        t=t.strip().upper()
        if not t: continue
        df=None
        if ds_ok(): df=fetch_ohlcv_ds(t,"15m",150)
        if df is None:
            try:
                raw=yf.download(t+".JK", period="5d", interval="15m",
                                progress=False, auto_adjust=True, threads=False)
                if not raw.empty:
                    if isinstance(raw.columns, pd.MultiIndex):
                        raw.columns = raw.columns.droplevel(1)
                    df = raw.dropna()
                    if len(df) < 10: df = None
                else:
                    df = None
            except: pass
        if df is None or len(df)<55:
            results.append({"Ticker":t,"Price":0,"Score":0,"Signal":"-","RSI-EMA":0,
                "Stoch K":0,"RVOL":0,"BB%":0,"Trend":"-","TP":0,"SL":0,"R:R":0,
                "ROC 3B%":0,"VWAP":0,"ATR":0,"Reasons":"No data","_class":""}); continue
        try:
            df=apply_intraday_indicators(df)
            r=df.iloc[-1]; p=df.iloc[-2]; p2=df.iloc[-3] if len(df)>=3 else p
            close=float(r["Close"]); atr=float(r["ATR"])
            if mode=="Scalping ⚡":   sc,reasons,_=score_scalping(r,p,p2);  tp=close+1.5*atr; sl=close-0.8*atr
            elif mode=="Momentum 🚀": sc,reasons,_=score_momentum(r,p,p2);  tp=close+2.0*atr; sl=close-1.0*atr
            else:                     sc,reasons,_=score_reversal(r,p,p2);  tp=close+2.5*atr; sl=close-0.8*atr
            sig=get_signal(sc,mode); rr=(tp-close)/max(close-sl,0.01)
            e9=float(r["EMA9"]); e21=float(r["EMA21"]); e50=float(r["EMA50"])
            trend="▲ UP" if e9>e21>e50 else("▼ DOWN" if e9<e21<e50 else "◆ SIDE")
            results.append({"Ticker":t,"Price":int(close),"Score":sc,"Signal":sig,"Trend":trend,
                "RSI-EMA":round(float(r["RSI_EMA"]),1),"Stoch K":round(float(r["STOCH_K"]),1),
                "RVOL":round(float(r["RVOL"]),2),"BB%":round(float(r["BB_pct"]),2),
                "ROC 3B%":round(float(r["ROC3"])*100,2),"VWAP":int(float(r["VWAP"])),
                "TP":int(tp),"SL":int(sl),"R:R":round(rr,1),"ATR":round(atr,0),
                "Reasons":" · ".join(reasons),"_class":get_card_class(sig)})
        except Exception as ex:
            results.append({"Ticker":t,"Price":0,"Score":0,"Signal":f"Err:{str(ex)[:25]}",
                "RSI-EMA":0,"Stoch K":0,"RVOL":0,"BB%":0,"Trend":"-","TP":0,"SL":0,
                "R:R":0,"ROC 3B%":0,"VWAP":0,"ATR":0,"Reasons":"","_class":""})
    return results

# ════════════════════════════════════════════════════
#  HEADER
# ════════════════════════════════════════════════════
now_jkt=datetime.now(jakarta_tz)
st.markdown(f"""
<div class="tt-header">
  <div>
    <div class="tt-logo">🔥 THETA TURBO</div>
    <div class="tt-sub">Intraday 15M Scanner · Hybrid Engine v5.0</div>
  </div>
  <div class="live-badge"><div class="live-dot"></div>LIVE {now_jkt.strftime("%H:%M:%S")} WIB</div>
</div>
""", unsafe_allow_html=True)

# ── MARKET REGIME ──
regime_data=get_market_regime()
regime,ihsg_price,ema20,ema55,regime_detail=regime_data[0],regime_data[1],regime_data[2],regime_data[3],regime_data[4]
ihsg_chg=regime_data[5] if len(regime_data)>5 else 0.0
rcfg=get_regime_config(regime)
rcolor=rcfg["color"]
chg_col="#00ff88" if ihsg_chg>=0 else "#ff3d5a"
chg_sym="▲" if ihsg_chg>=0 else "▼"

# ── STATUS BAR ──
quota_pct=(st.session_state.ds_calls_today/DS_DAILY_QUOTA*100) if DS_DAILY_QUOTA>0 else 0
qcol="#00ff88" if quota_pct<60 else("#ffb700" if quota_pct<85 else "#ff3d5a")
st.markdown(f"""
<div style="display:flex;gap:10px;margin-bottom:10px;flex-wrap:wrap;">
  <div style="font-family:Space Mono,monospace;font-size:10px;padding:4px 12px;border-radius:20px;
       background:rgba(0,229,255,.08);border:1px solid rgba(0,229,255,.2);color:#00e5ff;">
    📡 {st.session_state.data_source}
  </div>
  <div style="font-family:Space Mono,monospace;font-size:10px;padding:4px 12px;border-radius:20px;
       background:rgba(0,0,0,.3);border:1px solid #1c2533;color:{qcol};">
    DataSectors: {st.session_state.ds_calls_today}/{DS_DAILY_QUOTA} calls ({quota_pct:.1f}%)
  </div>
</div>
""", unsafe_allow_html=True)

# ── REGIME PANEL ──
st.markdown(f"""
<div style="background:rgba(0,0,0,.4);border:1px solid {rcolor}44;border-radius:8px;
     padding:12px 16px;margin-bottom:14px;border-left:4px solid {rcolor};">
  <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;">
    <div>
      <div style="font-family:Space Mono,monospace;font-size:12px;font-weight:700;
                  color:{rcolor};letter-spacing:1px;">{rcfg["label"]}</div>
      <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-top:3px;">{rcfg["desc"]}</div>
    </div>
    <div style="text-align:right;font-family:Space Mono,monospace;">
      <div style="font-size:18px;font-weight:700;color:{rcolor};">
        {ihsg_price:,.0f} <span style="font-size:11px;color:{chg_col}">{chg_sym}{abs(ihsg_chg):.2f}%</span>
      </div>
      <div style="font-size:9px;color:#4a5568;">EMA20 {ema20:,.0f} · EMA55 {ema55:,.0f}</div>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════
#  TABS
# ════════════════════════════════════════════════════
tab_scanner, tab_watchlist, tab_backtest = st.tabs([
    "🔥 Scanner Intraday", "👁️ Watchlist Analyzer", "📊 Backtest"
])

# ─────────────────────────────────────────────
#  TAB 1: SCANNER
# ─────────────────────────────────────────────
with tab_scanner:
    with st.expander("⚙️  Scanner Settings", expanded=False):
        sc1,sc2,sc3=st.columns(3)
        with sc1:
            st.markdown('<div class="settings-label">MODE SIGNAL</div>', unsafe_allow_html=True)
            auto_regime=st.toggle("🤖 Auto-Mode (Market Regime)", value=True, key="auto_reg")
            if auto_regime:
                scan_mode=rcfg["mode"]
                st.markdown(f'<div style="font-family:Space Mono,monospace;font-size:10px;padding:6px 10px;background:rgba(0,0,0,.3);border-radius:4px;color:{rcolor};">Auto: {scan_mode}</div>', unsafe_allow_html=True)
            else:
                scan_mode=st.radio("Mode Manual",["Scalping ⚡","Momentum 🚀","Reversal 🎯"],label_visibility="collapsed",key="scan_mode_manual")
            tele_on=st.toggle("📡 Telegram Alert", value=True, key="tele_toggle")
        with sc2:
            st.markdown('<div class="settings-label">FILTER</div>', unsafe_allow_html=True)
            auto_thresh=st.toggle("🤖 Auto-Threshold", value=True, key="auto_thr")
            if auto_thresh:
                min_score=rcfg["min_score"]; vol_thresh=rcfg["min_rvol"]
                st.caption(f"Auto: Score≥{min_score} · RVOL≥{vol_thresh}x")
            else:
                min_score=st.slider("Min Score (0-6)",0,6,4,key="msc")
                vol_thresh=st.slider("Min RVOL Spike",1.0,5.0,1.5,0.1,key="vol")
            min_turn=st.number_input("Min Turnover (M Rp)",value=500,step=100,key="trn")*1_000_000
        with sc3:
            st.markdown('<div class="settings-label">TAMPILAN</div>', unsafe_allow_html=True)
            view_mode=st.radio("View",["Card View 🃏","Table View 📊"],label_visibility="collapsed",key="view_mode")
            quick_mode=st.toggle("⚡ Quick (200 saham)", value=False, key="quick_mode")
            st.caption(f"🎯 Regime: {regime} · Mode: {scan_mode}")
            st.caption(f"📊 {len(raw_stocks)} emiten tersedia")

    # ── SCAN BUTTON — tidak auto-run ──
    do_scan = st.button("🔥 MULAI SCAN SEKARANG", type="primary",
                        use_container_width=True, key="btn_scan")

    if "data_dict" not in st.session_state:
        st.session_state.data_dict = {}
    if "scan_results" not in st.session_state:
        st.session_state.scan_results = []

    if not do_scan and not st.session_state.scan_results:
        st.markdown(f"""
        <div style="text-align:center;padding:48px 20px;color:#4a5568;font-family:Space Mono,monospace;">
          <div style="font-size:36px;margin-bottom:12px;">🔥</div>
          <div style="font-size:13px;letter-spacing:2px;">KLIK SCAN UNTUK MULAI</div>
          <div style="font-size:10px;margin-top:8px;color:#2d3748;">
            {"Quick Mode: 200 saham" if quick_mode else f"Full Mode: {len(raw_stocks)} saham"}<br>
            Regime: {regime} → Auto mode: {rcfg["mode"]}
          </div>
        </div>
        """, unsafe_allow_html=True)

    if do_scan:
        scan_list = stocks_yf[:200] if quick_mode else stocks_yf
        prog_ph = st.empty()
        with prog_ph.container():
            st.markdown(f'<div style="color:#ff7b00;font-family:Space Mono,monospace;font-size:12px;letter-spacing:1px;">🔥 Scanning {len(scan_list)} saham...</div>', unsafe_allow_html=True)
            pb = st.progress(0)
        try:
            data_dict = fetch_intraday(scan_list)
            st.session_state.data_dict = data_dict
            results = []; tickers = list(data_dict.keys())
            for i, ticker_yf in enumerate(tickers):
                pb.progress((i+1)/max(len(tickers),1))
                try:
                    df = data_dict[ticker_yf].copy()
                    if len(df) < 55: continue
                    df = apply_intraday_indicators(df)
                    r=df.iloc[-1]; p=df.iloc[-2]; p2=df.iloc[-3] if len(df)>=3 else p
                    close=float(r["Close"]); vol=float(r["Volume"]); turnover=close*vol; rvol=float(r["RVOL"])
                    if turnover < min_turn or rvol < vol_thresh: continue
                    if scan_mode=="Scalping ⚡":   sc,reasons,_=score_scalping(r,p,p2)
                    elif scan_mode=="Momentum 🚀": sc,reasons,_=score_momentum(r,p,p2)
                    else:                          sc,reasons,_=score_reversal(r,p,p2)
                    if sc < min_score: continue
                    sig = get_signal(sc, scan_mode)
                    if sig == "WAIT": continue
                    atr=float(r["ATR"]); slm=rcfg.get("sl_mult",0.8)
                    if scan_mode=="Scalping ⚡":   tp=close+1.5*atr; sl=close-slm*atr
                    elif scan_mode=="Momentum 🚀": tp=close+2.0*atr; sl=close-slm*atr
                    else:                          tp=close+2.5*atr; sl=close-slm*atr
                    rr=(tp-close)/max(close-sl,0.01)
                    e9=float(r["EMA9"]); e21=float(r["EMA21"]); e50=float(r["EMA50"])
                    trend="▲ UP" if e9>e21>e50 else("▼ DOWN" if e9<e21<e50 else "◆ SIDE")
                    results.append({"Ticker":stock_map[ticker_yf],"Price":int(close),"Score":sc,"Signal":sig,"Trend":trend,
                        "RSI-EMA":round(float(r["RSI_EMA"]),1),"Stoch K":round(float(r["STOCH_K"]),1),
                        "Stoch D":round(float(r["STOCH_D"]),1),"MACD Hist":round(float(r["MACD_Hist"]),4),
                        "RVOL":round(rvol,2),"BB%":round(float(r["BB_pct"]),2),
                        "ROC 3B%":round(float(r["ROC3"])*100,2),"VWAP":int(float(r["VWAP"])),
                        "TP":int(tp),"SL":int(sl),"R:R":round(rr,1),
                        "Turnover(M)":round(turnover/1e6,1),"Reasons":" · ".join(reasons),
                        "_class":get_card_class(sig)})
                except: continue
            prog_ph.empty()
            st.session_state.scan_results = results
        except Exception as e:
            prog_ph.empty()
            st.error(f"Scan error: {str(e)[:100]}")

    # ── DISPLAY RESULTS ──
    results = st.session_state.scan_results
    if results:
        df_out=pd.DataFrame(results).sort_values("Score",ascending=False).reset_index(drop=True)
        gacor=df_out[df_out["Signal"].str.contains("GACOR|REVERSAL",na=False)]
        potensi=df_out[df_out["Signal"].str.contains("POTENSIAL",na=False)]
        avg_rsi=df_out["RSI-EMA"].mean()
        st.markdown(f"""
        <div class="metric-row">
          <div class="metric-card" style="border-top-color:{rcolor}"><div class="metric-label">Regime</div>
            <div class="metric-value" style="font-size:16px;color:{rcolor}">{regime}</div>
            <div class="metric-sub">{ihsg_price:,.0f} {chg_sym}{abs(ihsg_chg):.2f}%</div></div>
          <div class="metric-card orange"><div class="metric-label">Mode</div>
            <div class="metric-value" style="font-size:13px;margin-top:4px;">{scan_mode}</div></div>
          <div class="metric-card green"><div class="metric-label">Signal Lolos</div>
            <div class="metric-value">{len(df_out)}</div>
            <div class="metric-sub">dari {len(raw_stocks)} emiten</div></div>
          <div class="metric-card red"><div class="metric-label">GACOR 🔥</div>
            <div class="metric-value">{len(gacor)}</div><div class="metric-sub">score ≥ 5</div></div>
          <div class="metric-card amber"><div class="metric-label">POTENSIAL</div>
            <div class="metric-value">{len(potensi)}</div></div>
          <div class="metric-card"><div class="metric-label">Avg RSI-EMA</div>
            <div class="metric-value" style="color:{"#00ff88" if avg_rsi>50 else "#ffb700" if avg_rsi>35 else "#ff3d5a"}">{avg_rsi:.1f}</div>
            <div class="metric-sub">{"Bullish" if avg_rsi>50 else "Neutral" if avg_rsi>35 else "Oversold"}</div></div>
        </div>""", unsafe_allow_html=True)

        th='<div class="tape-wrap"><div class="tape-inner">'
        for _,row in df_out.iterrows():
            roc=row["ROC 3B%"]; cls="up" if roc>0 else("down" if roc<0 else "flat"); sym="▲" if roc>0 else("▼" if roc<0 else "─")
            th+=f'<span class="tape-item {cls}">{row["Ticker"]} {int(row["Price"])} {sym}{abs(roc):.1f}% [{row["Signal"]}]</span>'
        th+=th.replace('tape-inner">',''); th+='</div></div>'
        st.markdown(th, unsafe_allow_html=True)

        if not gacor.empty:
            st.markdown(f'<div class="alert-box"><div class="alert-title">🚨 GACOR ALERT · {len(gacor)} SAHAM · {scan_mode}</div></div>', unsafe_allow_html=True)

        if tele_on and results:
            if "tt_last_sent" not in st.session_state: st.session_state.tt_last_sent=set()
            cur_set=set(df_out["Ticker"].tolist()); new_alr=cur_set-st.session_state.tt_last_sent
            if new_alr:
                top_new=df_out[df_out["Ticker"].isin(new_alr)].head(5).to_dict("records")
                if top_new: send_telegram_alert(top_new)
                perfect=df_out[(df_out["Ticker"].isin(new_alr))&(df_out["Score"]==6)]
                for _,rw in perfect.iterrows(): send_telegram_gacor(rw.to_dict())
                st.session_state.tt_last_sent.update(new_alr)
            st.session_state.tt_last_sent=st.session_state.tt_last_sent&cur_set

        if view_mode=="Card View 🃏":
            st.markdown('<div class="section-title">Signal Cards</div>', unsafe_allow_html=True)
            card_html='<div class="signal-grid">'
            for _,row in df_out.head(20).iterrows():
                sc_int=int(row["Score"])
                bars=''.join([f'<div class="sc-bar {"filled" if i<sc_int else "empty"}" style="width:28px"></div>' for i in range(6)])
                roc_c='#00ff88' if row["ROC 3B%"]>0 else '#ff3d5a'
                te="📈" if "▲" in row["Trend"] else("📉" if "▼" in row["Trend"] else "➡️")
                card_html+=f"""<div class="signal-card {row["_class"]}">
                  <div style="display:flex;justify-content:space-between;align-items:flex-start;">
                    <div><div class="sc-ticker">{row["Ticker"]}</div>
                    <div class="sc-price" style="color:{roc_c}">{int(row["Price"]):,} {te}</div></div>
                    <div style="text-align:right;">
                      <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;">SCORE</div>
                      <div style="font-family:Space Mono,monospace;font-size:20px;font-weight:700;color:{"#00ff88" if sc_int>=5 else "#ffb700" if sc_int>=4 else "#00e5ff"}">{row["Score"]}</div>
                    </div>
                  </div>
                  <div class="sc-signal" style="color:{"#00ff88" if "GACOR" in row["Signal"] or "REVERSAL" in row["Signal"] else "#ffb700" if "POTENSIAL" in row["Signal"] else "#00e5ff"}">{row["Signal"]}</div>
                  <div class="sc-bars">{bars}</div>
                  <div class="sc-stats">
                    <div class="sc-stat">RSI-EMA <span>{row["RSI-EMA"]}</span></div>
                    <div class="sc-stat">STOCH <span>{row["Stoch K"]:.0f}</span></div>
                    <div class="sc-stat">RVOL <span>{row["RVOL"]}x</span></div>
                    <div class="sc-stat">ROC <span style="color:{roc_c}">{row["ROC 3B%"]:+.1f}%</span></div>
                  </div>
                  <div class="sc-stats" style="margin-top:6px;">
                    <div class="sc-stat">TP <span style="color:#00ff88">{int(row["TP"]):,}</span></div>
                    <div class="sc-stat">SL <span style="color:#ff3d5a">{int(row["SL"]):,}</span></div>
                    <div class="sc-stat">R:R <span>{row["R:R"]}</span></div>
                  </div>
                  <div style="margin-top:8px;font-size:10px;color:#4a5568;line-height:1.4;font-family:Space Mono,monospace;">{row["Reasons"][:70]}</div>
                </div>"""
            card_html+='</div>'
            st.markdown(card_html, unsafe_allow_html=True)

        st.markdown('<div class="section-title">Full Signal Table</div>', unsafe_allow_html=True)
        display_cols=["Ticker","Price","Score","Signal","Trend","RSI-EMA","Stoch K","Stoch D","MACD Hist","RVOL","BB%","ROC 3B%","VWAP","TP","SL","R:R","Turnover(M)","Reasons"]
        st.dataframe(df_out[display_cols], width='stretch', hide_index=True, column_config={
            "Score":st.column_config.ProgressColumn("Score",min_value=0,max_value=6,format="%.1f"),
            "RSI-EMA":st.column_config.NumberColumn("RSI-EMA",format="%.1f"),
            "Stoch K":st.column_config.NumberColumn("Stoch K",format="%.1f"),
            "RVOL":st.column_config.NumberColumn("RVOL",format="%.1fx"),
            "ROC 3B%":st.column_config.NumberColumn("ROC 3B%",format="%.2f%%"),
            "Turnover(M)":st.column_config.NumberColumn("Turnover(M)",format="Rp%.0fM"),
        })

with tab_watchlist:
    st.markdown("""
    <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-bottom:12px;
         padding:10px 14px;background:#0d1117;border-radius:6px;border-left:3px solid #ff7b00;">
      Analisa mendalam untuk saham pilihan lo &amp; teman grup.
      Input ticker IDX (tanpa .JK), pisah koma atau enter.
    </div>
    """, unsafe_allow_html=True)

    wc1,wc2,wc3=st.columns([3,1,1])
    with wc1:
        wl_input=st.text_area("Ticker Watchlist",
            placeholder="Contoh:\nBBCA\nARCI, ASSA, GOTO\nBBRI, BMRI",
            height=120, label_visibility="collapsed", key="wl_input")
    with wc2:
        wl_mode=st.radio("Mode Analisa",["Scalping ⚡","Momentum 🚀","Reversal 🎯"],key="wl_mode")
        st.caption(f"Regime suggest: {rcfg['mode']}")
    with wc3:
        st.markdown("<br>", unsafe_allow_html=True)
        wl_run=st.button("🔍 Analisa", use_container_width=True, key="wl_run")
        wl_share=st.button("📋 Copy Hasil", use_container_width=True, key="wl_share")
        wl_tele=st.button("📡 Kirim Telegram", use_container_width=True, key="wl_tele")
        st.caption(f"DS calls: {st.session_state.ds_calls_today}/{DS_DAILY_QUOTA}")

    if wl_run and wl_input.strip():
        raw_wl=list(dict.fromkeys([t.strip().upper() for line in wl_input.split("\n") for t in line.split(",") if t.strip()]))
        if not raw_wl:
            st.warning("Masukkan minimal 1 ticker!")
        else:
            with st.spinner(f"Menganalisa {len(raw_wl)} saham..."):
                wl_results=analyze_watchlist(raw_wl, mode=wl_mode)
            st.session_state.wl_results=wl_results
            st.session_state.wl_mode_used=wl_mode
            # Auto-kirim ke Telegram kalau ada signal
            wl_gacor=[r for r in wl_results if r["Price"]>0 and ("GACOR" in r.get("Signal","") or "REVERSAL" in r.get("Signal",""))]
            wl_pot=[r for r in wl_results if r["Price"]>0 and "POTENSIAL" in r.get("Signal","")]
            wl_to_send = wl_gacor + wl_pot
            if wl_to_send:
                send_telegram_alert(wl_to_send[:5], source="Watchlist", mode=wl_mode)
                st.success(f"📡 Alert terkirim ke Telegram: {len(wl_to_send)} signal")
            ok_res=[r for r in wl_results if r["Score"]>0]
            gacor_wl=[r for r in ok_res if "GACOR" in r.get("Signal","") or "REVERSAL" in r.get("Signal","")]
            pot_wl=[r for r in ok_res if "POTENSIAL" in r.get("Signal","")]
            st.markdown(f"""
            <div class="metric-row" style="margin-top:16px;">
              <div class="metric-card orange"><div class="metric-label">Dipantau</div><div class="metric-value">{len(raw_wl)}</div></div>
              <div class="metric-card green"><div class="metric-label">GACOR 🔥</div><div class="metric-value">{len(gacor_wl)}</div></div>
              <div class="metric-card amber"><div class="metric-label">POTENSIAL</div><div class="metric-value">{len(pot_wl)}</div></div>
              <div class="metric-card"><div class="metric-label">Data OK</div><div class="metric-value">{len(ok_res)}</div></div>
            </div>""", unsafe_allow_html=True)
            ch='<div class="signal-grid">'
            for row in sorted(wl_results, key=lambda x: x["Score"], reverse=True):
                if row["Price"]==0:
                    ch+=f'<div class="signal-card"><div class="sc-ticker">{row["Ticker"]}</div><div style="font-size:11px;color:#4a5568;margin-top:6px;">{row.get("Signal","No data")}</div></div>'
                    continue
                sc_int=int(row["Score"]); bars=''.join([f'<div class="sc-bar {"filled" if i<sc_int else "empty"}" style="width:26px"></div>' for i in range(6)])
                sig=row.get("Signal","-")
                sc_col="#00ff88" if ("GACOR" in sig or "REVERSAL" in sig) else("#ffb700" if "POTENSIAL" in sig else "#00e5ff" if "WATCH" in sig else "#4a5568")
                rsi_v=row["RSI-EMA"]; rsi_c="#ff3d5a" if rsi_v<30 else("#ffb700" if rsi_v<45 else "#00ff88" if rsi_v>60 else "#c9d1d9")
                roc_c="#00ff88" if row.get("ROC 3B%",0)>0 else "#ff3d5a"
                te="📈" if "▲" in row["Trend"] else("📉" if "▼" in row["Trend"] else "➡️")
                ch+=f"""<div class="signal-card {row["_class"]}">
                  <div style="display:flex;justify-content:space-between;">
                    <div><div class="sc-ticker">{row["Ticker"]}</div>
                    <div class="sc-price" style="color:{roc_c}">{row["Price"]:,} {te}</div></div>
                    <div style="text-align:right">
                      <div style="font-family:Space Mono,monospace;font-size:9px;color:#4a5568">SCORE</div>
                      <div style="font-family:Space Mono,monospace;font-size:22px;font-weight:700;color:{"#00ff88" if sc_int>=5 else "#ffb700" if sc_int>=4 else "#00e5ff"}">{row["Score"]}</div>
                    </div>
                  </div>
                  <div class="sc-signal" style="color:{sc_col}">{sig}</div>
                  <div class="sc-bars">{bars}</div>
                  <div class="sc-stats">
                    <div class="sc-stat">RSI-EMA <span style="color:{rsi_c}">{rsi_v}</span></div>
                    <div class="sc-stat">STOCH <span>{row["Stoch K"]:.0f}</span></div>
                    <div class="sc-stat">RVOL <span>{row["RVOL"]}x</span></div>
                  </div>
                  <div class="sc-stats" style="margin-top:6px">
                    <div class="sc-stat">TP <span style="color:#00ff88">{int(row["TP"]):,}</span></div>
                    <div class="sc-stat">SL <span style="color:#ff3d5a">{int(row["SL"]):,}</span></div>
                    <div class="sc-stat">R:R <span>{row["R:R"]}</span></div>
                  </div>
                  <div style="margin-top:8px;font-size:10px;color:#4a5568;line-height:1.5;font-family:Space Mono,monospace">{row["Reasons"][:80]}</div>
                </div>"""
            ch+='</div>'
            st.markdown(ch, unsafe_allow_html=True)
            df_wl=pd.DataFrame([r for r in wl_results if r["Price"]>0])
            if not df_wl.empty:
                show=["Ticker","Price","Score","Signal","Trend","RSI-EMA","Stoch K","RVOL","BB%","ROC 3B%","VWAP","TP","SL","R:R","ATR","Reasons"]
                show=[c for c in show if c in df_wl.columns]
                st.dataframe(df_wl[show], width='stretch', hide_index=True, column_config={
                    "Score":st.column_config.ProgressColumn("Score",min_value=0,max_value=6,format="%.1f"),
                    "RSI-EMA":st.column_config.NumberColumn("RSI-EMA",format="%.1f"),
                    "RVOL":st.column_config.NumberColumn("RVOL",format="%.2fx"),
                    "ROC 3B%":st.column_config.NumberColumn("ROC 3B%",format="%.2f%%"),
                })

    if wl_tele and "wl_results" in st.session_state and st.session_state.wl_results:
        to_send = [r for r in st.session_state.wl_results if r["Price"]>0]
        if to_send:
            send_telegram_alert(to_send[:5], source="Watchlist",
                                mode=st.session_state.get("wl_mode_used",""))
            st.success(f"📡 Terkirim ke Telegram: {min(5,len(to_send))} teratas!")
        else:
            st.warning("Tidak ada data untuk dikirim.")

    elif wl_share and "wl_results" in st.session_state and st.session_state.wl_results:
        now_str=datetime.now(jakarta_tz).strftime("%d %b %Y %H:%M")
        txt=f"🔥 THETA TURBO WATCHLIST\n⏰ {now_str} WIB\n📊 Mode: {st.session_state.get('wl_mode_used','')} | Regime: {regime}\n"+"─"*28+"\n"
        for r in sorted(st.session_state.wl_results, key=lambda x: x["Score"], reverse=True):
            if r["Price"]==0: continue
            sig=r.get("Signal","-")
            em="🔥" if ("GACOR" in sig or "REVERSAL" in sig) else("⚡" if "POTENSIAL" in sig else "👀")
            txt+=f"{em} {r['Ticker']} | {r['Price']:,} | Score:{r['Score']} | RSI:{r['RSI-EMA']} | {sig}\n"
            if r.get("Reasons"): txt+=f"   → {r['Reasons'][:60]}\n"
        txt+="─"*28+"\nby Theta Turbo v5 🚀"
        st.text_area("Copy untuk grup:", txt, height=280, key="share_out")
        st.caption("Ctrl+A → Ctrl+C")
    else:
        st.markdown("""
        <div style="text-align:center;padding:48px;color:#4a5568;font-family:Space Mono,monospace;">
          <div style="font-size:32px;margin-bottom:12px;">👁️</div>
          <div style="font-size:12px;letter-spacing:2px;">MASUKKAN TICKER DI ATAS</div>
          <div style="font-size:10px;margin-top:8px;color:#2d3748;">
            Bisa 1 atau banyak · Pisah koma atau enter<br>
            Contoh: BBCA, ARCI, ASSA, GOTO
          </div>
        </div>
        """, unsafe_allow_html=True)

# ─────────────────────────────────────────────
#  TAB 3: BACKTEST ENGINE
# ─────────────────────────────────────────────
with tab_backtest:
    st.markdown('<div class="section-title">Backtest Engine · 15M Intraday</div>', unsafe_allow_html=True)
    st.markdown("""
    <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;line-height:1.9;margin-bottom:14px;">
    ℹ️  Entry = bar saat signal terpenuhi &nbsp;·&nbsp; Exit = kena TP / SL / N bar ke depan<br>
    ⏱️  1 bar = 15 menit &nbsp;·&nbsp; Data: 5 hari terakhir (~120 bar per saham)
    </div>
    """, unsafe_allow_html=True)
    bt_c1,bt_c2,bt_c3,bt_c4=st.columns(4)
    bt_mode=bt_c1.selectbox("Mode Backtest",["Scalping ⚡","Momentum 🚀","Reversal 🎯"],key="bt_mode")
    bt_sc=bt_c2.slider("Min Score Entry",0,6,4,key="bt_sc")
    bt_fwd=int(bt_c3.number_input("Hold (bars)",value=4,step=1,min_value=1,max_value=20))
    bt_sl_mult=bt_c4.number_input("SL mult (x ATR)",value=0.8,step=0.1,min_value=0.1,max_value=3.0)

    if st.button("🚀 Run Backtest", type="primary", key="bt_run"):
        data_dict = st.session_state.get("data_dict", {})
        if not data_dict:
            st.warning("Jalankan Scanner dulu agar data tersedia! (Tab Scanner → Klik Scan)")
        else:
            bt_results=[]; bt_by_trend={"▲ UP":[],"▼ DOWN":[],"◆ SIDE":[]}
            bt_by_session={"Pagi 09-11":[],"Siang 11-14":[],"Sore 14-16":[]}
            bt_by_score={4:[],5:[],6:[]}
            bt_pb=st.progress(0); sample=list(data_dict.keys())[:80]
            for bi,t_yf in enumerate(sample):
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
                        entry=float(r0["Close"]); atr_v=float(r0["ATR"]) if not np.isnan(float(r0["ATR"])) else entry*0.005
                        if bt_mode=="Scalping ⚡":   tp_p=entry+1.5*atr_v; sl_p=entry-bt_sl_mult*atr_v
                        elif bt_mode=="Momentum 🚀": tp_p=entry+2.0*atr_v; sl_p=entry-bt_sl_mult*atr_v
                        else:                         tp_p=entry+2.5*atr_v; sl_p=entry-bt_sl_mult*atr_v
                        exit_price=float(d.iloc[ii+bt_fwd]["Close"])
                        for fwd_i in range(1,bt_fwd+1):
                            bar=d.iloc[ii+fwd_i]
                            if float(bar["High"])>=tp_p: exit_price=tp_p; break
                            if float(bar["Low"])<=sl_p:  exit_price=sl_p; break
                        ret=(exit_price-entry)/entry*100; bt_results.append(ret)
                        e9=float(r0["EMA9"]); e21=float(r0["EMA21"]); e50=float(r0["EMA50"])
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
                    <span class="bt-metric"><div class="bt-metric-val" style="color:{"#00ff88" if wr>=55 else "#ffb700" if wr>=50 else "#ff3d5a"}">{wr:.1f}%</div><div class="bt-metric-lbl">Win Rate</div></span>
                    <span class="bt-metric"><div class="bt-metric-val" style="color:{"#00ff88" if avg>0 else "#ff3d5a"}">{avg:+.2f}%</div><div class="bt-metric-lbl">Avg Return</div></span>
                    <span class="bt-metric"><div class="bt-metric-val" style="color:#00e5ff">{med:+.2f}%</div><div class="bt-metric-lbl">Median</div></span>
                    <span class="bt-metric"><div class="bt-metric-val" style="color:{"#00ff88" if pf>=1.5 else "#ffb700" if pf>=1 else "#ff3d5a"}">{pf:.2f}x</div><div class="bt-metric-lbl">Profit Factor</div></span>
                    <span class="bt-metric"><div class="bt-metric-val" style="color:#ff3d5a">{mxdd:.1f}%</div><div class="bt-metric-lbl">Max Loss</div></span>
                  </div>
                </div>""", unsafe_allow_html=True)
                tab_trend,tab_session,tab_score=st.tabs(["📈 Per Trend","⏰ Per Sesi","🎯 Per Score"])
                with tab_trend:
                    for tr_name,vals in bt_by_trend.items():
                        if not vals: continue
                        a=np.array(vals); wr_t=len(a[a>0])/len(a)*100; avg_t=np.mean(a)
                        col="#00ff88" if wr_t>=55 else("#ffb700" if wr_t>=50 else "#ff3d5a")
                        st.markdown(f"""<div style="margin-bottom:12px;">
                          <div style="display:flex;justify-content:space-between;margin-bottom:4px;">
                            <span style="font-family:Space Mono,monospace;font-size:12px;color:#c9d1d9;">{tr_name}</span>
                            <span style="font-family:Space Mono,monospace;font-size:11px;color:{col};">{wr_t:.1f}% WR · {len(a)} trades · avg {avg_t:+.2f}%</span>
                          </div>
                          <div style="height:8px;background:var(--border);border-radius:4px;overflow:hidden;">
                            <div style="width:{int(wr_t)}%;height:100%;background:{col};border-radius:4px;"></div>
                          </div>
                        </div>""", unsafe_allow_html=True)
                with tab_session:
                    for sname,vals in bt_by_session.items():
                        if not vals: continue
                        a=np.array(vals); wr_s=len(a[a>0])/len(a)*100; avg_s=np.mean(a)
                        col="#00ff88" if wr_s>=55 else("#ffb700" if wr_s>=50 else "#ff3d5a")
                        st.markdown(f"""<div style="margin-bottom:12px;">
                          <div style="display:flex;justify-content:space-between;margin-bottom:4px;">
                            <span style="font-family:Space Mono,monospace;font-size:12px;color:#c9d1d9;">⏰ {sname}</span>
                            <span style="font-family:Space Mono,monospace;font-size:11px;color:{col};">{wr_s:.1f}% WR · {len(a)} trades · avg {avg_s:+.2f}%</span>
                          </div>
                          <div style="height:8px;background:var(--border);border-radius:4px;overflow:hidden;">
                            <div style="width:{int(wr_s)}%;height:100%;background:{col};border-radius:4px;"></div>
                          </div>
                        </div>""", unsafe_allow_html=True)
                with tab_score:
                    for sc_lv in [4,5,6]:
                        vals=bt_by_score.get(sc_lv,[])
                        if not vals: continue
                        a=np.array(vals); wr_v=len(a[a>0])/len(a)*100; avg_v=np.mean(a)
                        col="#00ff88" if wr_v>=55 else("#ffb700" if wr_v>=50 else "#ff3d5a")
                        st.markdown(f"""<div style="margin-bottom:12px;">
                          <div style="display:flex;justify-content:space-between;margin-bottom:4px;">
                            <span style="font-family:Space Mono,monospace;font-size:12px;color:#c9d1d9;">Score {sc_lv} [{"█"*sc_lv+"░"*(6-sc_lv)}]</span>
                            <span style="font-family:Space Mono,monospace;font-size:11px;color:{col};">{wr_v:.1f}% WR · {len(a)} trades · avg {avg_v:+.2f}%</span>
                          </div>
                          <div style="height:8px;background:var(--border);border-radius:4px;overflow:hidden;">
                            <div style="width:{int(wr_v)}%;height:100%;background:{col};border-radius:4px;"></div>
                          </div>
                        </div>""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
#  FOOTER + AUTO REFRESH
# ─────────────────────────────────────────────
st.markdown(f"""
<div style="margin-top:28px;padding-top:14px;border-top:1px solid #1c2533;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;">
  <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;">🔥 Theta Turbo v5.0 · Hybrid Engine · Market Regime Detector</div>
  <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;"><span style="color:#ff7b00">{datetime.now(jakarta_tz).strftime("%H:%M:%S")} WIB</span> · Next refresh 300s</div>
</div>""", unsafe_allow_html=True)

# Auto-refresh dinonaktifkan — refresh manual untuk data terbaru
