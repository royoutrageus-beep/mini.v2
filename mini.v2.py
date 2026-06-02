"""
╔══════════════════════════════════════════════════════════════╗
║         THETA TURBO v5.2 — DataSectors Edition               ║
║         Auto-scan on open · Daily scan · 15M deep dive       ║
║         Wyckoff · Bandar · RVOL · Auto Regime                ║
╚══════════════════════════════════════════════════════════════╝
"""
import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import requests
import time
import random
import pytz
from datetime import datetime

# ── CONFIG ──────────────────────────────────────────────────────────────────
TOKEN      = st.secrets.get("TELEGRAM_TOKEN", "")
CHAT_ID    = st.secrets.get("TELEGRAM_CHAT_ID", "")
DS_KEY     = st.secrets.get("DATASECTORS_API_KEY", "")
DS_BASE    = "https://api.datasectors.com"
jakarta_tz = pytz.timezone("Asia/Jakarta")

# ── SESSION STATE ────────────────────────────────────────────────────────────
for _k, _v in [
    ("scan_results", []), ("last_scan_time", None),
    ("wl_results", []),   ("bsjp_results", []),
    ("gapup_results", []),("sector_data", {}),
    ("tt_last_sent", set()),("first_scan_done", False),
]:
    if _k not in st.session_state: st.session_state[_k] = _v

st.set_page_config(layout="wide", page_title="Theta Turbo v5.2 DS",
                   page_icon="🔥", initial_sidebar_state="collapsed")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;600;800&display=swap');
:root{--bg:#080c10;--surface:#0d1117;--border:#1c2533;--accent:#00e5ff;
      --green:#00ff88;--red:#ff3d5a;--amber:#ffb700;--purple:#bf5fff;
      --orange:#ff7b00;--muted:#4a5568;--text:#c9d1d9;--heading:#e6edf3;}
html,body,[data-testid="stAppViewContainer"]{background:var(--bg)!important;color:var(--text)!important;font-family:'Syne',sans-serif;}
#MainMenu,footer,header{visibility:hidden;}
[data-testid="stSidebar"]{display:none!important;}
.metric-row{display:flex;gap:10px;margin-bottom:18px;flex-wrap:wrap;}
.metric-card{flex:1;min-width:110px;background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:12px 14px;position:relative;overflow:hidden;}
.metric-card::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:var(--accent);}
.metric-card.green::before{background:var(--green);}
.metric-card.red::before{background:var(--red);}
.metric-card.amber::before{background:var(--amber);}
.metric-card.orange::before{background:var(--orange);}
.metric-label{font-size:10px;color:var(--muted);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:4px;}
.metric-value{font-family:'Space Mono',monospace;font-size:22px;font-weight:700;color:var(--heading);line-height:1;}
.metric-sub{font-size:10px;color:var(--muted);margin-top:3px;}
.signal-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(270px,1fr));gap:12px;margin-bottom:20px;}
.signal-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px;position:relative;overflow:hidden;}
.signal-card.gacor{border-color:rgba(0,255,136,.4);background:rgba(0,255,136,.03);}
.signal-card.gacor::after{background:var(--green);}
.signal-card.potensial{border-color:rgba(255,183,0,.3);}
.signal-card.potensial::after{background:var(--amber);}
.signal-card::after{content:'';position:absolute;top:0;left:0;width:4px;height:100%;}
.sc-ticker{font-family:'Space Mono',monospace;font-size:17px;font-weight:700;color:var(--heading);}
.sc-bars{display:flex;gap:3px;margin:8px 0;}
.sc-bar{height:14px;border-radius:2px;}
.sc-bar.filled{background:var(--green);}
.sc-bar.empty{background:var(--border);}
.sc-stats{display:flex;gap:10px;flex-wrap:wrap;margin-top:6px;}
.sc-stat{font-family:'Space Mono',monospace;font-size:10px;color:var(--muted);}
.sc-stat span{color:var(--text);}
.log-box{background:#05080d;border:1px solid #1a2a3a;border-radius:4px;padding:10px 14px;
         font-size:11px;color:#607080;max-height:200px;overflow-y:auto;
         font-family:'Space Mono',monospace;line-height:1.8;}
.log-hot{color:#ff4444;font-weight:700;}.log-buy{color:#ffb700;font-weight:700;}
.log-ok{color:#00ff88;}.log-skip{color:#304050;}.log-scan{color:#00aaff;}
.tt-header{display:flex;align-items:center;padding:14px 0 10px 0;border-bottom:1px solid var(--border);margin-bottom:14px;}
.tt-logo{font-family:'Space Mono',monospace;font-size:20px;font-weight:700;color:var(--orange);}
.live-badge{display:inline-flex;align-items:center;gap:6px;padding:4px 12px;background:rgba(0,229,255,.08);border:1px solid rgba(0,229,255,.3);border-radius:20px;font-family:'Space Mono',monospace;font-size:10px;color:var(--accent);margin-left:auto;}
.live-dot{width:6px;height:6px;background:var(--green);border-radius:50%;animation:blink 1s infinite;}
@keyframes blink{0%,100%{opacity:1;}50%{opacity:.2;}}
.section-title{font-family:'Space Mono',monospace;font-size:11px;color:var(--muted);letter-spacing:2px;text-transform:uppercase;border-left:3px solid var(--orange);padding-left:10px;margin:18px 0 10px 0;}
.tape-wrap{overflow:hidden;white-space:nowrap;border-top:1px solid var(--border);border-bottom:1px solid var(--border);padding:5px 0;margin-bottom:14px;background:var(--surface);}
.tape-inner{display:inline-block;animation:marquee 35s linear infinite;}
@keyframes marquee{0%{transform:translateX(0)}100%{transform:translateX(-50%)}}
.tape-item{display:inline-block;margin:0 16px;font-family:'Space Mono',monospace;font-size:10px;}
.tape-item.up{color:var(--green);}.tape-item.down{color:var(--red);}.tape-item.flat{color:var(--muted);}
button[data-testid="baseButton-primary"]{background:var(--orange)!important;color:var(--bg)!important;font-family:'Space Mono',monospace!important;font-weight:700!important;border:none!important;}
::-webkit-scrollbar{width:4px;height:4px;}::-webkit-scrollbar-track{background:var(--bg);}::-webkit-scrollbar-thumb{background:var(--border);border-radius:2px;}
</style>
""", unsafe_allow_html=True)


# ── STOCK LIST ───────────────────────────────────────────────────────────────
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
    "YELO","YOII","YPAS","YULE","YUPI","ZATA","ZBRA","ZENI","ZINC","ZONE","ZYRX",
]
seen=set(); raw_stocks=[x for x in raw_stocks if not (x in seen or seen.add(x))]


# ── MARKET REGIME ────────────────────────────────────────────────────────────
def get_market_regime():
    """Fetch IHSG regime — pakai yf.Ticker().history() bukan download."""
    try:
        t = yf.Ticker("^JKSE")
        df = t.history(period="60d", interval="1d", auto_adjust=True)
        if df is None or len(df) < 20:
            return "SIDEWAYS", 0, 0, 0, "No data", 0.0
        close = df["Close"].squeeze().dropna()
        ema20 = float(close.ewm(span=20, adjust=False).mean().iloc[-1])
        ema55 = float(close.ewm(span=55, adjust=False).mean().iloc[-1])
        price = float(close.iloc[-1])
        chg   = float((close.iloc[-1] - close.iloc[-2]) / close.iloc[-2] * 100)
        if   price > ema20 and price > ema55: regime = "GREEN"
        elif price > ema20:                   regime = "SIDEWAYS"
        else:                                 regime = "RED"
        return regime, price, ema20, ema55, f"IHSG {price:,.0f}", chg
    except:
        return "SIDEWAYS", 0, 0, 0, "Error", 0.0

# ── CORE SCORER — IDX Life style, daily data ─────────────────────────────────
def score_ticker_daily(code: str):
    """
    Fetch daily data per ticker — sama kayak IDX Life.
    period="3mo", interval="1d" — lightweight, Yahoo sangat jarang rate limit ini.
    """
    try:
        ticker_yf = f"{code}.JK"
        t = yf.Ticker(ticker_yf)
        df = t.history(period="3mo", interval="1d", auto_adjust=True)
        if df is None or len(df) < 20:
            return None

        close  = df["Close"].squeeze()
        volume = df["Volume"].squeeze()
        high   = df["High"].squeeze()
        low    = df["Low"].squeeze()

        price  = float(close.iloc[-1])
        if price <= 0: return None

        # Returns
        chg_1d = float((close.iloc[-1] / close.iloc[-2] - 1) * 100) if len(close) > 1 else 0
        chg_3d = float((close.iloc[-1] / close.iloc[-4] - 1) * 100) if len(close) > 3 else chg_1d
        chg_5d = float((close.iloc[-1] / close.iloc[-6] - 1) * 100) if len(close) > 5 else chg_1d

        # Volume ratio
        vol_avg20 = float(volume.iloc[-21:-1].mean()) if len(volume) > 21 else float(volume.mean())
        vol_today = float(volume.iloc[-1])
        rvol      = vol_today / vol_avg20 if vol_avg20 > 0 else 1.0

        # Turnover (estimasi)
        turnover  = price * vol_today

        # RSI
        delta = close.diff()
        gain  = delta.clip(lower=0).ewm(span=14, adjust=False).mean()
        loss  = (-delta.clip(upper=0)).ewm(span=14, adjust=False).mean()
        rsi   = float((100 - 100 / (1 + gain / loss.replace(0, np.nan))).iloc[-1])
        rsi   = max(0, min(100, rsi))

        # EMA
        ema9   = float(close.ewm(span=9,   adjust=False).mean().iloc[-1])
        ema21  = float(close.ewm(span=21,  adjust=False).mean().iloc[-1])
        ema50  = float(close.ewm(span=50,  adjust=False).mean().iloc[-1])
        ema200 = float(close.ewm(span=200, adjust=False).mean().iloc[-1])

        # MACD
        ema12  = close.ewm(span=12, adjust=False).mean()
        ema26  = close.ewm(span=26, adjust=False).mean()
        macd_l = ema12 - ema26
        macd_s = macd_l.ewm(span=9, adjust=False).mean()
        macd_h = float((macd_l - macd_s).iloc[-1])
        macd_h_prev = float((macd_l - macd_s).iloc[-2]) if len(macd_l) > 1 else 0

        # Bollinger
        bb_mid = close.rolling(20).mean()
        bb_std = close.rolling(20).std()
        bb_up  = bb_mid + 2 * bb_std
        bb_lo  = bb_mid - 2 * bb_std
        bb_pct = float(((close - bb_lo) / (bb_up - bb_lo)).iloc[-1])
        bb_pct = max(0, min(1, bb_pct))

        # ATR
        tr = pd.concat([
            high - low,
            (high - close.shift()).abs(),
            (low  - close.shift()).abs()
        ], axis=1).max(axis=1)
        atr = float(tr.rolling(14).mean().iloc[-1])

        # Net volume (bandar proxy)
        net_vol = np.where(close >= close.shift(), volume, -volume)
        nv_ser  = pd.Series(net_vol, index=close.index)
        nv3     = float(nv_ser.rolling(3).sum().iloc[-1])
        nv8     = float(nv_ser.rolling(8).sum().iloc[-1])

        # Scoring (IDX Life style + enhanced)
        score = 0.0

        # Momentum
        score += min(chg_1d * 5, 20) if chg_1d > 0 else max(chg_1d * 2, -10)
        score += min(chg_3d * 2, 10) if chg_3d > 0 else 0
        score += min(chg_5d * 1,  5) if chg_5d > 0 else 0

        # Volume surge — paling penting
        if rvol >= 5.0:  score += 25
        elif rvol >= 3.0: score += 20
        elif rvol >= 2.0: score += 15
        elif rvol >= 1.5: score += 8
        elif rvol >= 1.0: score += 3
        else:             score -= 5

        # RSI zone
        if   40 <= rsi <= 60: score += 15   # sweet spot running
        elif 60 <  rsi <= 70: score += 10   # hot zone masih ok
        elif 30 <= rsi < 40:  score += 8    # rebound potential
        elif rsi > 75:        score -= 10   # overbought

        # MACD
        if macd_h > 0 and macd_h > macd_h_prev:
            score += 12   # expanding histogram
        elif macd_h > 0:
            score += 6
        elif macd_h < 0 and macd_h > macd_h_prev:
            score += 3    # diverging bullish

        # EMA structure
        if   ema9 > ema21 > ema50: score += 10
        elif ema9 > ema21:         score += 6
        elif price > ema50:        score += 3

        # Above EMA200 (bull market filter)
        if price > ema200 * 0.98:  score += 3

        # Bollinger room
        if bb_pct < 0.3:  score += 8    # banyak ruang naik
        elif bb_pct < 0.5: score += 4
        elif bb_pct > 0.9: score -= 5   # sudah di atas BB

        # Net volume (bandar proxy)
        if nv3 > 0 and nv8 > 0:  score += 10
        elif nv3 > 0:             score += 5

        score = min(max(score, 0), 100)

        # Signal
        if   score >= 70 and rvol >= 2.0: signal = "GACOR 🔥"
        elif score >= 55:                  signal = "POTENSIAL ⚡"
        elif score >= 40:                  signal = "WATCH 👀"
        else:                              signal = "SKIP ❄️"

        # TP/SL dari ATR
        tp = price + 2.5 * atr
        sl = price - 1.5 * atr
        rr = (tp - price) / max(price - sl, 0.01)

        trend = "▲ UP" if ema9 > ema21 > ema50 else ("▼ DOWN" if ema9 < ema21 < ema50 else "◆ SIDE")

        return {
            "Ticker":     code,
            "Price":      int(price),
            "Chg 1D %":   round(chg_1d, 2),
            "Chg 3D %":   round(chg_3d, 2),
            "Chg 5D %":   round(chg_5d, 2),
            "RVOL":       round(rvol, 2),
            "Turnover(M)":round(turnover / 1e6, 1),
            "RSI":        round(rsi, 1),
            "MACD Hist":  round(macd_h, 4),
            "BB%":        round(bb_pct, 2),
            "ATR":        round(atr, 0),
            "EMA9":       round(ema9, 0),
            "EMA21":      round(ema21, 0),
            "EMA50":      round(ema50, 0),
            "Trend":      trend,
            "NV3":        int(nv3),
            "NV8":        int(nv8),
            "Score":      round(score, 1),
            "Signal":     signal,
            "TP":         int(tp),
            "SL":         int(sl),
            "R:R":        round(rr, 1),
        }
    except:
        return None


# ── LIVE SCAN ENGINE — IDX Life pattern ──────────────────────────────────────
def run_live_scan(pool: list, scan_size: int = 200):
    """
    Sequential per-ticker scan — SAMA persis pola IDX Life.
    yf.Ticker().history() per ticker, daily, 3 bulan.
    Lolos rate limit karena: daily data ringan + sequential + small delay.
    """
    pool = pool[:scan_size]
    total   = len(pool)
    results = []
    log_lines = []

    status_ph = st.empty()
    prog_ph   = st.progress(0)
    log_ph    = st.empty()
    live_ph   = st.empty()

    status_ph.markdown(
        f'<div style="color:#ff9900;font-size:12px;font-family:Space Mono,monospace">'
        f'🔍 SCANNING {total} EMITEN — HARAP TUNGGU...</div>',
        unsafe_allow_html=True
    )

    for i, code in enumerate(pool):
        prog_ph.progress((i + 1) / total)
        ts = datetime.now(jakarta_tz).strftime("%H:%M:%S")

        log_lines.append(
            f'<span class="log-scan">[{ts}]</span> → {code}.JK...'
        )
        log_ph.markdown(
            f'<div class="log-box">{"<br>".join(log_lines[-12:])}</div>',
            unsafe_allow_html=True
        )

        result = score_ticker_daily(code)

        if result:
            s   = result["Score"]
            sig = result["Signal"]
            chg = result["Chg 1D %"]
            rv  = result["RVOL"]

            if "GACOR" in sig:
                cls = "log-hot"; icon = "🔥"
            elif "POTENSIAL" in sig:
                cls = "log-buy"; icon = "⚡"
            elif "WATCH" in sig:
                cls = "log-ok"; icon = "👀"
            else:
                cls = "log-skip"; icon = "❄️"

            log_lines[-1] = (
                f'<span class="log-scan">[{ts}]</span> '
                f'<span class="{cls}">{icon} {code} | '
                f'Score:{s:.0f} | Chg:{chg:+.2f}% | RVOL:{rv:.1f}x | {sig}</span>'
            )
            results.append(result)
        else:
            log_lines[-1] = (
                f'<span class="log-scan">[{ts}]</span> '
                f'<span class="log-skip">— {code} | no data</span>'
            )

        log_ph.markdown(
            f'<div class="log-box">{"<br>".join(log_lines[-12:])}</div>',
            unsafe_allow_html=True
        )

        # Live preview setiap 10 ticker
        if results and (i % 10 == 0 or i == total - 1):
            df_live = pd.DataFrame(results).sort_values("Score", ascending=False)
            live_ph.dataframe(
                df_live[["Ticker","Price","Chg 1D %","RVOL","RSI","Score","Signal"]].head(10),
                use_container_width=True, height=240
            )

        # Small delay — same IDX Life pattern
        # Tidak perlu delay besar karena daily data = lightweight
        time.sleep(random.uniform(0.05, 0.15))

    done_ts = datetime.now(jakarta_tz).strftime("%H:%M:%S")
    prog_ph.progress(1.0)
    status_ph.markdown(
        f'<div style="color:#00ff88;font-size:12px;font-family:Space Mono,monospace">'
        f'✅ SCAN SELESAI [{done_ts}] — {len(results)}/{total} emiten berhasil</div>',
        unsafe_allow_html=True
    )
    live_ph.empty()
    log_ph.empty()

    if not results:
        return []

    return sorted(results, key=lambda x: x["Score"], reverse=True)

# ── TELEGRAM ─────────────────────────────────────────────────────────────────
def send_telegram_alert(results_top):
    if not TOKEN or not CHAT_ID or not results_top: return
    now  = datetime.now(jakarta_tz)
    sep  = "━" * 26
    msg  = (f"🔥 *THETA TURBO DS — AUTO SCAN*\n"
            f"⏰ `{now.strftime('%H:%M:%S')} WIB · {now.strftime('%d %b %Y')}`\n{sep}\n")
    for i, r in enumerate(results_top[:5], 1):
        sig = r['Signal']; bar = "█" * int(r['Score'] / 10) + "░" * (10 - int(r['Score'] / 10))
        msg += (f"\n{'🔥' if 'GACOR' in sig else '⚡'} *#{i} {r['Ticker']}*  `{sig}`\n"
                f"   💰 `Rp{r['Price']:,}` | Chg: `{r['Chg 1D %']:+.2f}%`\n"
                f"   📊 `[{bar}] {r['Score']:.0f}/100`\n"
                f"   RVOL: `{r['RVOL']}x` | RSI: `{r['RSI']}`\n"
                f"   🎯 TP: `{r['TP']:,}` | SL: `{r['SL']:,}` | R:R `{r['R:R']}`\n")
    msg += f"\n{sep}\n⚠️ _Bukan saran investasi. DYOR!_"
    try:
        requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                      data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"},
                      timeout=10)
    except: pass

# ── REGIME ───────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def get_regime_cached():
    return get_market_regime()


# ── HEADER ────────────────────────────────────────────────────────────────────
regime, ihsg_price, ema20, ema55, regime_detail, ihsg_chg = get_regime_cached()
now_jkt = datetime.now(jakarta_tz)
rcolor  = {"GREEN": "#00ff88", "RED": "#ff3d5a", "SIDEWAYS": "#ffb700"}.get(regime, "#4a5568")
rmode   = {"GREEN": "Scalping/Momentum 🚀", "RED": "Reversal 🎯 only", "SIDEWAYS": "Scalping ⚡"}.get(regime, "Scalping ⚡")
chg_col = "#00ff88" if ihsg_chg >= 0 else "#ff3d5a"
chg_sym = "▲" if ihsg_chg >= 0 else "▼"

st.markdown(f"""
<div class="tt-header">
  <div>
    <div class="tt-logo">🔥 THETA TURBO v5.2 DS</div>
    <div style="font-size:10px;color:var(--muted);letter-spacing:2px;text-transform:uppercase;">
      Auto-scan · Daily Scan · IDX · {len(raw_stocks)} Emiten
    </div>
  </div>
  <div class="live-badge">
    <div class="live-dot"></div>LIVE {now_jkt.strftime("%H:%M:%S")} WIB
  </div>
</div>""", unsafe_allow_html=True)

st.markdown(f"""
<div style="background:rgba(0,0,0,.4);border:1px solid {rcolor}44;border-radius:8px;
     padding:10px 16px;margin-bottom:14px;border-left:4px solid {rcolor};">
  <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;">
    <div>
      <div style="font-family:Space Mono,monospace;font-size:11px;font-weight:700;color:{rcolor};">
        {'🟢' if regime=='GREEN' else '🔴' if regime=='RED' else '🟡'} MARKET {regime} — {rmode}
      </div>
      <div style="font-size:9px;color:#4a5568;margin-top:2px;">{regime_detail}</div>
    </div>
    <div style="text-align:right;font-family:Space Mono,monospace;">
      <div style="font-size:16px;font-weight:700;color:{rcolor};">
        {ihsg_price:,.0f} <span style="font-size:10px;color:{chg_col}">{chg_sym}{abs(ihsg_chg):.2f}%</span>
      </div>
      <div style="font-size:9px;color:#4a5568;">EMA20 {ema20:,.0f} · EMA55 {ema55:,.0f}</div>
    </div>
  </div>
</div>""", unsafe_allow_html=True)

# ── SCAN SIZE SELECTOR + CONTROLS ────────────────────────────────────────────
ctrl1, ctrl2, ctrl3 = st.columns([2, 2, 1])
with ctrl1:
    scan_size = st.radio(
        "Scan Size", ["LQ45 ⚡ (45)", "200 🔥", "Full 🦅 (semua)"],
        index=1, horizontal=True, key="scan_size"
    )
with ctrl2:
    min_score     = st.slider("Min Score", 0, 100, 40, 5, key="min_score")
    min_rvol      = st.slider("Min RVOL",  1.0, 5.0, 1.5, 0.5, key="min_rvol")
with ctrl3:
    st.markdown("<br>", unsafe_allow_html=True)
    tele_auto = st.toggle("📡 Auto Telegram", value=False, key="tele_auto")
    rescan    = st.button("🔄 RE-SCAN", type="primary", use_container_width=True, key="btn_rescan")

# LQ45 list
LQ45 = ["BBCA","BBRI","BMRI","TLKM","ASII","GOTO","BYAN","MDKA","UNVR","ICBP",
         "INDF","KLBF","SIDO","MNCN","EXCL","TOWR","PGAS","PTBA","ADRO","ITMG",
         "INCO","HRUM","JSMR","SMGR","WIKA","WSKT","ANTM","PTPP","TBIG","BRIS",
         "AMMN","MBMA","CUAN","BBNI","BBTN","BMTR","INKP","BRPT","TPIA","BREN",
         "EMTK","GOTO","PANI","DSSA","ISAT"]

if "45" in scan_size:
    pool = LQ45
elif "200" in scan_size:
    pool = raw_stocks[:200]
else:
    pool = raw_stocks

# ── AUTO-SCAN ON FIRST RUN — IDX Life pattern ─────────────────────────────────
pool_key  = f"{scan_size}_{len(pool)}"
first_run = not st.session_state.first_scan_done

if first_run or rescan or st.session_state.get("last_pool_key") != pool_key:
    st.session_state.last_pool_key    = pool_key
    st.session_state.first_scan_done  = True
    sz = len(pool)
    st.markdown(
        f'<div style="font-family:Space Mono,monospace;font-size:11px;color:#ffb700;'
        f'padding:8px 12px;background:#0d1117;border-radius:6px;margin-bottom:10px;">'
        f'⚡ AUTO-SCAN {sz} emiten · Daily data · {"Pertama kali buka app" if first_run else "Re-scan"}</div>',
        unsafe_allow_html=True
    )
    results = run_live_scan(pool, scan_size=sz)
    st.session_state.scan_results   = results
    st.session_state.last_scan_time = datetime.now(jakarta_tz).strftime("%H:%M:%S")

    # Auto telegram
    if tele_auto and results:
        top = [r for r in results if "GACOR" in r.get("Signal","") or "POTENSIAL" in r.get("Signal","")]
        if top and top != st.session_state.get("tt_last_sent_list"):
            send_telegram_alert(top[:5])
            st.session_state.tt_last_sent_list = top[:5]

# ── DISPLAY RESULTS ───────────────────────────────────────────────────────────
results = st.session_state.scan_results

if not results:
    st.info("Belum ada data. Scan sedang berjalan atau tidak ada emiten yang lolos.")
    st.stop()

# Filter
df_all  = pd.DataFrame(results)
df_out  = df_all[
    (df_all["Score"]    >= min_score) &
    (df_all["RVOL"]     >= min_rvol)
].copy().reset_index(drop=True)

gacor   = df_out[df_out["Signal"].str.contains("GACOR",   na=False)]
potensi = df_out[df_out["Signal"].str.contains("POTENSIAL", na=False)]
watch   = df_out[df_out["Signal"].str.contains("WATCH",   na=False)]
avg_rsi = df_out["RSI"].mean() if len(df_out) > 0 else 0

st.markdown(f"""
<div class="metric-row">
  <div class="metric-card" style="border-top-color:{rcolor}">
    <div class="metric-label">Market</div>
    <div class="metric-value" style="font-size:16px;color:{rcolor}">{regime}</div>
    <div class="metric-sub">{ihsg_price:,.0f} {chg_sym}{abs(ihsg_chg):.2f}%</div>
  </div>
  <div class="metric-card green">
    <div class="metric-label">GACOR 🔥</div>
    <div class="metric-value">{len(gacor)}</div>
    <div class="metric-sub">Score≥70 + RVOL≥2x</div>
  </div>
  <div class="metric-card amber">
    <div class="metric-label">POTENSIAL ⚡</div>
    <div class="metric-value">{len(potensi)}</div>
  </div>
  <div class="metric-card">
    <div class="metric-label">WATCH 👀</div>
    <div class="metric-value">{len(watch)}</div>
  </div>
  <div class="metric-card orange">
    <div class="metric-label">Dipindai</div>
    <div class="metric-value">{len(df_all)}</div>
    <div class="metric-sub">dari {len(pool)} emiten</div>
  </div>
  <div class="metric-card">
    <div class="metric-label">Avg RSI</div>
    <div class="metric-value" style="color:{'#00ff88' if avg_rsi>50 else '#ffb700' if avg_rsi>35 else '#ff3d5a'}">{avg_rsi:.0f}</div>
  </div>
  <div class="metric-card">
    <div class="metric-label">Last Scan</div>
    <div class="metric-value" style="font-size:14px;color:#00e5ff">{st.session_state.get('last_scan_time','--')}</div>
    <div class="metric-sub">WIB</div>
  </div>
</div>""", unsafe_allow_html=True)

# Ticker tape
if len(df_out) > 0:
    th = '<div class="tape-wrap"><div class="tape-inner">'
    for _, row in df_out.iterrows():
        roc = row["Chg 1D %"]
        cls = "up" if roc > 0 else ("down" if roc < 0 else "flat")
        sym = "🔥" if "GACOR" in row["Signal"] else ("⚡" if "POTENSIAL" in row["Signal"] else ("▲" if roc > 0 else "▼"))
        th += f'<span class="tape-item {cls}">{row["Ticker"]} {row["Price"]:,} {sym}{abs(roc):.1f}% [S:{row["Score"]:.0f}]</span>'
    th = th + th.replace('tape-inner">', '')
    th += '</div></div>'
    st.markdown(th, unsafe_allow_html=True)

# Signal Cards
if len(df_out) > 0:
    st.markdown('<div class="section-title">🏆 Top Candidates</div>', unsafe_allow_html=True)
    card_html = '<div class="signal-grid">'
    for _, row in df_out.head(20).iterrows():
        sc_int = int(min(row["Score"] / 10, 10))
        bars   = ''.join([f'<div class="sc-bar {"filled" if i < sc_int else "empty"}" style="width:22px"></div>' for i in range(10)])
        is_g   = "GACOR" in row["Signal"]
        sc_col = "#00ff88" if is_g else ("#ffb700" if "POTENSIAL" in row["Signal"] else "#00e5ff")
        roc_c  = "#00ff88" if row["Chg 1D %"] > 0 else "#ff3d5a"
        te     = "📈" if "▲" in row["Trend"] else ("📉" if "▼" in row["Trend"] else "➡️")
        card_html += f"""<div class="signal-card {'gacor' if is_g else 'potensial' if 'POTENSIAL' in row['Signal'] else ''}">
          <div style="display:flex;justify-content:space-between;align-items:flex-start;">
            <div>
              <div class="sc-ticker">{row['Ticker']}</div>
              <div style="font-family:Space Mono,monospace;font-size:12px;color:{roc_c}">{row['Price']:,} {te}</div>
            </div>
            <div style="text-align:right">
              <div style="font-family:Space Mono,monospace;font-size:9px;color:#4a5568">SCORE</div>
              <div style="font-family:Space Mono,monospace;font-size:20px;font-weight:700;color:{sc_col}">{row['Score']:.0f}</div>
            </div>
          </div>
          <div style="font-size:12px;font-weight:700;color:{sc_col};margin:5px 0">{row['Signal']}</div>
          <div class="sc-bars">{bars}</div>
          <div class="sc-stats">
            <div class="sc-stat">Chg1D <span style="color:{roc_c}">{row['Chg 1D %']:+.1f}%</span></div>
            <div class="sc-stat">RVOL <span>{row['RVOL']}x</span></div>
            <div class="sc-stat">RSI <span>{row['RSI']}</span></div>
            <div class="sc-stat">BB% <span>{row['BB%']:.2f}</span></div>
          </div>
          <div class="sc-stats" style="margin-top:5px">
            <div class="sc-stat">TP <span style="color:#00ff88">Rp{row['TP']:,}</span></div>
            <div class="sc-stat">SL <span style="color:#ff3d5a">Rp{row['SL']:,}</span></div>
            <div class="sc-stat">R:R <span>{row['R:R']}</span></div>
          </div>
        </div>"""
    card_html += '</div>'
    st.markdown(card_html, unsafe_allow_html=True)

# Full table
st.markdown('<div class="section-title">Full Table</div>', unsafe_allow_html=True)
show_cols = ["Ticker","Price","Score","Signal","Trend","Chg 1D %","Chg 3D %","RVOL",
             "RSI","MACD Hist","BB%","TP","SL","R:R","Turnover(M)"]
show_cols = [c for c in show_cols if c in df_out.columns]
st.dataframe(
    df_out[show_cols],
    width="stretch", hide_index=True,
    column_config={
        "Score":       st.column_config.ProgressColumn("Score", min_value=0, max_value=100, format="%.0f"),
        "Chg 1D %":   st.column_config.NumberColumn("Chg 1D %",  format="%.2f%%"),
        "Chg 3D %":   st.column_config.NumberColumn("Chg 3D %",  format="%.2f%%"),
        "RVOL":        st.column_config.NumberColumn("RVOL",       format="%.1fx"),
        "Turnover(M)": st.column_config.NumberColumn("Turnover(M)", format="Rp%.0fM"),
    }
)

# Telegram manual send
st.markdown("---")
tc1, tc2 = st.columns([3, 1])
with tc1:
    st.markdown(f'<div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568">📡 Telegram · {len(gacor)} GACOR · {len(potensi)} POTENSIAL · Token: {"✅" if TOKEN else "❌ set di secrets"}</div>', unsafe_allow_html=True)
with tc2:
    if st.button("📲 Kirim Telegram", use_container_width=True):
        top_send = df_out.head(5).to_dict("records")
        if top_send:
            send_telegram_alert(top_send)
            st.success("✅ Terkirim!")
        else:
            st.warning("Tidak ada hasil untuk dikirim.")

# Watchlist quick analysis
st.markdown("---")
st.markdown('<div class="section-title">👁️ Quick Watchlist</div>', unsafe_allow_html=True)
wl_input = st.text_input("Ticker (pisah koma)", placeholder="BBCA, ARCI, ASSA, GOTO", key="wl_quick")
if wl_input.strip():
    wl_tickers = [t.strip().upper() for t in wl_input.split(",") if t.strip()]
    wl_results = []
    with st.spinner(f"Analisa {len(wl_tickers)} ticker..."):
        for code in wl_tickers:
            r = score_ticker_daily(code)
            if r: wl_results.append(r)
            time.sleep(random.uniform(0.05, 0.15))
    if wl_results:
        df_wl = pd.DataFrame(wl_results).sort_values("Score", ascending=False)
        st.dataframe(df_wl[show_cols].head(20), width="stretch", hide_index=True)

# Footer
st.markdown(f"""
<div style="margin-top:24px;padding-top:12px;border-top:1px solid #1c2533;
     display:flex;justify-content:space-between;flex-wrap:wrap;gap:8px;">
  <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;">
    🔥 Theta Turbo v5.2 DS · IDX · Daily Scan · Auto-run on open
  </div>
  <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;">
    Last scan: {st.session_state.get('last_scan_time','--')} WIB · {len(pool)} emiten pool
  </div>
</div>
<div style="font-family:Space Mono,monospace;font-size:9px;color:#2d3748;text-align:center;margin-top:6px;">
  ⚠️ BUKAN saran investasi · Daily data (delayed) · DYOR selalu
</div>""", unsafe_allow_html=True)
