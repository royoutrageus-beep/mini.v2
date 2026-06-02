import yfinance as yf
import pandas as pd
import streamlit as st
import time
import random
import requests
import numpy as np
import pytz
from datetime import datetime

# ════════════════════════════════════════════════════
#  CONFIG
# ════════════════════════════════════════════════════
TOKEN      = st.secrets.get("TELEGRAM_TOKEN", "")
CHAT_ID    = st.secrets.get("TELEGRAM_CHAT_ID", "")
jakarta_tz = pytz.timezone('Asia/Jakarta')

for _k, _v in [("tt_last_sent", set()), ("wl_results", []),
                ("wl_mode_used", ""), ("scan_results", []),
                ("data_dict", {}), ("last_scan_time", None),
                ("last_scan_mode", "Scalping ⚡"),
                ("active_scan_mode", "Scalping ⚡"),
                ("active_auto_regime", True),
                ("sector_data", {}), ("beta_data", []),
                ("gapup_results", []), ("bsjp_results", []),
                ("first_scan_done", False),
                ("last_scan_pool_key", "")]:
    if _k not in st.session_state: st.session_state[_k] = _v

st.set_page_config(layout="wide", page_title="Theta Turbo v5.2", page_icon="🔥",
                   initial_sidebar_state="collapsed")

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
.metric-card.purple::before{background:var(--purple);}
.metric-label{font-size:10px;color:var(--muted);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:4px;}
.metric-value{font-family:'Space Mono',monospace;font-size:24px;font-weight:700;color:var(--heading);line-height:1;}
.metric-sub{font-size:10px;color:var(--muted);margin-top:3px;}
.signal-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:12px;margin-bottom:20px;}
.signal-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px;position:relative;overflow:hidden;transition:border-color .2s;}
.signal-card.gacor{border-color:rgba(0,255,136,.4);background:rgba(0,255,136,.03);}
.signal-card.potensial{border-color:rgba(255,183,0,.3);background:rgba(255,183,0,.03);}
.signal-card.watch{border-color:rgba(0,229,255,.2);}
.signal-card.bagger{border-color:rgba(191,95,255,.6);background:rgba(191,95,255,.05);box-shadow:0 0 20px rgba(191,95,255,.15);}
.signal-card::after{content:'';position:absolute;top:0;left:0;width:4px;height:100%;}
.signal-card.gacor::after{background:var(--green);}
.signal-card.potensial::after{background:var(--amber);}
.signal-card.watch::after{background:var(--accent);}
.signal-card.bagger::after{background:var(--purple);}
.sc-ticker{font-family:'Space Mono',monospace;font-size:18px;font-weight:700;color:var(--heading);}
.sc-price{font-family:'Space Mono',monospace;font-size:13px;color:var(--muted);}
.sc-signal{font-size:13px;font-weight:700;margin:6px 0;}
.sc-bars{display:flex;gap:3px;margin:8px 0;}
.sc-bar{height:16px;border-radius:2px;}
.sc-bar.filled{background:var(--green);}
.sc-bar.filled-purple{background:var(--purple);}
.sc-bar.empty{background:var(--border);}
.sc-stats{display:flex;gap:12px;flex-wrap:wrap;margin-top:8px;}
.sc-stat{font-family:'Space Mono',monospace;font-size:10px;color:var(--muted);}
.sc-stat span{color:var(--text);}
.alert-box{background:rgba(255,61,90,.06);border:1px solid rgba(255,61,90,.4);border-radius:8px;padding:14px 18px;margin-bottom:16px;animation:pulse-border 2s infinite;}
.bagger-alert-box{background:rgba(191,95,255,.06);border:1px solid rgba(191,95,255,.5);border-radius:8px;padding:14px 18px;margin-bottom:16px;animation:pulse-purple 2s infinite;}
@keyframes pulse-border{0%,100%{border-color:rgba(255,61,90,.4);}50%{border-color:rgba(255,61,90,.9);}}
@keyframes pulse-purple{0%,100%{border-color:rgba(191,95,255,.4);}50%{border-color:rgba(191,95,255,.9);}}
.alert-title{color:var(--red);font-family:'Space Mono',monospace;font-size:12px;font-weight:700;letter-spacing:2px;}
.bagger-title{color:var(--purple);font-family:'Space Mono',monospace;font-size:12px;font-weight:700;letter-spacing:2px;}
.tape-wrap{overflow:hidden;white-space:nowrap;border-top:1px solid var(--border);border-bottom:1px solid var(--border);padding:5px 0;margin-bottom:16px;background:var(--surface);}
.tape-inner{display:inline-block;animation:marquee 35s linear infinite;}
@keyframes marquee{0%{transform:translateX(0)}100%{transform:translateX(-50%)}}
.tape-item{display:inline-block;margin:0 18px;font-family:'Space Mono',monospace;font-size:10px;}
.tape-item.up{color:var(--green);}.tape-item.down{color:var(--red);}.tape-item.flat{color:var(--muted);}.tape-item.bagger{color:var(--purple);}
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
#  STOCK LIST — FULL IDX (900+ emiten)
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
    "YELO","YOII","YPAS","YULE","YUPI","ZATA","ZBRA","ZENI","ZINC","ZONE","ZYRX",
]
seen = set(); raw_stocks = [x for x in raw_stocks if not (x in seen or seen.add(x))]
stocks_yf  = [s + ".JK" for s in raw_stocks]
stock_map  = {s + ".JK": s for s in raw_stocks}

# ════════════════════════════════════════════════════
#  HUMANIZED FETCH HELPERS v2 — anti Yahoo rate limit
#  Strategy: random chunk size + variable human-like delay
# ════════════════════════════════════════════════════
import requests as _req_sess
_YF_SESSION = _req_sess.Session()
_YF_SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
})

# ════════════════════════════════════════════════════
#  DATASECTORS API — HYBRID: DS PRIMARY + YF FALLBACK
#  DS_KEY kosong = pure yFinance (tetap jalan)
# ════════════════════════════════════════════════════
try:    DS_KEY = st.secrets.get("DATASECTORS_API_KEY", "")
except: DS_KEY = ""
DS_BASE = "https://api.datasectors.com"

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

def _find_chartbit(obj, depth=0):
    if depth > 6: return None
    if isinstance(obj, dict):
        if "chartbit" in obj: return obj["chartbit"]
        for v in obj.values():
            r = _find_chartbit(v, depth+1)
            if r: return r
    return None

def fetch_ds_ohlcv(ticker, interval="15m", limit=200, force_fresh=False):
    """DataSectors API fetch — returns df with FBuy/FSell columns if available."""
    import requests as _rq
    if not DS_KEY: return None
    t  = ticker.replace(".JK","").upper().strip()
    tf = TF_MAP.get(str(interval).lower(), "15m")
    ts_p = int(time.time())
    url = f"{DS_BASE}/api/chart-saham/{t}/{tf}/latest?_={ts_p}"
    try:
        r = _rq.get(url, headers=_ds_headers(), timeout=12)
        if r.status_code != 200: return None
        rows = _find_chartbit(r.json())
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
        df = df.dropna(subset=["Close"]).sort_index()
        if len(df) < 20: return None
        return df
    except: return None


def _human_sleep(batch_num, total_batches):
    """
    Mimic human browsing pattern:
    - Checkpoint pause di 25/50/75% (kayak manusia baca layar)
    - 10% chance long pause (distracted)
    - 20% chance medium pause
    - Sisanya short random delay
    """
    pct = batch_num / max(total_batches, 1)
    if abs(pct-0.25)<0.06 or abs(pct-0.50)<0.06 or abs(pct-0.75)<0.06:
        pause = random.uniform(3.0, 6.0)
    elif random.random() < 0.10:
        pause = random.uniform(5.0, 9.0)
    elif random.random() < 0.20:
        pause = random.uniform(1.5, 3.5)
    else:
        pause = random.uniform(0.4, 1.3)
    time.sleep(pause)

def _random_chunks(tickers, min_sz=5, max_sz=15):
    """Random batch sizes — tidak fixed, lebih natural kayak manusia."""
    lst = list(tickers)
    batches = []
    i = 0
    while i < len(lst):
        sz = min(random.randint(min_sz, max_sz), len(lst)-i)
        batches.append(lst[i:i+sz])
        i += sz
    return batches

def _ticker_history(ticker_yf, period="7d", interval="15m"):
    """yf.Ticker().history() — pakai chart API, endpoint beda dari download."""
    try:
        t = yf.Ticker(ticker_yf, session=_YF_SESSION)
        df = t.history(period=period, interval=interval, auto_adjust=True)
        if df is None or df.empty: return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(-1)
        rename = {c:c.capitalize() for c in df.columns if c.islower()}
        if rename: df = df.rename(columns=rename)
        req = ["Open","High","Low","Close","Volume"]
        if any(c not in df.columns for c in req): return None
        df = df[req].dropna(subset=["Close"])
        df = df[df["Volume"] > 0]
        return df if len(df) >= 2 else None
    except: return None

def _yf_extract(raw, ticker, n_batch):
    """Robust yfinance MultiIndex extraction — all versions supported."""
    try:
        if raw is None or raw.empty: return None
        _ohlcv = {'Open','High','Low','Close','Volume','open','high','low','close','volume'}
        if n_batch == 1:
            df = raw.copy()
            if isinstance(df.columns, pd.MultiIndex):
                l0 = df.columns.get_level_values(0).unique().tolist()
                l1 = df.columns.get_level_values(1).unique().tolist()
                if any(x in _ohlcv for x in l0):   df = df.droplevel(1, axis=1)
                elif any(x in _ohlcv for x in l1): df = df.droplevel(0, axis=1)
        else:
            if not isinstance(raw.columns, pd.MultiIndex): return None
            l0 = raw.columns.get_level_values(0).unique().tolist()
            l1 = raw.columns.get_level_values(1).unique().tolist()
            if ticker in l0:   df = raw[ticker].copy()
            elif ticker in l1: df = raw.xs(ticker, axis=1, level=1).copy()
            else: return None
            if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.droplevel(-1)
        rename = {c: c.capitalize() for c in df.columns if c.islower()}
        if rename: df = df.rename(columns=rename)
        if 'Adj Close' in df.columns and 'Close' not in df.columns:
            df = df.rename(columns={'Adj Close': 'Close'})
        required = ['Open','High','Low','Close','Volume']
        if any(c not in df.columns for c in required): return None
        df = df[required].dropna(subset=['Close'])
        df = df[df['Volume'] > 0]
        return df if len(df) > 0 else None
    except: return None

# ════════════════════════════════════════════════════
#  FETCH INTRADAY — HUMANIZED v2
#  Random chunk + variable delay = anti Yahoo bot detection
# ════════════════════════════════════════════════════
@st.cache_data(ttl=300)
def fetch_intraday(tickers, chunk=None):
    """
    HF Terminal fetch style — 1 shot batch download semua ticker sekaligus.
    yf.download(ALL, period='5d', interval='15m', group_by='ticker', threads=True)
    @cache(ttl=300) = tidak re-fetch kalau belum 5 menit → no rate limit.
    Extract per-ticker dari hasil batch pakai _yf_extract.
    """
    ticker_list = list(tickers)
    if not ticker_list:
        return {}
    all_dfs = {}
    # Batch 50 ticker sekali → lebih manageable tapi tetap jauh lebih sedikit request
    # HF Terminal: 1 shot semua, kita pakai batch 50 untuk safety
    BATCH = 50
    batches = [ticker_list[i:i+BATCH] for i in range(0, len(ticker_list), BATCH)]
    for batch in batches:
        if not batch: continue
        try:
            raw = yf.download(
                list(batch),
                period="7d",
                interval="15m",
                group_by="ticker",
                progress=False,
                auto_adjust=True,
                threads=True,          # HF Terminal pakai ini — OK untuk batch daily/intraday
                session=_YF_SESSION,
            )
            for t in batch:
                try:
                    df = _yf_extract(raw, t, len(batch))
                    if df is not None and len(df) >= 30:
                        all_dfs[t] = df
                except:
                    pass
        except:
            # Fallback: humanized per-ticker kalau batch gagal
            _sub_batches = _random_chunks(batch, min_sz=5, max_sz=10)
            _n = len(_sub_batches)
            for _bi, _sub in enumerate(_sub_batches):
                for t in _sub:
                    if t in all_dfs: continue
                    try:
                        df = _ticker_history(t, "7d", "15m")
                        if df is None:
                            _rw = yf.download(t, period="7d", interval="15m",
                                              progress=False, auto_adjust=True,
                                              threads=False, session=_YF_SESSION)
                            df = _yf_extract(_rw, t, 1)
                        if df is not None and len(df) >= 30:
                            all_dfs[t] = df
                    except:
                        pass
                    time.sleep(random.uniform(0.1, 0.3))
                _human_sleep(_bi, _n)
    return all_dfs

@st.cache_data(ttl=3600)
def fetch_daily_bagger(tickers, chunk=None):
    """
    HF Terminal style — 1 shot batch download 60-hari daily untuk Wyckoff Bagger.
    @cache(ttl=3600) = cache 1 jam, daily data tidak perlu sering refresh.
    """
    ticker_list = list(tickers)
    if not ticker_list:
        return {}
    all_dfs = {}
    BATCH = 100  # daily lebih ringan, batch lebih besar
    batches = [ticker_list[i:i+BATCH] for i in range(0, len(ticker_list), BATCH)]
    for batch in batches:
        if not batch: continue
        try:
            raw = yf.download(
                list(batch),
                period="60d",
                interval="1d",
                group_by="ticker",
                progress=False,
                auto_adjust=True,
                threads=True,
                session=_YF_SESSION,
            )
            for t in batch:
                try:
                    df = _yf_extract(raw, t, len(batch))
                    if df is not None and len(df) >= 20:
                        all_dfs[t] = df
                except:
                    pass
        except:
            # Fallback humanized
            _sub = _random_chunks(batch, min_sz=8, max_sz=20)
            _n = len(_sub)
            for _bi, _sb in enumerate(_sub):
                for t in _sb:
                    if t in all_dfs: continue
                    try:
                        df = _ticker_history(t, "60d", "1d")
                        if df is None:
                            _rw = yf.download(t, period="60d", interval="1d",
                                              progress=False, auto_adjust=True,
                                              threads=False, session=_YF_SESSION)
                            df = _yf_extract(_rw, t, 1)
                        if df is not None and len(df) >= 20:
                            all_dfs[t] = df
                    except:
                        pass
                    time.sleep(random.uniform(0.1, 0.3))
                _human_sleep(_bi, _n)
    return all_dfs

# ════════════════════════════════════════════════════
#  MARKET REGIME DETECTOR — IHSG (FIXED)
# ════════════════════════════════════════════════════
@st.cache_data(ttl=300)
def get_market_regime():
    try:
        df = _ticker_history("^JKSE", period="60d", interval="1d")
        if df is None:
            try:
                df = yf.download("^JKSE", period="60d", interval="1d",
                                 progress=False, auto_adjust=True, session=_YF_SESSION)
                if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.droplevel(-1)
            except: pass
        if df is None or df.empty or len(df) < 10:
            return ("UNKNOWN", 0, 0, 0, "Data IHSG kurang", 0.0)
        if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.droplevel(-1)
        close = df["Close"].dropna()
        if isinstance(close, pd.DataFrame): close = close.iloc[:, 0]
        if len(close) < 10: return ("UNKNOWN", 0, 0, 0, "Data close kurang", 0.0)
        ema20 = float(close.ewm(span=20, adjust=False).mean().iloc[-1])
        ema55 = float(close.ewm(span=min(55, len(close)-1), adjust=False).mean().iloc[-1])
        price = float(close.iloc[-1])
        chg   = float(((close.iloc[-1] - close.iloc[-2]) / close.iloc[-2]) * 100)
        band           = 0.012
        pct_vs_e20     = (price - ema20) / ema20 * 100
        above_e20_any  = price > ema20 * (1 - band)
        above_e20_clear= price > ema20 * (1 + band)
        above_e55      = price > ema55
        recovering     = chg > 0.3
        bearish_confirm= chg < -0.3 and not above_e20_any
        if above_e20_clear and above_e55:
            regime = "GREEN"; detail = f"IHSG {price:,.0f} > EMA20 & EMA55 → Bullish ✅"
        elif above_e20_any and above_e55:
            regime = "GREEN"; detail = f"IHSG {price:,.0f} dekat EMA20({ema20:,.0f}) & > EMA55"
        elif above_e20_any and not above_e55:
            regime = "SIDEWAYS"; detail = f"IHSG {price:,.0f} > EMA20 tapi < EMA55({ema55:,.0f})"
        elif not above_e20_any and recovering:
            regime = "SIDEWAYS"; detail = f"IHSG {price:,.0f} recovery {chg:+.2f}%"
        elif bearish_confirm:
            regime = "RED"; detail = f"IHSG {price:,.0f} < EMA20 {pct_vs_e20:+.1f}% + turun {chg:.2f}%"
        else:
            regime = "SIDEWAYS"; detail = f"IHSG {price:,.0f} konsolidasi (EMA20={ema20:,.0f})"
        return (regime, price, ema20, ema55, detail, chg)
    except Exception as e:
        return ("UNKNOWN", 0, 0, 0, f"IHSG error: {str(e)[:40]}", 0.0)

def get_regime_config(regime):
    return {
        "RED": {
            "mode": "Reversal 🎯", "min_score": 5, "min_rvol": 2.0, "sl_mult": 0.6,
            "label": "🔴 MARKET MERAH — Reversal Only, Score ≥ 5",
            "color": "#ff3d5a", "desc": "Market bearish. Fokus reversal oversold, filter ketat."
        },
        "GREEN": {
            "mode": "Bagger 💎", "min_score": 4, "min_rvol": 1.5, "sl_mult": 0.8,
            "label": "🟢 MARKET HIJAU — Wyckoff Bagger Hunt (Daily TF)",
            "color": "#00ff88", "desc": "Market bullish. Cari akumulasi Wyckoff di chart harian."
        },
        "SIDEWAYS": {
            "mode": "Scalping ⚡", "min_score": 4, "min_rvol": 2.0, "sl_mult": 0.7,
            "label": "🟡 MARKET SIDEWAYS — Scalping, RVOL ≥ 2x",
            "color": "#ffb700", "desc": "Market sideways. RVOL harus lebih kuat."
        },
        "UNKNOWN": {
            "mode": "Scalping ⚡", "min_score": 4, "min_rvol": 1.5, "sl_mult": 0.8,
            "label": "⚪ REGIME UNKNOWN — Manual Mode",
            "color": "#4a5568", "desc": "Tidak bisa deteksi kondisi market."
        },
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

def apply_indicators(df):
    if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.droplevel(-1)
    df['EMA9']  = ema(df['Close'],9);  df['EMA21'] = ema(df['Close'],21)
    df['EMA50'] = ema(df['Close'],50); df['EMA200']= ema(df['Close'],200)
    df['RSI'], df['RSI_EMA'] = rsi_smooth(df['Close'],14,3)
    df['STOCH_K'], df['STOCH_D'] = stochastic(df['High'],df['Low'],df['Close'],14,3)
    df['MACD'], df['MACD_Sig'], df['MACD_Hist'] = macd(df['Close'])
    try:    df['VWAP'] = vwap(df)
    except: df['VWAP'] = df['Close']
    df['BB_mid']   = df['Close'].rolling(20).mean()
    df['BB_std']   = df['Close'].rolling(20).std()
    df['BB_upper'] = df['BB_mid']+2*df['BB_std']
    df['BB_lower'] = df['BB_mid']-2*df['BB_std']
    df['BB_pct']   = (df['Close']-df['BB_lower'])/(df['BB_upper']-df['BB_lower'])
    df['AvgVol']   = df['Volume'].rolling(20).mean()
    df['RVOL']     = df['Volume']/df['AvgVol'].replace(0, np.nan)
    df['NetVol']   = np.where(df['Close']>=df['Open'],df['Volume'],-df['Volume'])
    df['NetVol3']  = pd.Series(df['NetVol'],index=df.index).rolling(3).sum()
    df['NetVol8']  = pd.Series(df['NetVol'],index=df.index).rolling(8).sum()
    df['VolSpike'] = df['RVOL']>2.5
    df['Body']     = (df['Close']-df['Open']).abs()
    df['BodyRatio']= df['Body']/(df['High']-df['Low']).replace(0,np.nan)
    df['BullBar']  = (df['Close']>df['Open'])&(df['BodyRatio']>0.5)
    df['ROC3']     = df['Close'].pct_change(3)
    df['ROC8']     = df['Close'].pct_change(8)
    df['HH']= df['High']>df['High'].shift(1);  df['HL']= df['Low']>df['Low'].shift(1)
    df['LL']= df['Low']<df['Low'].shift(1);    df['LH']= df['High']<df['High'].shift(1)
    tr = pd.concat([df['High']-df['Low'],
                    (df['High']-df['Close'].shift()).abs(),
                    (df['Low'] -df['Close'].shift()).abs()],axis=1).max(axis=1)
    df['ATR'] = tr.rolling(14).mean()
    # DS asing flow — hanya ada kalau data dari DataSectors
    if 'FBuy' in df.columns and 'FSell' in df.columns:
        df['FNet']=df['FBuy']-df['FSell']; df['FCum']=df['FNet'].cumsum()
        df['FNet3']=df['FNet'].rolling(3).sum(); df['FNet8']=df['FNet'].rolling(8).sum()
        tot=df['FBuy']+df['FSell']
        df['FRatio']=(df['FBuy']/tot.replace(0,np.nan)).fillna(0.5)
    return df

apply_intraday_indicators = apply_indicators  # legacy alias

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
    rev=0; pk=float(p['STOCH_K']); pd_=float(p['STOCH_D'])
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

def score_bagger(r, p, p2, df_full):
    score=0; reasons=[]; close=float(r['Close'])
    e9=float(r['EMA9']); e21=float(r['EMA21']); e50=float(r['EMA50']); e200=float(r['EMA200'])
    rvol=float(r['RVOL']); rsi_e=float(r['RSI_EMA']); wyckoff_phase="SCANNING"
    is_sideways=False; range_high=close*1.05; range_low=close*0.95
    sideways_bars=min(20,len(df_full)-2)
    try:
        r_highs=df_full['High'].iloc[-sideways_bars-1:-1]; r_lows=df_full['Low'].iloc[-sideways_bars-1:-1]
        range_high=float(r_highs.max()); range_low=float(r_lows.min())
        range_pct=(range_high-range_low)/max(range_low,0.01)*100; is_sideways=range_pct<10.0
        if is_sideways:
            tightness_bonus=max(0,(10.0-range_pct)/10.0); score+=1.0+tightness_bonus*0.5
            reasons.append(f"Sideways {range_pct:.1f}% ({sideways_bars} hari) ✦"); wyckoff_phase="A-B"
    except: pass
    try:
        vol_ma20=float(df_full['AvgVol'].iloc[-1]); vol_last5=float(df_full['Volume'].iloc[-6:-1].mean())
        dry_ratio=vol_last5/max(vol_ma20,1)
        if dry_ratio<0.5 and is_sideways:   score+=2.0; reasons.append(f"Dry vol {dry_ratio:.2f}x — stealth accum ✦✦"); wyckoff_phase="A-B AKUMULASI"
        elif dry_ratio<0.7 and is_sideways: score+=1.2; reasons.append(f"Vol drying {dry_ratio:.2f}x ✦"); wyckoff_phase="A-B AKUMULASI"
        elif dry_ratio<0.85 and is_sideways:score+=0.6; reasons.append(f"Vol below avg {dry_ratio:.2f}x")
    except: pass
    try:
        if len(df_full)>=12:
            netvols_10=[float(df_full['NetVol'].iloc[i]) for i in range(-11,-1)]
            net_positive=sum(1 for v in netvols_10 if v>0); net_ratio=net_positive/10
            if net_ratio>=0.7 and is_sideways:  score+=1.5; reasons.append(f"Stealth net buy {net_positive}/10 hari ✦✦")
            elif net_ratio>=0.6:                score+=0.8; reasons.append(f"Net buy {net_positive}/10 hari")
            elif net_ratio>=0.5:                score+=0.4
    except:
        nv3=float(r['NetVol3']); nv8=float(r['NetVol8'])
        if nv3>0 and nv8>0: score+=0.8; reasons.append("Net buyer sustained ✦")
        elif nv3>0:          score+=0.3
    try:
        bb_curr=float(r['BB_std']); bb_avg10=float(df_full['BB_std'].iloc[-11:-1].mean())
        sq_ratio=bb_curr/max(bb_avg10,0.0001)
        if sq_ratio<0.7 and is_sideways:  score+=1.5; reasons.append(f"BB squeeze {sq_ratio:.2f}x ✦✦")
        elif sq_ratio<0.85:               score+=0.8; reasons.append(f"BB squeeze {sq_ratio:.2f}x")
    except: pass
    spring_detected=False
    try:
        lookback_sp=min(15,len(df_full)-3); prior_lows=df_full['Low'].iloc[-lookback_sp-2:-2]
        support=float(prior_lows.min()); bar_low=float(r['Low']); bar_close=float(r['Close']); bar_high=float(r['High'])
        is_spring=bar_low<support and bar_close>support
        if is_spring:
            recovery_strength=(bar_close-bar_low)/max(bar_high-bar_low,0.0001)
            if recovery_strength>0.7 and rvol>1.2:
                score+=3.0; reasons.append(f"🔥 SPRING! {recovery_strength:.0%} rebound ✦✦✦"); wyckoff_phase="SPRING ⚡"; spring_detected=True
            elif recovery_strength>0.5:
                score+=1.8; reasons.append(f"Spring ({recovery_strength:.0%}) ✦✦"); wyckoff_phase="SPRING"; spring_detected=True
        is_post_spring=(float(p['Low'])<support and float(p['Close'])>support and bar_close>float(p['Close']))
        if is_post_spring and not spring_detected:
            score+=2.0; reasons.append("Post-spring confirmation 🚀 ✦✦"); wyckoff_phase="POST-SPRING"; spring_detected=True
    except: pass
    try:
        above_resistance=close>range_high*0.998; thick_body=float(r['BodyRatio'])>0.55; bull_bar_flag=float(r['Close'])>float(r['Open'])
        if rvol>3.0 and above_resistance and thick_body and bull_bar_flag:
            score+=3.0; reasons.append(f"🚀 PHASE D! RVOL={rvol:.1f}x daily breakout ✦✦✦"); wyckoff_phase="PHASE D 🚀"
        elif rvol>2.0 and above_resistance and bull_bar_flag:
            score+=2.2; reasons.append(f"Breakout daily RVOL={rvol:.1f}x ✦✦"); wyckoff_phase="BREAKOUT ✦"
        elif rvol>1.5 and above_resistance: score+=1.5; reasons.append(f"Breakout attempt RVOL={rvol:.1f}x")
        elif above_resistance:              score+=0.8; reasons.append("Above resistance")
        else:
            if rvol>3.0:   score+=1.2; reasons.append(f"RVOL={rvol:.1f}x SURGE 🔥")
            elif rvol>2.0: score+=0.8; reasons.append(f"RVOL={rvol:.1f}x")
            elif rvol>1.5: score+=0.4
            elif rvol<1.0 and wyckoff_phase not in ["A-B AKUMULASI","SPRING","POST-SPRING"]: score-=0.5
    except:
        if rvol>3.0:   score+=1.2; reasons.append(f"RVOL={rvol:.1f}x SURGE 🔥")
        elif rvol>2.0: score+=0.8; reasons.append(f"RVOL={rvol:.1f}x")
        elif rvol>1.5: score+=0.4
    if e9>e21>e50>e200:  score+=1.5; reasons.append("EMA golden stack ✦✦")
    elif e9>e21>e50:     score+=1.0; reasons.append("EMA stack ▲")
    elif e9>e21:         score+=0.4
    elif is_sideways and wyckoff_phase in ["A-B AKUMULASI","SPRING","POST-SPRING"]: score+=0.2
    if wyckoff_phase in ["A-B","A-B AKUMULASI","SPRING","POST-SPRING"]:
        if 25<=rsi_e<=52:  score+=1.0; reasons.append(f"RSI-EMA={rsi_e:.1f} accum zone ✓")
        elif rsi_e<25:     score+=0.6; reasons.append(f"RSI-EMA={rsi_e:.1f} extreme OS")
        elif rsi_e>65:     score-=0.3
    else:
        if 52<rsi_e<72:   score+=1.0; reasons.append(f"RSI-EMA={rsi_e:.1f} momentum")
        elif rsi_e>=72:   score-=0.5; reasons.append(f"⚠️ RSI OB {rsi_e:.1f}")
        elif rsi_e<40:    score-=0.3
    if close>float(r['VWAP']): score+=0.5; reasons.append("Above VWAP")
    if e200>0 and close<e200*0.88: score-=1.0
    try:
        if len(df_full)>=4:
            bc=sum(1 for i in range(-3,0) if float(df_full['Close'].iloc[i])>float(df_full['Open'].iloc[i]))
            if bc==3:   score+=0.8; reasons.append("3x consecutive bull bar")
            elif bc==2: score+=0.3
    except: pass
    if wyckoff_phase != "SCANNING": reasons.insert(0, f"⚙️ Wyckoff: {wyckoff_phase}")
    return max(0, min(6, round(score, 1))), reasons, {"wyckoff_phase": wyckoff_phase}

def score_bsjp(r, p, p2):
    score=0; reasons=[]
    hi_lo=float(r["High"])-float(r["Low"]); close_pct=(float(r["Close"])-float(r["Low"]))/max(hi_lo,1)
    if close_pct>0.7:   score+=2;   reasons.append(f"Tutup dekat High ({close_pct:.0%})")
    elif close_pct>0.5: score+=1;   reasons.append(f"Tutup kuat ({close_pct:.0%})")
    rvol=float(r["RVOL"])
    if rvol>3.0:   score+=2;   reasons.append(f"RVOL={rvol:.1f}x SURGE 🔥")
    elif rvol>2.0: score+=1.5; reasons.append(f"RVOL={rvol:.1f}x kuat")
    elif rvol>1.5: score+=0.8; reasons.append(f"RVOL={rvol:.1f}x")
    if r["EMA9"]>r["EMA21"]>r["EMA50"]:  score+=1.5; reasons.append("EMA stack ▲")
    elif r["EMA9"]>r["EMA21"]:            score+=0.8; reasons.append("EMA9>21")
    rsi_e=float(r["RSI_EMA"])
    if 45<rsi_e<70:  score+=1;   reasons.append(f"RSI-EMA={rsi_e:.1f} ✓")
    elif rsi_e>=70:  score-=1;   reasons.append(f"⚠️ RSI OB {rsi_e:.1f}")
    elif rsi_e<40:   score+=0.5; reasons.append(f"RSI-EMA={rsi_e:.1f} oversold")
    if float(r["MACD_Hist"])>0 and float(r["MACD_Hist"])>float(p["MACD_Hist"]):
        score+=1; reasons.append("MACD hist expanding ✦")
    elif float(r["MACD_Hist"])>0: score+=0.5; reasons.append("MACD +")
    if float(r["Close"])>float(r["VWAP"]): score+=0.5; reasons.append("Above VWAP")
    if float(r["NetVol8"])>0: score+=0.5; reasons.append("Net buyer 8 bar ✦")
    elif float(r["NetVol3"])>0: score+=0.3
    return max(0,min(6,round(score,1))), reasons, {}

def get_sinyal_v2(r, p, p2):
    def sf(v,d=0.):
        try: x=float(v); return d if(np.isnan(x) or np.isinf(x)) else x
        except: return d
    cl=sf(r.get('Close',0)); e9=sf(r.get('EMA9')); e21=sf(r.get('EMA21')); e50=sf(r.get('EMA50'))
    rsi_ema=sf(r.get('RSI_EMA',50)); rsi_ema_p=sf(p.get('RSI_EMA',50))
    sk=sf(r.get('STOCH_K',50)); sd=sf(r.get('STOCH_D',50))
    sk_p=sf(p.get('STOCH_K',50)); sd_p=sf(p.get('STOCH_D',50))
    mh=sf(r.get('MACD_Hist',0)); mh_p=sf(p.get('MACD_Hist',0))
    macd_v=sf(r.get('MACD',0)); sig_v=sf(r.get('MACD_Sig',0))
    macd_p=sf(p.get('MACD',0)); sig_p=sf(p.get('MACD_Sig',0))
    rv=sf(r.get('RVOL',1)); lw=sf(r.get('LWick',0)); uw=sf(r.get('UWick',0))
    body=sf(r.get('Body',50)); vwap_v=sf(r.get('VWAP',cl))
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


def get_aksi_v2(sinyal, gc_now, score):
    if "BANDAR" in sinyal or (("HAKA" in sinyal or "SUPER" in sinyal) and score>=35):
        return "AT ENTRY 🎯"
    elif "REBOUND" in sinyal: return "WATCH REB 🏀"
    elif gc_now:              return "GC NOW ⚡"
    elif score>=25:           return "AT ENTRY 🎯"
    elif score>=15:           return "WAIT GC ⏳"
    else:                     return "WAIT ❌"


def get_signal(score, mode):
    t = {
        "Scalping ⚡": {5:"GACOR ⚡",    4:"POTENSIAL 🔥", 3:"WATCH 👀"},
        "Momentum 🚀": {5:"GACOR 🚀",    4:"POTENSIAL 🔥", 3:"WATCH 👀"},
        "Reversal 🎯": {5:"REVERSAL 🎯", 4:"POTENSIAL 🔥", 3:"WATCH 👀"},
        "Bagger 💎":   {5:"BAGGER 💎",   4:"KANDIDAT 🚀",  3:"WATCH 👀"},
    }.get(mode, {})
    for thresh in sorted(t.keys(), reverse=True):
        if score >= thresh: return t[thresh]
    return "WAIT"

def get_card_class(signal):
    if "BAGGER" in signal or "KANDIDAT" in signal: return "bagger"
    if "GACOR"  in signal or "REVERSAL" in signal: return "gacor"
    if "POTENSIAL" in signal:                      return "potensial"
    if "WATCH"  in signal:                         return "watch"
    return ""

# ════════════════════════════════════════════════════
#  TELEGRAM
# ════════════════════════════════════════════════════
def send_telegram(results_top, source="Scanner"):
    if not TOKEN or not CHAT_ID: return
    now=datetime.now(jakarta_tz); is_open=9<=now.hour<16
    sep="━"*28
    hdr=(f"{'🔴 MARKET OPEN' if is_open else '🌙 AFTER HOURS'}\n"
         f"🔥 *THETA TURBO {'WATCHLIST' if source=='Watchlist' else 'ALERT'}*\n"
         f"⏰ `{now.strftime('%H:%M:%S')} WIB` · `{now.strftime('%d %b %Y')}`\n{sep}\n")
    body=""
    for r in results_top[:5]:
        sig=r.get('Signal','-')
        em="💎" if "BAGGER" in sig else("🏆" if("GACOR" in sig or "REVERSAL" in sig) else("🔥" if "POTENSIAL" in sig else "👀"))
        te="📈" if "▲" in r.get('Trend','') else("📉" if "▼" in r.get('Trend','') else "➡️")
        bar="█"*int(r['Score'])+"░"*(6-int(r['Score']))
        tf_label=" [D1]" if r.get("TF","")=="Daily" else " [15M]"
        body+=(f"\n{em} *{r['Ticker']}*  `{sig}`{tf_label}\n"
               f"   💰 Price: `{r['Price']:,}` {te}\n"
               f"   📊 Score: `[{bar}] {r['Score']}/6`\n"
               f"   📈 RSI-EMA: `{r.get('RSI-EMA',0)}` | RVOL: `{r.get('RVOL',0)}x`\n"
               f"   🎯 TP: `{r['TP']:,}` | 🛑 SL: `{r['SL']:,}` | R:R `{r['R:R']}`\n"
               f"   💡 _{r.get('Reasons','')[:60]}_\n")
    footer=f"\n{sep}\n⚡ _Theta Turbo v5.2 · Wyckoff Bagger Daily · IDX_\n⚠️ _BUKAN saran investasi. DYOR!_"
    try:
        requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                      data={"chat_id":CHAT_ID,"text":hdr+body+footer,"parse_mode":"Markdown"},timeout=10)
    except: pass

# ════════════════════════════════════════════════════
#  PIVOT POINTS
# ════════════════════════════════════════════════════
def calc_pivot_points(high, low, close):
    pp=(high+low+close)/3; r1=2*pp-low; r2=pp+(high-low); r3=high+2*(pp-low)
    s1=2*pp-high; s2=pp-(high-low); s3=low-2*(high-pp)
    return {"PP":pp,"R1":r1,"R2":r2,"R3":r3,"S1":s1,"S2":s2,"S3":s3}

@st.cache_data(ttl=3600)
def fetch_pivot_data(ticker_yf):
    try:
        raw=yf.download(ticker_yf,period="5d",interval="1d",progress=False,auto_adjust=True,threads=False,session=_YF_SESSION)
        if raw is None or raw.empty or len(raw)<2: return None
        df=_yf_extract(raw,ticker_yf,1)
        if df is None or len(df)<2: return None
        prev=df.iloc[-2]
        return calc_pivot_points(float(prev["High"]),float(prev["Low"]),float(prev["Close"]))
    except: return None

def get_pivot_position(price, pivots):
    if pivots is None: return "Unknown","#4a5568"
    pp=pivots["PP"]
    if price>pivots["R2"]:   return "Above R2 🔴","#ff3d5a"
    elif price>pivots["R1"]: return "R1→R2 🟠","#ff7b00"
    elif price>pp:           return "PP→R1 🟢","#00ff88"
    elif price>pivots["S1"]: return "S1→PP 🟡","#ffb700"
    elif price>pivots["S2"]: return "S2→S1 🔴","#ff3d5a"
    else:                    return "Below S2 🔴","#ff3d5a"

# ════════════════════════════════════════════════════
#  MULTI-TIMEFRAME — resample dari 15m, zero extra request
# ════════════════════════════════════════════════════
@st.cache_data(ttl=360)
def fetch_mtf_data(ticker_yf):
    result={}
    try:
        raw=yf.download(ticker_yf,period="7d",interval="15m",progress=False,auto_adjust=True,threads=False,session=_YF_SESSION)
        df15=_yf_extract(raw,ticker_yf,1)
        if df15 is None or len(df15)<10: return result
        if len(df15)>=20: result["M15"]=df15
        for rs_rule,rs_key,min_b in [("1h","H1",10),("1D","D1",3)]:
            try:
                df_rs=df15.resample(rs_rule).agg({"Open":"first","High":"max","Low":"min","Close":"last","Volume":"sum"}).dropna(subset=["Close"])
                df_rs=df_rs[df_rs["Volume"]>0]
                if len(df_rs)>=min_b: result[rs_key]=df_rs
            except: pass
    except: pass
    return result

def score_mtf(ticker_yf, mode="Scalping ⚡"):
    mtf=fetch_mtf_data(ticker_yf); scores={}
    for tf_key,df in mtf.items():
        try:
            df=apply_indicators(df.copy())
            if len(df)<3: continue
            r=df.iloc[-1]; p=df.iloc[-2]; p2=df.iloc[-3]
            if mode=="Scalping ⚡":   sc,_,_=score_scalping(r,p,p2)
            elif mode=="Momentum 🚀": sc,_,_=score_momentum(r,p,p2)
            elif mode=="Bagger 💎":   sc,_,_=score_bagger(r,p,p2,df)
            else:                     sc,_,_=score_reversal(r,p,p2)
            scores[tf_key]=round(sc,1)
        except: scores[tf_key]=0
    return scores

def mtf_alignment(scores):
    if not scores: return "No Data","#4a5568",0
    vals=list(scores.values()); avg=sum(vals)/len(vals)
    bc=sum(1 for v in vals if v>=4)
    if bc==len(vals):  return "FULL ALIGN 🔥","#00ff88",avg
    elif bc>=2:        return "PARTIAL ⚡","#ffb700",avg
    elif bc==1:        return "MIXED ⚠️","#ff7b00",avg
    else:              return "NO ALIGN ❌","#ff3d5a",avg

# ════════════════════════════════════════════════════
#  SEKTOR, GAP UP, TRAILING STOP
# ════════════════════════════════════════════════════
SECTORS={
    "Energi & Mining":    ["ADRO","BYAN","ITMG","PTBA","HRUM","DOID","GEMS","PGAS","ELSA","MEDC","ESSA","AKRA","RIGS","DSSA","MBAP","KKGI","MYOH","SMMT","BSSR","INDY"],
    "Perbankan":          ["BBCA","BBRI","BMRI","BBNI","BBTN","BJBR","BJTM","BNGA","BDMN","NISP","MEGA","BBYB","ARTO","BRIS","AGRO","BBHI","NOBU","PNBN","BACA","MAYA"],
    "Properti":           ["BSDE","CTRA","SMRA","LPKR","PWON","APLN","ASRI","DILD","DUTI","MDLN","MKPI","JRPT","KIJA","BEST","GPRA","NUSA","DART","CITY","BKSL","MTLA"],
    "Infrastruktur":      ["JSMR","TLKM","EXCL","ISAT","TBIG","TOWR","WIKA","ADHI","PTPP","WSKT","WTON","WEGE","ACST","DGIK","TRUK","BIRD","GIAA","TMAS","SMDR","BBRM"],
    "Konsumer":           ["UNVR","ICBP","INDF","MYOR","KLBF","SIDO","GGRM","HMSP","ULTJ","DLTA","ROTI","SKBM","GOOD","HOKI","CLEO","MIKA","HEAL","SILO","KAEF","DVLA"],
    "Industri & Otomotif":["ASII","AUTO","SMSM","HEXA","UNTR","SCCO","KBLI","VOKS","BRAM","GJTL","IMAS","INTP","SMGR","AMFG","LION","CPIN","JPFA","MAIN","BRPT","TPIA"],
    "Teknologi":          ["GOTO","BUKA","EMTK","MNCN","SCMA","MTEL","MTDL","MLPT","CHIP","LUCK","DCII","WIFI","DIGI","AWAN","AXIO","INET","MCAS","WIRG","TECH","VKTR"],
    "Shipping & Logistik":["TMAS","SMDR","BBRM","NELY","AKSI","SHIP","ELPI","BIRD","GIAA","TAXI","ASSA","WEHA","SAFE","MIRA","HEXA","RAJA","RIGS","MBSS","IATA","BULL"],
    "Petrokimia & Kimia": ["TPIA","BRPT","BUDI","EKAD","INCI","DPNS","ETWA","MDKI","ESSA","AKPI","ADMG","CPRO","SRSN","MOLI","PURA","CEKA","KBLM","JPFA","CPIN","UNIC"],
}
HORMUZ_SECTORS=["Energi & Mining","Shipping & Logistik","Petrokimia & Kimia"]

@st.cache_data(ttl=600)
def fetch_sector_rotation(sector_stocks):
    """Humanized fetch per small batch — anti rate limit."""
    results = []
    tickers_yf = [s + ".JK" for s in sector_stocks[:10]]
    batches = _random_chunks(tickers_yf, min_sz=3, max_sz=6)
    n_b = len(batches)
    for bi, batch in enumerate(batches):
        if not batch: continue
        try:
            raw = yf.download(list(batch), period="5d", interval="1d",
                              group_by="ticker", progress=False,
                              threads=False, auto_adjust=True, session=_YF_SESSION)
            for t in batch:
                tkr = t.replace(".JK", "")
                try:
                    df = _yf_extract(raw, t, len(batch))
                    if df is None or len(df) < 2: continue
                    close = float(df["Close"].iloc[-1]); prev = float(df["Close"].iloc[-2])
                    chg = (close - prev) / prev * 100
                    vol = float(df["Volume"].iloc[-1])
                    avg_v = float(df["Volume"].mean())
                    rvol = vol / avg_v if avg_v > 0 else 1.0
                    results.append({"ticker": tkr, "close": close, "chg": chg, "rvol": round(rvol, 2)})
                except: pass
        except:
            for t in batch:
                tkr = t.replace(".JK", "")
                try:
                    df = _ticker_history(t, "5d", "1d")
                    if df is None:
                        raw_s = yf.download(t, period="5d", interval="1d",
                                            progress=False, auto_adjust=True,
                                            threads=False, session=_YF_SESSION)
                        df = _yf_extract(raw_s, t, 1)
                    if df is None or len(df) < 2: continue
                    close = float(df["Close"].iloc[-1]); prev = float(df["Close"].iloc[-2])
                    chg = (close - prev) / prev * 100
                    vol = float(df["Volume"].iloc[-1])
                    avg_v = float(df["Volume"].mean())
                    rvol = vol / avg_v if avg_v > 0 else 1.0
                    results.append({"ticker": tkr, "close": close, "chg": chg, "rvol": round(rvol, 2)})
                except: pass
                time.sleep(random.uniform(0.3, 0.8))
        _human_sleep(bi, n_b)
    return results


@st.cache_data(ttl=3600)
def fetch_all_sectors(sectors_dict, top_n=10, is_jk=True):
    """Fetch SEMUA sektor dalam 1 pass humanized. Split hasil per sektor."""
    suffix = ".JK" if is_jk else ""
    ticker_to_sectors = {}
    for sec_name, sec_stocks in sectors_dict.items():
        for s in sec_stocks[:top_n]:
            t = s + suffix
            if t not in ticker_to_sectors:
                ticker_to_sectors[t] = []
            ticker_to_sectors[t].append(sec_name)
    all_tickers = list(ticker_to_sectors.keys())
    results_raw = {}
    batches = _random_chunks(all_tickers, min_sz=5, max_sz=10)
    n_b = len(batches)
    for bi, batch in enumerate(batches):
        if not batch: continue
        try:
            raw = yf.download(list(batch), period="5d", interval="1d",
                              group_by="ticker", progress=False,
                              threads=False, auto_adjust=True, session=_YF_SESSION)
            for t in batch:
                tkr = t.replace(".JK","") if is_jk else t
                try:
                    df = _yf_extract(raw, t, len(batch))
                    if df is None or len(df)<2: continue
                    close=float(df["Close"].iloc[-1]); prev=float(df["Close"].iloc[-2])
                    chg=(close-prev)/prev*100; vol=float(df["Volume"].iloc[-1])
                    avg_v=float(df["Volume"].mean()); rvol=vol/avg_v if avg_v>0 else 1.0
                    results_raw[t]={"ticker":tkr,"close":close,"chg":chg,"rvol":round(rvol,2)}
                except: pass
        except:
            for t in batch:
                if t in results_raw: continue
                tkr = t.replace(".JK","") if is_jk else t
                try:
                    if is_jk:
                        df=_ticker_history(t,"5d","1d")
                    else:
                        df=None
                    if df is None:
                        raw_s=yf.download(t,period="5d",interval="1d",progress=False,
                                          auto_adjust=True,threads=False,session=_YF_SESSION)
                        df=_yf_extract(raw_s,t,1)
                    if df is None or len(df)<2: continue
                    close=float(df["Close"].iloc[-1]); prev=float(df["Close"].iloc[-2])
                    chg=(close-prev)/prev*100; vol=float(df["Volume"].iloc[-1])
                    avg_v=float(df["Volume"].mean()); rvol=vol/avg_v if avg_v>0 else 1.0
                    results_raw[t]={"ticker":tkr,"close":close,"chg":chg,"rvol":round(rvol,2)}
                except: pass
                time.sleep(random.uniform(0.3,0.8))
        _human_sleep(bi,n_b)
    # Split per sektor
    sec_data={}
    for sec_name,sec_stocks in sectors_dict.items():
        sec_results=[results_raw[s+suffix] for s in sec_stocks[:top_n] if (s+suffix) in results_raw]
        if not sec_results: continue
        avg_chg=sum(r["chg"] for r in sec_results)/len(sec_results)
        avg_rvol=sum(r["rvol"] for r in sec_results)/len(sec_results)
        bullish=sum(1 for r in sec_results if r["chg"]>0)
        sec_data[sec_name]={"avg_chg":round(avg_chg,2),"avg_rvol":round(avg_rvol,2),
                            "bullish":bullish,"total":len(sec_results),"stocks":sec_results}
    return sec_data


@st.cache_data(ttl=3600)
def calc_sector_beta(sector_name, sector_stocks, lookback=20):
    try:
        # IHSG — pakai _ticker_history dulu, fallback yf.download
        ihsg = _ticker_history("^JKSE", "60d", "1d")
        if ihsg is None:
            raw_ihsg = yf.download("^JKSE", period="60d", interval="1d",
                                   progress=False, auto_adjust=True, session=_YF_SESSION)
            ihsg = _yf_extract(raw_ihsg, "^JKSE", 1)
        if ihsg is None or len(ihsg) < lookback: return None
        ihsg_ret = ihsg["Close"].pct_change().dropna()

        tickers_yf = [s + ".JK" for s in sector_stocks[:8]]
        sec_rets = []
        # Humanized — fetch 2-3 per batch
        batches = _random_chunks(tickers_yf, min_sz=2, max_sz=4)
        n_b = len(batches)
        for bi, batch in enumerate(batches):
            try:
                raw = yf.download(list(batch), period="60d", interval="1d",
                                  group_by="ticker", progress=False,
                                  threads=False, auto_adjust=True, session=_YF_SESSION)
                for t in batch:
                    try:
                        df = _yf_extract(raw, t, len(batch))
                        if df is not None and len(df) >= lookback:
                            sec_rets.append(df["Close"].pct_change().dropna())
                    except: pass
            except:
                for t in batch:
                    try:
                        df = _ticker_history(t, "60d", "1d")
                        if df is not None and len(df) >= lookback:
                            sec_rets.append(df["Close"].pct_change().dropna())
                    except: pass
                    time.sleep(random.uniform(0.3, 0.7))
            _human_sleep(bi, n_b)

        if not sec_rets: return None
        sec_avg = pd.concat(sec_rets, axis=1).mean(axis=1)
        aligned = pd.concat([ihsg_ret, sec_avg], axis=1).dropna()
        aligned.columns = ["IHSG", "Sektor"]
        if len(aligned) < 10: return None
        cov = aligned["Sektor"].cov(aligned["IHSG"])
        var = aligned["IHSG"].var()
        beta = round(cov / var, 2) if var > 0 else 1.0
        corr = round(aligned["Sektor"].corr(aligned["IHSG"]), 2)
        rs5  = round((aligned["Sektor"].tail(5).sum() - aligned["IHSG"].tail(5).sum()) * 100, 2)
        ret_1m_sec = round(aligned["Sektor"].tail(20).sum() * 100, 2)
        down_days  = aligned[aligned["IHSG"] < -0.005]
        avg_down   = round(down_days["Sektor"].mean() * 100, 2) if len(down_days) > 0 else 0.0
        return {"sector": sector_name, "beta": beta, "corr": corr, "rs5": rs5,
                "ret_1m_sec": ret_1m_sec, "avg_down": avg_down,
                "defensive": beta < 0.8 and corr < 0.7}
    except: return None
@st.cache_data(ttl=300)
def scan_gap_up(tickers_yf, min_gap_pct=0.5):
    results=[]
    batches=_random_chunks(list(tickers_yf), min_sz=8, max_sz=20)
    n_b=len(batches)
    for bi,batch in enumerate(batches):
        try:
            raw=yf.download(list(batch),period="5d",interval="1d",group_by="ticker",progress=False,threads=False,auto_adjust=True,session=_YF_SESSION)
            for t in batch:
                tkr=t.replace(".JK","")
                try:
                    df=_yf_extract(raw,t,len(batch))
                    if df is None or len(df)<3: continue
                    today=df.iloc[-1]; prev=df.iloc[-2]
                    close=float(today["Close"]); high_t=float(today["High"]); low_t=float(today["Low"])
                    high_p=float(prev["High"]); vol=float(today["Volume"])
                    avg_vol=float(df["Volume"].mean()); rvol=vol/avg_vol if avg_vol>0 else 1.0
                    gap_score=0; reasons=[]
                    if close>high_p:
                        gap_pct=(close-high_p)/high_p*100; gap_score+=3; reasons.append(f"Gap {gap_pct:.1f}% above prev High ✦✦")
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
                except: pass
        except: pass
        _human_sleep(bi, n_b)
    return sorted(results,key=lambda x:x["Gap Score"],reverse=True)

def calc_trailing_stop(entry,current,atr,method="ATR",atr_mult=2.0,pct=3.0):
    if method=="ATR":      trail_dist=atr*atr_mult; stop_price=current-trail_dist
    elif method=="Persen": trail_dist=current*(pct/100); stop_price=current*(1-pct/100)
    else:                  trail_dist=atr*1.5; stop_price=current-trail_dist
    profit_pct=(current-entry)/entry*100
    locked_pct=(stop_price-entry)/entry*100 if stop_price>entry else 0
    return {"stop":round(stop_price,0),"distance":round(trail_dist,0),
            "profit_float":round(profit_pct,2),"profit_locked":round(locked_pct,2),
            "is_profitable":stop_price>entry}

# ════════════════════════════════════════════════════
#  HEADER
# ════════════════════════════════════════════════════
regime,ihsg_price,ema20,ema55,regime_detail,ihsg_chg=get_market_regime()
rcfg=get_regime_config(regime); rcolor=rcfg["color"]
chg_col="#00ff88" if ihsg_chg>=0 else "#ff3d5a"; chg_sym="▲" if ihsg_chg>=0 else "▼"
now_jkt=datetime.now(jakarta_tz)

st.markdown(f"""
<div class="tt-header">
  <div>
    <div class="tt-logo">🔥 THETA TURBO</div>
    <div class="tt-sub">Intraday 15M + Daily Bagger · Wyckoff · Auto Regime · v5.2</div>
  </div>
  <div class="live-badge"><div class="live-dot"></div>{'⚡ DS+yF' if DS_KEY else '📊 yF only'} · LIVE {now_jkt.strftime("%H:%M:%S")} WIB</div>
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
tab_scanner,tab_watchlist,tab_bsjp,tab_sector,tab_gapup,tab_trail,tab_backtest=st.tabs(
    ["🔥 Scanner","👁️ Watchlist","🌙 BSJP","🏭 Sektor","📈 Gap Up","🎯 Trailing Stop","📊 Backtest"])

# ════════════════════════════════════════════════════
#  TAB 1: SCANNER
# ════════════════════════════════════════════════════
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
                _prev=st.session_state.get("active_scan_mode","Scalping ⚡")
                _opts=["Scalping ⚡","Momentum 🚀","Reversal 🎯","Bagger 💎"]
                _idx=_opts.index(_prev) if _prev in _opts else 0
                scan_mode=st.radio("Mode",_opts,index=_idx,label_visibility="collapsed",key="scan_mode_radio")
            st.session_state.active_scan_mode=scan_mode
            st.session_state.active_auto_regime=auto_regime
            tele_on=st.toggle("📡 Telegram Alert",value=True,key="tele_on")
        with sc2:
            st.markdown('<div class="settings-label">FILTER</div>',unsafe_allow_html=True)
            auto_thresh=st.toggle("🤖 Auto-Threshold",value=True,key="auto_thr")
            if auto_thresh:
                min_score=rcfg["min_score"]; vol_thresh=rcfg["min_rvol"]
                st.caption(f"Auto: Score≥{min_score} · RVOL≥{vol_thresh}x")
            else:
                min_score=st.slider("Min Score (0-6)",0,6,4,key="msc")
                vol_thresh=st.slider("Min RVOL",1.0,5.0,1.5,0.1,key="vol")
            min_turn=st.number_input("Min Turnover (M Rp)",value=500,step=100,key="trn")*1_000_000
        with sc3:
            st.markdown('<div class="settings-label">TAMPILAN</div>',unsafe_allow_html=True)
            view_mode=st.radio("View",["Card View 🃏","Table View 📊"],label_visibility="collapsed",key="view_mode")
            scan_size=st.radio("Scan Size",["LQ45 ⚡ (45)","200 🔥","Full 🦅"],index=1,horizontal=True,key="scan_size")
            is_bagger_mode=(scan_mode=="Bagger 💎")
            if is_bagger_mode:
                st.markdown('<div style="font-family:Space Mono,monospace;font-size:9px;color:#bf5fff;padding:4px 8px;background:rgba(191,95,255,.1);border-radius:4px;">📅 Bagger: Daily TF (60 hari)</div>',unsafe_allow_html=True)
            st.caption(f"🎯 {regime} · {scan_mode} · {len(raw_stocks)} emiten")

    # ── AUTO-SCAN ON FIRST RUN — IDX Life pattern ──────────────────────────
    _pool_key = st.session_state.get("scan_size","200 🔥")
    _first_run = not st.session_state.first_scan_done
    _pool_changed = st.session_state.last_scan_pool_key != _pool_key

    if _first_run or _pool_changed:
        st.session_state.first_scan_done   = True
        st.session_state.last_scan_pool_key = _pool_key
        if not st.session_state.scan_results:  # hanya auto kalau belum ada hasil
            # Trigger scan otomatis
            st.session_state._auto_first_run = True

    do_scan=st.button("🔥 MULAI SCAN SEKARANG",type="primary",use_container_width=True,key="btn_scan")
    if st.session_state.get("_auto_first_run") and not do_scan:
        do_scan = True
        st.session_state._auto_first_run = False
    _now_check=datetime.now(jakarta_tz).timestamp(); auto_triggered=False
    if st.session_state.last_scan_time and not do_scan:
        if _now_check-st.session_state.last_scan_time>=300 and st.session_state.scan_results:
            do_scan=True; auto_triggered=True
            scan_mode=st.session_state.get("active_scan_mode",scan_mode)

    if do_scan:
        # LQ45 list
        _LQ45_YF = [s+".JK" for s in ["BBCA","BBRI","BMRI","TLKM","ASII","GOTO","BYAN","MDKA",
            "UNVR","ICBP","INDF","KLBF","SIDO","MNCN","EXCL","TOWR","PGAS","PTBA","ADRO","ITMG",
            "INCO","HRUM","JSMR","SMGR","WIKA","WSKT","ANTM","PTPP","TBIG","BRIS","AMMN","MBMA",
            "CUAN","BBNI","BBTN","BMTR","INKP","BRPT","TPIA","BREN","EMTK","PANI","DSSA","ISAT","PGAS"]]
        _sz = st.session_state.get("scan_size","200 🔥")
        if "45" in _sz:   scan_list=_LQ45_YF
        elif "200" in _sz: scan_list=stocks_yf[:200]
        else:              scan_list=stocks_yf
        is_bagger=(scan_mode=="Bagger 💎")
        prog_ph=st.empty(); pb=st.progress(0)
        tf_label="📅 DAILY (60 hari)" if is_bagger else "⚡ 15M INTRADAY"
        label="🔄 AUTO-REFRESH" if auto_triggered else "🔥 SCANNING"
        prog_ph.markdown(f'<div style="color:#ff7b00;font-family:Space Mono,monospace;font-size:12px;">{label} {len(scan_list)} saham · {scan_mode} · {tf_label}</div>',unsafe_allow_html=True)
        try:
            if is_bagger:
                data_dict=fetch_daily_bagger(tuple(scan_list))
            elif DS_KEY:
                # ── HYBRID: DS primary + yFinance humanized fallback ──
                data_dict={}
                _scan_tf_ds="15m"
                # DS parallel fetch (ThreadPoolExecutor — DS bukan Yahoo, no rate limit)
                from concurrent.futures import ThreadPoolExecutor, as_completed
                def _ds_f(t):
                    raw_t=t.replace(".JK","").upper()
                    df=fetch_ds_ohlcv(raw_t,_scan_tf_ds,200,True)
                    return t,df
                with ThreadPoolExecutor(max_workers=10) as ex:
                    futs={ex.submit(_ds_f,t):t for t in scan_list}
                    for fut in as_completed(futs):
                        try:
                            t,df=fut.result(timeout=15)
                            if df is not None and len(df)>=20: data_dict[t]=df
                        except: pass
                # yFinance humanized fallback untuk yang missing dari DS
                _missing=[t for t in scan_list if t not in data_dict]
                if _missing:
                    _fb=fetch_intraday(tuple(_missing))
                    data_dict.update(_fb)
            else:
                data_dict=fetch_intraday(tuple(scan_list))
            st.session_state.data_dict=data_dict; n_fetched=len(data_dict)
            if n_fetched==0:
                prog_ph.empty(); pb.empty()
                try: log_ph.empty()
                except: pass
                st.warning("⚠️ Tidak ada data. Coba lagi atau kurangi jumlah saham.")
            else:
                prog_ph.markdown(f'<div style="color:#00ff88;font-family:Space Mono,monospace;font-size:11px;">✅ {n_fetched} saham berhasil · Scoring {scan_mode}...</div>',unsafe_allow_html=True)
                results=[]; tickers=list(data_dict.keys())
                min_bars=20 if is_bagger else 30
                log_lines=[]; log_ph=st.empty()
                for i,ticker_yf in enumerate(tickers):
                    pb.progress((i+1)/max(len(tickers),1))
                    ts_log=datetime.now(jakarta_tz).strftime("%H:%M:%S")
                    tkr_log=stock_map.get(ticker_yf,ticker_yf.replace(".JK",""))
                    try:
                        df=data_dict[ticker_yf].copy()
                        if len(df)<min_bars: continue
                        df=apply_indicators(df)
                        r=df.iloc[-1]; p=df.iloc[-2]; p2=df.iloc[-3] if len(df)>=3 else p
                        close=float(r['Close']); vol=float(r['Volume'])
                        turnover=close*vol; rvol=float(r['RVOL'])
                        if turnover<min_turn or rvol<vol_thresh: continue
                        if scan_mode=="Scalping ⚡":   sc,reasons,_=score_scalping(r,p,p2)
                        elif scan_mode=="Momentum 🚀": sc,reasons,_=score_momentum(r,p,p2)
                        elif scan_mode=="Bagger 💎":   sc,reasons,_=score_bagger(r,p,p2,df)
                        else:                          sc,reasons,_=score_reversal(r,p,p2)
                        if sc<min_score:
                            log_lines.append(f'<span style="color:#304050">[{ts_log}] ❄️ {tkr_log} score:{sc:.1f} skip</span>')
                            if len(log_lines)%5==0: log_ph.markdown(f'<div style="background:#05080d;border:1px solid #1a2a3a;border-radius:4px;padding:8px 12px;font-size:11px;color:#607080;max-height:180px;overflow-y:auto;font-family:Space Mono,monospace;line-height:1.8">{"<br>".join(log_lines[-10:])}</div>',unsafe_allow_html=True)
                            continue
                        sig=get_signal(sc,scan_mode)
                        if sig=="WAIT": continue
                        atr=float(r['ATR']) if not np.isnan(float(r['ATR'])) else close*0.01
                        slm=rcfg.get("sl_mult",0.8)
                        if scan_mode=="Scalping ⚡":   tp=close+1.5*atr; sl=close-slm*atr
                        elif scan_mode=="Momentum 🚀": tp=close+2.0*atr; sl=close-slm*atr
                        elif scan_mode=="Bagger 💎":   tp=close+3.0*atr; sl=close-1.0*atr
                        else:                          tp=close+2.5*atr; sl=close-slm*atr
                        rr=(tp-close)/max(close-sl,0.01)
                        e9=float(r['EMA9']); e21=float(r['EMA21']); e50=float(r['EMA50'])
                        trend="▲ UP" if e9>e21>e50 else("▼ DOWN" if e9<e21<e50 else"◆ SIDE")
                        # DS asing flow data (hanya ada kalau dari DS API)
                        def _sf(v,d=0.):
                            try: x=float(v); return d if(np.isnan(x) or np.isinf(x)) else x
                            except: return d
                        fnet3=_sf(r.get('FNet3',0)); fnet8=_sf(r.get('FNet8',0))
                        fratio=_sf(r.get('FRatio',0.5)); fbuy=_sf(r.get('FBuy',0)); fsell=_sf(r.get('FSell',0))
                        has_asing=(fbuy+fsell)>0
                        if not has_asing:          fdir="—";       fc_="#4a5568"
                        elif fnet3>0 and fnet8>0:  fdir="🔵 BELI"; fc_="#4da6ff"
                        elif fnet3<0 and fnet8<0:  fdir="🔴 JUAL"; fc_="#ff3d5a"
                        else:                       fdir="⚪ MIX";  fc_="#888888"
                        # DS sinyal_v2 (hanya kalau ada FBuy/FSell data)
                        if has_asing:
                            _sig_v2,_sc_v2,_flags_v2,_gc=get_sinyal_v2(r,p,p2)
                            _aksi_v2=get_aksi_v2(_sig_v2,_gc,_sc_v2)
                        else:
                            _sig_v2=sig; _aksi_v2="-"; _flags_v2=""
                        gain_pct=float(r['ROC3'])*100
                        _log_icon="🔥" if ("GACOR" in sig or "BAGGER" in sig or "REVERSAL" in sig) else "⚡" if "POTENSIAL" in sig else "👀"
                        _log_cls="color:#ff9900;font-weight:700" if "GACOR" in sig or "BAGGER" in sig else "color:#ffb700" if "POTENSIAL" in sig else "color:#00ff88"
                        log_lines.append(f'<span style="{_log_cls}">[{ts_log}] {_log_icon} {tkr_log} | score:{sc:.1f} | {sig} | RVOL:{rvol:.1f}x</span>')
                        log_ph.markdown(f'<div style="background:#05080d;border:1px solid #1a2a3a;border-radius:4px;padding:8px 12px;font-size:11px;color:#607080;max-height:180px;overflow-y:auto;font-family:Space Mono,monospace;line-height:1.8">{"<br>".join(log_lines[-12:])}</div>',unsafe_allow_html=True)
                        results.append({
                            "Ticker":stock_map.get(ticker_yf,ticker_yf.replace(".JK","")),
                            "Price":int(close),"Score":sc,"Signal":sig,"Trend":trend,
                            "TF":"Daily" if is_bagger else "15m",
                            "RSI-EMA":round(float(r['RSI_EMA']),1),"Stoch K":round(float(r['STOCH_K']),1),
                            "Stoch D":round(float(r['STOCH_D']),1),"MACD Hist":round(float(r['MACD_Hist']),4),
                            "RVOL":round(rvol,2),"BB%":round(float(r['BB_pct']),2),
                            "ROC 3B%":round(gain_pct,2),"VWAP":int(float(r['VWAP'])),
                            "TP":int(tp),"SL":int(sl),"R:R":round(rr,1),
                            "Turnover(M)":round(turnover/1e6,1),"Reasons":" · ".join(reasons),
                            "_class":get_card_class(sig),
                            # DS asing flow
                            "FDir":fdir,"FC":fc_,
                            "FNet3":int(fnet3),"FNet8":int(fnet8),"FRatio":round(fratio,2),
                            "Sinyal_v2":_sig_v2,"Aksi_v2":_aksi_v2,
                            "Source":"DS" if has_asing else "yF",
                        })
                    except: continue
                prog_ph.empty(); pb.empty()
                st.session_state.scan_results=results
                st.session_state.last_scan_time=datetime.now(jakarta_tz).timestamp()
                st.session_state.last_scan_mode=scan_mode
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
            st.error(f"Scan error: {str(e)[:150]}")

    if st.session_state.last_scan_time:
        _now_cd=datetime.now(jakarta_tz).timestamp()
        _rem_cd=max(0,300-(_now_cd-st.session_state.last_scan_time))
        _last_cd=datetime.fromtimestamp(st.session_state.last_scan_time,jakarta_tz).strftime("%H:%M:%S")
        last_mode=st.session_state.get("last_scan_mode","")
        tf_info="📅 Daily" if last_mode=="Bagger 💎" else "⚡ 15M"
        st.caption(f"⏱️ Next auto-scan: {int(_rem_cd//60):02d}:{int(_rem_cd%60):02d} · Last: {_last_cd} WIB · {tf_info} · {last_mode}")

    results=st.session_state.scan_results
    if not results and not do_scan:
        st.markdown(f"""
        <div style="text-align:center;padding:48px;color:#4a5568;font-family:Space Mono,monospace;">
          <div style="font-size:36px;margin-bottom:12px;">🔥</div>
          <div style="font-size:13px;letter-spacing:2px;">KLIK SCAN UNTUK MULAI</div>
          <div style="font-size:10px;margin-top:8px;color:#2d3748;">
            {"⚡ Quick: 200 saham" if quick_mode else f"Full: {len(raw_stocks)} saham"} · {regime} · {rcfg["mode"]}
          </div>
        </div>""", unsafe_allow_html=True)
    elif results:
        df_out=pd.DataFrame(results).sort_values("Score",ascending=False).reset_index(drop=True)
        gacor=df_out[df_out["Signal"].str.contains("GACOR|REVERSAL",na=False)]
        bagger=df_out[df_out["Signal"].str.contains("BAGGER|KANDIDAT",na=False)]
        potensi=df_out[df_out["Signal"].str.contains("POTENSIAL",na=False)]
        avg_rsi=df_out['RSI-EMA'].mean()
        last_mode=st.session_state.get("last_scan_mode","")
        tf_badge='<span style="font-size:9px;color:#bf5fff;background:rgba(191,95,255,.1);padding:2px 6px;border-radius:3px;">📅 DAILY TF</span>' if last_mode=="Bagger 💎" else '<span style="font-size:9px;color:#00e5ff;background:rgba(0,229,255,.1);padding:2px 6px;border-radius:3px;">⚡ 15M</span>'
        st.markdown(f"""
        <div class="metric-row">
          <div class="metric-card" style="border-top-color:{rcolor}"><div class="metric-label">Regime</div>
            <div class="metric-value" style="font-size:16px;color:{rcolor}">{regime}</div>
            <div class="metric-sub">{ihsg_price:,.0f} {chg_sym}{abs(ihsg_chg):.2f}%</div></div>
          <div class="metric-card orange"><div class="metric-label">Mode</div>
            <div class="metric-value" style="font-size:12px;margin-top:4px;">{last_mode}</div>
            <div class="metric-sub">{tf_badge}</div></div>
          <div class="metric-card green"><div class="metric-label">Signal Lolos</div>
            <div class="metric-value">{len(df_out)}</div><div class="metric-sub">dari {len(raw_stocks)} emiten</div></div>
          <div class="metric-card purple"><div class="metric-label">BAGGER 💎</div>
            <div class="metric-value">{len(bagger)}</div><div class="metric-sub">Wyckoff Daily</div></div>
          <div class="metric-card red"><div class="metric-label">GACOR 🔥</div>
            <div class="metric-value">{len(gacor)}</div><div class="metric-sub">score ≥ 5</div></div>
          <div class="metric-card amber"><div class="metric-label">POTENSIAL</div>
            <div class="metric-value">{len(potensi)}</div></div>
          <div class="metric-card"><div class="metric-label">Avg RSI-EMA</div>
            <div class="metric-value" style="color:{'#00ff88' if avg_rsi>50 else '#ffb700' if avg_rsi>35 else '#ff3d5a'}">{avg_rsi:.1f}</div></div>
        </div>""", unsafe_allow_html=True)

        th='<div class="tape-wrap"><div class="tape-inner">'
        for _,row in df_out.iterrows():
            roc=row['ROC 3B%']; is_bag="BAGGER" in row['Signal'] or "KANDIDAT" in row['Signal']
            cls='bagger' if is_bag else('up' if roc>0 else('down' if roc<0 else'flat'))
            sym='💎' if is_bag else('▲' if roc>0 else('▼' if roc<0 else'─'))
            tf_t="[D]" if row.get("TF","")=="Daily" else "[15M]"
            th+=f'<span class="tape-item {cls}">{row["Ticker"]} {int(row["Price"])} {sym}{abs(roc):.1f}% {tf_t}</span>'
        th+=th.replace('tape-inner">',''); th+='</div></div>'
        st.markdown(th, unsafe_allow_html=True)

        if not bagger.empty:
            st.markdown(f'<div class="bagger-alert-box"><div class="bagger-title">💎 WYCKOFF BAGGER ALERT · {len(bagger)} KANDIDAT · DAILY TF</div><div style="font-size:11px;color:#4a5568;margin-top:4px;">Phase A-B (Sideways+Dry Vol) · Spring/Shakeout · Phase D (RVOL+Breakout)</div></div>',unsafe_allow_html=True)
        # DS asing flow summary (hanya tampil kalau ada DS data)
        if "FDir" in df_out.columns and df_out["Source"].eq("DS").any():
            _ds_count=df_out["Source"].eq("DS").sum()
            _ab=df_out[df_out["FDir"]=="🔵 BELI"]["Ticker"].tolist()
            _aj=df_out[df_out["FDir"]=="🔴 JUAL"]["Ticker"].tolist()
            _ab_str=", ".join(_ab[:5]) or "—"; _aj_str=", ".join(_aj[:5]) or "—"
            st.markdown(f"""
<div style="display:flex;gap:10px;margin-bottom:10px;flex-wrap:wrap">
  <div style="background:#0a1525;border:1px solid #4da6ff44;border-radius:8px;padding:8px 14px;flex:1;min-width:120px">
    <div style="font-family:Space Mono,monospace;font-size:9px;color:#4da6ff;letter-spacing:1px">🔵 ASING NET BUY</div>
    <div style="font-family:Space Mono,monospace;font-size:18px;font-weight:700;color:#4da6ff">{len(_ab)}</div>
    <div style="font-size:9px;color:#4a5568">{_ab_str}</div>
  </div>
  <div style="background:#200d0d;border:1px solid #ff3d5a44;border-radius:8px;padding:8px 14px;flex:1;min-width:120px">
    <div style="font-family:Space Mono,monospace;font-size:9px;color:#ff3d5a;letter-spacing:1px">🔴 ASING NET SELL</div>
    <div style="font-family:Space Mono,monospace;font-size:18px;font-weight:700;color:#ff3d5a">{len(_aj)}</div>
    <div style="font-size:9px;color:#4a5568">{_aj_str}</div>
  </div>
  <div style="background:#0d1117;border:1px solid #1c2533;border-radius:8px;padding:8px 14px;flex:1;min-width:120px">
    <div style="font-family:Space Mono,monospace;font-size:9px;color:#4a5568;letter-spacing:1px">⚡ DATA SOURCE</div>
    <div style="font-family:Space Mono,monospace;font-size:14px;font-weight:700;color:#2dd4bf">Hybrid</div>
    <div style="font-size:9px;color:#4a5568">{_ds_count} DS · {len(df_out)-_ds_count} yF</div>
  </div>
</div>""", unsafe_allow_html=True)
        elif DS_KEY:
            st.caption("⚡ DS API key aktif — data asing tersedia setelah scan")

        if not gacor.empty:
            st.markdown(f'<div class="alert-box"><div class="alert-title">🚨 GACOR ALERT · {len(gacor)} SAHAM · {last_mode}</div><div style="font-size:11px;color:#4a5568;margin-top:4px;">Score ≥ 5 · Multi-indikator · R:R optimal</div></div>',unsafe_allow_html=True)

        if view_mode=="Card View 🃏":
            st.markdown('<div class="section-title">Signal Cards</div>',unsafe_allow_html=True)
            card_html='<div class="signal-grid">'
            for _,row in df_out.head(20).iterrows():
                sc_int=int(row['Score']); is_bag="BAGGER" in row['Signal'] or "KANDIDAT" in row['Signal']
                bar_cls="filled-purple" if is_bag else "filled"
                bars=''.join([f'<div class="sc-bar {bar_cls if i<sc_int else "empty"}" style="width:28px"></div>' for i in range(6)])
                roc_c='#00ff88' if row['ROC 3B%']>0 else'#ff3d5a'
                te="📈" if "▲" in row['Trend'] else("📉" if "▼" in row['Trend'] else"➡️")
                sig_color='#bf5fff' if is_bag else('#00ff88' if sc_int>=5 else '#ffb700' if sc_int>=4 else '#00e5ff')
                tf_badge_card='<span style="font-size:8px;color:#bf5fff;margin-left:4px;">[D1]</span>' if row.get("TF","")=="Daily" else '<span style="font-size:8px;color:#4a5568;margin-left:4px;">[15M]</span>'
                card_html+=f"""<div class="signal-card {row['_class']}">
                  <div style="display:flex;justify-content:space-between;align-items:flex-start;">
                    <div><div class="sc-ticker">{row['Ticker']}{tf_badge_card}</div>
                    <div class="sc-price" style="color:{roc_c}">{int(row['Price']):,} {te}</div></div>
                    <div style="text-align:right;">
                      <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;">SCORE</div>
                      <div style="font-family:Space Mono,monospace;font-size:20px;font-weight:700;color:{sig_color}">{row['Score']}</div>
                    </div>
                  </div>
                  <div class="sc-signal" style="color:{sig_color}">{row['Signal']}</div>
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
                  <div style="margin-top:8px;font-size:10px;color:#4a5568;line-height:1.4;font-family:Space Mono,monospace;">{row['Reasons'][:80]}</div>
                </div>"""
            card_html+='</div>'
            st.markdown(card_html, unsafe_allow_html=True)

        st.markdown('<div class="section-title">Full Signal Table</div>',unsafe_allow_html=True)
        display_cols=["Ticker","TF","Price","Score","Signal","Sinyal_v2","Aksi_v2","FDir","Trend","RSI-EMA","Stoch K","Stoch D","MACD Hist","RVOL","BB%","ROC 3B%","VWAP","TP","SL","R:R","Turnover(M)","Source","Reasons"]
        display_cols=[c for c in display_cols if c in df_out.columns]
        st.dataframe(df_out[display_cols],width='stretch',hide_index=True,column_config={
            "Score":      st.column_config.ProgressColumn("Score",min_value=0,max_value=6,format="%.1f"),
            "RSI-EMA":    st.column_config.NumberColumn("RSI-EMA",format="%.1f"),
            "RVOL":       st.column_config.NumberColumn("RVOL",format="%.1fx"),
            "ROC 3B%":    st.column_config.NumberColumn("ROC 3B%",format="%.2f%%"),
            "Turnover(M)":st.column_config.NumberColumn("Turnover(M)",format="Rp%.0fM"),
            "FDir":       st.column_config.TextColumn("Asing Flow"),
            "Sinyal_v2":  st.column_config.TextColumn("Sinyal DS"),
            "Aksi_v2":    st.column_config.TextColumn("Aksi DS"),
            "Source":     st.column_config.TextColumn("Source"),
        })

# ════════════════════════════════════════════════════
#  TAB 2: WATCHLIST
# ════════════════════════════════════════════════════
with tab_watchlist:
    st.markdown('<div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-bottom:12px;padding:10px 14px;background:#0d1117;border-radius:6px;border-left:3px solid #ff7b00;">Analisa mendalam per saham. Bagger 💎 otomatis pakai <b style="color:#bf5fff">Daily TF</b>.</div>',unsafe_allow_html=True)
    wc1,wc2,wc3=st.columns([3,1,1])
    with wc1:
        wl_input=st.text_area("Ticker",placeholder="Contoh:\nBBCA\nARCI, ASSA, GOTO",height=120,label_visibility="collapsed",key="wl_input")
    with wc2:
        wl_mode=st.radio("Mode",["Scalping ⚡","Momentum 🚀","Reversal 🎯","Bagger 💎"],key="wl_mode")
        st.caption(f"Regime suggest: {rcfg['mode']}")
        if wl_mode=="Bagger 💎":
            st.markdown('<div style="font-size:9px;color:#bf5fff;">📅 Pakai Daily TF</div>',unsafe_allow_html=True)
    with wc3:
        st.markdown("<br>",unsafe_allow_html=True)
        wl_run=st.button("🔍 Analisa",use_container_width=True,key="wl_run")
        wl_tele=st.button("📡 Kirim Telegram",use_container_width=True,key="wl_tele")
        wl_share=st.button("📋 Copy Hasil",use_container_width=True,key="wl_share")

    if wl_run and wl_input.strip():
        raw_wl=list(dict.fromkeys([t.strip().upper() for line in wl_input.split("\n")
                                   for t in line.split(",") if t.strip()]))
        if raw_wl:
            with st.spinner(f"Menganalisa {len(raw_wl)} saham ({'Daily' if wl_mode=='Bagger 💎' else '15M'})..."):
                wl_res=[]
                for t in raw_wl:
                    df=None
                    try:
                        if wl_mode=="Bagger 💎":
                            df=_ticker_history(t+".JK","60d","1d")
                            if df is None:
                                raw=yf.download(t+".JK",period="60d",interval="1d",progress=False,auto_adjust=True,threads=False,session=_YF_SESSION)
                                df=_yf_extract(raw,t+".JK",1)
                            min_b=20
                        else:
                            df=_ticker_history(t+".JK","7d","15m")
                            if df is None:
                                raw=yf.download(t+".JK",period="7d",interval="15m",progress=False,auto_adjust=True,threads=False,session=_YF_SESSION)
                                df=_yf_extract(raw,t+".JK",1)
                            min_b=30
                        if df is not None and len(df)<min_b: df=None
                    except: pass
                    if df is None:
                        wl_res.append({"Ticker":t,"Price":0,"Score":0,"Signal":"No data","Trend":"-","TF":"-",
                            "RSI-EMA":0,"Stoch K":0,"RVOL":0,"BB%":0,"ROC 3B%":0,
                            "VWAP":0,"TP":0,"SL":0,"R:R":0,"ATR":0,"Reasons":"No data","_class":"","MACD Hist":0}); continue
                    try:
                        df=apply_indicators(df)
                        r=df.iloc[-1]; p=df.iloc[-2]; p2=df.iloc[-3] if len(df)>=3 else p
                        close=float(r['Close']); atr=float(r['ATR']) if not np.isnan(float(r['ATR'])) else close*0.01
                        slm=rcfg.get("sl_mult",0.8)
                        if wl_mode=="Scalping ⚡":   sc,reasons,_=score_scalping(r,p,p2);  tp=close+1.5*atr; sl=close-slm*atr
                        elif wl_mode=="Momentum 🚀": sc,reasons,_=score_momentum(r,p,p2);  tp=close+2.0*atr; sl=close-slm*atr
                        elif wl_mode=="Bagger 💎":   sc,reasons,_=score_bagger(r,p,p2,df); tp=close+3.0*atr; sl=close-1.0*atr
                        else:                        sc,reasons,_=score_reversal(r,p,p2);  tp=close+2.5*atr; sl=close-slm*atr
                        sig=get_signal(sc,wl_mode); rr=(tp-close)/max(close-sl,0.01)
                        e9=float(r['EMA9']); e21=float(r['EMA21']); e50=float(r['EMA50'])
                        trend="▲ UP" if e9>e21>e50 else("▼ DOWN" if e9<e21<e50 else "◆ SIDE")
                        _pvt=None
                        try:
                            if len(df)>=2:
                                prev=df.iloc[-2]; pp=(float(prev["High"])+float(prev["Low"])+float(prev["Close"]))/3
                                _pvt={"PP":pp,"R1":2*pp-float(prev["Low"]),"R2":pp+(float(prev["High"])-float(prev["Low"])),"S1":2*pp-float(prev["High"]),"S2":pp-(float(prev["High"])-float(prev["Low"]))}
                        except: pass
                        _pvt_pos=get_pivot_position(close,_pvt)[0] if _pvt else "-"
                        _mtf={}
                        try:
                            _df_mtf=apply_indicators(df.copy())
                            if len(_df_mtf)>=3:
                                _r=_df_mtf.iloc[-1]; _p=_df_mtf.iloc[-2]; _p2=_df_mtf.iloc[-3]
                                if wl_mode=="Scalping ⚡":   _sc_m,_,_=score_scalping(_r,_p,_p2)
                                elif wl_mode=="Momentum 🚀": _sc_m,_,_=score_momentum(_r,_p,_p2)
                                elif wl_mode=="Bagger 💎":   _sc_m,_,_=score_bagger(_r,_p,_p2,_df_mtf)
                                else:                         _sc_m,_,_=score_reversal(_r,_p,_p2)
                                _mtf["M15"]=round(_sc_m,1)
                                for _rs_rule,_rs_key,_min_b in [("1h","H1",10),("1D","D1",3)]:
                                    try:
                                        _df_rs=df.resample(_rs_rule).agg({"Open":"first","High":"max","Low":"min","Close":"last","Volume":"sum"}).dropna(subset=["Close"])
                                        _df_rs=_df_rs[_df_rs["Volume"]>0]
                                        if len(_df_rs)>=_min_b:
                                            _df_rs=apply_indicators(_df_rs)
                                            _rr=_df_rs.iloc[-1]; _pp=_df_rs.iloc[-2]; _p2r=_df_rs.iloc[-3] if len(_df_rs)>=3 else _pp
                                            if wl_mode=="Scalping ⚡":   _sc_r,_,_=score_scalping(_rr,_pp,_p2r)
                                            elif wl_mode=="Momentum 🚀": _sc_r,_,_=score_momentum(_rr,_pp,_p2r)
                                            elif wl_mode=="Bagger 💎":   _sc_r,_,_=score_bagger(_rr,_pp,_p2r,_df_rs)
                                            else:                         _sc_r,_,_=score_reversal(_rr,_pp,_p2r)
                                            _mtf[_rs_key]=round(_sc_r,1)
                                    except: pass
                        except: pass
                        _align,_align_col,_avg=mtf_alignment(_mtf)
                        wl_res.append({"Ticker":t,"Price":int(close),"Score":sc,"Signal":sig,
                            "Trend":trend,"TF":"Daily" if wl_mode=="Bagger 💎" else "15M",
                            "RSI-EMA":round(float(r['RSI_EMA']),1),"Stoch K":round(float(r['STOCH_K']),1),
                            "RVOL":round(float(r['RVOL']),2),"BB%":round(float(r['BB_pct']),2),
                            "ROC 3B%":round(float(r['ROC3'])*100,2),"VWAP":int(float(r['VWAP'])),
                            "TP":int(tp),"SL":int(sl),"R:R":round(rr,1),"ATR":round(atr,0),
                            "MACD Hist":round(float(r['MACD_Hist']),4),"Reasons":" · ".join(reasons),
                            "_class":get_card_class(sig),"Pivot Pos":_pvt_pos,
                            "PP":int(_pvt["PP"]) if _pvt else 0,"R1":int(_pvt["R1"]) if _pvt else 0,
                            "S1":int(_pvt["S1"]) if _pvt else 0,"MTF Align":_align,
                            "M15":_mtf.get("M15",0),"H1":_mtf.get("H1",0),"D1":_mtf.get("D1",0)})
                    except Exception as ex:
                        wl_res.append({"Ticker":t,"Price":0,"Score":0,"Signal":f"Err:{str(ex)[:20]}",
                            "RSI-EMA":0,"Stoch K":0,"RVOL":0,"BB%":0,"Trend":"-","TF":"-",
                            "TP":0,"SL":0,"R:R":0,"ROC 3B%":0,"VWAP":0,"ATR":0,"Reasons":"","_class":"","MACD Hist":0})
            st.session_state.wl_results=wl_res; st.session_state.wl_mode_used=wl_mode
            ok=[r for r in wl_res if r["Score"]>0]
            bag=[r for r in ok if any(k in r.get("Signal","") for k in ["BAGGER","KANDIDAT"])]
            gcr=[r for r in ok if any(k in r.get("Signal","") for k in ["GACOR","REVERSAL"])]
            pot=[r for r in ok if "POTENSIAL" in r.get("Signal","")]
            st.markdown(f"""<div class="metric-row" style="margin-top:16px;">
              <div class="metric-card orange"><div class="metric-label">Dipantau</div><div class="metric-value">{len(raw_wl)}</div></div>
              <div class="metric-card purple"><div class="metric-label">BAGGER 💎</div><div class="metric-value">{len(bag)}</div></div>
              <div class="metric-card green"><div class="metric-label">GACOR 🔥</div><div class="metric-value">{len(gcr)}</div></div>
              <div class="metric-card amber"><div class="metric-label">POTENSIAL</div><div class="metric-value">{len(pot)}</div></div>
              <div class="metric-card"><div class="metric-label">Data OK</div><div class="metric-value">{len(ok)}</div></div>
            </div>""",unsafe_allow_html=True)
            ch='<div class="signal-grid">'
            for row in sorted(wl_res,key=lambda x:x["Score"],reverse=True):
                if row["Price"]==0:
                    ch+=f'<div class="signal-card"><div class="sc-ticker">{row["Ticker"]}</div><div style="font-size:11px;color:#4a5568;margin-top:6px;">{row.get("Signal","No data")}</div></div>'
                    continue
                sc_int=int(row["Score"]); bars=''.join([f'<div class="sc-bar {"filled" if i<sc_int else "empty"}" style="width:26px"></div>' for i in range(6)])
                sig=row.get("Signal","-"); is_bag="BAGGER" in sig or "KANDIDAT" in sig
                sc_col="#bf5fff" if is_bag else("#00ff88" if("GACOR" in sig or "REVERSAL" in sig) else("#ffb700" if "POTENSIAL" in sig else "#00e5ff" if "WATCH" in sig else "#4a5568"))
                rsi_v=row["RSI-EMA"]; rsi_c="#ff3d5a" if rsi_v<30 else("#ffb700" if rsi_v<45 else "#00ff88" if rsi_v>60 else "#c9d1d9")
                roc_c="#00ff88" if row.get("ROC 3B%",0)>0 else "#ff3d5a"
                te="📈" if "▲" in row["Trend"] else("📉" if "▼" in row["Trend"] else "➡️")
                tf_b='<span style="font-size:8px;color:#bf5fff;">[D1]</span>' if row.get("TF","")=="Daily" else '<span style="font-size:8px;color:#4a5568;">[15M]</span>'
                ch+=f"""<div class="signal-card {row['_class']}">
                  <div style="display:flex;justify-content:space-between;">
                    <div><div class="sc-ticker">{row['Ticker']} {tf_b}</div>
                    <div class="sc-price" style="color:{roc_c}">{row['Price']:,} {te}</div></div>
                    <div style="text-align:right">
                      <div style="font-family:Space Mono,monospace;font-size:9px;color:#4a5568">SCORE</div>
                      <div style="font-family:Space Mono,monospace;font-size:22px;font-weight:700;color:{sc_col}">{row['Score']}</div>
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
                    M15:{row.get('M15',0)} · H1:{row.get('H1',0)} · D1:{row.get('D1',0)} | PP:{row.get('PP',0):,} · R1:{row.get('R1',0):,} · S1:{row.get('S1',0):,}
                  </div>
                </div>"""
            ch+='</div>'
            st.markdown(ch,unsafe_allow_html=True)
            df_wl=pd.DataFrame([r for r in wl_res if r["Price"]>0])
            if not df_wl.empty:
                show=["Ticker","TF","Price","Score","Signal","Trend","RSI-EMA","Stoch K","RVOL","BB%","ROC 3B%","VWAP","TP","SL","R:R","MTF Align","M15","H1","D1","Pivot Pos","PP","R1","S1","ATR","Reasons"]
                show=[c for c in show if c in df_wl.columns]
                st.dataframe(df_wl[show],width='stretch',hide_index=True,column_config={
                    "Score":   st.column_config.ProgressColumn("Score",min_value=0,max_value=6,format="%.1f"),
                    "RSI-EMA": st.column_config.NumberColumn("RSI-EMA",format="%.1f"),
                    "RVOL":    st.column_config.NumberColumn("RVOL",format="%.2fx"),
                    "ROC 3B%": st.column_config.NumberColumn("ROC 3B%",format="%.2f%%"),
                })

    if wl_tele and st.session_state.wl_results:
        to_send=[r for r in st.session_state.wl_results if r["Price"]>0]
        if to_send: send_telegram(to_send[:5],source="Watchlist"); st.success(f"📡 Terkirim!")

    if wl_share and st.session_state.wl_results:
        now_str=datetime.now(jakarta_tz).strftime("%d %b %Y %H:%M")
        wl_used=st.session_state.get('wl_mode_used','')
        txt=f"🔥 THETA TURBO WATCHLIST\n⏰ {now_str} WIB\n📊 Mode: {wl_used} | Regime: {regime}\n"+"─"*28+"\n"
        for r in sorted(st.session_state.wl_results,key=lambda x:x["Score"],reverse=True):
            if r["Price"]==0: continue
            sig=r.get("Signal","-")
            em="💎" if("BAGGER" in sig or "KANDIDAT" in sig) else("🔥" if("GACOR" in sig or "REVERSAL" in sig) else("⚡" if "POTENSIAL" in sig else "👀"))
            tf_t="[D1]" if r.get("TF","")=="Daily" else "[15M]"
            txt+=f"{em} {r['Ticker']}{tf_t} | {r['Price']:,} | Score:{r['Score']} | RSI:{r['RSI-EMA']} | {sig}\n"
            if r.get("Reasons"): txt+=f"   → {r['Reasons'][:60]}\n"
        txt+="─"*28+"\nby Theta Turbo v5.2 🔥 (Wyckoff Bagger Daily)"
        st.text_area("Copy untuk grup:",txt,height=280,key="share_out")

    if not st.session_state.wl_results and not wl_run:
        st.markdown('<div style="text-align:center;padding:48px;color:#4a5568;font-family:Space Mono,monospace;"><div style="font-size:32px;margin-bottom:12px;">👁️</div><div>MASUKKAN TICKER DI ATAS</div></div>',unsafe_allow_html=True)

# ════════════════════════════════════════════════════
#  TAB 3: BSJP
# ════════════════════════════════════════════════════
with tab_bsjp:
    now_wib=datetime.now(jakarta_tz)
    is_entry_time=(now_wib.hour==14 and now_wib.minute>=30) or (now_wib.hour==15 and now_wib.minute<=45)
    is_exit_time=(now_wib.hour==9) or (now_wib.hour==10 and now_wib.minute==0)
    st.markdown(f"""
    <div style="background:rgba(191,95,255,.08);border:1px solid rgba(191,95,255,.3);border-radius:8px;padding:14px 18px;margin-bottom:16px;">
      <div style="font-family:Space Mono,monospace;font-size:13px;font-weight:700;color:#bf5fff;letter-spacing:1px;">🌙 BELI SORE JUAL PAGI</div>
      <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-top:4px;">
        Entry: <span style="color:#ffb700">14:30 – 15:45 WIB</span> &nbsp;·&nbsp;
        Exit: <span style="color:#00ff88">Besok 09:00 – 10:00 WIB</span> &nbsp;·&nbsp;
        Status: <span style="color:{'#00ff88' if is_entry_time else '#ffb700' if is_exit_time else '#4a5568'}">
          {'🟢 WAKTU ENTRY!' if is_entry_time else '🟡 WAKTU EXIT!' if is_exit_time else '⏳ Tunggu 14:30 WIB'}
        </span>
      </div>
    </div>""",unsafe_allow_html=True)
    bsjp_c1,bsjp_c2=st.columns([2,1])
    with bsjp_c1:
        bsjp_min_score=st.slider("Min BSJP Score",0,6,4,key="bsjp_score")
        bsjp_min_rvol=st.slider("Min RVOL",1.0,5.0,1.5,0.1,key="bsjp_rvol")
    with bsjp_c2:
        bsjp_min_turn=st.number_input("Min Turnover (M Rp)",value=500,step=100,key="bsjp_turn")*1_000_000
        bsjp_tele=st.toggle("📡 Telegram Alert",value=True,key="bsjp_tele")
    do_bsjp=st.button("🌙 SCAN BSJP SEKARANG",type="primary",use_container_width=True,key="btn_bsjp")
    if "bsjp_results" not in st.session_state: st.session_state.bsjp_results=[]
    if do_bsjp:
        bsjp_prog=st.empty(); bsjp_prog.info("🌙 Scanning BSJP candidates...")
        bsjp_res=[]; scan_data=st.session_state.get("data_dict",{})
        if not scan_data:
            try: scan_data=fetch_intraday(tuple(stocks_yf[:200]))
            except: pass
        pb_bsjp=st.progress(0); tickers_bsjp=list(scan_data.keys())
        for i,ticker_yf in enumerate(tickers_bsjp):
            pb_bsjp.progress((i+1)/max(len(tickers_bsjp),1))
            try:
                df=scan_data[ticker_yf].copy()
                if len(df)<30: continue
                df_copy=apply_indicators(df)
                r=df_copy.iloc[-1]; p=df_copy.iloc[-2]; p2=df_copy.iloc[-3] if len(df_copy)>=3 else p
                close=float(r['Close']); vol=float(r['Volume'])
                turnover=close*vol; rvol=float(r['RVOL'])
                if turnover<bsjp_min_turn or rvol<bsjp_min_rvol: continue
                sc,reasons,_=score_bsjp(r,p,p2)
                if sc<bsjp_min_score: continue
                if sc>=5:   bsjp_sig="STRONG BUY 🌙"
                elif sc>=4: bsjp_sig="BUY ⚡"
                else:       bsjp_sig="WATCH 👀"
                atr=float(r['ATR']); tp=close+2.0*atr; sl=close-1.0*atr; rr=(tp-close)/max(close-sl,0.01)
                pvt=None
                try:
                    if len(df_copy)>=2:
                        prev=df_copy.iloc[-2]; pp=(float(prev["High"])+float(prev["Low"])+float(prev["Close"]))/3
                        pvt={"PP":pp,"R1":2*pp-float(prev["Low"]),"R2":pp+(float(prev["High"])-float(prev["Low"])),"S1":2*pp-float(prev["High"]),"S2":pp-(float(prev["High"])-float(prev["Low"]))}
                except: pass
                pvt_pos=get_pivot_position(close,pvt)[0] if pvt else "-"
                e9=float(r['EMA9']); e21=float(r['EMA21']); e50=float(r['EMA50'])
                trend="▲ UP" if e9>e21>e50 else("▼ DOWN" if e9<e21<e50 else"◆ SIDE")
                bsjp_res.append({
                    "Ticker":stock_map.get(ticker_yf,ticker_yf.replace(".JK","")),
                    "Price":int(close),"Score":sc,"Signal":bsjp_sig,"Trend":trend,
                    "RSI-EMA":round(float(r['RSI_EMA']),1),"Stoch K":round(float(r['STOCH_K']),1),
                    "RVOL":round(rvol,2),"TP":int(tp),"SL":int(sl),"R:R":round(rr,1),
                    "Turnover(M)":round(turnover/1e6,1),"Pivot Pos":pvt_pos,
                    "PP":int(pvt["PP"]) if pvt else 0,"R1":int(pvt["R1"]) if pvt else 0,
                    "S1":int(pvt["S1"]) if pvt else 0,"Reasons":" · ".join(reasons),
                    "_class":"gacor" if sc>=5 else "potensial" if sc>=4 else "watch"
                })
            except: continue
        pb_bsjp.empty(); bsjp_prog.empty()
        bsjp_res=sorted(bsjp_res,key=lambda x:x["Score"],reverse=True)
        st.session_state.bsjp_results=bsjp_res
        if bsjp_tele and bsjp_res:
            now_b=datetime.now(jakarta_tz); sep="━"*28
            msg=(f"🌙 *BSJP ALERT — BELI SORE JUAL PAGI*\n⏰ `{now_b.strftime('%H:%M:%S')} WIB`\n{sep}\n")
            for r in bsjp_res[:5]:
                bar="█"*int(r['Score'])+"░"*(6-int(r['Score']))
                msg+=(f"\n🌙 *{r['Ticker']}* `{r['Signal']}`\n"
                      f"   💰 Price: `{r['Price']:,}`\n📊 `[{bar}] {r['Score']}/6`\n"
                      f"   📈 RSI-EMA: `{r['RSI-EMA']}` | RVOL: `{r['RVOL']}x`\n"
                      f"   🎯 TP: `{r['TP']:,}` | SL: `{r['SL']:,}` | R:R `{r['R:R']}`\n"
                      f"   💡 _{r['Reasons'][:50]}_\n")
            msg+=f"\n{sep}\n🌙 _Entry 14:30-15:45 · Exit besok 09:00-10:00_\n⚠️ _BUKAN saran investasi!_"
            try:
                requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                              data={"chat_id":CHAT_ID,"text":msg,"parse_mode":"Markdown"},timeout=10)
            except: pass

    bsjp_results=st.session_state.bsjp_results
    if bsjp_results:
        strong=[r for r in bsjp_results if "STRONG" in r.get("Signal","")]
        buy=[r for r in bsjp_results if r.get("Signal","")=="BUY ⚡"]
        st.markdown(f"""
        <div class="metric-row">
          <div class="metric-card" style="border-top-color:#bf5fff"><div class="metric-label">Dipindai</div><div class="metric-value">{len(bsjp_results)}</div></div>
          <div class="metric-card green"><div class="metric-label">Strong Buy 🌙</div><div class="metric-value">{len(strong)}</div></div>
          <div class="metric-card amber"><div class="metric-label">Buy ⚡</div><div class="metric-value">{len(buy)}</div></div>
          <div class="metric-card"><div class="metric-label">Entry</div><div class="metric-value" style="font-size:13px;color:#ffb700">14:30</div></div>
          <div class="metric-card"><div class="metric-label">Exit</div><div class="metric-value" style="font-size:13px;color:#00ff88">09:00</div></div>
        </div>""",unsafe_allow_html=True)
        if len(bsjp_results)>=1:
            medals=["🥇","🥈","🥉"]; cols_top=st.columns(min(3,len(bsjp_results)))
            for idx,col in enumerate(cols_top):
                if idx>=len(bsjp_results): break
                row=bsjp_results[idx]; sig_col="#00ff88" if "STRONG" in row["Signal"] else "#ffb700"
                with col:
                    st.markdown(f"""
                    <div style="background:#0d1117;border:1px solid {sig_col}44;border-radius:10px;padding:16px;text-align:center;border-top:3px solid {sig_col};">
                      <div style="font-size:24px">{medals[idx]}</div>
                      <div style="font-family:Space Mono,monospace;font-size:18px;font-weight:700;color:#e6edf3;">{row['Ticker']}</div>
                      <div style="font-family:Space Mono,monospace;font-size:28px;font-weight:700;color:{sig_col};">{row['Score']}</div>
                      <div style="font-size:11px;font-weight:700;color:{sig_col};">{row['Signal']}</div>
                      <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-top:6px;">RVOL {row['RVOL']}x · RSI {row['RSI-EMA']}<br>TP {row['TP']:,} · SL {row['SL']:,}</div>
                    </div>""",unsafe_allow_html=True)
        df_bsjp=pd.DataFrame(bsjp_results)
        show_cols=["Ticker","Price","Score","Signal","Trend","RSI-EMA","Stoch K","RVOL","TP","SL","R:R","Pivot Pos","PP","R1","S1","Turnover(M)","Reasons"]
        show_cols=[c for c in show_cols if c in df_bsjp.columns]
        st.dataframe(df_bsjp[show_cols],width='stretch',hide_index=True,column_config={
            "Score":st.column_config.ProgressColumn("Score",min_value=0,max_value=6,format="%.1f"),
            "RVOL":st.column_config.NumberColumn("RVOL",format="%.2fx"),
        })
    elif not do_bsjp:
        st.markdown('<div style="text-align:center;padding:48px;color:#4a5568;font-family:Space Mono,monospace;"><div style="font-size:32px;margin-bottom:12px;">🌙</div><div>KLIK SCAN BSJP</div></div>',unsafe_allow_html=True)

# ════════════════════════════════════════════════════
#  TAB 4: SEKTOR
# ════════════════════════════════════════════════════
with tab_sector:
    st.markdown('<div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-bottom:14px;padding:10px 14px;background:#0d1117;border-radius:6px;border-left:3px solid #ff7b00;">Track sektor momentum IDX hari ini.</div>',unsafe_allow_html=True)
    do_sector=st.button("🏭 REFRESH SECTORS",type="primary",use_container_width=True,key="btn_sector")
    if "sector_data" not in st.session_state: st.session_state.sector_data={}
    if do_sector:
        with st.spinner("Fetching sector data..."):
            sec_data=fetch_all_sectors(SECTORS,top_n=10,is_jk=True)
            st.session_state.sector_data=sec_data
    if st.session_state.sector_data:
        sorted_secs=sorted(st.session_state.sector_data.items(),key=lambda x:x[1]["avg_chg"],reverse=True)
        st.markdown('<div class="section-title">Sector Heatmap</div>',unsafe_allow_html=True)
        cols_sec=st.columns(3)
        for idx,(sec_name,sec_info) in enumerate(sorted_secs):
            chg=sec_info["avg_chg"]; col="#00ff88" if chg>1 else("#ffb700" if chg>0 else "#ff3d5a")
            bg="rgba(0,255,136,.06)" if chg>1 else("rgba(255,183,0,.06)" if chg>0 else "rgba(255,61,90,.06)")
            bull_pct=int(sec_info["bullish"]/max(sec_info["total"],1)*100)
            with cols_sec[idx%3]:
                st.markdown(f"""
                <div style="background:{bg};border:1px solid {col}44;border-radius:8px;padding:12px;margin-bottom:10px;">
                  <div style="font-family:Space Mono,monospace;font-size:10px;font-weight:700;color:#c9d1d9;">{sec_name}</div>
                  <div style="font-family:Space Mono,monospace;font-size:22px;font-weight:700;color:{col};margin:4px 0;">{chg:+.2f}%</div>
                  <div style="font-size:9px;color:#4a5568;">RVOL avg: {sec_info['avg_rvol']:.1f}x · Bullish: {sec_info['bullish']}/{sec_info['total']} ({bull_pct}%)</div>
                  <div style="height:4px;background:#1c2533;border-radius:2px;margin-top:6px;overflow:hidden;">
                    <div style="width:{bull_pct}%;height:100%;background:{col};border-radius:2px;"></div>
                  </div>
                </div>""",unsafe_allow_html=True)
        top3=sorted_secs[:3]; cols_top=st.columns(3)
        for cidx,(sec_name,sec_info) in enumerate(top3):
            with cols_top[cidx]:
                chg=sec_info["avg_chg"]; col="#00ff88" if chg>0 else "#ff3d5a"
                st.markdown(f'<div style="font-family:Space Mono,monospace;font-size:11px;color:{col};font-weight:700;margin-bottom:8px;">{sec_name}</div>',unsafe_allow_html=True)
                for stk in sorted(sec_info["stocks"],key=lambda x:x["chg"],reverse=True)[:5]:
                    sc="#00ff88" if stk["chg"]>0 else "#ff3d5a"
                    st.markdown(f'<div style="display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid #1c2533;font-family:Space Mono,monospace;font-size:10px;"><span style="color:#c9d1d9;">{stk["ticker"]}</span><span style="color:{sc}">{stk["chg"]:+.1f}%</span><span style="color:#4a5568;">RVOL {stk["rvol"]}x</span></div>',unsafe_allow_html=True)

    st.markdown('<div class="section-title" style="margin-top:24px;">Beta vs IHSG</div>',unsafe_allow_html=True)
    do_beta=st.button("🔬 Calculate Beta All Sectors",use_container_width=True,key="btn_beta")
    if "beta_data" not in st.session_state: st.session_state.beta_data=[]
    if do_beta:
        beta_res=[]; bp=st.progress(0); secs=list(SECTORS.items())
        for i,(sec_name,sec_stocks) in enumerate(secs):
            bp.progress((i+1)/len(secs))
            res=calc_sector_beta(sec_name,sec_stocks)
            if res: beta_res.append(res)
        bp.empty(); beta_res=sorted(beta_res,key=lambda x:x["beta"])
        st.session_state.beta_data=beta_res
    if st.session_state.beta_data:
        for b in st.session_state.beta_data:
            beta_lbl,beta_col=get_beta_label(b["beta"])
            rs_col="#00ff88" if b["rs5"]>0 else "#ff3d5a"
            width=min(100,int(abs(b["beta"])*50))
            st.markdown(f"""
            <div style="background:#0d1117;border:1px solid #1c2533;border-radius:8px;padding:12px 16px;margin-bottom:8px;border-left:4px solid {beta_col};">
              <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;">
                <div style="flex:2;"><div style="font-family:Space Mono,monospace;font-size:11px;font-weight:700;color:#c9d1d9;">{b['sector']}</div>
                  <div style="font-family:Space Mono,monospace;font-size:9px;color:#4a5568;">Corr: {b['corr']} · 1M: {b['ret_1m_sec']:+.1f}%</div></div>
                <div style="text-align:center;min-width:80px;"><div style="font-family:Space Mono,monospace;font-size:20px;font-weight:700;color:{beta_col};">{b['beta']}</div>
                  <div style="font-size:9px;color:{beta_col};">{beta_lbl}</div></div>
                <div style="text-align:center;min-width:80px;"><div style="font-family:Space Mono,monospace;font-size:14px;font-weight:700;color:{rs_col};">{b['rs5']:+.1f}%</div>
                  <div style="font-size:9px;color:#4a5568;">RS 5 Days</div></div>
              </div>
              <div style="height:4px;background:#1c2533;border-radius:2px;margin-top:10px;overflow:hidden;">
                <div style="width:{width}%;height:100%;background:{beta_col};border-radius:2px;"></div>
              </div>
            </div>""",unsafe_allow_html=True)
    if not st.session_state.sector_data:
        st.markdown('<div style="text-align:center;padding:48px;color:#4a5568;font-family:Space Mono,monospace;"><div style="font-size:32px;margin-bottom:12px;">🏭</div><div>KLIK REFRESH SECTORS</div></div>',unsafe_allow_html=True)

# ════════════════════════════════════════════════════
#  TAB 5: GAP UP
# ════════════════════════════════════════════════════
with tab_gapup:
    st.markdown('<div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-bottom:14px;padding:10px 14px;background:#0d1117;border-radius:6px;border-left:3px solid #00ff88;">Deteksi saham IDX berpotensi <b style="color:#00ff88">Gap Up</b> besok pagi.</div>',unsafe_allow_html=True)
    gu_c1,gu_c2=st.columns(2)
    with gu_c1: gu_min_score=st.slider("Min Gap Score",1,6,3,key="gu_score")
    with gu_c2: gu_quick=st.toggle("⚡ Quick Scan (200)",value=True,key="gu_quick")
    do_gapup=st.button("📈 SCAN GAP UP",type="primary",use_container_width=True,key="btn_gapup")
    if "gapup_results" not in st.session_state: st.session_state.gapup_results=[]
    if do_gapup:
        scan_tickers=stocks_yf[:200] if gu_quick else stocks_yf
        with st.spinner(f"Scanning {len(scan_tickers)} saham..."):
            gu_res=scan_gap_up(scan_tickers)
            gu_res=[r for r in gu_res if r["Gap Score"]>=gu_min_score]
            st.session_state.gapup_results=gu_res
        if gu_res and TOKEN and CHAT_ID:
            now_g=datetime.now(jakarta_tz); sep="━"*28
            msg=f"📈 *GAP UP SCANNER IDX*\n⏰ `{now_g.strftime('%H:%M:%S')} WIB`\n{sep}\n"
            for r in gu_res[:5]:
                msg+=(f"\n🚀 *{r['Ticker']}* `{r['Signal']}`\n"
                      f"   💰 Price: `{r['Price']:,}` ({r['Chg %']:+.1f}%)\n"
                      f"   📊 Gap Score: `{r['Gap Score']}/6`\n"
                      f"   🌊 RVOL: `{r['RVOL']}x`\n💡 _{r['Reasons'][:50]}_\n")
            msg+=f"\n{sep}\n⚠️ _BUKAN saran investasi!_"
            try:
                requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                              data={"chat_id":CHAT_ID,"text":msg,"parse_mode":"Markdown"},timeout=10)
                st.success("📡 Gap Up alert terkirim!")
            except: pass
    gapup_res=st.session_state.gapup_results
    if gapup_res:
        gap_confirmed=[r for r in gapup_res if "GAP UP" in r.get("Signal","")]
        potential=[r for r in gapup_res if "POTENTIAL" in r.get("Signal","")]
        st.markdown(f"""<div class="metric-row">
          <div class="metric-card green"><div class="metric-label">Gap Confirmed 🚀</div><div class="metric-value">{len(gap_confirmed)}</div></div>
          <div class="metric-card amber"><div class="metric-label">Potential ⚡</div><div class="metric-value">{len(potential)}</div></div>
          <div class="metric-card"><div class="metric-label">Total</div><div class="metric-value">{len(gapup_res)}</div></div>
        </div>""",unsafe_allow_html=True)
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
              <div style="margin-top:8px;font-size:10px;color:#4a5568;font-family:Space Mono,monospace;">{row['Reasons'][:80]}</div>
            </div>"""
        gu_html+='</div>'
        st.markdown(gu_html,unsafe_allow_html=True)
        df_gu=pd.DataFrame(gapup_res)
        st.dataframe(df_gu,width='stretch',hide_index=True,column_config={
            "Gap Score":st.column_config.ProgressColumn("Gap Score",min_value=0,max_value=6,format="%.1f"),
            "RVOL":st.column_config.NumberColumn("RVOL",format="%.2fx"),
            "Chg %":st.column_config.NumberColumn("Chg %",format="%.2f%%"),
        })
    elif not do_gapup:
        st.markdown('<div style="text-align:center;padding:48px;color:#4a5568;font-family:Space Mono,monospace;"><div style="font-size:32px;margin-bottom:12px;">📈</div><div>KLIK SCAN GAP UP</div></div>',unsafe_allow_html=True)

# ════════════════════════════════════════════════════
#  TAB 6: TRAILING STOP
# ════════════════════════════════════════════════════
with tab_trail:
    st.markdown('<div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-bottom:14px;padding:10px 14px;background:#0d1117;border-radius:6px;border-left:3px solid #bf5fff;">Lock profit. ATR 2x = scalping · ATR 3x = swing · Persen = fixed trail.</div>',unsafe_allow_html=True)
    tr_c1,tr_c2=st.columns(2)
    with tr_c1:
        st.markdown('<div class="settings-label">POSISI LO</div>',unsafe_allow_html=True)
        tr_ticker=st.text_input("Ticker IDX (tanpa .JK)",value="BBCA",key="tr_ticker").upper()
        tr_entry=st.number_input("Harga Entry (Rp)",value=9000,step=50,key="tr_entry")
        tr_qty=st.number_input("Lot (1 lot=100 lembar)",value=10,step=1,key="tr_qty")
    with tr_c2:
        st.markdown('<div class="settings-label">TRAILING SETTINGS</div>',unsafe_allow_html=True)
        tr_method=st.radio("Method",["ATR","Persen","Swing Low"],key="tr_method")
        if tr_method=="ATR":      tr_atr_mult=st.slider("ATR Multiplier",1.0,5.0,2.0,0.5,key="tr_atr_m")
        elif tr_method=="Persen": tr_pct=st.slider("Trailing %",1.0,10.0,3.0,0.5,key="tr_pct")
        tr_alert=st.toggle("🔔 Telegram Alert",value=True,key="tr_alert")
    if st.button("🎯 CALCULATE TRAILING STOP",type="primary",use_container_width=True,key="btn_trail"):
        with st.spinner(f"Fetching {tr_ticker}..."):
            try:
                df_tr=_ticker_history(tr_ticker+".JK","7d","15m")
                if df_tr is None:
                    raw_tr=yf.download(tr_ticker+".JK",period="7d",interval="15m",progress=False,auto_adjust=True,threads=False,session=_YF_SESSION)
                    df_tr=_yf_extract(raw_tr,tr_ticker+".JK",1)
                if df_tr is not None and len(df_tr)>=20:
                    df_tr=apply_indicators(df_tr)
                    current=float(df_tr["Close"].iloc[-1]); atr_val=float(df_tr["ATR"].iloc[-1])
                    if tr_method=="ATR":      trail_result=calc_trailing_stop(tr_entry,current,atr_val,"ATR",tr_atr_mult)
                    elif tr_method=="Persen": trail_result=calc_trailing_stop(tr_entry,current,atr_val,"Persen",pct=tr_pct)
                    else:                     trail_result=calc_trailing_stop(tr_entry,current,atr_val,"Swing Low")
                    stop=trail_result["stop"]; dist=trail_result["distance"]
                    p_float=trail_result["profit_float"]; p_locked=trail_result["profit_locked"]
                    is_profit=trail_result["is_profitable"]
                    profit_rp=(current-tr_entry)*tr_qty*100; locked_rp=max(0,(stop-tr_entry)*tr_qty*100)
                    stop_col="#00ff88" if is_profit else "#ff3d5a"; profit_col="#00ff88" if profit_rp>=0 else "#ff3d5a"
                    st.markdown(f"""
                    <div style="background:#0d1117;border:1px solid {stop_col}44;border-radius:10px;padding:20px;margin-top:12px;">
                      <div class="metric-row">
                        <div class="metric-card"><div class="metric-label">Harga Sekarang</div>
                          <div class="metric-value" style="color:#00e5ff">{current:,.0f}</div>
                          <div class="metric-sub">ATR: {atr_val:,.0f}</div></div>
                        <div class="metric-card" style="border-top-color:{stop_col}">
                          <div class="metric-label">🎯 Trailing Stop</div>
                          <div class="metric-value" style="color:{stop_col}">{stop:,.0f}</div>
                          <div class="metric-sub">Distance: {dist:,.0f}</div></div>
                        <div class="metric-card" style="border-top-color:{profit_col}">
                          <div class="metric-label">Float P&L</div>
                          <div class="metric-value" style="color:{profit_col}">{p_float:+.1f}%</div>
                          <div class="metric-sub">Rp {profit_rp:,.0f}</div></div>
                        <div class="metric-card" style="border-top-color:#00ff88">
                          <div class="metric-label">Locked 🔒</div>
                          <div class="metric-value" style="color:#00ff88">{p_locked:+.1f}%</div>
                          <div class="metric-sub">Rp {locked_rp:,.0f}</div></div>
                      </div>
                      <div style="margin-top:12px;font-family:Space Mono,monospace;font-size:10px;color:#4a5568;">
                        💼 {tr_qty} lot · {'✅ Profit terkunci!' if is_profit else '⚠️ Stop di bawah entry'}
                      </div>
                    </div>""",unsafe_allow_html=True)
                    if tr_alert and TOKEN and CHAT_ID:
                        now_tr=datetime.now(jakarta_tz)
                        msg_tr=(f"🎯 *TRAILING STOP UPDATE*\n⏰ `{now_tr.strftime('%H:%M:%S')} WIB`\n{'━'*28}\n"
                                f"📌 *{tr_ticker}* | {tr_method}\n💰 Entry: `{tr_entry:,}` → Now: `{current:,.0f}`\n"
                                f"🎯 Stop: `{stop:,.0f}` | Locked: `{p_locked:+.1f}%` (Rp {locked_rp:,.0f})\n"
                                f"📊 Float: `{p_float:+.1f}%` (Rp {profit_rp:,.0f})\n{'━'*28}\n⚠️ _BUKAN saran investasi!_")
                        try:
                            requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                                          data={"chat_id":CHAT_ID,"text":msg_tr,"parse_mode":"Markdown"},timeout=10)
                            st.success("📡 Alert terkirim!")
                        except: pass
                else:
                    st.error(f"Data {tr_ticker} tidak tersedia. Coba lagi.")
            except Exception as ex:
                st.error(f"Error: {str(ex)[:80]}")

# ════════════════════════════════════════════════════
#  TAB 7: BACKTEST
# ════════════════════════════════════════════════════
with tab_backtest:
    st.markdown('<div class="section-title">Backtest Engine · 15M Intraday · IDX</div>',unsafe_allow_html=True)
    bt_c1,bt_c2,bt_c3,bt_c4=st.columns(4)
    bt_mode=bt_c1.selectbox("Mode",["Scalping ⚡","Momentum 🚀","Reversal 🎯","Bagger 💎"],key="bt_mode")
    bt_sc=bt_c2.slider("Min Score",0,6,4,key="bt_sc")
    bt_fwd=int(bt_c3.number_input("Hold (bars)",value=4,step=1,min_value=1,max_value=20))
    bt_sl_mult=bt_c4.number_input("SL mult (xATR)",value=0.8,step=0.1,min_value=0.1,max_value=3.0)
    st.caption(f"Hold {bt_fwd} bars × 15 min = ~{bt_fwd*15} menit per trade")
    if st.button("🚀 Run Backtest",type="primary",key="bt_run"):
        data_dict=st.session_state.get("data_dict",{})
        if not data_dict:
            st.warning("Run Scanner dulu bro!")
        else:
            bt_results=[]; bt_by_trend={"▲ UP":[],"▼ DOWN":[],"◆ SIDE":[]}
            bt_by_session={"Pagi 09-11":[],"Siang 11-14":[],"Sore 14-16":[]}
            bt_by_score={4:[],5:[],6:[]}
            bt_pb=st.progress(0); sample=list(data_dict.keys())[:80]
            for bi,ticker in enumerate(sample):
                bt_pb.progress((bi+1)/len(sample))
                try:
                    d=data_dict[ticker].copy()
                    if len(d)<60: continue
                    d=apply_indicators(d)
                    for ii in range(50,len(d)-bt_fwd):
                        r0=d.iloc[ii]; r1=d.iloc[ii-1]; r2=d.iloc[ii-2]
                        if bt_mode=="Scalping ⚡":   sc,_,_=score_scalping(r0,r1,r2)
                        elif bt_mode=="Momentum 🚀": sc,_,_=score_momentum(r0,r1,r2)
                        elif bt_mode=="Bagger 💎":   sc,_,_=score_bagger(r0,r1,r2,d.iloc[:ii+1])
                        else:                         sc,_,_=score_reversal(r0,r1,r2)
                        if sc<bt_sc: continue
                        entry=float(r0['Close']); atr_v=float(r0['ATR']) if not np.isnan(float(r0['ATR'])) else entry*0.005
                        if bt_mode=="Scalping ⚡":   tp_p=entry+1.5*atr_v; sl_p=entry-bt_sl_mult*atr_v
                        elif bt_mode=="Momentum 🚀": tp_p=entry+2.0*atr_v; sl_p=entry-bt_sl_mult*atr_v
                        elif bt_mode=="Bagger 💎":   tp_p=entry+3.0*atr_v; sl_p=entry-1.0*atr_v
                        else:                         tp_p=entry+2.5*atr_v; sl_p=entry-bt_sl_mult*atr_v
                        exit_price=float(d.iloc[ii+bt_fwd]['Close'])
                        for fwd_i in range(1,bt_fwd+1):
                            bar=d.iloc[ii+fwd_i]
                            if float(bar['High'])>=tp_p: exit_price=tp_p; break
                            if float(bar['Low'])<=sl_p:  exit_price=sl_p; break
                        ret=(exit_price-entry)/entry*100; bt_results.append(ret)
                        e9=float(r0['EMA9']); e21=float(r0['EMA21']); e50=float(r0['EMA50'])
                        tr="▲ UP" if e9>e21>e50 else("▼ DOWN" if e9<e21<e50 else "◆ SIDE")
                        bt_by_trend[tr].append(ret)
                        try:
                            hr=d.index[ii].hour
                            if 9<=hr<11:  bt_by_session["Pagi 09-11"].append(ret)
                            elif 11<=hr<14: bt_by_session["Siang 11-14"].append(ret)
                            elif 14<=hr<16: bt_by_session["Sore 14-16"].append(ret)
                        except: pass
                        sc_int=int(sc)
                        if sc_int in bt_by_score: bt_by_score[sc_int].append(ret)
                except: continue
            bt_pb.empty()
            if not bt_results:
                st.warning("Tidak ada trade yang match. Kurangi Min Score.")
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
                </div>""",unsafe_allow_html=True)
                tab_tr,tab_ses,tab_sc2=st.tabs(["📈 Per Trend","⏰ Per Sesi","🎯 Per Score"])
                with tab_tr:
                    for tr_name,vals in bt_by_trend.items():
                        if not vals: continue
                        a=np.array(vals); wr_t=len(a[a>0])/len(a)*100; avg_t=np.mean(a)
                        col="#00ff88" if wr_t>=55 else("#ffb700" if wr_t>=50 else "#ff3d5a")
                        st.markdown(f'<div style="margin-bottom:10px;"><div style="display:flex;justify-content:space-between;"><span style="font-family:Space Mono,monospace;font-size:12px;color:#c9d1d9;">{tr_name}</span><span style="font-family:Space Mono,monospace;font-size:11px;color:{col};">{wr_t:.1f}% WR · avg {avg_t:+.2f}% · {len(a)} trades</span></div><div style="height:8px;background:var(--border);border-radius:4px;overflow:hidden;margin-top:4px;"><div style="width:{int(wr_t)}%;height:100%;background:{col};border-radius:4px;"></div></div></div>',unsafe_allow_html=True)
                with tab_ses:
                    for sname,vals in bt_by_session.items():
                        if not vals: continue
                        a=np.array(vals); wr_s=len(a[a>0])/len(a)*100; avg_s=np.mean(a)
                        col="#00ff88" if wr_s>=55 else("#ffb700" if wr_s>=50 else "#ff3d5a")
                        st.markdown(f'<div style="margin-bottom:10px;"><div style="display:flex;justify-content:space-between;"><span style="font-family:Space Mono,monospace;font-size:12px;color:#c9d1d9;">⏰ {sname}</span><span style="font-family:Space Mono,monospace;font-size:11px;color:{col};">{wr_s:.1f}% WR · avg {avg_s:+.2f}% · {len(a)} trades</span></div><div style="height:8px;background:var(--border);border-radius:4px;overflow:hidden;margin-top:4px;"><div style="width:{int(wr_s)}%;height:100%;background:{col};border-radius:4px;"></div></div></div>',unsafe_allow_html=True)
                with tab_sc2:
                    for sc_lv in [4,5,6]:
                        vals=bt_by_score.get(sc_lv,[])
                        if not vals: continue
                        a=np.array(vals); wr_v=len(a[a>0])/len(a)*100; avg_v=np.mean(a)
                        col="#00ff88" if wr_v>=55 else("#ffb700" if wr_v>=50 else "#ff3d5a")
                        st.markdown(f'<div style="margin-bottom:10px;"><div style="display:flex;justify-content:space-between;"><span style="font-family:Space Mono,monospace;font-size:12px;color:#c9d1d9;">Score {sc_lv} [{"█"*sc_lv+"░"*(6-sc_lv)}]</span><span style="font-family:Space Mono,monospace;font-size:11px;color:{col};">{wr_v:.1f}% WR · avg {avg_v:+.2f}% · {len(a)} trades</span></div><div style="height:8px;background:var(--border);border-radius:4px;overflow:hidden;margin-top:4px;"><div style="width:{int(wr_v)}%;height:100%;background:{col};border-radius:4px;"></div></div></div>',unsafe_allow_html=True)

# ════════════════════════════════════════════════════
#  FOOTER
# ════════════════════════════════════════════════════
_now_f=datetime.now(jakarta_tz).timestamp()
if st.session_state.last_scan_time:
    _rem2=max(0,300-(_now_f-st.session_state.last_scan_time))
    mnt2=int(_rem2//60); sec2=int(_rem2%60)
    last_t2=datetime.fromtimestamp(st.session_state.last_scan_time,jakarta_tz).strftime("%H:%M:%S")
    time_info=f"⏱️ Next auto-scan: <span style='color:#ff7b00'>{mnt2:02d}:{sec2:02d}</span> · Last: <span style='color:#2dd4bf'>{last_t2} WIB</span>"
else:
    time_info="⏱️ Klik Scan untuk mulai"

st.markdown(f"""
<div style="margin-top:28px;padding-top:14px;border-top:1px solid #1c2533;
     display:flex;justify-content:space-between;flex-wrap:wrap;gap:8px;">
  <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;">
    🔥 Theta Turbo v5.2 · IDX · 15M + Daily Wyckoff · yFinance Humanized
  </div>
  <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;">{time_info}</div>
</div>
<div style="font-family:Space Mono,monospace;font-size:9px;color:#2d3748;text-align:center;margin-top:8px;">
  ⚠️ BUKAN saran investasi · Untuk tujuan edukasi · DYOR selalu
</div>""",unsafe_allow_html=True)

# ════════════════════════════════════════════════════
#  AUTO-REFRESH
# ════════════════════════════════════════════════════
if st.session_state.last_scan_time:
    _now_f2=datetime.now(jakarta_tz).timestamp()
    if _now_f2-st.session_state.last_scan_time>=295:
        time.sleep(5); st.rerun()
