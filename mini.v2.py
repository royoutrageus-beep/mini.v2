import yfinance as yf
import pandas as pd
import streamlit as st
import time, random, requests, numpy as np, pytz
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

TOKEN   = st.secrets.get("TELEGRAM_TOKEN", "")
CHAT_ID = st.secrets.get("TELEGRAM_CHAT_ID", "")
jakarta_tz = pytz.timezone("Asia/Jakarta")

for _k,_v in [("tt_last_sent",set()),("wl_results",[]),("wl_mode_used",""),
               ("scan_results",[]),("data_dict",{}),("last_scan_time",None),
               ("last_scan_mode","Scalping ⚡"),("active_scan_mode","Scalping ⚡"),
               ("active_auto_regime",True),("sector_data",{}),("beta_data",[]),
               ("gapup_results",[]),("bsjp_results",[]),("first_scan_done",False),
               ("last_scan_pool_key","")]:
    if _k not in st.session_state: st.session_state[_k]=_v

st.set_page_config(layout="wide",page_title="Theta Turbo v5.3",page_icon="🔥",
                   initial_sidebar_state="collapsed")
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;600;800&display=swap');
:root{--bg:#080c10;--surface:#0d1117;--border:#1c2533;--accent:#00e5ff;
      --green:#00ff88;--red:#ff3d5a;--amber:#ffb700;--purple:#bf5fff;
      --orange:#ff7b00;--muted:#4a5568;--text:#c9d1d9;--heading:#e6edf3;}
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
.metric-card.green::before{background:var(--green);}.metric-card.red::before{background:var(--red);}
.metric-card.amber::before{background:var(--amber);}.metric-card.orange::before{background:var(--orange);}
.metric-card.purple::before{background:var(--purple);}
.metric-label{font-size:10px;color:var(--muted);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:4px;}
.metric-value{font-family:'Space Mono',monospace;font-size:24px;font-weight:700;color:var(--heading);line-height:1;}
.metric-sub{font-size:10px;color:var(--muted);margin-top:3px;}
.signal-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:12px;margin-bottom:20px;}
.signal-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px;position:relative;overflow:hidden;}
.signal-card.gacor{border-color:rgba(0,255,136,.4);background:rgba(0,255,136,.03);}
.signal-card.potensial{border-color:rgba(255,183,0,.3);background:rgba(255,183,0,.03);}
.signal-card.watch{border-color:rgba(0,229,255,.2);}
.signal-card.bagger{border-color:rgba(191,95,255,.6);background:rgba(191,95,255,.05);box-shadow:0 0 20px rgba(191,95,255,.15);}
.signal-card::after{content:'';position:absolute;top:0;left:0;width:4px;height:100%;}
.signal-card.gacor::after{background:var(--green);}.signal-card.potensial::after{background:var(--amber);}
.signal-card.watch::after{background:var(--accent);}.signal-card.bagger::after{background:var(--purple);}
.sc-ticker{font-family:'Space Mono',monospace;font-size:18px;font-weight:700;color:var(--heading);}
.sc-price{font-family:'Space Mono',monospace;font-size:13px;color:var(--muted);}
.sc-signal{font-size:13px;font-weight:700;margin:6px 0;}
.sc-bars{display:flex;gap:3px;margin:8px 0;}
.sc-bar{height:16px;border-radius:2px;}
.sc-bar.filled{background:var(--green);}.sc-bar.filled-purple{background:var(--purple);}.sc-bar.empty{background:var(--border);}
.sc-stats{display:flex;gap:12px;flex-wrap:wrap;margin-top:8px;}
.sc-stat{font-family:'Space Mono',monospace;font-size:10px;color:var(--muted);}.sc-stat span{color:var(--text);}
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
</style>""",unsafe_allow_html=True)

# ════ STOCK LIST ════
raw_stocks = [
    "AADI","AALI","ABBA","ABDA","ABMM","ACES","ACRO","ACST","ADCP","ADES","ADHI","ADMF","ADMG","ADMR","ADRO",
    "AEGS","AGAR","AGII","AGRO","AGRS","AHAP","AIMS","AISA","AKKU","AKPI","AKRA","AKSI","ALDO","ALII","ALKA",
    "ALMI","ALTO","AMAG","AMAN","AMAR","AMFG","AMIN","AMMN","AMMS","AMOR","AMRT","ANDI","ANJT","ANTM","APEX",
    "APIC","APII","APLI","APLN","ARCI","AREA","ARGO","ARII","ARKA","ARKO","ARMY","ARNA","ARTA","ARTI","ARTO",
    "ASBI","ASDM","ASGR","ASHA","ASII","ASJT","ASLI","ASLC","ASMI","ASPI","ASPR","ASRI","ASRM","ASSA","ATAP",
    "ATIC","ATLA","AUTO","AVIA","AWAN","AXIO","AYAM","AYLS","BABA","BABP","BABY","BACA","BAIK","BAJA","BALI",
    "BANK","BAPA","BAPI","BATA","BATR","BAUT","BAYU","BBCA","BBHI","BBKP","BBLD","BBMD","BBNI","BBRI","BBRM",
    "BBSI","BBSS","BBTN","BBYB","BCAP","BCIC","BCIP","BDKR","BDMN","BEBS","BEEF","BEER","BEKS","BELI","BELL",
    "BESS","BEST","BFIN","BGTG","BHAT","BHIT","BIAS","BIKA","BIKE","BIMA","BINA","BINO","BIPI","BIPP","BIRD",
    "BISI","BIWA","BJBR","BJTM","BKDP","BKSL","BKSW","BLES","BLOG","BLTA","BLTZ","BLUE","BMAS","BMBL","BMHS",
    "BMRI","BMSR","BMTR","BNBA","BNBR","BNGA","BNII","BNLI","BOAT","BOBA","BOGA","BOLA","BOLT","BOSS","BPFI",
    "BPII","BPTR","BRAM","BREN","BRIS","BRMS","BRNA","BRPT","BRRC","BSBK","BSDE","BSIM","BSML","BSSR","BSWD",
    "BTEK","BTEL","BTON","BTPN","BTPS","BUAH","BUDI","BUKA","BUKK","BULL","BUMI","BUVA","BVIC","BWPT","BYAN",
    "CAKK","CAMP","CANI","CARE","CARS","CASA","CASH","CASS","CBDK","CBPE","CBRE","CBUT","CBMF","CCSI","CDIA",
    "CEKA","CENT","CFIN","CGAS","CHEK","CHEM","CHIP","CINT","CITA","CITY","CLAY","CLEO","CLPI","CMNP","CMNT",
    "CMPP","CMRY","CNKO","CNMA","CNTX","COAL","COCO","COIN","COWL","CPIN","CPRI","CPRO","CRAB","CRSN","CSAP",
    "CSIS","CSMI","CSRA","CTBN","CTRA","CTTH","CUAN","CYBR","DAAZ","DADA","DART","DATA","DAYA","DCII","DEAL",
    "DEFI","DEPO","DEWA","DEWI","DFAM","DGNS","DGWG","DGIK","DIGI","DILD","DIVA","DKFT","DKHH","DLTA","DMAS",
    "DMMX","DMND","DNAR","DNET","DOID","DOOH","DOSS","DPNS","DPUM","DRMA","DSFI","DSNG","DSSA","DUCK","DUTI",
    "DVLA","DWGL","DYAN","EAST","ECII","EDGE","EKAD","ELIT","ELPI","ELSA","ELTY","EMAS","EMDE","EMTK","ENAK",
    "ENRG","ENVY","ENZO","EPAC","EPMT","ERAL","ERAA","ERTX","ESIP","ESSA","ESTA","ESTI","ETWA","EURO","EXCL",
    "FAPA","FAST","FASW","FILM","FIMP","FIRE","FISH","FITT","FLMC","FOLK","FOOD","FORE","FORU","FPNI","FUJI",
    "FUTR","FWCT","GAMA","GDST","GDYR","GEMA","GEMS","GGRP","GGRM","GHON","GIAA","GJTL","GLOB","GLVA","GMFI",
    "GMTD","GOLF","GOLD","GOLL","GOOD","GOTO","GPRA","GPSO","GRIA","GRPH","GRPM","GRII","GSMF","GTBO","GTRA",
    "GTSI","GULA","GUNA","GWSA","GZCO","HADE","HAIS","HAJJ","HALO","HATM","HBAT","HDFA","HDIT","HEAL","HELI",
    "HERO","HEXA","HGII","HILL","HITS","HKMU","HMSP","HOKI","HOME","HOMI","HOPE","HOTL","HRME","HRTA","HRUM",
    "HUMI","HYGN","IATA","IBFN","IBOS","IBST","ICBP","ICON","IDEA","IDPR","IFII","IFSH","IGAR","IIKP","IKAI",
    "IKAN","IKBI","IKPM","IMAS","IMJS","IMPC","INAF","INAI","INCF","INCI","INCO","INDF","INDO","INDR","INDS",
    "INDX","INDY","INET","INKP","INOV","INPC","INPP","INPS","INRU","INTA","INTD","INTP","IOTF","IPAC","IPCC",
    "IPCM","IPOL","IPPE","IPTV","IRRA","IRSX","ISAP","ISAT","ISEA","ISSP","ITIC","ITMA","ITMG","JAAS","JARR",
    "JAST","JATI","JAVA","JAYA","JECC","JGLE","JIHD","JKON","JMAS","JPFA","JRPT","JSKY","JSMR","JSPT","JTPE",
    "KAEF","KAQI","KARW","KARY","KAST","KAYU","KBAG","KBLI","KBLM","KBLV","KBRI","KDSI","KDTN","KEEN","KEJU",
    "KETR","KIAS","KICI","KIJA","KING","KINO","KIOS","KJEN","KKES","KKGI","KLAS","KLBF","KLIN","KMDS","KMTR",
    "KOBX","KOCI","KOIN","KOKA","KONI","KOPI","KOTA","KPIG","KRAH","KRAS","KREN","KSIX","KUAS","LABA","LABS",
    "LAJU","LAND","LAPD","LCGP","LCKM","LEAD","LFLO","LIFE","LINK","LION","LIVE","LMAS","LMPI","LMSH","LOPI",
    "LPCK","LPGI","LPIN","LPKR","LPLI","LPPF","LPPS","LRNA","LSIP","LTLS","LUCK","LUCY","MAAS","MABA","MADA",
    "MAGP","MAHA","MAIN","MANG","MAPA","MAPB","MAPI","MARI","MARK","MASA","MASB","MAYA","MBAP","MBMA","MBSS",
    "MBTO","MCAS","MCOL","MCOR","MDIA","MDKA","MDKI","MDLA","MDLN","MDRN","MEDC","MEDS","MEGA","MEJA","MENN",
    "MERI","MERK","META","MFMI","MGNA","MGRO","MHKI","MICE","MIDI","MIKA","MINA","MINE","MIRA","MITI","MKAP",
    "MKPI","MKTR","MLBI","MLIA","MLPL","MLPT","MMLP","MMIX","MNCN","MOLI","MORA","MPOW","MPMX","MPPA","MPRO",
    "MPXL","MRAT","MREI","MSIE","MSIN","MSJA","MSKY","MSTI","MTDL","MTEL","MTFN","MTLA","MTMH","MTPS","MTRA",
    "MTRN","MTSM","MTWI","MUTU","MYOH","MYOR","MYTX","NAIK","NANO","NASA","NASI","NATO","NAYZ","NCKL","NELY",
    "NEST","NETV","NICE","NICK","NICL","NIKL","NINE","NIRO","NISP","NOBU","NPGF","NRCA","NSSS","NTBK","NUSA",
    "NZIA","OASA","OBAT","OBMD","OCAP","OILS","OKAS","OLIV","OMED","OMRE","OPMS","PACK","PADA","PADI","PALM",
    "PAMG","PANI","PANR","PANS","PART","PBID","PBSA","PBRX","PCAR","PDES","PDPP","PEGE","PEHA","PELI","PENT",
    "PERW","PEVE","PGAS","PGEO","PGJO","PGLI","PGUN","PICO","PIPA","PJAA","PJHB","PKPK","PLAN","PLAS","PLIN",
    "PMJS","PMMP","PMUI","PNBN","PNBS","PNGO","PNIN","PNLF","PNSE","POLA","POLI","POLL","POLU","POLY","POOL",
    "PORT","POSA","POWR","PPGL","PPRI","PPRE","PPRO","PRAY","PRDA","PRIM","PSAB","PSAT","PSDN","PSGO","PSKT",
    "PSSI","PTBA","PTDU","PTIS","PTMP","PTMR","PTPP","PTPS","PTPW","PTRO","PTSN","PTSP","PUDP","PURA","PURE",
    "PURI","PWON","PYFA","PZZA","RAAM","RAFI","RAJA","RALS","RANC","RATU","RBMS","RCCC","RDTX","REAL","RELF",
    "RELI","REPP","RGAS","RICY","RIGS","RIMO","RISE","RLCO","RMBA","RMKE","RMKO","RMLP","ROCK","RODA","ROLI",
    "RONY","ROTI","RSCH","RSGK","RUIS","RUNS","SAFE","SAGE","SAGI","SAME","SAMF","SAMR","SAMP","SANO","SAPX",
    "SATU","SBAT","SBMA","SCCO","SCMA","SCNP","SCPI","SDMU","SDPC","SDRA","SEMA","SFAN","SGER","SGGH","SGJL",
    "SGRO","SHID","SHIP","SICO","SIDO","SIER","SILO","SIMA","SIMP","SINI","SIPD","SKBM","SKLT","SKRN","SKYB",
    "SLIS","SMAR","SMDM","SMDR","SMGA","SMGR","SMKM","SMKL","SMLE","SMMA","SMMT","SMRA","SMRU","SMSM","SNLK",
    "SOCI","SOFA","SOHO","SOLA","SONA","SOSS","SOTS","SOUL","SPMA","SPRE","SPTO","SQMI","SRAJ","SREI","SRIL",
    "SRSN","SRTG","SSIA","SSMS","SSTM","STAA","STAR","STRK","STTP","SUGI","SULI","SUNI","SUPA","SUPR","SURE",
    "SWAT","SWID","SYAI","TALF","TAMA","TAMU","TAPG","TARA","TAXI","TAYS","TBIG","TBLA","TBMS","TCID","TCPI",
    "TDPM","TEBE","TECH","TELE","TFAS","TFCO","TGKA","TGRA","TGUK","TIFA","TINS","TIRA","TIRT","TKIM","TLDN",
    "TLKM","TMAS","TMPO","TNCA","TOBA","TOOL","TOPS","TOSK","TOTL","TOTO","TOWR","TOYS","TPAI","TPIA","TPMA",
    "TRAM","TRGU","TRIL","TRIM","TRIN","TRIO","TRIS","TRJA","TRON","TRST","TRUE","TRUK","TRUS","TSPC","TUGU",
    "TULT","TYRE","UANG","UCID","UDNG","UFOE","ULTJ","UNIC","UNIQ","UNIT","UNSP","UNTR","UNVR","URBN","UVCR",
    "VAST","VATE","VCOK","VERN","VICI","VICO","VINS","VISA","VISI","VIVA","VKTR","VOKS","VOSS","VRNA","VTNY",
    "WAPO","WBSA","WEGE","WEHA","WGSH","WICO","WIDI","WIFI","WIIM","WIKA","WINE","WINR","WINS","WIRG","WITA",
    "WMPP","WMUU","WOMF","WONS","WOOD","WOWS","WPOW","WSBP","WSKT","WTON","YELO","YOII","YPAS","YULE","YUPI",
    "ZATA","ZBRA","ZENI","ZINC","ZONE","ZYRX",
]
seen=set(); raw_stocks=[x for x in raw_stocks if not(x in seen or seen.add(x))]
stocks_yf=[s+".JK" for s in raw_stocks]
stock_map={s+".JK":s for s in raw_stocks}

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

# ════ FETCH ENGINE v5.3 — ZERO RATE LIMIT ════
# yf.Ticker().history() = chart API = tidak kena rate limit
# yf.download() = bulk endpoint = DIHAPUS TOTAL

@st.cache_data(ttl=300,show_spinner=False)
def _fetch_ticker(ticker_yf,period="7d",interval="15m"):
    try:
        t=yf.Ticker(ticker_yf)
        df=t.history(period=period,interval=interval,auto_adjust=True,timeout=15)
        if df is None or df.empty: return None
        df.columns=[c.capitalize() for c in df.columns]
        req=["Open","High","Low","Close","Volume"]
        if any(c not in df.columns for c in req): return None
        df=df[req].dropna(subset=["Close"])
        df=df[df["Volume"]>0]
        return df if len(df)>=2 else None
    except: return None

def _fetch_parallel(tickers,period="7d",interval="15m",workers=5,delay=(0.1,0.4)):
    results={}; lock=__import__("threading").Lock()
    shuffled=list(tickers); random.shuffle(shuffled)
    def _one(t):
        time.sleep(random.uniform(*delay))
        df=_fetch_ticker(t,period,interval)
        if df is not None:
            with lock: results[t]=df
    with ThreadPoolExecutor(max_workers=workers) as exe:
        list(exe.map(_one,shuffled))
    return results

@st.cache_data(ttl=300,show_spinner=False)
def fetch_intraday(tickers,chunk=None):
    return _fetch_parallel(list(tickers),"7d","15m",workers=5)

@st.cache_data(ttl=3600,show_spinner=False)
def fetch_daily_bagger(tickers,chunk=None):
    return _fetch_parallel(list(tickers),"60d","1d",workers=5)

@st.cache_data(ttl=300,show_spinner=False)
def get_market_regime():
    try:
        df=_fetch_ticker("^JKSE","60d","1d")
        if df is None or len(df)<10: return("UNKNOWN",0,0,0,"Data IHSG kurang",0.0)
        close=df["Close"].dropna()
        ema20=float(close.ewm(span=20,adjust=False).mean().iloc[-1])
        ema55=float(close.ewm(span=min(55,len(close)-1),adjust=False).mean().iloc[-1])
        price=float(close.iloc[-1]); chg=float((close.iloc[-1]-close.iloc[-2])/close.iloc[-2]*100)
        band=0.012
        ae_any=price>ema20*(1-band); ae_clear=price>ema20*(1+band); ae55=price>ema55
        if ae_clear and ae55:   r="GREEN";   d=f"IHSG {price:,.0f} > EMA20 & EMA55 ✅"
        elif ae_any and ae55:   r="GREEN";   d=f"IHSG {price:,.0f} dekat EMA20"
        elif ae_any and not ae55:r="SIDEWAYS";d=f"IHSG {price:,.0f} > EMA20 tapi < EMA55"
        elif not ae_any and chg>0.3:r="SIDEWAYS";d=f"IHSG {price:,.0f} recovery {chg:+.2f}%"
        elif chg<-0.3 and not ae_any:r="RED";d=f"IHSG {price:,.0f} < EMA20 + turun"
        else:                   r="SIDEWAYS";d=f"IHSG {price:,.0f} konsolidasi"
        return(r,price,ema20,ema55,d,chg)
    except Exception as e: return("UNKNOWN",0,0,0,f"Error:{str(e)[:40]}",0.0)

@st.cache_data(ttl=3600,show_spinner=False)
def fetch_pivot_data(ticker_yf):
    try:
        df=_fetch_ticker(ticker_yf,"5d","1d")
        if df is None or len(df)<2: return None
        p=df.iloc[-2]; h,l,c=float(p["High"]),float(p["Low"]),float(p["Close"])
        pp=(h+l+c)/3
        return{"PP":pp,"R1":2*pp-l,"R2":pp+(h-l),"R3":h+2*(pp-l),"S1":2*pp-h,"S2":pp-(h-l),"S3":l-2*(h-pp)}
    except: return None

@st.cache_data(ttl=600,show_spinner=False)
def fetch_all_sectors(sectors_dict,top_n=10,is_jk=True):
    suffix=".JK" if is_jk else ""
    all_t=list({s+suffix for stks in sectors_dict.values() for s in stks[:top_n]})
    raw=_fetch_parallel(all_t,"5d","1d",workers=5)
    sec_data={}
    for sec_name,sec_stocks in sectors_dict.items():
        res=[]
        for s in sec_stocks[:top_n]:
            df=raw.get(s+suffix)
            if df is None or len(df)<2: continue
            try:
                c=float(df["Close"].iloc[-1]); p=float(df["Close"].iloc[-2])
                chg=(c-p)/p*100; vol=float(df["Volume"].iloc[-1]); avgv=float(df["Volume"].mean())
                res.append({"ticker":s,"close":c,"chg":round(chg,2),"rvol":round(vol/avgv if avgv>0 else 1,2)})
            except: continue
        if not res: continue
        sec_data[sec_name]={"avg_chg":round(sum(r["chg"] for r in res)/len(res),2),
            "avg_rvol":round(sum(r["rvol"] for r in res)/len(res),2),
            "bullish":sum(1 for r in res if r["chg"]>0),"total":len(res),"stocks":res}
    return sec_data

@st.cache_data(ttl=3600,show_spinner=False)
def calc_sector_beta(sector_name,sector_stocks,lookback=20):
    try:
        di=_fetch_ticker("^JKSE","60d","1d")
        if di is None or len(di)<lookback: return None
        ir=di["Close"].pct_change().dropna()
        raw=_fetch_parallel([s+".JK" for s in sector_stocks[:8]],"60d","1d",workers=4)
        sr=[df["Close"].pct_change().dropna() for df in raw.values() if df is not None and len(df)>=lookback]
        if not sr: return None
        sa=pd.concat(sr,axis=1).mean(axis=1)
        al=pd.concat([ir,sa],axis=1).dropna(); al.columns=["IHSG","Sektor"]
        if len(al)<10: return None
        cov=al["Sektor"].cov(al["IHSG"]); var=al["IHSG"].var()
        beta=round(cov/var,2) if var>0 else 1.0
        return{"sector":sector_name,"beta":beta,"corr":round(al["Sektor"].corr(al["IHSG"]),2),
               "rs5":round((al["Sektor"].tail(5).sum()-al["IHSG"].tail(5).sum())*100,2),
               "ret_1m_sec":round(al["Sektor"].tail(20).sum()*100,2),
               "avg_down":round(al[al["IHSG"]<-0.005]["Sektor"].mean()*100,2),
               "defensive":beta<0.8}
    except: return None

@st.cache_data(ttl=300,show_spinner=False)
def scan_gap_up(tickers_yf,min_gap_pct=0.5):
    raw=_fetch_parallel(list(tickers_yf),"5d","1d",workers=6)
    results=[]
    for t,df in raw.items():
        tkr=t.replace(".JK","")
        try:
            if df is None or len(df)<3: continue
            td=df.iloc[-1]; pv=df.iloc[-2]
            cl=float(td["Close"]); ht=float(td["High"]); lt=float(td["Low"])
            hp=float(pv["High"]); vol=float(td["Volume"]); avgv=float(df["Volume"].mean())
            rvol=vol/avgv if avgv>0 else 1.0
            gs=0; reasons=[]
            if cl>hp: gs+=3; reasons.append(f"Gap {(cl-hp)/hp*100:.1f}% above prev High ✦✦")
            cr=(cl-lt)/max(ht-lt,1)
            if cr>0.85: gs+=2; reasons.append(f"Tutup dekat High {cr:.0%}")
            elif cr>0.70: gs+=1; reasons.append(f"Tutup kuat {cr:.0%}")
            if rvol>3.0: gs+=2; reasons.append(f"RVOL={rvol:.1f}x SURGE 🔥")
            elif rvol>2.0: gs+=1; reasons.append(f"RVOL={rvol:.1f}x")
            elif rvol>1.5: gs+=0.5
            if len(df)>=3:
                c3=(cl-float(df.iloc[-3]["Close"]))/float(df.iloc[-3]["Close"])*100
                if c3>3: gs+=1; reasons.append(f"3D ROC +{c3:.1f}%")
                elif c3>1: gs+=0.5
            if gs<3: continue
            results.append({"Ticker":tkr,"Price":int(cl),"Gap Score":round(gs,1),
                "Chg %":round((cl-float(pv["Close"]))/float(pv["Close"])*100,2),
                "Close Ratio":round(cr,2),"RVOL":round(rvol,2),"Prev High":int(hp),
                "Signal":"GAP UP 🚀" if gs>=4 else "POTENTIAL ⚡",
                "Reasons":" · ".join(reasons[:3])})
        except: continue
    return sorted(results,key=lambda x:x["Gap Score"],reverse=True)

def fetch_mtf(ticker_yf):
    result={}
    df15=_fetch_ticker(ticker_yf,"7d","15m")
    if df15 is None or len(df15)<10: return result
    result["M15"]=df15
    for rs,rk,mb in [("1h","H1",10),("1D","D1",3)]:
        try:
            dr=df15.resample(rs).agg({"Open":"first","High":"max","Low":"min","Close":"last","Volume":"sum"}).dropna(subset=["Close"])
            dr=dr[dr["Volume"]>0]
            if len(dr)>=mb: result[rk]=dr
        except: pass
    return result

def get_regime_config(regime):
    return{"RED":{"mode":"Reversal 🎯","min_score":5,"min_rvol":2.0,"sl_mult":0.6,
                  "label":"🔴 MARKET MERAH — Reversal Only, Score ≥ 5","color":"#ff3d5a",
                  "desc":"Market bearish. Fokus reversal oversold."},
           "GREEN":{"mode":"Bagger 💎","min_score":4,"min_rvol":1.5,"sl_mult":0.8,
                    "label":"🟢 MARKET HIJAU — Wyckoff Bagger Hunt (Daily TF)","color":"#00ff88",
                    "desc":"Market bullish. Cari akumulasi Wyckoff di chart harian."},
           "SIDEWAYS":{"mode":"Scalping ⚡","min_score":4,"min_rvol":2.0,"sl_mult":0.7,
                       "label":"🟡 MARKET SIDEWAYS — Scalping, RVOL ≥ 2x","color":"#ffb700",
                       "desc":"Market sideways. RVOL harus lebih kuat."},
           "UNKNOWN":{"mode":"Scalping ⚡","min_score":4,"min_rvol":1.5,"sl_mult":0.8,
                      "label":"⚪ REGIME UNKNOWN — Manual Mode","color":"#4a5568","desc":""},
    }.get(regime,{"mode":"Scalping ⚡","min_score":4,"min_rvol":1.5,"sl_mult":0.8,"label":"⚪","color":"#4a5568","desc":""})

# ════ INDICATORS ════
def ema(s,n): return s.ewm(span=n,adjust=False).mean()
def rsi_smooth(s,p=14,sm=3):
    d=s.diff(); g=d.clip(lower=0).rolling(p).mean(); l=(-d.clip(upper=0)).rolling(p).mean()
    rs=g/l.replace(0,np.nan); raw=100-100/(1+rs); return raw,ema(raw,sm)
def stochastic(h,l,c,k=14,d=3):
    ll=l.rolling(k).min(); hh=h.rolling(k).max()
    K=100*(c-ll)/(hh-ll).replace(0,np.nan); D=K.rolling(d).mean()
    return K.fillna(50),D.fillna(50)
def macd(s,f=12,sl=26,sg=9):
    ml=ema(s,f)-ema(s,sl); sig=ema(ml,sg); return ml,sig,ml-sig
def vwap(df):
    tp=(df["High"]+df["Low"]+df["Close"])/3; return(tp*df["Volume"]).cumsum()/df["Volume"].cumsum()

def apply_indicators(df):
    if isinstance(df.columns,pd.MultiIndex): df.columns=df.columns.droplevel(-1)
    df["EMA9"]=ema(df["Close"],9);   df["EMA21"]=ema(df["Close"],21)
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
    df["RVOL"]=df["Volume"]/df["AvgVol"].replace(0,np.nan)
    df["NetVol"]=np.where(df["Close"]>=df["Open"],df["Volume"],-df["Volume"])
    df["NetVol3"]=pd.Series(df["NetVol"],index=df.index).rolling(3).sum()
    df["NetVol8"]=pd.Series(df["NetVol"],index=df.index).rolling(8).sum()
    df["VolSpike"]=df["RVOL"]>2.5
    df["Body"]=(df["Close"]-df["Open"]).abs()
    df["BodyRatio"]=df["Body"]/(df["High"]-df["Low"]).replace(0,np.nan)
    df["BullBar"]=(df["Close"]>df["Open"])&(df["BodyRatio"]>0.5)
    df["ROC3"]=df["Close"].pct_change(3); df["ROC8"]=df["Close"].pct_change(8)
    df["HH"]=df["High"]>df["High"].shift(1);  df["HL"]=df["Low"]>df["Low"].shift(1)
    df["LL"]=df["Low"]<df["Low"].shift(1);    df["LH"]=df["High"]<df["High"].shift(1)
    tr=pd.concat([df["High"]-df["Low"],(df["High"]-df["Close"].shift()).abs(),(df["Low"]-df["Close"].shift()).abs()],axis=1).max(axis=1)
    df["ATR"]=tr.rolling(14).mean()
    return df

apply_intraday_indicators=apply_indicators

def get_pivot_position(price,pivots):
    if pivots is None: return "Unknown","#4a5568"
    pp=pivots["PP"]
    if price>pivots["R2"]:   return "Above R2 🔴","#ff3d5a"
    elif price>pivots["R1"]: return "R1→R2 🟠","#ff7b00"
    elif price>pp:           return "PP→R1 🟢","#00ff88"
    elif price>pivots["S1"]: return "S1→PP 🟡","#ffb700"
    elif price>pivots["S2"]: return "S2→S1 🔴","#ff3d5a"
    else:                    return "Below S2 🔴","#ff3d5a"

def get_beta_label(beta):
    if beta<0.5:   return "Defensif 🛡️","#00e5ff"
    elif beta<0.8: return "Low Beta","#00ff88"
    elif beta<1.2: return "Market Beta","#ffb700"
    elif beta<1.5: return "High Beta","#ff7b00"
    else:          return "Agresif 🔥","#ff3d5a"

def mtf_alignment(scores):
    if not scores: return "No Data","#4a5568",0
    vals=list(scores.values()); avg=sum(vals)/len(vals); bc=sum(1 for v in vals if v>=4)
    if bc==len(vals):  return "FULL ALIGN 🔥","#00ff88",avg
    elif bc>=2:        return "PARTIAL ⚡","#ffb700",avg
    elif bc==1:        return "MIXED ⚠️","#ff7b00",avg
    else:              return "NO ALIGN ❌","#ff3d5a",avg

def calc_trailing_stop(entry,current,atr,method="ATR",atr_mult=2.0,pct=3.0):
    if method=="ATR":      td=atr*atr_mult; sp=current-td
    elif method=="Persen": td=current*(pct/100); sp=current*(1-pct/100)
    else:                  td=atr*1.5; sp=current-td
    return{"stop":round(sp,0),"distance":round(td,0),
           "profit_float":round((current-entry)/entry*100,2),
           "profit_locked":round((sp-entry)/entry*100,2) if sp>entry else 0,
           "is_profitable":sp>entry}

def calc_pivot_points(high,low,close):
    pp=(high+low+close)/3; r1=2*pp-low; r2=pp+(high-low); r3=high+2*(pp-low)
    s1=2*pp-high; s2=pp-(high-low); s3=low-2*(high-pp)
    return{"PP":pp,"R1":r1,"R2":r2,"R3":r3,"S1":s1,"S2":s2,"S3":s3}

# ════ SCORING ════
def score_scalping(r,p,p2):
    s=0; rs=[]
    if r["EMA9"]>r["EMA21"]>r["EMA50"]: s+=1.5;rs.append("EMA stack ▲")
    elif r["EMA9"]>r["EMA21"]:           s+=0.8;rs.append("EMA9>21")
    if r["Close"]>r["VWAP"]:             s+=1;  rs.append("Above VWAP")
    mh=r["MACD_Hist"]; pmh=float(p["MACD_Hist"])
    if mh>0 and mh>pmh:
        s+=1.5;rs.append("MACD hist expanding ✦")
        if p2 is not None and pmh>float(p2["MACD_Hist"]): s+=0.3
    elif mh>0: s+=0.5;rs.append("MACD hist +")
    re=float(r["RSI_EMA"])
    if 52<re<68: s+=0.8;rs.append(f"RSI-EMA={re:.1f}")
    elif re>=68: s-=0.5
    rv=float(r["RVOL"])
    if rv>2.0: s+=1;rs.append(f"RVOL={rv:.1f}x surge")
    elif rv>1.5: s+=0.6;rs.append(f"RVOL={rv:.1f}x")
    if bool(r["BullBar"]): s+=0.5;rs.append("Bullish bar")
    if float(r["NetVol3"])>0: s+=0.4;rs.append("Net vol +")
    if r["Close"]<r["EMA200"]*0.98: s-=0.5
    return max(0,min(6,round(s,1))),rs,{}

def score_momentum(r,p,p2):
    s=0; rs=[]
    if bool(r["HH"]) and bool(r["HL"]): s+=1.5;rs.append("HH+HL pattern ▲")
    elif bool(r["HH"]): s+=0.8
    rv=float(r["RVOL"])
    if rv>3.0: s+=1.5;rs.append(f"RVOL={rv:.1f}x SURGE 🔥")
    elif rv>2.0: s+=1.0;rs.append(f"RVOL={rv:.1f}x")
    elif rv>1.5: s+=0.5
    roc=float(r["ROC3"])*100
    if roc>2.0: s+=1.5;rs.append(f"ROC3={roc:.1f}%")
    elif roc>1.0: s+=0.8;rs.append(f"ROC3={roc:.1f}%")
    elif roc<0: s-=0.5
    re=float(r["RSI_EMA"])
    if 55<re<75: s+=0.8;rs.append(f"RSI-EMA={re:.1f}")
    if re>78: s-=0.8;rs.append("⚠️ RSI overbought")
    sk=float(r["STOCH_K"]); sd=float(r["STOCH_D"])
    if sk>60 and sk>sd: s+=0.8;rs.append("STOCH K>D bullish")
    if r["MACD_Hist"]>0 and r["MACD_Hist"]>float(p["MACD_Hist"]): s+=0.8;rs.append("MACD expanding")
    if r["Close"]>r["VWAP"]: s+=0.5;rs.append("Above VWAP")
    return max(0,min(6,round(s,1))),rs,{}

def score_reversal(r,p,p2):
    s=0; rs=[]; osc=0
    re=float(r["RSI_EMA"])
    if re<30:   osc+=1;s+=1.5;rs.append(f"RSI-EMA={re:.1f} OS extreme")
    elif re<40: osc+=1;s+=0.8;rs.append(f"RSI-EMA={re:.1f} OS")
    sk=float(r["STOCH_K"]); sd=float(r["STOCH_D"])
    if sk<20:   osc+=1;s+=1;  rs.append(f"STOCH={sk:.0f} extreme OS")
    elif sk<30: osc+=1;s+=0.5
    bp=float(r["BB_pct"])
    if bp<0.05:  osc+=1;s+=1; rs.append("BB lower touch")
    elif bp<0.15:osc+=1;s+=0.5
    if osc<1.5: return 0,[],{}
    rev=0; pk=float(p["STOCH_K"]); pd_=float(p["STOCH_D"])
    if sk<30 and sk>sd and pk<=pd_:   rev+=1;s+=2;  rs.append("STOCH %K cross ↑ OS ✦✦")
    elif sk<25 and sk>sd:             rev+=1;s+=1.2;rs.append("STOCH K>D extreme OS")
    rp=float(p["RSI_EMA"])
    if re>rp and re<42: rev+=1;s+=1.2;rs.append("RSI-EMA pivot ↑")
    mh=float(r["MACD_Hist"]); mph=float(p["MACD_Hist"])
    if mh>mph and mh<0: rev+=1;s+=0.8;rs.append("MACD hist diverge ↑")
    if rev==0: s*=0.3
    if bool(r["VolSpike"]) and float(r["Close"])<float(r["Open"]): s+=0.8;rs.append("Volume climax sell")
    elif float(r["RVOL"])>1.5: s+=0.4
    if float(r["NetVol3"])>0: s+=0.5;rs.append("Net vol turning +")
    if float(r["BodyRatio"])>0.75 and float(r["Close"])<float(r["Open"]): s-=0.8
    return max(0,min(6,round(s,1))),rs,{}

def score_bagger(r,p,p2,df_full):
    s=0; rs=[]; cl=float(r["Close"]); rv=float(r["RVOL"]); re=float(r["RSI_EMA"])
    e9=float(r["EMA9"]); e21=float(r["EMA21"]); e50=float(r["EMA50"]); e200=float(r["EMA200"])
    wp="SCANNING"; is_sw=False; rh=cl*1.05; rl=cl*0.95; swb=min(20,len(df_full)-2)
    try:
        rh=float(df_full["High"].iloc[-swb-1:-1].max()); rl=float(df_full["Low"].iloc[-swb-1:-1].min())
        rp=(rh-rl)/max(rl,0.01)*100; is_sw=rp<10.0
        if is_sw: s+=1.0+max(0,(10-rp)/10)*0.5; rs.append(f"Sideways {rp:.1f}% ({swb}d) ✦"); wp="A-B"
    except: pass
    try:
        vm20=float(df_full["AvgVol"].iloc[-1]); vl5=float(df_full["Volume"].iloc[-6:-1].mean())
        dr=vl5/max(vm20,1)
        if dr<0.5 and is_sw:    s+=2.0;rs.append(f"Dry vol {dr:.2f}x stealth accum ✦✦");wp="A-B AKUMULASI"
        elif dr<0.7 and is_sw:  s+=1.2;rs.append(f"Vol drying {dr:.2f}x ✦");wp="A-B AKUMULASI"
        elif dr<0.85 and is_sw: s+=0.6;rs.append(f"Vol below avg {dr:.2f}x")
    except: pass
    try:
        if len(df_full)>=12:
            nv=[float(df_full["NetVol"].iloc[i]) for i in range(-11,-1)]
            np_=sum(1 for v in nv if v>0); nr=np_/10
            if nr>=0.7 and is_sw:  s+=1.5;rs.append(f"Stealth net buy {np_}/10d ✦✦")
            elif nr>=0.6:          s+=0.8;rs.append(f"Net buy {np_}/10d")
            elif nr>=0.5:          s+=0.4
    except:
        if float(r["NetVol3"])>0 and float(r["NetVol8"])>0: s+=0.8;rs.append("Net buyer sustained ✦")
        elif float(r["NetVol3"])>0: s+=0.3
    try:
        bc=float(r["BB_std"]); ba=float(df_full["BB_std"].iloc[-11:-1].mean())
        sq=bc/max(ba,0.0001)
        if sq<0.7 and is_sw:  s+=1.5;rs.append(f"BB squeeze {sq:.2f}x ✦✦")
        elif sq<0.85:         s+=0.8;rs.append(f"BB squeeze {sq:.2f}x")
    except: pass
    spd=False
    try:
        lb=min(15,len(df_full)-3); pl=df_full["Low"].iloc[-lb-2:-2]
        sup=float(pl.min()); bl=float(r["Low"]); bc_=float(r["Close"]); bh=float(r["High"])
        if bl<sup and bc_>sup:
            rc=(bc_-bl)/max(bh-bl,0.0001)
            if rc>0.7 and rv>1.2:  s+=3.0;rs.append(f"🔥 SPRING! {rc:.0%} rebound ✦✦✦");wp="SPRING ⚡";spd=True
            elif rc>0.5:           s+=1.8;rs.append(f"Spring ({rc:.0%}) ✦✦");wp="SPRING";spd=True
        if float(p["Low"])<sup and float(p["Close"])>sup and bc_>float(p["Close"]) and not spd:
            s+=2.0;rs.append("Post-spring confirmation 🚀 ✦✦");wp="POST-SPRING";spd=True
    except: pass
    try:
        ar=cl>rh*0.998; tb=float(r["BodyRatio"])>0.55; bb=float(r["Close"])>float(r["Open"])
        if rv>3.0 and ar and tb and bb:  s+=3.0;rs.append(f"🚀 PHASE D! RVOL={rv:.1f}x ✦✦✦");wp="PHASE D 🚀"
        elif rv>2.0 and ar and bb:       s+=2.2;rs.append(f"Breakout RVOL={rv:.1f}x ✦✦");wp="BREAKOUT ✦"
        elif rv>1.5 and ar:              s+=1.5;rs.append(f"Breakout attempt RVOL={rv:.1f}x")
        elif ar:                         s+=0.8;rs.append("Above resistance")
        else:
            if rv>3.0:   s+=1.2;rs.append(f"RVOL={rv:.1f}x SURGE 🔥")
            elif rv>2.0: s+=0.8;rs.append(f"RVOL={rv:.1f}x")
            elif rv>1.5: s+=0.4
            elif rv<1.0 and wp not in ["A-B AKUMULASI","SPRING","POST-SPRING"]: s-=0.5
    except:
        if rv>3.0:   s+=1.2;rs.append(f"RVOL={rv:.1f}x SURGE 🔥")
        elif rv>2.0: s+=0.8
        elif rv>1.5: s+=0.4
    if e9>e21>e50>e200: s+=1.5;rs.append("EMA golden stack ✦✦")
    elif e9>e21>e50:    s+=1.0;rs.append("EMA stack ▲")
    elif e9>e21:        s+=0.4
    if wp in ["A-B","A-B AKUMULASI","SPRING","POST-SPRING"]:
        if 25<=re<=52:  s+=1.0;rs.append(f"RSI-EMA={re:.1f} accum zone ✓")
        elif re<25:     s+=0.6
        elif re>65:     s-=0.3
    else:
        if 52<re<72:   s+=1.0;rs.append(f"RSI-EMA={re:.1f} momentum")
        elif re>=72:   s-=0.5;rs.append(f"⚠️ RSI OB {re:.1f}")
        elif re<40:    s-=0.3
    if cl>float(r["VWAP"]): s+=0.5;rs.append("Above VWAP")
    if e200>0 and cl<e200*0.88: s-=1.0
    try:
        if len(df_full)>=4:
            bc2=sum(1 for i in range(-3,0) if float(df_full["Close"].iloc[i])>float(df_full["Open"].iloc[i]))
            if bc2==3: s+=0.8;rs.append("3x consecutive bull bar")
            elif bc2==2: s+=0.3
    except: pass
    if wp!="SCANNING": rs.insert(0,f"⚙️ Wyckoff: {wp}")
    return max(0,min(6,round(s,1))),rs,{"wyckoff_phase":wp}

def score_bsjp(r,p,p2):
    s=0; rs=[]
    hi_lo=float(r["High"])-float(r["Low"]); cp=(float(r["Close"])-float(r["Low"]))/max(hi_lo,1)
    if cp>0.7:   s+=2;  rs.append(f"Tutup dekat High ({cp:.0%})")
    elif cp>0.5: s+=1;  rs.append(f"Tutup kuat ({cp:.0%})")
    rv=float(r["RVOL"])
    if rv>3.0:   s+=2;  rs.append(f"RVOL={rv:.1f}x SURGE 🔥")
    elif rv>2.0: s+=1.5;rs.append(f"RVOL={rv:.1f}x kuat")
    elif rv>1.5: s+=0.8;rs.append(f"RVOL={rv:.1f}x")
    if r["EMA9"]>r["EMA21"]>r["EMA50"]:   s+=1.5;rs.append("EMA stack ▲")
    elif r["EMA9"]>r["EMA21"]:             s+=0.8;rs.append("EMA9>21")
    re=float(r["RSI_EMA"])
    if 45<re<70:  s+=1;  rs.append(f"RSI-EMA={re:.1f} ✓")
    elif re>=70:  s-=1;  rs.append(f"⚠️ RSI OB {re:.1f}")
    elif re<40:   s+=0.5;rs.append(f"RSI-EMA={re:.1f} oversold")
    if float(r["MACD_Hist"])>0 and float(r["MACD_Hist"])>float(p["MACD_Hist"]):
        s+=1;rs.append("MACD hist expanding ✦")
    elif float(r["MACD_Hist"])>0: s+=0.5;rs.append("MACD +")
    if float(r["Close"])>float(r["VWAP"]): s+=0.5;rs.append("Above VWAP")
    if float(r["NetVol8"])>0: s+=0.5;rs.append("Net buyer 8 bar ✦")
    elif float(r["NetVol3"])>0: s+=0.3
    return max(0,min(6,round(s,1))),rs,{}

def get_signal(score,mode):
    t={"Scalping ⚡":{"5":"GACOR ⚡","4":"POTENSIAL 🔥","3":"WATCH 👀"},
       "Momentum 🚀":{"5":"GACOR 🚀","4":"POTENSIAL 🔥","3":"WATCH 👀"},
       "Reversal 🎯":{"5":"REVERSAL 🎯","4":"POTENSIAL 🔥","3":"WATCH 👀"},
       "Bagger 💎":  {"5":"BAGGER 💎","4":"KANDIDAT 🚀","3":"WATCH 👀"}}.get(mode,{})
    for th in sorted([float(k) for k in t.keys()],reverse=True):
        if score>=th: return t[str(int(th))]
    return "WAIT"

def get_card_class(sig):
    if "BAGGER" in sig or "KANDIDAT" in sig: return "bagger"
    if "GACOR"  in sig or "REVERSAL" in sig: return "gacor"
    if "POTENSIAL" in sig: return "potensial"
    if "WATCH"  in sig:    return "watch"
    return ""

def send_telegram(results_top,source="Scanner"):
    if not TOKEN or not CHAT_ID: return
    now=datetime.now(jakarta_tz); is_open=9<=now.hour<16; sep="━"*28
    mkt_s="🔴 MARKET OPEN" if is_open else "🌙 AFTER HOURS"
    src_s="WATCHLIST" if source=="Watchlist" else "ALERT"
    hdr=(f"{mkt_s}\n"
         f"🔥 *THETA TURBO {src_s}*\n"
         f"⏰ `{now.strftime('%H:%M:%S')} WIB` · `{now.strftime('%d %b %Y')}`\n{sep}\n")
    body=""
    for r in results_top[:5]:
        sig=r.get("Signal","-")
        em="💎" if "BAGGER" in sig else("🏆" if("GACOR" in sig or "REVERSAL" in sig) else("🔥" if "POTENSIAL" in sig else "👀"))
        te="📈" if "▲" in r.get("Trend","") else("📉" if "▼" in r.get("Trend","") else "➡️")
        bar="█"*int(r["Score"])+"░"*(6-int(r["Score"]))
        tfl=" [D1]" if r.get("TF","")=="Daily" else " [15M]"
        body+=(f"\n{em} *{r['Ticker']}*  `{sig}`{tfl}\n"
               f"   💰 Price: `{r['Price']:,}` {te}\n"
               f"   📊 Score: `[{bar}] {r['Score']}/6`\n"
               f"   📈 RSI-EMA: `{r.get('RSI-EMA',0)}` | RVOL: `{r.get('RVOL',0)}x`\n"
               f"   🎯 TP: `{r['TP']:,}` | 🛑 SL: `{r['SL']:,}` | R:R `{r['R:R']}`\n"
               f"   💡 _{r.get('Reasons','')[:60]}_\n")
    footer=f"\n{sep}\n⚡ _Theta Turbo v5.3 · Zero Rate Limit · IDX_\n⚠️ _BUKAN saran investasi. DYOR!_"
    try:
        requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                      data={"chat_id":CHAT_ID,"text":hdr+body+footer,"parse_mode":"Markdown"},timeout=10)
    except: pass

# ════ HEADER ════
regime,ihsg_price,ema20,ema55,regime_detail,ihsg_chg=get_market_regime()
rcfg=get_regime_config(regime); rcolor=rcfg["color"]
chg_col="#00ff88" if ihsg_chg>=0 else "#ff3d5a"; chg_sym="▲" if ihsg_chg>=0 else "▼"
now_jkt=datetime.now(jakarta_tz)

st.markdown(f"""
<div class="tt-header">
  <div><div class="tt-logo">🔥 THETA TURBO</div>
  <div class="tt-sub">Intraday 15M + Daily Bagger · Wyckoff · Auto Regime · v5.3</div></div>
  <div class="live-badge"><div class="live-dot"></div>📊 yF Chart API · LIVE {now_jkt.strftime("%H:%M:%S")} WIB</div>
</div>""",unsafe_allow_html=True)

st.markdown(f"""
<div style="background:rgba(0,0,0,.4);border:1px solid {rcolor}44;border-radius:8px;padding:12px 16px;margin-bottom:14px;border-left:4px solid {rcolor};">
  <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;">
    <div><div style="font-family:Space Mono,monospace;font-size:12px;font-weight:700;color:{rcolor};letter-spacing:1px;">{rcfg["label"]}</div>
      <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-top:3px;">{rcfg["desc"]}</div></div>
    <div style="text-align:right;font-family:Space Mono,monospace;">
      <div style="font-size:18px;font-weight:700;color:{rcolor};">{ihsg_price:,.0f} <span style="font-size:11px;color:{chg_col}">{chg_sym}{abs(ihsg_chg):.2f}%</span></div>
      <div style="font-size:9px;color:#4a5568;">EMA20 {ema20:,.0f} · EMA55 {ema55:,.0f}</div>
    </div>
  </div>
</div>""",unsafe_allow_html=True)

tab_scanner,tab_watchlist,tab_bsjp,tab_sector,tab_gapup,tab_trail,tab_backtest=st.tabs(
    ["🔥 Scanner","👁️ Watchlist","🌙 BSJP","🏭 Sektor","📈 Gap Up","🎯 Trailing Stop","📊 Backtest"])

# ════ TAB SCANNER ════
with tab_scanner:
    with st.expander("⚙️ Scanner Settings",expanded=False):
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
                scan_mode=st.radio("Mode",_opts,index=_opts.index(_prev) if _prev in _opts else 0,label_visibility="collapsed",key="smr")
            st.session_state.active_scan_mode=scan_mode
            tele_on=st.toggle("📡 Telegram Alert",value=True,key="tele_on")
        with sc2:
            st.markdown('<div class="settings-label">FILTER</div>',unsafe_allow_html=True)
            auto_thresh=st.toggle("🤖 Auto-Threshold",value=True,key="auto_thr")
            if auto_thresh:
                min_score=rcfg["min_score"]; vol_thresh=rcfg["min_rvol"]
                st.caption(f"Auto: Score≥{min_score} · RVOL≥{vol_thresh}x")
            else:
                min_score=st.slider("Min Score",0,6,4,key="msc")
                vol_thresh=st.slider("Min RVOL",1.0,5.0,1.5,0.1,key="vol")
            min_turn=st.number_input("Min Turnover (M Rp)",value=500,step=100,key="trn")*1_000_000
        with sc3:
            st.markdown('<div class="settings-label">TAMPILAN</div>',unsafe_allow_html=True)
            view_mode=st.radio("View",["Card View 🃏","Table View 📊"],label_visibility="collapsed",key="vm")
            scan_size=st.radio("Scan Size",["LQ45 ⚡ (45)","200 🔥","Full 🦅"],index=1,horizontal=True,key="ss")
            if scan_mode=="Bagger 💎":
                st.markdown('<div style="font-family:Space Mono,monospace;font-size:9px;color:#bf5fff;padding:4px 8px;background:rgba(191,95,255,.1);border-radius:4px;">📅 Bagger: Daily TF</div>',unsafe_allow_html=True)
            st.caption(f"🎯 {regime} · {scan_mode} · {len(raw_stocks)} emiten")

    _pool_key=st.session_state.get("ss","200 🔥")
    if not st.session_state.first_scan_done or st.session_state.last_scan_pool_key!=_pool_key:
        st.session_state.first_scan_done=True; st.session_state.last_scan_pool_key=_pool_key
        if not st.session_state.scan_results: st.session_state._afr=True

    do_scan=st.button("🔥 MULAI SCAN SEKARANG",type="primary",use_container_width=True,key="btn_scan")
    if st.session_state.get("_afr") and not do_scan: do_scan=True; st.session_state._afr=False
    _nc=datetime.now(jakarta_tz).timestamp()
    if st.session_state.last_scan_time and not do_scan:
        if _nc-st.session_state.last_scan_time>=300 and st.session_state.scan_results:
            do_scan=True; scan_mode=st.session_state.get("active_scan_mode",scan_mode)

    if do_scan:
        _LQ45=[s+".JK" for s in ["BBCA","BBRI","BMRI","TLKM","ASII","GOTO","BYAN","MDKA","UNVR","ICBP","INDF","KLBF","SIDO","MNCN","EXCL","TOWR","PGAS","PTBA","ADRO","ITMG","INCO","HRUM","JSMR","SMGR","WIKA","WSKT","ANTM","PTPP","TBIG","BRIS","AMMN","MBMA","CUAN","BBNI","BBTN","BMTR","INKP","BRPT","TPIA","BREN","EMTK","PANI","DSSA","ISAT","PGAS"]]
        _sz=st.session_state.get("ss","200 🔥")
        if "45" in _sz:    sl=_LQ45
        elif "200" in _sz: sl=stocks_yf[:200]
        else:              sl=stocks_yf
        is_bag=(scan_mode=="Bagger 💎")
        ph=st.empty(); pb=st.progress(0)
        ph.markdown(f'<div style="color:#ff7b00;font-family:Space Mono,monospace;font-size:12px;">🔥 SCANNING {len(sl)} saham · {scan_mode} · {"📅 DAILY" if is_bag else "⚡ 15M"}</div>',unsafe_allow_html=True)
        try:
            data_dict=fetch_daily_bagger(tuple(sl)) if is_bag else fetch_intraday(tuple(sl))
            st.session_state.data_dict=data_dict; nf=len(data_dict)
            if nf==0: ph.empty();pb.empty();st.warning("⚠️ Tidak ada data.")
            else:
                ph.markdown(f'<div style="color:#00ff88;font-family:Space Mono,monospace;font-size:11px;">✅ {nf} saham · Scoring {scan_mode}...</div>',unsafe_allow_html=True)
                results=[]; min_b=20 if is_bag else 30
                ll=[]; lph=st.empty()
                for i,tyf in enumerate(list(data_dict.keys())):
                    pb.progress((i+1)/max(len(data_dict),1))
                    ts=datetime.now(jakarta_tz).strftime("%H:%M:%S")
                    tkl=stock_map.get(tyf,tyf.replace(".JK",""))
                    try:
                        df=data_dict[tyf].copy()
                        if len(df)<min_b: continue
                        df=apply_indicators(df)
                        r=df.iloc[-1]; p=df.iloc[-2]; p2=df.iloc[-3] if len(df)>=3 else p
                        cl=float(r["Close"]); vol=float(r["Volume"]); to=cl*vol; rv=float(r["RVOL"])
                        if to<min_turn or rv<vol_thresh: continue
                        if scan_mode=="Scalping ⚡":   sc,rs,_=score_scalping(r,p,p2)
                        elif scan_mode=="Momentum 🚀": sc,rs,_=score_momentum(r,p,p2)
                        elif scan_mode=="Bagger 💎":   sc,rs,_=score_bagger(r,p,p2,df)
                        else:                          sc,rs,_=score_reversal(r,p,p2)
                        if sc<min_score:
                            ll.append(f'<span style="color:#304050">[{ts}] ❄️ {tkl} score:{sc:.1f}</span>')
                            if len(ll)%5==0: lph.markdown(f'<div style="background:#05080d;border:1px solid #1a2a3a;border-radius:4px;padding:8px 12px;font-size:11px;max-height:180px;overflow-y:auto;font-family:Space Mono,monospace;line-height:1.8">{"<br>".join(ll[-10:])}</div>',unsafe_allow_html=True)
                            continue
                        sig=get_signal(sc,scan_mode)
                        if sig=="WAIT": continue
                        atr=float(r["ATR"]) if not np.isnan(float(r["ATR"])) else cl*0.01
                        slm=rcfg.get("sl_mult",0.8)
                        if scan_mode=="Scalping ⚡":   tp=cl+1.5*atr;sl_=cl-slm*atr
                        elif scan_mode=="Momentum 🚀": tp=cl+2.0*atr;sl_=cl-slm*atr
                        elif scan_mode=="Bagger 💎":   tp=cl+3.0*atr;sl_=cl-1.0*atr
                        else:                          tp=cl+2.5*atr;sl_=cl-slm*atr
                        rr=(tp-cl)/max(cl-sl_,0.01)
                        e9=float(r["EMA9"]); e21=float(r["EMA21"]); e50=float(r["EMA50"])
                        trend="▲ UP" if e9>e21>e50 else("▼ DOWN" if e9<e21<e50 else "◆ SIDE")
                        li="🔥" if("GACOR" in sig or "BAGGER" in sig or "REVERSAL" in sig) else "⚡" if "POTENSIAL" in sig else "👀"
                        lc="color:#ff9900;font-weight:700" if "GACOR" in sig or "BAGGER" in sig else "color:#ffb700" if "POTENSIAL" in sig else "color:#00ff88"
                        ll.append(f'<span style="{lc}">[{ts}] {li} {tkl} | score:{sc:.1f} | {sig} | RVOL:{rv:.1f}x</span>')
                        lph.markdown(f'<div style="background:#05080d;border:1px solid #1a2a3a;border-radius:4px;padding:8px 12px;font-size:11px;max-height:180px;overflow-y:auto;font-family:Space Mono,monospace;line-height:1.8">{"<br>".join(ll[-12:])}</div>',unsafe_allow_html=True)
                        results.append({"Ticker":stock_map.get(tyf,tyf.replace(".JK","")),"Price":int(cl),"Score":sc,
                            "Signal":sig,"Trend":trend,"TF":"Daily" if is_bag else "15m",
                            "RSI-EMA":round(float(r["RSI_EMA"]),1),"Stoch K":round(float(r["STOCH_K"]),1),
                            "Stoch D":round(float(r["STOCH_D"]),1),"MACD Hist":round(float(r["MACD_Hist"]),4),
                            "RVOL":round(rv,2),"BB%":round(float(r["BB_pct"]),2),
                            "ROC 3B%":round(float(r["ROC3"])*100,2),"VWAP":int(float(r["VWAP"])),
                            "TP":int(tp),"SL":int(sl_),"R:R":round(rr,1),
                            "Turnover(M)":round(to/1e6,1),"Reasons":" · ".join(rs),
                            "_class":get_card_class(sig)})
                    except: continue
                ph.empty(); pb.empty()
                st.session_state.scan_results=results
                st.session_state.last_scan_time=datetime.now(jakarta_tz).timestamp()
                st.session_state.last_scan_mode=scan_mode
                if tele_on and results:
                    if "tt_last_sent" not in st.session_state: st.session_state.tt_last_sent=set()
                    dft=pd.DataFrame(results).sort_values("Score",ascending=False)
                    cs=set(dft["Ticker"].tolist()); na=cs-st.session_state.tt_last_sent
                    if na:
                        tn=dft[dft["Ticker"].isin(na)].head(5).to_dict("records")
                        if tn: send_telegram(tn)
                        st.session_state.tt_last_sent.update(na)
                    st.session_state.tt_last_sent=st.session_state.tt_last_sent&cs
        except Exception as e:
            try: ph.empty();pb.empty()
            except: pass
            st.error(f"Scan error: {str(e)[:150]}")

    if st.session_state.last_scan_time:
        _nc2=datetime.now(jakarta_tz).timestamp()
        _rem=max(0,300-(_nc2-st.session_state.last_scan_time))
        _lt=datetime.fromtimestamp(st.session_state.last_scan_time,jakarta_tz).strftime("%H:%M:%S")
        lm=st.session_state.get("last_scan_mode","")
        st.caption(f"⏱️ Next auto-scan: {int(_rem//60):02d}:{int(_rem%60):02d} · Last: {_lt} WIB · {'📅 Daily' if lm=='Bagger 💎' else '⚡ 15M'} · {lm}")

        results = st.session_state.scan_results

    # === JINAKKAN VARIABEL DI SINI (SOLUSI) ===
    # Kita kasih nilai default "-" kalau variabelnya belum pernah dibuat
    _sz = _sz if '_sz' in locals() else "-"
    regime = regime if 'regime' in locals() else "-"
    
    # Untuk rcfg, kita cek apakah sudah ada, kalau belum buat dictionary default
    if 'rcfg' not in locals():
        rcfg = {"mode": lm if 'lm' in locals() else "-"} 
    # ==========================================

    if not results and not do_scan:
        st.markdown(f'''<div style="text-align:center;padding:48px;color:#4a5568;font-family:Space Mono,monospace;">
          <div style="font-size:36px;margin-bottom:12px;">🔥</div>
          <div style="font-size:13px;letter-spacing:2px;">KLIK SCAN UNTUK MULAI</div>
          <div style="font-size:10px;margin-top:8px;color:#2d3748;">{_sz} · {regime} · {rcfg["mode"]}</div>
        </div>''', unsafe_allow_html=True)

    elif results:
        df_out=pd.DataFrame(results).sort_values("Score",ascending=False).reset_index(drop=True)
        gacor=df_out[df_out["Signal"].str.contains("GACOR|REVERSAL",na=False)]
        bagger=df_out[df_out["Signal"].str.contains("BAGGER|KANDIDAT",na=False)]
        potensi=df_out[df_out["Signal"].str.contains("POTENSIAL",na=False)]
        avg_rsi=df_out["RSI-EMA"].mean()
        lm=st.session_state.get("last_scan_mode","")
        tfb='<span style="font-size:9px;color:#bf5fff;background:rgba(191,95,255,.1);padding:2px 6px;border-radius:3px;">📅 DAILY TF</span>' if lm=="Bagger 💎" else '<span style="font-size:9px;color:#00e5ff;background:rgba(0,229,255,.1);padding:2px 6px;border-radius:3px;">⚡ 15M</span>'
        st.markdown(f"""
        <div class="metric-row">
          <div class="metric-card" style="border-top-color:{rcolor}"><div class="metric-label">Regime</div>
            <div class="metric-value" style="font-size:16px;color:{rcolor}">{regime}</div>
            <div class="metric-sub">{ihsg_price:,.0f} {chg_sym}{abs(ihsg_chg):.2f}%</div></div>
          <div class="metric-card orange"><div class="metric-label">Mode</div>
            <div class="metric-value" style="font-size:12px;margin-top:4px;">{lm}</div><div class="metric-sub">{tfb}</div></div>
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
        </div>""",unsafe_allow_html=True)
        th='<div class="tape-wrap"><div class="tape-inner">'
        for _,row in df_out.iterrows():
            roc=row["ROC 3B%"]; ib="BAGGER" in row["Signal"] or "KANDIDAT" in row["Signal"]
            cls="bagger" if ib else("up" if roc>0 else("down" if roc<0 else "flat"))
            sym="💎" if ib else("▲" if roc>0 else("▼" if roc<0 else "─"))
            tft="[D]" if row.get("TF","")=="Daily" else "[15M]"
            th+=f'<span class="tape-item {cls}">{row["Ticker"]} {int(row["Price"])} {sym}{abs(roc):.1f}% {tft}</span>'
        th+=th.replace('tape-inner">',''); th+='</div></div>'
        st.markdown(th,unsafe_allow_html=True)
        if not bagger.empty:
            st.markdown(f'<div class="bagger-alert-box"><div class="bagger-title">💎 WYCKOFF BAGGER ALERT · {len(bagger)} KANDIDAT · DAILY TF</div><div style="font-size:11px;color:#4a5568;margin-top:4px;">Phase A-B · Spring/Shakeout · Phase D (RVOL+Breakout)</div></div>',unsafe_allow_html=True)
        if not gacor.empty:
            st.markdown(f'<div class="alert-box"><div class="alert-title">🚨 GACOR ALERT · {len(gacor)} SAHAM · {lm}</div></div>',unsafe_allow_html=True)
        if view_mode=="Card View 🃏":
            st.markdown('<div class="section-title">Signal Cards</div>',unsafe_allow_html=True)
            ch='<div class="signal-grid">'
            for _,row in df_out.head(20).iterrows():
                si=int(row["Score"]); ib="BAGGER" in row["Signal"] or "KANDIDAT" in row["Signal"]
                bc2="filled-purple" if ib else "filled"
                bars="".join([f'<div class="sc-bar {bc2 if i<si else "empty"}" style="width:28px"></div>' for i in range(6)])
                rc="#00ff88" if row["ROC 3B%"]>0 else "#ff3d5a"
                te="📈" if "▲" in row["Trend"] else("📉" if "▼" in row["Trend"] else "➡️")
                sc2="#bf5fff" if ib else("#00ff88" if si>=5 else "#ffb700" if si>=4 else "#00e5ff")
                tfbc='<span style="font-size:8px;color:#bf5fff;margin-left:4px;">[D1]</span>' if row.get("TF","")=="Daily" else '<span style="font-size:8px;color:#4a5568;margin-left:4px;">[15M]</span>'
                ch+=f'''<div class="signal-card {row["_class"]}">
                  <div style="display:flex;justify-content:space-between;align-items:flex-start;">
                    <div><div class="sc-ticker">{row["Ticker"]}{tfbc}</div>
                    <div class="sc-price" style="color:{rc}">{int(row["Price"]):,} {te}</div></div>
                    <div style="text-align:right;"><div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;">SCORE</div>
                    <div style="font-family:Space Mono,monospace;font-size:20px;font-weight:700;color:{sc2}">{row["Score"]}</div></div>
                  </div>
                  <div class="sc-signal" style="color:{sc2}">{row["Signal"]}</div>
                  <div class="sc-bars">{bars}</div>
                  <div class="sc-stats">
                    <div class="sc-stat">RSI-EMA <span>{row["RSI-EMA"]}</span></div>
                    <div class="sc-stat">STOCH <span>{row["Stoch K"]:.0f}</span></div>
                    <div class="sc-stat">RVOL <span>{row["RVOL"]}x</span></div>
                    <div class="sc-stat">ROC <span style="color:{rc}">{row["ROC 3B%"]:+.1f}%</span></div>
                  </div>
                  <div class="sc-stats" style="margin-top:6px;">
                    <div class="sc-stat">TP <span style="color:#00ff88">{int(row["TP"]):,}</span></div>
                    <div class="sc-stat">SL <span style="color:#ff3d5a">{int(row["SL"]):,}</span></div>
                    <div class="sc-stat">R:R <span>{row["R:R"]}</span></div>
                  </div>
                  <div style="margin-top:8px;font-size:10px;color:#4a5568;line-height:1.4;font-family:Space Mono,monospace;">{row["Reasons"][:80]}</div>
                </div>'''
            ch+='</div>'
            st.markdown(ch,unsafe_allow_html=True)
        st.markdown('<div class="section-title">Full Signal Table</div>',unsafe_allow_html=True)
        dc=["Ticker","TF","Price","Score","Signal","Trend","RSI-EMA","Stoch K","Stoch D","MACD Hist","RVOL","BB%","ROC 3B%","VWAP","TP","SL","R:R","Turnover(M)","Reasons"]
        dc=[c for c in dc if c in df_out.columns]
        st.dataframe(df_out[dc],use_container_width=True,hide_index=True,column_config={
            "Score":      st.column_config.ProgressColumn("Score",min_value=0,max_value=6,format="%.1f"),
            "RSI-EMA":    st.column_config.NumberColumn("RSI-EMA",format="%.1f"),
            "RVOL":       st.column_config.NumberColumn("RVOL",format="%.1fx"),
            "ROC 3B%":    st.column_config.NumberColumn("ROC 3B%",format="%.2f%%"),
            "Turnover(M)":st.column_config.NumberColumn("Turnover(M)",format="Rp%.0fM"),
        })

# ════ TAB WATCHLIST ════
with tab_watchlist:
    st.markdown('<div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-bottom:12px;padding:10px 14px;background:#0d1117;border-radius:6px;border-left:3px solid #ff7b00;">Analisa mendalam per saham. Bagger 💎 otomatis pakai <b style="color:#bf5fff">Daily TF</b>.</div>',unsafe_allow_html=True)
    wc1,wc2,wc3=st.columns([3,1,1])
    with wc1:
        wl_input=st.text_area("Ticker",placeholder="Contoh:\nBBCA\nARCI, ASSA, GOTO",height=120,label_visibility="collapsed",key="wl_input")
    with wc2:
        wl_mode=st.radio("Mode",["Scalping ⚡","Momentum 🚀","Reversal 🎯","Bagger 💎"],key="wl_mode")
        st.caption(f"Regime suggest: {rcfg['mode']}")
        if wl_mode=="Bagger 💎": st.markdown('<div style="font-size:9px;color:#bf5fff;">📅 Pakai Daily TF</div>',unsafe_allow_html=True)
    with wc3:
        st.markdown("<br>",unsafe_allow_html=True)
        wl_run=st.button("🔍 Analisa",use_container_width=True,key="wl_run")
        wl_tele=st.button("📡 Kirim Telegram",use_container_width=True,key="wl_tele")
        wl_share=st.button("📋 Copy Hasil",use_container_width=True,key="wl_share")

    if wl_run and wl_input.strip():
        raw_wl=list(dict.fromkeys([t.strip().upper() for ln in wl_input.split("\n") for t in ln.split(",") if t.strip()]))
        if raw_wl:
            pr="60d" if wl_mode=="Bagger 💎" else "7d"; iv="1d" if wl_mode=="Bagger 💎" else "15m"; mb=20 if wl_mode=="Bagger 💎" else 30
            with st.spinner(f"Menganalisa {len(raw_wl)} saham ({'Daily' if wl_mode=='Bagger 💎' else '15M'})..."):
                raw_d=_fetch_parallel([t+".JK" for t in raw_wl],pr,iv,workers=5)
                wl_res=[]
                for t in raw_wl:
                    df=raw_d.get(t+".JK")
                    if df is None or len(df)<mb:
                        wl_res.append({"Ticker":t,"Price":0,"Score":0,"Signal":"No data","Trend":"-","TF":"-","RSI-EMA":0,"Stoch K":0,"RVOL":0,"BB%":0,"ROC 3B%":0,"VWAP":0,"TP":0,"SL":0,"R:R":0,"ATR":0,"Reasons":"No data","_class":"","MACD Hist":0}); continue
                    try:
                        df=apply_indicators(df); r=df.iloc[-1]; p=df.iloc[-2]; p2=df.iloc[-3] if len(df)>=3 else p
                        cl=float(r["Close"]); atr=float(r["ATR"]) if not np.isnan(float(r["ATR"])) else cl*0.01
                        slm=rcfg.get("sl_mult",0.8)
                        if wl_mode=="Scalping ⚡":   sc,rs,_=score_scalping(r,p,p2);  tp=cl+1.5*atr;sl_=cl-slm*atr
                        elif wl_mode=="Momentum 🚀": sc,rs,_=score_momentum(r,p,p2);  tp=cl+2.0*atr;sl_=cl-slm*atr
                        elif wl_mode=="Bagger 💎":   sc,rs,_=score_bagger(r,p,p2,df); tp=cl+3.0*atr;sl_=cl-1.0*atr
                        else:                        sc,rs,_=score_reversal(r,p,p2);  tp=cl+2.5*atr;sl_=cl-slm*atr
                        sig=get_signal(sc,wl_mode); rr=(tp-cl)/max(cl-sl_,0.01)
                        e9=float(r["EMA9"]); e21=float(r["EMA21"]); e50=float(r["EMA50"])
                        tr2="▲ UP" if e9>e21>e50 else("▼ DOWN" if e9<e21<e50 else "◆ SIDE")
                        pv=None
                        try:
                            if len(df)>=2:
                                prev=df.iloc[-2]; pp=(float(prev["High"])+float(prev["Low"])+float(prev["Close"]))/3
                                pv={"PP":pp,"R1":2*pp-float(prev["Low"]),"R2":pp+(float(prev["High"])-float(prev["Low"])),"S1":2*pp-float(prev["High"]),"S2":pp-(float(prev["High"])-float(prev["Low"]))}
                        except: pass
                        pp=get_pivot_position(cl,pv)[0] if pv else "-"
                        mtf2={"M15":sc}
                        try:
                            for rs_,rk_,mb_ in [("1h","H1",10),("1D","D1",3)]:
                                dr=df.resample(rs_).agg({"Open":"first","High":"max","Low":"min","Close":"last","Volume":"sum"}).dropna(subset=["Close"])
                                dr=dr[dr["Volume"]>0]
                                if len(dr)>=mb_:
                                    dr=apply_indicators(dr); rr2=dr.iloc[-1]; pp2=dr.iloc[-2]; p2r=dr.iloc[-3] if len(dr)>=3 else pp2
                                    if wl_mode=="Scalping ⚡":   scr,_,_=score_scalping(rr2,pp2,p2r)
                                    elif wl_mode=="Momentum 🚀": scr,_,_=score_momentum(rr2,pp2,p2r)
                                    elif wl_mode=="Bagger 💎":   scr,_,_=score_bagger(rr2,pp2,p2r,dr)
                                    else:                         scr,_,_=score_reversal(rr2,pp2,p2r)
                                    mtf2[rk_]=round(scr,1)
                        except: pass
                        al,_,_=mtf_alignment(mtf2)
                        wl_res.append({"Ticker":t,"Price":int(cl),"Score":sc,"Signal":sig,"Trend":tr2,
                            "TF":"Daily" if wl_mode=="Bagger 💎" else "15M",
                            "RSI-EMA":round(float(r["RSI_EMA"]),1),"Stoch K":round(float(r["STOCH_K"]),1),
                            "RVOL":round(float(r["RVOL"]),2),"BB%":round(float(r["BB_pct"]),2),
                            "ROC 3B%":round(float(r["ROC3"])*100,2),"VWAP":int(float(r["VWAP"])),
                            "TP":int(tp),"SL":int(sl_),"R:R":round(rr,1),"ATR":round(atr,0),
                            "MACD Hist":round(float(r["MACD_Hist"]),4),"Reasons":" · ".join(rs),
                            "_class":get_card_class(sig),"Pivot Pos":pp,
                            "PP":int(pv["PP"]) if pv else 0,"R1":int(pv["R1"]) if pv else 0,
                            "S1":int(pv["S1"]) if pv else 0,"MTF Align":al,
                            "M15":mtf2.get("M15",0),"H1":mtf2.get("H1",0),"D1":mtf2.get("D1",0)})
                    except ex:
                        wl_res.append({"Ticker":t,"Price":0,"Score":0,"Signal":f"Err:{str(ex)[:20]}","RSI-EMA":0,"Stoch K":0,"RVOL":0,"BB%":0,"Trend":"-","TF":"-","TP":0,"SL":0,"R:R":0,"ROC 3B%":0,"VWAP":0,"ATR":0,"Reasons":"","_class":"","MACD Hist":0})
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
                    ch+=f'<div class="signal-card"><div class="sc-ticker">{row["Ticker"]}</div><div style="font-size:11px;color:#4a5568;margin-top:6px;">{row.get("Signal","No data")}</div></div>'; continue
                si=int(row["Score"]); bars="".join([f'<div class="sc-bar {"filled" if i<si else "empty"}" style="width:26px"></div>' for i in range(6)])
                sig=row.get("Signal","-"); ib="BAGGER" in sig or "KANDIDAT" in sig
                sc2="#bf5fff" if ib else("#00ff88" if("GACOR" in sig or "REVERSAL" in sig) else("#ffb700" if "POTENSIAL" in sig else "#00e5ff" if "WATCH" in sig else "#4a5568"))
                rv=row["RSI-EMA"]; rc2="#ff3d5a" if rv<30 else("#ffb700" if rv<45 else "#00ff88" if rv>60 else "#c9d1d9")
                roc2="#00ff88" if row.get("ROC 3B%",0)>0 else "#ff3d5a"
                te="📈" if "▲" in row["Trend"] else("📉" if "▼" in row["Trend"] else "➡️")
                tfb='<span style="font-size:8px;color:#bf5fff;">[D1]</span>' if row.get("TF","")=="Daily" else '<span style="font-size:8px;color:#4a5568;">[15M]</span>'
                ch+=f'''<div class="signal-card {row["_class"]}">
                  <div style="display:flex;justify-content:space-between;">
                    <div><div class="sc-ticker">{row["Ticker"]} {tfb}</div>
                    <div class="sc-price" style="color:{roc2}">{row["Price"]:,} {te}</div></div>
                    <div style="text-align:right"><div style="font-family:Space Mono,monospace;font-size:9px;color:#4a5568">SCORE</div>
                    <div style="font-family:Space Mono,monospace;font-size:22px;font-weight:700;color:{sc2}">{row["Score"]}</div></div>
                  </div>
                  <div class="sc-signal" style="color:{sc2}">{sig}</div>
                  <div class="sc-bars">{bars}</div>
                  <div class="sc-stats">
                    <div class="sc-stat">RSI-EMA <span style="color:{rc2}">{rv}</span></div>
                    <div class="sc-stat">STOCH <span>{row["Stoch K"]:.0f}</span></div>
                    <div class="sc-stat">RVOL <span>{row["RVOL"]}x</span></div>
                  </div>
                  <div class="sc-stats" style="margin-top:6px">
                    <div class="sc-stat">TP <span style="color:#00ff88">{int(row["TP"]):,}</span></div>
                    <div class="sc-stat">SL <span style="color:#ff3d5a">{int(row["SL"]):,}</span></div>
                    <div class="sc-stat">R:R <span>{row["R:R"]}</span></div>
                  </div>
                  <div style="margin-top:8px;font-size:10px;color:#4a5568;line-height:1.5;font-family:Space Mono,monospace">{row["Reasons"][:80]}</div>
                  <div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap;">
                    <div style="font-family:Space Mono,monospace;font-size:9px;padding:2px 7px;border-radius:10px;background:rgba(0,0,0,.3);color:#4a5568;">📍 {row.get("Pivot Pos","-")}</div>
                    <div style="font-family:Space Mono,monospace;font-size:9px;padding:2px 7px;border-radius:10px;background:rgba(0,0,0,.3);color:#4a5568;">MTF: {row.get("MTF Align","-")}</div>
                  </div>
                  <div style="font-family:Space Mono,monospace;font-size:9px;color:#4a5568;margin-top:4px;">M15:{row.get("M15",0)} · H1:{row.get("H1",0)} · D1:{row.get("D1",0)} | PP:{row.get("PP",0):,} · R1:{row.get("R1",0):,} · S1:{row.get("S1",0):,}</div>
                </div>'''
            ch+='</div>'; st.markdown(ch,unsafe_allow_html=True)
            df_wl=pd.DataFrame([r for r in wl_res if r["Price"]>0])
            if not df_wl.empty:
                sh=["Ticker","TF","Price","Score","Signal","Trend","RSI-EMA","Stoch K","RVOL","BB%","ROC 3B%","VWAP","TP","SL","R:R","MTF Align","M15","H1","D1","Pivot Pos","PP","R1","S1","ATR","Reasons"]
                sh=[c for c in sh if c in df_wl.columns]
                st.dataframe(df_wl[sh],use_container_width=True,hide_index=True,column_config={
                    "Score":st.column_config.ProgressColumn("Score",min_value=0,max_value=6,format="%.1f"),
                    "RSI-EMA":st.column_config.NumberColumn("RSI-EMA",format="%.1f"),
                    "RVOL":st.column_config.NumberColumn("RVOL",format="%.2fx"),
                    "ROC 3B%":st.column_config.NumberColumn("ROC 3B%",format="%.2f%%"),
                })
    if wl_tele and st.session_state.wl_results:
        ts=[r for r in st.session_state.wl_results if r["Price"]>0]
        if ts: send_telegram(ts[:5],source="Watchlist"); st.success("📡 Terkirim!")
    if wl_share and st.session_state.wl_results:
        ns=datetime.now(jakarta_tz).strftime("%d %b %Y %H:%M"); wu=st.session_state.get("wl_mode_used","")
        txt=f"🔥 THETA TURBO WATCHLIST\n⏰ {ns} WIB\n📊 Mode: {wu} | Regime: {regime}\n"+"─"*28+"\n"
        for r in sorted(st.session_state.wl_results,key=lambda x:x["Score"],reverse=True):
            if r["Price"]==0: continue
            sig=r.get("Signal","-")
            em="💎" if("BAGGER" in sig or "KANDIDAT" in sig) else("🔥" if("GACOR" in sig or "REVERSAL" in sig) else("⚡" if "POTENSIAL" in sig else "👀"))
            tft="[D1]" if r.get("TF","")=="Daily" else "[15M]"
            txt+=f"{em} {r['Ticker']}{tft} | {r['Price']:,} | Score:{r['Score']} | RSI:{r['RSI-EMA']} | {sig}\n"
            if r.get("Reasons"): txt+=f"   → {r['Reasons'][:60]}\n"
        txt+="─"*28+"\nby Theta Turbo v5.3 🔥 (Zero Rate Limit Edition)"
        st.text_area("Copy untuk grup:",txt,height=280,key="share_out")
    if not st.session_state.wl_results and not wl_run:
        st.markdown('<div style="text-align:center;padding:48px;color:#4a5568;font-family:Space Mono,monospace;"><div style="font-size:32px;margin-bottom:12px;">👁️</div><div>MASUKKAN TICKER DI ATAS</div></div>',unsafe_allow_html=True)

# ════ TAB BSJP ════
with tab_bsjp:
    nw=datetime.now(jakarta_tz)
    ie=(nw.hour==14 and nw.minute>=30) or (nw.hour==15 and nw.minute<=45)
    ix=(nw.hour==9) or (nw.hour==10 and nw.minute==0)
    st.markdown(f"""<div style="background:rgba(191,95,255,.08);border:1px solid rgba(191,95,255,.3);border-radius:8px;padding:14px 18px;margin-bottom:16px;">
      <div style="font-family:Space Mono,monospace;font-size:13px;font-weight:700;color:#bf5fff;letter-spacing:1px;">🌙 BELI SORE JUAL PAGI</div>
      <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-top:4px;">
        Entry: <span style="color:#ffb700">14:30–15:45 WIB</span> · Exit: <span style="color:#00ff88">Besok 09:00–10:00 WIB</span> ·
        Status: <span style="color:{'#00ff88' if ie else '#ffb700' if ix else '#4a5568'}">{'🟢 WAKTU ENTRY!' if ie else '🟡 WAKTU EXIT!' if ix else '⏳ Tunggu 14:30 WIB'}</span>
      </div>
    </div>""",unsafe_allow_html=True)
    bc1,bc2=st.columns([2,1])
    with bc1:
        bms=st.slider("Min BSJP Score",0,6,4,key="bsjp_score")
        bmr=st.slider("Min RVOL",1.0,5.0,1.5,0.1,key="bsjp_rvol")
    with bc2:
        bmt=st.number_input("Min Turnover (M Rp)",value=500,step=100,key="bsjp_turn")*1_000_000
        btele=st.toggle("📡 Telegram Alert",value=True,key="bsjp_tele")
    do_bsjp=st.button("🌙 SCAN BSJP SEKARANG",type="primary",use_container_width=True,key="btn_bsjp")
    if do_bsjp:
        bp2=st.empty(); bp2.info("🌙 Scanning BSJP candidates...")
        bres=[]; sd=st.session_state.get("data_dict",{})
        if not sd:
            try: sd=fetch_intraday(tuple(stocks_yf[:200]))
            except: pass
        pb3=st.progress(0); tk3=list(sd.keys())
        for i,tyf in enumerate(tk3):
            pb3.progress((i+1)/max(len(tk3),1))
            try:
                df=sd[tyf].copy()
                if len(df)<30: continue
                df=apply_indicators(df); r=df.iloc[-1]; p=df.iloc[-2]; p2=df.iloc[-3] if len(df)>=3 else p
                cl=float(r["Close"]); vol=float(r["Volume"]); to=cl*vol; rv=float(r["RVOL"])
                if to<bmt or rv<bmr: continue
                sc,rs,_=score_bsjp(r,p,p2)
                if sc<bms: continue
                bsig="STRONG BUY 🌙" if sc>=5 else("BUY ⚡" if sc>=4 else "WATCH 👀")
                atr=float(r["ATR"]); tp=cl+2.0*atr; sl_=cl-1.0*atr; rr=(tp-cl)/max(cl-sl_,0.01)
                pv=None
                try:
                    if len(df)>=2:
                        prev=df.iloc[-2]; pp2=(float(prev["High"])+float(prev["Low"])+float(prev["Close"]))/3
                        pv={"PP":pp2,"R1":2*pp2-float(prev["Low"]),"R2":pp2+(float(prev["High"])-float(prev["Low"])),"S1":2*pp2-float(prev["High"]),"S2":pp2-(float(prev["High"])-float(prev["Low"]))}
                except: pass
                pvp=get_pivot_position(cl,pv)[0] if pv else "-"
                e9=float(r["EMA9"]); e21=float(r["EMA21"]); e50=float(r["EMA50"])
                tr3="▲ UP" if e9>e21>e50 else("▼ DOWN" if e9<e21<e50 else "◆ SIDE")
                bres.append({"Ticker":stock_map.get(tyf,tyf.replace(".JK","")),"Price":int(cl),"Score":sc,
                    "Signal":bsig,"Trend":tr3,"RSI-EMA":round(float(r["RSI_EMA"]),1),
                    "Stoch K":round(float(r["STOCH_K"]),1),"RVOL":round(rv,2),
                    "TP":int(tp),"SL":int(sl_),"R:R":round(rr,1),"Turnover(M)":round(to/1e6,1),
                    "Pivot Pos":pvp,"PP":int(pv["PP"]) if pv else 0,"R1":int(pv["R1"]) if pv else 0,
                    "S1":int(pv["S1"]) if pv else 0,"Reasons":" · ".join(rs),
                    "_class":"gacor" if sc>=5 else "potensial" if sc>=4 else "watch"})
            except: continue
        pb3.empty(); bp2.empty()
        bres=sorted(bres,key=lambda x:x["Score"],reverse=True)
        st.session_state.bsjp_results=bres
        if btele and bres:
            nb=datetime.now(jakarta_tz); sep="━"*28
            msg=f"🌙 *BSJP ALERT — BELI SORE JUAL PAGI*\n⏰ `{nb.strftime('%H:%M:%S')} WIB`\n{sep}\n"
            for r in bres[:5]:
                bar="█"*int(r["Score"])+"░"*(6-int(r["Score"]))
                msg+=(f"\n🌙 *{r['Ticker']}* `{r['Signal']}`\n   💰 `{r['Price']:,}`\n📊 `[{bar}] {r['Score']}/6`\n   🎯 TP:`{r['TP']:,}` SL:`{r['SL']:,}` R:R `{r['R:R']}` | RVOL:`{r['RVOL']}x`\n   💡 _{r['Reasons'][:50]}_\n")
            msg+=f"\n{sep}\n🌙 _Entry 14:30-15:45 · Exit besok 09:00-10:00_\n⚠️ _BUKAN saran investasi!_"
            try:
                requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",data={"chat_id":CHAT_ID,"text":msg,"parse_mode":"Markdown"},timeout=10)
            except: pass
    br=st.session_state.bsjp_results
    if br:
        strong=[r for r in br if "STRONG" in r.get("Signal","")]; buy=[r for r in br if r.get("Signal","")=="BUY ⚡"]
        st.markdown(f"""<div class="metric-row">
          <div class="metric-card" style="border-top-color:#bf5fff"><div class="metric-label">Dipindai</div><div class="metric-value">{len(br)}</div></div>
          <div class="metric-card green"><div class="metric-label">Strong Buy 🌙</div><div class="metric-value">{len(strong)}</div></div>
          <div class="metric-card amber"><div class="metric-label">Buy ⚡</div><div class="metric-value">{len(buy)}</div></div>
          <div class="metric-card"><div class="metric-label">Entry</div><div class="metric-value" style="font-size:13px;color:#ffb700">14:30</div></div>
          <div class="metric-card"><div class="metric-label">Exit</div><div class="metric-value" style="font-size:13px;color:#00ff88">09:00</div></div>
        </div>""",unsafe_allow_html=True)
        if len(br)>=1:
            medals=["🥇","🥈","🥉"]; ct=st.columns(min(3,len(br)))
            for idx,col in enumerate(ct):
                if idx>=len(br): break
                row=br[idx]; sc2="#00ff88" if "STRONG" in row["Signal"] else "#ffb700"
                with col:
                    st.markdown(f"""<div style="background:#0d1117;border:1px solid {sc2}44;border-radius:10px;padding:16px;text-align:center;border-top:3px solid {sc2};">
                      <div style="font-size:24px">{medals[idx]}</div>
                      <div style="font-family:Space Mono,monospace;font-size:18px;font-weight:700;color:#e6edf3;">{row["Ticker"]}</div>
                      <div style="font-family:Space Mono,monospace;font-size:28px;font-weight:700;color:{sc2};">{row["Score"]}</div>
                      <div style="font-size:11px;font-weight:700;color:{sc2};">{row["Signal"]}</div>
                      <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-top:6px;">RVOL {row["RVOL"]}x · RSI {row["RSI-EMA"]}<br>TP {row["TP"]:,} · SL {row["SL"]:,}</div>
                    </div>""",unsafe_allow_html=True)
        df_bs=pd.DataFrame(br)
        sc3=["Ticker","Price","Score","Signal","Trend","RSI-EMA","Stoch K","RVOL","TP","SL","R:R","Pivot Pos","PP","R1","S1","Turnover(M)","Reasons"]
        sc3=[c for c in sc3 if c in df_bs.columns]
        st.dataframe(df_bs[sc3],use_container_width=True,hide_index=True,column_config={
            "Score":st.column_config.ProgressColumn("Score",min_value=0,max_value=6,format="%.1f"),
            "RVOL":st.column_config.NumberColumn("RVOL",format="%.2fx"),
        })
    elif not do_bsjp:
        st.markdown('<div style="text-align:center;padding:48px;color:#4a5568;font-family:Space Mono,monospace;"><div style="font-size:32px;margin-bottom:12px;">🌙</div><div>KLIK SCAN BSJP</div></div>',unsafe_allow_html=True)

# ════ TAB SEKTOR ════
with tab_sector:
    st.markdown('<div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-bottom:14px;padding:10px 14px;background:#0d1117;border-radius:6px;border-left:3px solid #ff7b00;">Track sektor momentum IDX hari ini.</div>',unsafe_allow_html=True)
    do_sec=st.button("🏭 REFRESH SECTORS",type="primary",use_container_width=True,key="btn_sector")
    if do_sec:
        with st.spinner("Fetching sector data..."):
            st.session_state.sector_data=fetch_all_sectors(SECTORS,top_n=10,is_jk=True)
    if st.session_state.sector_data:
        ss=sorted(st.session_state.sector_data.items(),key=lambda x:x[1]["avg_chg"],reverse=True)
        st.markdown('<div class="section-title">Sector Heatmap</div>',unsafe_allow_html=True)
        cs3=st.columns(3)
        for idx,(sn,si) in enumerate(ss):
            chg=si["avg_chg"]; col="#00ff88" if chg>1 else("#ffb700" if chg>0 else "#ff3d5a")
            bg="rgba(0,255,136,.06)" if chg>1 else("rgba(255,183,0,.06)" if chg>0 else "rgba(255,61,90,.06)")
            bp=int(si["bullish"]/max(si["total"],1)*100)
            with cs3[idx%3]:
                st.markdown(f"""<div style="background:{bg};border:1px solid {col}44;border-radius:8px;padding:12px;margin-bottom:10px;">
                  <div style="font-family:Space Mono,monospace;font-size:10px;font-weight:700;color:#c9d1d9;">{sn}</div>
                  <div style="font-family:Space Mono,monospace;font-size:22px;font-weight:700;color:{col};margin:4px 0;">{chg:+.2f}%</div>
                  <div style="font-size:9px;color:#4a5568;">RVOL avg: {si["avg_rvol"]:.1f}x · Bullish: {si["bullish"]}/{si["total"]} ({bp}%)</div>
                  <div style="height:4px;background:#1c2533;border-radius:2px;margin-top:6px;overflow:hidden;">
                    <div style="width:{bp}%;height:100%;background:{col};border-radius:2px;"></div>
                  </div>
                </div>""",unsafe_allow_html=True)
        top3=ss[:3]; ct3=st.columns(3)
        for cidx,(sn,si) in enumerate(top3):
            with ct3[cidx]:
                chg=si["avg_chg"]; col="#00ff88" if chg>0 else "#ff3d5a"
                st.markdown(f'<div style="font-family:Space Mono,monospace;font-size:11px;color:{col};font-weight:700;margin-bottom:8px;">{sn}</div>',unsafe_allow_html=True)
                for stk in sorted(si["stocks"],key=lambda x:x["chg"],reverse=True)[:5]:
                    sc4="#00ff88" if stk["chg"]>0 else "#ff3d5a"
                    st.markdown(f'<div style="display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid #1c2533;font-family:Space Mono,monospace;font-size:10px;"><span style="color:#c9d1d9;">{stk["ticker"]}</span><span style="color:{sc4}">{stk["chg"]:+.1f}%</span><span style="color:#4a5568;">RVOL {stk["rvol"]}x</span></div>',unsafe_allow_html=True)
    st.markdown('<div class="section-title" style="margin-top:24px;">Beta vs IHSG</div>',unsafe_allow_html=True)
    do_beta=st.button("🔬 Calculate Beta All Sectors",use_container_width=True,key="btn_beta")
    if do_beta:
        br2=[]; bpb=st.progress(0); secs=list(SECTORS.items())
        for i,(sn,ss2) in enumerate(secs):
            bpb.progress((i+1)/len(secs))
            res=calc_sector_beta(sn,ss2)
            if res: br2.append(res)
        bpb.empty(); br2=sorted(br2,key=lambda x:x["beta"]); st.session_state.beta_data=br2
    if st.session_state.beta_data:
        for b in st.session_state.beta_data:
            bl,bc3=get_beta_label(b["beta"]); rc="#00ff88" if b["rs5"]>0 else "#ff3d5a"
            w=min(100,int(abs(b["beta"])*50))
            st.markdown(f"""<div style="background:#0d1117;border:1px solid #1c2533;border-radius:8px;padding:12px 16px;margin-bottom:8px;border-left:4px solid {bc3};">
              <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;">
                <div style="flex:2;"><div style="font-family:Space Mono,monospace;font-size:11px;font-weight:700;color:#c9d1d9;">{b["sector"]}</div>
                  <div style="font-family:Space Mono,monospace;font-size:9px;color:#4a5568;">Corr: {b["corr"]} · 1M: {b["ret_1m_sec"]:+.1f}%</div></div>
                <div style="text-align:center;min-width:80px;"><div style="font-family:Space Mono,monospace;font-size:20px;font-weight:700;color:{bc3};">{b["beta"]}</div>
                  <div style="font-size:9px;color:{bc3};">{bl}</div></div>
                <div style="text-align:center;min-width:80px;"><div style="font-family:Space Mono,monospace;font-size:14px;font-weight:700;color:{rc};">{b["rs5"]:+.1f}%</div>
                  <div style="font-size:9px;color:#4a5568;">RS 5 Days</div></div>
              </div>
              <div style="height:4px;background:#1c2533;border-radius:2px;margin-top:10px;overflow:hidden;">
                <div style="width:{w}%;height:100%;background:{bc3};border-radius:2px;"></div>
              </div>
            </div>""",unsafe_allow_html=True)
    if not st.session_state.sector_data:
        st.markdown('<div style="text-align:center;padding:48px;color:#4a5568;font-family:Space Mono,monospace;"><div style="font-size:32px;margin-bottom:12px;">🏭</div><div>KLIK REFRESH SECTORS</div></div>',unsafe_allow_html=True)

# ════ TAB GAP UP ════
with tab_gapup:
    st.markdown('<div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-bottom:14px;padding:10px 14px;background:#0d1117;border-radius:6px;border-left:3px solid #00ff88;">Deteksi saham IDX berpotensi <b style="color:#00ff88">Gap Up</b> besok pagi.</div>',unsafe_allow_html=True)
    gc1,gc2=st.columns(2)
    with gc1: gms=st.slider("Min Gap Score",1,6,3,key="gu_score")
    with gc2: gq=st.toggle("⚡ Quick Scan (200)",value=True,key="gu_quick")
    do_gu=st.button("📈 SCAN GAP UP",type="primary",use_container_width=True,key="btn_gapup")
    if do_gu:
        gtk=tuple(stocks_yf[:200]) if gq else tuple(stocks_yf)
        with st.spinner(f"Scanning {len(gtk)} saham..."):
            gr=scan_gap_up(gtk); gr=[r for r in gr if r["Gap Score"]>=gms]
            st.session_state.gapup_results=gr
        if gr and TOKEN and CHAT_ID:
            ng=datetime.now(jakarta_tz); sep="━"*28
            msg=f"📈 *GAP UP SCANNER IDX*\n⏰ `{ng.strftime('%H:%M:%S')} WIB`\n{sep}\n"
            for r in gr[:5]:
                msg+=(f"\n🚀 *{r['Ticker']}* `{r['Signal']}`\n   💰 `{r['Price']:,}` ({r['Chg %']:+.1f}%)\n   📊 Gap Score:`{r['Gap Score']}/6` RVOL:`{r['RVOL']}x`\n   💡 _{r['Reasons'][:50]}_\n")
            msg+=f"\n{sep}\n⚠️ _BUKAN saran investasi!_"
            try:
                requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",data={"chat_id":CHAT_ID,"text":msg,"parse_mode":"Markdown"},timeout=10)
                st.success("📡 Gap Up alert terkirim!")
            except: pass
    gr=st.session_state.gapup_results
    if gr:
        gc3=[r for r in gr if "GAP UP" in r.get("Signal","")]; gp=[r for r in gr if "POTENTIAL" in r.get("Signal","")]
        st.markdown(f"""<div class="metric-row">
          <div class="metric-card green"><div class="metric-label">Gap Confirmed 🚀</div><div class="metric-value">{len(gc3)}</div></div>
          <div class="metric-card amber"><div class="metric-label">Potential ⚡</div><div class="metric-value">{len(gp)}</div></div>
          <div class="metric-card"><div class="metric-label">Total</div><div class="metric-value">{len(gr)}</div></div>
        </div>""",unsafe_allow_html=True)
        gh='<div class="signal-grid">'
        for row in gr[:20]:
            si=int(min(row["Gap Score"],6)); bars="".join([f'<div class="sc-bar {"filled" if i<si else "empty"}" style="width:26px"></div>' for i in range(6)])
            ig="GAP UP" in row.get("Signal",""); sc5="#00ff88" if ig else "#ffb700"
            cc="#00ff88" if row["Chg %"]>0 else "#ff3d5a"
            gh+=f'''<div class="signal-card {'gacor' if ig else 'potensial'}">
              <div style="display:flex;justify-content:space-between;">
                <div><div class="sc-ticker">{row["Ticker"]}</div>
                <div class="sc-price" style="color:{cc}">{row["Price"]:,} ({row["Chg %"]:+.1f}%)</div></div>
                <div style="text-align:right"><div style="font-family:Space Mono,monospace;font-size:9px;color:#4a5568">GAP SCORE</div>
                <div style="font-family:Space Mono,monospace;font-size:22px;font-weight:700;color:{sc5}">{row["Gap Score"]}</div></div>
              </div>
              <div class="sc-signal" style="color:{sc5}">{row["Signal"]}</div>
              <div class="sc-bars">{bars}</div>
              <div class="sc-stats">
                <div class="sc-stat">RVOL <span>{row["RVOL"]}x</span></div>
                <div class="sc-stat">Close% <span>{row["Close Ratio"]:.0%}</span></div>
                <div class="sc-stat">PrevHigh <span>{row["Prev High"]:,}</span></div>
              </div>
              <div style="margin-top:8px;font-size:10px;color:#4a5568;font-family:Space Mono,monospace;">{row["Reasons"][:80]}</div>
            </div>'''
        gh+='</div>'; st.markdown(gh,unsafe_allow_html=True)
        df_gu=pd.DataFrame(gr)
        st.dataframe(df_gu,use_container_width=True,hide_index=True,column_config={
            "Gap Score":st.column_config.ProgressColumn("Gap Score",min_value=0,max_value=6,format="%.1f"),
            "RVOL":st.column_config.NumberColumn("RVOL",format="%.2fx"),
            "Chg %":st.column_config.NumberColumn("Chg %",format="%.2f%%"),
        })
    elif not do_gu:
        st.markdown('<div style="text-align:center;padding:48px;color:#4a5568;font-family:Space Mono,monospace;"><div style="font-size:32px;margin-bottom:12px;">📈</div><div>KLIK SCAN GAP UP</div></div>',unsafe_allow_html=True)

# ════ TAB TRAILING STOP ════
with tab_trail:
    st.markdown('<div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-bottom:14px;padding:10px 14px;background:#0d1117;border-radius:6px;border-left:3px solid #bf5fff;">Lock profit. ATR 2x=scalping · ATR 3x=swing · Persen=fixed trail.</div>',unsafe_allow_html=True)
    tc1,tc2=st.columns(2)
    with tc1:
        st.markdown('<div class="settings-label">POSISI LO</div>',unsafe_allow_html=True)
        tr_t=st.text_input("Ticker IDX (tanpa .JK)",value="BBCA",key="tr_ticker").upper()
        tr_e=st.number_input("Harga Entry (Rp)",value=9000,step=50,key="tr_entry")
        tr_q=st.number_input("Lot (1 lot=100 lembar)",value=10,step=1,key="tr_qty")
    with tc2:
        st.markdown('<div class="settings-label">TRAILING SETTINGS</div>',unsafe_allow_html=True)
        tr_m=st.radio("Method",["ATR","Persen","Swing Low"],key="tr_method")
        if tr_m=="ATR":      tr_am=st.slider("ATR Multiplier",1.0,5.0,2.0,0.5,key="tr_atr_m")
        elif tr_m=="Persen": tr_p=st.slider("Trailing %",1.0,10.0,3.0,0.5,key="tr_pct")
        tr_al=st.toggle("🔔 Telegram Alert",value=True,key="tr_alert")
    if st.button("🎯 CALCULATE TRAILING STOP",type="primary",use_container_width=True,key="btn_trail"):
        with st.spinner(f"Fetching {tr_t}..."):
            try:
                df_tr=_fetch_ticker(tr_t+".JK","7d","15m")
                if df_tr is not None and len(df_tr)>=20:
                    df_tr=apply_indicators(df_tr)
                    cur=float(df_tr["Close"].iloc[-1]); atr_v=float(df_tr["ATR"].iloc[-1])
                    if tr_m=="ATR":      res2=calc_trailing_stop(tr_e,cur,atr_v,"ATR",tr_am)
                    elif tr_m=="Persen": res2=calc_trailing_stop(tr_e,cur,atr_v,"Persen",pct=tr_p)
                    else:                res2=calc_trailing_stop(tr_e,cur,atr_v,"Swing Low")
                    stop=res2["stop"]; dist=res2["distance"]
                    pf=res2["profit_float"]; pl=res2["profit_locked"]; ip=res2["is_profitable"]
                    prp=(cur-tr_e)*tr_q*100; lrp=max(0,(stop-tr_e)*tr_q*100)
                    sc6="#00ff88" if ip else "#ff3d5a"; pc2="#00ff88" if prp>=0 else "#ff3d5a"
                    st.markdown(f"""<div style="background:#0d1117;border:1px solid {sc6}44;border-radius:10px;padding:20px;margin-top:12px;">
                      <div class="metric-row">
                        <div class="metric-card"><div class="metric-label">Harga Sekarang</div>
                          <div class="metric-value" style="color:#00e5ff">{cur:,.0f}</div><div class="metric-sub">ATR: {atr_v:,.0f}</div></div>
                        <div class="metric-card" style="border-top-color:{sc6}"><div class="metric-label">🎯 Trailing Stop</div>
                          <div class="metric-value" style="color:{sc6}">{stop:,.0f}</div><div class="metric-sub">Distance: {dist:,.0f}</div></div>
                        <div class="metric-card" style="border-top-color:{pc2}"><div class="metric-label">Float P&L</div>
                          <div class="metric-value" style="color:{pc2}">{pf:+.1f}%</div><div class="metric-sub">Rp {prp:,.0f}</div></div>
                        <div class="metric-card" style="border-top-color:#00ff88"><div class="metric-label">Locked 🔒</div>
                          <div class="metric-value" style="color:#00ff88">{pl:+.1f}%</div><div class="metric-sub">Rp {lrp:,.0f}</div></div>
                      </div>
                      <div style="margin-top:12px;font-family:Space Mono,monospace;font-size:10px;color:#4a5568;">
                        💼 {tr_q} lot · {"✅ Profit terkunci!" if ip else "⚠️ Stop di bawah entry"}
                      </div>
                    </div>""",unsafe_allow_html=True)
                    if tr_al and TOKEN and CHAT_ID:
                        nt=datetime.now(jakarta_tz)
                        mt=(f"🎯 *TRAILING STOP UPDATE*\n⏰ `{nt.strftime('%H:%M:%S')} WIB`\n{"━"*28}\n"
                            f"📌 *{tr_t}* | {tr_m}\n💰 Entry: `{tr_e:,}` → Now: `{cur:,.0f}`\n"
                            f"🎯 Stop: `{stop:,.0f}` | Locked: `{pl:+.1f}%` (Rp {lrp:,.0f})\n"
                            f"📊 Float: `{pf:+.1f}%` (Rp {prp:,.0f})\n{"━"*28}\n⚠️ _BUKAN saran investasi!_")
                        try:
                            requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",data={"chat_id":CHAT_ID,"text":mt,"parse_mode":"Markdown"},timeout=10)
                            st.success("📡 Alert terkirim!")
                        except: pass
                else:
                    st.error(f"Data {tr_t} tidak tersedia.")
            except Exception as ex:
                st.error(f"Error: {str(ex)[:80]}")

# ════ TAB BACKTEST ════
with tab_backtest:
    st.markdown('<div class="section-title">Backtest Engine · 15M Intraday · IDX</div>',unsafe_allow_html=True)
    bt1,bt2,bt3,bt4=st.columns(4)
    bt_mode=bt1.selectbox("Mode",["Scalping ⚡","Momentum 🚀","Reversal 🎯","Bagger 💎"],key="bt_mode")
    bt_sc2=bt2.slider("Min Score",0,6,4,key="bt_sc")
    bt_fwd=int(bt3.number_input("Hold (bars)",value=4,step=1,min_value=1,max_value=20))
    bt_sl=bt4.number_input("SL mult (xATR)",value=0.8,step=0.1,min_value=0.1,max_value=3.0)
    st.caption(f"Hold {bt_fwd} bars × 15 min = ~{bt_fwd*15} menit per trade")
    if st.button("🚀 Run Backtest",type="primary",key="bt_run"):
        dd=st.session_state.get("data_dict",{})
        if not dd: st.warning("Run Scanner dulu bro!")
        else:
            bt_r=[]; bt_tr={"▲ UP":[],"▼ DOWN":[],"◆ SIDE":[]}
            bt_ses={"Pagi 09-11":[],"Siang 11-14":[],"Sore 14-16":[]}
            bt_sc3={4:[],5:[],6:[]}
            bpb2=st.progress(0); sample=list(dd.keys())[:80]
            for bi,tk in enumerate(sample):
                bpb2.progress((bi+1)/len(sample))
                try:
                    d=dd[tk].copy()
                    if len(d)<60: continue
                    d=apply_indicators(d)
                    for ii in range(50,len(d)-bt_fwd):
                        r0=d.iloc[ii]; r1=d.iloc[ii-1]; r2=d.iloc[ii-2]
                        if bt_mode=="Scalping ⚡":   sc7,_,_=score_scalping(r0,r1,r2)
                        elif bt_mode=="Momentum 🚀": sc7,_,_=score_momentum(r0,r1,r2)
                        elif bt_mode=="Bagger 💎":   sc7,_,_=score_bagger(r0,r1,r2,d.iloc[:ii+1])
                        else:                         sc7,_,_=score_reversal(r0,r1,r2)
                        if sc7<bt_sc2: continue
                        en=float(r0["Close"]); av=float(r0["ATR"]) if not np.isnan(float(r0["ATR"])) else en*0.005
                        if bt_mode=="Scalping ⚡":   tp2=en+1.5*av; sl2=en-bt_sl*av
                        elif bt_mode=="Momentum 🚀": tp2=en+2.0*av; sl2=en-bt_sl*av
                        elif bt_mode=="Bagger 💎":   tp2=en+3.0*av; sl2=en-1.0*av
                        else:                         tp2=en+2.5*av; sl2=en-bt_sl*av
                        ex=float(d.iloc[ii+bt_fwd]["Close"])
                        for fi in range(1,bt_fwd+1):
                            bar=d.iloc[ii+fi]
                            if float(bar["High"])>=tp2: ex=tp2; break
                            if float(bar["Low"])<=sl2:  ex=sl2; break
                        ret=(ex-en)/en*100; bt_r.append(ret)
                        e9=float(r0["EMA9"]); e21=float(r0["EMA21"]); e50=float(r0["EMA50"])
                        tr4="▲ UP" if e9>e21>e50 else("▼ DOWN" if e9<e21<e50 else "◆ SIDE")
                        bt_tr[tr4].append(ret)
                        try:
                            hr=d.index[ii].hour
                            if 9<=hr<11:    bt_ses["Pagi 09-11"].append(ret)
                            elif 11<=hr<14: bt_ses["Siang 11-14"].append(ret)
                            elif 14<=hr<16: bt_ses["Sore 14-16"].append(ret)
                        except: pass
                        si2=int(sc7)
                        if si2 in bt_sc3: bt_sc3[si2].append(ret)
                except: continue
            bpb2.empty()
            if not bt_r: st.warning("Tidak ada trade yang match.")
            else:
                arr=np.array(bt_r); wr=len(arr[arr>0])/len(arr)*100
                avg=np.mean(arr); med=np.median(arr)
                pf=arr[arr>0].sum()/max(abs(arr[arr<0].sum()),0.01)
                mxdd=arr[arr<0].min() if len(arr[arr<0])>0 else 0
                st.markdown(f"""<div class="bt-result">
                  <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;letter-spacing:2px;margin-bottom:14px;">
                    {len(arr)} TRADES · SCORE≥{bt_sc2} · HOLD {bt_fwd} BARS (~{bt_fwd*15}M) · {bt_mode}
                  </div>
                  <div style="display:flex;flex-wrap:wrap;">
                    <span class="bt-metric"><div class="bt-metric-val" style="color:{'#00ff88' if wr>=55 else '#ffb700' if wr>=50 else '#ff3d5a'}">{wr:.1f}%</div><div class="bt-metric-lbl">Win Rate</div></span>
                    <span class="bt-metric"><div class="bt-metric-val" style="color:{'#00ff88' if avg>0 else '#ff3d5a'}">{avg:+.2f}%</div><div class="bt-metric-lbl">Avg Return</div></span>
                    <span class="bt-metric"><div class="bt-metric-val" style="color:#00e5ff">{med:+.2f}%</div><div class="bt-metric-lbl">Median</div></span>
                    <span class="bt-metric"><div class="bt-metric-val" style="color:{'#00ff88' if pf>=1.5 else '#ffb700' if pf>=1 else '#ff3d5a'}">{pf:.2f}x</div><div class="bt-metric-lbl">Profit Factor</div></span>
                    <span class="bt-metric"><div class="bt-metric-val" style="color:#ff3d5a">{mxdd:.1f}%</div><div class="bt-metric-lbl">Max Loss</div></span>
                    <span class="bt-metric"><div class="bt-metric-val" style="color:#00ff88">{sum(1 for x in bt_r if x>0)}</div><div class="bt-metric-lbl">TP Hits</div></span>
                    <span class="bt-metric"><div class="bt-metric-val" style="color:#ff3d5a">{sum(1 for x in bt_r if x<0)}</div><div class="bt-metric-lbl">SL Hits</div></span>
                  </div>
                </div>""",unsafe_allow_html=True)
                ttr,tse,tsc=st.tabs(["📈 Per Trend","⏰ Per Sesi","🎯 Per Score"])
                with ttr:
                    for tn,vals in bt_tr.items():
                        if not vals: continue
                        a=np.array(vals); wr2=len(a[a>0])/len(a)*100; avg2=np.mean(a)
                        col="#00ff88" if wr2>=55 else("#ffb700" if wr2>=50 else "#ff3d5a")
                        st.markdown(f'<div style="margin-bottom:10px;"><div style="display:flex;justify-content:space-between;"><span style="font-family:Space Mono,monospace;font-size:12px;color:#c9d1d9;">{tn}</span><span style="font-family:Space Mono,monospace;font-size:11px;color:{col};">{wr2:.1f}% WR · avg {avg2:+.2f}% · {len(a)} trades</span></div><div style="height:8px;background:var(--border);border-radius:4px;overflow:hidden;margin-top:4px;"><div style="width:{int(wr2)}%;height:100%;background:{col};border-radius:4px;"></div></div></div>',unsafe_allow_html=True)
                with tse:
                    for sn3,vals in bt_ses.items():
                        if not vals: continue
                        a=np.array(vals); wr3=len(a[a>0])/len(a)*100; avg3=np.mean(a)
                        col="#00ff88" if wr3>=55 else("#ffb700" if wr3>=50 else "#ff3d5a")
                        st.markdown(f'<div style="margin-bottom:10px;"><div style="display:flex;justify-content:space-between;"><span style="font-family:Space Mono,monospace;font-size:12px;color:#c9d1d9;">⏰ {sn3}</span><span style="font-family:Space Mono,monospace;font-size:11px;color:{col};">{wr3:.1f}% WR · avg {avg3:+.2f}% · {len(a)} trades</span></div><div style="height:8px;background:var(--border);border-radius:4px;overflow:hidden;margin-top:4px;"><div style="width:{int(wr3)}%;height:100%;background:{col};border-radius:4px;"></div></div></div>',unsafe_allow_html=True)
                with tsc:
                    for sl3 in [4,5,6]:
                        vals=bt_sc3.get(sl3,[])
                        if not vals: continue
                        a=np.array(vals); wr4=len(a[a>0])/len(a)*100; avg4=np.mean(a)
                        col="#00ff88" if wr4>=55 else("#ffb700" if wr4>=50 else "#ff3d5a")
                        st.markdown(f'<div style="margin-bottom:10px;"><div style="display:flex;justify-content:space-between;"><span style="font-family:Space Mono,monospace;font-size:12px;color:#c9d1d9;">Score {sl3} [{"█"*sl3+"░"*(6-sl3)}]</span><span style="font-family:Space Mono,monospace;font-size:11px;color:{col};">{wr4:.1f}% WR · avg {avg4:+.2f}% · {len(a)} trades</span></div><div style="height:8px;background:var(--border);border-radius:4px;overflow:hidden;margin-top:4px;"><div style="width:{int(wr4)}%;height:100%;background:{col};border-radius:4px;"></div></div></div>',unsafe_allow_html=True)

# ════ FOOTER + AUTO-REFRESH ════
_nf=datetime.now(jakarta_tz).timestamp()
if st.session_state.last_scan_time:
    _r2=max(0,300-(_nf-st.session_state.last_scan_time)); m2=int(_r2//60); s2=int(_r2%60)
    _lt2=datetime.fromtimestamp(st.session_state.last_scan_time,jakarta_tz).strftime("%H:%M:%S")
    tinfo=f"⏱️ Next: <span style='color:#ff7b00'>{m2:02d}:{s2:02d}</span> · Last: <span style='color:#2dd4bf'>{_lt2} WIB</span>"
else:
    tinfo="⏱️ Klik Scan untuk mulai"

st.markdown(f"""
<div style="margin-top:28px;padding-top:14px;border-top:1px solid #1c2533;display:flex;justify-content:space-between;flex-wrap:wrap;gap:8px;">
  <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;">🔥 Theta Turbo v5.3 · IDX · 15M+Daily · Zero Rate Limit ✅</div>
  <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;">{tinfo}</div>
</div>
<div style="font-family:Space Mono,monospace;font-size:9px;color:#2d3748;text-align:center;margin-top:8px;">⚠️ BUKAN saran investasi · Untuk tujuan edukasi · DYOR selalu</div>""",unsafe_allow_html=True)

if st.session_state.last_scan_time:
    if datetime.now(jakarta_tz).timestamp()-st.session_state.last_scan_time>=295:
        time.sleep(5); st.rerun()
