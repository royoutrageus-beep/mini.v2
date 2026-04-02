import yfinance as yf
import pandas as pd
import streamlit as st
import time
import requests
import pytz
import numpy as np
from datetime import datetime

# --- CONFIG ---
TOKEN = st.secrets["TELEGRAM_TOKEN"]
CHAT_ID = st.secrets["TELEGRAM_CHAT_ID"]
jakarta_tz = pytz.timezone('Asia/Jakarta')

st.set_page_config(layout="wide", page_title="Theta Turbo V4.0", page_icon="⚡")

# --- FUNGSI INDIKATOR MANUAL ---
def calculate_indicators(df):
    # EMA 20, 50, 100
    df['EMA20'] = df['Close'].ewm(span=20, adjust=False).mean()
    df['EMA50'] = df['Close'].ewm(span=50, adjust=False).mean()
    df['EMA100'] = df['Close'].ewm(span=100, adjust=False).mean()
    
    # RSI (14)
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))
    df['RSI_EMA'] = df['RSI'].ewm(span=14, adjust=False).mean()
    
    # MACD (12, 26, 9)
    exp1 = df['Close'].ewm(span=12, adjust=False).mean()
    exp2 = df['Close'].ewm(span=26, adjust=False).mean()
    df['MACD'] = exp1 - exp2
    df['MACD_Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
    df['MACDH'] = df['MACD'] - df['MACD_Signal']
    
    # Volume Average
    df['Vol_Avg'] = df['Volume'].rolling(window=20).mean()
    return df

def send_telegram(message):
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}, timeout=5)
    except: pass

# --- UI SIDEBAR ---
st.sidebar.title("🎮 Command Center")
tele_notif = st.sidebar.checkbox("🚀 Kirim Notif Telegram", value=False)
min_score_filter = st.sidebar.slider("Minimal Score Tampil", 2, 4, 3)
vol_threshold = st.sidebar.slider("Vol Spike Threshold (x)", 1.0, 5.0, 1.5)

# --- DATABASE LIST ---
list_saham = ["GOTO.JK", "BUKA.JK", "EMTK.JK", "INET.JK", "MLPT.JK", "DCII.JK", "ATIC.JK", "GLVA.JK", "MTDL.JK", "WIFI.JK", "LUCK.JK", "AWAN.JK", "CHIP.JK", "ELIT.JK", "CYBR.JK", "GALB.JK", "IRSX.JK", "LUCY.JK", "METI.JK", "NINE.JK", "BBCA.JK", "BBRI.JK", "BMRI.JK", "BBNI.JK", "ARTO.JK", "BRIS.JK", "BBTN.JK", "BDMN.JK", "PNBN.JK", "BJBR.JK", "BJTM.JK", "BNLI.JK", "BVIC.JK", "MEGA.JK", "BNGA.JK", "ADMF.JK", "CFIN.JK", "BBYB.JK", "BINA.JK", "DNAR.JK", "AGRO.JK", "BABP.JK", "BACA.JK", "BAEK.JK", "BCIC.JK", "BEKS.JK", "BGTG.JK", "MAYA.JK", "MCOR.JK", "NISP.JK", "NOBU.JK", "PNBS.JK", "SDRA.JK", "VICI.JK", "AMAR.JK", "MASB.JK", "ADRO.JK", "PTBA.JK", "ITMG.JK", "HRUM.JK", "INDY.JK", "MEDC.JK", "ENRG.JK", "PGAS.JK", "AKRA.JK", "DOID.JK", "BUMI.JK", "RMKE.JK", "ELSA.JK", "ADMR.JK", "MBMA.JK", "KKGI.JK", "GEMS.JK", "SGER.JK", "BYAN.JK", "RAJA.JK", "APEX.JK", "ARTI.JK", "BIPI.JK", "BOSS.JK", "DEWA.JK", "TOBA.JK", "IATA.JK", "INPS.JK", "JSKY.JK", "KOPI.JK", "MBSS.JK", "MCOL.JK", "MITI.JK", "MTFN.JK", "MYOH.JK", "PKPK.JK", "RUIS.JK", "SURE.JK", "WOWS.JK", "TEBE.JK", "UNVR.JK", "ICBP.JK", "INDF.JK", "AMRT.JK", "MIDI.JK", "CPIN.JK", "JPFA.JK", "MAIN.JK", "MYOR.JK", "GGRM.JK", "HMSP.JK", "DSNG.JK", "AALI.JK", "LSIP.JK", "TAPG.JK", "STAA.JK", "TBLA.JK", "CLEO.JK", "ROTI.JK", "WMPP.JK", "ADES.JK", "AISA.JK", "ALTO.JK", "ANDI.JK", "BEEF.JK", "CAMP.JK", "CEKA.JK", "DLTA.JK", "FOOD.JK", "GOOD.JK", "HOKI.JK", "IKAN.JK", "KEJU.JK", "MLBI.JK", "PCAR.JK", "PSDN.JK", "SKBM.JK", "SKLT.JK", "STTP.JK", "ULTJ.JK", "MAPI.JK", "ACES.JK", "ERAA.JK", "ASII.JK", "SMSM.JK", "IMAS.JK", "GJTL.JK", "MNCN.JK", "SCMA.JK", "RALS.JK", "LPPF.JK", "PNLF.JK", "MAPA.JK", "AUTO.JK", "MASA.JK", "PANI.JK", "BIRD.JK", "FILM.JK", "FORZ.JK", "GLOB.JK", "HERO.JK", "HOME.JK", "HOTL.JK", "ICON.JK", "KBLV.JK", "LPPS.JK", "MICE.JK", "MPPA.JK", "MSIN.JK", "PBSA.JK", "RICY.JK", "TARA.JK", "UNIT.JK", "WOOD.JK", "ZINC.JK", "TOSK.JK", "VIVA.JK", "KDTN.JK", "BELI.JK", "KLBF.JK", "MIKA.JK", "HEAL.JK", "SILO.JK", "PRDA.JK", "SAME.JK", "PEHA.JK", "PYFA.JK", "IRRA.JK", "KAEF.JK", "INAF.JK", "DGNS.JK", "BMHS.JK", "TSPC.JK", "DVLA.JK", "MERK.JK", "SIDO.JK", "SOHO.JK", "PRIM.JK", "RSGK.JK", "TLKM.JK", "ISAT.JK", "EXCL.JK", "JSMR.JK", "BREN.JK", "POWR.JK", "KEEN.JK", "ADHI.JK", "PTPP.JK", "WIKA.JK", "WKTK.JK", "META.JK", "TOWR.JK", "TBIG.JK", "PGEO.JK", "BRPT.JK", "FREN.JK", "LINK.JK", "BALI.JK", "BUKK.JK", "CASS.JK", "CENT.JK", "CMNP.JK", "GAMA.JK", "GHON.JK", "GOLD.JK", "IBST.JK", "IPCC.JK", "JKON.JK", "KARE.JK", "LAPD.JK", "MANT.JK", "NRCA.JK", "OASA.JK", "PBSA.JK", "PORT.JK", "SSIA.JK", "SUPR.JK", "TELE.JK", "TOPS.JK", "UNTR.JK", "ARNA.JK", "ASGR.JK", "IMPC.JK", "MLIA.JK", "HEXA.JK", "GMFI.JK", "BPTR.JK", "ABMM.JK", "WOOD.JK", "KMTR.JK", "SPTO.JK", "VOKS.JK", "AMFG.JK", "APLI.JK", "BRAM.JK", "DYAN.JK", "IKAI.JK", "JECC.JK", "KBLI.JK", "KBLM.JK", "LION.JK", "LMSH.JK", "PICO.JK", "PRAS.JK", "SCCO.JK", "SIPD.JK", "SULI.JK", "TALF.JK", "TIRT.JK", "TPIA.JK", "ANTM.JK", "INCO.JK", "TINS.JK", "MDKA.JK", "SMGR.JK", "INTP.JK", "INKP.JK", "NCKL.JK", "ADMG.JK", "AVIA.JK", "ESSA.JK", "SRTG.JK", "AGII.JK", "ALDO.JK", "ALKA.JK", "BAJA.JK", "BTON.JK", "CTBN.JK", "DPNS.JK", "EKAD.JK", "ETWA.JK", "GDST.JK", "IAAS.JK", "IGAR.JK", "INAI.JK", "INCI.JK", "ISSP.JK", "KBRI.JK", "KDSI.JK", "NIKL.JK", "JIHD.JK", "SMDR.JK", "SMMT.JK", "SPMA.JK", "TOTO.JK", "BSDE.JK", "PWON.JK", "SMRA.JK", "CTRA.JK", "ASRI.JK", "MKPI.JK", "DILD.JK", "LPCK.JK", "LPKR.JK", "DMAS.JK", "BEST.JK", "KIJA.JK", "MTLA.JK", "JRPT.JK", "ADCP.JK", "AMAN.JK", "APLN.JK", "ARMY.JK", "BAPA.JK", "BAPI.JK", "BBSS.JK", "BCIP.JK", "BIPP.JK", "BKDP.JK", "BKSL.JK", "COCO.JK", "CPRI.JK", "CSIS.JK", "DUTI.JK", "ELTY.JK", "EMDE.JK", "FMII.JK", "GMTD.JK", "GPRA.JK", "GWSA.JK", "HDIT.JK", "INPP.JK", "ASSA.JK", "TMAS.JK", "GIAA.JK", "NELY.JK", "BLUE.JK", "PSSI.JK", "ELPI.JK", "HUMI.JK", "JAYA.JK", "SDMU.JK", "AKSI.JK", "BESS.JK", "BPTR.JK", "COAL.JK", "GTSI.JK", "HELI.JK", "HOPE.JK", "KAYU.JK", "MIRA.JK", "SAFE.JK", "SAPX.JK", "SHIP.JK", "TNCA.JK", "TRUK.JK", "AYLS.JK", "BNBR.JK", "NZIA.JK", "GSMF.JK", "RGAS.JK", "YPAS.JK", "TOOL.JK", "OILS.JK", "BAIK.JK", "ASPR.JK", "CGAS.JK", "EURO.JK", "AIMS.JK", "ASPI.JK", "BELL.JK", "ZYRX.JK", "BRMS.JK", "POLI.JK", "ARCI.JK", "HRTA.JK", "EMAS.JK", "RLCO.JK", "CUAN.JK", "CDIA.JK", "PTRO.JK", "BUVA.JK", "MINA.JK", "PADI.JK", "BRNA.JK", "AKPI.JK", "ESIP.JK", "IPOL.JK", "PACK.JK", "PBID.JK", "JARR.JK", "PGUN.JK", "UANG.JK", "FAST.JK", "PPRE.JK", "ALII.JK", "ERAL.JK", "DATA.JK", "DOOH.JK", "KIOS.JK", "PBRX.JK", "TRIS.JK", "NETV.JK", "INOV.JK", "PSAB.JK", "COIN.JK", "MDIA.JK", "BULL.JK", "SINI.JK", "UNIQ.JK", "ACRO.JK", "CBDK.JK", "ESTI.JK", "ERTX.JK", "OKAS.JK", "IFII.JK", "SOCI.JK", "PDPP.JK", "RATU.JK", "JGLE.JK", "PSKT.JK", "BBHI.JK", "KUAS.JK", "RMKO.JK", "CLAY.JK", "ENAK.JK", "VKTR.JK", "PART.JK", "UNSP.JK", "ZATA.JK", "AMMN.JK", "TKIM.JK", "KRAS.JK", "NICL.JK", "DKFT.JK", "FORE.JK", "FPNI.JK", "SOLA.JK", "SMBR.JK", "SMGA.JK", "WTON.JK", "DAAZ.JK", "CHEM.JK", "BSBK.JK", "DKHH.JK", "OPMS.JK", "SSMS.JK", "MINE.JK", "NICE.JK", "PPRI.JK", "NPGF.JK", "SRSN.JK", "CITA.JK", "UDNG.JK", "SMLE.JK", "DGWG.JK", "KAQI.JK", "CLPI.JK", "MDKI.JK", "BLES.JK", "IFSH.JK", "BATR.JK", "FWCT.JK", "GGRP.JK", "TBMS.JK", "INCF.JK", "SAMF.JK", "SWID.JK", "LTLS.JK", "OBMD.JK", "UNIC.JK", "SMKL.JK", "CMNT.JK", "KKES.JK", "YELO.JK", "AADI.JK", "CBRE.JK", "LEAD.JK", "BSSR.JK", "ATLA.JK", "FIRE.JK", "DSSA.JK", "BBRM.JK", "PSAT.JK", "MAHA.JK", "TPMA.JK", "BOAT.JK", "WINS.JK", "SICO.JK", "MBAP.JK", "BSML.JK", "MEJA.JK", "ITMA.JK", "DWGL.JK", "GTBO.JK", "ARII.JK", "MKAP.JK", "RIGS.JK", "CANI.JK", "PTIS.JK", "SUNI.JK", "GZCO.JK", "BWPT.JK", "ASHA.JK", "CPRO.JK", "WMUU.JK", "NASI.JK", "SIMP.JK", "SMAR.JK", "AYAM.JK", "DSFI.JK", "PTPS.JK", "NSSS.JK", "DEWI.JK", "ISEA.JK", "CMRY.JK", "ANJT.JK", "WAPO.JK", "JAWA.JK", "CSRA.JK", "DPUM.JK", "NEST.JK", "GULA.JK", "IBOS.JK", "STRK.JK", "TAYS.JK", "PSGO.JK", "BISI.JK", "ENZO.JK", "GRPM.JK", "NAYZ.JK", "YUPI.JK", "TLDN.JK", "MKTR.JK", "CRAB.JK", "FISH.JK", "BOBA.JK", "SUPA.JK", "BBKP.JK", "INPC.JK", "TRUE.JK", "MHKI.JK", "LAJU.JK"]
list_saham = list(set(list_saham))

# --- ENGINE ---
st.title("⚡ Theta Turbo - High Speed Scanner")
st.write(f"Status: **Scanning {len(list_saham)} Saham** | Update: {datetime.now(jakarta_tz).strftime('%H:%M:%S')} WIB")

# JURUS ANTI RATE LIMIT: NYICIL DATA
@st.cache_data(ttl=300)
def fetch_data_secure(tickers):
    all_dfs = {}
    chunk_size = 20
    p_bar = st.progress(0)
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        try:
            data = yf.download(chunk, period="10d", interval="15m", group_by='ticker', progress=False, threads=True)
            for t in chunk:
                if t in data.columns.get_level_values(0):
                    df_single = data[t].dropna()
                    if not df_single.empty:
                        all_dfs[t] = df_single
        except: pass
        
        p_bar.progress(min((i + chunk_size) / len(tickers), 1.0))
        time.sleep(0.3) # Jeda nafas buat Yahoo
    
    p_bar.empty()
    return all_dfs

data_dict = fetch_data_secure(list_saham)

if data_dict:
    results = []
    for ticker, df in data_dict.items():
        try:
            if len(df) < 50: continue
            df = calculate_indicators(df)
            last = df.iloc[-1]
            prev = df.iloc[-2]

            score = 0
            if last['RSI'] > last['RSI_EMA'] and prev['RSI'] <= prev['RSI_EMA']: score += 1
            if last['MACDH'] > 0 and last['MACDH'] > prev['MACDH']: score += 1
            if last['Close'] > last['EMA20'] > last['EMA50']: score += 1
            if last['Volume'] > (last['Vol_Avg'] * vol_threshold): score += 1

            if score >= min_score_filter:
                v_ratio = last['Volume']/last['Vol_Avg']
                results.append({
                    "Symbol": ticker.replace(".JK", ""),
                    "Price": int(last['Close']),
                    "Score": f"{score}/4",
                    "Status": "🚀 GACOR" if score == 4 else "🔥 POTENSIAL",
                    "RSI": round(last['RSI'], 2),
                    "V-Ratio": f"{v_ratio:.2f}x",
                    "EMA_Trend": "Bullish" if last['Close'] > last['EMA100'] else "Retrace"
                })

                if score == 4 and tele_notif:
                    msg = (f"⚡ *THETA SIGNAL: {ticker}* ⚡\nPrice: `{int(last['Close'])}` | Score: *4/4*")
                    send_telegram(msg)
        except: continue

    if results:
        res_df = pd.DataFrame(results).sort_values("Score", ascending=False)
        st.dataframe(res_df, width=1500) # Ganti ke width fix biar gak error warning
    else:
        st.warning("Belum ada signal valid. Standby Bro...")

# --- AUTO REFRESH ---
st.divider()
st.caption(f"Last Scan: {datetime.now(jakarta_tz).strftime('%H:%M:%S')} WIB | Refresh: 300s")
time.sleep(300) # Refresh tiap 5 menit biar aman dari ban
st.rerun()
