# ═══════════════════════════════════════════════════════════════
#  MESIN PRESISI v2.2 — SKILLS BUILD
#  Arjum (daily) + yfinance (intraday) + Patterns/Zeta/Div/SMC + Trading Plan
# ═══════════════════════════════════════════════════════════════
import math, yfinance as yf, pandas as pd, streamlit as st, time, random, requests, numpy as np, pytz, pickle, threading, os
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from scipy.signal import argrelextrema
from scipy.stats import linregress
from scipy.special import expit

def _get_secret(*keys, default=""):
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
try: DS_KEY = st.secrets.get("DATASECTORS_API_KEY", "")
except: DS_KEY = ""
DS_BASE = "https://api.datasectors.com"

def idx_tick(price):
    p=float(price)
    return 1 if p<200 else 2 if p<500 else 5 if p<2000 else 10 if p<5000 else 25

DEFAULT_COMM_RT=0.25
def trade_cost_pct(price, comm_rt=DEFAULT_COMM_RT):
    p=max(float(price),1.0)
    return comm_rt + idx_tick(p)/p*100

CACHE_DIR = Path("/tmp/hp_cache") if Path("/tmp").exists() else Path.home()/".hp_cache"
CACHE_DIR.mkdir(exist_ok=True)
CACHE_TTL=300
_mem={}; _mem_lock=threading.Lock()

def _ck(t,tf): return f"tt_{t}_{tf}"
def _disk_get(key):
    fp=CACHE_DIR/f"{key}.pkl"
    try:
        if fp.exists():
            d=pickle.loads(fp.read_bytes())
            if time.time()-d["ts"]<CACHE_TTL: return d["df"]
    except: pass
    return None
def _disk_set(key,df):
    try: (CACHE_DIR/f"{key}.pkl").write_bytes(pickle.dumps({"ts":time.time(),"df":df}))
    except: pass
def _cache_get(t,tf):
    key=_ck(t,tf)
    with _mem_lock:
        if key in _mem:
            ts,df=_mem[key]
            if time.time()-ts<CACHE_TTL: return df
    df=_disk_get(key)
    if df is not None:
        with _mem_lock: _mem[key]=(time.time(),df)
    return df
def _cache_set(t,tf,df):
    key=_ck(t,tf)
    with _mem_lock: _mem[key]=(time.time(),df)
    _disk_set(key,df)

# ════════════ ARJUM API ADAPTER ════════════
ARJUM_KEY=_get_secret("ARJUM_API_KEY")
ARJUM_BASE="https://stock.arjum.com"
ARJUM_DAILY_LIMIT=21000
ARJUM_DAILY_TTL=4*3600
_ARJUM_COUNTER=CACHE_DIR/"arjum_calls.pkl"
_arjum_ctr={"date":None,"count":0,"last_sync":0}
_arjum_lock=threading.Lock()

def arjum_usage():
    today=datetime.now(jakarta_tz).strftime("%Y-%m-%d")
    with _arjum_lock:
        if _arjum_ctr["date"]==today: return _arjum_ctr["count"]
    try:
        if _ARJUM_COUNTER.exists():
            d=pickle.loads(_ARJUM_COUNTER.read_bytes())
            if d.get("date")==today:
                with _arjum_lock:
                    _arjum_ctr["date"]=today; _arjum_ctr["count"]=int(d.get("count",0))
                return int(d.get("count",0))
    except: pass
    with _arjum_lock:
        _arjum_ctr["date"]=today; _arjum_ctr["count"]=0
    return 0

def _arjum_incr():
    today=datetime.now(jakarta_tz).strftime("%Y-%m-%d")
    with _arjum_lock:
        if _arjum_ctr["date"]!=today: _arjum_ctr["date"]=today; _arjum_ctr["count"]=0
        _arjum_ctr["count"]+=1
        cnt=_arjum_ctr["count"]; now=time.time()
        need_sync=(cnt%20==0) or (now-_arjum_ctr["last_sync"]>60)
        if need_sync: _arjum_ctr["last_sync"]=now
    if need_sync:
        try: _ARJUM_COUNTER.write_bytes(pickle.dumps({"date":today,"count":cnt}))
        except: pass

def _arjum_get(path, timeout=15):
    if not ARJUM_KEY: return None
    if arjum_usage()>=ARJUM_DAILY_LIMIT: return None
    try:
        r=requests.get(f"{ARJUM_BASE}{path}",
            headers={"X-API-Key":ARJUM_KEY,"Accept":"application/json"}, timeout=timeout)
        _arjum_incr()
        if r.status_code!=200: return None
        return r.json()
    except: return None

def _arjum_daily_cache_get(t):
    key=_ck(t,"arjum_daily")
    with _mem_lock:
        if key in _mem:
            ts,df=_mem[key]
            if time.time()-ts<ARJUM_DAILY_TTL: return df
    fp=CACHE_DIR/f"{key}.pkl"
    try:
        if fp.exists():
            d=pickle.loads(fp.read_bytes())
            if time.time()-d["ts"]<ARJUM_DAILY_TTL:
                with _mem_lock: _mem[key]=(d["ts"],d["df"])
                return d["df"]
    except: pass
    return None

def _arjum_daily_cache_set(t,df):
    key=_ck(t,"arjum_daily")
    with _mem_lock: _mem[key]=(time.time(),df)
    try: (CACHE_DIR/f"{key}.pkl").write_bytes(pickle.dumps({"ts":time.time(),"df":df}))
    except: pass

def _fetch_arjum_daily(ticker, limit=200, force_fresh=False):
    if not ARJUM_KEY: return None
    if not force_fresh:
        cached=_arjum_daily_cache_get(ticker)
        if cached is not None: return cached
    t=ticker.replace(".JK","").upper().strip()
    data=_arjum_get(f"/api/history/{t}")
    if not data or "rows" not in data: return None
    rows=data.get("rows") or []
    if len(rows)<2: return None
    try:
        df=pd.DataFrame(rows)
        rename={"date":"Date","open":"Open","high":"High","low":"Low","close":"Close",
            "volume":"Volume","value":"Value","f_buy":"FBuy","f_sell":"FSell",
            "n_foreign":"FNet","avg":"Avg","freq":"Freq"}
        df=df.rename(columns={k:v for k,v in rename.items() if k in df.columns})
        if "Close" not in df.columns: return None
        for c in ["Open","High","Low","Close","Volume","Value","FBuy","FSell","FNet"]:
            if c in df.columns: df[c]=pd.to_numeric(df[c],errors="coerce").fillna(0)
        if "Date" in df.columns:
            df["_dt"]=pd.to_datetime(df["Date"],errors="coerce")
            df=df.set_index("_dt").drop(columns=["Date"])
        df=df[df["Close"]>0].sort_index()
        if len(df)<2: return None
        if len(df)>limit: df=df.tail(limit)
        _arjum_daily_cache_set(ticker,df)
        return df
    except: return None

def fetch_arjum_daily_ohlcv(ticker, limit=200, force_fresh=False):
    return _fetch_arjum_daily(ticker, limit, force_fresh)

# ════════════ PHYSICS ENGINE ════════════
def ewma_sigma_daily(df, lam=0.94):
    try:
        c=df["Close"].astype(float).values; c=c[c>0]
        if len(c)<15: return None
        r=np.diff(np.log(c))
        var=float(np.var(r[:10])) if len(r)>=10 else float(r[0]**2)
        for x in r[10:]: var=lam*var+(1-lam)*x*x
        s=float(np.sqrt(max(var,0))*100)
        return s if s>0 else None
    except: return None

def tail_stats_daily(df):
    try:
        c=df["Close"].astype(float).values; c=c[c>0]
        if len(c)<30: return None
        r=np.diff(c)/c[:-1]*100
        m=float(np.mean(r)); sd=float(np.std(r))
        if sd<=0: return None
        worst=float(np.min(r)); p99=float(np.percentile(np.abs(r),99))
        gauss99=2.326*sd
        kurt=float(np.mean((r-m)**4)/max(sd**4,1e-12)-3.0)
        return {"sigma":sd,"worst":worst,"p99":p99,"tail_mult":p99/max(gauss99,1e-9),"kurt":kurt}
    except: return None

def adv20_rp(df, fallback=0.0):
    try:
        d=df.tail(20); v=(d["Close"].astype(float)*d["Volume"].astype(float)); v=v[v>0]
        if len(v)>=5: return float(v.mean())
    except: pass
    return float(fallback or 0.0)

def sqrt_impact(pos_rp, adv_rp, sigma_pct, Y=1.0):
    try:
        if not adv_rp or adv_rp<=0 or not sigma_pct: return None,None
        part=float(pos_rp)/float(adv_rp)
        return round(Y*sigma_pct*math.sqrt(max(part,0)),3), round(part*100,2)
    except: return None,None

def kelly_fraction(w, aw, al):
    try:
        if al<=0 or aw<=0: return None
        return float(w-(1-w)/(aw/al))
    except: return None

# ════════════ SIGNAL LOG ════════════
SIGNAL_LOG=CACHE_DIR/"signal_log.csv"
_SIGLOG_COLS=["ts","date","ticker","mode","score","signal","sinyal_v2","mesin_grade",
    "mesin_score","iq_verdict","iq_score","price","atr_pct","rvol","rsi_ema",
    "above_vwap","ema_stack","roc_atr","src","cost_pct","sigma_d","impact_pct",
    "part_pct","t1_ret","t3_ret","t5_ret"]
_SIGLOG_NUM=["score","mesin_score","iq_score","price","atr_pct","rvol","rsi_ema",
    "above_vwap","ema_stack","roc_atr","cost_pct","sigma_d","impact_pct","part_pct",
    "t1_ret","t3_ret","t5_ret"]
_siglog_lock=threading.Lock()

def _gsheet_ws():
    if "_gs_ws" in st.session_state: return st.session_state["_gs_ws"]
    ws=None
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        sid=str(_get_secret("GSHEET_SIGNAL_LOG_ID"))
        info=None
        try: info=dict(st.secrets["gcp_service_account"])
        except: info=None
        if sid and info:
            creds=Credentials.from_service_account_info(info,scopes=["https://www.googleapis.com/auth/spreadsheets"])
            gc=gspread.authorize(creds)
            sh=gc.open_by_key(sid)
            try: ws=sh.worksheet("signal_log")
            except:
                ws=sh.add_worksheet("signal_log",rows=4000,cols=len(_SIGLOG_COLS))
                ws.append_row(_SIGLOG_COLS)
            try:
                if ws.row_values(1)!=_SIGLOG_COLS: ws.update("A1",[_SIGLOG_COLS])
            except: pass
    except: ws=None
    st.session_state["_gs_ws"]=ws
    return ws

def siglog_backend(): return "☁️ GSheets" if _gsheet_ws() is not None else "💾 CSV lokal"

def _df_align(df):
    for c in _SIGLOG_COLS:
        if c not in df.columns: df[c]=np.nan
    df=df[_SIGLOG_COLS]
    for c in _SIGLOG_NUM: df[c]=pd.to_numeric(df[c],errors="coerce")
    return df

def load_signal_log():
    ws=_gsheet_ws()
    if ws is not None:
        try:
            vals=ws.get_all_values()
            if len(vals)>=2:
                df=pd.DataFrame(vals[1:],columns=vals[0]).replace("",np.nan)
                return _df_align(df)
            return pd.DataFrame(columns=_SIGLOG_COLS)
        except: pass
    try:
        if SIGNAL_LOG.exists(): return _df_align(pd.read_csv(SIGNAL_LOG))
    except: pass
    return pd.DataFrame(columns=_SIGLOG_COLS)

def save_signal_log(df):
    df=_df_align(df.copy())
    try:
        with _siglog_lock: df.to_csv(SIGNAL_LOG,index=False)
    except: pass
    ws=_gsheet_ws()
    if ws is not None:
        try:
            body=[_SIGLOG_COLS]+df.fillna("").astype(str).values.tolist()
            ws.clear(); ws.update("A1",body); return True
        except: return False
    return True

def log_signals_batch(rows):
    if not rows: return 0
    try:
        df=load_signal_log()
        existing=set()
        if not df.empty:
            existing=set(zip(df["ticker"].astype(str),df["date"].astype(str),df["mode"].astype(str)))
        new=[r for r in rows if (str(r["ticker"]),str(r["date"]),str(r["mode"])) not in existing]
        if not new: return 0
        ndf=_df_align(pd.DataFrame(new))
        allf=pd.concat([df,ndf],ignore_index=True) if not df.empty else ndf
        try:
            with _siglog_lock: _df_align(allf).to_csv(SIGNAL_LOG,index=False)
        except: pass
        ws=_gsheet_ws()
        if ws is not None:
            try: ws.append_rows(ndf.fillna("").astype(str).values.tolist())
            except: pass
        return len(new)
    except: return 0

def _fetch_daily_for_outcome(ticker):
    df=None
    if ARJUM_KEY: df=fetch_arjum_daily_ohlcv(ticker,limit=200,force_fresh=False)
    if df is None and DS_KEY: df=fetch_ds_ohlcv(ticker,"daily",100,False)
    if df is None: df=_fetch_yf_ticker(ticker+".JK","60d","1d")
    return df

def update_signal_outcomes(max_tickers=40):
    df=load_signal_log()
    if df.empty: return 0,"Log kosong — jalankan Scanner dulu."
    pend=df[df["t5_ret"].isna()].copy()
    if pend.empty: return 0,"Semua sinyal sudah punya outcome lengkap."
    updated=0
    for tkr in list(pend["ticker"].astype(str).unique())[:max_tickers]:
        dfd=_fetch_daily_for_outcome(tkr)
        if dfd is None or len(dfd)<2: continue
        try:
            closes=dfd["Close"].astype(float)
            ddates=[pd.Timestamp(x).date() for x in dfd.index]
        except: continue
        for idx in pend[pend["ticker"].astype(str)==tkr].index:
            try:
                sdate=pd.Timestamp(df.loc[idx,"date"]).date()
                entry=float(df.loc[idx,"price"]); cost=float(df.loc[idx,"cost_pct"])
                if entry<=0: continue
                pos=next((i for i,d0 in enumerate(ddates) if d0>sdate),None)
                if pos is None: continue
                def _ret(off):
                    j=pos+off
                    if j>=len(closes): return np.nan
                    return round((float(closes.iloc[j])-entry)/entry*100-cost,2)
                r1,r3,r5=_ret(0),_ret(2),_ret(4)
                if not np.isnan(r1): df.loc[idx,"t1_ret"]=r1; updated+=1
                if not np.isnan(r3): df.loc[idx,"t3_ret"]=r3
                if not np.isnan(r5): df.loc[idx,"t5_ret"]=r5
            except: continue
    save_signal_log(df)
    return updated,f"{updated} outcome T+1 di-update — tersimpan ke {siglog_backend()}."

# ════════════ DATASECTORS FETCH ════════════
TF_MAP={"1m":"1m","5m":"5m","15m":"15m","15":"15m","30m":"30m","1h":"1h","4h":"4h","1d":"daily","d":"daily","daily":"daily"}

def find_chartbit(obj, depth=0):
    if depth>6: return None
    if isinstance(obj,dict):
        if "chartbit" in obj: return obj["chartbit"]
        for v in obj.values():
            r=find_chartbit(v,depth+1)
            if r: return r
    return None

def fetch_ds_ohlcv(ticker, interval="15m", limit=200, force_fresh=False):
    if not DS_KEY: return None
    if not force_fresh:
        cached=_cache_get(ticker,interval)
        if cached is not None: return cached
    t=ticker.replace(".JK","").upper().strip()
    tf=TF_MAP.get(str(interval).lower(),"15m")
    url=f"{DS_BASE}/api/chart-saham/{t}/{tf}/latest?_={int(time.time())}"
    try:
        r=requests.get(url,headers={"X-API-Key":DS_KEY,"Accept":"*/*",
            "Cache-Control":"no-cache, no-store, must-revalidate","Pragma":"no-cache"},timeout=12)
        if r.status_code!=200: return None
        rows=find_chartbit(r.json())
        if not rows: return None
        df=pd.DataFrame(rows)
        rename={'open':'Open','high':'High','low':'Low','close':'Close','volume':'Volume',
            'datetime':'Datetime','date':'Date','unix_timestamp':'UnixTs',
            'foreign_buy':'FBuy','foreign_sell':'FSell','value':'Value','frequency':'Frequency'}
        df.rename(columns={k:v for k,v in rename.items() if k in df.columns},inplace=True)
        for col in ["Open","High","Low","Close","Volume","FBuy","FSell","Value","Frequency"]:
            if col in df.columns: df[col]=pd.to_numeric(df[col],errors='coerce').fillna(0)
        parsed=False
        for dc in ["Datetime","Date"]:
            if dc in df.columns:
                df["_dt"]=pd.to_datetime(df[dc],errors='coerce')
                if not df["_dt"].isna().all(): df=df.set_index("_dt"); parsed=True; break
        if not parsed and "UnixTs" in df.columns:
            df["_dt"]=pd.to_datetime(df["UnixTs"],unit='s',errors='coerce'); df=df.set_index("_dt")
        df=df.dropna(subset=["Close"]).sort_index()
        if len(df)<20: return None
        _cache_set(ticker,interval,df)
        return df
    except: return None

# ════════════ YFINANCE ════════════
def _normalize_yf_df(df):
    if df is None or df.empty: return None
    try:
        if isinstance(df.columns,pd.MultiIndex): df.columns=df.columns.get_level_values(0)
        df.columns=[str(c).strip().capitalize() for c in df.columns]
        df=df.rename(columns={"Adj close":"Close","Adj_close":"Close","Adjclose":"Close"})
        req=["Open","High","Low","Close","Volume"]
        if any(c not in df.columns for c in req): return None
        df=df[req].copy()
        df["Close"]=pd.to_numeric(df["Close"],errors="coerce")
        df["Open"]=pd.to_numeric(df["Open"],errors="coerce").fillna(df["Close"])
        df["High"]=pd.to_numeric(df["High"],errors="coerce").fillna(df["Close"])
        df["Low"]=pd.to_numeric(df["Low"],errors="coerce").fillna(df["Close"])
        df["Volume"]=pd.to_numeric(df["Volume"],errors="coerce").fillna(0)
        df=df.dropna(subset=["Close"]); df=df[df["Close"]>0]
        return df if len(df)>=2 else None
    except: return None

def _fetch_yf_ticker(ticker_yf, period="7d", interval="15m"):
    try:
        t=yf.Ticker(ticker_yf)
        df=t.history(period=period,interval=interval,auto_adjust=True,actions=False)
        r=_normalize_yf_df(df)
        if r is not None: return r
    except: pass
    time.sleep(random.uniform(0.3,0.7))
    try:
        df=yf.download(ticker_yf,period=period,interval=interval,auto_adjust=True,progress=False,threads=False)
        r=_normalize_yf_df(df)
        if r is not None: return r
    except: pass
    time.sleep(random.uniform(0.5,1.0))
    try:
        t=yf.Ticker(ticker_yf)
        df=t.history(period=period,interval=interval,auto_adjust=False,actions=False)
        if df is not None and not df.empty and "Adj Close" in df.columns:
            df=df.drop(columns=["Adj Close"])
        r=_normalize_yf_df(df)
        if r is not None: return r
    except: pass
    return None

def _fetch_yf_parallel(tickers_yf, period="7d", interval="15m", workers=4, delay=(0.2,0.5)):
    results={}; lock=threading.Lock()
    shuffled=list(tickers_yf); random.shuffle(shuffled)
    def _one(t):
        time.sleep(random.uniform(*delay))
        df=_fetch_yf_ticker(t,period,interval)
        if df is not None:
            with lock: results[t]=df
    with ThreadPoolExecutor(max_workers=workers) as exe:
        list(exe.map(_one,shuffled))
    return results

# ════════════ IDX QUANT 151 ════════════
def _iq_sma(p,n):
    if len(p)<n: return None
    return round(sum(p[-n:])/n,2)

def _iq_ema(p,n):
    if len(p)<n: return None
    k=2/(n+1); e=p[-n]
    for x in p[-n+1:]: e=x*k+e*(1-k)
    return round(e,2)

def _iq_rsi(p,n=14):
    if len(p)<n+1: return None
    deltas=[p[i+1]-p[i] for i in range(len(p)-1)]
    g=[d for d in deltas[-n:] if d>0]; l=[-d for d in deltas[-n:] if d<0]
    ag=sum(g)/n if g else 0; al=sum(l)/n if l else 1e-9
    return round(100-100/(1+ag/al),1)

def _iq_momentum(p):
    n=len(p); s=50; sig=[]
    if n>=20:
        r=(p[-1]-p[-20])/p[-20]*100
        s+=15 if r>5 else -15 if r<-5 else 0; sig.append(f"1M: {r:+.1f}%")
    if n>=60:
        r=(p[-1]-p[-60])/p[-60]*100
        s+=12 if r>10 else -12 if r<-10 else 0; sig.append(f"3M: {r:+.1f}%")
    if n>=120:
        r=(p[-1]-p[-120])/p[-120]*100
        s+=10 if r>20 else -10 if r<-15 else 0; sig.append(f"6M: {r:+.1f}%")
    return {"score":min(100,max(0,s)),"signals":sig}

def _iq_ma_signal(p):
    e5=_iq_ema(p,5); e13=_iq_ema(p,13); e34=_iq_ema(p,34)
    s20=_iq_sma(p,20); s50=_iq_sma(p,50)
    s=50; lbl="MIXED"; sig=[]
    if e5 and e13:
        if e5>e13: s+=15; sig.append("EMA5>13 ✓")
        else: s-=15; sig.append("EMA5<13 ✗")
    if e13 and e34:
        if e13>e34: s+=12; sig.append("EMA13>34 ✓")
        else: s-=12
    if s20 and s50:
        if s20>s50: s+=8; sig.append("SMA20>50 ✓")
        else: s-=8
    if e5 and e13 and e34:
        if e5>e13>e34: lbl="BULLISH"
        elif e5<e13<e34: lbl="BEARISH"
    return {"score":min(100,max(0,s)),"label":lbl,"e5":e5,"e13":e13,"e34":e34,"s20":s20,"s50":s50,"signals":sig}

def _iq_bagger(ms, mas, mal, va, p):
    s=0; sig=[]
    s+=ms/100*40
    if ms>=60: sig.append("✓ Momentum kuat (3.1)")
    s+=mas/100*25
    if mal=="BULLISH": sig.append("✓ MA stack bullish (3.12/3.13)")
    if va:
        if 25<=va<=70: s+=20; sig.append(f"✓ Vol {va}% sweet spot")
        elif va<25: s+=10
        else: s+=5
    n=len(p)
    if n>=60:
        h=max(p[-min(252,n):]); l=min(p[-min(252,n):]); cur=p[-1]
        rng=h-l
        if rng>0:
            pct=(cur-l)/rng*100
            if pct<35: s+=15; sig.append(f"✓ Near 52W low {pct:.0f}% (3.3)")
            elif pct<55: s+=8
    return {"score":round(min(100,s),1),"signals":sig}

def _iq_pivot_daily(df):
    try:
        if df is None or len(df)<2: return {}
        prev=df.iloc[-2]
        h=float(prev["High"]); l=float(prev["Low"]); c=float(prev["Close"])
        pp=(h+l+c)/3; r1=2*pp-l; r2=pp+(h-l); s1=2*pp-h; s2=pp-(h-l)
        cur=float(df.iloc[-1]["Close"])
        return {"pp":round(pp),"r1":round(r1),"r2":round(r2),"s1":round(s1),"s2":round(s2),
            "above":cur>pp,"dist_r1":round((r1-cur)/cur*100,2)}
    except: return {}

def iq_analyze(df_daily):
    empty={"iq_score":0,"iq_verdict":"UNKNOWN","iq_mom":0,"iq_ma":"—","iq_bagger":0,
        "iq_rsi":None,"iq_pivot":{},"iq_signals":[]}
    if df_daily is None or len(df_daily)<10: return empty
    try:
        prices=[float(x) for x in df_daily["Close"].tolist() if x>0]
        if len(prices)<10: return empty
        vol_ann=None
        if len(prices)>=21:
            rets=[(prices[i+1]-prices[i])/prices[i] for i in range(len(prices)-21,len(prices)-1)]
            mn=sum(rets)/len(rets)
            var=sum((r-mn)**2 for r in rets)/(len(rets)-1) if len(rets)>1 else 0
            vol_ann=round(math.sqrt(var)*math.sqrt(252)*100,1) if var>0 else None
        mom=_iq_momentum(prices); ma=_iq_ma_signal(prices)
        bag=_iq_bagger(mom["score"],ma["score"],ma["label"],vol_ann,prices)
        rsi=_iq_rsi(prices); pv=_iq_pivot_daily(df_daily)
        comp=round(mom["score"]*0.40+ma["score"]*0.35+bag["score"]*0.25,1)
        verdict="BUY" if comp>=55 else "HOLD" if comp>=40 else "WAIT"
        return {"iq_score":comp,"iq_verdict":verdict,"iq_mom":mom["score"],"iq_ma":ma["label"],
            "iq_bagger":bag["score"],"iq_rsi":rsi,"iq_pivot":pv,"iq_vol_ann":vol_ann,
            "iq_signals":mom["signals"]+ma["signals"][:2]+bag["signals"][:2],
            "iq_e5":ma["e5"],"iq_e13":ma["e13"],"iq_e34":ma["e34"]}
    except: return empty

def calc_mesin_grade(tt_score, tt_signal, iq_score, iq_verdict, iq_bagger):
    tt_score=max(0,min(tt_score,6))
    is_s=any(k in tt_signal for k in ["GACOR","REVERSAL"])
    is_m=any(k in tt_signal for k in ["BANDAR","HAKA","SUPER"])
    is_b=any(k in tt_signal for k in ["BAGGER","KANDIDAT"])
    is_mod=any(k in tt_signal for k in ["POTENSIAL","REBOUND","AKUM"])
    is_w=any(k in tt_signal for k in ["WATCH","ON TRACK"])
    iq_buy=iq_verdict=="BUY"; iq_hold=iq_verdict=="HOLD"; iq_un=iq_verdict=="UNKNOWN"
    if is_m and iq_buy: return "BANDAR 🔵","#4da6ff",min(100,tt_score/6*50+iq_score/100*50+22)
    if is_s and iq_buy: return "PRESISI 🎯","#00ff88",min(100,tt_score/6*50+iq_score/100*50+20)
    if is_b and iq_bagger>=60: return "BAGGER 💎","#bf5fff",min(100,tt_score/6*50+iq_score/100*50+18)
    if is_mod and iq_buy: return "KUAT ⚡","#ffb700",min(100,tt_score/6*50+iq_score/100*50+10)
    if is_s and iq_hold: return "KUAT ⚡","#ffb700",min(100,tt_score/6*50+iq_score/100*50+8)
    if iq_un:
        if is_m: return "BANDAR 🔵","#4da6ff",min(100,tt_score/6*70+15)
        if is_s: return "TT-ONLY 🔥","#ff7b00",min(100,tt_score/6*70+12)
        if is_b: return "BAGGER 💎","#bf5fff",min(100,tt_score/6*70+10)
        if is_mod: return "KUAT ⚡","#ffb700",min(100,tt_score/6*70+5)
        if is_w: return "WATCH 👀","#00e5ff",min(60,tt_score/6*60)
    if iq_buy and (is_s or is_mod or is_m): return "MONITOR 👁️","#00e5ff",tt_score/6*50+iq_score/100*50
    if (is_s or is_m) and iq_hold: return "MONITOR 👁️","#00e5ff",tt_score/6*50+iq_score/100*50-5
    if is_w and (iq_buy or iq_hold): return "WATCH 👀","#00e5ff",min(60,tt_score/6*50+iq_score/100*50-5)
    return "WAIT ❌","#ff3d5a",max(0,tt_score/6*50+iq_score/100*50-10)

# ════════════ SESSION STATE ════════════
_TT_RESULTS_FILE=CACHE_DIR/"tt_last_results.pkl"
_TT_RESULTS_TTL=1800
def _tt_save(r,ts):
    try: _TT_RESULTS_FILE.write_bytes(pickle.dumps({"results":r,"ts":ts}))
    except: pass
def _tt_load():
    try:
        if _TT_RESULTS_FILE.exists():
            d=pickle.loads(_TT_RESULTS_FILE.read_bytes())
            if time.time()-d["ts"]<_TT_RESULTS_TTL: return d
    except: pass
    return None

for _k,_v in [("tt_last_sent",set()),("wl_results",[]),("wl_mode_used",""),
    ("scan_results",[]),("data_dict",{}),("last_scan_time",None),
    ("last_scan_mode","Scalping ⚡"),("bsjp_results",[]),("gapup_results",[]),
    ("sector_data",{}),("beta_data",[])]:
    if _k not in st.session_state: st.session_state[_k]=_v

if not st.session_state.scan_results:
    _s=_tt_load()
    if _s:
        st.session_state.scan_results=_s["results"]
        st.session_state.last_scan_time=_s["ts"]

st.set_page_config(layout="wide",page_title="Mesin Presisi v2.2",page_icon="⚡",initial_sidebar_state="collapsed")

st.markdown("""<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;600;800&display=swap');
:root{--bg:#080c10;--surface:#0d1117;--border:#1c2533;--accent:#00e5ff;--green:#00ff88;--red:#ff3d5a;--amber:#ffb700;--purple:#bf5fff;--orange:#ff7b00;--muted:#4a5568;--text:#c9d1d9;--heading:#e6edf3;}
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
.signal-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px;position:relative;overflow:hidden;}
.signal-card.gacor{border-color:rgba(0,255,136,.4);background:rgba(0,255,136,.03);}
.signal-card.potensial{border-color:rgba(255,183,0,.3);background:rgba(255,183,0,.03);}
.signal-card.watch{border-color:rgba(0,229,255,.2);}
.signal-card::after{content:'';position:absolute;top:0;left:0;width:4px;height:100%;}
.signal-card.gacor::after{background:var(--green);}.signal-card.potensial::after{background:var(--amber);}
.signal-card.watch::after{background:var(--accent);}
.signal-card.bagger{border-color:rgba(191,95,255,.6);background:rgba(191,95,255,.05);box-shadow:0 0 20px rgba(191,95,255,.15);}
.signal-card.bagger::after{background:var(--purple);}.sc-bar.filled-purple{background:var(--purple);}
.bagger-alert-box{background:rgba(191,95,255,.06);border:1px solid rgba(191,95,255,.5);border-radius:8px;padding:14px 18px;margin-bottom:16px;}
.bagger-title{color:var(--purple);font-family:'Space Mono',monospace;font-size:12px;font-weight:700;letter-spacing:2px;}
.sc-ticker{font-family:'Space Mono',monospace;font-size:18px;font-weight:700;color:var(--heading);}
.sc-price{font-family:'Space Mono',monospace;font-size:13px;color:var(--muted);}
.sc-signal{font-size:13px;font-weight:700;margin:6px 0;}
.sc-bars{display:flex;gap:3px;margin:8px 0;}
.sc-bar{height:16px;border-radius:2px;}.sc-bar.filled{background:var(--green);}.sc-bar.empty{background:var(--border);}
.sc-stats{display:flex;gap:12px;flex-wrap:wrap;margin-top:8px;}
.sc-stat{font-family:'Space Mono',monospace;font-size:10px;color:var(--muted);}.sc-stat span{color:var(--text);}
.alert-box{background:rgba(255,61,90,.06);border:1px solid rgba(255,61,90,.4);border-radius:8px;padding:14px 18px;margin-bottom:16px;}
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

# ════════════ STOCK LIST ════════════
raw_stocks=["AADI","AALI","ABBA","ABDA","ABMM","ACES","ACRO","ACST","ADCP","ADES","ADHI","ADMF","ADMG","ADMR","ADRO","AEGS","AGAR","AGII","AGRO","AGRS","AHAP","AIMS","AISA","AKKU","AKPI","AKRA","AKSI","ALDO","ALII","ALKA","ALMI","ALTO","AMAG","AMAN","AMAR","AMFG","AMIN","AMMN","AMMS","AMOR","AMRT","ANDI","ANJT","ANTM","APEX","APIC","APII","APLI","APLN","ARCI","AREA","ARGO","ARII","ARKA","ARKO","ARMY","ARNA","ARTA","ARTI","ARTO","ASBI","ASDM","ASGR","ASHA","ASII","ASJT","ASLI","ASLC","ASMI","ASPI","ASPR","ASRI","ASRM","ASSA","ATAP","ATIC","ATLA","AUTO","AVIA","AWAN","AXIO","AYAM","AYLS","BABA","BABP","BABY","BACA","BAIK","BAJA","BALI","BANK","BAPA","BAPI","BATA","BATR","BAUT","BAYU","BBCA","BBHI","BBKP","BBLD","BBMD","BBNI","BBRI","BBRM","BBSI","BBSS","BBTN","BBYB","BCAP","BCIC","BCIP","BDKR","BDMN","BEBS","BEEF","BEER","BEKS","BELI","BELL","BESS","BEST","BFIN","BGTG","BHAT","BHIT","BIAS","BIKA","BIKE","BIMA","BINA","BINO","BIPI","BIPP","BIRD","BISI","BIWA","BJBR","BJTM","BKDP","BKSL","BKSW","BLES","BLOG","BLTA","BLTZ","BLUE","BMAS","BMBL","BMHS","BMRI","BMSR","BMTR","BNBA","BNBR","BNGA","BNII","BNLI","BOAT","BOBA","BOGA","BOLA","BOLT","BOSS","BPFI","BPII","BPTR","BRAM","BREN","BRIS","BRMS","BRNA","BRPT","BRRC","BSBK","BSDE","BSIM","BSML","BSSR","BSWD","BTEK","BTEL","BTON","BTPN","BTPS","BUAH","BUDI","BUKA","BUKK","BULL","BUMI","BUVA","BVIC","BWPT","BYAN","CAKK","CAMP","CANI","CARE","CARS","CASA","CASH","CASS","CBDK","CBPE","CBRE","CBUT","CBMF","CCSI","CDIA","CEKA","CENT","CFIN","CGAS","CHEK","CHEM","CHIP","CINT","CITA","CITY","CLAY","CLEO","CLPI","CMNP","CMNT","CMPP","CMRY","CNKO","CNMA","CNTX","COAL","COCO","COIN","COWL","CPIN","CPRI","CPRO","CRAB","CRSN","CSAP","CSIS","CSMI","CSRA","CTBN","CTRA","CTTH","CUAN","CYBR","DAAZ","DADA","DART","DATA","DAYA","DCII","DEAL","DEFI","DEPO","DEWA","DEWI","DFAM","DGNS","DGWG","DGIK","DIGI","DILD","DIVA","DKFT","DKHH","DLTA","DMAS","DMMX","DMND","DNAR","DNET","DOID","DOOH","DOSS","DPNS","DPUM","DRMA","DSFI","DSNG","DSSA","DUCK","DUTI","DVLA","DWGL","DYAN","EAST","ECII","EDGE","EKAD","ELIT","ELPI","ELSA","ELTY","EMAS","EMDE","EMTK","ENAK","ENRG","ENVY","ENZO","EPAC","EPMT","ERAL","ERAA","ERTX","ESIP","ESSA","ESTA","ESTI","ETWA","EURO","EXCL","FAPA","FAST","FASW","FILM","FIMP","FIRE","FISH","FITT","FLMC","FOLK","FOOD","FORE","FORU","FPNI","FUJI","FUTR","FWCT","GAMA","GDST","GDYR","GEMA","GEMS","GGRP","GGRM","GHON","GIAA","GJTL","GLOB","GLVA","GMFI","GMTD","GOLF","GOLD","GOLL","GOOD","GOTO","GPRA","GPSO","GRIA","GRPH","GRPM","GRII","GSMF","GTBO","GTRA","GTSI","GULA","GUNA","GWSA","GZCO","HADE","HAIS","HAJJ","HALO","HATM","HBAT","HDFA","HDIT","HEAL","HELI","HERO","HEXA","HGII","HILL","HITS","HKMU","HMSP","HOKI","HOME","HOMI","HOPE","HOTL","HRME","HRTA","HRUM","HUMI","HYGN","IATA","IBFN","IBOS","IBST","ICBP","ICON","IDEA","IDPR","IFII","IFSH","IGAR","IIKP","IKAI","IKAN","IKBI","IKPM","IMAS","IMJS","IMPC","INAF","INAI","INCF","INCI","INCO","INDF","INDO","INDR","INDS","INDX","INDY","INET","INKP","INOV","INPC","INPP","INPS","INRU","INTA","INTD","INTP","IOTF","IPAC","IPCC","IPCM","IPOL","IPPE","IPTV","IRRA","IRSX","ISAP","ISAT","ISEA","ISSP","ITIC","ITMA","ITMG","JAAS","JARR","JAST","JATI","JAVA","JAYA","JECC","JGLE","JIHD","JKON","JMAS","JPFA","JRPT","JSKY","JSMR","JSPT","JTPE","KAEF","KAQI","KARW","KARY","KAST","KAYU","KBAG","KBLI","KBLM","KBLV","KBRI","KDSI","KDTN","KEEN","KEJU","KETR","KIAS","KICI","KIJA","KING","KINO","KIOS","KJEN","KKES","KKGI","KLAS","KLBF","KLIN","KMDS","KMTR","KOBX","KOCI","KOIN","KOKA","KONI","KOPI","KOTA","KPIG","KRAH","KRAS","KREN","KSIX","KUAS","LABA","LABS","LAJU","LAND","LAPD","LCGP","LCKM","LEAD","LFLO","LIFE","LINK","LION","LIVE","LMAS","LMPI","LMSH","LOPI","LPCK","LPGI","LPIN","LPKR","LPLI","LPPF","LPPS","LRNA","LSIP","LTLS","LUCK","LUCY","MAAS","MABA","MADA","MAGP","MAHA","MAIN","MANG","MAPA","MAPB","MAPI","MARI","MARK","MASA","MASB","MAYA","MBAP","MBMA","MBSS","MBTO","MCAS","MCOL","MCOR","MDIA","MDKA","MDKI","MDLA","MDLN","MDRN","MEDC","MEDS","MEGA","MEJA","MENN","MERI","MERK","META","MFMI","MGNA","MGRO","MHKI","MICE","MIDI","MIKA","MINA","MINE","MIRA","MITI","MKAP","MKPI","MKTR","MLBI","MLIA","MLPL","MLPT","MMLP","MMIX","MNCN","MOLI","MORA","MPOW","MPMX","MPPA","MPRO","MPXL","MRAT","MREI","MSIE","MSIN","MSJA","MSKY","MSTI","MTDL","MTEL","MTFN","MTLA","MTMH","MTPS","MTRA","MTRN","MTSM","MTWI","MUTU","MYOH","MYOR","MYTX","NAIK","NANO","NASA","NASI","NATO","NAYZ","NCKL","NELY","NEST","NETV","NICE","NICK","NICL","NIKL","NINE","NIRO","NISP","NOBU","NPGF","NRCA","NSSS","NTBK","NUSA","NZIA","OASA","OBAT","OBMD","OCAP","OILS","OKAS","OLIV","OMED","OMRE","OPMS","PACK","PADA","PADI","PALM","PAMG","PANI","PANR","PANS","PART","PBID","PBSA","PBRX","PCAR","PDES","PDPP","PEGE","PEHA","PELI","PENT","PERW","PEVE","PGAS","PGEO","PGJO","PGLI","PGUN","PICO","PIPA","PJAA","PJHB","PKPK","PLAN","PLAS","PLIN","PMJS","PMMP","PMUI","PNBN","PNBS","PNGO","PNIN","PNLF","PNSE","POLA","POLI","POLL","POLU","POLY","POOL","PORT","POSA","POWR","PPGL","PPRI","PPRE","PPRO","PRAY","PRDA","PRIM","PSAB","PSAT","PSDN","PSGO","PSKT","PSSI","PTBA","PTDU","PTIS","PTMP","PTMR","PTPP","PTPS","PTPW","PTRO","PTSN","PTSP","PUDP","PURA","PURE","PURI","PWON","PYFA","PZZA","RAAM","RAFI","RAJA","RALS","RANC","RATU","RBMS","RCCC","RDTX","REAL","RELF","RELI","REPP","RGAS","RICY","RIGS","RIMO","RISE","RLCO","RMBA","RMKE","RMKO","RMLP","ROCK","RODA","ROLI","RONY","ROTI","RSCH","RSGK","RUIS","RUNS","SAFE","SAGE","SAGI","SAME","SAMF","SAMR","SAMP","SANO","SAPX","SATU","SBAT","SBMA","SCCO","SCMA","SCNP","SCPI","SDMU","SDPC","SDRA","SEMA","SFAN","SGER","SGGH","SGJL","SGRO","SHID","SHIP","SICO","SIDO","SIER","SILO","SIMA","SIMP","SINI","SIPD","SKBM","SKLT","SKRN","SKYB","SLIS","SMAR","SMDM","SMDR","SMGA","SMGR","SMKM","SMKL","SMLE","SMMA","SMMT","SMRA","SMRU","SMSM","SNLK","SOCI","SOFA","SOHO","SOLA","SONA","SOSS","SOTS","SOUL","SPMA","SPRE","SPTO","SQMI","SRAJ","SREI","SRIL","SRSN","SRTG","SSIA","SSMS","SSTM","STAA","STAR","STRK","STTP","SUGI","SULI","SUNI","SUPA","SUPR","SURE","SWAT","SWID","SYAI","TALF","TAMA","TAMU","TAPG","TARA","TAXI","TAYS","TBIG","TBLA","TBMS","TCID","TCPI","TDPM","TEBE","TECH","TELE","TFAS","TFCO","TGKA","TGRA","TGUK","TIFA","TINS","TIRA","TIRT","TKIM","TLDN","TLKM","TMAS","TMPO","TNCA","TOBA","TOOL","TOPS","TOSK","TOTL","TOTO","TOWR","TOYS","TPAI","TPIA","TPMA","TRAM","TRGU","TRIL","TRIM","TRIN","TRIO","TRIS","TRJA","TRON","TRST","TRUE","TRUK","TRUS","TSPC","TUGU","TULT","TYRE","UANG","UCID","UDNG","UFOE","ULTJ","UNIC","UNIQ","UNIT","UNSP","UNTR","UNVR","URBN","UVCR","VAST","VATE","VCOK","VERN","VICI","VICO","VINS","VISA","VISI","VIVA","VKTR","VOKS","VOSS","VRNA","VTNY","WAPO","WBSA","WEGE","WEHA","WGSH","WICO","WIDI","WIFI","WIIM","WIKA","WINE","WINR","WINS","WIRG","WITA","WMPP","WMUU","WOMF","WONS","WOOD","WOWS","WPOW","WSBP","WSKT","WTON","YELO","YOII","YPAS","YULE","YUPI","ZATA","ZBRA","ZENI","ZINC","ZONE","ZYRX"]
seen=set(); raw_stocks=[x for x in raw_stocks if not(x in seen or seen.add(x))]
_STATIC_STOCKS = list(raw_stocks)  # backup kalau Arjum down

# ════════════════════════════════════════════════════
#  DYNAMIC STOCK UNIVERSE — auto IPO/delisting dari Arjum
#  Cache 24 jam. Fallback ke static list kalau API gagal.
# ════════════════════════════════════════════════════
_STOCK_UNIVERSE_CACHE = CACHE_DIR / "arjum_universe.pkl"
_STOCK_UNIVERSE_TTL   = 24 * 3600  # refresh harian

def _clean_ticker_code(code):
    """Validasi kode saham IDX: 3-5 huruf kapital."""
    if not code or not isinstance(code, str): return None
    c = code.strip().upper()
    return c if 3 <= len(c) <= 5 and c.isalpha() else None

def fetch_arjum_universe(force_fresh=False):
    """Ambil universe saham IDX dari Arjum (auto IPO/delisting).
    Strategi: /api/search → /api/market-cap paginated → None.
    Cache 24 jam. Return list ticker atau None."""
    if not ARJUM_KEY: return None
    # Cache hit
    if not force_fresh:
        try:
            if _STOCK_UNIVERSE_CACHE.exists():
                d = pickle.loads(_STOCK_UNIVERSE_CACHE.read_bytes())
                if time.time() - d["ts"] < _STOCK_UNIVERSE_TTL:
                    return d["tickers"]
        except: pass
    tickers = set()

    # ── Coba /api/search (paling simpel, kemungkinan full universe) ──
    data = _arjum_get("/api/search")
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                code = _clean_ticker_code(item.get("stock_code") or item.get("code"))
                if code: tickers.add(code)
    elif isinstance(data, dict):
        for key in ["data", "rows", "stocks", "results"]:
            if key in data and isinstance(data[key], list):
                for item in data[key]:
                    if isinstance(item, dict):
                        code = _clean_ticker_code(item.get("stock_code") or item.get("code"))
                        if code: tickers.add(code)
                break

    # ── Fallback /api/market-cap paginated (963 stocks based on sample) ──
    if len(tickers) < 100:
        page = 1
        for _ in range(50):  # safety cap
            data2 = _arjum_get(f"/api/market-cap?page={page}&per_page=1000")
            if not isinstance(data2, dict) or "data" not in data2: break
            for item in (data2.get("data") or []):
                if isinstance(item, dict):
                    code = _clean_ticker_code(item.get("code") or item.get("stock_code"))
                    if code: tickers.add(code)
            total_pages = int(data2.get("total_pages", 1) or 1)
            if page >= total_pages: break
            page += 1

    # ── Minimal 100 ticker baru dianggap valid (jangan cache hasil parsial) ──
    if len(tickers) < 100:
        return None
    result = sorted(tickers)
    try:
        _STOCK_UNIVERSE_CACHE.write_bytes(
            pickle.dumps({"ts": time.time(), "tickers": result, "count": len(result)}))
    except: pass
    return result

def get_universe_age_hours():
    try:
        if _STOCK_UNIVERSE_CACHE.exists():
            d = pickle.loads(_STOCK_UNIVERSE_CACHE.read_bytes())
            return round((time.time() - d["ts"]) / 3600, 1)
    except: pass
    return None

# ── Resolusi universe: coba Arjum dulu, fallback static ──
try:
    _arjum_univ = fetch_arjum_universe(force_fresh=False)
    if _arjum_univ and len(_arjum_univ) >= 100:
        raw_stocks = _arjum_univ
        _STOCK_SRC_LABEL = f"Arjum ({len(raw_stocks)})"
        _STOCK_SRC_DETAIL = f"refresh {_STOCK_UNIVERSE_TTL//3600}j · umur {get_universe_age_hours()}j"
    else:
        raw_stocks = _STATIC_STOCKS
        _STOCK_SRC_LABEL = f"Static ({len(raw_stocks)})"
        _STOCK_SRC_DETAIL = "Arjum API unavailable — fallback ke list statis"
except Exception as _e:
    raw_stocks = _STATIC_STOCKS
    _STOCK_SRC_LABEL = f"Static ({len(raw_stocks)})"
    _STOCK_SRC_DETAIL = f"Arjum error: {str(_e)[:40]}"

stocks_yf = [s + ".JK" for s in raw_stocks]
stock_map = {s + ".JK": s for s in raw_stocks}

@st.cache_data(ttl=300)
def get_market_regime():
    try:
        df=_fetch_yf_ticker("^JKSE","60d","1d")
        if df is None or len(df)<10: return ("UNKNOWN",0,0,0,"Data IHSG kurang",0.0)
        close=df["Close"].dropna()
        if len(close)<10: return ("UNKNOWN",0,0,0,"Data close kurang",0.0)
        ema20=float(close.ewm(span=10,adjust=False).mean().iloc[-1])
        ema55=float(close.ewm(span=min(30,len(close)-1),adjust=False).mean().iloc[-1])
        price=float(close.iloc[-1])
        chg=float(((close.iloc[-1]-close.iloc[-2])/close.iloc[-2])*100)
        chg3=float((close.iloc[-1]/close.iloc[-4]-1)*100) if len(close)>4 else chg
        ret5=float((close.iloc[-1]/close.iloc[-6]-1)*100) if len(close)>6 else chg3
        band=0.012; pct_vs_e20=(price-ema20)/ema20*100
        above_e20_clear=price>ema20*(1+band); above_e20_any=price>ema20*(1-band)
        above_e55=price>ema55; recovering=chg3>0.5
        bearish_confirm=chg<-0.3 and not above_e20_any
        if above_e20_clear and above_e55 and ret5>=2.0:
            return ("GREEN",price,ema20,ema55,f"IHSG {price:,.0f} > EMA10 & EMA30, momentum +{ret5:.1f}%/5D → Bullish kuat ✅ ({pct_vs_e20:+.1f}% vs EMA10)",chg)
        elif above_e20_clear and above_e55:
            return ("SIDEWAYS",price,ema20,ema55,f"IHSG {price:,.0f} di atas EMA10 & EMA30 tapi momentum lemah ({ret5:+.1f}%/5D) → belum cukup buat rally",chg)
        elif above_e20_any and above_e55:
            return ("GREEN",price,ema20,ema55,f"IHSG {price:,.0f} dekat EMA10 & di atas EMA30 → Bullish",chg)
        elif above_e20_any and not above_e55:
            return ("SIDEWAYS",price,ema20,ema55,f"IHSG {price:,.0f} > EMA10 tapi < EMA30({ema55:,.0f}) → Sideways",chg)
        elif not above_e20_any and recovering:
            return ("SIDEWAYS",price,ema20,ema55,f"IHSG {price:,.0f} recovery 3D {chg3:+.2f}% → Sideways",chg)
        elif bearish_confirm:
            return ("RED",price,ema20,ema55,f"IHSG {price:,.0f} < EMA10({ema20:,.0f}) {pct_vs_e20:+.1f}% → Bearish",chg)
        else:
            return ("SIDEWAYS",price,ema20,ema55,f"IHSG {price:,.0f} sedikit di bawah EMA10 → Sideways",chg)
    except Exception as e: return ("UNKNOWN",0,0,0,f"IHSG error: {str(e)[:40]}",0.0)

def get_regime_config(regime):
    return {"RED":{"mode":"Reversal 🎯","min_score":3,"min_rvol":1.2,"sl_mult":0.6,"label":"🔴 MARKET MERAH — Reversal Only, Score ≥ 3","color":"#ff3d5a","desc":"Market bearish. Fokus reversal oversold."},"GREEN":{"mode":"Bagger 💎","min_score":3,"min_rvol":1.3,"sl_mult":0.8,"label":"🟢 MARKET HIJAU — Wyckoff Bagger Hunt, Score ≥ 3","color":"#00ff88","desc":"Market bullish. Cari akumulasi Wyckoff + breakout."},"SIDEWAYS":{"mode":"Scalping ⚡","min_score":3,"min_rvol":1.3,"sl_mult":0.7,"label":"🟡 MARKET SIDEWAYS — Semua Mode, RVOL ≥ 1.3x","color":"#ffb700","desc":"Market sideways. Cari momentum + volume."},"UNKNOWN":{"mode":"Scalping ⚡","min_score":2,"min_rvol":1.0,"sl_mult":0.8,"label":"⚪ REGIME UNKNOWN","color":"#4a5568","desc":""}}.get(regime,{"mode":"Scalping ⚡","min_score":2,"min_rvol":1.0,"sl_mult":0.8,"label":"⚪","color":"#4a5568","desc":""})

@st.cache_data(ttl=300)
def fetch_sector_rotation(sector_stocks):
    tickers_yf=[s+".JK" for s in sector_stocks[:10]]
    raw=_fetch_yf_parallel(tickers_yf,"3d","1d",workers=5)
    results=[]
    for t,df in raw.items():
        try:
            if df is None or len(df)<2: continue
            close=float(df["Close"].iloc[-1]); prev=float(df["Close"].iloc[-2])
            chg=(close-prev)/prev*100; vol=float(df["Volume"].iloc[-1])
            avgv=float(df["Volume"].mean()); rvol=vol/avgv if avgv>0 else 1.0
            results.append({"ticker":t.replace(".JK",""),"close":close,"chg":round(chg,2),"rvol":round(rvol,2)})
        except: continue
    return results

@st.cache_data(ttl=300)
def scan_gap_up(tickers_yf, min_gap_pct=0.5):
    raw=_fetch_yf_parallel(list(tickers_yf),"5d","1d",workers=6)
    results=[]
    for t,df in raw.items():
        tkr=t.replace(".JK","")
        try:
            if df is None or len(df)<3: continue
            today=df.iloc[-1]; prev=df.iloc[-2]
            close=float(today["Close"]); high_t=float(today["High"]); low_t=float(today["Low"])
            high_p=float(prev["High"]); vol=float(today["Volume"])
            avg_vol=float(df["Volume"].mean()); rvol=vol/avg_vol if avg_vol>0 else 1.0
            try: rng_pct=float(((df["High"]-df["Low"])/df["Close"].replace(0,np.nan)).mean())*100
            except: rng_pct=2.0
            rng_pct=max(rng_pct if not np.isnan(rng_pct) else 2.0,0.5)
            gs=0; reasons=[]
            if close>high_p:
                gap_pct=(close-high_p)/high_p*100; gap_r=gap_pct/rng_pct
                if gap_r>0.5: gs+=3; reasons.append(f"Gap {gap_pct:.1f}% ({gap_r:.1f}R) ✦✦")
                else: gs+=1.5; reasons.append(f"Gap {gap_pct:.1f}% tipis")
            cr=(close-low_t)/max(high_t-low_t,1)
            if cr>0.85: gs+=2; reasons.append(f"Tutup dekat High {cr:.0%}")
            elif cr>0.70: gs+=1; reasons.append(f"Tutup kuat {cr:.0%}")
            if rvol>3.0: gs+=2; reasons.append(f"RVOL={rvol:.1f}x SURGE 🔥")
            elif rvol>2.0: gs+=1; reasons.append(f"RVOL={rvol:.1f}x")
            elif rvol>1.5: gs+=0.5
            if len(df)>=3:
                chg3=(close-float(df.iloc[-3]["Close"]))/float(df.iloc[-3]["Close"])*100
                chg3_r=chg3/rng_pct
                if chg3_r>1.5: gs+=1; reasons.append(f"3D ROC +{chg3:.1f}% ({chg3_r:.1f}R)")
                elif chg3_r>0.5: gs+=0.5
            if gs<3: continue
            chg_today=(close-float(prev["Close"]))/float(prev["Close"])*100
            results.append({"Ticker":tkr,"Price":int(close),"Gap Score":round(gs,1),
                "Chg %":round(chg_today,2),"Close Ratio":round(cr,2),"RVOL":round(rvol,2),
                "Prev High":int(high_p),"Signal":"GAP UP 🚀" if gs>=4 else "POTENTIAL ⚡",
                "Reasons":" · ".join(reasons[:3])})
        except: continue
    return sorted(results,key=lambda x:x["Gap Score"],reverse=True)

def ema(s,n): return s.ewm(span=n,adjust=False).mean()

def apply_intraday_indicators(df):
    if isinstance(df.columns,pd.MultiIndex): df.columns=df.columns.droplevel(1)
    c=df["Close"]
    df["EMA9"]=ema(c,9); df["EMA21"]=ema(c,21); df["EMA50"]=ema(c,50); df["EMA200"]=ema(c,200)
    d=c.diff(); g=d.clip(lower=0).ewm(span=14,adjust=False).mean(); l=(-d.clip(upper=0)).ewm(span=14,adjust=False).mean()
    rsi_raw=(100-100/(1+g/l.replace(0,np.nan))).fillna(50)
    df["RSI"]=rsi_raw; df["RSI_EMA"]=rsi_raw.ewm(span=14,adjust=False).mean()
    d5=c.diff(); g5=d5.clip(lower=0).ewm(span=5,adjust=False).mean(); l5=(-d5.clip(upper=0)).ewm(span=5,adjust=False).mean()
    df["RSI5"]=(100-100/(1+g5/l5.replace(0,np.nan))).fillna(50)
    lo10=df["Low"].rolling(10).min(); hi10=df["High"].rolling(10).max()
    raw_k=(100*(c-lo10)/(hi10-lo10).replace(0,np.nan)).fillna(50)
    df["STOCH_K"]=raw_k.ewm(span=5,adjust=False).mean(); df["STOCH_D"]=df["STOCH_K"].ewm(span=5,adjust=False).mean()
    ema12=c.ewm(span=12,adjust=False).mean(); ema26=c.ewm(span=26,adjust=False).mean()
    macd_line=ema12-ema26; signal_line=macd_line.ewm(span=9,adjust=False).mean()
    df["MACD"]=macd_line; df["MACD_Sig"]=signal_line; df["MACD_Hist"]=(macd_line-signal_line).fillna(0)
    try:
        tp=(df["High"]+df["Low"]+df["Close"])/3
        _day=pd.Series(pd.to_datetime(df.index).date,index=df.index)
        _pv=(tp*df["Volume"]).groupby(_day).cumsum(); _vv=df["Volume"].groupby(_day).cumsum()
        df["VWAP"]=(_pv/_vv.replace(0,np.nan)).fillna(c)
    except: df["VWAP"]=c
    df["BB_mid"]=c.rolling(20).mean(); df["BB_std"]=c.rolling(20).std()
    df["BB_upper"]=df["BB_mid"]+2*df["BB_std"]; df["BB_lower"]=df["BB_mid"]-2*df["BB_std"]
    df["BB_pct"]=(c-df["BB_lower"])/(df["BB_upper"]-df["BB_lower"])
    df["AvgVol"]=df["Volume"].rolling(20).mean().shift(1)
    df["RVOL"]=df["Volume"]/df["AvgVol"].replace(0,np.nan)
    tr=pd.concat([df["High"]-df["Low"],(df["High"]-c.shift()).abs(),(df["Low"]-c.shift()).abs()],axis=1).max(axis=1)
    df["ATR"]=tr.rolling(14).mean()
    body_top=df[["Close","Open"]].max(axis=1); body_bot=df[["Close","Open"]].min(axis=1)
    hl=(df["High"]-df["Low"]).replace(0,np.nan)
    df["LWick"]=((body_bot-df["Low"])/hl*100).fillna(0); df["UWick"]=((df["High"]-body_top)/hl*100).fillna(0)
    df["Body"]=(body_top-body_bot)/hl*100; df["BodyRatio"]=(body_top-body_bot)/hl.fillna(0)
    df["BullBar"]=(df["Close"]>df["Open"])&(df["BodyRatio"]>0.5)
    df["NetVol"]=np.where(c>=df["Open"],df["Volume"],-df["Volume"])
    df["NetVol3"]=pd.Series(df["NetVol"],index=df.index).rolling(3).sum()
    df["NetVol8"]=pd.Series(df["NetVol"],index=df.index).rolling(8).sum()
    df["VolSpike"]=df["RVOL"]>2.5
    df["ROC3"]=c.pct_change(3); df["ROC8"]=c.pct_change(8)
    df["HH"]=df["High"]>df["High"].shift(1); df["HL"]=df["Low"]>df["Low"].shift(1)
    df["LL"]=df["Low"]<df["Low"].shift(1); df["LH"]=df["High"]<df["High"].shift(1)
    if "FBuy" in df.columns and "FSell" in df.columns:
        df["FNet"]=df["FBuy"]-df["FSell"]; df["FCum"]=df["FNet"].cumsum()
        df["FNet3"]=df["FNet"].rolling(3).sum(); df["FNet8"]=df["FNet"].rolling(8).sum()
        tot=df["FBuy"]+df["FSell"]; df["FRatio"]=(df["FBuy"]/tot.replace(0,np.nan)).fillna(0.5)
    return df

def score_scalping(r,p,p2):
    s=0; reasons=[]
    if r["EMA9"]>r["EMA21"]>r["EMA50"]: s+=1.5; reasons.append("EMA stack ▲")
    elif r["EMA9"]>r["EMA21"]: s+=0.8; reasons.append("EMA9>21")
    if r["Close"]>r["VWAP"]: s+=1; reasons.append("Above VWAP")
    if r["MACD_Hist"]>0 and r["MACD_Hist"]>float(p["MACD_Hist"]):
        s+=1.5; reasons.append("MACD hist expanding ✦")
        if p2 is not None and float(p["MACD_Hist"])>float(p2["MACD_Hist"]): s+=0.3
    elif r["MACD_Hist"]>0: s+=0.5; reasons.append("MACD hist +")
    rsi_e=float(r["RSI_EMA"])
    if 52<rsi_e<68: s+=0.8; reasons.append(f"RSI-EMA={rsi_e:.1f}")
    elif rsi_e>=68: s-=0.5
    rvol=float(r["RVOL"]) if not np.isnan(float(r["RVOL"])) else 1.0
    if rvol>2.0: s+=1; reasons.append(f"RVOL={rvol:.1f}x surge")
    elif rvol>1.5: s+=0.6; reasons.append(f"RVOL={rvol:.1f}x")
    if bool(r["BullBar"]): s+=0.5; reasons.append("Bullish bar")
    if float(r["NetVol3"])>0: s+=0.4; reasons.append("Net vol +")
    if r["Close"]<r["EMA200"]*0.98: s-=0.5
    return max(0,min(6,round(s,1))),reasons,{}

def score_momentum(r,p,p2):
    s=0; reasons=[]
    if bool(r["HH"]) and bool(r["HL"]): s+=1.5; reasons.append("HH+HL pattern ▲")
    elif bool(r["HH"]): s+=0.8
    rvol=float(r["RVOL"]) if not np.isnan(float(r["RVOL"])) else 1.0
    if rvol>3.0: s+=1.5; reasons.append(f"RVOL={rvol:.1f}x SURGE 🔥")
    elif rvol>2.0: s+=1.0; reasons.append(f"RVOL={rvol:.1f}x")
    elif rvol>1.5: s+=0.5
    roc=float(r["ROC3"])*100
    try:
        _a=float(r.get("ATR",np.nan)); _c=float(r["Close"])
        atr_pct=(_a/_c*100) if (_c>0 and not np.isnan(_a) and _a>0) else 1.0
    except: atr_pct=1.0
    roc_atr=roc/max(atr_pct,0.05)
    if roc_atr>2.0: s+=1.5; reasons.append(f"ROC3={roc:.1f}% ({roc_atr:.1f}xATR)")
    elif roc_atr>1.0: s+=0.8; reasons.append(f"ROC3={roc:.1f}% ({roc_atr:.1f}xATR)")
    elif roc<0: s-=0.5
    rsi_e=float(r["RSI_EMA"])
    if 55<rsi_e<75: s+=0.8; reasons.append(f"RSI-EMA={rsi_e:.1f}")
    if rsi_e>78: s-=0.8; reasons.append("⚠️ RSI overbought")
    sk=float(r["STOCH_K"]); sd=float(r["STOCH_D"])
    if sk>60 and sk>sd: s+=0.8; reasons.append("STOCH K>D bullish")
    if r["MACD_Hist"]>0 and r["MACD_Hist"]>float(p["MACD_Hist"]): s+=0.8; reasons.append("MACD expanding")
    if r["Close"]>r["VWAP"]: s+=0.5; reasons.append("Above VWAP")
    return max(0,min(6,round(s,1))),reasons,{}

def score_reversal(r,p,p2):
    s=0; reasons=[]; os_cnt=0
    rsi_e=float(r["RSI_EMA"])
    if rsi_e<30: os_cnt+=1; s+=1.5; reasons.append(f"RSI-EMA={rsi_e:.1f} OS extreme")
    elif rsi_e<40: os_cnt+=1; s+=0.8; reasons.append(f"RSI-EMA={rsi_e:.1f} OS")
    sk=float(r["STOCH_K"]); sd=float(r["STOCH_D"])
    if sk<20: os_cnt+=1; s+=1; reasons.append(f"STOCH={sk:.0f} extreme OS")
    elif sk<30: os_cnt+=1; s+=0.5
    bp=float(r["BB_pct"])
    if bp<0.05: os_cnt+=1; s+=1; reasons.append("BB lower touch")
    elif bp<0.15: os_cnt+=1; s+=0.5
    if os_cnt<1.5: return 0,[],{}
    rev=0; pk=float(p["STOCH_K"]); pd_=float(p["STOCH_D"])
    if sk<30 and sk>sd and pk<=pd_: rev+=1; s+=2; reasons.append("STOCH %K cross ↑ OS ✦✦")
    elif sk<25 and sk>sd: rev+=1; s+=1.2; reasons.append("STOCH K>D extreme OS")
    rsi_p=float(p["RSI_EMA"])
    if rsi_e>rsi_p and rsi_e<42: rev+=1; s+=1.2; reasons.append("RSI-EMA pivot ↑")
    mh=float(r["MACD_Hist"]); mph=float(p["MACD_Hist"])
    if mh>mph and mh<0: rev+=1; s+=0.8; reasons.append("MACD hist diverge ↑")
    if rev==0: s*=0.3
    if bool(r["VolSpike"]) and float(r["Close"])<float(r["Open"]): s+=0.8; reasons.append("Volume climax sell")
    elif float(r["RVOL"])>1.5: s+=0.4
    if float(r["NetVol3"])>0: s+=0.5; reasons.append("Net vol turning +")
    if float(r["BodyRatio"])>0.75 and float(r["Close"])<float(r["Open"]): s-=0.8; reasons.append("⚠️ Bearish bar kuat")
    return max(0,min(6,round(s,1))),reasons,{}

def score_bagger(r,p,p2,df_full):
    def _sf(v,d=0.):
        try: x=float(v); return d if(np.isnan(x) or np.isinf(x)) else x
        except: return d
    s=0; reasons=[]
    try: close=float(r["Close"])
    except: close=0
    e9=_sf(r.get("EMA9",0)); e21=_sf(r.get("EMA21",0)); e50=_sf(r.get("EMA50",0)); e200=_sf(r.get("EMA200",0))
    rvol=_sf(r.get("RVOL",1),1.0); rsi_e=_sf(r.get("RSI_EMA",50),50)
    phase="SCANNING"; is_sideways=False
    range_high=close*1.05; range_low=close*0.95
    sb=min(20,len(df_full)-2)
    try:
        rh=float(df_full["High"].iloc[-sb-1:-1].max()); rl=float(df_full["Low"].iloc[-sb-1:-1].min())
        rp=(rh-rl)/max(rl,0.01)*100; _atr_now=_sf(r.get("ATR",0))
        if _atr_now>0: is_sideways=(rh-rl)<5.0*_atr_now
        else: is_sideways=rp<8.0
        range_high=rh; range_low=rl
        if is_sideways: s+=1.0+max(0,(8.0-min(rp,8.0))/8.0)*0.5; reasons.append(f"Sideways {rp:.1f}% ({sb}B) ✦"); phase="A-B"
    except: pass
    try:
        vm20=float(df_full["AvgVol"].iloc[-1]); vl5=float(df_full["Volume"].iloc[-6:-1].mean()); dr=vl5/max(vm20,1)
        if dr<0.5 and is_sideways: s+=2.0; reasons.append(f"Dry vol {dr:.2f}x stealth accum ✦✦"); phase="A-B AKUMULASI"
        elif dr<0.7 and is_sideways: s+=1.2; reasons.append(f"Vol drying {dr:.2f}x ✦"); phase="A-B AKUMULASI"
        elif dr<0.85 and is_sideways: s+=0.6; reasons.append(f"Vol below avg {dr:.2f}x")
    except: pass
    try:
        if "FBuy" in df_full.columns and "FSell" in df_full.columns:
            fb5=float(df_full["FBuy"].iloc[-6:-1].sum()); fs5=float(df_full["FSell"].iloc[-6:-1].sum()); ft5=fb5+fs5
            if ft5>0:
                fr5=fb5/ft5
                if fr5>0.65 and is_sideways: s+=2.0; reasons.append(f"🔵 Asing stealth accum {fr5:.0%} buy ✦✦"); phase="A-B AKUMULASI"
                elif fr5>0.55: s+=1.0; reasons.append(f"Asing net buy {fr5:.0%} ✦")
        else:
            if len(df_full)>=12:
                nv=[float(df_full["NetVol"].iloc[i]) for i in range(-11,-1)]
                np_=sum(1 for v in nv if v>0); nr=np_/10
                if nr>=0.7 and is_sideways: s+=1.5; reasons.append(f"Stealth net buy {np_}/10 bars ✦✦")
                elif nr>=0.6: s+=0.8; reasons.append(f"Net buy {np_}/10 bars")
                elif nr>=0.5: s+=0.4
    except:
        nv3=_sf(r.get("NetVol3",0)); nv8=_sf(r.get("NetVol8",0))
        if nv3>0 and nv8>0: s+=0.8; reasons.append("Net buyer sustained ✦")
        elif nv3>0: s+=0.3
    try:
        bc=float(r["BB_std"]); ba=float(df_full["BB_std"].iloc[-11:-1].mean()); sq=bc/max(ba,0.0001)
        if sq<0.7 and is_sideways: s+=1.5; reasons.append(f"BB squeeze extreme {sq:.2f}x ✦✦")
        elif sq<0.85: s+=0.8; reasons.append(f"BB squeeze {sq:.2f}x")
    except: pass
    spring=False
    try:
        lb=min(15,len(df_full)-3); pl=df_full["Low"].iloc[-lb-2:-2]
        sup=float(pl.min()); bl=float(r["Low"]); bc2=float(r["Close"]); bh=float(r["High"])
        if bl<sup and bc2>sup:
            rc=(bc2-bl)/max(bh-bl,0.0001)
            if rc>0.7 and rvol>1.2: s+=3.0; reasons.append(f"🔥 SPRING! Support break → rebound {rc:.0%} ✦✦✦"); phase="SPRING ⚡"; spring=True
            elif rc>0.5: s+=1.8; reasons.append(f"Spring pattern (recovery {rc:.0%}) ✦✦"); phase="SPRING"; spring=True
        if float(p["Low"])<sup and float(p["Close"])>sup and bc2>float(p["Close"]) and not spring:
            s+=2.0; reasons.append("Post-spring confirmation 🚀 ✦✦"); phase="POST-SPRING"; spring=True
    except: pass
    try:
        ar=close>range_high*0.998; tb=_sf(r.get("BodyRatio",0))>0.55; bf=float(r["Close"])>float(r["Open"])
        if rvol>4.0 and ar and tb and bf: s+=3.0; reasons.append(f"🚀 PHASE D! RVOL={rvol:.1f}x ✦✦✦"); phase="PHASE D 🚀"
        elif rvol>3.0 and ar and bf: s+=2.2; reasons.append(f"Breakout confirmed RVOL={rvol:.1f}x ✦✦"); phase="BREAKOUT ✦"
        elif rvol>2.0 and ar: s+=1.5; reasons.append(f"Breakout attempt RVOL={rvol:.1f}x")
        elif ar: s+=0.8; reasons.append("Above resistance (low vol)")
        else:
            if rvol>4.0: s+=1.5; reasons.append(f"RVOL={rvol:.1f}x MASSIVE 🔥🔥")
            elif rvol>3.0: s+=1.0; reasons.append(f"RVOL={rvol:.1f}x SURGE 🔥")
            elif rvol>2.0: s+=0.5; reasons.append(f"RVOL={rvol:.1f}x")
            elif rvol<1.3 and phase not in ["A-B AKUMULASI","SPRING","POST-SPRING"]: s-=0.5
    except:
        if rvol>4.0: s+=1.5; reasons.append(f"RVOL={rvol:.1f}x MASSIVE 🔥🔥")
        elif rvol>3.0: s+=1.0; reasons.append(f"RVOL={rvol:.1f}x SURGE 🔥")
        elif rvol>2.0: s+=0.5
    if e9>e21>e50>e200: s+=1.5; reasons.append("EMA golden stack ✦✦")
    elif e9>e21>e50: s+=1.0; reasons.append("EMA stack ▲")
    elif e9>e21: s+=0.4
    elif is_sideways and phase in ["A-B AKUMULASI","SPRING","POST-SPRING"]: s+=0.2
    if phase in ["A-B","A-B AKUMULASI","SPRING","POST-SPRING"]:
        if 25<=rsi_e<=52: s+=1.0; reasons.append(f"RSI-EMA={rsi_e:.1f} accum zone ✓")
        elif rsi_e<25: s+=0.6
        elif rsi_e>65: s-=0.3
    else:
        if 52<rsi_e<72: s+=1.0; reasons.append(f"RSI-EMA={rsi_e:.1f} momentum zone")
        elif rsi_e>=72: s-=0.5; reasons.append(f"⚠️ RSI OB {rsi_e:.1f}")
        elif rsi_e<40: s-=0.3
    if close>_sf(r.get("VWAP",close)): s+=0.5; reasons.append("Above VWAP")
    if e200>0 and close<e200*0.88: s-=1.0
    try:
        if len(df_full)>=4:
            bcc=sum(1 for i in range(-3,0) if float(df_full["Close"].iloc[i])>float(df_full["Open"].iloc[i]))
            if bcc==3: s+=0.8; reasons.append("3x consecutive bull bars")
            elif bcc==2: s+=0.3
    except: pass
    if phase!="SCANNING": reasons.insert(0,f"⚙️ Wyckoff: {phase}")
    return max(0,min(6,round(s,1))),reasons,{"wyckoff_phase":phase}

def get_signal(score,mode):
    t={"Scalping ⚡":{5:"GACOR ⚡",4:"POTENSIAL 🔥",3:"WATCH 👀",2:"LEMAH 🟡",1:"MARGINAL 🔸"},
       "Momentum 🚀":{5:"GACOR 🚀",4:"POTENSIAL 🔥",3:"WATCH 👀",2:"LEMAH 🟡",1:"MARGINAL 🔸"},
       "Reversal 🎯":{5:"REVERSAL 🎯",4:"POTENSIAL 🔥",3:"WATCH 👀",2:"LEMAH 🟡",1:"MARGINAL 🔸"},
       "Bagger 💎":{5:"BAGGER 💎",4:"KANDIDAT 🚀",3:"WATCH 👀",2:"LEMAH 🟡",1:"MARGINAL 🔸"}}.get(mode,{})
    for th in sorted(t.keys(),reverse=True):
        if score>=th: return t[th]
    return "WAIT"

def get_card_class(signal):
    if "BAGGER" in signal or "KANDIDAT" in signal: return "bagger"
    if "GACOR" in signal or "REVERSAL" in signal: return "gacor"
    if "POTENSIAL" in signal: return "potensial"
    if "WATCH" in signal: return "watch"
    return ""

def score_bsjp(r,p,p2):
    s=0; reasons=[]
    hi_lo=float(r["High"])-float(r["Low"]); cp=(float(r["Close"])-float(r["Low"]))/max(hi_lo,1)
    if cp>0.7: s+=2; reasons.append(f"Tutup dekat High ({cp:.0%})")
    elif cp>0.5: s+=1; reasons.append(f"Tutup kuat ({cp:.0%})")
    rvol=float(r["RVOL"]) if not np.isnan(float(r["RVOL"])) else 1.0
    if rvol>3.0: s+=2; reasons.append(f"RVOL={rvol:.1f}x SURGE 🔥")
    elif rvol>2.0: s+=1.5; reasons.append(f"RVOL={rvol:.1f}x kuat")
    elif rvol>1.5: s+=0.8; reasons.append(f"RVOL={rvol:.1f}x")
    if r["EMA9"]>r["EMA21"]>r["EMA50"]: s+=1.5; reasons.append("EMA stack ▲")
    elif r["EMA9"]>r["EMA21"]: s+=0.8; reasons.append("EMA9>21")
    rsi_e=float(r["RSI_EMA"])
    if 45<rsi_e<70: s+=1; reasons.append(f"RSI-EMA={rsi_e:.1f} ✓")
    elif rsi_e>=70: s-=1; reasons.append(f"RSI-EMA={rsi_e:.1f} OB ⚠️")
    elif rsi_e<40: s+=0.5; reasons.append(f"RSI-EMA={rsi_e:.1f} OS")
    if float(r["MACD_Hist"])>0 and float(r["MACD_Hist"])>float(p["MACD_Hist"]): s+=1; reasons.append("MACD hist expanding ✦")
    elif float(r["MACD_Hist"])>0: s+=0.5; reasons.append("MACD +")
    if float(r["Close"])>float(r["VWAP"]): s+=0.5; reasons.append("Above VWAP")
    return max(0,min(6,round(s,1))),reasons,{}

def calc_trailing_stop(entry,current,atr,method="ATR",atr_mult=2.0,pct=3.0):
    if method=="ATR": td=atr*atr_mult; sp=current-td
    elif method=="Persen": td=current*(pct/100); sp=current*(1-pct/100)
    else: td=atr*1.5; sp=current-td
    return {"stop":round(sp,0),"distance":round(td,0),
        "profit_float":round((current-entry)/entry*100,2),
        "profit_locked":round((sp-entry)/entry*100,2) if sp>entry else 0,
        "is_profitable":sp>entry}

def get_sinyal_v2(r,p,p2):
    def sf(v,d=0.):
        try: x=float(v); return d if(np.isnan(x) or np.isinf(x)) else x
        except: return d
    cl=sf(r.get("Close",0)); e9=sf(r.get("EMA9")); e21=sf(r.get("EMA21")); e50=sf(r.get("EMA50"))
    rsi_ema=sf(r.get("RSI_EMA",50),50); rsi_ema_p=sf(p.get("RSI_EMA",50),50)
    sk=sf(r.get("STOCH_K",50),50); sd=sf(r.get("STOCH_D",50),50)
    sk_p=sf(p.get("STOCH_K",50),50); sd_p=sf(p.get("STOCH_D",50),50)
    mh=sf(r.get("MACD_Hist",0)); mh_p=sf(p.get("MACD_Hist",0))
    macd_v=sf(r.get("MACD",0)); sig_v=sf(r.get("MACD_Sig",0))
    macd_p=sf(p.get("MACD",0)); sig_p=sf(p.get("MACD_Sig",0))
    rv=sf(r.get("RVOL",1),1.0); lw=sf(r.get("LWick",0)); uw=sf(r.get("UWick",0))
    body=sf(r.get("Body",50),50); vwap_v=sf(r.get("VWAP",cl),cl)
    score=0; flags=[]
    ema_bull=e9>e21>e50; ema_gc=e9>e21; ema_bear=e9<e21<e50
    p_e9=sf(p.get("EMA9")); p_e21=sf(p.get("EMA21"))
    gc_now=(e9>e21) and (p_e9<=p_e21)
    if ema_bull: score+=15; flags.append("EMA▲")
    elif ema_gc: score+=8; flags.append("EMA GC")
    elif ema_bear: score-=12
    stoch_os=sk<20; stoch_ob=sk>80; stoch_cu=sk>sd and sk_p<=sd_p
    if stoch_os: score+=12; flags.append(f"STOCH OS {sk:.0f}"); 
    if stoch_os and stoch_cu: score+=8; flags.append("STOCH ↑")
    elif stoch_ob: score-=10
    elif stoch_cu and sk<60: score+=6; flags.append("STOCH ↑")
    rsi_os=rsi_ema<40; rsi_os2=rsi_ema<30; rsi_ob=rsi_ema>65
    rsi_cu=rsi_ema>rsi_ema_p and rsi_ema_p<40
    if rsi_os2: score+=12; flags.append(f"RSI {rsi_ema:.0f} OS")
    elif rsi_os: score+=7; flags.append(f"RSI {rsi_ema:.0f}")
    elif 45<rsi_ema<65: score+=5
    elif rsi_ob: score-=8
    if rsi_cu: score+=6; flags.append("RSI ↑")
    macd_cu=macd_v>sig_v and macd_p<=sig_p; macd_cd=macd_v<sig_v and macd_p>=sig_p
    macd_exp=mh>0 and mh>mh_p
    if macd_cu: score+=10; flags.append("MACD ↑")
    elif macd_exp: score+=7; flags.append("MACD Exp")
    elif mh>0: score+=3
    elif macd_cd: score-=10; flags.append("MACD ↓")
    elif mh<0 and mh<mh_p: score-=5
    if rv>3: score+=15; flags.append(f"RVOL {rv:.1f}x 🔥")
    elif rv>2: score+=10; flags.append(f"RVOL {rv:.1f}x")
    elif rv>1.5: score+=5; flags.append(f"RVOL {rv:.1f}x")
    elif rv<0.5: score-=5
    if lw>60: score+=10; flags.append(f"LWick {lw:.0f}%")
    elif lw>40: score+=6
    elif lw>25: score+=3
    uw_sell=uw>50 and body<30
    if uw_sell: flags.append(f"UWick JUAL {uw:.0f}%")
    if cl>vwap_v: score+=5; flags.append("VWAP▲")
    elif cl<vwap_v: score-=3
    fnet3=sf(r.get("FNet3",0)); fnet8=sf(r.get("FNet8",0)); fratio=sf(r.get("FRatio",0.5),0.5)
    if fnet3>0 and fnet8>0:
        score+=10; flags.append("🔵 Asing Akum")
        if fratio>0.7: score+=5; flags.append("Asing Dom")
    elif fnet3<0 and fnet8<0: score-=8; flags.append("🔴 Asing Jual")
    score=max(-50,min(60,score))
    entry_kuat=((stoch_os or (sk<50 and ema_bear)) and (rsi_os or rsi_cu) and (macd_cu or macd_exp) and rv>=1.2)
    entry_mod=sum([stoch_os or stoch_cu,rsi_os or rsi_cu,macd_exp or macd_cu])>=2 and rv>=1.0
    is_bandar=(fnet3>0 and fnet8>0 and fratio>0.6 and rv>1.2 and ema_gc)
    is_haka=(ema_bull and rv>1.5 and macd_exp and rsi_ema>50 and sk>sd and cl>vwap_v)
    is_super=(entry_kuat and rv>2 and score>=35)
    is_rebound=(entry_kuat and (stoch_os or rsi_os2))
    is_jual=(uw_sell and (stoch_ob or rsi_ob) and rv>1.0)
    if is_jual: return "JUAL ⬇️",score," · ".join(flags[:3]),gc_now
    if is_bandar: return "BANDAR 🔵",score," · ".join(flags[:3]),gc_now
    if is_haka: return "HAKA 🔨",score," · ".join(flags[:3]),gc_now
    if is_super: return "SUPER 🔥",score," · ".join(flags[:3]),gc_now
    if is_rebound: return "REBOUND 🏀",score," · ".join(flags[:3]),gc_now
    if entry_mod and score>=20: return "AKUM 📦",score," · ".join(flags[:3]),gc_now
    if score>=15: return "ON TRACK ✅",score," · ".join(flags[:3]),gc_now
    return "WAIT ❌",score," · ".join(flags[:3]),gc_now

def get_aksi_v2(sinyal,gc_now,score):
    if "BANDAR" in sinyal or (("HAKA" in sinyal or "SUPER" in sinyal) and score>=35): return "AT ENTRY 🎯"
    elif "REBOUND" in sinyal: return "WATCH REB 🏀"
    elif gc_now: return "GC NOW ⚡"
    elif score>=25: return "AT ENTRY 🎯"
    elif score>=15: return "WAIT GC ⏳"
    else: return "WAIT ❌"

# ════════════ SKILL MODULES ════════════
def _pat_slope(values):
    try: return linregress(np.arange(len(values)),values)[0]
    except: return 0.0

def _pat_align(values,tol=0.015):
    if len(values)<2: return False
    avg=np.mean(values)
    return all(abs(v-avg)/max(avg,1e-9)<tol for v in values)

def detect_chart_pattern(df):
    if df is None or len(df)<50: return None,"neutral",0
    try:
        d=df.reset_index(drop=True)
        mi=argrelextrema(d['Low'].values,np.less_equal,order=3)[0]
        ma=argrelextrema(d['High'].values,np.greater_equal,order=3)[0]
        if len(mi)<3 or len(ma)<3: return None,"neutral",0
        peaks=d['High'].values[ma]; valleys=d['Low'].values[mi]
        s_high=_pat_slope(peaks[-4:]); s_low=_pat_slope(valleys[-4:])
        if abs(s_high)<0.0005 and s_low>0.0002: return "Ascending Triangle","bullish",2.0
        if abs(s_low)<0.0005 and s_high<-0.0002: return "Descending Triangle","bearish",2.0
        if _pat_align(valleys[-2:]): return "Double Bottom","bullish",1.5
        if _pat_align(peaks[-2:]): return "Double Top","bearish",1.5
        if -0.002<s_high<-0.0002 and -0.002<s_low<-0.0002: return "Bull Flag","bullish",1.5
        if abs(s_high)<0.0005 and abs(s_low)<0.0005: return "Rectangle","neutral",0.5
    except: pass
    return None,"neutral",0

def calc_zeta_field(df, basis=0.0):
    try:
        high=df['High']; low=df['Low']; close=df['Close']; vol=df['Volume']
        tr=pd.concat([high-low,(high-close.shift()).abs(),(low-close.shift()).abs()],axis=1).max(axis=1)
        atr=tr.rolling(14).mean(); natr=atr/close.replace(0,np.nan)*100
        v_term=float(expit(float(natr.iloc[-1])/5.0)) if not pd.isna(natr.iloc[-1]) else 0.5
        mfm=((close-low)-(high-close))/(high-low).replace(0,np.nan)
        cmf=(mfm*vol).rolling(20).sum()/vol.rolling(20).sum().replace(0,np.nan)
        f_term=(float(cmf.iloc[-1])+1)/2 if not pd.isna(cmf.iloc[-1]) else 0.5
        tp=(high+low+close)/3; sma_tp=tp.rolling(20).mean()
        mad=tp.rolling(20).apply(lambda x: np.mean(np.abs(x-np.mean(x))),raw=True)
        cci=(tp-sma_tp)/(0.015*mad.replace(0,np.nan))
        c_term=float(expit(float(cci.iloc[-1])/100)) if not pd.isna(cci.iloc[-1]) else 0.5
        b_term=1.0-min(abs(basis)*100,1.0)
        dd=close.diff(); g=dd.clip(lower=0).rolling(14).mean(); ll=(-dd.clip(upper=0)).rolling(14).mean()
        rsi=100-100/(1+g/ll.replace(0,np.nan))
        s_term=float(rsi.iloc[-1])/100.0 if not pd.isna(rsi.iloc[-1]) else 0.5
        roc=close.pct_change(9)*100
        a_term=float(expit(float(roc.iloc[-1]))) if not pd.isna(roc.iloc[-1]) else 0.5
        rvol=vol.iloc[-1]/max(vol.rolling(20).mean().iloc[-1],1); h_term=min(rvol/5.0,1.0)
        up=high.diff(); dn=-low.diff()
        plus_dm=((up>dn)&(up>0))*up; minus_dm=((dn>up)&(dn>0))*dn
        tr14=tr.rolling(14).sum()
        plus_di=100*(plus_dm.rolling(14).sum()/tr14.replace(0,np.nan))
        minus_di=100*(minus_dm.rolling(14).sum()/tr14.replace(0,np.nan))
        dx=100*(plus_di-minus_di).abs()/(plus_di+minus_di).replace(0,np.nan)
        adx=dx.rolling(14).mean()
        t_term=float(adx.iloc[-1])/100.0 if not pd.isna(adx.iloc[-1]) else 0.5
        zeta=((v_term+f_term+c_term+b_term+s_term+a_term+h_term+t_term)/8.0)*100.0
        score_add=0; reason=""
        if zeta>70: score_add,reason=1,f"ζ-High ({zeta:.0f})"
        elif zeta<30: score_add,reason=1,f"ζ-Low ({zeta:.0f})"
        return round(zeta,1),score_add,reason
    except: return 50.0,0,""

def detect_divergence(df):
    try:
        if df is None or len(df)<30: return 0,""
        close=df['Close'].values
        dd=df['Close'].diff()
        g=dd.clip(lower=0).ewm(span=14,adjust=False).mean(); ll=(-dd.clip(upper=0)).ewm(span=14,adjust=False).mean()
        rsi=100-100/(1+g/ll.replace(0,np.nan))
        rmin=rsi.rolling(14).min(); rmax=rsi.rolling(14).max()
        stoch_k=(100*(rsi-rmin)/(rmax-rmin).replace(0,np.nan)).values
        hi=argrelextrema(close,np.greater,order=3)[0]; lo=argrelextrema(close,np.less,order=3)[0]
        score=0; reasons=[]
        if len(hi)>=2 and not pd.isna(stoch_k[hi[-1]]) and not pd.isna(stoch_k[hi[-2]]):
            if close[hi[-1]]>close[hi[-2]] and stoch_k[hi[-1]]<stoch_k[hi[-2]]:
                score-=2; reasons.append("Bear Div")
        if len(lo)>=2 and not pd.isna(stoch_k[lo[-1]]) and not pd.isna(stoch_k[lo[-2]]):
            if close[lo[-1]]<close[lo[-2]] and stoch_k[lo[-1]]>stoch_k[lo[-2]]:
                score+=2; reasons.append("Bull Div")
        return score,", ".join(reasons)
    except: return 0,""

def _smc_pivots(df, order=5):
    mi=argrelextrema(df['Low'].values,np.less_equal,order=order)[0]
    ma=argrelextrema(df['High'].values,np.greater_equal,order=order)[0]
    return df['High'].values[ma],df['Low'].values[mi]

def smc_market_structure(df):
    try:
        highs,lows=_smc_pivots(df)
        if len(highs)<2 or len(lows)<2: return "Neutral"
        curr=float(df['Close'].iloc[-1]); lh=float(highs[-1]); ll_=float(lows[-1])
        if abs(curr-ll_)/max(ll_,1)<0.015: return "HL" if ll_>lows[-2] else "LL"
        if abs(curr-lh)/max(lh,1)<0.015: return "HH" if lh>highs[-2] else "LH"
        return "Mid-Range"
    except: return "Neutral"

def smc_order_blocks(df, lookback=50):
    obs={'bull':[],'bear':[]}
    d=df.tail(lookback).reset_index(drop=True)
    for i in range(len(d)-3,0,-1):
        try:
            if d['Close'].iloc[i]<d['Open'].iloc[i] and d['Close'].iloc[i+1]>d['High'].iloc[i]:
                obs['bull'].append((float(d['Low'].iloc[i]),float(d['High'].iloc[i])))
            if d['Close'].iloc[i]>d['Open'].iloc[i] and d['Close'].iloc[i+1]<d['Low'].iloc[i]:
                obs['bear'].append((float(d['Low'].iloc[i]),float(d['High'].iloc[i])))
        except: continue
    return obs

def smc_check_zone(price, obs):
    for l,h in obs['bull'][-5:]:
        if l*0.999<=price<=h*1.001: return "Demand"
    for l,h in obs['bear'][-5:]:
        if l*0.999<=price<=h*1.001: return "Supply"
    return "None"

def analyze_smc(df, side="Long"):
    try:
        score=0; reasons=[]
        struct=smc_market_structure(df)
        if side=="Long":
            if struct=="HL": score+=2; reasons.append("SMC·HL ✓")
            elif struct in ["HH","LL"]: return False,0,[f"Avoid Long @ {struct}"]
        else:
            if struct=="LH": score+=2; reasons.append("SMC·LH ✓")
            elif struct in ["HH","LL"]: return False,0,[f"Avoid Short @ {struct}"]
        obs=smc_order_blocks(df); zone=smc_check_zone(float(df['Close'].iloc[-1]),obs)
        if side=="Long":
            if zone=="Demand": score+=2; reasons.append("In Bullish OB")
            elif zone=="Supply": return False,0,["Avoid Long into Supply"]
        else:
            if zone=="Supply": score+=2; reasons.append("In Bearish OB")
            elif zone=="Demand": return False,0,["Avoid Short into Demand"]
        return True,score,reasons
    except: return True,0,[]

def generate_trading_plan(ticker, close, atr, direction="LONG", tp_mult=1.5, sl_mult=0.8, modal_rp=5_000_000, risk_pct=1.0):
    try:
        if direction=="SHORT": return {"direction":"WAIT","note":"IDX retail: no short"}
        entry=float(close); sl=entry-sl_mult*atr
        tp1=entry+tp_mult*atr; tp2=entry+(tp_mult+0.75)*atr; tp3=entry+(tp_mult+1.75)*atr
        risk_pct_val=abs(entry-sl)/entry*100
        rr1=abs(tp1-entry)/max(abs(entry-sl),1); rr2=abs(tp2-entry)/max(abs(entry-sl),1)
        rr3=abs(tp3-entry)/max(abs(entry-sl),1)
        risk_rp=modal_rp*(risk_pct/100.0)
        shares=int(risk_rp/max(abs(entry-sl),1)); lots=shares//100
        actual_risk=lots*100*abs(entry-sl)
        lots_tp1=max(lots//3,0); lots_tp2=max(lots//3,0); lots_tp3=max(lots-lots_tp1-lots_tp2,0)
        horizon="1-3 hari" if tp_mult<=1.5 else ("3-7 hari" if tp_mult<2.5 else "5-10 hari")
        return {"ticker":ticker,"direction":"LONG","entry":round(entry),"sl":round(sl),
            "tp1":round(tp1),"tp2":round(tp2),"tp3":round(tp3),
            "risk_pct":round(risk_pct_val,2),"rr1":round(rr1,2),"rr2":round(rr2,2),"rr3":round(rr3,2),
            "lots":lots,"lots_tp1":lots_tp1,"lots_tp2":lots_tp2,"lots_tp3":lots_tp3,
            "risk_rp":round(actual_risk),"modal_pct":round(lots*100*entry/max(modal_rp,1)*100,1),
            "horizon":horizon,"invalidation":f"SL hit OR close daily < {round(sl)}"}
    except: return None

# ════════════ LANJUT KE PART 2 ════════════
# ════════════ FETCH INTRADAY ════════════
def fetch_intraday(tickers, interval="15m"):
    all_dfs={}; ticker_list=list(tickers); need_fetch=[]
    for t in ticker_list:
        raw_t=t.replace(".JK","").upper()
        cached=_cache_get(raw_t,interval)
        if cached is not None: all_dfs[t]=cached; continue
        need_fetch.append(t)
    if not need_fetch: return all_dfs
    if DS_KEY:
        def _fetch_one(t):
            raw_t=t.replace(".JK","").upper()
            return t,fetch_ds_ohlcv(raw_t,interval,200,True)
        with ThreadPoolExecutor(max_workers=10) as ex:
            futs={ex.submit(_fetch_one,t):t for t in need_fetch}
            for f in as_completed(futs):
                try:
                    t,df=f.result(timeout=20)
                    if df is not None and len(df)>=20: all_dfs[t]=df
                except: pass
    missing_yf=[t for t in need_fetch if t not in all_dfs]
    if missing_yf:
        yf_data=_fetch_yf_parallel(missing_yf,"7d",interval,workers=5)
        for t_yf,df_yf in yf_data.items():
            if df_yf is not None and len(df_yf)>=20: all_dfs[t_yf]=df_yf
    return all_dfs

def send_telegram(results_top, source="Scanner"):
    if not TOKEN or not CHAT_ID: return False
    now=datetime.now(jakarta_tz); is_open=9<=now.hour<16; sep="━"*28
    market_status="🔴 MARKET OPEN" if is_open else "🌙 AFTER HOURS"
    alert_type="WATCHLIST" if source=="Watchlist" else "SCANNER"
    hdr=(f"{market_status}\n⚡ *MESIN PRESISI {alert_type}*\n"
         f"⏰ `{now.strftime('%H:%M:%S')} WIB` · `{now.strftime('%d %b %Y')}`\n{sep}\n")
    body=""
    for r in results_top[:8]:
        sig=r.get("Signal","-"); mg=r.get("Mesin_Grade","—"); ms=r.get("Mesin_Score",0)
        em=("🎯" if "PRESISI" in mg else "🔵" if "BANDAR" in mg else "💎" if "BAGGER" in mg
            else "⚡" if "KUAT" in mg else "🏆" if("GACOR" in sig or "REVERSAL" in sig)
            else "🔥" if "POTENSIAL" in sig else "👀")
        te="📈" if "▲" in r.get("Trend","") else("📉" if "▼" in r.get("Trend","") else "➡️")
        iq_v=r.get("IQ_Verdict","—"); iq_s=r.get("IQ_Score",0)
        body+=(f"\n{em} *{r['Ticker']}* `[{mg}]` MS:`{ms:.0f}`\n"
               f"   💰 `{r['Price']:,}` {te} · TT:`{sig}`\n"
               f"   📊 IQ: `{iq_v}` ({iq_s:.0f}/100)\n"
               f"   📈 RSI: `{r.get('RSI-EMA',0):.0f}` · RVOL: `{r.get('RVOL',0):.1f}x`\n"
               f"   🎯 TP: `{r['TP']:,}` 🛑 SL: `{r['SL']:,}` R:R `{r.get('R:R',0)}`\n"
               f"   💡 _{r.get('Reasons','')[:60]}_\n")
    footer=f"\n{sep}\n⚡ _Mesin Presisi v2.2 · TT 15M × IQ Daily × Skills_\n⚠️ _BUKAN saran investasi!_"
    try:
        resp=requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            data={"chat_id":CHAT_ID,"text":hdr+body+footer,"parse_mode":"Markdown"},timeout=10)
        return resp.status_code==200
    except: return False

SECTORS={
    "Energi & Mining":["ADRO","BYAN","ITMG","PTBA","HRUM","DOID","GEMS","PGAS","ELSA","MEDC","ESSA","AKRA","RIGS","DSSA","MBAP","KKGI","MYOH","SMMT","BSSR","INDY"],
    "Perbankan":["BBCA","BBRI","BMRI","BBNI","BBTN","BJBR","BJTM","BNGA","BDMN","MEGA","BBYB","ARTO","BRIS","AGRO","BBHI","NOBU","BACA","MAYA"],
    "Properti":["BSDE","CTRA","SMRA","LPKR","PWON","APLN","ASRI","DILD","DUTI","MDLN","MKPI","JRPT","KIJA","BEST","GPRA","DART","MTLA"],
    "Infrastruktur":["JSMR","TLKM","EXCL","ISAT","TBIG","TOWR","WIKA","ADHI","PTPP","WSKT","WTON","WEGE","BIRD","TMAS","SMDR","BBRM"],
    "Konsumer":["UNVR","ICBP","INDF","MYOR","KLBF","GGRM","HMSP","DLTA","ROTI","SKBM","GOOD","HOKI","CLEO","MIKA","HEAL","SILO","KAEF","DVLA"],
    "Industri & Otomotif":["ASII","AUTO","SMSM","HEXA","UNTR","SCCO","KBLI","VOKS","BRAM","GJTL","IMAS","INTP","SMGR","CPIN","JPFA","BRPT","TPIA"],
    "Teknologi":["GOTO","BUKA","EMTK","MNCN","SCMA","MTEL","MTDL","MLPT","CHIP","LUCK","NFCX","WIFI","DIGI","AWAN","INET","MCAS","TECH"],
    "Shipping & Logistik":["TMAS","SMDR","BBRM","NELY","SHIP","ELPI","BIRD","ASSA","WEHA","SAFE","ATLI","MIRA","RAJA","RIGS","MBSS"],
    "Petrokimia & Kimia":["TPIA","BRPT","BUDI","EKAD","ETWA","ESSA","AKPI","CPRO","SRSN","UNIC"],
}
HORMUZ_SECTORS=["Energi & Mining","Shipping & Logistik","Petrokimia & Kimia"]

regime,ihsg_price,ema20,ema55,regime_detail,ihsg_chg=get_market_regime()
rcfg=get_regime_config(regime); rcolor=rcfg["color"]
chg_col="#00ff88" if ihsg_chg>=0 else "#ff3d5a"; chg_sym="▲" if ihsg_chg>=0 else "▼"
now_jkt=datetime.now(jakarta_tz); is_open=9<=now_jkt.hour<16

_arj_used=arjum_usage()
_arj_pct=_arj_used/ARJUM_DAILY_LIMIT*100 if ARJUM_DAILY_LIMIT else 0
_arj_col="#00ff88" if _arj_pct<50 else "#ffb700" if _arj_pct<80 else "#ff3d5a"
_src_lbl="⚡ Arjum" if ARJUM_KEY else ("⚡ DS" if DS_KEY else "📊 yF")

st.markdown(f"""<div class="tt-header">
  <div><div class="tt-logo">⚡ MESIN PRESISI v2.2</div>
  <div class="tt-sub">TT 15M × IQ Daily × Skills (Patterns·Zeta·Div·SMC) · Cost-Aware · Falsifiable</div></div>
  <div class="live-badge"><div class="live-dot"></div>
    {_src_lbl} · {now_jkt.strftime("%H:%M:%S")} WIB<br>
    <span style="font-size:9px;color:{_arj_col}">API {_arj_used:,}/{ARJUM_DAILY_LIMIT:,} ({_arj_pct:.0f}%) · Universe: {_STOCK_SRC_LABEL}</span>
  </div>
</div>""", unsafe_allow_html=True)

st.markdown(f"""<div style="background:rgba(0,0,0,.4);border:1px solid {rcolor}44;border-radius:8px;padding:12px 16px;margin-bottom:14px;border-left:4px solid {rcolor};">
  <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;">
    <div><div style="font-family:Space Mono,monospace;font-size:12px;font-weight:700;color:{rcolor};letter-spacing:1px;">{rcfg["label"]}</div>
         <div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-top:3px;">{rcfg["desc"]}</div></div>
    <div style="text-align:right;font-family:Space Mono,monospace;">
      <div style="font-size:18px;font-weight:700;color:{rcolor};">{ihsg_price:,.0f} <span style="font-size:11px;color:{chg_col}">{chg_sym}{abs(ihsg_chg):.2f}%</span></div>
      <div style="font-size:9px;color:#4a5568;">EMA10 {ema20:,.0f} · EMA30 {ema55:,.0f}</div>
    </div>
  </div>
</div>""", unsafe_allow_html=True)

tab_scanner,tab_watchlist,tab_bsjp,tab_sector,tab_gapup,tab_trail,tab_backtest,tab_sizing,tab_plan,tab_journal=st.tabs(
    ["🔥 Scanner","👁️ Watchlist","🌙 BSJP","🏭 Sektor","📈 Gap Up","🎯 Trailing","📊 Backtest","🧮 Sizing","📋 Plan","📓 Journal"])

# ════ TAB SCANNER ════
with tab_scanner:
    with st.expander("⚙️ Scanner Settings", expanded=False):
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
            if tele_on and TOKEN and CHAT_ID:
                st.caption(f"✅ TG ready · Token: ...{TOKEN[-8:]}")
                if st.button("🔔 Test TG",key="tg_test_btn",use_container_width=True):
                    ok=send_telegram([{"Ticker":"TEST","Price":1000,"Signal":"TEST 🔔","Mesin_Grade":"PRESISI 🎯","Mesin_Score":99,"IQ_Verdict":"BUY","IQ_Score":80,"Trend":"▲ UP","RSI-EMA":55,"RVOL":2.1,"TP":1050,"SL":980,"R:R":2.0,"Reasons":"Test notif"}])
                    if ok: st.success("✅ Terkirim!")
                    else: st.error("❌ Gagal")
        with sc2:
            st.markdown('<div class="settings-label">FILTER</div>',unsafe_allow_html=True)
            auto_thresh=st.toggle("🤖 Auto-Threshold",value=True,key="auto_thr")
            if auto_thresh:
                min_score=rcfg["min_score"]; vol_thresh=rcfg["min_rvol"]
                st.caption(f"Auto: Score≥{min_score} · RVOL≥{vol_thresh}x")
            else:
                min_score=st.slider("Min Score",0,6,2,key="msc")
                vol_thresh=st.slider("Min RVOL",1.0,5.0,1.5,0.1,key="vol")
            only_liquid=st.toggle("✅ Only Liquid",value=True,key="only_liquid")
            min_avg_turn=st.number_input("Min Avg Turnover/bar (Jt Rp)",value=50,step=10,key="min_avg_turn")
            min_nonzero_ratio=st.slider("Min Nonzero Vol Ratio",0.0,1.0,0.6,0.05,key="min_nonzero_ratio")
            min_atr_pct=st.slider("Min ATR%",0.05,1.5,0.25,0.05,key="min_atr_pct")
            liq_window=st.slider("Volume Window",10,60,20,5,key="liq_window")
            min_turn=st.number_input("Min Turnover (M Rp)",value=100,step=100,key="trn")*1_000_000
            pos_size_jt=st.number_input("Modal per Posisi (Jt Rp)",value=5,step=1,key="pos_size_jt")
        with sc3:
            st.markdown('<div class="settings-label">TAMPILAN</div>',unsafe_allow_html=True)
            view_mode=st.radio("View",["Card View 🃏","Table View 📊"],label_visibility="collapsed",key="vm")
            quick_mode=st.toggle("⚡ Quick (200 saham)",value=False,key="quick_mode")
            auto_scan_on=st.toggle("🔄 Auto-Scan",value=True,key="auto_scan_on")
            auto_scan_sec=st.selectbox("Interval",[900,1800,3600],index=1,format_func=lambda s:f"{s//60} menit",key="auto_scan_sec")
            st.caption(f"🎯 Regime: {regime} · Mode: {scan_mode}")
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("🔄 Refresh Universe", use_container_width=True, key="btn_refresh_univ",
                         help="Force fetch list saham terbaru dari Arjum (IPO/delisting)"):
                _new_univ = fetch_arjum_universe(force_fresh=True)
                if _new_univ and len(_new_univ) >= 100:
                    st.success(f"✅ Universe refresh: {len(_new_univ)} saham")
                    st.rerun()
                else:
                    st.error("❌ Gagal refresh — API Arjum unavailable")
            st.caption(f"📊 Sumber: {_STOCK_SRC_LABEL}")
            st.caption(f"⏱️ {_STOCK_SRC_DETAIL}")

    do_scan_btn=st.button("🔥 MULAI SCAN SEKARANG",type="primary",use_container_width=True,key="btn_scan")
    _now_check=now_jkt.timestamp(); auto_trigger=False
    _auto_on=bool(st.session_state.get("auto_scan_on",True))
    _auto_sec=int(st.session_state.get("auto_scan_sec",1800))
    if _auto_on and not do_scan_btn:
        _lst=st.session_state.last_scan_time
        if _lst is None: auto_trigger=True
        elif _now_check-_lst>=_auto_sec: auto_trigger=True

    if do_scan_btn or auto_trigger:
        scan_list=stocks_yf[:200] if quick_mode else stocks_yf
        n_scan=len(scan_list); prog_ph=st.empty(); pb=st.progress(0)
        prog_ph.markdown(f'<div style="color:#ff7b00;font-family:Space Mono,monospace;font-size:12px;">⚡ Fetching {n_scan} saham...</div>',unsafe_allow_html=True)
        pb.progress(0.1)
        data_dict={}
        try:
            ticker_list=list(scan_list); need_fetch=[]
            for t in ticker_list:
                raw_t=t.replace(".JK","").upper()
                cached=_cache_get(raw_t,"15m")
                if cached is not None: data_dict[t]=cached; continue
                need_fetch.append(t)
            n_need=len(need_fetch)
            def _f(t):
                raw_t=t.replace(".JK","").upper()
                if DS_KEY:
                    df=fetch_ds_ohlcv(raw_t,"15m",200,True)
                    if df is not None: return t,df
                return t,None
            done=[0]
            if need_fetch:
                with ThreadPoolExecutor(max_workers=10) as ex:
                    futs={ex.submit(_f,t):t for t in need_fetch}
                    for fut in as_completed(futs):
                        try:
                            t,df=fut.result(timeout=15); done[0]+=1
                            if df is not None and len(df)>=20: data_dict[t]=df
                            if done[0]%30==0: pb.progress(min(0.10+(done[0]/max(n_need,1))*0.40,0.50))
                        except: done[0]+=1
            missing_yf=[t for t in ticker_list if t not in data_dict]
            if missing_yf:
                yf_fetched=_fetch_yf_parallel(missing_yf,"5d","15m",workers=6)
                for t_yf,df_yf in yf_fetched.items():
                    if df_yf is not None and len(df_yf)>=20: data_dict[t_yf]=df_yf
            pb.progress(0.76)
            st.session_state.data_dict=data_dict
            n_ds=sum(1 for t in data_dict if hasattr(data_dict[t],"columns") and "FBuy" in data_dict[t].columns)
            n_yf=len(data_dict)-n_ds
            src_lbl=f"DS:{n_ds} + yF:{n_yf}" if DS_KEY else f"yF:{n_yf}"
            prog_ph.markdown(f'<div style="color:#00ff88;font-family:Space Mono,monospace;font-size:11px;">✅ Data siap: <b>{len(data_dict)}</b> saham [{src_lbl}]</div>',unsafe_allow_html=True)
            if len(data_dict)==0:
                st.error("⚠️ 0 saham berhasil di-fetch.")
                st.stop()
            pb.progress(0.78)
            daily_dict={}
            if ARJUM_KEY:
                def _fd(t):
                    raw_t=t.replace(".JK","").upper()
                    return t,fetch_arjum_daily_ohlcv(raw_t,limit=200,force_fresh=False)
                with ThreadPoolExecutor(max_workers=8) as ex:
                    futs={ex.submit(_fd,t):t for t in list(data_dict.keys())}
                    for f in as_completed(futs):
                        try:
                            t,df=f.result(timeout=12)
                            if df is not None and len(df)>=2: daily_dict[t]=df
                        except: pass
            missing_daily=[t for t in list(data_dict.keys()) if t not in daily_dict]
            if missing_daily:
                yf_daily=_fetch_yf_parallel(missing_daily,"60d","1d",workers=4)
                for t_d,df_d in yf_daily.items():
                    if df_d is not None and len(df_d)>=2: daily_dict[t_d]=df_d
            st.session_state.daily_dict=daily_dict
            pb.progress(0.85)
            prog_ph.markdown(f'<div style="color:#00ff88;font-family:Space Mono,monospace;font-size:11px;">⚙️ Processing {len(data_dict)} saham + skills...</div>',unsafe_allow_html=True)
            results=[]; tickers=list(data_dict.keys()); skip={"short":0,"price0":0,"liq":0,"score":0,"error":0}
            last_err=[""]; _sig_batch=[]
            for i,ticker_yf in enumerate(tickers):
                pb.progress(0.85+(i+1)/max(len(tickers),1)*0.14)
                try:
                    df=data_dict[ticker_yf].copy()
                    if len(df)<30: skip["short"]+=1; continue
                    src_tag="DS" if "FBuy" in df.columns else "YF"
                    df=apply_intraday_indicators(df)
                    r=df.iloc[-1]; p=df.iloc[-2]; p2=df.iloc[-3] if len(df)>=3 else p
                    def _si(v,d=0):
                        try: x=float(v); return d if(np.isnan(x) or np.isinf(x)) else int(x)
                        except: return d
                    def _sff(v,d=0.0):
                        try: x=float(v); return d if(np.isnan(x) or np.isinf(x)) else x
                        except: return d
                    close=_sff(r.get("Close",0)); vol=_sff(r.get("Volume",0))
                    if close<=0: skip["price0"]+=1; continue
                    if only_liquid:
                        avg_vol_val=_sff(r.get("AvgVol",0),0.0); avg_turn_val=avg_vol_val*close
                        atr_v=_sff(r.get("ATR",0),0.0); atr_pct_val=(atr_v/close)*100 if close>0 else 0.0
                        try:
                            tail=df["Volume"].tail(int(liq_window))
                            nzr=float((tail>0).mean()) if len(tail) else 0.0
                        except: nzr=0.0
                        if avg_turn_val<float(min_avg_turn)*1e6: skip["liq"]+=1; continue
                        if nzr<float(min_nonzero_ratio): skip["liq"]+=1; continue
                        if atr_pct_val<float(min_atr_pct): skip["liq"]+=1; continue
                    ticker_raw=ticker_yf.replace(".JK","").upper()
                    df_d=daily_dict.get(ticker_yf)
                    if df_d is None:
                        df_d=daily_dict.get(ticker_raw)
                    if df_d is not None and len(df_d)>=2:
                        c1=float(df_d.iloc[-1]["Close"]); c0=float(df_d.iloc[-2]["Close"])
                        gain_pct=(c1-c0)/max(c0,1)*100
                        d_vol=float(df_d.iloc[-1]["Volume"]); turnover=c1*d_vol if d_vol>0 else close*float(df["Volume"].fillna(0).sum())
                    else:
                        turnover=close*float(df["Volume"].fillna(0).sum()); gain_pct=float(r.get("ROC3",0))*100
                    rvol=float(r["RVOL"]) if not np.isnan(float(r["RVOL"])) else 1.0
                    if turnover<min_turn or rvol<vol_thresh: skip["score"]+=1; continue
                    if scan_mode=="Scalping ⚡": sc,reasons,_=score_scalping(r,p,p2)
                    elif scan_mode=="Momentum 🚀": sc,reasons,_=score_momentum(r,p,p2)
                    elif scan_mode=="Bagger 💎": sc,reasons,_=score_bagger(r,p,p2,df)
                    else: sc,reasons,_=score_reversal(r,p,p2)
                    if sc<min_score: skip["score"]+=1; continue
                    sig=get_signal(sc,scan_mode)
                    sig_v2,sc_v2,flags_v2,gc_now=get_sinyal_v2(r,p,p2)
                    aksi_v2=get_aksi_v2(sig_v2,gc_now,sc_v2)
                    atr=float(r["ATR"]) if not np.isnan(float(r["ATR"])) else close*0.01
                    _atrp=(atr/close*100) if close>0 else 1.0
                    if scan_mode=="Scalping ⚡": tp=close+1.5*atr; sl=close-0.8*atr
                    elif scan_mode=="Momentum 🚀": tp=close+2.0*atr; sl=close-0.8*atr
                    elif scan_mode=="Bagger 💎": tp=close+3.0*atr; sl=close-1.0*atr
                    else: tp=close+2.5*atr; sl=close-0.6*atr
                    rr=(tp-close)/max(close-sl,0.01)
                    e9=float(r["EMA9"]); e21=float(r["EMA21"]); e50=float(r["EMA50"])
                    trend="▲ UP" if e9>e21>e50 else("▼ DOWN" if e9<e21<e50 else "◆ SIDE")
                    def _sf(v,d=0.):
                        try: x=float(v); return d if(np.isnan(x) or np.isinf(x)) else x
                        except: return d
                    fnet3=_sf(r.get("FNet3",0)); fnet8=_sf(r.get("FNet8",0)); fratio=_sf(r.get("FRatio",0.5),0.5)
                    fbuy=_sf(r.get("FBuy",0)); fsell=_sf(r.get("FSell",0))
                    has_asing=(fbuy+fsell)>0
                    if not has_asing: fdir="—"; fc_="#4a5568"
                    elif fnet3>0 and fnet8>0: fdir="🔵 BELI"; fc_="#4da6ff"
                    elif fnet3<0 and fnet8<0: fdir="🔴 JUAL"; fc_="#ff3d5a"
                    else: fdir="⚪ MIX"; fc_="#888888"
                    lwick=_sf(r.get("LWick",0)); vb=turnover/1e9
                    val_str=f"{vb:.1f}B" if vb>=1 else f"{round(vb*1000,0):.0f}M"
                    iq=iq_analyze(df_d)
                    mg,mg_col,ms=calc_mesin_grade(sc,sig+"|"+sig_v2,iq["iq_score"],iq["iq_verdict"],iq["iq_bagger"])
                    # SKILLS
                    pattern_name,pattern_bias,pattern_str=detect_chart_pattern(df)
                    zeta_score,zeta_add,zeta_reason=calc_zeta_field(df,basis=0.0)
                    div_score,div_reason=detect_divergence(df)
                    _side="Long" if ("JUAL" not in sig_v2 and "BEAR" not in iq["iq_ma"]) else "Short"
                    smc_ok,smc_score,smc_reasons=analyze_smc(df,side=_side)
                    skill_tags=[]; skill_bonus=0.0
                    if pattern_name:
                        skill_tags.append(f"📐{pattern_name}")
                        if pattern_bias=="bullish": skill_bonus+=pattern_str*0.5
                        elif pattern_bias=="bearish": skill_bonus-=pattern_str*0.5
                    if zeta_reason:
                        skill_tags.append(zeta_reason); skill_bonus+=zeta_add*0.5
                    if div_reason:
                        skill_tags.append(f"🔀{div_reason}"); skill_bonus+=div_score*0.25
                    if smc_ok and smc_reasons:
                        skill_tags.extend(smc_reasons); skill_bonus+=smc_score*0.25
                    elif not smc_ok:
                        skill_tags.append(f"⛔{'·'.join(smc_reasons)}"); skill_bonus-=1.0
                    skill_bonus=max(-2.0,min(2.0,skill_bonus))
                    ms=max(0,min(100,ms+skill_bonus))
                    # Daily foreign flow
                    d_fnet3=0.0; d_fnet8=0.0; d_fratio=0.5
                    if df_d is not None and "FBuy" in df_d.columns and "FSell" in df_d.columns:
                        try:
                            _fn=df_d["FBuy"]-df_d["FSell"]
                            d_fnet3=float(_fn.tail(3).sum()); d_fnet8=float(_fn.tail(8).sum())
                            _fb8=float(df_d["FBuy"].tail(8).sum()); _fs8=float(df_d["FSell"].tail(8).sum())
                            if _fb8+_fs8>0: d_fratio=_fb8/(_fb8+_fs8)
                        except: pass
                    # Trading plan
                    _tp_mult=(1.5 if scan_mode=="Scalping ⚡" else 2.5 if scan_mode=="Momentum 🚀" else 3.0 if scan_mode=="Bagger 💎" else 2.5)
                    plan_dir="LONG" if smc_ok and "JUAL" not in sig_v2 else "WAIT"
                    trade_plan=generate_trading_plan(ticker_raw,close,atr,direction=plan_dir,tp_mult=_tp_mult,sl_mult=0.8,modal_rp=float(pos_size_jt)*1e6,risk_pct=1.0) if plan_dir=="LONG" else None
                    # Physics
                    sigma_d=ewma_sigma_daily(df_d)
                    adv_rp=adv20_rp(df_d,fallback=turnover)
                    impact_pct,part_pct=sqrt_impact(float(pos_size_jt)*1e6,adv_rp,sigma_d)
                    tot_cost=round(trade_cost_pct(close)+(impact_pct or 0),2)
                    if part_pct is not None and part_pct>2.0:
                        reasons.append(f"⚠️ Posisi {part_pct:.1f}% ADV")
                    results.append({
                        "Ticker":stock_map.get(ticker_yf,ticker_raw),"Price":_si(close),"Score":sc,
                        "Signal":sig,"Sinyal_v2":sig_v2,"Aksi_v2":aksi_v2,"Trend":trend,
                        "RSI-EMA":round(_sff(r.get("RSI_EMA",50),50),1),
                        "Stoch K":round(_sff(r.get("STOCH_K",50),50),1),
                        "MACD Hist":round(_sff(r.get("MACD_Hist",0)),4),
                        "RVOL":round(rvol,2),"BB%":round(_sff(r.get("BB_pct",0.5),0.5),2),
                        "ROC 3B%":round(gain_pct,2),"Gain":round(gain_pct,1),
                        "VWAP":_si(r.get("VWAP",close),_si(close)),
                        "TP":_si(tp),"SL":_si(sl),"R:R":round(rr,1),
                        "Turnover(M)":round(turnover/1e6,1),"Val":val_str,
                        "Reasons":" · ".join(reasons),"_class":get_card_class(sig),
                        "LWick":round(lwick,1),"FDir":fdir,"FC":fc_,
                        "FNet3":_si(fnet3),"FNet8":_si(fnet8),"FRatio":round(fratio,2),
                        "sc_v2":sc_v2,"gc_now":gc_now,"Src":src_tag,"Cost%":tot_cost,
                        "Impact%":round(impact_pct,2) if impact_pct is not None else 0.0,
                        "Part%":part_pct if part_pct is not None else 0.0,
                        "Sigma_d":round(sigma_d,2) if sigma_d else 0.0,
                        "IQ_Score":round(iq["iq_score"],1),"IQ_Verdict":iq["iq_verdict"],
                        "IQ_MA":iq["iq_ma"],"IQ_Mom":iq["iq_mom"],"IQ_Bagger":iq["iq_bagger"],
                        "IQ_RSI":iq["iq_rsi"],
                        "Skills":" · ".join(skill_tags) if skill_tags else "—",
                        "Pattern":pattern_name or "—","Zeta":zeta_score,
                        "Div":"Bull" if div_score>0 else ("Bear" if div_score<0 else "—"),
                        "SMC":"✓" if smc_ok else "⛔",
                        "D_FNet3":d_fnet3,"D_FNet8":d_fnet8,"D_FRatio":round(d_fratio,2),
                        "D_FDir":("🔵" if d_fnet3>0 and d_fnet8>0 else "🔴" if d_fnet3<0 and d_fnet8<0 else "⚪"),
                        "Mesin_Grade":mg,"Mesin_Color":mg_col,"Mesin_Score":round(ms,1),
                        "Plan":trade_plan,
                    })
                    try:
                        _roc3_15=_sff(r.get("ROC3",0))*100
                        _sig_batch.append({
                            "ts":now_jkt.strftime("%H:%M:%S"),"date":now_jkt.strftime("%Y-%m-%d"),
                            "ticker":ticker_raw,"mode":scan_mode,"score":sc,"signal":sig,
                            "sinyal_v2":sig_v2,"mesin_grade":mg,"mesin_score":round(ms,1),
                            "iq_verdict":iq["iq_verdict"],"iq_score":round(iq["iq_score"],1),
                            "price":_si(close),"atr_pct":round(_atrp,2),"rvol":round(rvol,2),
                            "rsi_ema":round(_sff(r.get("RSI_EMA",50),50),1),
                            "above_vwap":int(close>_sf(r.get("VWAP",close),close)),
                            "ema_stack":int(e9>e21>e50),
                            "roc_atr":round(_roc3_15/max(_atrp,0.05),2),
                            "src":src_tag,
                            "cost_pct":round(trade_cost_pct(close)/2.0+(impact_pct or 0),2),
                            "sigma_d":round(sigma_d,2) if sigma_d else np.nan,
                            "impact_pct":round(impact_pct,3) if impact_pct is not None else np.nan,
                            "part_pct":part_pct if part_pct is not None else np.nan,
                            "t1_ret":np.nan,"t3_ret":np.nan,"t5_ret":np.nan,
                        })
                    except: pass
                except Exception as _exc:
                    skip["error"]+=1; last_err[0]=f"{type(_exc).__name__}: {str(_exc)[:80]}"; continue
            try: _n_logged=log_signals_batch(_sig_batch)
            except: _n_logged=0
            pb.progress(1.0); prog_ph.empty(); pb.empty()
            st.session_state.scan_results=results
            st.session_state.last_scan_time=now_jkt.timestamp()
            _tt_save(results,st.session_state.last_scan_time)
            st.session_state.last_scan_mode=scan_mode
            if not results:
                st.warning(f"⚠️ 0 sinyal dari {len(data_dict)} saham.")
                st.error(f"**Skip:** short={skip['short']} price0={skip['price0']} liq={skip['liq']} score={skip['score']} err={skip['error']}\nLast error: `{last_err[0] or 'none'}`")
            else:
                st.success(f"✅ {len(results)} sinyal · 📓 {_n_logged} baru ke Journal")
            if tele_on and results:
                if "tt_last_sent" not in st.session_state: st.session_state.tt_last_sent=set()
                dft=pd.DataFrame(results).sort_values("Mesin_Score",ascending=False)
                qm=dft["Mesin_Grade"].astype(str).str.contains("PRESISI|BANDAR|BAGGER|KUAT|MONITOR|TT-ONLY|WATCH",na=False)
                dft_q=dft[qm]; cs=set(dft_q["Ticker"].tolist())
                na=cs-st.session_state.tt_last_sent
                if na:
                    tn=dft_q[dft_q["Ticker"].isin(na)].head(8).to_dict("records")
                    if tn: send_telegram(tn)
                    st.session_state.tt_last_sent.update(na)
                st.session_state.tt_last_sent=st.session_state.tt_last_sent&cs
        except Exception as e:
            try: prog_ph.empty(); pb.empty()
            except: pass
            st.error(f"Scan error: {str(e)[:100]}")

    if st.session_state.last_scan_time:
        _nc=now_jkt.timestamp(); _sec=int(st.session_state.get("auto_scan_sec",1800))
        _rem=max(0,_sec-(_nc-st.session_state.last_scan_time))
        _lt=datetime.fromtimestamp(st.session_state.last_scan_time,jakarta_tz).strftime("%H:%M:%S")
        st.caption(f"⏱️ Next: {int(_rem//60):02d}:{int(_rem%60):02d} · Last: {_lt} WIB")

    results=st.session_state.scan_results
    if not results and not do_scan_btn and not auto_trigger:
        st.markdown(f"""<div style="text-align:center;padding:48px;color:#4a5568;font-family:Space Mono,monospace;">
          <div style="font-size:36px;margin-bottom:12px;">⚡</div>
          <div style="font-size:13px;letter-spacing:2px;">KLIK SCAN UNTUK MULAI</div>
        </div>""", unsafe_allow_html=True)
    elif results:
        df_out=pd.DataFrame(results).sort_values("Mesin_Score",ascending=False).reset_index(drop=True)
        _mgcol=df_out["Mesin_Grade"].astype(str)
        presisi_list=df_out[_mgcol.str.contains("PRESISI",na=False)]["Ticker"].tolist()
        bandar_m_list=df_out[_mgcol.str.contains("BANDAR",na=False)]["Ticker"].tolist()
        kuat_list=df_out[_mgcol.str.contains("KUAT",na=False)]["Ticker"].tolist()
        bagger_m_list=df_out[_mgcol.str.contains("BAGGER",na=False)]["Ticker"].tolist()
        iq_buy_cnt=len(df_out[df_out["IQ_Verdict"]=="BUY"])
        rsi_avg=df_out["RSI-EMA"].mean()
        rsi_color_avg="#00ff88" if rsi_avg>50 else "#ffb700" if rsi_avg>35 else "#ff3d5a"
        st.markdown(f"""<div class="metric-row">
          <div class="metric-card" style="border-top-color:{rcolor}"><div class="metric-label">Regime</div>
            <div class="metric-value" style="font-size:16px;color:{rcolor}">{regime}</div>
            <div class="metric-sub">{ihsg_price:,.0f} {chg_sym}{abs(ihsg_chg):.2f}%</div></div>
          <div class="metric-card orange"><div class="metric-label">Mode</div>
            <div class="metric-value" style="font-size:13px;">{st.session_state.last_scan_mode}</div></div>
          <div class="metric-card green"><div class="metric-label">Signal</div>
            <div class="metric-value">{len(df_out)}</div></div>
          <div class="metric-card purple"><div class="metric-label">BAGGER</div>
            <div class="metric-value" style="color:#bf5fff">{len(bagger_m_list)}</div></div>
          <div class="metric-card red"><div class="metric-label">PRESISI</div>
            <div class="metric-value">{len(presisi_list)}</div></div>
          <div class="metric-card amber"><div class="metric-label">KUAT</div>
            <div class="metric-value">{len(kuat_list)}</div></div>
          <div class="metric-card"><div class="metric-label">IQ BUY</div>
            <div class="metric-value" style="color:#2dd4bf">{iq_buy_cnt}</div></div>
          <div class="metric-card"><div class="metric-label">Avg RSI</div>
            <div class="metric-value" style="color:{rsi_color_avg}">{rsi_avg:.1f}</div></div>
        </div>""", unsafe_allow_html=True)

        th='<div class="tape-wrap"><div class="tape-inner">'
        for _,row in df_out.head(20).iterrows():
            roc=row["ROC 3B%"]; ib="BAGGER" in row["Signal"] or "KANDIDAT" in row["Signal"]
            cls="bagger" if ib else("up" if roc>0 else("down" if roc<0 else "flat"))
            sym="[BAG]" if "BAGGER" in str(row.get("Mesin_Grade","")) else("[PRESISI]" if "PRESISI" in str(row.get("Mesin_Grade","")) else "UP" if roc>0 else "DN")
            th+=f'<span class="tape-item {cls}">{row["Ticker"]} {int(row["Price"])} {sym} IQ:{row.get("IQ_Verdict","?")}</span>'
        th+=th.replace('tape-inner">',''); th+='</div></div>'
        st.markdown(th, unsafe_allow_html=True)

        if view_mode=="Card View 🃏":
            st.markdown('<div class="section-title">Signal Cards</div>', unsafe_allow_html=True)
            ch='<div class="signal-grid">'
            for _,row in df_out.head(20).iterrows():
                si=int(row["Score"]); ib="BAGGER" in row["Signal"] or "KANDIDAT" in row["Signal"]
                bc="filled-purple" if ib else "filled"
                bars="".join([f'<div class="sc-bar {bc if i<si else "empty"}" style="width:24px"></div>' for i in range(6)])
                rc="#00ff88" if row["ROC 3B%"]>0 else "#ff3d5a"
                te="UP" if "UP" in row.get("Trend","") else("DN" if "DN" in row.get("Trend","") else "~")
                mg_v=str(row.get("Mesin_Grade","—")); mg_c=str(row.get("Mesin_Color","#4a5568"))
                ms_v=float(row.get("Mesin_Score",0)); iq_v=str(row.get("IQ_Verdict","—")); iq_s=float(row.get("IQ_Score",0))
                skills_s=str(row.get("Skills","—"))[:40]
                ch+=(f'<div class="signal-card {row["_class"]}">'
                     f'<div style="display:flex;justify-content:space-between;">'
                     f'<div><div class="sc-ticker">{row["Ticker"]}</div>'
                     f'<div class="sc-price" style="color:{rc}">{int(row["Price"]):,} {te}</div></div>'
                     f'<div style="text-align:right"><div style="font-family:Space Mono,monospace;font-size:9px;color:#4a5568">MS</div>'
                     f'<div style="font-family:Space Mono,monospace;font-size:18px;font-weight:700;color:{mg_c}">{ms_v:.0f}</div></div></div>'
                     f'<div style="margin:5px 0;font-size:11px;color:{mg_c}">{mg_v}</div>'
                     f'<div class="sc-bars">{bars}</div>'
                     f'<div class="sc-stats"><div class="sc-stat">IQ <span>{iq_v}</span></div>'
                     f'<div class="sc-stat">RSI <span>{row["RSI-EMA"]}</span></div>'
                     f'<div class="sc-stat">RVOL <span>{row["RVOL"]}x</span></div></div>'
                     f'<div class="sc-stats" style="margin-top:6px"><div class="sc-stat">TP <span style="color:#00ff88">{int(row["TP"]):,}</span></div>'
                     f'<div class="sc-stat">SL <span style="color:#ff3d5a">{int(row["SL"]):,}</span></div></div>'
                     f'<div style="margin-top:6px;font-size:9px;color:#4a5568;font-family:Space Mono,monospace">{skills_s}</div>'
                     f'</div>')
            ch+='</div>'; st.markdown(ch, unsafe_allow_html=True)
        else:
            st.markdown('<div class="section-title">Full Table</div>', unsafe_allow_html=True)
            tbl_cols=["Ticker","Mesin_Grade","Mesin_Score","Signal","IQ_Verdict","IQ_Score","RVOL","ROC 3B%","Price","TP","SL","Cost%","Skills","Pattern","Zeta","SMC"]
            show=df_out[[c for c in tbl_cols if c in df_out.columns]].head(50)
            st.dataframe(show, use_container_width=True, hide_index=True)

# ════ TAB WATCHLIST ════
with tab_watchlist:
    st.markdown('<div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-bottom:12px;padding:10px 14px;background:#0d1117;border-radius:6px;border-left:3px solid #ff7b00;">Input ticker IDX (tanpa .JK).</div>',unsafe_allow_html=True)
    wc1,wc2,wc3=st.columns([3,1,1])
    with wc1:
        wl_input=st.text_area("Ticker",placeholder="BBCA\nARCI, ASSA",height=120,label_visibility="collapsed",key="wl_input")
    with wc2:
        wl_mode=st.radio("Mode",["Scalping ⚡","Momentum 🚀","Reversal 🎯","Bagger 💎"],key="wl_mode")
    with wc3:
        st.markdown("<br>",unsafe_allow_html=True)
        wl_run=st.button("🔍 Analisa",use_container_width=True,key="wl_run")
        wl_tele=st.button("📡 Telegram",use_container_width=True,key="wl_tele")
    if wl_run and wl_input.strip():
        raw_wl=list(dict.fromkeys([t.strip().upper() for ln in wl_input.split("\n") for t in ln.split(",") if t.strip()]))
        if raw_wl:
            wl_res=[]; _pb=st.progress(0)
            for i,t in enumerate(raw_wl):
                _pb.progress((i+1)/len(raw_wl))
                df=None
                try:
                    if DS_KEY: df=fetch_ds_ohlcv(t,"15m",200,True)
                    if df is None: df=_fetch_yf_ticker(t+".JK","7d","15m")
                except: pass
                if df is None or len(df)<30:
                    wl_res.append({"Ticker":t,"Price":0,"Score":0,"Signal":"No data"}); continue
                try:
                    df2=apply_intraday_indicators(df.copy())
                    r=df2.iloc[-1]; p=df2.iloc[-2]; p2=df2.iloc[-3] if len(df2)>=3 else p
                    close=float(r["Close"]); atr=float(r["ATR"])
                    if wl_mode=="Scalping ⚡": sc,reasons,_=score_scalping(r,p,p2); tp=close+1.5*atr; sl=close-0.8*atr
                    elif wl_mode=="Momentum 🚀": sc,reasons,_=score_momentum(r,p,p2); tp=close+2.0*atr; sl=close-0.8*atr
                    elif wl_mode=="Bagger 💎": sc,reasons,_=score_bagger(r,p,p2,df2); tp=close+3.0*atr; sl=close-1.0*atr
                    else: sc,reasons,_=score_reversal(r,p,p2); tp=close+2.5*atr; sl=close-0.6*atr
                    sig=get_signal(sc,wl_mode)
                    e9=float(r["EMA9"]); e21=float(r["EMA21"]); e50=float(r["EMA50"])
                    trend="▲ UP" if e9>e21>e50 else("▼ DOWN" if e9<e21<e50 else "◆ SIDE")
                    wl_res.append({"Ticker":t,"Price":int(close),"Score":sc,"Signal":sig,"Trend":trend,
                        "RSI-EMA":round(float(r["RSI_EMA"]),1),"RVOL":round(float(r["RVOL"]),2) if not np.isnan(float(r["RVOL"])) else 0,
                        "TP":int(tp),"SL":int(sl),"Reasons":" · ".join(reasons),"_class":get_card_class(sig)})
                except:
                    wl_res.append({"Ticker":t,"Price":0,"Score":0,"Signal":"Err"})
            _pb.empty(); st.session_state.wl_results=wl_res
    if wl_tele and st.session_state.wl_results:
        to_send=[r for r in st.session_state.wl_results if r["Price"]>0]
        if to_send: send_telegram(to_send[:5],source="Watchlist"); st.success("📡 Terkirim!")
    if st.session_state.wl_results:
        ch='<div class="signal-grid">'
        for row in sorted(st.session_state.wl_results,key=lambda x:x["Score"],reverse=True):
            if row["Price"]==0:
                ch+=f'<div class="signal-card"><div class="sc-ticker">{row["Ticker"]}</div><div style="font-size:11px;color:#4a5568;">No data</div></div>'; continue
            si=int(row["Score"]); bars="".join([f'<div class="sc-bar {"filled" if i<si else "empty"}" style="width:26px"></div>' for i in range(6)])
            ch+=f'<div class="signal-card {row["_class"]}"><div style="display:flex;justify-content:space-between;"><div><div class="sc-ticker">{row["Ticker"]}</div><div class="sc-price">{row["Price"]:,}</div></div><div style="text-align:right"><div style="font-family:Space Mono,monospace;font-size:22px;color:#00ff88">{row["Score"]}</div></div></div><div class="sc-bars">{bars}</div><div class="sc-stats"><div class="sc-stat">RSI <span>{row["RSI-EMA"]}</span></div><div class="sc-stat">RVOL <span>{row["RVOL"]}x</span></div></div></div>'
        ch+='</div>'; st.markdown(ch,unsafe_allow_html=True)

# ════ TAB BSJP ════
with tab_bsjp:
    nw=datetime.now(jakarta_tz)
    iet=(nw.hour==14 and nw.minute>=30) or (nw.hour==15 and nw.minute<=45)
    st.markdown(f'<div style="background:rgba(191,95,255,.08);border:1px solid rgba(191,95,255,.3);border-radius:8px;padding:14px 18px;margin-bottom:16px;"><div style="font-family:Space Mono,monospace;font-size:13px;font-weight:700;color:#bf5fff;">🌙 BSJP — Beli Sore Jual Pagi</div><div style="font-family:Space Mono,monospace;font-size:10px;color:#4a5568;margin-top:4px;">{"🟢 WAKTU ENTRY!" if iet else "⏳ Tunggu 14:30 WIB"}</div></div>',unsafe_allow_html=True)
    bc1,bc2=st.columns([2,1])
    with bc1:
        bsjp_min_score=st.slider("Min BSJP Score",0,6,4,key="bsjp_score")
        bsjp_min_rvol=st.slider("Min RVOL",1.0,5.0,1.5,0.1,key="bsjp_rvol")
    with bc2:
        bsjp_min_turn=st.number_input("Min Turnover (M)",value=500,step=100,key="bsjp_turn")*1_000_000
    do_bsjp=st.button("🌙 SCAN BSJP",type="primary",use_container_width=True,key="btn_bsjp")
    if do_bsjp:
        bsjp_res=[]; scan_data=st.session_state.get("data_dict",{})
        if not scan_data: scan_data=fetch_intraday(tuple(stocks_yf[:200]))
        pb=st.progress(0); tbs=list(scan_data.keys())
        for i,ty in enumerate(tbs):
            pb.progress((i+1)/max(len(tbs),1))
            try:
                df=scan_data[ty].copy()
                if len(df)<30: continue
                df2=apply_intraday_indicators(df)
                r=df2.iloc[-1]; p=df2.iloc[-2]; p2=df2.iloc[-3] if len(df2)>=3 else p
                close=float(r["Close"]); vol=float(r["Volume"]); to=close*vol
                rv=float(r["RVOL"]) if not np.isnan(float(r["RVOL"])) else 1.0
                if to<bsjp_min_turn or rv<bsjp_min_rvol: continue
                sc,reasons,_=score_bsjp(r,p,p2)
                if sc<bsjp_min_score: continue
                bs="STRONG BUY 🌙" if sc>=5 else("BUY ⚡" if sc>=4 else "WATCH 👀")
                atr=float(r["ATR"]); tp=close+2.0*atr; sl=close-1.0*atr; rr=(tp-close)/max(close-sl,0.01)
                bsjp_res.append({"Ticker":stock_map.get(ty,ty.replace(".JK","")),"Price":int(close),
                    "Score":sc,"Signal":bs,"RVOL":round(rv,2),"TP":int(tp),"SL":int(sl),
                    "R:R":round(rr,1),"Reasons":" · ".join(reasons),
                    "_class":"gacor" if sc>=5 else "potensial" if sc>=4 else "watch"})
            except: continue
        pb.empty(); st.session_state.bsjp_results=sorted(bsjp_res,key=lambda x:x["Score"],reverse=True)
    br=st.session_state.bsjp_results
    if br:
        bh='<div class="signal-grid">'
        for row in br[:12]:
            si=int(row["Score"]); bars="".join([f'<div class="sc-bar {"filled" if i<si else "empty"}" style="width:26px"></div>' for i in range(6)])
            bh+=f'<div class="signal-card {row["_class"]}"><div class="sc-ticker">{row["Ticker"]}</div><div class="sc-price">{row["Price"]:,}</div><div class="sc-signal" style="color:#bf5fff">{row["Signal"]}</div><div class="sc-bars">{bars}</div><div class="sc-stats"><div class="sc-stat">RVOL <span>{row["RVOL"]}x</span></div><div class="sc-stat">RR <span>{row["R:R"]}</span></div></div></div>'
        bh+='</div>'; st.markdown(bh,unsafe_allow_html=True)

# ════ TAB SECTOR ════
with tab_sector:
    do_sector=st.button("🏭 REFRESH SEKTOR",type="primary",use_container_width=True,key="btn_sector")
    if do_sector:
        sec_data={}
        for sn,ss in SECTORS.items():
            res=fetch_sector_rotation(ss)
            if res:
                sec_data[sn]={"avg_chg":round(sum(r["chg"] for r in res)/len(res),2),
                    "avg_rvol":round(sum(r["rvol"] for r in res)/len(res),2),
                    "bullish":sum(1 for r in res if r["chg"]>0),"total":len(res),
                    "is_hormuz":sn in HORMUZ_SECTORS}
        st.session_state.sector_data=sec_data
    if st.session_state.sector_data:
        ss=sorted(st.session_state.sector_data.items(),key=lambda x:x[1]["avg_chg"],reverse=True)
        cols_sec=st.columns(3)
        for idx,(sn,si) in enumerate(ss):
            chg=si["avg_chg"]; col="#00ff88" if chg>1 else("#ffb700" if chg>0 else "#ff3d5a")
            hb=' ⚡' if si["is_hormuz"] else ""
            bp=int(si["bullish"]/max(si["total"],1)*100)
            with cols_sec[idx%3]:
                st.markdown(f'<div style="background:#0d1117;border:1px solid {col}44;border-radius:8px;padding:12px;margin-bottom:10px;"><div style="font-family:Space Mono,monospace;font-size:10px;color:#c9d1d9;">{sn}{hb}</div><div style="font-family:Space Mono,monospace;font-size:22px;font-weight:700;color:{col};">{chg:+.2f}%</div><div style="font-size:9px;color:#4a5568;">Bullish {si["bullish"]}/{si["total"]}</div></div>',unsafe_allow_html=True)

# ════ TAB GAP UP ════
with tab_gapup:
    gc1,gc2=st.columns(2)
    with gc1: gms=st.slider("Min Gap Score",1,6,3,key="gu_score")
    with gc2: gq=st.toggle("⚡ Quick (200)",value=True,key="gu_quick")
    do_gu=st.button("📈 SCAN GAP UP",type="primary",use_container_width=True,key="btn_gapup")
    if do_gu:
        gt=stocks_yf[:200] if gq else stocks_yf
        gr=scan_gap_up(tuple(gt))
        st.session_state.gapup_results=[r for r in gr if r["Gap Score"]>=gms]
    gr=st.session_state.gapup_results
    if gr:
        gh='<div class="signal-grid">'
        for row in gr[:20]:
            si=int(min(row["Gap Score"],6)); bars="".join([f'<div class="sc-bar {"filled" if i<si else "empty"}" style="width:26px"></div>' for i in range(6)])
            ig="GAP UP" in row.get("Signal",""); sc2="#00ff88" if ig else "#ffb700"
            gh+=f'<div class="signal-card {"gacor" if ig else "potensial"}"><div class="sc-ticker">{row["Ticker"]}</div><div class="sc-price">{row["Price"]:,} ({row["Chg %"]:+.1f}%)</div><div class="sc-signal" style="color:{sc2}">{row["Signal"]}</div><div class="sc-bars">{bars}</div><div class="sc-stats"><div class="sc-stat">RVOL <span>{row["RVOL"]}x</span></div></div></div>'
        gh+='</div>'; st.markdown(gh,unsafe_allow_html=True)

# ════ TAB TRAILING ════
with tab_trail:
    tc1,tc2=st.columns(2)
    with tc1:
        tr_t=st.text_input("Ticker",value="BBCA",key="tr_ticker").upper()
        tr_e=st.number_input("Entry",value=9000,step=10,key="tr_entry")
        tr_q=st.number_input("Lot",value=10,step=1,key="tr_qty")
    with tc2:
        tr_m=st.radio("Metode",["ATR","Persen","Swing Low"],key="tr_method")
        if tr_m=="ATR": tr_am=st.slider("ATR Multiplier",1.0,5.0,2.0,0.5,key="tr_atr_m")
        elif tr_m=="Persen": tr_p=st.slider("Trailing %",1.0,10.0,3.0,0.5,key="tr_pct")
        tr_al=st.toggle("🔔 Alert Telegram",value=True,key="tr_alert")
    if st.button("🎯 HITUNG",type="primary",use_container_width=True,key="btn_trail"):
        try:
            df_tr=None
            if DS_KEY: df_tr=fetch_ds_ohlcv(tr_t,"15m",200)
            if df_tr is None: df_tr=_fetch_yf_ticker(tr_t+".JK","7d","15m")
            if df_tr is not None and len(df_tr)>20:
                df_tr2=apply_intraday_indicators(df_tr.copy())
                cur=float(df_tr2["Close"].iloc[-1]); av=float(df_tr2["ATR"].iloc[-1])
                if tr_m=="ATR": res=calc_trailing_stop(tr_e,cur,av,"ATR",tr_am)
                elif tr_m=="Persen": res=calc_trailing_stop(tr_e,cur,av,"Persen",pct=tr_p)
                else: res=calc_trailing_stop(tr_e,cur,av,"Swing Low")
                sc2="#00ff88" if res["is_profitable"] else "#ff3d5a"
                st.markdown(f'<div style="background:#0d1117;border:1px solid {sc2}44;border-radius:10px;padding:20px;"><div class="metric-row"><div class="metric-card"><div class="metric-label">Now</div><div class="metric-value">{int(cur):,}</div></div><div class="metric-card" style="border-top-color:{sc2}"><div class="metric-label">Stop</div><div class="metric-value" style="color:{sc2}">{int(res["stop"]):,}</div></div><div class="metric-card green"><div class="metric-label">Float</div><div class="metric-value" style="color:#00ff88">{res["profit_float"]:+.1f}%</div></div><div class="metric-card purple"><div class="metric-label">Locked</div><div class="metric-value">{res["profit_locked"]:+.1f}%</div></div></div></div>',unsafe_allow_html=True)
        except Exception as ex: st.error(f"Error: {str(ex)[:80]}")

# ════ TAB BACKTEST ════
with tab_backtest:
    st.markdown('<div class="section-title">Backtest — SL-first · Cost-aware · Non-overlapping</div>',unsafe_allow_html=True)
    bt1,bt2,bt3,bt4,bt5=st.columns(5)
    bt_mode=bt1.selectbox("Mode",["Scalping ⚡","Momentum 🚀","Reversal 🎯","Bagger 💎"],key="bt_mode")
    bt_sc=bt2.slider("Min Score",0,6,4,key="bt_sc")
    bt_fwd=int(bt3.number_input("Hold (bars)",value=4,step=1,min_value=1,max_value=20))
    bt_sl=bt4.number_input("SL mult (ATR)",value=0.8,step=0.1,min_value=0.1,max_value=3.0)
    bt_comm=bt5.number_input("Komisi RT %",value=0.25,step=0.05,min_value=0.0,max_value=1.0,key="bt_comm")
    if st.button("🚀 Run Backtest",type="primary",key="bt_run"):
        dd=st.session_state.get("data_dict",{})
        if not dd: st.warning("Scanner dulu bro!")
        else:
            bt_r=[]; bt_holds=[]; n_ambig=[0]; bpb=st.progress(0); sample=list(dd.keys())[:80]
            for bi,ty in enumerate(sample):
                bpb.progress((bi+1)/len(sample))
                try:
                    d=dd[ty].copy()
                    if len(d)<60: continue
                    d=apply_intraday_indicators(d)
                    ii=50
                    while ii<len(d)-bt_fwd:
                        r0=d.iloc[ii]; r1=d.iloc[ii-1]; r2=d.iloc[ii-2]
                        if bt_mode=="Scalping ⚡": sc,_,_=score_scalping(r0,r1,r2)
                        elif bt_mode=="Momentum 🚀": sc,_,_=score_momentum(r0,r1,r2)
                        elif bt_mode=="Bagger 💎": sc,_,_=score_bagger(r0,r1,r2,d.iloc[:ii+1])
                        else: sc,_,_=score_reversal(r0,r1,r2)
                        if sc<bt_sc: ii+=1; continue
                        en=float(r0["Close"]); av=float(r0["ATR"]) if not np.isnan(float(r0["ATR"])) else en*0.005
                        if bt_mode=="Bagger 💎": tp2=en+3.0*av; sl2=en-1.0*av
                        else: tp2=en+2.0*av; sl2=en-bt_sl*av
                        ex=float(d.iloc[ii+bt_fwd]["Close"]); exit_bar=bt_fwd
                        for fi in range(1,bt_fwd+1):
                            bar=d.iloc[ii+fi]
                            hit_sl=float(bar["Low"])<=sl2; hit_tp=float(bar["High"])>=tp2
                            if hit_sl and hit_tp: n_ambig[0]+=1
                            if hit_sl: ex=sl2; exit_bar=fi; break
                            if hit_tp: ex=tp2; exit_bar=fi; break
                        gross=(ex-en)/en*100; net=gross-trade_cost_pct(en,bt_comm)
                        bt_r.append(net); bt_holds.append(exit_bar); ii+=exit_bar+1
                except: continue
            bpb.empty()
            if not bt_r: st.warning("Tidak ada trades.")
            else:
                arr=np.array(bt_r); wr=len(arr[arr>0])/len(arr)*100; avg=np.mean(arr)
                med=np.median(arr); pf=arr[arr>0].sum()/max(abs(arr[arr<0].sum()),0.01)
                mxdd=arr[arr<0].min() if len(arr[arr<0])>0 else 0
                st.markdown(f'<div class="bt-result"><div style="font-family:Space Mono;font-size:10px;color:#4a5568;">{len(arr)} TRADES · NET · Score≥{bt_sc} · Max {bt_fwd} bars · Ambig→SL:{n_ambig[0]}</div><div class="metric-row" style="margin-top:10px;"><div class="metric-card green"><div class="metric-label">Win Rate</div><div class="metric-value">{wr:.1f}%</div></div><div class="metric-card"><div class="metric-label">Avg Net</div><div class="metric-value" style="color:{"#00ff88" if avg>0 else "#ff3d5a"}">{avg:+.2f}%</div></div><div class="metric-card"><div class="metric-label">PF</div><div class="metric-value">{pf:.2f}x</div></div><div class="metric-card red"><div class="metric-label">Max Loss</div><div class="metric-value">{mxdd:.1f}%</div></div></div></div>',unsafe_allow_html=True)

# ════ TAB SIZING ════
with tab_sizing:
    st.markdown('<div class="section-title">Position Sizing — 3 Cap: Risk · Kelly · √Impact</div>',unsafe_allow_html=True)
    sz1,sz2,sz3,sz4=st.columns(4)
    sz_ticker=sz1.text_input("Ticker",value="BBCA",key="sz_ticker").upper().strip()
    sz_modal=sz2.number_input("Modal (Jt Rp)",value=100,step=10,min_value=1,key="sz_modal")
    sz_risk=sz3.number_input("Risk %",value=1.0,step=0.25,min_value=0.1,key="sz_risk")
    sz_slm=sz4.number_input("SL × σ",value=1.5,step=0.25,min_value=0.5,key="sz_slm")
    if st.button("🧮 HITUNG",type="primary",use_container_width=True,key="btn_sizing"):
        df_sz=None
        try:
            if ARJUM_KEY: df_sz=fetch_arjum_daily_ohlcv(sz_ticker,limit=200)
            if df_sz is None: df_sz=_fetch_yf_ticker(sz_ticker+".JK","6mo","1d")
        except: pass
        if df_sz is None or len(df_sz)<30: st.error("Data kurang")
        else:
            close_sz=float(df_sz["Close"].iloc[-1]); sig_d=ewma_sigma_daily(df_sz) or 1.0
            ts_=tail_stats_daily(df_sz); adv=adv20_rp(df_sz)
            modal_rp=float(sz_modal)*1e6; risk_rp=modal_rp*float(sz_risk)/100
            stop_pct=float(sz_slm)*sig_d; size_risk=risk_rp/max(stop_pct/100,1e-6)
            size_impact=0.02*adv if adv>0 else size_risk
            caps=[("RISK",size_risk),("IMPACT",size_impact)]
            binding=min(caps,key=lambda x:x[1]); rec_rp=max(binding[1],0)
            lots=int(rec_rp/max(close_sz*100,1)); rec_real=lots*close_sz*100
            imp_rec,part_rec=sqrt_impact(rec_real,adv,sig_d)
            tail_warn=(ts_ is not None and stop_pct<ts_["p99"])
            st.markdown(f'<div class="metric-row"><div class="metric-card"><div class="metric-label">Now</div><div class="metric-value">{close_sz:,.0f}</div><div class="metric-sub">σd {sig_d:.2f}%</div></div><div class="metric-card green"><div class="metric-label">Lot</div><div class="metric-value">{lots}</div><div class="metric-sub">{binding[0]}</div></div><div class="metric-card amber"><div class="metric-label">Stop</div><div class="metric-value">{stop_pct:.2f}%</div></div><div class="metric-card purple"><div class="metric-label">Impact</div><div class="metric-value">{(imp_rec or 0):.3f}%</div><div class="metric-sub">{(part_rec or 0):.2f}% ADV</div></div></div>',unsafe_allow_html=True)
            if tail_warn: st.warning(f"⚠️ Stop {stop_pct:.2f}% < p99 {ts_['p99']:.2f}%")

# ════ TAB PLAN ════
with tab_plan:
    st.markdown('<div class="section-title">📋 Trading Plan — Entry · SL · TP1/2/3 · Size · Horizon</div>', unsafe_allow_html=True)
    _res_plan=st.session_state.scan_results
    if not _res_plan:
        st.info("Jalankan Scanner dulu bro.")
    else:
        pcol1,pcol2=st.columns([1,2])
        with pcol1:
            plan_modal=st.number_input("Modal per Posisi (Jt Rp)",value=5,step=1,min_value=1,key="plan_modal")
            plan_max=st.slider("Max plan",3,20,8,key="plan_max")
            plan_min_ms=st.slider("Min MS",0,100,40,key="plan_min_ms")
        _dfp=pd.DataFrame(_res_plan)
        _dfp=_dfp[_dfp["Mesin_Score"]>=plan_min_ms].sort_values("Mesin_Score",ascending=False).head(plan_max)
        if _dfp.empty:
            st.info(f"Tidak ada signal MS ≥ {plan_min_ms}.")
        else:
            for _, _r in _dfp.iterrows():
                _plan=_r.get("Plan")
                if _plan is None or (isinstance(_plan,dict) and _plan.get("direction")!="LONG"):
                    _atr_est=(float(_r["TP"])-float(_r["Price"]))/1.5 if _r["TP"]>_r["Price"] else float(_r["Price"])*0.02
                    _plan=generate_trading_plan(_r["Ticker"],float(_r["Price"]),_atr_est,direction="LONG",tp_mult=1.5,sl_mult=0.8,modal_rp=float(plan_modal)*1e6,risk_pct=1.0)
                if _plan is None: continue
                _mg=str(_r.get("Mesin_Grade","—")); _mgc=str(_r.get("Mesin_Color","#4a5568"))
                _skills=str(_r.get("Skills","—"))[:100]
                st.markdown(f"""<div style="background:#0d1117;border:1px solid {_mgc}44;border-left:4px solid {_mgc};border-radius:10px;padding:16px;margin-bottom:12px;">
                  <div style="display:flex;justify-content:space-between;flex-wrap:wrap;gap:12px;">
                    <div><div style="font-family:Space Mono;font-size:20px;font-weight:700;color:#e6edf3;">{_r["Ticker"]} <span style="font-size:10px;color:{_mgc}">[{_mg}]</span></div>
                    <div style="font-family:Space Mono;font-size:10px;color:#4a5568;">{_r.get("Signal","—")} · MS {_r.get("Mesin_Score",0):.0f}</div></div>
                    <div style="text-align:right;"><div style="font-size:9px;color:#4a5568;">HORIZON</div><div style="font-family:Space Mono;font-size:14px;font-weight:700;color:#00e5ff;">{_plan["horizon"]}</div></div>
                  </div>
                  <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(90px,1fr));gap:8px;margin-top:12px;">
                    <div style="background:#0a1a10;border:1px solid #00ff8844;border-radius:6px;padding:8px;text-align:center;"><div style="font-size:9px;color:#00ff88;">ENTRY</div><div style="font-family:Space Mono;font-size:15px;font-weight:700;color:#00ff88;">{_plan["entry"]:,}</div></div>
                    <div style="background:#1a0a0a;border:1px solid #ff3d5a44;border-radius:6px;padding:8px;text-align:center;"><div style="font-size:9px;color:#ff3d5a;">SL</div><div style="font-family:Space Mono;font-size:15px;font-weight:700;color:#ff3d5a;">{_plan["sl"]:,}</div><div style="font-size:9px;color:#ff3d5a;">-{_plan["risk_pct"]}%</div></div>
                    <div style="background:#0a1525;border:1px solid #4da6ff44;border-radius:6px;padding:8px;text-align:center;"><div style="font-size:9px;color:#4da6ff;">TP1 (33%)</div><div style="font-family:Space Mono;font-size:15px;font-weight:700;color:#4da6ff;">{_plan["tp1"]:,}</div><div style="font-size:9px;color:#4da6ff;">RR {_plan["rr1"]} · {_plan["lots_tp1"]} lot</div></div>
                    <div style="background:#0a1525;border:1px solid #4da6ff44;border-radius:6px;padding:8px;text-align:center;"><div style="font-size:9px;color:#4da6ff;">TP2 (33%)</div><div style="font-family:Space Mono;font-size:15px;font-weight:700;color:#4da6ff;">{_plan["tp2"]:,}</div><div style="font-size:9px;color:#4da6ff;">RR {_plan["rr2"]} · {_plan["lots_tp2"]} lot</div></div>
                    <div style="background:#150a25;border:1px solid #bf5fff44;border-radius:6px;padding:8px;text-align:center;"><div style="font-size:9px;color:#bf5fff;">TP3 (33%)</div><div style="font-family:Space Mono;font-size:15px;font-weight:700;color:#bf5fff;">{_plan["tp3"]:,}</div><div style="font-size:9px;color:#bf5fff;">RR {_plan["rr3"]} · {_plan["lots_tp3"]} lot</div></div>
                  </div>
                  <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:8px;margin-top:10px;font-family:Space Mono;font-size:10px;">
                    <div style="color:#c9d1d9;">📦 Size: <b style="color:#ffb700">{_plan["lots"]} lot</b> ({_plan["modal_pct"]}%)</div>
                    <div style="color:#c9d1d9;">💸 Risk: <b style="color:#ff3d5a">Rp {_plan["risk_rp"]:,}</b></div>
                    <div style="color:#c9d1d9;">🎯 Invalidasi: <b style="color:#ff3d5a">{_plan["invalidation"]}</b></div>
                  </div>
                  <div style="margin-top:8px;padding-top:8px;border-top:1px dashed #1c2533;font-family:Space Mono;font-size:9px;color:#4a5568;">SKILLS: {_skills}</div>
                </div>""", unsafe_allow_html=True)

# ════════════ AUTO-TRACKING ════════════
_LAST_UPD_FILE=CACHE_DIR/"last_outcome_autoupdate.pkl"
AUTO_UPDATE_INTERVAL_SEC=6*3600

def _get_last_autoupdate():
    try:
        if _LAST_UPD_FILE.exists(): return pickle.loads(_LAST_UPD_FILE.read_bytes())["ts"]
    except: pass
    return 0

def _set_last_autoupdate(ts):
    try: _LAST_UPD_FILE.write_bytes(pickle.dumps({"ts":ts}))
    except: pass

def maybe_auto_update_outcomes():
    now_ts=time.time()
    if now_ts-_get_last_autoupdate()>=AUTO_UPDATE_INTERVAL_SEC:
        try:
            n_upd,msg=update_signal_outcomes(max_tickers=60)
            _set_last_autoupdate(now_ts)
            return n_upd,msg
        except Exception as e:
            _set_last_autoupdate(now_ts)
            return 0,f"Err: {e}"
    return None,None

def ticker_expectancy_table(done, min_n=2):
    g=done.groupby("ticker").agg(N=("t3_ret","count"),
        WinPct_T3=("t3_ret",lambda s:round(float((s>0).mean())*100,1)),
        Avg_T1=("t1_ret","mean"),Avg_T3=("t3_ret","mean"),Avg_T5=("t5_ret","mean"),
        LastGrade=("mesin_grade",lambda s:s.iloc[-1]),LastDate=("date","max")).reset_index()
    g=g[g["N"]>=min_n]
    for c in ["Avg_T1","Avg_T3","Avg_T5"]: g[c]=g[c].astype(float).round(2)
    return g.sort_values("Avg_T3",ascending=False)

def period_rollup(done, freq="W"):
    d=done.copy(); d["_dt"]=pd.to_datetime(d["date"]); d["period"]=d["_dt"].dt.to_period(freq).astype(str)
    g=d.groupby("period").agg(N=("t3_ret","count"),
        WinPct=("t3_ret",lambda s:round(float((s>0).mean())*100,1)),
        AvgT3=("t3_ret","mean")).reset_index()
    g["AvgT3"]=g["AvgT3"].astype(float).round(2)
    return g

# ════ TAB JOURNAL ════
with tab_journal:
    st.markdown('<div class="section-title">📓 Signal Journal — Expectancy dari DATA</div>',unsafe_allow_html=True)
    _n_auto,_msg_auto=maybe_auto_update_outcomes()
    if _n_auto: st.toast(f"📓 {_msg_auto}",icon="📓")
    jdf=load_signal_log()
    jc1,jc2,jc3=st.columns(3)
    with jc3:
        if st.button("🔄 Update Outcomes",use_container_width=True,key="btn_upd_out",type="primary"):
            with st.spinner("Fetching..."):
                n_upd,msg=update_signal_outcomes(); _set_last_autoupdate(time.time())
            if n_upd>0: st.success(msg)
            else: st.info(msg)
            jdf=load_signal_log()
    with jc1: st.metric("Total Sinyal",len(jdf))
    with jc2: st.metric("Punya T+3",int(jdf["t3_ret"].notna().sum()) if not jdf.empty else 0)
    st.caption(f"Backend: {siglog_backend()}")
    if not jdf.empty:
        done=jdf.dropna(subset=["t3_ret"]).copy()
        if not done.empty:
            g=done.groupby("mesin_grade").agg(N=("t3_ret","count"),
                WinPct_T3=("t3_ret",lambda s:round(float((s>0).mean())*100,1)),
                Avg_T1=("t1_ret","mean"),Avg_T3=("t3_ret","mean"),Avg_T5=("t5_ret","mean")).reset_index()
            for c in ["Avg_T1","Avg_T3","Avg_T5"]: g[c]=g[c].astype(float).round(2)
            st.markdown('<div class="section-title">Expectancy per Mesin Grade</div>',unsafe_allow_html=True)
            st.dataframe(g.sort_values("Avg_T3",ascending=False),use_container_width=True,hide_index=True)
            st.markdown('<div class="section-title">🎯 Per-Ticker Track Record</div>',unsafe_allow_html=True)
            tt=ticker_expectancy_table(done,min_n=2)
            if not tt.empty: st.dataframe(tt,use_container_width=True,hide_index=True)
            st.markdown('<div class="section-title">📅 Trend Mingguan/Bulanan</div>',unsafe_allow_html=True)
            rl=period_rollup(done,"W")
            st.dataframe(rl,use_container_width=True,hide_index=True)
        else:
            st.info("Belum ada outcome T+3. Tunggu 1 hari bursa, klik Update Outcomes.")
        with st.expander("📋 Raw Signal Log (50 terakhir)"):
            st.dataframe(jdf.tail(50),use_container_width=True,hide_index=True)
        try:
            st.download_button("⬇️ Download CSV",jdf.to_csv(index=False).encode(),"signal_log.csv","text/csv",key="dl_siglog")
        except: pass
    else:
        st.info("Jalankan Scanner dulu → sinyal otomatis ke-log di sini.")

# ════════════ FOOTER + HEARTBEAT ════════════
_nf=now_jkt.timestamp()
_FT_AUTO_ON=bool(st.session_state.get("auto_scan_on",True))
_FT_AUTO_SEC=int(st.session_state.get("auto_scan_sec",1800))
if st.session_state.last_scan_time:
    _r2=max(0,_FT_AUTO_SEC-(_nf-st.session_state.last_scan_time)); m2=int(_r2//60); s2=int(_r2%60)
    _lt2=datetime.fromtimestamp(st.session_state.last_scan_time,jakarta_tz).strftime("%H:%M:%S")
    ti=f"⏱️ Auto-scan next: <span style='color:#ff7b00'>{m2:02d}:{s2:02d}</span> · Last: <span style='color:#2dd4bf'>{_lt2} WIB</span>"
else:
    ti="⏱️ Auto-scan aktif" if _FT_AUTO_ON else "⏱️ Klik Scan untuk mulai"

st.markdown(f"""<div style="margin-top:28px;padding-top:14px;border-top:1px solid #1c2533;display:flex;justify-content:space-between;flex-wrap:wrap;gap:8px;">
  <div style="font-family:Space Mono;font-size:10px;color:#4a5568;">🔥 Mesin Presisi v2.2 · Skills · Cost-Aware · SL-First · Non-Overlap · Journal</div>
  <div style="font-family:Space Mono;font-size:10px;color:#4a5568;">{ti}</div>
</div>""",unsafe_allow_html=True)

if _FT_AUTO_ON:
    try:
        @st.fragment(run_every="30s")
        def _auto_heartbeat():
            _lst=st.session_state.get("last_scan_time"); _sec=int(st.session_state.get("auto_scan_sec",1800))
            if _lst is None or (time.time()-_lst)>=_sec: st.rerun(scope="app")
            n_upd,msg=maybe_auto_update_outcomes()
            if n_upd: st.toast(f"📓 {msg}",icon="📓")
        _auto_heartbeat()
    except Exception:
        import streamlit.components.v1 as _comp
        _comp.html(f"<script>setTimeout(function(){{window.parent.location.reload();}},{_FT_AUTO_SEC*1000});</script>",height=0)
