# ═══════════════════════════════════════════════════════════════
#  PATCH — Auto Tracking + Per-Ticker Clarity
#  Cara pasang: 3 blok di bawah, masing-masing ada instruksi lokasi.
#  Tidak mengubah fungsi yang sudah ada — cuma nambahin.
# ═══════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────
# BLOK 1 — taruh SETELAH definisi update_signal_outcomes()
# (fungsi yang sudah ada di script lu, sebelum bagian DATASECTORS FETCH)
# ─────────────────────────────────────────────────────────────
_LAST_UPD_FILE = CACHE_DIR / "last_outcome_autoupdate.pkl"
AUTO_UPDATE_INTERVAL_SEC = 6 * 3600  # cek tiap 6 jam — cukup buat isi T+1..T+5 harian

def _get_last_autoupdate():
    try:
        if _LAST_UPD_FILE.exists():
            return pickle.loads(_LAST_UPD_FILE.read_bytes())["ts"]
    except: pass
    return 0

def _set_last_autoupdate(ts):
    try: _LAST_UPD_FILE.write_bytes(pickle.dumps({"ts": ts}))
    except: pass

def maybe_auto_update_outcomes():
    """Dipanggil tiap render/heartbeat. Cuma benar-benar jalan kalau interval
    sudah lewat, jadi aman dipanggil sesering apapun (rate-limited)."""
    now_ts = time.time()
    if now_ts - _get_last_autoupdate() >= AUTO_UPDATE_INTERVAL_SEC:
        try:
            n_upd, msg = update_signal_outcomes(max_tickers=60)
            _set_last_autoupdate(now_ts)
            return n_upd, msg
        except Exception as e:
            _set_last_autoupdate(now_ts)  # cegah retry loop tiap render kalau error
            return 0, f"Auto-update error: {e}"
    return None, None

def ticker_expectancy_table(done, min_n=2):
    """Per-ticker: saham SPESIFIK mana yang beneran konsisten naik/turun
    tiap kali disinyal — bukan cuma agregat grade."""
    g = done.groupby("ticker").agg(
        N=("t3_ret", "count"),
        WinPct_T3=("t3_ret", lambda s: round(float((s > 0).mean()) * 100, 1)),
        Avg_T1=("t1_ret", "mean"),
        Avg_T3=("t3_ret", "mean"),
        Avg_T5=("t5_ret", "mean"),
        LastGrade=("mesin_grade", lambda s: s.iloc[-1]),
        LastDate=("date", "max"),
    ).reset_index()
    g = g[g["N"] >= min_n]
    for c in ["Avg_T1", "Avg_T3", "Avg_T5"]:
        g[c] = g[c].astype(float).round(2)
    return g.sort_values("Avg_T3", ascending=False)

def period_rollup(done, freq="W"):
    """Trend mingguan/bulanan — apakah edge sistem stabil seiring N bertambah,
    atau cuma noise satu periode doang."""
    d = done.copy()
    d["_dt"] = pd.to_datetime(d["date"])
    d["period"] = d["_dt"].dt.to_period(freq).astype(str)
    g = d.groupby("period").agg(
        N=("t3_ret", "count"),
        WinPct=("t3_ret", lambda s: round(float((s > 0).mean()) * 100, 1)),
        AvgT3=("t3_ret", "mean"),
    ).reset_index()
    g["AvgT3"] = g["AvgT3"].astype(float).round(2)
    return g


# ─────────────────────────────────────────────────────────────
# BLOK 2 — GANTI fungsi _auto_heartbeat() yang sudah ada di paling bawah
# script (di dalam `if _FT_AUTO_ON:` block) dengan versi ini.
# Ini bikin outcome T+1/3/5 ke-update sendiri tanpa perlu klik manual,
# selama app-nya kebuka / ke-hit heartbeat.
#
# CATATAN PENTING: kalau app Streamlit Cloud lu idle/sleep karena
# nggak ada yang buka, heartbeat ini juga ikut mati (Streamlit Cloud
# suspend app yang nggak diakses). Kalau mau update jalan walau app
# lagi nggak dibuka siapa-siapa, opsi paling gampang: pasang cron
# eksternal (GitHub Actions / cron-job.org) yang hit URL app lu
# tiap pagi buat "wake up" app-nya — itu udah cukup trigger heartbeat.
# ─────────────────────────────────────────────────────────────
if _FT_AUTO_ON:
    try:
        @st.fragment(run_every="30s")
        def _auto_heartbeat():
            _lst = st.session_state.get("last_scan_time")
            _sec = int(st.session_state.get("auto_scan_sec", 900))
            if _lst is None or (time.time() - _lst) >= _sec:
                st.rerun(scope="app")
            # ── Auto-track outcomes ──
            n_upd, msg = maybe_auto_update_outcomes()
            if n_upd:
                st.toast(f"📓 Auto-update outcome: {msg}", icon="📓")
        _auto_heartbeat()
    except Exception:
        import streamlit.components.v1 as _components
        _components.html(
            f"<script>setTimeout(function(){{window.parent.location.reload();}}, {_FT_AUTO_SEC*1000});</script>",
            height=0)


# ─────────────────────────────────────────────────────────────
# BLOK 3 — taruh di dalam `with tab_journal:`, PERSIS SETELAH baris:
#   st.caption("🎲 Kelly per grade butuh ≥10 outcome per grade — biarkan mesin nabung dulu.")
# (yaitu setelah blok tabel Kelly Sizing per Grade, sebelum "Expectancy per Mode Scan")
# ─────────────────────────────────────────────────────────────
"""
            st.markdown('<div class="section-title">🎯 Per-Ticker Track Record — saham mana yang BENERAN konsisten naik/turun</div>',unsafe_allow_html=True)
            tick_tbl = ticker_expectancy_table(done, min_n=2)
            if not tick_tbl.empty:
                st.dataframe(tick_tbl, use_container_width=True, hide_index=True)
                winners = tick_tbl[(tick_tbl["N"]>=3)&(tick_tbl["WinPct_T3"]>=60)]
                losers  = tick_tbl[(tick_tbl["N"]>=3)&(tick_tbl["WinPct_T3"]<=35)]
                if not winners.empty:
                    st.success(f"✅ Konsisten naik (N≥3, Win≥60%): {', '.join(winners['ticker'].tolist())}")
                if not losers.empty:
                    st.error(f"❌ Konsisten jelek (N≥3, Win≤35%): {', '.join(losers['ticker'].tolist())}")
                st.caption("N<3 belum cukup buat nyimpulin pola per-ticker — masih bisa kebetulan.")
            else:
                st.caption("Belum ada ticker yang muncul ≥2x dengan outcome T+3 — biarkan mesin nabung data dulu.")

            st.markdown('<div class="section-title">📅 Trend Mingguan / Bulanan — apakah edge-nya stabil seiring waktu?</div>',unsafe_allow_html=True)
            roll_freq = st.radio("Periode", ["Mingguan","Bulanan"], horizontal=True, key="roll_freq")
            freq_code = "W" if roll_freq=="Mingguan" else "ME"
            roll_tbl = period_rollup(done, freq_code)
            st.dataframe(roll_tbl, use_container_width=True, hide_index=True)
            st.caption("Kalau AvgT3 flip-flop antar periode → edge belum stabil, jangan size besar dulu. Kalau konsisten positif across periode → mulai bisa dipercaya.")
"""
