/* ============================================================================
   ENGINE — MESIN SINYAL SERVER-SIDE (satu sumber kebenaran untuk semua device)

   Masalah yang diselesaikan: sebelumnya sinyal dihitung di dalam browser masing-masing,
   sehingga device berbeda bisa menghasilkan sinyal berbeda (data pasar, offset jam, dan
   terutama: sinyal hanya "terkunci" bila device terbuka saat detik ke-2 sesi).

   Sekarang server yang menghitung:
     - data pasar bergulir per aset (1s + 5m/15m) lewat polling REST Binance,
     - sinyal sesi KANONIK memakai computeSignal() dari capture.js (RUMUS SAMA dengan yang
       direkam ke ledger & dipelajari learner -> tidak ada duplikasi logika),
     - dikunci sekali per sesi (t0), jadi semua device melihat sinyal yang SAMA.

   Snapshot disajikan lewat SSE /api/live?tf=5m (lihat server.js) tiap ~1 detik.
   Polling hanya berjalan bila ada subscriber (hemat kuota Binance).
   ============================================================================ */
const { computeSignal, DUR_S } = require("./capture.js");
const GATES_DEF = require("./gates.js");
const FLOW = require("./flow.js");        // OFI live (order flow per menit)
const TRADE = require("./trade-plan.js");  // TRADE ASSISTANT: modul bersama (server + browser)
const CONF = require("./confidence.js");  // MODEL CONFIDENCE: modul bersama (server + browser)
const MP = require("./mobile-pred.js");   // MOBILE PREDICTION: modul bersama (server + browser)
const fs = require("fs");
const path = require("path");
// Tabel backtest (kontinuasi/ladder exit). Tidak wajib: bila tidak ada, teks plan kehilangan
// bagian "sisa potensi" saja, keputusan entry/close tetap berjalan.
let TIERS = null;
try {
  // Sama seperti loadTiers() di klien: base early_tiers.json + tabel tambahan yang di-merge.
  TIERS = JSON.parse(fs.readFileSync(path.join(__dirname, "backtest/out/early_tiers.json"), "utf8"));
  const EXTRA = { continuation: "continuation.json", locktouch: "locktouch.json", byMinute: "tier_by_minute.json" };
  for (const k of Object.keys(EXTRA)) {
    try {
      const j = JSON.parse(fs.readFileSync(path.join(__dirname, "backtest/out", EXTRA[k]), "utf8"));
      TIERS[k] = (k === "byMinute") ? (j.tiers || null) : j;
    } catch (_) {}
  }
} catch (_) { TIERS = null; }
const SignalCore = require("./signal-core.js");

// Semua interval yang dipakai UI diproses server (termasuk 1h) supaya klien tidak perlu
// menghitung sinyal sendiri di interval mana pun.
const TFS = ["5m", "15m", "1h"];

function createEngine(deps) {
  const { getKlines, getModel, getGates, log = console.log } = deps;
  const tfsOf = (sym) => (sym === "BNB" ? ["5m"] : TFS);   // BNB hanya 5m
  const market = { BTC: { ones: [], tf: {}, five5m: [], five5s: [], onesHist: [], lastOne: 0 },
                   ETH: { ones: [], tf: {}, five5m: [], five5s: [], onesHist: [], lastOne: 0 },
                   BNB: { ones: [], tf: {}, five5m: [], five5s: [], onesHist: [], lastOne: 0 } };
  // State Trade Assistant per (aset, tf): peak/dwell/entered/closed. Direset tiap sesi baru.
  const planState = { BTC: {}, ETH: {}, BNB: {} };
  // ===== PERSISTENSI STATE TRADE ASSISTANT =====
  // State (posisi terbuka + early close + snapshot sesi) sebelumnya hanya di MEMORI, sehingga
  // setiap restart container (mis. saat deploy) menghapusnya -> chip ENTRY/EARLY CLOSE yang tadinya
  // SUCCESS bisa "hilang", dan sesi itu tercatat tanpa entry. Sekarang state disimpan ke file di
  // volume (deps.stateFile) dan dimuat kembali saat boot supaya tidak hilang oleh restart.
  const STATE_FILE = deps.stateFile || null;
  let stateDirty = false, stateSavedAt = 0, stateLoaded = false;
  function loadState() {
    if (!STATE_FILE) return;
    try {
      const j = JSON.parse(require("fs").readFileSync(STATE_FILE, "utf8"));
      if (j && j.finishedTrades) Object.assign(finishedTrades, j.finishedTrades);
      if (j && j.guideLock) { for (const sy of Object.keys(guideLock)) { const src = j.guideLock[sy]; if (src) for (const t of Object.keys(src)) guideLock[sy][t] = src[t]; } }
      if (j && j.planState) {
        for (const sym of Object.keys(planState)) {
          const src = j.planState[sym];
          if (!src) continue;
          for (const tf of Object.keys(src)) {
            const e = src[tf];
            if (e && e.key && e.st) planState[sym][tf] = { key: e.key, st: e.st };
          }
        }
      }
      log(`[ENGINE] state trade dipulihkan dari ${STATE_FILE} (finishedTrades=${Object.keys(finishedTrades).length})`);
    } catch (_) { /* belum ada / rusak -> mulai bersih */ }
    stateLoaded = true;
  }
  function saveState(force) {
    if (!STATE_FILE || (!stateDirty && !force)) return;
    if (!force && Date.now() - stateSavedAt < 3000) return;      // throttle 3 detik
    try {
      require("fs").writeFileSync(STATE_FILE, JSON.stringify({ at: Date.now(), finishedTrades, planState, guideLock }));
      stateDirty = false; stateSavedAt = Date.now();
    } catch (_) { /* gagal tulis: coba lagi nanti */ }
  }
  // Hasil trade sesi yang SUDAH berakhir (untuk dicatat ke ledger saat hasil sesi dinilai).
  // Menyimpan: apakah posisi dibuka (entry) dan apakah early close ter-signal.
  const finishedTrades = {};
  // ===== GARIS BANTU SESI (DIBEKUKAN) =====
  // S/R & TARGET dihitung SEKALI di awal sesi lalu dipakai sampai sesi berakhir, supaya garis di
  // chart TIDAK berpindah-pindah tiap tick (permintaan user: garis yang sudah dibuat di awal sesi
  // harus lock hingga sesi selesai). Ladder ENTRY/TAMBAH memang sudah tetap karena hanya
  // bergantung pada harga LOCK.
  const guideLock = { BTC: {}, ETH: {}, BNB: {} };
  function guidesFor(sym, tf, key, C, win, tierTarget) {
    const g = guideLock[sym][tf];
    if (g && g.key === key) {
      // Sudah dibekukan. Hanya SATU penyempurnaan yang diizinkan: mengisi TARGET bila saat
      // pembekuan nilainya belum ada (mis. plan belum terkunci). S/R TIDAK pernah diubah.
      if (g.target == null && tierTarget != null) { g.target = tierTarget; stateDirty = true; }
      return g;
    }
    if (!win || win.length < 8 || C == null) return null;     // window belum cukup -> coba lagi nanti
    let support = null, resistance = null;
    try {
      const sw = CONF.detectSwings(win, Math.max(3, Math.round(win.length / 6)));
      const lows = (sw.lows || []).map((x) => x.price).filter((p) => p < C * 0.9999);
      const highs = (sw.highs || []).map((x) => x.price).filter((p) => p > C * 1.0001);
      if (lows.length) support = Math.max.apply(null, lows);
      if (highs.length) resistance = Math.min.apply(null, highs);
    } catch (_) {}
    const rec = { key, support, resistance, target: (tierTarget != null ? tierTarget : null), at: Date.now() };
    guideLock[sym][tf] = rec;
    stateDirty = true;
    return rec;
  }
  // Dimuat SETELAH semua struktur state ada (kalau dipanggil lebih awal -> TDZ -> restore gagal senyap)
  loadState();
  // PENTING (permintaan user): yang dicatat adalah ENTRY & EARLY CLOSE yang PERTAMA.
  // State ini hanya diisi sekali per sesi ("first write wins") — entry berikutnya atau
  // close berikutnya pada sesi yang sama TIDAK menimpa, lihat trade-plan.js buildPlan().
  function tradeStateOf(st, key) {
    const e = (st && st.entered) ? st.entered[key] : null;
    const c = (st && st.closed) ? st.closed[key] : null;
    return {
      entered: !!e, entryPrice: e ? e.price : null, entryAt: e ? e.since : null,
      entryRNow: e ? e.rNow : null, entryRetrace: e ? e.retrace : null,
      entryExtremeDepth: e ? e.extremeDepth : null, entryRemainSec: e ? e.remainSec : null,
      closed: !!c, closePrice: c ? c.price : null, closeAt: c ? c.at : null, closeReason: c ? c.reason : null,
      taVer: (require("./ta-config.js").VER),
    };
  }
  function snapshotTrade(sym, tf, key, st) {
    if (!key || !st) return;
    stateDirty = true;
    finishedTrades[key] = tradeStateOf(st, key);
    const keys = Object.keys(finishedTrades);
    if (keys.length > 1000) delete finishedTrades[keys[0]];      // batasi memori
  }
  function planStateFor(sym, tf, key) {
    let s = planState[sym][tf];
    if (!s || s.key !== key) {
      if (s && s.st && s.key) snapshotTrade(sym, tf, s.key, s.st);   // sesi lama berakhir -> simpan
      s = planState[sym][tf] = { key, st: { peak: {}, dwell: {}, entered: {}, closed: {} } };
      stateDirty = true;
    }
    return s.st;
  }
  // Buffer inkremental dari polling 1s: dipakai untuk window 5s (turn/structure) dan
  // deret close sesi (trail SMA). Dedupe berdasarkan waktu supaya boleh dipanggil tiap tick.
  function pushSeries(sym, ones) {
    const m = market[sym];
    let last = m.lastOne || 0;
    for (const c of ones || []) {
      if (!c || !isFinite(c.time) || c.time <= last) continue;
      m.onesHist.push({ t: c.time, c: c.close });
      if (m.onesHist.length > 3700) m.onesHist.shift();          // > 1 jam (sesi terpanjang)
      const t5 = Math.floor(c.time / 5) * 5;
      let cur = m.five5s[m.five5s.length - 1];
      if (!cur || cur.time !== t5) m.five5s.push({ time: t5, open: c.open, high: c.high, low: c.low, close: c.close, vol: c.vol || 0 });
      else { cur.high = Math.max(cur.high, c.high); cur.low = Math.min(cur.low, c.low); cur.close = c.close; cur.vol += (c.vol || 0); }
      if (m.five5s.length > 400) m.five5s.shift();
      last = c.time;
    }
    m.lastOne = last;
  }
  const session = { BTC: {}, ETH: {}, BNB: {} };          // sym -> tf -> { t0, signal|null, skipped, at }
  const stats = { refreshes: 0, errors: 0, locked: 0, lastAt: null, lastErr: null, subscribers: 0, demand: 0 };
  let busy = false;

  async function refresh(sym) {
    const nowSec = Math.floor(Date.now() / 1000);
    const m = market[sym];
    m.ones = await getKlines(sym, "1s", nowSec, 130);      // cukup utk t0..t0+1 + 60s sigma
    FLOW.addKlines(sym, m.ones);                            // OFI dari REST (WS diblok di Railway)
    pushSeries(sym, m.ones);                                // window 5s + deret close (trail)
    for (const tf of tfsOf(sym)) m.tf[tf] = await getKlines(sym, tf, nowSec, 60);   // termasuk candle sesi berjalan
    m.five5m = m.tf["5m"] || [];                            // RSI 5m (tanpa fetch tambahan)
  }

  /* TRADE ASSISTANT dihitung di SERVER memakai modul bersama trade-plan.js.
     Bias = arah sinyal yang DITAMPILKAN (accepted). Data yang dibutuhkan semuanya sudah ada
     di server: window 5s (dari polling 1s), std/slope/z (statsOfWindow), OFI (flow.js),
     RSI + histTrend (SignalCore), deret close sesi (trail). */
  /* MODEL CONFIDENCE dihitung di SERVER (modul bersama confidence.js).
     Butuh: window 5s, deret 5s (baseline volume), candle tf (tren 3 sesi), std/slope/rsi/z,
     sisa waktu sesi. Semua sudah tersedia dari polling -> tidak ada lagi hitung di klien. */
  function computeConf(sym, tf, t0, nowSec, sig, m) {
    const durMs = (DUR_S[tf] || 300) * 1000;
    const win = (m.five5s || []).slice(-(TRADE.MON_WINDOW[tf] || 24));
    if (win.length < 5) return null;
    const ones = m.ones || [];
    const price = ones.length ? ones[ones.length - 1].close : null;
    // LOCK = harga OPEN sesi (sama dengan yang digambar chart & dipakai plan), TIDAK diambil
    // dari objek sinyal: sinyal bisa ditolak/flat, tetapi model confidence tetap dihitung
    // untuk semua sesi (klien pun menghitungnya walau verdict flat).
    const sessC = (m.tf[tf] || []).find((c) => c.time === t0);
    const lock = sessC ? sessC.open : ((sig && sig.lock != null) ? sig.lock : null);
    if (price == null || lock == null) return null;
    const stat = TRADE.statsOfWindow(win);
    let rsi = null;
    try { rsi = SignalCore.rsiFromSeries((m.tf["5m"] || []).filter((c) => c.time < nowSec).slice(-50), 14); } catch (_) {}
    const remainingMs = Math.max(0, (t0 + (DUR_S[tf] || 300)) * 1000 - Date.now());
    return CONF.confidenceFor({
      sym, tf, C: price, O: lock,
      std: stat.std, slope: stat.slope, rsi, z: stat.z,
      remainingMs, win, five: m.five5s || [], tfCandles: m.tf[tf] || [],
      swingLookback: Math.max(4, Math.min(Math.round((TRADE.MON_WINDOW[tf] || 24) * 0.3), Math.floor(win.length / 3))),
      volTypical: CONF.VOL_TYPICAL[sym],
    });
  }

  /* MOBILE PREDICTION di server (modul bersama mobile-pred.js): arah sesi + keyakinan.
     Butuh candle 5m (pola sesi sebelumnya) + candle 1s (arah candle pertama sesi ini). */
  function computeMobilePred(sym, tf, t0, m) {
    const ones = m.ones || [];
    const price = ones.length ? ones[ones.length - 1].close : null;
    const sessC = (m.tf[tf] || []).find((c) => c.time === t0);
    const lockPrice = sessC ? sessC.open : (price != null ? price : null);
    if (price == null || lockPrice == null) return null;
    try {
      return MP.predictSessionStart({
        tf, durMs: (DUR_S[tf] || 300) * 1000, now: Date.now(),
        price, lockPrice, candles5m: m.tf["5m"] || [], ones,
      });
    } catch (_) { return null; }
  }

  /* ===== METRIK TAMPILAN yang dihitung SERVER =====
     Klien tidak lagi menurunkan nilai apa pun: zone/momentum/rsi/peak/reversal/reward/trend
     dihitung di sini dan dikirim lewat snapshot (assets[sym].all[tf].disp). */
  function computeDisp(sym, tf, t0, nowSec, sig, m, plan) {
    const durS = DUR_S[tf] || 300;
    const win = (m.five5s || []).slice(-(TRADE.MON_WINDOW[tf] || 24));
    if (win.length < 5) return null;
    const ones = m.ones || [];
    const price = ones.length ? ones[ones.length - 1].close : null;
    if (price == null) return null;
    const stat = TRADE.statsOfWindow(win);
    const conf = computeConf(sym, tf, t0, nowSec, sig, m) || {};
    const sessC = (m.tf[tf] || []).find((c) => c.time === t0);
    const O = sessC ? sessC.open : ((sig && sig.lock != null) ? sig.lock : price);
    const C = price, std = stat.std, slope = stat.slope, z = stat.z;
    let rsi = null;
    try { rsi = SignalCore.rsiFromSeries((m.tf["5m"] || []).filter((c) => c.time < nowSec).slice(-50), 14); } catch (_) {}
    const peak = conf.peak ? { price: conf.peak.price, dir: conf.peak.dir, conf: conf.peak.conf } : null;
    const tol = Math.max(O * 0.001, std * 0.5);
    const isTopPeak = !!(peak && peak.dir === "top");
    const isBotPeak = !!(peak && peak.dir === "bot");
    const nearTop = isTopPeak && Math.abs(C - peak.price) < tol;
    const nearBot = isBotPeak && Math.abs(C - peak.price) < tol;
    const droppedFromPeak = isTopPeak && C <= peak.price - tol;
    const roseFromPeak = isBotPeak && C >= peak.price + tol;
    const overbought = z > 1.6, oversold = z < -1.6;
    const rollOver = slope < 0, turnUp = slope > 0;
    const momentum = slope > 0 ? "BULLISH" : slope < 0 ? "BEARISH" : "FLAT";
    let zone = "NETRAL";
    if (isTopPeak) zone = nearTop ? "TOP PEAK" : "NEAR TOP PEAK";
    else if (isBotPeak) zone = nearBot ? "BOTTOM PEAK" : "NEAR BOTTOM PEAK";
    else if (z > 1.2) zone = "NEAR TOP PEAK";
    else if (z < -1.2) zone = "NEAR BOTTOM PEAK";
    // proyeksi penutup + arah kontinuasi (dipakai untuk reward/REVERSAL)
    const remSec = Math.max(0, (t0 + durS) * 1000 - Date.now()) / 1000;
    const projectedClose = C + Math.max(-std * 3, Math.min(std * 3, slope * remSec));
    const trend = conf.trendDir || "flat";
    const trendBias = trend === "up" ? "up" : trend === "down" ? "down" : "flat";
    let verdict = "flat", mode = "CONT", peakPrice = null, reward = 0;
    if (isTopPeak && rollOver && (nearTop || droppedFromPeak || overbought) && peak.conf && trendBias !== "up") { verdict = "down"; mode = "REVERSAL↓"; peakPrice = peak.price; }
    else if (isBotPeak && turnUp && (nearBot || roseFromPeak || oversold) && peak.conf && trendBias !== "down") { verdict = "up"; mode = "REVERSAL↑"; peakPrice = peak.price; }
    else { verdict = "flat"; mode = "CONT"; }
    if (peakPrice != null && verdict !== "flat") {
      reward = verdict === "down" ? (peakPrice - projectedClose) / peakPrice * 100 : (projectedClose - peakPrice) / peakPrice * 100;
    }
    // kekuatan trend: run-length candle searah di ujung deret (+ penalti bila peak berlawanan)
    let trendPct = 0;
    try {
      const cs = (m.tf[tf] || []).filter((c) => c.time < nowSec);
      const mainDir = trend === "up" ? 1 : trend === "down" ? -1 : 0;
      if (mainDir !== 0) {
        const dirs = cs.map((c) => (c.close > c.open ? 1 : c.close < c.open ? -1 : 0));
        let last = dirs.length - 1;
        while (last >= 0 && dirs[last] === 0) last--;
        if (last >= 0 && dirs[last] === mainDir) {
          let run = 0;
          for (let i = last; i >= 0; i--) { if (dirs[i] === mainDir) run++; else if (dirs[i] === 0) continue; else break; }
          trendPct = Math.min(100, run * 14);
          const rev = (trend === "up" && peak && peak.dir === "top") || (trend === "down" && peak && peak.dir === "bot");
          if (rev) trendPct = Math.max(0, trendPct - (peak.conf ? 40 : 20));
        }
      }
    } catch (_) {}
    // ===== SUPPORT / RESISTANCE (gaya analis) — DIBEKUKAN di awal sesi =====
    // Dihitung sekali dari swing high/low window analisis (resistance = swing high terendah di
    // atas harga, support = swing low tertinggi di bawah harga), lalu TIDAK berubah sampai sesi
    // berakhir. Fallback: batas high/low candle sesi bila swing belum tersedia.
    const keyG = `${sym}_${tf}_${t0}`;
    const tierTarget = (plan && plan.cont && plan.cont.target != null) ? plan.cont.target : null;
    let gNow = guidesFor(sym, tf, keyG, C, win, tierTarget);
    if (!gNow) return null;                                   // window belum cukup -> tunggu tick berikut
    let support = gNow.support, resistance = gNow.resistance;
    if (support == null || resistance == null) {
      const sessC2 = (m.tf[tf] || []).filter((x) => x.time < nowSec);
      if (sessC2.length) {
        const lastTf = sessC2[sessC2.length - 1];
        if (support == null && lastTf.low < C) { support = lastTf.low; gNow.support = support; }
        if (resistance == null && lastTf.high > C) { resistance = lastTf.high; gNow.resistance = resistance; }
      }
    }
    return {
      support, resistance, srFrom: "dibekukan awal sesi",
      targetFrozen: gNow.target, guideAt: gNow.at,
      zone, momentum, rsi, mean: stat.mean, std, slope, slopeRecent: stat.slopeRecent, z,
      peak: peak ? { price: peak.price, dir: peak.dir, conf: peak.conf } : null,
      reversal: verdict === "flat" ? null : { dir: verdict, mode, peakPrice, reward },
      reward, projectedClose,
      vol5s: conf.rel != null ? conf.rel : null,
      liquidity: conf.liquidity || null,
      trendDir: trend, trendPct,
      lockedAt: (sig && sig.lockedAt) || null,
    };
  }

  function computePlan(sym, tf, t0, nowSec, sig, m) {
    const bias = (sig && sig.accepted && (sig.dir === "up" || sig.dir === "down")) ? sig.dir : null;
    const key = `${sym}_${tf}_${t0}`;
    const st = planStateFor(sym, tf, key);
    if (!bias) return null;                                   // sinyal flat/ditolak -> tanpa plan
    const win = (m.five5s || []).slice(-(TRADE.MON_WINDOW[tf] || 24));
    const ones = m.ones || [];
    const price = ones.length ? ones[ones.length - 1].close : null;
    if (price == null || win.length < 5) return null;
    const stat = TRADE.statsOfWindow(win);
    const ofi = FLOW.sessionOFI(sym, t0, nowSec);
    const ofiShort = FLOW.sessionOFI(sym, nowSec - 120, nowSec);
    let rsi = null, histTrend = null;
    try { rsi = SignalCore.rsiFromSeries((m.tf["5m"] || []).filter((c) => c.time < nowSec).slice(-50), 14); } catch (_) {}
    try { histTrend = SignalCore.analyzeHistoricalTrend(m.tf[tf] || [], 50); } catch (_) {}
    const closes = (m.onesHist || []).filter((o) => o.t >= t0).map((o) => o.c);
    return TRADE.buildPlan({
      bias, tf, lock: sig.lock, price,
      std: stat.std, slope: stat.slope, slopeRecent: stat.slopeRecent, rsi, z: stat.z,
      ofi, ofiShort, histTrend, win, sessionCloses: closes,
      durMs: (DUR_S[tf] || 300) * 1000, remainMs: Math.max(0, (t0 + (DUR_S[tf] || 300)) * 1000 - Date.now()),
      key, now: Date.now(), tiers: TIERS, state: st,
    });
  }

  async function loop() {
    if (busy) return;
    // Polling penuh bila ada penonton (SSE) ATAU endpoint JSON baru dipanggil (demand 15s).
    // Tanpa penonton tetap polling LAMBAT (5s) supaya state Trade Assistant tiap sesi ikut
    // terbentuk — tanpa itu, catatan entry/early close di ledger bisa kosong untuk sesi yang
    // tidak ditonton saat itu.
    const idle = stats.subscribers <= 0 && Date.now() - stats.demand > 15000;
    if (idle && Date.now() - (stats.lastIdlePoll || 0) < 5000) return;
    if (idle) stats.lastIdlePoll = Date.now();
    busy = true;
    try {
      const nowSec = Math.floor(Date.now() / 1000);
      const profile = (typeof getGates === "function" ? getGates() : null) || GATES_DEF.BOOTSTRAP;
      for (const sym of ["BTC", "ETH", "BNB"]) {
        try { await refresh(sym); } catch (e) { stats.errors++; stats.lastErr = e && e.message; continue; }
        for (const tf of tfsOf(sym)) {
          const t0 = Math.floor(nowSec / DUR_S[tf]) * DUR_S[tf];
          if (nowSec - t0 < 3) continue;                    // tunggu detik ke-2 selesai
          const cur = session[sym][tf];
          if (cur && cur.t0 === t0) continue;                // sudah terkunci untuk sesi ini
          const tfc = market[sym].tf[tf] || [];
          const idx = tfc.findIndex((c) => c.time === t0);
          // PENTING: candle 1s di detik t0 & t0+1 di-fetch TERTARGET ke t0 (bukan window bergulir),
          // supaya sinyal kanonik tetap bisa dihitung walau engine baru mulai di tengah sesi
          // (mis. device pertama baru membuka app setelah sesi berjalan) -> hasilnya tetap sama.
          let ones = market[sym].ones;
          try {
            const targeted = await getKlines(sym, "1s", t0 + 2, 70);
            if (targeted && targeted.some((c) => c.time === t0) && targeted.some((c) => c.time === t0 + 1)) ones = targeted;
          } catch (_) {}
          const r = computeSignal({
            sym, tf, t0, tfc, idx, ones, five5m: market[sym].five5m,
            profile, getModel, SignalCore, nowSec,
          });
          const sigForPlan = r.skipped ? null : r.signal;
          const plan = computePlan(sym, tf, t0, nowSec, sigForPlan, market[sym]);
          const conf = computeConf(sym, tf, t0, nowSec, sigForPlan, market[sym]);
          session[sym][tf] = { t0, signal: sigForPlan, skipped: r.skipped || null, at: Date.now(), plan, conf };
          if (!r.skipped) {
            stats.locked++;
            log(`[ENGINE] ${sym} ${tf} terkunci: dir=${r.signal.dir} grade=${r.signal.grade || "-"} accepted=${r.signal.accepted} volRel2=${r.signal.volRel2} surprise=${String(r.signal.surprise).slice(0, 6)}`);
          } else {
            log(`[ENGINE] ${sym} ${tf} tanpa sinyal: ${r.skipped}`);
          }
        }
        // ===== UPDATE LIVE setiap tick =====
        // Sinyal memang DIKUNCI sekali per sesi, TAPI Trade Assistant dan model confidence
        // adalah metrik hidup: dwell turn/fade, status entry/close, jarak ke lock, sisa waktu,
        // dan feasibilitas berubah tiap detik. Jadi keduanya dihitung ULANG di sini setiap tick
        // (bukan memakai hasil saat lock) supaya angka yang dikirim = keadaan sekarang.
        // simpan state (entry/close/dwell) ke volume — throttle 3 detik di dalam saveState()
        for (const tf of tfsOf(sym)) {
          const durS = DUR_S[tf] || 300;
          const t0Live = Math.floor(nowSec / durS) * durS;
          const cur = session[sym][tf];
          if (!cur || cur.t0 !== t0Live) continue;            // belum terkunci untuk sesi ini
          // Pastikan state sesi LAMA di-snapshot (untuk catatan entry/early close di ledger)
          // walau sesi baru tidak menghasilkan plan (mis. sinyal flat/ditolak).
          planStateFor(sym, tf, `${sym}_${tf}_${t0Live}`);
          try {
            cur.plan = computePlan(sym, tf, t0Live, nowSec, cur.signal, market[sym]);
            cur.conf = computeConf(sym, tf, t0Live, nowSec, cur.signal, market[sym]);
            cur.mobilePred = computeMobilePred(sym, tf, t0Live, market[sym]);
            cur.disp = computeDisp(sym, tf, t0Live, nowSec, cur.signal, market[sym], cur.plan);
          } catch (e) { stats.errors++; stats.lastErr = e && e.message; }
        }
        saveState();      // persist state trade (posisi/early close) ke volume
      }
      stats.refreshes++; stats.lastAt = Date.now();
    } catch (e) { stats.errors++; stats.lastErr = e && e.message; }
    finally { busy = false; }
  }

  // Snapshot untuk UI: harga live, lock, selisih $, sinyal sesi (kanonik, dari server)
  // Harga OPEN candle sesi untuk (sym, tf) pada t0 — dipakai sebagai LOCK cadangan.
  function lockOpenOf(sym, tf, t0Sec) {
    const arr = (market[sym] && market[sym].tf && market[sym].tf[tf]) ? market[sym].tf[tf] : [];
    if (!arr.length) return null;
    // 1) candle sesi yang tepat
    const exact = arr.find((x) => x.time === t0Sec);
    if (exact) return exact.open;
    // 2) candle sesi berikutnya (bila ada) -> open-nya = harga di awal sesi berikutnya
    const after = arr.find((x) => x.time > t0Sec);
    if (after) return after.open;
    // 3) candle terakhir sebelum sesi -> close-nya = harga tepat di batas sesi
    const prev = arr.filter((x) => x.time <= t0Sec).pop();
    if (prev) return prev.close;
    return arr[arr.length - 1].close;
  }

  // Ringkas satu (sym, tf) untuk dipakai lintas-tf di snapshot.
  function tfEntry(sym, tf, price) {
    const durMs = (DUR_S[tf] || 300) * 1000;
    const now = Date.now();
    const t0 = Math.floor(now / durMs) * durMs;
    const sess = session[sym] && session[sym][tf];
    const same = !!(sess && sess.t0 === t0 / 1000);
    const sig = same ? sess.signal : null;
    return {
      t0: t0 / 1000,
      skipped: same ? sess.skipped : "pending",
      signal: sig ? Object.assign({}, sig, { verdict: sig.accepted ? sig.dir : "flat" }) : null,
      plan: same ? (sess.plan || null) : null,
      // LOCK sesi: harga OPEN candle sesi — SELALU tersedia walau sinyalnya belum terkunci atau
      // di-skip. Sebelumnya nilainya hanya ada bila ada sinyal, sehingga GARIS LOCK di chart
      // desktop bisa hilang pada sesi tanpa sinyal (mobile punya fallback lokal, desktop tidak).
      lock: (sess && same && sess.signal && sess.signal.lock != null) ? sess.signal.lock : lockOpenOf(sym, tf, t0),
      conf: same ? (sess.conf || null) : null,
      disp: same ? (sess.disp || null) : null,
      mobilePred: same ? (sess.mobilePred || null) : null,
      ofi: FLOW.sessionOFI(sym, t0 / 1000, Math.floor(now / 1000)),
      ofiShort: FLOW.sessionOFI(sym, Math.floor(now / 1000) - 120, Math.floor(now / 1000)),
    };
  }

  function snapshot(tf) {
    const now = Date.now();
    const durMs = (DUR_S[tf] || 300) * 1000;
    const t0 = Math.floor(now / durMs) * durMs;
    const out = {
      ts: now, tf, sessionT0: t0 / 1000,
      remainSec: Math.max(0, Math.round((t0 + durMs - now) / 1000)),
      source: "engine", assets: {},
    };
    for (const sym of ["BTC", "ETH", "BNB"]) {
      const ones = (market[sym] && market[sym].ones) || [];
      const px = ones.length ? ones[ones.length - 1].close : null;
      const sess = session[sym] && session[sym][tf];
      const sameSession = !!(sess && sess.t0 === t0 / 1000);
      const sig = sameSession ? sess.signal : null;
      // Semua tf dilayani server (lihat TFS) -> klien tidak perlu menghitung sinyal sendiri.
      const all = {};
      for (const t of tfsOf(sym)) all[t] = tfEntry(sym, t, px);
      out.assets[sym] = {
        all,
        price: px,
        // LOCK per aset: dari sinyal, atau open candle sesi (selalu ada) bila sinyal belum/skip.
        lock: (sig && sig.lock != null) ? sig.lock : lockOpenOf(sym, tf, t0 / 1000),
        deltaUsd: (px != null && sig && sig.lock) ? +(px - sig.lock).toFixed(2) : null,
        // `dir` = arah mentah (dipakai ledger/learning). `verdict` = yang DITAMPILKAN:
        // "flat" bila gate menolak (tier/likuiditas/threshold) — sama seperti app.
        signal: sig ? Object.assign({}, sig, { verdict: sig.accepted ? sig.dir : "flat" }) : null,
        // Trade Assistant (diproses server): aksi, level, status entry/close, health.
        plan: (sess && sameSession) ? (sess.plan || null) : null,
        // Model confidence (diproses server): angka untuk arah up/down + konteks.
        conf: (sess && sameSession) ? (sess.conf || null) : null,
        disp: (sess && sameSession) ? (sess.disp || null) : null,
        // Mobile prediction (diproses server).
        mobilePred: (sess && sameSession) ? (sess.mobilePred || null) : null,
        // OFI 120 detik terakhir (lebih responsif; dipakai tooltip).
        ofiShort: FLOW.sessionOFI(sym, Math.floor(now / 1000) - 120, Math.floor(now / 1000)),
        // OFI LIVE (dihitung ulang setiap snapshot, bukan beku saat sinyal dikunci):
        // parameter sesi berjalan -> semua device menampilkan angka yang SAMA.
        ofi: FLOW.sessionOFI(sym, t0 / 1000, Math.floor(now / 1000)),
        skipped: sameSession ? sess.skipped : "pending",
        engine: { ones: ones.length, tfs: Object.keys(market[sym].tf || {}) },
      };
    }
    return out;
  }

  function start() {
    stats.subscribers = 0;
    setInterval(() => loop().catch(() => {}), 2000);
    setTimeout(() => loop().catch(() => {}), 300);
    log(`[ENGINE] aktif — sinyal server-side untuk ${TFS.join(", ")} (polling hanya bila ada subscriber)`);
  }
  return {
    // Status trade akhir sebuah sesi (entry/early close) untuk dicatat ke ledger.
    // Diambil dari snapshot sesi yang sudah berakhir; bila sesi itu MASIH sesi berjalan
    // (belum rollover) dibaca dari state live — supaya resolusi tetap dapat data walau
    // sesi baru belum pernah diproses.
    tradeFor: (sym, tf, t0Sec) => {
      const k = `${sym}_${tf}_${t0Sec}`;
      if (finishedTrades[k]) return finishedTrades[k];
      const cur = planState[sym] && planState[sym][tf];
      if (cur && cur.key === k && cur.st) return tradeStateOf(cur.st, k);
      return null;
    },
    start, loop, snapshot,
    touch: () => { stats.demand = Date.now(); },
    addSubscriber: () => { stats.subscribers++; return () => { stats.subscribers = Math.max(0, stats.subscribers - 1); }; },
    status: () => Object.assign({}, stats, {
      market: Object.keys(market).map((s) => ({ sym: s, ones: (market[s].ones || []).length, tf: Object.keys(market[s].tf || {}) })),
      sessions: Object.keys(session).reduce((a, s) => { a[s] = Object.keys(session[s]).reduce((b, t) => { b[t] = session[s][t] ? { t0: session[s][t].t0, dir: session[s][t].signal ? session[s][t].signal.dir : null, grade: session[s][t].signal ? session[s][t].signal.grade : null, skipped: session[s][t].skipped } : null; return b; }, {}); return a; }, {}),
    }),
  };
}

module.exports = { createEngine, TFS };
