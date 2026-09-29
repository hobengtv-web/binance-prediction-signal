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

const TFS = ["5m", "15m"];   // 1h tidak disajikan server (di 2 detik sinyalnya flat by design)

function createEngine(deps) {
  const { getKlines, getModel, getGates, log = console.log } = deps;
  const market = { BTC: { ones: [], tf: {}, five5m: [], five5s: [], onesHist: [], lastOne: 0 },
                   ETH: { ones: [], tf: {}, five5m: [], five5s: [], onesHist: [], lastOne: 0 } };
  // State Trade Assistant per (aset, tf): peak/dwell/entered/closed. Direset tiap sesi baru.
  const planState = { BTC: {}, ETH: {} };
  function planStateFor(sym, tf, key) {
    let s = planState[sym][tf];
    if (!s || s.key !== key) {
      s = planState[sym][tf] = { key, st: { peak: {}, dwell: {}, entered: {}, closed: {} } };
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
  const session = { BTC: {}, ETH: {} };          // sym -> tf -> { t0, signal|null, skipped, at }
  const stats = { refreshes: 0, errors: 0, locked: 0, lastAt: null, lastErr: null, subscribers: 0, demand: 0 };
  let busy = false;

  async function refresh(sym) {
    const nowSec = Math.floor(Date.now() / 1000);
    const m = market[sym];
    m.ones = await getKlines(sym, "1s", nowSec, 130);      // cukup utk t0..t0+1 + 60s sigma
    FLOW.addKlines(sym, m.ones);                            // OFI dari REST (WS diblok di Railway)
    pushSeries(sym, m.ones);                                // window 5s + deret close (trail)
    for (const tf of TFS) m.tf[tf] = await getKlines(sym, tf, nowSec, 60);   // termasuk candle sesi berjalan
    m.five5m = m.tf["5m"] || [];                            // RSI 5m (tanpa fetch tambahan)
  }

  /* TRADE ASSISTANT dihitung di SERVER memakai modul bersama trade-plan.js.
     Bias = arah sinyal yang DITAMPILKAN (accepted). Data yang dibutuhkan semuanya sudah ada
     di server: window 5s (dari polling 1s), std/slope/z (statsOfWindow), OFI (flow.js),
     RSI + histTrend (SignalCore), deret close sesi (trail). */
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
      key, now: Date.now(), tiers: TIERS, state: st,
    });
  }

  async function loop() {
    if (busy) return;
    // Polling hanya bila ada penonton (SSE) ATAU endpoint JSON baru dipanggil (demand 15s).
    if (stats.subscribers <= 0 && Date.now() - stats.demand > 15000) return;
    busy = true;
    try {
      const nowSec = Math.floor(Date.now() / 1000);
      const profile = (typeof getGates === "function" ? getGates() : null) || GATES_DEF.BOOTSTRAP;
      for (const sym of ["BTC", "ETH"]) {
        try { await refresh(sym); } catch (e) { stats.errors++; stats.lastErr = e && e.message; continue; }
        for (const tf of TFS) {
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
          const plan = computePlan(sym, tf, t0, nowSec, r.skipped ? null : r.signal, market[sym]);
          session[sym][tf] = { t0, signal: r.skipped ? null : r.signal, skipped: r.skipped || null, at: Date.now(), plan };
          if (!r.skipped) {
            stats.locked++;
            log(`[ENGINE] ${sym} ${tf} terkunci: dir=${r.signal.dir} grade=${r.signal.grade || "-"} accepted=${r.signal.accepted} volRel2=${r.signal.volRel2} surprise=${String(r.signal.surprise).slice(0, 6)}`);
          } else {
            log(`[ENGINE] ${sym} ${tf} tanpa sinyal: ${r.skipped}`);
          }
        }
      }
      stats.refreshes++; stats.lastAt = Date.now();
    } catch (e) { stats.errors++; stats.lastErr = e && e.message; }
    finally { busy = false; }
  }

  // Snapshot untuk UI: harga live, lock, selisih $, sinyal sesi (kanonik, dari server)
  function snapshot(tf) {
    const now = Date.now();
    const durMs = (DUR_S[tf] || 300) * 1000;
    const t0 = Math.floor(now / durMs) * durMs;
    const out = {
      ts: now, tf, sessionT0: t0 / 1000,
      remainSec: Math.max(0, Math.round((t0 + durMs - now) / 1000)),
      source: "engine", assets: {},
    };
    for (const sym of ["BTC", "ETH"]) {
      const ones = (market[sym] && market[sym].ones) || [];
      const px = ones.length ? ones[ones.length - 1].close : null;
      const sess = session[sym] && session[sym][tf];
      const sameSession = !!(sess && sess.t0 === t0 / 1000);
      const sig = sameSession ? sess.signal : null;
      out.assets[sym] = {
        price: px,
        lock: sig ? sig.lock : null,
        deltaUsd: (px != null && sig && sig.lock) ? +(px - sig.lock).toFixed(2) : null,
        // `dir` = arah mentah (dipakai ledger/learning). `verdict` = yang DITAMPILKAN:
        // "flat" bila gate menolak (tier/likuiditas/threshold) — sama seperti app.
        signal: sig ? Object.assign({}, sig, { verdict: sig.accepted ? sig.dir : "flat" }) : null,
        // Trade Assistant (diproses server): aksi, level, status entry/close, health.
        plan: (sess && sameSession) ? (sess.plan || null) : null,
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
