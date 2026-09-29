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
const SignalCore = require("./signal-core.js");

const TFS = ["5m", "15m"];   // 1h tidak disajikan server (di 2 detik sinyalnya flat by design)

function createEngine(deps) {
  const { getKlines, getModel, getGates, log = console.log } = deps;
  const market = { BTC: { ones: [], tf: {}, five5m: [] }, ETH: { ones: [], tf: {}, five5m: [] } };
  const session = { BTC: {}, ETH: {} };          // sym -> tf -> { t0, signal|null, skipped, at }
  const stats = { refreshes: 0, errors: 0, locked: 0, lastAt: null, lastErr: null, subscribers: 0, demand: 0 };
  let busy = false;

  async function refresh(sym) {
    const nowSec = Math.floor(Date.now() / 1000);
    const m = market[sym];
    m.ones = await getKlines(sym, "1s", nowSec, 130);      // cukup utk t0..t0+1 + 60s sigma
    for (const tf of TFS) m.tf[tf] = await getKlines(sym, tf, nowSec, 60);   // termasuk candle sesi berjalan
    m.five5m = m.tf["5m"] || [];                            // RSI 5m (tanpa fetch tambahan)
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
          session[sym][tf] = { t0, signal: r.skipped ? null : r.signal, skipped: r.skipped || null, at: Date.now() };
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
