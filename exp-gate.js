/* ============================================================================
   exp-gate.js — Evaluator gate EKSPERIMENTAL "MOMENTUM × TREND-AGREE"
   OBSERVASIONAL: TIDAK mengubah accepted/reject produksi. Hanya menilai cohort
   dari fitur yang SUDAH terekam di ledger, lalu membandingkannya apple-to-apple
   dengan gate produksi (gate.accepted).

   Dipakai oleh:
     - server.js  (dijalankan tiap refit -> models/current/exp.json, endpoint /api/exp)
     - backtest/exp_gate.js (CLI laporan harian)
   ============================================================================ */
const BAD_HOURS_WIB = new Set([3, 5, 10, 12, 14, 21]);   // WR<52% (data 9 hari)

function wilson(k, n, z = 1.96) {
  if (!n) return 0;
  const p = k / n, den = 1 + (z * z) / n;
  const c = (p + (z * z) / (2 * n)) / den;
  const h = (z * Math.sqrt((p * (1 - p)) / n + (z * z) / (4 * n * n))) / den;
  return Math.max(0, c - h);
}
const mean = (a) => (a.length ? a.reduce((x, y) => x + y, 0) / a.length : 0);

function rowOf(r) {
  const s = r.sig || {}, res = r.res || {};
  const dir = s.dir;
  if (dir !== "up" && dir !== "down") return null;
  if (res.won !== 0 && res.won !== 1) return null;
  const ind = s.ind || {};
  const p = r.odds && r.odds[dir];
  const price = (typeof p === "number" && p > 0 && p < 1) ? p : null;
  let roi = (typeof res.settleRoi === "number") ? res.settleRoi : null;
  if (roi == null && price != null) roi = (res.won === 1) ? ((1 - price) / price * 100) : -100;
  if (roi == null && r.bot && typeof r.bot.roiPct === "number") roi = r.bot.roiPct;
  const t0 = r.t0 || 0;
  return {
    k: r.k, t0, dir, won: res.won, price, roi,
    acc: !!(r.gate && r.gate.accepted), flatEntry: !!(s.flatEntry),
    mv2: typeof s.mv2 === "number" ? s.mv2 : 0,
    vol: typeof s.volRel2 === "number" ? s.volRel2 : 0,
    emaCross: ind.emaCross || null, macdDir: ind.macdDir || null,
    hourWIB: Math.floor(((t0 + 7 * 3600) % 86400) / 3600),
  };
}
const agree = (r) => r.emaCross === r.dir && r.macdDir === r.dir;

const COHORTS = {
  "PROD (gate sekarang)": (r) => r.acc && !r.flatEntry,
  "MOM mv2>=.015":        (r) => r.mv2 >= 0.015,
  "TREND agree":          (r) => agree(r),
  "CORE trend+jam":       (r) => agree(r) && !BAD_HOURS_WIB.has(r.hourWIB),
  "CORE+MOM (mv2>=.008)": (r) => agree(r) && !BAD_HOURS_WIB.has(r.hourWIB) && r.mv2 >= 0.008,
  "CORE+VOL sehat":       (r) => agree(r) && !BAD_HOURS_WIB.has(r.hourWIB) && (r.vol < 1.3 || r.vol >= 5),
};

const evUnit = (r) => (r.won === 1 ? (1 - r.price) / r.price : -1);
function compound(rois, frac = 0.05) { let e = 100; for (const x of rois) e *= (1 + (frac * x) / 100); return e; }

function stats(rows) {
  const n = rows.length; if (!n) return null;
  const w = rows.reduce((a, r) => a + r.won, 0);
  const od = rows.filter((r) => r.price != null);
  const ev = od.length ? mean(od.map(evUnit)) : null;
  const rois = rows.map((r) => r.roi).filter((x) => typeof x === "number");
  return { n, wr: +(100 * w / n).toFixed(2), lb: +(100 * wilson(w, n)).toFixed(2), nOd: od.length,
    avgPx: od.length ? +mean(od.map((r) => r.price)).toFixed(4) : null,
    ev: ev == null ? null : +ev.toFixed(4), evNet5: ev == null ? null : +(ev - 0.05).toFixed(4),
    eq: rois.length ? +compound(rois).toFixed(1) : null, nRoi: rois.length };
}

function evaluate(records) {
  const rows = (records || []).map(rowOf).filter(Boolean).sort((a, b) => a.t0 - b.t0);
  const out = { generated: new Date().toISOString(), rows: rows.length, badHoursWIB: [...BAD_HOURS_WIB], cohorts: {} };
  if (!rows.length) return out;
  out.range = { from: new Date(rows[0].t0 * 1000).toISOString(), to: new Date(rows[rows.length - 1].t0 * 1000).toISOString() };
  const cut = Math.floor(rows.length * 0.7);
  const tr = rows.slice(0, cut), te = rows.slice(cut);
  const ods = rows.filter((r) => r.price != null && typeof r.roi === "number");
  const oc = Math.floor(ods.length * 0.6);
  const otr = ods.slice(0, oc), ote = ods.slice(oc);
  for (const [name, f] of Object.entries(COHORTS)) {
    const full = stats(rows.filter(f));
    if (!full) { out.cohorts[name] = null; continue; }
    const a = stats(tr.filter(f)), b = stats(te.filter(f));
    const oa = stats(otr.filter(f)), ob = stats(ote.filter(f));
    out.cohorts[name] = Object.assign(full, {
      wf: { wrTrain: a ? a.wr : null, wrTest: b ? b.wr : null, evTest: b ? b.ev : null, eqTest: b ? b.eq : null },
      wfOdds: { evTrain: oa ? oa.ev : null, evTest: ob ? ob.ev : null, eqTest: ob ? ob.eq : null, nTest: ob ? ob.nOd : 0 },
    });
  }
  return out;
}

module.exports = { evaluate, COHORTS, rowOf, wilson };
