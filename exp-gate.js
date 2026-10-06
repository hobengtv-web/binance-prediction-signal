/* ============================================================================
   exp-gate.js — Gate EKSPERIMENTAL "MOMENTUM × TREND-AGREE" (OBSERVASIONAL)
   Spec FROZEN: lihat EXPERIMENT.md. TIDAK mengubah accepted/reject produksi.

   Dipakai oleh:
     - capture.js      : label per-sesi (sig.exp) -> ledger/snapshot/event
     - server.js       : evaluator refit -> models/current/exp.json, /api/exp
     - backtest/exp_gate.js : CLI laporan harian
     - prediction-bot  : shadow paper tracker (via /api/signal field exp)
   ============================================================================ */
const BAD_HOURS_WIB = new Set([3, 5, 10, 12, 14, 21]);   // WR<52% (data 9 hari) — FROZEN

// Definisi cohort (FROZEN — jangan ubah tanpa freeze baru di EXPERIMENT.md)
const COHORT_DEFS = [
  { id: "TREND",    name: "TREND agree",          rule: "emaCross==dir && macdDir==dir" },
  { id: "MOM",      name: "MOM mv2>=.015",        rule: "mv2>=0.015%" },
  { id: "CORE",     name: "CORE trend+jam",       rule: "TREND && hourWIB not bad" },
  { id: "CORE_MOM", name: "CORE+MOM (mv2>=.008)", rule: "CORE && mv2>=0.008%" },
];
const NAME_BY_ID = Object.fromEntries(COHORT_DEFS.map((c) => [c.id, c.name]));

function wilson(k, n, z = 1.96) {
  if (!n) return 0;
  const p = k / n, den = 1 + (z * z) / n;
  const c = (p + (z * z) / (2 * n)) / den;
  const h = (z * Math.sqrt((p * (1 - p)) / n + (z * z) / (4 * n * n))) / den;
  return Math.max(0, c - h);
}
const mean = (a) => (a.length ? a.reduce((x, y) => x + y, 0) / a.length : 0);
const hourWIBof = (t0) => Math.floor(((t0 + 7 * 3600) % 86400) / 3600);

// o = { dir, mv2, emaCross, macdDir, hourWIB }
function cohortTags(o) {
  const trend = o.emaCross && o.emaCross === o.dir && o.macdDir === o.dir;
  const goodHr = !BAD_HOURS_WIB.has(o.hourWIB);
  const tags = [];
  if (trend) tags.push("TREND");
  if (o.mv2 >= 0.015) tags.push("MOM");
  if (trend && goodHr) tags.push("CORE");
  if (trend && goodHr && o.mv2 >= 0.008) tags.push("CORE_MOM");
  return tags;
}
// Helper untuk capture.js: dari objek signal + t0
function cohortOfSignal(sig, t0) {
  const ind = (sig && sig.ind) || {};
  return cohortTags({
    dir: sig && sig.dir, mv2: (sig && typeof sig.mv2 === "number") ? sig.mv2 : 0,
    emaCross: ind.emaCross || null, macdDir: ind.macdDir || null, hourWIB: hourWIBof(t0 || 0),
  });
}

function rowOf(r) {
  const s = r.sig || {}, res = r.res || {};
  if (s.skipped) return null;                       // hanya sesi berarah NON-FLAT (tradeable)
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
  const base = { dir, mv2: typeof s.mv2 === "number" ? s.mv2 : 0, emaCross: ind.emaCross || null, macdDir: ind.macdDir || null, hourWIB: hourWIBof(t0) };
  return {
    k: r.k, t0, dir, won: res.won, price, roi,
    acc: !!(r.gate && r.gate.accepted), flatEntry: !!(s.flatEntry),
    mv2: base.mv2, vol: typeof s.volRel2 === "number" ? s.volRel2 : 0,
    emaCross: base.emaCross, macdDir: base.macdDir, hourWIB: base.hourWIB,
    tags: cohortTags(base),
  };
}

const COST = Number(process.env.EXP_COST_PCT || 0.03);        // haircut spread+fee (fraksi) — FROZEN default 3%
const evUnit = (r) => (r.won === 1 ? (1 - r.price) / r.price : -1);
const netUnit = (r) => evUnit(r) - COST;                       // EV net setelah haircut
const netRoiPct = (r) => (evUnit(r) - COST) * 100;             // ROI% net per trade (hold-to-settle)
function compound(rois, frac = 0.05) { let e = 100; for (const x of rois) e *= (1 + (frac * x) / 100); return e; }
function eqAndDD(rois) { let peak = 100, eq = 100, dd = 0; for (const x of rois) { eq *= (1 + 0.05 * x / 100); peak = Math.max(peak, eq); dd = Math.min(dd, eq / peak - 1); } return { eq, dd: 100 * dd }; }

function stats(rows) {
  const n = rows.length; if (!n) return null;
  const w = rows.reduce((a, r) => a + r.won, 0);
  const od = rows.filter((r) => r.price != null);
  const ev = od.length ? mean(od.map(evUnit)) : null;
  const rois = rows.map((r) => r.roi).filter((x) => typeof x === "number");
  const netRois = od.map(netRoiPct);
  const g = eqAndDD(rois), ne = eqAndDD(netRois);
  return { n, wr: +(100 * w / n).toFixed(2), lb: +(100 * wilson(w, n)).toFixed(2), nOd: od.length,
    cost: COST,
    avgPx: od.length ? +mean(od.map((r) => r.price)).toFixed(4) : null,
    ev: ev == null ? null : +ev.toFixed(4), evNet: ev == null ? null : +(ev - COST).toFixed(4),
    eq: rois.length ? +g.eq.toFixed(1) : null, maxDD: rois.length ? +g.dd.toFixed(1) : null, nRoi: rois.length,
    netEq: netRois.length ? +ne.eq.toFixed(1) : null, netMaxDD: netRois.length ? +ne.dd.toFixed(1) : null };
}

const PREDS = {
  "PROD": (r) => r.acc && !r.flatEntry,
};
for (const c of COHORT_DEFS) PREDS[c.id] = (r) => r.tags.includes(c.id);

function evaluate(records) {
  const rows = (records || []).map(rowOf).filter(Boolean).sort((a, b) => a.t0 - b.t0);
  const out = { generated: new Date().toISOString(), rows: rows.length, badHoursWIB: [...BAD_HOURS_WIB], defs: COHORT_DEFS, cohorts: {} };
  if (!rows.length) return out;
  out.range = { from: new Date(rows[0].t0 * 1000).toISOString(), to: new Date(rows[rows.length - 1].t0 * 1000).toISOString() };
  const cut = Math.floor(rows.length * 0.7);
  const tr = rows.slice(0, cut), te = rows.slice(cut);
  const ods = rows.filter((r) => r.price != null && typeof r.roi === "number");
  const oc = Math.floor(ods.length * 0.6);
  const otr = ods.slice(0, oc), ote = ods.slice(oc);
  for (const [key, f] of Object.entries(PREDS)) {
    const display = NAME_BY_ID[key] || "PROD (gate sekarang)";
    const full = stats(rows.filter(f));
    if (!full) { out.cohorts[display] = null; continue; }
    const a = stats(tr.filter(f)), b = stats(te.filter(f));
    const oa = stats(otr.filter(f)), ob = stats(ote.filter(f));
    full.id = key;
    full.wf = { wrTrain: a ? a.wr : null, wrTest: b ? b.wr : null, evTest: b ? b.ev : null, eqTest: b ? b.eq : null };
    full.wfOdds = { evTrain: oa ? oa.ev : null, evTest: ob ? ob.ev : null, eqTest: ob ? ob.eq : null, nTest: ob ? ob.nOd : 0 };
    out.cohorts[display] = full;
  }
  return out;
}

module.exports = { evaluate, cohortTags, cohortOfSignal, COHORT_DEFS, BAD_HOURS_WIB, wilson };
