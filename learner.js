/* ============================================================================
   LEARNER (shared library) — dipakai oleh server (re-fit terjadwal) dan skrip backtest.
   Prinsip anti-overfit yang sama: split berurutan waktu (bukan random), Wilson bound,
   minimal n, dan aturan hanya sah bila lolos jendela UJI.
   Model = FILTER konteks (bukan penebak arah): daftar konteks yang ditahan.
   ============================================================================ */

const wilson = (w, n, z = 1.96) => {
  if (!n) return { lo: 0, hi: 1 };
  const p = w / n, d = 1 + (z * z) / n, c = p + (z * z) / (2 * n);
  const m = z * Math.sqrt((p * (1 - p)) / n + (z * z) / (4 * n * n));
  return { lo: Math.max(0, (c - m) / d), hi: Math.min(1, (c + m) / d) };
};
const stat = (rows, key) => {
  const n = rows.length, w = rows.reduce((a, r) => a + (r[key] === 1 ? 1 : 0), 0);
  const { lo, hi } = wilson(w, n);
  return { n, wr: n ? w / n : 0, w, lb: lo, ub: hi };
};
const mean = (a) => (a.length ? a.reduce((x, y) => x + y, 0) / a.length : 0);

/* ---------- bucket (HARUS identik dengan app.js learnBuckets) ---------- */
const NUM = { "<0.005": 0.002, "0.005-0.01": 0.007, "0.01-0.02": 0.015, "0.02-0.035": 0.027, ">0.035": 0.05 };
const bMinute = (m) => (m <= 1 ? "1" : m <= 3 ? "2-3" : m <= 6 ? "4-6" : "7+");
const bRsi = (r) => (r == null ? "na" : r < 30 ? "<30" : r < 40 ? "30-40" : r < 60 ? "40-60" : r < 70 ? "60-70" : ">70");
const bVol = (v) => (v == null ? "na" : v < 0.7 ? "<0.7" : v < 1.0 ? "0.7-1" : v < 1.5 ? "1-1.5" : v < 2.5 ? "1.5-2.5" : ">2.5");
const bHour = (h) => (h == null ? "na" : h < 4 ? "0-3" : h < 8 ? "4-7" : h < 12 ? "8-11" : h < 16 ? "12-15" : h < 20 ? "16-19" : "20-23");
const bHist = (s) => (s == null ? "na" : s < 10 ? "<10" : s < 20 ? "10-20" : ">20");
const bGap = (g) => (g == null ? "na" : g < 0.005 ? "<0.005" : g < 0.01 ? "0.005-0.01" : g < 0.02 ? "0.01-0.02" : g < 0.035 ? "0.02-0.035" : ">0.035");
const isBucket = (v) => typeof v === "string" && /[<>\-]/.test(v);

/* ---------- record ledger -> baris fitur ---------- */
// CANONICAL_MAX_MS: sinyal yang dipakai belajar = capture paling dekat ke detik ke-2.
// Browser yang dibuka di tengah sesi menghasilkan capOffsetMs besar -> DIKECUALIKAN dari
// pelatihan (bukan dihapus; tetap ada di record sebagai `alts` untuk studi entry telat).
const CANONICAL_MAX_MS = 6000;
function rowsFrom(records, minT0 = 1700000000, opts = {}) {
  const includeLate = !!opts.includeLate;
  const out = [];
  const skipped = { noSig: 0, noRes: 0, late: 0, badDir: 0, old: 0 };
  for (const r of records || []) {
    if (!r || !r.t0 || r.t0 < minT0) { skipped.old++; continue; }
    if (!r.sig) { skipped.noSig++; continue; }
    if (!r.res) { skipped.noRes++; continue; }
    const s = r.sig, dir = s.dir;
    if (dir !== "up" && dir !== "down") { skipped.badDir++; continue; }
    // kanonik: capOffsetMs ada -> wajib <= 6s; record lama (tanpa capOffsetMs) -> pakai minuteIn
    const off = typeof s.capOffsetMs === "number" ? s.capOffsetMs : null;
    const canonical = off != null ? off <= CANONICAL_MAX_MS : (s.minuteIn == null || s.minuteIn <= 1);
    if (!canonical && !includeLate) { skipped.late++; continue; }
    const gapRaw = (s.learn && isBucket(s.learn.gap)) ? s.learn.gap : bGap(s.rewardPct != null ? Math.abs(s.rewardPct) : null);
    out.push({
      t0: r.t0, asset: r.asset, interval: r.interval,
      symbol: r.asset, mode: s.mode || "na",
      minute: bMinute(s.minuteIn != null ? s.minuteIn : 1),
      rsi: bRsi(s.rsi), vol: bVol(s.volRel2 != null ? s.volRel2 : s.volRel),
      hour: bHour(new Date(r.t0 * 1000).getUTCHours()),
      hist: bHist(s.histStrength),
      trend: (s.learn && s.learn.trend && s.learn.trend !== "na") ? s.learn.trend : "na",
      dir, gap: gapRaw,
      capOffsetMs: off, canonical,
      won: r.res.won === 1 ? 1 : 0,
      touch: r.res.touch === 1 ? 1 : 0,
      mfeFav: r.res.mfeFav, maeFav: r.res.maeFav,
    });
  }
  out.sort((a, b) => a.t0 - b.t0);
  out.skipped = skipped;
  return out;
}

const GATE_FEATS = { interval: (r) => r.interval, symbol: (r) => r.symbol, mode: (r) => r.mode, minute: (r) => r.minute, rsi: (r) => r.rsi, vol: (r) => r.vol, hour: (r) => r.hour, hist: (r) => r.hist, trend: (r) => r.trend };
const TOUCH_FEATS = { interval: (r) => r.interval, symbol: (r) => r.symbol, gap: (r) => r.gap, hour: (r) => r.hour, dir: (r) => r.dir };
const GATE_PAIRS = [["interval", "minute"], ["interval", "vol"], ["rsi", "trend"], ["symbol", "hour"], ["interval", "rsi"], ["hist", "trend"], ["minute", "vol"]];
const TOUCH_PAIRS = [["interval", "gap"], ["symbol", "gap"], ["hour", "gap"], ["dir", "gap"], ["interval", "hour"]];

const groupBy = (rr, kf) => { const m = new Map(); for (const r of rr) { const k = kf(r); if (!m.has(k)) m.set(k, []); m.get(k).push(r); } return m; };
const mkKey = (feats, FEATS) => (r) => feats.map((f) => `${f}=${FEATS[f](r)}`).join("&");

function bucketsOf(train, test, FEATS) {
  const out = {};
  for (const f of Object.keys(FEATS)) {
    if (f === "dir") continue;
    const g1 = groupBy(train, FEATS[f]), g2 = groupBy(test, FEATS[f]);
    out[f] = {};
    for (const [k, rr] of g1) {
      const s = stat(rr, "won"), st = stat(rr, "touch");
      const t = g2.get(k), u = t ? stat(t, "won") : { n: 0, wr: 0, lb: 0, ub: 0 }, u2 = t ? stat(t, "touch") : { n: 0, wr: 0, lb: 0, ub: 0 };
      out[f][k] = { n: s.n, wr: +s.wr.toFixed(4), touch: +st.wr.toFixed(4), nTest: u.n, wrTest: +u.wr.toFixed(4), lbTest: +u.lb.toFixed(4), ubTest: +u.ub.toFixed(4), touchTest: +u2.wr.toFixed(4), touchLbTest: +u2.lb.toFixed(4) };
    }
  }
  return out;
}

function mineRules(train, test, FEATS, PAIRS, base, metric, opts) {
  const MIN_TR = opts.minTrain, MIN_TE = opts.minTest;
  const out = [];
  const run = (feats) => {
    const kf = mkKey(feats, FEATS);
    const g1 = groupBy(train, kf), g2 = groupBy(test, kf);
    for (const [k, rr] of g1) {
      if (rr.length < MIN_TR) continue;
      const t = g2.get(k);
      if (!t || t.length < MIN_TE) continue;
      const s = stat(rr, metric), u = stat(t, metric);
      out.push({ k, n: rr.length, wr: +s.wr.toFixed(4), nTest: t.length, wrTest: +u.wr.toFixed(4), lbTest: +u.lb.toFixed(4), ubTest: +u.ub.toFixed(4), delta: +(u.wr - base).toFixed(4), verdict: u.lb > base ? "boost" : u.ub < base ? "suppress" : "neutral" });
    }
  };
  for (const f of Object.keys(FEATS)) run([f]);
  for (const p of PAIRS) if (p.every((x) => FEATS[x])) run(p);
  out.sort((a, b) => b.delta - a.delta);
  return out;
}

/* ---------- evaluasi model sebagai FILTER (dipakai untuk keputusan promosi) ---------- */
// Aturan penahan yang dipakai app: gate -> HANYA aturan interval tunggal; touch -> HANYA gap tunggal.
function blockersOf(rules, metric) {
  return rules.filter((r) => r.verdict === "suppress" && r.k.indexOf("&") === -1 &&
    (r.metric === "won" ? r.k.indexOf("interval=") === 0 : r.k.indexOf("gap=") === 0));
}
function decide(row, blockers) {
  return !blockers.some((b) => {
    const i = b.k.indexOf("="), f = b.k.slice(0, i), v = b.k.slice(i + 1);
    const rv = f === "gap" ? row.gap : f === "interval" ? row.interval : f === "symbol" ? row.symbol : f === "hour" ? row.hour : f === "dir" ? row.dir : null;
    return rv === v;
  });
}
// skor = winrate dari sinyal yang DIAMBIL x akar(cakupan) — filternya harus berguna,
// bukan sekadar mengambil sedikit sinyal.
function evalModel(rows, gateRules, touchRules) {
  const gb = blockersOf(gateRules.map((r) => ({ ...r, metric: "won" })), "won");
  const tb = blockersOf(touchRules.map((r) => ({ ...r, metric: "touch" })), "touch");
  const taken = rows.filter((r) => decide(r, gb) && decide(r, tb));
  const cov = rows.length ? taken.length / rows.length : 0;
  const wr = taken.length ? mean(taken.map((r) => r.won)) : 0;
  return { n: rows.length, taken: taken.length, coverage: +cov.toFixed(4), takenWinrate: +wr.toFixed(4), score: +(wr * Math.sqrt(cov)).toFixed(4), gateBlockers: gb.map((b) => b.k), touchBlockers: tb.map((b) => b.k) };
}

/* ---------- bangun model dari baris fitur ---------- */
function buildModel(rows, opts = {}) {
  const o = Object.assign({ minTrain: 60, minTest: 40, minRows: 120 }, opts);
  if (rows.length < o.minRows) return { ok: false, reason: `butuh >= ${o.minRows} hasil, ada ${rows.length}`, rows: rows.length };
  const splitIdx = Math.floor(rows.length * 0.7);
  const train = rows.slice(0, splitIdx), test = rows.slice(splitIdx);
  const dirBase = mean(test.map((r) => r.won)), touchBase = mean(test.map((r) => r.touch));
  const dirTrain = mean(train.map((r) => r.won));
  const gateBuckets = bucketsOf(train, test, GATE_FEATS);
  const touchBuckets = bucketsOf(train, test, TOUCH_FEATS);
  const gateRules = mineRules(train, test, GATE_FEATS, GATE_PAIRS, dirBase, "won", o);
  const touchRules = mineRules(train, test, TOUCH_FEATS, TOUCH_PAIRS, touchBase, "touch", o);
  const gateSuppress = gateRules.filter((r) => r.verdict === "suppress").map((r) => r.k);
  const touchSuppress = touchRules.filter((r) => r.verdict === "suppress").map((r) => r.k);
  const metrics = evalModel(test, gateRules, touchRules);
  return {
    ok: true, rows: rows.length, splitIdx,
    baseline: { dirTrain: +dirTrain.toFixed(4), dirTest: +dirBase.toFixed(4), touchTest: +touchBase.toFixed(4) },
    gate: { buckets: gateBuckets, rules: gateRules, suppress: gateSuppress, boost: gateRules.filter((r) => r.verdict === "boost").map((r) => r.k) },
    touch: { buckets: touchBuckets, rules: touchRules, suppress: touchSuppress, boost: touchRules.filter((r) => r.verdict === "boost").map((r) => r.k) },
    lessons: { lessons: lessonsFrom(gateRules, touchRules, test, dirBase, touchBase) },
    metrics,
  };
}

function lessonsFrom(gateRules, touchRules, test, dirBase, touchBase) {
  const L = [];
  for (const r of gateRules.filter((x) => x.verdict === "boost").slice(0, 12)) L.push({ type: "boost", rule: r.k, nTest: r.nTest, wrTest: +r.wrTest.toFixed(4), lbTest: +r.lbTest.toFixed(4), text: `konteks ${r.k}: winrate uji ${(r.wrTest * 100).toFixed(1)}% (LB ${(r.lbTest * 100).toFixed(1)}%, n=${r.nTest})` });
  for (const r of gateRules.filter((x) => x.verdict === "suppress").slice(0, 12)) L.push({ type: "suppress", rule: r.k, nTest: r.nTest, wrTest: +r.wrTest.toFixed(4), ubTest: +r.ubTest.toFixed(4), text: `konteks ${r.k}: winrate uji ${(r.wrTest * 100).toFixed(1)}% (UB ${(r.ubTest * 100).toFixed(1)}%, n=${r.nTest}) — hindari` });
  for (const r of touchRules.filter((x) => x.verdict === "suppress" && x.k.indexOf("&") === -1).slice(0, 8)) L.push({ type: "suppress", rule: r.k, nTest: r.nTest, wrTest: +r.wrTest.toFixed(4), ubTest: +r.ubTest.toFixed(4), text: `peluang kembali ke lock ${r.k}: ${(r.wrTest * 100).toFixed(1)}% (UB ${(r.ubTest * 100).toFixed(1)}%, n=${r.nTest})` });
  // penyebab: fitur yang lebih sering muncul pada sinyal KALAH
  const WIN = test.filter((r) => r.won === 1), LOSE = test.filter((r) => r.won === 0);
  for (const f of Object.keys(GATE_FEATS)) {
    const gw = groupBy(WIN, GATE_FEATS[f]), gl = groupBy(LOSE, GATE_FEATS[f]);
    for (const k of new Set([...gw.keys(), ...gl.keys()])) {
      const pl = ((gl.get(k) || []).length) / (LOSE.length || 1), pw = ((gw.get(k) || []).length) / (WIN.length || 1);
      if (pl - pw >= 0.06 && (gl.get(k) || []).length >= 10) L.push({ type: "cause", feature: f, bucket: k, pLose: +pl.toFixed(4), pWin: +pw.toFixed(4), text: `faktor "${f}=${k}" lebih sering pada sinyal SALAH (${(pl * 100).toFixed(1)}% vs ${(pw * 100).toFixed(1)}%)` });
    }
  }
  return L.slice(0, 40);
}

/* ---------- keputusan promosi: hanya bila MENANG pada jendela uji ---------- */
function shouldPromote(candidate, incumbent, minTake = 40) {
  const c = candidate && candidate.metrics, i = incumbent && incumbent.metrics;
  if (!c) return { promote: false, why: "kandidat tidak valid" };
  if (c.taken < minTake) return { promote: false, why: `sinyal diambil hanya ${c.taken} (< ${minTake}) — bukti belum cukup` };
  if (!i) return { promote: true, why: "belum ada model berjalan → pakai kandidat" };
  if (c.score > i.score) return { promote: true, why: `skor kandidat ${c.score} > insiden ${i.score} (winrate ${(c.takenWinrate * 100).toFixed(1)}% vs ${(i.takenWinrate * 100).toFixed(1)}%, cakupan ${(c.coverage * 100).toFixed(0)}% vs ${(i.coverage * 100).toFixed(0)}%)` };
  return { promote: false, why: `skor kandidat ${c.score} tidak mengalahkan insiden ${i.score}` };
}

module.exports = { wilson, stat, mean, rowsFrom, buildModel, evalModel, shouldPromote, blockersOf, decide, CANONICAL_MAX_MS, GATE_FEATS, TOUCH_FEATS, BUCKETS: { bMinute, bRsi, bVol, bHour, bHist, bGap } };
