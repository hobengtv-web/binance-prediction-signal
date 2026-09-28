/* ============================================================================
   PHASE 1 — LEARNER: belajar dari 63k sinyal yang SUDAH berlalu.
   Tidak belajar "arah", tapi belajar KONTEKS: konteks mana yang layak dipercaya
   dan konteks mana yang harus di-suppress. Anti-overfit:
     - split berurutan waktu (70% latih -> 30% uji), bukan random
     - Wilson lower/upper bound (bukan winrate mentah)
     - minimal n, dan aturan hanya dipakai bila lolos jendela UJI
   Output: backtest/out/learn_gate.json  +  backtest/out/lessons.json
   ============================================================================ */
const fs = require("fs"), path = require("path");
const OUT = path.join(__dirname, "out");
const A = JSON.parse(fs.readFileSync(path.join(OUT, "signals.json"), "utf8"));
A.sort((a, b) => a.t0 - b.t0);

/* ---------------- bucketing (harus identik dengan yang di app.js) ---------------- */
const BK = {
  interval: (v) => String(v),
  symbol: (v) => String(v),
  minute: (m) => (m <= 1 ? "1" : m <= 3 ? "2-3" : m <= 6 ? "4-6" : "7+"),
  rsi: (r) => (r == null ? "na" : r < 30 ? "<30" : r < 40 ? "30-40" : r < 60 ? "40-60" : r < 70 ? "60-70" : ">70"),
  vol: (v) => (v == null ? "na" : v < 0.7 ? "<0.7" : v < 1.0 ? "0.7-1" : v < 1.5 ? "1-1.5" : v < 2.5 ? "1.5-2.5" : ">2.5"),
  trend: (t) => String(t),
  hour: (h) => (h < 4 ? "0-3" : h < 8 ? "4-7" : h < 12 ? "8-11" : h < 16 ? "12-15" : h < 20 ? "16-19" : "20-23"),
  hist: (s) => (s == null ? "na" : s < 10 ? "<10" : s < 20 ? "10-20" : ">20"),
  mode: (m) => String(m),
};
const FEATS = {
  interval: (r) => BK.interval(r.interval),
  symbol: (r) => BK.symbol(r.symbol),
  minute: (r) => BK.minute(r.minutesIn),
  rsi: (r) => BK.rsi(r.rsi),
  vol: (r) => BK.vol(r.volRel),
  trend: (r) => BK.trend(r.sessTrend),
  hour: (r) => BK.hour(r.hour),
  hist: (r) => BK.hist(r.histStrength),
  mode: (r) => BK.mode(r.mode),
};
const PAIRS = [
  ["interval", "minute"], ["interval", "vol"], ["interval", "trend"], ["interval", "rsi"],
  ["rsi", "trend"], ["symbol", "interval"], ["hour", "interval"], ["vol", "trend"],
  ["minute", "vol"], ["hist", "trend"], ["symbol", "hour"],
];

/* ---------------- statistik ---------------- */
const wilson = (w, n, z = 1.96) => {
  if (!n) return { lo: 0, hi: 1 };
  const p = w / n, d = 1 + (z * z) / n, c = p + (z * z) / (2 * n), m = z * Math.sqrt((p * (1 - p)) / n + (z * z) / (4 * n * n));
  return { lo: Math.max(0, (c - m) / d), hi: Math.min(1, (c + m) / d) };
};
const stat = (rows) => {
  const n = rows.length, w = rows.reduce((a, r) => a + r.won, 0);
  const { lo, hi } = wilson(w, n);
  return { n, wr: n ? w / n : 0, w, lb: lo, ub: hi };
};

/* ---------------- split berurutan waktu 70/30 ---------------- */
const splitIdx = Math.floor(A.length * 0.7);
const TRAIN = A.slice(0, splitIdx), TEST = A.slice(splitIdx);
const bTrain = stat(TRAIN), bTest = stat(TEST);
const days = (A[A.length - 1].t0 - A[0].t0) / 86400;

console.log(`=== LEARNER (Phase 1) — ${A.length} sinyal · ${days.toFixed(1)} hari ===`);
console.log(`baseline TRAIN ${(bTrain.wr * 100).toFixed(2)}% (n=${bTrain.n})  |  TEST ${(bTest.wr * 100).toFixed(2)}% (n=${bTest.n}, LB ${(bTest.lb * 100).toFixed(2)}%)`);
console.log(`split: train t0<=${TRAIN[TRAIN.length - 1].t0} (${new Date(TRAIN[TRAIN.length - 1].t0 * 1000).toISOString().slice(0, 10)}) · test mulai ${new Date(TEST[0].t0 * 1000).toISOString().slice(0, 10)}\n`);

/* ---------------- tabel bucket tunggal ---------------- */
const buckets = {};
const groupBy = (rows, keyFn) => {
  const m = new Map();
  for (const r of rows) { const k = keyFn(r); if (!m.has(k)) m.set(k, []); m.get(k).push(r); }
  return m;
};
for (const f of Object.keys(FEATS)) {
  const tr = groupBy(TRAIN, FEATS[f]), te = groupBy(TEST, FEATS[f]);
  buckets[f] = {};
  for (const [k, rows] of tr) {
    const s = stat(rows), t = te.get(k) ? stat(te.get(k)) : { n: 0, wr: 0, lb: 0, ub: 0 };
    buckets[f][k] = { n: s.n, wr: +s.wr.toFixed(4), nTest: t.n, wrTest: +t.wr.toFixed(4), lbTest: +t.lb.toFixed(4), ubTest: +t.ub.toFixed(4) };
  }
}

/* ---------------- rule mining (1 & 2 fitur) ---------------- */
const MIN_TRAIN = 150, MIN_TEST = 80;
const ruleStats = [];
const evalRule = (feats) => {
  const keyOf = (r) => feats.map((f) => `${f}=${FEATS[f](r)}`).join("&");
  const tr = groupBy(TRAIN, keyOf), te = groupBy(TEST, keyOf);
  for (const [k, rows] of tr) {
    const s = stat(rows);
    if (s.n < MIN_TRAIN) continue;
    const tt = te.get(k);
    if (!tt) continue;
    const t = stat(tt);
    if (t.n < MIN_TEST) continue;
    ruleStats.push({
      k, feats, n: s.n, wr: +s.wr.toFixed(4), lb: +s.lb.toFixed(4),
      nTest: t.n, wrTest: +t.wr.toFixed(4), lbTest: +t.lb.toFixed(4), ubTest: +t.ub.toFixed(4),
    });
  }
};
for (const f of Object.keys(FEATS)) evalRule([f]);
for (const [a, b] of PAIRS) evalRule([a, b]);

// verdict hanya dari jendela UJI
for (const r of ruleStats) {
  r.verdict = r.lbTest > bTest.wr ? "boost" : r.ubTest < bTest.wr ? "suppress" : "neutral";
  r.delta = +(r.wrTest - bTest.wr).toFixed(4);
}
ruleStats.sort((a, b) => b.delta - a.delta);
const boost = ruleStats.filter((r) => r.verdict === "boost");
const suppress = ruleStats.filter((r) => r.verdict === "suppress");

console.log(`--- KONTEKS TERBAIK (lolos uji, Wilson LB di atas baseline) : ${boost.length}`);
for (const r of boost.slice(0, 15)) console.log(`  ${r.k.padEnd(34)} test ${(r.wrTest * 100).toFixed(1)}% (LB ${(r.lbTest * 100).toFixed(1)}%, n=${r.nTest})  vs baseline ${(bTest.wr * 100).toFixed(1)}%`);
console.log(`\n--- KONTEKS TERBURUK yang harus di-SUPPRESS (upper bound di bawah baseline) : ${suppress.length}`);
for (const r of suppress.slice(-15).reverse()) console.log(`  ${r.k.padEnd(34)} test ${(r.wrTest * 100).toFixed(1)}% (UB ${(r.ubTest * 100).toFixed(1)}%, n=${r.nTest})  vs baseline ${(bTest.wr * 100).toFixed(1)}%`);

/* ---------------- "mengapa salah" — profil sinyal yang KALAH ----------------
   Bandingkan distribusi fitur pada sinyal menang vs kalah, lalu cari selisih terbesar. */
console.log(`\n--- MENGAPA SINYAL SALAH (profil menang vs kalah, seluruh data) ---`);
const WIN = A.filter((r) => r.won === 1), LOSE = A.filter((r) => r.won === 0);
const why = [];
for (const f of Object.keys(FEATS)) {
  const gw = groupBy(WIN, FEATS[f]), gl = groupBy(LOSE, FEATS[f]);
  const keys = new Set([...gw.keys(), ...gl.keys()]);
  for (const k of keys) {
    const pw = (gw.get(k) || []).length / WIN.length, pl = (gl.get(k) || []).length / LOSE.length;
    why.push({ f, k, pw, pl, lift: pl - pw, nLose: (gl.get(k) || []).length });
  }
}
why.sort((a, b) => b.lift - a.lift);
for (const w of why.slice(0, 12)) {
  console.log(`  ${(w.f + "=" + w.k).padEnd(22)} hadir di ${(w.pl * 100).toFixed(1)}% sinyal KALAH vs ${(w.pw * 100).toFixed(1)}% sinyal MENANG  (lebih sering ${(w.lift * 100).toFixed(1)}pp · n kalah ${w.nLose})`);
}

/* ---------------- tulis output ---------------- */
fs.writeFileSync(path.join(OUT, "learn_gate.json"), JSON.stringify({
  generated: new Date().toISOString(),
  source: "signals.json", rows: A.length, days: +days.toFixed(2),
  baseline: { train: +bTrain.wr.toFixed(4), test: +bTest.wr.toFixed(4), testLB: +bTest.lb.toFixed(4), nTrain: bTrain.n, nTest: bTest.n },
  minTrain: MIN_TRAIN, minTest: MIN_TEST,
  buckets, rules: ruleStats,
  suppress: suppress.map((r) => r.k), boost: boost.map((r) => r.k),
}, null, 1));

fs.writeFileSync(path.join(OUT, "lessons.json"), JSON.stringify({
  generated: new Date().toISOString(),
  baseline: { train: +bTrain.wr.toFixed(4), test: +bTest.wr.toFixed(4) },
  lessons: [
    ...boost.slice(0, 20).map((r) => ({ type: "boost", rule: r.k, nTest: r.nTest, wrTest: +r.wrTest.toFixed(4), lbTest: +r.lbTest.toFixed(4), text: `konteks ${r.k}: winrate uji ${(r.wrTest * 100).toFixed(1)}% (LB ${(r.lbTest * 100).toFixed(1)}%, n=${r.nTest}) — di atas baseline ${(bTest.wr * 100).toFixed(1)}%` })),
    ...suppress.map((r) => ({ type: "suppress", rule: r.k, nTest: r.nTest, wrTest: +r.wrTest.toFixed(4), ubTest: +r.ubTest.toFixed(4), text: `konteks ${r.k}: winrate uji ${(r.wrTest * 100).toFixed(1)}% (UB ${(r.ubTest * 100).toFixed(1)}%, n=${r.nTest}) — di bawah baseline ${(bTest.wr * 100).toFixed(1)}%, hindari` })),
    ...why.slice(0, 12).map((w) => ({ type: "cause", feature: w.f, bucket: w.k, pLose: +w.pl.toFixed(4), pWin: +w.pw.toFixed(4), text: `faktor "${w.f}=${w.k}" lebih sering muncul pada sinyal SALAH (${(w.pl * 100).toFixed(1)}%) daripada benar (${(w.pw * 100).toFixed(1)}%)` })),
  ],
}, null, 1));

console.log(`\n-> ditulis: backtest/out/learn_gate.json (${boost.length} boost, ${suppress.length} suppress) & lessons.json`);
