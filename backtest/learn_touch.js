/* ============================================================================
   PHASE 1b — LEARNER (play lock-touch): konteks mana yang layak untuk play
   mean-reversion (entry 2s -> kembali ke lock), dan konteks mana yang tidak.
   Data: 7d klines 1s BTC+ETH. Anti-overfit: split berurutan waktu 70/30 + Wilson.
   Output: backtest/out/learn_touch.json
   ============================================================================ */
const fs = require("fs"), path = require("path");
const DATA = path.join(__dirname, "data"), OUT = path.join(__dirname, "out"), DUR = 300;
const load = (s, t) => JSON.parse(fs.readFileSync(path.join(DATA, `${s}_${t}.json`), "utf8"));
const mean = (a) => a.length ? a.reduce((x, y) => x + y, 0) / a.length : 0;
const pct = (a, p) => a.length ? a.slice().sort((x, y) => x - y)[Math.min(a.length - 1, Math.floor(p / 100 * (a.length - 1)))] : 0;
const VOL_TYPICAL = { BTC: 0.515, ETH: 8.22 };
const wilson = (w, n, z = 1.96) => { if (!n) return { lo: 0, hi: 1 }; const p = w / n, d = 1 + z * z / n, c = p + z * z / (2 * n), m = z * Math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)); return { lo: Math.max(0, (c - m) / d), hi: Math.min(1, (c + m) / d) }; };
const stat = (rows, key) => { const n = rows.length, w = rows.reduce((a, r) => a + r[key], 0), { lo, hi } = wilson(w, n); return { n, wr: n ? w / n : 0, lb: lo, ub: hi }; };

/* ---------------- ekstraksi sesi (entry 2s, konservatif) ---------------- */
const BKT = {
  gap: (g) => g < 0.005 ? "<0.005" : g < 0.01 ? "0.005-0.01" : g < 0.02 ? "0.01-0.02" : g < 0.035 ? "0.02-0.035" : ">0.035",
  vol2: (v) => v < 0.9 ? "<0.9" : v < 1.5 ? "0.9-1.5" : v < 3 ? "1.5-3" : ">3",
  surp: (s) => s < 1 ? "<1" : s < 2 ? "1-2" : s < 3 ? "2-3" : ">3",
  hour: (h) => h < 4 ? "0-3" : h < 8 ? "4-7" : h < 12 ? "8-11" : h < 16 ? "12-15" : h < 20 ? "16-19" : "20-23",
  tier: (t) => t, symbol: (s) => s,
};
const FEATS = {
  symbol: (r) => BKT.symbol(r.symbol), tier: (r) => BKT.tier(r.tier), gap: (r) => BKT.gap(r.gap),
  vol2: (r) => BKT.vol2(r.volRel2), surp: (r) => BKT.surp(r.surprise), hour: (r) => BKT.hour(r.hour),
};
const PAIRS = [["tier", "gap"], ["tier", "vol2"], ["gap", "vol2"], ["symbol", "gap"], ["hour", "gap"], ["tier", "hour"], ["surp", "gap"]];

const rows = [];
for (const sym of ["BTC", "ETH"]) {
  const ones = load(sym === "BTC" ? "BTCUSDT" : "ETHUSDT", "1s");
  const five = load(sym, "5m");
  const om = new Map(ones.map((c) => [c.t, c])), oidx = new Map(ones.map((c, i) => [c.t, i]));
  const t0min = ones[0].t, t0max = ones[ones.length - 1].t;
  for (const c of five) {
    const t0 = c.time;
    if (t0 < t0min + 120 || t0 + DUR > t0max) continue;
    const o1 = om.get(t0), o2 = om.get(t0 + 1); if (!o1 || !o2) continue;
    const lock = c.open, C2 = o2.c;
    if (Math.abs(C2 - lock) / lock < 1e-9) continue;
    const prior = five.filter((x) => x.time < t0).slice(-25);
    const baseVol = prior.length ? mean(prior.map((x) => x.vol || 0)) : 0;
    const vol2sum = (o1.v || 0) + (o2.v || 0);
    const volRel2 = baseVol > 0 ? (vol2sum * (DUR / 2)) / baseVol : 1;
    const i0 = oidx.get(t0), pre = (i0 != null && i0 >= 60) ? ones.slice(i0 - 60, i0) : [];
    const sigma1s = pre.length ? mean(pre.map((x) => x.h - x.l)) : 0;
    const surprise = sigma1s > 0 ? Math.abs(C2 - lock) / sigma1s : 0;
    const tier = (volRel2 >= 3 && surprise >= 3) ? "STRONG" : (volRel2 >= 1.5 && surprise >= 2) ? "GOOD" : volRel2 >= 0.9 ? "FAIR" : null;
    if (!tier) continue;
    const prior5 = five.filter((x) => x.time < t0).slice(-50).map((x) => x.vol || 0).filter((v) => v > 0);
    const typ5m = VOL_TYPICAL[sym] * 60;
    if (typ5m > 0 && (vol2sum * (DUR / 2)) < Math.max(pct(prior5, 15), typ5m * 0.3)) continue;
    const dir = C2 < lock ? "up" : "down";
    const fav = (p) => dir === "up" ? (p - C2) / C2 * 100 : (C2 - p) / C2 * 100;
    const ser = [];
    for (let t = t0 + 2; t < t0 + DUR; t++) { const x = om.get(t); if (!x) continue; ser.push(fav(x.c)); }
    if (ser.length < 80) continue;
    const lockFav = fav(lock), gap = lockFav;
    const sma = ser.map((_, i) => mean(ser.slice(Math.max(0, i - 14), i + 1)));
    const touchIdx = ser.findIndex((v) => v >= gap);
    const touched = touchIdx >= 0 ? 1 : 0;
    let trailVal = ser[ser.length - 1];
    if (touched) { let pk = sma[touchIdx]; for (let i = touchIdx; i < sma.length; i++) { if (sma[i] > pk) pk = sma[i]; if (pk - sma[i] >= 0.01) { trailVal = sma[i]; break; } } if (trailVal === 0) trailVal = sma[sma.length - 1]; }
    const v1 = touched ? gap : ser[ser.length - 1];
    const v2 = ser.some((v) => v >= gap + 0.02) ? gap + 0.02 : ser[ser.length - 1];
    const ladder = 0.4 * v1 + 0.3 * v2 + 0.3 * trailVal;
    rows.push({ symbol: sym, tier, gap, volRel2, surprise, hour: new Date(t0 * 1000).getUTCHours(), t0, touched, won: ladder > 0 ? 1 : 0, ladder });
  }
}
rows.sort((a, b) => a.t0 - b.t0);
const splitIdx = Math.floor(rows.length * 0.7);
const TRAIN = rows.slice(0, splitIdx), TEST = rows.slice(splitIdx);
const bT = stat(TRAIN, "touched"), bE = stat(TEST, "touched"), lT = stat(TRAIN, "won"), lE = stat(TEST, "won");

console.log(`=== LEARNER LOCK-TOUCH — n=${rows.length} sinyal gated (7d 1s) ===`);
console.log(`lock-touch  : TRAIN ${(bT.wr * 100).toFixed(1)}% | TEST ${(bE.wr * 100).toFixed(1)}% (LB ${(bE.lb * 100).toFixed(1)}%, n=${bE.n})`);
console.log(`ladder win  : TRAIN ${(lT.wr * 100).toFixed(1)}% | TEST ${(lE.wr * 100).toFixed(1)}% (LB ${(lE.lb * 100).toFixed(1)}%)\n`);

const groupBy = (rr, kf) => { const m = new Map(); for (const r of rr) { const k = kf(r); if (!m.has(k)) m.set(k, []); m.get(k).push(r); } return m; };
const buckets = {};
for (const f of Object.keys(FEATS)) {
  const tr = groupBy(TRAIN, FEATS[f]), te = groupBy(TEST, FEATS[f]);
  buckets[f] = {};
  for (const [k, rr] of tr) { const s = stat(rr, "touched"), s2 = stat(rr, "won"), t = te.get(k) ? stat(te.get(k), "touched") : { n: 0, wr: 0, lb: 0 }, t2 = te.get(k) ? stat(te.get(k), "won") : { n: 0, wr: 0 }; buckets[f][k] = { n: s.n, touch: +s.wr.toFixed(4), ladderWin: +s2.wr.toFixed(4), nTest: t.n, touchTest: +t.wr.toFixed(4), touchLbTest: +t.lb.toFixed(4), ladderWinTest: +t2.wr.toFixed(4) }; }
}
const rules = [];
const evalRule = (feats) => {
  const kf = (r) => feats.map((f) => `${f}=${FEATS[f](r)}`).join("&");
  const tr = groupBy(TRAIN, kf), te = groupBy(TEST, kf);
  for (const [k, rr] of tr) {
    if (rr.length < 60) continue;
    const tt = te.get(k); if (!tt || tt.length < 40) continue;
    const s = stat(rr, "won"), t = stat(tt, "won");
    rules.push({ k, n: rr.length, win: +s.wr.toFixed(4), nTest: tt.length, winTest: +t.wr.toFixed(4), lbTest: +t.lb.toFixed(4), ubTest: +t.ub.toFixed(4), delta: +(t.wr - lE.wr).toFixed(4) });
  }
};
for (const f of Object.keys(FEATS)) evalRule([f]);
for (const [a, b] of PAIRS) evalRule([a, b]);
for (const r of rules) r.verdict = r.lbTest > lE.wr ? "boost" : r.ubTest < lE.wr ? "suppress" : "neutral";
rules.sort((a, b) => b.delta - a.delta);
const boost = rules.filter((r) => r.verdict === "boost"), supp = rules.filter((r) => r.verdict === "suppress");
console.log("--- konteks TERBAIK untuk ladder (lolos uji) ---");
for (const r of boost.slice(0, 10)) console.log(`  ${r.k.padEnd(26)} win ${(r.winTest * 100).toFixed(1)}% (LB ${(r.lbTest * 100).toFixed(1)}%, n=${r.nTest}) vs ${(lE.wr * 100).toFixed(1)}%`);
console.log("--- konteks TERBURUK (suppress) ---");
for (const r of supp.slice(-10).reverse()) console.log(`  ${r.k.padEnd(26)} win ${(r.winTest * 100).toFixed(1)}% (UB ${(r.ubTest * 100).toFixed(1)}%, n=${r.nTest}) vs ${(lE.wr * 100).toFixed(1)}%`);

fs.writeFileSync(path.join(OUT, "learn_touch.json"), JSON.stringify({
  generated: new Date().toISOString(), rows: rows.length,
  baseline: { touchTrain: +bT.wr.toFixed(4), touchTest: +bE.wr.toFixed(4), winTrain: +lT.wr.toFixed(4), winTest: +lE.wr.toFixed(4), winLbTest: +lE.lb.toFixed(4), nTest: lE.n },
  buckets, rules, suppress: supp.map((r) => r.k), boost: boost.map((r) => r.k),
}, null, 1));
console.log(`\n-> ditulis: backtest/out/learn_touch.json`);
