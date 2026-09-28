/* ============================================================================
   PHASE 1c — LEARNER LOCK-TOUCH versi 90 HARI (klines 1m, n ~ puluhan ribu).
   Pertanyaan: "kalau harga menyimpang dari lock, seberapa besar peluang kembali ke
   lock?" — dipecah per konteks (interval, jarak gap, volume, RSI, tren, jam, aset).
   Entry = akhir candle 1m pertama sesi. Anti-overfit: split waktu 70/30 + Wilson.
   Output: backtest/out/learn_touch90.json
   ============================================================================ */
const fs = require("fs"), path = require("path");
const DATA = path.join(__dirname, "data"), OUT = path.join(__dirname, "out");
const load = (s, t) => JSON.parse(fs.readFileSync(path.join(DATA, `${s}_${t}.json`), "utf8"));
const mean = (a) => a.length ? a.reduce((x, y) => x + y, 0) / a.length : 0;
const wilson = (w, n, z = 1.96) => { if (!n) return { lo: 0, hi: 1 }; const p = w / n, d = 1 + z * z / n, c = p + z * z / (2 * n), m = z * Math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)); return { lo: Math.max(0, (c - m) / d), hi: Math.min(1, (c + m) / d) }; };
const stat = (rows, k) => { const n = rows.length, w = rows.reduce((a, r) => a + r[k], 0), { lo, hi } = wilson(w, n); return { n, wr: n ? w / n : 0, lb: lo, ub: hi }; };
const TF = { "5m": 300, "15m": 900 };

const rows = [];
for (const sym of ["BTC", "ETH"]) {
  const one = load(sym, "1m");
  const byTf = { "5m": load(sym, "5m"), "15m": load(sym, "15m") };
  for (const tf of Object.keys(TF)) {
    const durSec = TF[tf], tfCandles = byTf[tf];
    for (const c of tfCandles) {
      const t0 = c.time, endSec = t0 + durSec;
      const intra = one.filter((x) => x.time >= t0 && x.time < endSec);
      if (intra.length !== durSec / 60) continue;
      const prev = one.filter((x) => x.time < t0).slice(-26);
      if (prev.length < 26) continue;
      const lock = intra[0].open, C = intra[0].close;           // entry = akhir menit ke-1
      if (!lock || Math.abs(C - lock) / lock < 1e-9) continue;
      const dir = C < lock ? "up" : "down";
      const gap = Math.abs(lock - C) / C * 100;
      // touched bila sisa sesi menyentuh kembali lock
      const rest = intra.slice(1);
      const hit = rest.some((x) => dir === "up" ? x.high >= lock : x.low <= lock);
      const closeBeyond = dir === "up" ? intra[intra.length - 1].close >= lock : intra[intra.length - 1].close <= lock;
      const base5 = mean(prev.slice(0, 25).map((x) => x.vol));
      const last5 = mean(prev.slice(21).map((x) => x.vol));
      const volRel = base5 > 0 ? last5 / base5 : 1;
      const hh = Math.max(...intra.map((x) => x.high)), ll = Math.min(...intra.map((x) => x.low));
      rows.push({
        symbol: sym, interval: tf, t0, gap, dir, touched: hit ? 1 : 0, binary: closeBeyond ? 1 : 0,
        volRel: +volRel.toFixed(3), rsi: null, trend: null, hour: new Date(t0 * 1000).getUTCHours(),
        // jarak minimum untuk menyentuh lock (proxy kesulitan)
      });
    }
  }
}
rows.sort((a, b) => a.t0 - b.t0);
console.log(`=== LEARNER LOCK-TOUCH 90d (klines 1m) — n=${rows.length} sesi ===`);

const splitIdx = Math.floor(rows.length * 0.7);
const TRAIN = rows.slice(0, splitIdx), TEST = rows.slice(splitIdx);
const bT = stat(TRAIN, "touched"), bE = stat(TEST, "touched"), bBT = stat(TEST, "binary");
console.log(`lock-touch : TRAIN ${(bT.wr * 100).toFixed(1)}% | TEST ${(bE.wr * 100).toFixed(1)}% (LB ${(bE.lb * 100).toFixed(1)}%, n=${bE.n})`);
console.log(`close>lock : TEST ${(bBT.wr * 100).toFixed(1)}% (kalah dari touch -> exit di lock tetap kunci)\n`);

const BKT = {
  interval: (v) => String(v), symbol: (v) => String(v),
  gap: (g) => g < 0.005 ? "<0.005" : g < 0.01 ? "0.005-0.01" : g < 0.02 ? "0.01-0.02" : g < 0.035 ? "0.02-0.035" : ">0.035",
  vol: (v) => v < 0.7 ? "<0.7" : v < 1.0 ? "0.7-1" : v < 1.5 ? "1-1.5" : v < 2.5 ? "1.5-2.5" : ">2.5",
  hour: (h) => h < 4 ? "0-3" : h < 8 ? "4-7" : h < 12 ? "8-11" : h < 16 ? "12-15" : h < 20 ? "16-19" : "20-23",
  dir: (d) => String(d),
};
const FEATS = { interval: (r) => BKT.interval(r.interval), symbol: (r) => BKT.symbol(r.symbol), gap: (r) => BKT.gap(r.gap), vol: (r) => BKT.vol(r.volRel), hour: (r) => BKT.hour(r.hour), dir: (r) => BKT.dir(r.dir) };
const PAIRS = [["interval", "gap"], ["gap", "vol"], ["symbol", "gap"], ["interval", "vol"], ["hour", "gap"], ["dir", "gap"], ["interval", "hour"]];

const groupBy = (rr, kf) => { const m = new Map(); for (const r of rr) { const k = kf(r); if (!m.has(k)) m.set(k, []); m.get(k).push(r); } return m; };
const buckets = {};
for (const f of Object.keys(FEATS)) {
  const tr = groupBy(TRAIN, FEATS[f]), te = groupBy(TEST, FEATS[f]);
  buckets[f] = {};
  for (const [k, rr] of tr) {
    const s = stat(rr, "touched"), t = te.get(k) ? stat(te.get(k), "touched") : { n: 0, wr: 0, lb: 0, ub: 0 };
    buckets[f][k] = { n: s.n, touch: +s.wr.toFixed(4), nTest: t.n, touchTest: +t.wr.toFixed(4), lbTest: +t.lb.toFixed(4), ubTest: +t.ub.toFixed(4) };
  }
}
const rules = [];
const evalRule = (feats) => {
  const kf = (r) => feats.map((f) => `${f}=${FEATS[f](r)}`).join("&");
  const tr = groupBy(TRAIN, kf), te = groupBy(TEST, kf);
  for (const [k, rr] of tr) {
    if (rr.length < 300) continue;
    const tt = te.get(k); if (!tt || tt.length < 150) continue;
    const s = stat(rr, "touched"), t = stat(tt, "touched");
    rules.push({ k, n: rr.length, touch: +s.wr.toFixed(4), nTest: tt.length, touchTest: +t.wr.toFixed(4), lbTest: +t.lb.toFixed(4), ubTest: +t.ub.toFixed(4), delta: +(t.wr - bE.wr).toFixed(4) });
  }
};
for (const f of Object.keys(FEATS)) evalRule([f]);
for (const [a, b] of PAIRS) evalRule([a, b]);
for (const r of rules) r.verdict = r.lbTest > bE.wr ? "boost" : r.ubTest < bE.wr ? "suppress" : "neutral";
rules.sort((a, b) => b.delta - a.delta);
const boost = rules.filter((r) => r.verdict === "boost"), supp = rules.filter((r) => r.verdict === "suppress");
console.log(`--- konteks peluang KEMBALI KE LOCK terbaik (lolos uji) : ${boost.length}`);
for (const r of boost.slice(0, 12)) console.log(`  ${r.k.padEnd(28)} touch ${(r.touchTest * 100).toFixed(1)}% (LB ${(r.lbTest * 100).toFixed(1)}%, n=${r.nTest}) vs ${(bE.wr * 100).toFixed(1)}%`);
console.log(`--- konteks TERBURUK : ${supp.length}`);
for (const r of supp.slice(-12).reverse()) console.log(`  ${r.k.padEnd(28)} touch ${(r.touchTest * 100).toFixed(1)}% (UB ${(r.ubTest * 100).toFixed(1)}%, n=${r.nTest}) vs ${(bE.wr * 100).toFixed(1)}%`);

fs.writeFileSync(path.join(OUT, "learn_touch90.json"), JSON.stringify({
  generated: new Date().toISOString(), rows: rows.length,
  baseline: { touchTrain: +bT.wr.toFixed(4), touchTest: +bE.wr.toFixed(4), touchLbTest: +bE.lb.toFixed(4), nTest: bE.n, closeBeyondTest: +bBT.wr.toFixed(4) },
  buckets, rules, suppress: supp.map((r) => r.k), boost: boost.map((r) => r.k),
}, null, 1));
console.log(`\n-> ditulis: backtest/out/learn_touch90.json`);
