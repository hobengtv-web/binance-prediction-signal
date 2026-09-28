/* ============================================================================
   PHASE 3 (pondasi) — LEARNER DARI LEDGER LIVE.
   Membaca ledger produksi (data yang benar-benar terjadi, bukan replay) lalu
   menjalankan metodologi yang sama: split 70/30 berurutan waktu + Wilson bound.
   Output: backtest/out/learn_live.json (JANGAN menimpa tabel offline) + perbandingan.
   Pemakaian:
     node backtest/learn_live.js                          (ambil dari localhost:8000)
     node backtest/learn_live.js --url http://host/api/ledger?dump=1
     node backtest/learn_live.js --file ledger/signals.jsonl
   ============================================================================ */
const fs = require("fs"), path = require("path");
const OUT = path.join(__dirname, "out");
const argv = process.argv.slice(2);
const argOf = (n) => { const i = argv.indexOf(n); return i >= 0 ? argv[i + 1] : null; };
const URL_ARG = argOf("--url") || "http://localhost:8000/api/ledger?dump=1";
const FILE_ARG = argOf("--file");
const MIN_T0 = 1700000000;                 // buang record probe/sampah

const wilson = (w, n, z = 1.96) => { if (!n) return { lo: 0, hi: 1 }; const p = w / n, d = 1 + z * z / n, c = p + z * z / (2 * n), m = z * Math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)); return { lo: Math.max(0, (c - m) / d), hi: Math.min(1, (c + m) / d) }; };
const stat = (rows, key) => { const n = rows.length, w = rows.reduce((a, r) => a + r[key], 0), { lo, hi } = wilson(w, n); return { n, wr: n ? w / n : 0, lb: lo, ub: hi }; };

async function loadLedger() {
  if (FILE_ARG) {
    const txt = fs.readFileSync(FILE_ARG, "utf8");
    if (FILE_ARG.endsWith(".jsonl")) return txt.split("\n").filter(Boolean).map((l) => JSON.parse(l));
    const j = JSON.parse(txt);
    return Array.isArray(j) ? j : (j.records || []);
  }
  const res = await fetch(URL_ARG);
  const j = await res.json();
  return j.records || [];
}

const BKT = {
  minute: (m) => m <= 1 ? "1" : m <= 3 ? "2-3" : m <= 6 ? "4-6" : "7+",
  rsi: (r) => (r == null ? "na" : r < 30 ? "<30" : r < 40 ? "30-40" : r < 60 ? "40-60" : r < 70 ? "60-70" : ">70"),
  vol: (v) => (v == null ? "na" : v < 0.7 ? "<0.7" : v < 1.0 ? "0.7-1" : v < 1.5 ? "1-1.5" : v < 2.5 ? "1.5-2.5" : ">2.5"),
  hour: (h) => (h == null ? "na" : h < 4 ? "0-3" : h < 8 ? "4-7" : h < 12 ? "8-11" : h < 16 ? "12-15" : h < 20 ? "16-19" : "20-23"),
  hist: (s) => (s == null ? "na" : s < 10 ? "<10" : s < 20 ? "10-20" : ">20"),
  gap: (g) => (g == null ? "na" : g < 0.005 ? "<0.005" : g < 0.01 ? "0.005-0.01" : g < 0.02 ? "0.01-0.02" : g < 0.035 ? "0.02-0.035" : ">0.035"),
};
const groupBy = (rr, kf) => { const m = new Map(); for (const r of rr) { const k = kf(r); if (!m.has(k)) m.set(k, []); m.get(k).push(r); } return m; };

async function main() {
  const raw = await loadLedger();
  const recs = raw.filter((r) => r && r.sig && r.t0 >= MIN_T0);
  const done = recs.filter((r) => r.res).sort((a, b) => a.t0 - b.t0);
  console.log(`=== LEARNER LIVE (dari ledger) ===`);
  console.log(`record: ${raw.length} · valid ${recs.length} · sudah ada hasil ${done.length}`);
  if (done.length < 30) {
    console.log(`\nBelum cukup data untuk belajar (butuh >= 30 hasil, sekarang ${done.length}).`);
    console.log(`Ledger mengumpulkan otomatis dari sinyal nyata; jalankan lagi setelah beberapa jam.`);
    fs.writeFileSync(path.join(OUT, "learn_live.json"), JSON.stringify({ generated: new Date().toISOString(), records: recs.length, withRes: done.length, ready: false }, null, 1));
    return;
  }
  const rows = done.map((r) => ({
    t0: r.t0, symbol: r.asset, interval: r.interval, mode: r.sig.mode,
    minute: BKT.minute(r.sig.minuteIn), rsi: BKT.rsi(r.sig.rsi), vol: BKT.vol(r.sig.volRel2 != null ? r.sig.volRel2 : r.sig.volRel),
    hour: BKT.hour(new Date(r.t0 * 1000).getUTCHours()), hist: BKT.hist(r.sig.histStrength),
    trend: (r.sig.learn && r.sig.learn.trend) || "na", dir: r.sig.dir,
    gap: BKT.gap(r.sig.learn && r.sig.learn.gap ? gapPctOf(r.sig.learn.gap) : r.sig.rewardPct),
    won: r.res.won === 1 ? 1 : 0, touch: r.res.touch === 1 ? 1 : 0,
    mfeFav: r.res.mfeFav, maeFav: r.res.maeFav, tTouchSec: r.res.tTouchSec,
  }));
  const splitIdx = Math.floor(rows.length * 0.7);
  const TR = rows.slice(0, splitIdx), TE = rows.slice(splitIdx);
  const bt = stat(TR, "won"), be = stat(TE, "won"), tt = stat(TE, "touch");
  console.log(`baseline arah : TRAIN ${(bt.wr * 100).toFixed(1)}% | TEST ${(be.wr * 100).toFixed(1)}% (LB ${(be.lb * 100).toFixed(1)}%, n=${be.n})`);
  console.log(`baseline touch: TEST ${(tt.wr * 100).toFixed(1)}% (n=${tt.n})`);

  const FEATS = { interval: (r) => r.interval, symbol: (r) => r.symbol, mode: (r) => r.mode, minute: (r) => r.minute, rsi: (r) => r.rsi, vol: (r) => r.vol, hour: (r) => r.hour, hist: (r) => r.hist, trend: (r) => r.trend, gap: (r) => r.gap };
  const rules = [];
  const addRule = (feats, key, base) => {
    const kf = (r) => feats.map((f) => `${f}=${FEATS[f](r)}`).join("&");
    const g1 = groupBy(TR, kf), g2 = groupBy(TE, kf);
    for (const [k, rr] of g1) {
      const t = g2.get(k);
      if (rr.length < 30 || !t || t.length < 15) continue;
      const s = stat(rr, key), u = stat(t, key);
      rules.push({ k, n: rr.length, wr: +s.wr.toFixed(4), nTest: t.length, wrTest: +u.wr.toFixed(4), lbTest: +u.lb.toFixed(4), ubTest: +u.ub.toFixed(4), delta: +(u.wr - base).toFixed(4), metric: key });
    }
  };
  for (const f of Object.keys(FEATS)) { addRule([f], "won", be.wr); if (f === "gap" || f === "interval" || f === "hour") addRule([f], "touch", tt.wr); }
  for (const p of [["interval", "gap"], ["tier", "gap"], ["gap", "vol"], ["symbol", "gap"], ["interval", "minute"], ["rsi", "trend"]]) if (p.every((x) => FEATS[x])) addRule(p, "won", be.wr);
  rules.sort((a, b) => b.delta - a.delta);
  console.log(`\naturan lolos uji: ${rules.length}`);
  for (const r of rules.slice(0, 10)) console.log(`  ${r.metric.padEnd(5)} ${r.k.padEnd(30)} test ${(r.wrTest * 100).toFixed(1)}% (LB ${(r.lbTest * 100).toFixed(1)}%, n=${r.nTest})`);
  console.log("\ncontoh penyebab kalah (rata-rata fitur pada sinyal salah vs benar):");
  const WIN = rows.filter((r) => r.won === 1), LOSE = rows.filter((r) => r.won === 0);
  const num = (a, f) => { const v = a.map((r) => r[f]).filter((x) => typeof x === "number" && isFinite(x)); return v.length ? (v.reduce((x, y) => x + y, 0) / v.length).toFixed(3) : "—"; };
  for (const f of ["mfeFav", "maeFav", "tTouchSec"]) console.log(`  ${f.padEnd(9)} menang ${num(WIN, f)} · kalah ${num(LOSE, f)}`);

  fs.writeFileSync(path.join(OUT, "learn_live.json"), JSON.stringify({
    generated: new Date().toISOString(), ready: true, records: recs.length, withRes: done.length,
    baseline: { dirTrain: +bt.wr.toFixed(4), dirTest: +be.wr.toFixed(4), dirLbTest: +be.lb.toFixed(4), touchTest: +tt.wr.toFixed(4) },
    rules, rulesTested: rules.length,
  }, null, 1));
  console.log(`\n-> ditulis: backtest/out/learn_live.json`);
}
function gapPctOf(bucket) {  // bucket -> titik tengah, untuk fitur numerik learner
  return { "<0.005": 0.002, "0.005-0.01": 0.007, "0.01-0.02": 0.015, "0.02-0.035": 0.027, ">0.035": 0.05 }[bucket] || null;
}
main().catch((e) => { console.error("gagal:", e.message); process.exit(1); });
