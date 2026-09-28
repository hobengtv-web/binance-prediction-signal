/* Kenapa "tahan ke akhir sesi" 43% sedangkan versi produksi lama 68%?
   Uji: winrate arah (vs lock) sebagai fungsi WAKTU sinyal. BTC+ETH, 7d 1s klines.
   - CONTINUATION = close searah harga saat sinyal (ini yang dipakai versi lama / replay.js)
   - REVERSION    = kebalikannya (yang dipakai strategi contra-lock sekarang)   */
const fs = require("fs"), path = require("path");
const DATA = path.join(__dirname, "data"), DUR = 300;
const load = (s, t) => JSON.parse(fs.readFileSync(path.join(DATA, `${s}_${t}.json`), "utf8"));
const mean = (a) => a.length ? a.reduce((x, y) => x + y, 0) / a.length : 0;
const pc = (x, n) => n ? x / n * 100 : 0;
const VOL_TYPICAL = { BTC: 0.515, ETH: 8.22 };
const ES = [2, 3, 5, 10, 20, 30, 45, 60, 90, 120, 180, 240];

const rows = [];
for (const sym of ["BTC", "ETH"]) {
  const ones = load(sym === "BTC" ? "BTCUSDT" : "ETHUSDT", "1s");
  const five = load(sym, "5m");
  const om = new Map(ones.map((c) => [c.t, c]));
  const oidx = new Map(ones.map((c, i) => [c.t, i]));
  const t0min = ones[0].t, t0max = ones[ones.length - 1].t;
  for (const c of five) {
    const t0 = c.time;
    if (t0 < t0min + 120 || t0 + DUR > t0max) continue;
    const lock = c.open;
    if (!om.get(t0)) continue;
    const actual = c.close >= lock ? "up" : "down";
    // gate (sama seperti sinyal 2s sekarang)
    const prior = five.filter((x) => x.time < t0).slice(-25);
    const baseVol = prior.length ? mean(prior.map((x) => x.vol || 0)) : 0;
    const o1 = om.get(t0), o2 = om.get(t0 + 1);
    const vol2sum = o1 && o2 ? (o1.v || 0) + (o2.v || 0) : 0;
    const volRel2 = baseVol > 0 ? (vol2sum * (DUR / 2)) / baseVol : 1;
    const i0 = oidx.get(t0);
    const pre = (i0 != null && i0 >= 60) ? ones.slice(i0 - 60, i0) : [];
    const sigma1s = pre.length ? mean(pre.map((x) => x.h - x.l)) : 0;
    const surprise = sigma1s > 0 && o2 ? Math.abs(o2.c - lock) / sigma1s : 0;
    const prior5 = five.filter((x) => x.time < t0).slice(-50).map((x) => x.vol || 0).filter((v) => v > 0);
    const typ5m = VOL_TYPICAL[sym] * 60;
    const sorted = prior5.slice().sort((a, b) => a - b);
    const p15 = sorted.length ? sorted[Math.floor(0.15 * (sorted.length - 1))] : 0;
    const liqOK = !(typ5m > 0 && (vol2sum * (DUR / 2)) < Math.max(p15, typ5m * 0.3));
    const grade = !liqOK ? null : (volRel2 >= 3 && surprise >= 3) ? "STRONG" : (volRel2 >= 1.5 && surprise >= 2) ? "GOOD" : volRel2 >= 0.9 ? "FAIR" : null;
    const r = { sym, grade };
    for (const e of ES) {
      const x = om.get(t0 + e - 1);
      if (!x) { r["e" + e] = null; continue; }
      const dir = x.c > lock ? "up" : x.c < lock ? "down" : null;
      r["e" + e] = dir == null ? null : (dir === actual ? 1 : 0);   // 1 = continuation menang
    }
    rows.push(r);
  }
}

const show = (label, R) => {
  console.log(`\n${label} (n=${R.length})`);
  console.log("  waktu sinyal : " + ES.map((e) => String(e + "s").padStart(6)).join(""));
  const cont = ES.map((e) => pc(R.filter((r) => r["e" + e] === 1).length, R.filter((r) => r["e" + e] != null).length));
  const nAt = ES.map((e) => R.filter((r) => r["e" + e] != null).length);
  console.log("  continuation : " + cont.map((v) => (v.toFixed(1) + "%").padStart(6)).join(""));
  console.log("  reversion    : " + cont.map((v) => ((100 - v).toFixed(1) + "%").padStart(6)).join(""));
  console.log("  n            : " + nAt.map((v) => String(v).padStart(6)).join(""));
};

console.log("=== WINRATE ARAH vs WAKTU SINYAL — BTC+ETH, 7d 1s ===");
console.log("(continuation = close searah harga saat sinyal; reversion = sebaliknya = arah strategi contra-lock)");
show("SEMUA sesi (tanpa gate)", rows);
for (const g of ["STRONG", "GOOD", "FAIR"]) show(`GATED tier ${g}`, rows.filter((r) => r.grade === g));
show("GATED (semua tier)", rows.filter((r) => r.grade));
