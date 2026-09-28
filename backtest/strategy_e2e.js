/* E2E strategi saat ini: gate sinyal 2s (tier + likuiditas) -> entry -> exit (ladder/trail).
   Dua varian entry: (a) langsung di detik ke-2, (b) di puncak contra terkonfirmasi +4s (perilaku TA).
   Output: winrate tahan-ke-akhir-sesi vs early close (lock tersentuh), per tier. BTC+ETH 7d. */
const fs = require("fs"), path = require("path");
const DATA = path.join(__dirname, "data"), DUR = 300;
const load = (s, t) => JSON.parse(fs.readFileSync(path.join(DATA, `${s}_${t}.json`), "utf8"));
const pct = (a, p) => a.length ? a.slice().sort((x, y) => x - y)[Math.min(a.length - 1, Math.floor(p / 100 * (a.length - 1)))] : 0;
const mean = (a) => a.length ? a.reduce((x, y) => x + y, 0) / a.length : 0;
const VOL_TYPICAL = { BTC: 0.515, ETH: 8.22 };
const CONF_DELAY = 4;   // DWELL_ENTRY_MS: entry ~4s after the contra extreme
const DELAYS = process.env.DELAYS ? process.env.DELAYS.split(",").map(Number) : [4];

const tierOf = (v, s) => (v >= 3 && s >= 3) ? "STRONG" : (v >= 1.5 && s >= 2) ? "GOOD" : v >= 0.9 ? "FAIR" : null;

function run(sym) {
  const ones = load(sym === "BTC" ? "BTCUSDT" : "ETHUSDT", "1s");
  const five = load(sym, "5m");
  const om = new Map(ones.map((c) => [c.t, c]));
  const oidx = new Map(ones.map((c, i) => [c.t, i]));
  const t0min = ones[0].t, t0max = ones[ones.length - 1].t;
  const out = [];
  for (const c of five) {
    const t0 = c.time;
    if (t0 < t0min + 120 || t0 + DUR > t0max) continue;
    const o1 = om.get(t0), o2 = om.get(t0 + 1);
    if (!o1 || !o2) continue;
    const lock = c.open, C2 = o2.c;
    if (Math.abs(C2 - lock) / lock < 1e-9) continue;
    // ---- fitur 2s (mirror app.js) ----
    const prior = five.filter((x) => x.time < t0).slice(-25);
    const baseVol = prior.length ? mean(prior.map((x) => x.vol || 0)) : 0;
    const vol2sum = (o1.v || 0) + (o2.v || 0);
    const volRel2 = baseVol > 0 ? (vol2sum * (DUR / 2)) / baseVol : 1;
    const i0 = oidx.get(t0);
    const pre = (i0 != null && i0 >= 60) ? ones.slice(i0 - 60, i0) : [];
    const sigma1s = pre.length ? mean(pre.map((x) => x.h - x.l)) : 0;
    const surprise = sigma1s > 0 ? Math.abs(C2 - lock) / sigma1s : 0;
    const grade = tierOf(volRel2, surprise);
    if (!grade) continue;
    const prior5 = five.filter((x) => x.time < t0).slice(-50).map((x) => x.vol || 0).filter((v) => v > 0);
    const typ5m = VOL_TYPICAL[sym] * 60;
    const liqLow = typ5m > 0 && (vol2sum * (DUR / 2)) < Math.max(pct(prior5, 15), typ5m * 0.3);
    if (liqLow) continue;
    // ---- arah: mean-reversion, harga harus kembali ke lock ----
    const dir = C2 < lock ? "up" : "down";
    const fav = (p) => dir === "up" ? (p - C2) / C2 * 100 : (C2 - p) / C2 * 100;
    const ser = [], px = [];
    for (let t = t0 + 2; t < t0 + DUR; t++) { const x = om.get(t); if (!x) continue; ser.push(fav(x.c)); px.push(x.c); }
    if (ser.length < 80) continue;
    const lockFav = fav(lock);                              // jarak lock (dari harga 2s), %
    let mi = 0; for (let i = 1; i < px.length; i++) if (dir === "up" ? px[i] < px[mi] : px[i] > px[mi]) mi = i;
    const variants = { two_s: 0 };
    for (const d of DELAYS) variants[`peak+${d}s`] = mi + d < ser.length ? mi + d : null;
    for (const [vname, from] of Object.entries(variants)) {
      if (from == null) continue;
      const ser0 = ser.slice(from), base = ser[from];
      const serR = ser0.map((v) => v - base);               // P&L relatif ke harga entry
      if (serR.length < 60) continue;
      const sma = serR.map((_, i) => mean(serR.slice(Math.max(0, i - 14), i + 1)));
      const gap = lockFav - base;                           // jarak entry -> lock (%)
      const closeFav = serR[serR.length - 1];               // P&L bila ditahan sampai akhir sesi
      const touchIdx = serR.findIndex((v) => v >= gap);
      const touched = touchIdx >= 0;
      let trailVal = serR[serR.length - 1], trailIdx = null;
      if (touched) {
        let pk = sma[touchIdx];
        for (let i = touchIdx; i < sma.length; i++) { if (sma[i] > pk) pk = sma[i]; if (pk - sma[i] >= 0.01) { trailVal = sma[i]; trailIdx = i; break; } }
        if (trailIdx == null) trailVal = sma[sma.length - 1];
      }
      const v1 = touched ? gap : closeFav;
      const v2 = serR.some((v) => v >= gap + 0.02) ? gap + 0.02 : closeFav;
      const ladder = 0.4 * v1 + 0.3 * v2 + 0.3 * trailVal;
      out.push({
        sym, grade, vname, gap, touched, ladder, closeFav,
        binaryLock: closeFav >= gap,        // biner: close menembus lock
        earlyClose: touched,                // lock tersentuh -> exit bisa lebih awal
        trailFired: trailIdx != null,
      });
    }
  }
  return out;
}

const all = [...run("BTC"), ...run("ETH")];
const grades = ["STRONG", "GOOD", "FAIR", "ALL"];
const pc = (x, n) => n ? `${(x / n * 100).toFixed(1)}%` : "—";

for (const vname of ["two_s", ...DELAYS.map((d) => `peak+${d}s`)]) {
  const V = all.filter((r) => r.vname === vname);
  console.log(`\n######## ENTRY: ${vname === "two_s" ? "detik ke-2 (langsung sinyal)" : "puncak contra + " + vname.split("+")[1] + " (perilaku TA, optimistis)"} ########`);
  console.log("TIER   n    | lock-touch | AKHIR SESI (biner vs lock)   | EARLY CLOSE (lock tersentuh)   | ladder blend");
  console.log("            |            | win%   P&L>0   cap-med       | n/%    win%   cap-med          | win%   cap-med  E");
  for (const g of grades) {
    const R = V.filter((r) => g === "ALL" || r.grade === g), n = R.length;
    if (!n) continue;
    const ec = R.filter((r) => r.earlyClose), he = R.filter((r) => !r.earlyClose);
    console.log(
      `${g.padEnd(6)} ${String(n).padEnd(4)}| ${pc(R.filter((r) => r.touched).length, n).padEnd(10)} | ` +
      `${pc(R.filter((r) => r.binaryLock).length, n).padEnd(6)} ${pc(he.filter((r) => r.closeFav > 0).length, he.length).padEnd(7)} ${(pct(he.map((r) => r.closeFav), 50).toFixed(3) + "%").padEnd(13)}| ` +
      `${(String(ec.length) + "/" + (ec.length / n * 100).toFixed(0) + "%").padEnd(6)} ${pc(ec.filter((r) => r.ladder > 0).length, ec.length).padEnd(6)} ${(pct(ec.map((r) => r.ladder), 50).toFixed(3) + "%").padEnd(16)}| ` +
      `${pc(R.filter((r) => r.ladder > 0).length, n).padEnd(6)} ${(pct(R.map((r) => r.ladder), 50).toFixed(3) + "%").padEnd(8)}${(mean(R.map((r) => r.ladder)) >= 0 ? "+" : "") + mean(R.map((r) => r.ladder)).toFixed(3)}%`
    );
  }
  const ec = V.filter((r) => r.earlyClose), he = V.filter((r) => !r.earlyClose);
  console.log(`  --- ALL split ---`);
  console.log(`  early close  : ${ec.length} (${(ec.length / V.length * 100).toFixed(1)}%) · ladder>0 ${pc(ec.filter((r) => r.ladder > 0).length, ec.length)} · P&L>0 ${pc(ec.filter((r) => r.ladder > 0).length, ec.length)} · cap-med ${pct(ec.map((r) => r.ladder), 50).toFixed(3)}% · trail fired ${pc(ec.filter((r) => r.trailFired).length, ec.length)}`);
  console.log(`  tahan akhir  : ${he.length} (${(he.length / V.length * 100).toFixed(1)}%) · ladder>0 ${pc(he.filter((r) => r.ladder > 0).length, he.length)} · cap-med ${pct(he.map((r) => r.ladder), 50).toFixed(3)}%`);
  for (const s of ["BTC", "ETH"]) {
    const R = V.filter((r) => r.sym === s);
    console.log(`  ${s}: n=${R.length} lock-touch ${pc(R.filter((r) => r.touched).length, R.length)} biner ${pc(R.filter((r) => r.binaryLock).length, R.length)} ladder>0 ${pc(R.filter((r) => r.ladder > 0).length, R.length)}`);
  }
}
console.log(`\n(gap/touch dihitung dari harga ENTRY ke LOCK; "biner vs lock" = harga close menembus lock = payout biner)`);
