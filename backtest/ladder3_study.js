/* Bandingkan LADDER PARTIAL EXIT 3-level (entry di detik ke-2, arah menuju lock).
   Trail = SMA15 dengan give-back 0.01% setelah lock tersentuh. */
const fs = require("fs"), path = require("path");
const DATA = path.join(__dirname, "data"), DUR = 300;
const load = (s, t) => JSON.parse(fs.readFileSync(path.join(DATA, `${s}_${t}.json`), "utf8"));
const pct = (a, p) => a.length ? a.slice().sort((x, y) => x - y)[Math.min(a.length - 1, Math.floor(p / 100 * (a.length - 1)))] : 0;
const mean = (a) => a.length ? a.reduce((x, y) => x + y, 0) / a.length : 0;

const R = [];
for (const sym of ["BTC", "ETH"]) {
  const ones = load(sym === "BTC" ? "BTCUSDT" : "ETHUSDT", "1s");
  const five = load(sym, "5m");
  const om = new Map(ones.map((c) => [c.t, c]));
  const t0min = ones[0].t, t0max = ones[ones.length - 1].t;
  for (const c of five) {
    const t0 = c.time;
    if (t0 < t0min || t0 + DUR - 1 > t0max) continue;
    const c2 = om.get(t0 + 1); if (!c2) continue;
    const lock = c.open, entry = c2.c;
    if (Math.abs(entry - lock) / lock < 1e-9) continue;
    const dir = entry < lock ? 1 : -1;
    const gap = Math.abs(lock - entry) / entry * 100;
    const ser = [];
    for (let t = t0 + 2; t < t0 + DUR; t++) {
      const x = om.get(t); if (!x) continue;
      ser.push(dir === 1 ? (x.c - entry) / entry * 100 : (entry - x.c) / entry * 100);
    }
    if (ser.length < 40) continue;
    const sma = ser.map((_, i) => mean(ser.slice(Math.max(0, i - 14), i + 1)));
    R.push({ entry, lock, dir, gap, ser, sma, mfe: Math.max(...ser) });
  }
}
const n = R.length, price = mean(R.map((r) => r.entry)), mfeMed = pct(R.map((r) => r.mfe), 50);

const legTarget = (r, T) => { const tgt = r.gap + T; return r.ser.some((v) => v >= tgt) ? tgt : r.ser[r.ser.length - 1]; };
const legTrail = (r, Tr) => {
  const sm = r.sma;
  const ci = r.ser.findIndex((v) => v >= r.gap);
  if (ci < 0) return r.ser[r.ser.length - 1];
  let pk = sm[ci];
  for (let i = ci; i < sm.length; i++) { if (sm[i] > pk) pk = sm[i]; if (pk - sm[i] >= Tr) return sm[i]; }
  return sm[sm.length - 1];
};

console.log(`\n=== LADDER 3-LEVEL (BTC+ETH n=${n}) ideal cap-med ${mfeMed.toFixed(3)}% ($${(mfeMed / 100 * price).toFixed(2)}) ===`);
console.log("  komposisi                              cap-med ($)        ekspektansi   win%   efisiensi");
const CFG = [
  ["100% lock (1 leg, pembanding)", [1], [0]],
  ["100% trail", [1], ["t"]],
  ["50% lock | 30% +0.01% | 20% trail", [0.5, 0.3, 0.2], [0, 0.01, "t"]],
  ["50% lock | 25% +0.01% | 25% trail", [0.5, 0.25, 0.25], [0, 0.01, "t"]],
  ["40% lock | 30% +0.01% | 30% trail", [0.4, 0.3, 0.3], [0, 0.01, "t"]],
  ["34/33/33 (lock | +0.01% | trail)", [0.34, 0.33, 0.33], [0, 0.01, "t"]],
  ["40% lock | 30% +0.02% | 30% trail", [0.4, 0.3, 0.3], [0, 0.02, "t"]],
  ["50% lock | 30% +0.02% | 20% trail", [0.5, 0.3, 0.2], [0, 0.02, "t"]],
  ["34% lock | 33% +0.02% | 33% trail", [0.34, 0.33, 0.33], [0, 0.02, "t"]],
];
for (const [name, w, t] of CFG) {
  const caps = R.map((r) => w.reduce((acc, wi, i) => acc + wi * (t[i] === "t" ? legTrail(r, 0.01) : legTarget(r, t[i])), 0));
  const med = pct(caps, 50), m = mean(caps), win = caps.filter((x) => x > 0).length / n * 100;
  console.log(`  ${name.padEnd(38)} ${med.toFixed(3)}% ($${(med / 100 * price).toFixed(2)})`.padEnd(56) + `${m >= 0 ? "+" : ""}${m.toFixed(3)}%`.padEnd(14) + `${win.toFixed(1)}%`.padEnd(7) + `${(med / mfeMed * 100).toFixed(0)}%`);
}
