/* Exit simulator + compounding dari hasil bt30 (entries.jsonl).
   - Jalur harga token model (kalibrasi data prediction.jsonl): oddsUp(z)=clamp(0.51+1.05*z,.02,.98), z=(spot-lock)/lock*100.
   - Gate produksi MIN_ODDS/MAX_ODDS = [0.50,0.70] pada harga token sisi entry.
   - Early-close = TA_SIMPLE (arm captured>=50%, trail retrace>=0.12% dari puncak favor, min-hold 10s/20s).
   - PnL: win settle=(1/p_in-1), lose=-1, early=(p_exit/p_in-1); minus FEE.
   Usage: node backtest/bt30exit.js [stakePct] [capUsd] [haircutPct] */
const fs = require("fs");
const path = require("path");
const D = path.join(__dirname, "data30");
const { DUR_S } = require("../capture.js");
const STAKE = Number(process.argv[2] || 0.04);
const CAP = Number(process.argv[3] || 8);
const HAIRCUT = Number(process.argv[4] || 0);
const PREM = Number(process.argv[5] || 0);
const FEE = 0.02, ARM = 50, CB = 0.12, MIN_ODDS = 0.50, MAX_ODDS = 0.70;
const DUR = { "5m": 300, "15m": 900 };
const clamp = (x, a, b) => Math.max(a, Math.min(b, x));
const oddsUp = (z) => clamp(0.51 + 1.05 * z, 0.02, 0.98);
const pOf = (dir, z) => clamp((dir === "up" ? oddsUp(z) : 1 - oddsUp(z)) + PREM, 0.02, 0.98);

const entries = fs.readFileSync(path.join(__dirname, "out30", "entries.jsonl"), "utf8").trim().split("\n").map(JSON.parse).sort((a, b) => a.t0 - b.t0);
const onesBy = {}, idxBy = {};
for (const sym of ["BTC", "ETH", "BNB"]) { const a = JSON.parse(fs.readFileSync(path.join(D, `${sym}_1s.json`), "utf8")); onesBy[sym] = a; idxBy[sym] = new Map(a.map((c, i) => [c.t, i])); }
const fiveBy = {}; for (const sym of ["BTC", "ETH", "BNB"]) { const m = new Map(); for (const c of JSON.parse(fs.readFileSync(path.join(D, `${sym}_5m.json`), "utf8"))) m.set(c.t, c); fiveBy[sym] = m; }

function simulate(r) {
  const sym = r.asset, tf = r.tf, dir = r.dir, isUp = dir === "up";
  const L = (fiveBy[sym].get(r.t0) || {}).o; if (L == null) return null;
  const tIn = r.src === "2s" ? r.t0 + 2 : (r.stage === "late90" ? r.t0 + 90 : r.t0 + 60);
  const o = onesBy[sym], oi = idxBy[sym];
  const i0 = oi.get(tIn); if (i0 == null) return null;
  const C0 = o[i0].c;
  const p_in = pOf(dir, (C0 - L) / L * 100);
  if (p_in < MIN_ODDS || p_in > MAX_ODDS) return { skip: true };   // GATE ODDS produksi
  const contT = isUp ? L * 1.0005 : L * 0.9995;
  const pot = Math.max(Math.abs(contT - C0), C0 * 0.005);
  const MINHOLD = tf === "15m" ? 20000 : 10000;
  let peakFav = -Infinity, armedAt = null, exit = null;
  for (let i = i0; i < o.length; i++) {
    const t = o[i].t; if (t >= r.t0 + (DUR[tf] || 300)) break;
    const px = o[i].c, favor = isUp ? (px - L) : (L - px);
    if (favor > peakFav) peakFav = favor;
    const traveled = isUp ? (px - C0) : (C0 - px);
    const cap = clamp(traveled / pot * 100, -200, 200);
    if (armedAt == null && cap >= ARM) armedAt = t * 1000;
    const trailRetrace = (peakFav - favor) / px * 100;
    if (armedAt != null && trailRetrace >= CB && (t * 1000 - armedAt) >= MINHOLD) { exit = { t, px }; break; }
  }
  let ret, why;
  if (exit) { const p_exit = pOf(dir, (exit.px - L) / L * 100); ret = (p_exit / p_in - 1) * (1 - HAIRCUT / 100); why = "early"; }
  else if (r.win) { ret = (1 / p_in - 1); why = "settleWin"; }
  else { ret = -1; why = "settleLose"; }
  ret -= FEE;
  return { skip: false, p_in, ret, why };
}

const res = [];
for (const r of entries) { const s = simulate(r); if (s && !s.skip) res.push({ ...r, ...s }); }
const nSkip = entries.length - res.length;
console.log(`avg p_in = ${(res.reduce((a,x)=>a+x.p_in,0)/res.length).toFixed(3)}`);
console.log(`entries ${entries.length} | lolos gate odds ${res.length} | ditolak odds ${nSkip}`);
for (const why of ["early", "settleWin", "settleLose"]) { const a = res.filter((x) => x.why === why); console.log(`  ${why}: ${a.length} (${(100 * a.length / res.length).toFixed(1)}%)`); }

// compounding
function comp(stake, cap) {
  let bal = 30, peak = 30, dd = 0;
  for (const r of res) { if (bal < 0.5) break; let st = stake * bal; if (cap > 0) st = Math.min(st, cap); if (st <= 0) continue; bal += st * r.ret; if (bal > peak) peak = bal; const d = (peak - bal) / peak; if (d > dd) dd = d; }
  return { bal, mult: bal / 30, dd: 100 * dd };
}
console.log("\nCOMPOUNDING dari $30:");
for (const [stk, cap, label] of [[0.04, 0, "stake 4%, tanpa cap"], [0.04, 3, "stake 4%, cap $3"], [0.04, 8, "stake 4%, cap $8"], [0.04, 8, null]]) {
  if (!label) continue; const c = comp(stk, cap);
  console.log(`  ${label}: $${c.bal.toFixed(2)} (x${c.mult.toFixed(1)}), maxDD ${c.dd.toFixed(1)}%`);
}
const c8 = comp(0.04, 8);
console.log(`\n=> stake 4% + early-close + CAP $8: $30 -> $${c8.bal.toFixed(2)} (x${c8.mult.toFixed(1)})`);
