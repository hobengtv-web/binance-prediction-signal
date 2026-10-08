/* ============================================================================
   WONBLOCKER CALIBRATION v4 — PER KEY, replicating capture.js matchK logic.
   capture.js:362 matchK uses featVal that includes mv2, mAlignB, mAgreeB, mRanZone, mBody.
   blockersOf/decide from learner.js FILTERS by APPLY_KEYS (excludes mv2) — so we
   replicate matchK locally to match live behavior exactly.
   ============================================================================ */
const fs = require("fs"), path = require("path");
const L = require("../learner.js");

const FILE = process.argv[2] || path.join(__dirname, "..", "ledger", "signals.jsonl");
const raw = fs.readFileSync(FILE, "utf8").split("\n").filter(Boolean).map((l) => JSON.parse(l));
const rows = L.rowsFrom(raw);

// Group per key (matching server.js flow: mineWonBlockers(kr))
const keys = {};
for (const r of rows) {
  if ((r.dir !== "up" && r.dir !== "down") || (r.won !== 0 && r.won !== 1)) continue;
  const k = r.symbol + "_" + r.interval;
  (keys[k] = keys[k] || []).push(r);
}

// featVal builder — IDENTICAL to capture.js:350 (line 350-359)
function featValOf(r) {
  return {
    interval: r.interval, symbol: r.symbol, dir: r.dir, hour: r.hour, gap: r.gap, mode: r.mode,
    rsi: r.rsiB, vol: r.vol, hist: r.hist, trend: r.trend, minute: r.minute,
    mv2: r.mv2B,
    mAlignB: r.mAlignB, mAgreeB: r.mAgreeB, mRanZone: r.mRanZone, mBody: r.mBody,
  };
}

// matchK — replicate capture.js:362-367
function matchK(ruleK, fv) {
  if (typeof ruleK !== "string") return false;
  const parts = ruleK.split("&").map((p) => { const i = p.indexOf("="); return i < 0 ? null : { f: p.slice(0, i), v: p.slice(i + 1) }; }).filter(Boolean);
  if (!parts.length) return false;
  return parts.every((p) => fv[p.f] != null && String(fv[p.f]) === p.v);
}

// wonBlocked check — replicate capture.js:369
function isWonBlocked(r, wonRules) {
  const fv = featValOf(r);
  return wonRules.some((rule) => matchK(rule.k ? rule.k : rule, fv));
}

function evalAggregate(minDelta, minN, covCap, ubMax) {
  let totalBlkW = 0, totalBlkL = 0, totalKeptN = 0, totalKeptW = 0, totalAllN = 0, keysWithRules = 0, totalRules = 0;
  const perKey = {};
  for (const k of Object.keys(keys)) {
    const kr = keys[k];
    if (kr.length < 60) continue;       // mineWonBlockers returns ok:false if <60
    const model = L.mineWonBlockers(kr, { minN, minDelta, covCap, ubMax });
    const wonRules = model.ok ? model.rules : [];
    let blkW = 0, blkL = 0, keptW = 0, keptN = 0;
    for (const r of kr) {
      if (wonRules.length && isWonBlocked(r, wonRules)) { if (r.won === 1) blkW++; else blkL++; }
      else { keptN++; if (r.won === 1) keptW++; }
    }
    totalBlkW += blkW; totalBlkL += blkL; totalKeptW += keptW; totalKeptN += keptN; totalAllN += kr.length;
    if (model.rules.length) { keysWithRules++; totalRules += model.rules.length; }
    if (model.rules.length || blkW + blkL > 0) perKey[k] = {
      n: kr.length, baseWR: ((keys[k].filter(r=>r.won===1).length/kr.length*100).toFixed(1)),
      rules: model.rules.length, blockedFrac: model.blockedFrac ? (model.blockedFrac*100).toFixed(0) : 0,
      blkW, blkL, keptWR: keptN ? (keptW/keptN*100).toFixed(1) : "100", cov: (keptN/kr.length*100).toFixed(0),
    };
  }
  const totalBlkN = totalBlkW + totalBlkL;
  const overallCov = totalAllN ? totalKeptN / totalAllN : 0;
  const overallKeptWR = totalKeptN ? totalKeptW / totalKeptN : 0;
  const overallBaseWR = totalAllN ? (totalBlkW + totalKeptW) / totalAllN : 0;
  const netEv = totalBlkL - totalBlkW;
  return { netEv, totalBlkW, totalBlkL, totalBlkN, totalKeptN, totalKeptW, totalAllN,
    overallCov, overallKeptWR, overallBaseWR, keysWithRules, totalRules, perKey };
}

const minDeltas = [0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.08, 0.10, 0.12];
const minNs = [15, 20, 25, 30, 40, 50];
const covCaps = [0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.5];
const ubMaxs = [0.50, 0.52, 0.55, 0.58, 0.60];

console.log("Scanning param grid...");
const results = [];
for (const mN of minNs) for (const d of minDeltas) for (const ub of ubMaxs) for (const cc of covCaps) {
  const res = evalAggregate(d, mN, cc, ub);
  if (res.totalBlkN === 0) continue;
  results.push({ params: { minN: mN, minDelta: d, ubMax: ub, covCap: cc }, res });
}

// Baseline (current default)
const base = evalAggregate(0.04, 20, 0.5, 0.55);
console.log(`\nBaseline (minN=20, Δ=0.04, ubMax=0.55, covCap=0.5):`);
console.log(`  netEv=${base.netEv} blkW=${base.totalBlkW} blkL=${base.totalBlkL} cov=${(base.overallCov*100).toFixed(0)}% keptWR=${(base.overallKeptWR*100).toFixed(1)}% baseWR=${(base.overallBaseWR*100).toFixed(1)}% keysWRules=${base.keysWithRules} totalRules=${base.totalRules}`);
console.log("  per-key:", JSON.stringify(base.perKey, null, 1));

// Best netEv by covCap
console.log("\n=== Best netEv by covCap ===");
for (const cc of covCaps) {
  const set = results.filter((r) => r.params.covCap === cc);
  const best = set.reduce((a, b) => (b.res.netEv > a.res.netEv ? b : a), { res: { netEv: -999 } });
  if (best.res.netEv === -999) continue;
  const p = best.params, r = best.res;
  console.log(`covCap=${cc}: minN=${p.minN} Δ=${p.minDelta} ubMax=${p.ubMax} → netEv=${r.netEv} blkW=${r.totalBlkW} blkL=${r.totalBlkL} cov=${(r.overallCov*100).toFixed(0)}% keptWR=${(r.overallKeptWR*100).toFixed(1)}% keys=${r.keysWithRules}`);
}

// Top 15 by netEv with coverage floor
for (const covFloor of [0.60, 0.55, 0.50, 0.40]) {
  console.log(`\n=== TOP 10 by netEv (covFloor=${(covFloor*100).toFixed(0)}%) ===`);
  const top = results.filter((r) => r.res.overallCov >= covFloor).sort((a, b) => b.res.netEv - a.res.netEv).slice(0, 10);
  console.log("minN  Δ     ubMax cc  | netEv blkW blkL | cov   keptWR  keys");
  for (const s of top) {
    const p = s.params, r = s.res;
    console.log(`${String(p.minN).padEnd(5)}${p.minDelta.toFixed(3).padEnd(6)}${p.ubMax.toFixed(2).padEnd(6)}${p.covCap.toFixed(2).padEnd(4)} | ${String(r.netEv).padEnd(5)}${r.totalBlkW.toString().padEnd(5)}${r.totalBlkL.toString().padEnd(5)} | ${(r.overallCov*100).toFixed(0).padEnd(5)} ${(r.overallKeptWR*100).toFixed(1).padEnd(8)} ${r.keysWithRules}`);
  }
}

// Most efficient blockers (netEv per blocked trade)
console.log("\n=== TOP 10 by netEvPerBlock (covFloor=50%) ===");
const topEff = results.filter((r) => r.res.overallCov >= 0.50 && r.res.totalBlkN > 0).sort((a, b) => (b.res.netEv/b.res.totalBlkN) - (a.res.netEv/a.res.totalBlkN)).slice(0, 10);
for (const s of topEff) {
  const p = s.params, r = s.res;
  const perBlock = r.netEv / r.totalBlkN;
  console.log(`${String(p.minN).padEnd(5)}${p.minDelta.toFixed(3).padEnd(6)}${p.ubMax.toFixed(2).padEnd(6)}${p.covCap.toFixed(2).padEnd(4)} | netEv/block=${perBlock.toFixed(3)} netEv=${r.netEv} blkW=${r.totalBlkW} blkL=${r.totalBlkL} cov=${(r.overallCov*100).toFixed(0)}%`);
}

// Zero winner-blocking configs
console.log("\n=== ZERO winner-blocking configs (blkW=0, netEv>0, cov>=50%) ===");
const zeroW = results.filter((r) => r.res.overallCov >= 0.50 && r.res.totalBlkW === 0 && r.res.netEv > 0).sort((a, b) => b.res.netEv - a.res.totalNetEv).slice(0, 10);
for (const s of zeroW) {
  const p = s.params, r = s.res;
  console.log(`minN=${p.minN} Δ=${p.minDelta} ubMax=${p.ubMax} cc=${p.covCap} → netEv=${r.netEv} blkL=${r.totalBlkL} cov=${(r.overallCov*100).toFixed(0)}%`);
}
if (zeroW.length === 0) console.log("(none)");

// RECOMMENDED: best netEv with covFloor=55%, weighted by winner-fraction penalty
console.log("\n=== RECOMMENDED ===");
const rec = results.filter((r) => r.res.overallCov >= 0.55 && r.res.netEv > 0)
  .sort((a, b) => {
    const pa = a.res; const pb = b.res;
    // Score: netEv - heavy penalty for blocking winners + coverage bonus
    const sa = pa.netEv - (pa.totalBlkW / (pa.totalBlkN || 1)) * 100 + (pa.overallCov >= 0.6 ? 5 : 0);
    const sb = pb.netEv - (pb.totalBlkW / (pb.totalBlkN || 1)) * 100 + (pb.overallCov >= 0.6 ? 5 : 0);
    return sb - sa;
  })[0];
if (rec) {
  console.log("params:", JSON.stringify(rec.params));
  console.log("result:", JSON.stringify({ ...rec.res, perKey: undefined }, null, 2));
} else {
  console.log("No config met criteria. Showing top netEv regardless:");
  const alt = results.sort((a, b) => b.res.netEv - a.res.netEv).slice(0, 5);
  for (const s of alt) console.log(JSON.stringify(s.params), `netEv=${s.res.netEv} cov=${(s.res.overallCov*100).toFixed(0)}%`);
}
