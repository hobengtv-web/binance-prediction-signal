/* Test whether order-flow imbalance (OFI) adds predictive value over the price rule.
   Usage: node backtest/ofi_test.js */
const fs = require("fs");
const path = require("path");
const DATA = path.join(__dirname, "data");
const SYMS = ["BTC", "ETH"];
const MS = { "5m": 300000, "15m": 900000 };
const LOCK_FRAC = 0.6;
const load = (s, t) => JSON.parse(fs.readFileSync(path.join(DATA, `${s}_${t}.json`), "utf8"));
const wr = (a) => a.length ? (a.reduce((x, r) => x + r.won, 0) / a.length * 100).toFixed(1) + "%" : "—";
const pct = (a) => a.length ? (a.reduce((x, r) => x + (r.o >= 0 ? 1 : 0), 0) / a.length * 100).toFixed(1) + "%" : "—";

function ofiMap(sym) {
  const arr = JSON.parse(fs.readFileSync(path.join(DATA, `${sym}_ofi.json`), "utf8"));
  const m = new Map();
  for (const r of arr) m.set(r.t, r);
  return m;
}
function ofiSum(map, fromSec, toSec) {
  let buy = 0, sell = 0, ok = 0, need = 0;
  for (let t = fromSec; t < toSec; t += 60) { need++; const o = map.get(t); if (o) { buy += o.buy; sell += o.sell; ok++; } }
  if (!ok || ok < need) return null;
  const tot = buy + sell;
  return tot > 0 ? (buy - sell) / tot : 0;
}

const OUT = path.join(__dirname, "out");
const out = {};
for (const tf of ["5m", "15m"]) {
  const durSec = MS[tf] / 1000;
  const step = durSec / 60;
  const lockMin = Math.max(1, Math.ceil(LOCK_FRAC * step));   // evaluation minute for CONFIRMED
  const acc = {};
  const push = (k, obj) => { (acc[k] = acc[k] || []).push(obj); };

  for (const sym of SYMS) {
    const one = load(sym, "1m"), tfC = load(sym, tf);
    const oi = new Map(one.map((c, i) => [c.time, i]));
    const om = ofiMap(sym);
    for (const c of tfC) {
      const t0 = c.time;
      if (t0 % durSec !== 0) continue;
      const i1 = oi.get(t0); if (i1 == null) continue;
      const iPrev = i1 + (lockMin - 1);
      if (iPrev >= one.length) continue;
      const lock = c.open;
      const outcome = c.close >= lock ? 1 : 0;

      // PREVIEW features at minute 1
      const pPrice = one[i1].close >= lock ? 1 : 0;
      const pOfi = ofiSum(om, t0, t0 + 60);
      // CONFIRMED features at lockMin
      const cPrice = one[iPrev].close >= lock ? 1 : 0;
      const cOfi = ofiSum(om, t0, t0 + lockMin * 60);
      if (pOfi == null || cOfi == null) continue;

      const o = { won: 0, wpre: pPrice === outcome ? 1 : 0, wconf: cPrice === outcome ? 1 : 0,
                  pDir: pPrice, cDir: cPrice, pOfi, cOfi, outcome };
      const pAgree = (pOfi >= 0) === (pPrice === 1);
      const cAgree = (cOfi >= 0) === (cPrice === 1);
      push("previewBase", { won: o.wpre });
      push("previewOfiOnly", { won: ((pOfi >= 0 ? 1 : 0) === outcome) ? 1 : 0 });
      push(pAgree ? "previewAgree" : "previewDisagree", { won: o.wpre });
      push("confBase", { won: o.wconf });
      push("confOfiOnly", { won: ((cOfi >= 0 ? 1 : 0) === outcome) ? 1 : 0 });
      push(cAgree ? "confAgree" : "confDisagree", { won: o.wconf });
      if (cAgree && Math.abs(cOfi) >= 0.2) push("confAgreeStrong", { won: o.wconf });
      if (pAgree && Math.abs(pOfi) >= 0.2) push("previewAgreeStrong", { won: o.wpre });
    }
  }
  console.log(`\n=== ${tf} (lockMin=${lockMin}) n(base)=${acc.confBase.length} ===`);
  console.log(`  PREVIEW base (price min1)      ${wr(acc.previewBase)}`);
  console.log(`  PREVIEW OFI-only               ${wr(acc.previewOfiOnly)}`);
  console.log(`  PREVIEW price+OFI agree        ${wr(acc.previewAgree)}   (n=${acc.previewAgree.length})`);
  console.log(`  PREVIEW price+OFI agree |OFI|>=0.2  ${wr(acc.previewAgreeStrong)}   (n=${acc.previewAgreeStrong.length})`);
  console.log(`  PREVIEW price+OFI DISagree     ${wr(acc.previewDisagree)}   (n=${acc.previewDisagree.length})`);
  console.log(`  CONFIRMED base (price lockMin) ${wr(acc.confBase)}`);
  console.log(`  CONFIRMED OFI-only             ${wr(acc.confOfiOnly)}`);
  console.log(`  CONFIRMED price+OFI agree      ${wr(acc.confAgree)}   (n=${acc.confAgree.length})`);
  console.log(`  CONFIRMED price+OFI agree |OFI|>=0.2  ${wr(acc.confAgreeStrong)}   (n=${acc.confAgreeStrong.length})`);
  console.log(`  CONFIRMED price+OFI DISagree   ${wr(acc.confDisagree)}   (n=${acc.confDisagree.length})`);
  const W = (a) => a.length ? +(a.reduce((x, r) => x + r.won, 0) / a.length).toFixed(4) : null;
  out[`${tf}|PREVIEW`] = { n: acc.previewAgree.length, wr: W(acc.previewAgree) };
  out[`${tf}|PREVIEW|strong`] = { n: acc.previewAgreeStrong.length, wr: W(acc.previewAgreeStrong) };
  out[`${tf}|CONFIRMED`] = { n: acc.confAgree.length, wr: W(acc.confAgree) };
  out[`${tf}|CONFIRMED|strong`] = { n: acc.confAgreeStrong.length, wr: W(acc.confAgreeStrong) };
}
if (!fs.existsSync(OUT)) fs.mkdirSync(OUT, { recursive: true });
fs.writeFileSync(path.join(OUT, "ofi_tiers.json"), JSON.stringify({ generated: new Date().toISOString(), windowDays: 30, ofi: out }, null, 2));
console.log(`\nWrote out/ofi_tiers.json`);
