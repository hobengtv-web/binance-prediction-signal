/* Backtest 30 hari — LOGIC PRODUKSI APA ADANYA.
   Entry = apa yang benar-benar memicu bot: sinyal 2s (computeSignal, lengkap gate+veto+model) ATAU
   late60/late90 (logika engine.latePass: mv60>=model + tradeable + jam OFF).
   Hasil = "predict-fun" up/down = candle sesi (open vs close) [settle Chainlink ~= spot].
   PnL = model payout binary pada harga token p (odds) — p TIDAK tersedia historis, jadi dilaporkan
   untuk beberapa p. Fee/slippage mengikuti env bot (FEE, SLIPPAGE_BPS).
   Usage: node backtest/bt30.js [assets] [tfs] [limitDays] */
const fs = require("fs");
const path = require("path");
require("./envprod.js");   // env produksi (gate/TA) — WAJIB agar logic identik
const Core = require("../signal-core.js");
const MODELS = path.join(__dirname, "..", "models");
const DATA = path.join(__dirname, "data30");
const DUR_S = { "5m": 300, "15m": 900, "1h": 3600 };

const MODELDIR = fs.readdirSync(MODELS).filter((d) => d.startsWith("v-")).sort().pop();
const MFILE = { gate: "gates.json", veto: "learn_veto.json", mv60: "learn_mv60.json", mv2: "learn_mv2.json", ta: "learn_ta.json", spread: "learn_spread.json", sizing: "learn_sizing.json", touch: "learn_touch90.json", wonblock: "learn_wonblock.json", score: "learn_score.json" };
function getModel(part) { try { return JSON.parse(fs.readFileSync(path.join(MODELS, MODELDIR, MFILE[part]), "utf8")); } catch (_) { return null; } }
const profile = getModel("gate");
const { computeSignal } = require("../capture.js");
const TRADE = require("../trade-plan.js");

const load = (f) => JSON.parse(fs.readFileSync(path.join(DATA, f), "utf8"));
const lowerBound = (arr, t) => { let lo = 0, hi = arr.length; while (lo < hi) { const m = (lo + hi) >> 1; if (arr[m].t < t) lo = m + 1; else hi = m; } return lo; };
const sliceT = (arr, a, b) => arr.slice(lowerBound(arr, a), lowerBound(arr, b));
const arrTfc = (arr) => arr.map((c) => ({ time: c.t, open: c.o, high: c.h, low: c.l, close: c.c, vol: c.v }));

const FEE = Number(process.env.FEE || 0.02);
const ASSETS = (process.argv[2] || "BTC,ETH,BNB").split(",");
const TFS = (process.argv[3] || "5m,15m").split(",");
const LIMIT_DAYS = Number(process.argv[4] || 0);   // 0 = all

const rows = [];
function processAsset(sym) {
  const ones = load(`${sym}_1s.json`);
  const one1mA = load(`${sym}_1m.json`);
  const five5mA = load(`${sym}_5m.json`);
  const tfA = {}; for (const tf of ["5m", "15m", "1h"]) tfA[tf] = arrTfc(load(`${sym}_${tf}.json`));
  const nowT = ones[ones.length - 1].t;
  const minT = LIMIT_DAYS ? nowT - LIMIT_DAYS * 86400 : 0;

  for (const tf of TFS) {
    const tfc = tfA[tf];
    if (!tfc) continue;
    const mv60 = getModel("mv60"), veto = getModel("veto");
    for (let idx = 26; idx < tfc.length - 1; idx++) {
      const t0 = tfc[idx].time;
      if (t0 < minT) continue;
      if (t0 + DUR_S[tf] > nowT) break;
      const key = `${sym}_${tf}`;
      // --- input computeSignal (persis engine.lockSession) ---
      const o1 = ones[lowerBound(ones, t0)]; const o2 = ones[lowerBound(ones, t0 + 1)];
      if (!o1 || o1.t !== t0 || !o2 || o2.t !== t0 + 1) continue;
      const win1s = sliceT(ones, t0 - 60, t0 + 2).map((c) => ({ time: c.t, open: c.o, high: c.h, low: c.l, close: c.c, vol: c.v }));
      const onesLong = sliceT(ones, t0 - 9600, t0).map((c) => ({ time: c.t, close: c.c }));
      const five5m = sliceT(five5mA, t0 - 50 * 300, t0).map((c) => ({ time: c.t, open: c.o, high: c.h, low: c.l, close: c.c, vol: c.v }));
      const one1m = sliceT(one1mA, t0 - 60 * 60, t0).map((c) => ({ time: c.t, open: c.o, high: c.h, low: c.l, close: c.c, vol: c.v }));
      // align per tf (engine: analyzeHistoricalTrend dari candle < now)
      const align = {};
      for (const t of ["5m", "15m", "1h"]) { const a = tfA[t].filter((c) => c.time < t0); if (a.length >= 20) align[t] = Core.analyzeHistoricalTrend(a, 50).predictDir; }
      let sRaw;
      try { sRaw = computeSignal({ sym, tf, t0, tfc, idx, ones: win1s, onesLong, five5m, one1m, profile, getModel, SignalCore: Core, nowSec: t0, align }); } catch (e) { sRaw = { skipped: "err:" + e.message }; }
      const S = (sRaw && sRaw.signal) ? sRaw.signal : null;

      let dir = null, src = null, stage = null, mv = null;
      if (S && S.accepted && (S.dir === "up" || S.dir === "down")) { dir = S.dir; src = "2s"; stage = S.grade || "2s"; }
      else if (tf === "5m" && sRaw && (sRaw.skipped === "flat-noise" || sRaw.skipped === "flat-price")) {
        // LATE (engine.latePass): mv60/mv90 vs LOCK
        const lock = tfc[idx].open;
        const byk = (m, k) => (m && m.byKey && m.byKey[k]) ? m.byKey[k] : null;
        const thr = (byk(mv60, key) && typeof byk(mv60, key).minMv60 === "number") ? byk(mv60, key).minMv60 : 0.02;
        const prof = byk(veto && veto.prof, key);
        const tradeable = !(prof && prof.tradeable === false);
        const hrs = (byk(veto, key) && byk(veto, key).hours) || [];
        const wibH = Math.floor(((t0 + 7 * 3600) % 86400) / 3600);
        if (tradeable && hrs.indexOf(wibH) < 0) {
          for (const sec of [60, 90]) {
            const pxc = ones[lowerBound(ones, t0 + sec) - 1];
            if (!pxc || pxc.t < t0 || pxc.t > t0 + sec) continue;
            const px = pxc.c; const m = Math.abs(px - lock) / lock * 100;
            if (m >= thr) { dir = px > lock ? "up" : "down"; src = "late"; stage = "late" + sec; mv = m; break; }
          }
        }
      }
      if (!dir) continue;
      const c = tfc[idx];
      const outcome = c.close > c.open ? "up" : "down";   // predict-fun up/down
      const win = dir === outcome;
      rows.push({ key, asset: sym, tf, t0, hour: Math.floor(((t0 + 7 * 3600) % 86400) / 3600), src, stage, dir, outcome, win, mv });
    }
    process.stdout.write(`${sym} ${tf}: sessions done\n`);
  }
}

for (const sym of ASSETS) processAsset(sym);
fs.mkdirSync(path.join(__dirname, "out30"), { recursive: true });
fs.writeFileSync(path.join(__dirname, "out30", "entries.jsonl"), rows.map((r) => JSON.stringify(r)).join("\n"));
console.log("TOTAL entries:", rows.length, "| model:", MODELDIR);
