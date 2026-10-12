require("./envprod.js");
const fs = require("fs"), path = require("path");
const Core = require("../signal-core.js");
const MODELS = path.join(__dirname, "..", "models");
const DATA = path.join(__dirname, "data30");
const MODELDIR = fs.readdirSync(MODELS).filter((d) => d.startsWith("v-")).sort().pop();
const MFILE = { gate: "gates.json", veto: "learn_veto.json", mv60: "learn_mv60.json", mv2: "learn_mv2.json", ta: "learn_ta.json", spread: "learn_spread.json", sizing: "learn_sizing.json", touch: "learn_touch90.json", wonblock: "learn_wonblock.json", score: "learn_score.json" };
const getModel = (p) => { try { return JSON.parse(fs.readFileSync(path.join(MODELS, MODELDIR, MFILE[p]), "utf8")); } catch (e) { return null; } };
const profile = getModel("gate");
const { computeSignal } = require("../capture.js");
const load = (f) => JSON.parse(fs.readFileSync(path.join(DATA, f), "utf8"));
const lb = (a, t) => { let lo = 0, hi = a.length; while (lo < hi) { const m = (lo + hi) >> 1; if (a[m].t < t) lo = m + 1; else hi = m; } return lo; };
const sl = (a, x, y) => a.slice(lb(a, x), lb(a, y));
const ones = load("BTC_1s.json"), one1m = load("BTC_1m.json"), five = load("BTC_5m.json");
const tf5 = load("BTC_5m.json").map((c) => ({ time: c.t, open: c.o, high: c.h, low: c.l, close: c.c, vol: c.v }));
const tf15 = load("BTC_15m.json").map((c) => ({ time: c.t, open: c.o, high: c.h, low: c.l, close: c.c, vol: c.v }));
const tf1h = load("BTC_1h.json").map((c) => ({ time: c.t, open: c.o, high: c.h, low: c.l, close: c.c, vol: c.v }));
const now = ones[ones.length - 1].t, cut = now - 86400;
const tally = {}; let n = 0, acc = 0;
for (let idx = 26; idx < tf5.length - 1; idx++) {
  const t0 = tf5[idx].time; if (t0 < cut) continue; if (t0 + 300 > now) break;
  const o1 = ones[lb(ones, t0)]; const o2 = ones[lb(ones, t0 + 1)]; if (!o1 || o1.t !== t0 || !o2 || o2.t !== t0 + 1) continue;
  const win1s = sl(ones, t0 - 60, t0 + 2).map((c) => ({ time: c.t, open: c.o, high: c.h, low: c.l, close: c.c, vol: c.v }));
  const onesLong = sl(ones, t0 - 9600, t0).map((c) => ({ time: c.t, close: c.c }));
  const f5 = sl(five, t0 - 50 * 300, t0).map((c) => ({ time: c.t, open: c.o, high: c.h, low: c.l, close: c.c, vol: c.v }));
  const m1 = sl(one1m, t0 - 3600, t0).map((c) => ({ time: c.t, open: c.o, high: c.h, low: c.l, close: c.c, vol: c.v }));
  const align = {}; for (const [t, arr] of [["5m", tf5], ["15m", tf15], ["1h", tf1h]]) { const a = arr.filter((c) => c.time < t0); if (a.length >= 20) align[t] = Core.analyzeHistoricalTrend(a, 50).predictDir; }
  let s; try { s = computeSignal({ sym: "BTC", tf: "5m", t0, tfc: tf5, idx, ones: win1s, onesLong, five5m: f5, one1m: m1, profile, getModel, SignalCore: Core, nowSec: t0, align }); } catch (e) { s = { skipped: "err:" + e.message }; }
  const g = s.signal || {};
  n++; const key = s.skipped ? ("skip:" + s.skipped) : (g.accepted ? ("ACC " + (g.grade || "")) : ("rej:" + (g.reject || "?")));
  tally[key] = (tally[key] || 0) + 1; if (g.accepted) acc++;
}
console.log("sessions:", n, "accepted:", acc);
console.log(Object.entries(tally).sort((a, b) => b[1] - a[1]).slice(0, 20).map(([k, v]) => k + " " + v).join("\n"));

// dump sample non-skipped sigs
(() => {
  let shown = 0;
  for (let idx = 26; idx < tf5.length - 1 && shown < 2; idx++) {
    const t0 = tf5[idx].time; if (t0 < cut || t0 + 300 > now) continue;
    const o1 = ones[lb(ones, t0)], o2 = ones[lb(ones, t0 + 1)]; if (!o1 || o1.t !== t0 || !o2 || o2.t !== t0 + 1) continue;
    const win1s = sl(ones, t0 - 60, t0 + 2).map((c) => ({ time: c.t, open: c.o, high: c.h, low: c.l, close: c.c, vol: c.v }));
    const onesLong = sl(ones, t0 - 9600, t0).map((c) => ({ time: c.t, close: c.c }));
    const f5 = sl(five, t0 - 50 * 300, t0).map((c) => ({ time: c.t, open: c.o, high: c.h, low: c.l, close: c.c, vol: c.v }));
    const m1 = sl(one1m, t0 - 3600, t0).map((c) => ({ time: c.t, open: c.o, high: c.h, low: c.l, close: c.c, vol: c.v }));
    const align = {}; for (const [t, arr] of [["5m", tf5], ["15m", tf15], ["1h", tf1h]]) { const a = arr.filter((c) => c.time < t0); if (a.length >= 20) align[t] = Core.analyzeHistoricalTrend(a, 50).predictDir; }
    let s; try { s = computeSignal({ sym: "BTC", tf: "5m", t0, tfc: tf5, idx, ones: win1s, onesLong, five5m: f5, one1m: m1, profile, getModel, SignalCore: Core, nowSec: t0, align }); } catch (e) { s = { skipped: "err:" + e.message }; }
    if (!s.skipped && !s.accepted) { console.log("SAMPLE t0", new Date(t0 * 1000).toISOString(), JSON.stringify(s).slice(0, 500)); shown++; }
  }
})();
