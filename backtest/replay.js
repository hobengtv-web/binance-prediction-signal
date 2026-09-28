/* Walk-forward replay of the signal engine over Binance klines.
   Usage: node backtest/replay.js
   No lookahead:
   - 1m candles = intra-session path + session structure
   - tf candles (5m/15m/1h) = historical trend from COMPLETED candles before session
   - 5m candles = RSI from COMPLETED candles
   - evaluate each completed 1m step from session start; take FIRST non-flat verdict (session lock)
   - lock = open of first 1m candle in session; outcome = last 1m close vs lock
   Feature levels (must all be reproducible by live app from completed candles):
     L0 = tf|mode|dir
     L1 = + rsiBucket + strengthBucket
     L2 = + volBucket   (vol is resolution-sensitive; kept only for reference)
*/
const fs = require("fs");
const path = require("path");
const Core = require("../signal-core.js");

const DATA = path.join(__dirname, "data");
const OUT = path.join(__dirname, "out");
const SYMBOLS = ["BTC", "ETH"];
const TFS = ["5m", "15m", "1h"];
const MS = Core.INTERVAL_MS;

const load = (sym, tf) => JSON.parse(fs.readFileSync(path.join(DATA, `${sym}_${tf}.json`), "utf8"));
const lowerBound = (arr, t) => {
  let lo = 0, hi = arr.length;
  while (lo < hi) { const m = (lo + hi) >> 1; if (arr[m].time < t) lo = m + 1; else hi = m; }
  return lo;
};
const rsiBucket = (r) => r == null ? "na" : r < 30 ? "<30" : r < 40 ? "30-40" : r <= 60 ? "40-60" : r <= 70 ? "60-70" : ">70";
const volBucket = (v) => v < 1.05 ? "<1.05" : v < 1.3 ? "1.05-1.3" : v < 2 ? "1.3-2" : ">=2";
const strBucket = (s) => s < 35 ? "<35" : s < 50 ? "35-50" : s < 70 ? "50-70" : ">=70";
const hourBlock = (h) => `${Math.floor(h / 4) * 4}h`;

function buildRecords() {
  const records = [];
  for (const sym of SYMBOLS) {
    const one = load(sym, "1m");
    const five = load(sym, "5m");
    const byTf = { "5m": five, "15m": load(sym, "15m"), "1h": load(sym, "1h") };

    for (const tf of TFS) {
      const durSec = MS[tf] / 1000;
      const step = durSec / 60;
      const tfCandles = byTf[tf];

      for (let p = 0; p < one.length; p++) {
        const t0 = one[p].time;
        if (t0 % durSec !== 0) continue;
        const endSec = t0 + durSec;
        const intra = [];
        for (let q = p; q < one.length && one[q].time < endSec; q++) intra.push(one[q]);
        if (intra.length !== step) continue;

        const lock = intra[0].open;
        const k = lowerBound(tfCandles, t0);
        const tfArr = (tfCandles[k] && tfCandles[k].time === t0) ? tfCandles.slice(0, k + 1) : tfCandles.slice(0, k);
        if (tfArr.length < 51) continue;
        const histTrend = Core.analyzeHistoricalTrend(tfArr, 50);
        const sessTrend = Core.sessionTrend(tfArr, 3);

        const p1 = lowerBound(one, t0);
        const baseVols = one.slice(Math.max(0, p1 - 25), p1).map((c) => c.vol);
        const baseVol = baseVols.length ? baseVols.reduce((a, b) => a + b, 0) / baseVols.length : 1;

        let sig = null;
        for (let j = 1; j <= intra.length; j++) {
          const nowSec = intra[j - 1].time + 60;
          const C = intra[j - 1].close;
          const currentDir = C > lock ? "up" : C < lock ? "down" : "flat";
          const firstCandleDir = intra[0].close > intra[0].open ? "bullish" : "bearish";
          const w = intra.slice(Math.max(0, j - 5), j).map((c) => c.vol);
          const volRel = (w.reduce((a, b) => a + b, 0) / w.length) / (baseVol || 1);
          const fi = lowerBound(five, nowSec);
          const rsi = Core.rsiFromSeries(five.slice(0, fi).slice(-50), 14);
          const res = Core.decideSignal({ tf, elapsed: (nowSec - t0) * 1000, histTrend, firstCandleDir, currentDir, volRel, rsi });
          if (res.verdict !== "flat") { sig = { ...res, minutesIn: Math.round((nowSec - t0) / 60), rsi, volRel }; break; }
        }

        const lastClose = intra[intra.length - 1].close;
        const actual = lastClose >= lock ? "up" : "down";
        if (!sig) continue;
        records.push({
          symbol: sym, interval: tf, t0,
          dir: sig.verdict, mode: sig.mode, conf: sig.conf,
          won: sig.verdict === actual ? 1 : 0, actual,
          minutesIn: sig.minutesIn,
          rsi: sig.rsi != null ? +sig.rsi.toFixed(1) : null,
          volRel: +sig.volRel.toFixed(3),
          histStrength: histTrend.strength, histMomentum: histTrend.momentum, histDir: histTrend.predictDir,
          sessTrend, hour: new Date(t0 * 1000).getUTCHours(),
        });
      }
    }
  }
  records.sort((a, b) => a.t0 - b.t0);
  return records;
}

function keyOf(r, level) {
  let k = `${r.interval}|${r.mode}|${r.dir}`;
  if (level >= 1) k += `|rsi:${rsiBucket(r.rsi)}|str:${strBucket(r.histStrength)}`;
  if (level >= 2) k += `|vol:${volBucket(r.volRel)}`;
  return k;
}

function buildTable(rows, level) {
  const m = new Map();
  for (const r of rows) {
    const k = keyOf(r, level);
    let o = m.get(k);
    if (!o) { o = { key: k, n: 0, wins: 0 }; m.set(k, o); }
    o.n++; o.wins += r.won;
  }
  for (const o of m.values()) { o.wr = o.wins / o.n; o.lb = Core.wilsonLowerBound(o.wins, o.n); }
  return m;
}

function walkForward(records, level, days) {
  const splitIdx = Math.floor(records.length * 0.7);
  const train = records.slice(0, splitIdx);
  const test = records.slice(splitIdx);
  const table = buildTable(train, level);
  const rows = [];
  for (const minN of [20, 30, 50, 100]) {
    for (const lbMin of [0.5, 0.6, 0.7, 0.8, 0.9]) {
      const good = new Set();
      for (const o of table.values()) if (o.n >= minN && o.lb >= lbMin) good.add(o.key);
      const sel = test.filter((r) => good.has(keyOf(r, level)));
      const sw = sel.reduce((a, r) => a + r.won, 0);
      rows.push({
        minN, lbMin, n: sel.length,
        coverage: test.length ? sel.length / test.length : 0,
        winrate: sel.length ? sw / sel.length : 0,
        perDay: days ? sel.length / days : 0,
      });
    }
  }
  return { table, rows };
}

function main() {
  if (!fs.existsSync(OUT)) fs.mkdirSync(OUT, { recursive: true });
  const t0ms = Date.now();
  const records = buildRecords();
  const days = (records[records.length - 1].t0 - records[0].t0) / 86400;
  const wins = records.reduce((a, r) => a + r.won, 0);
  console.log(`\n=== BASELINE ===`);
  console.log(`signals: ${records.length}  days: ${days.toFixed(1)}  per day: ${(records.length / days).toFixed(1)}`);
  console.log(`overall winrate: ${(wins / records.length * 100).toFixed(1)}%  (Wilson LB ${(Core.wilsonLowerBound(wins, records.length) * 100).toFixed(1)}%)`);

  const byTfMode = new Map();
  for (const r of records) {
    const k = `${r.interval}|${r.mode}`;
    const o = byTfMode.get(k) || { n: 0, w: 0 };
    o.n++; o.w += r.won; byTfMode.set(k, o);
  }
  console.log(`\n=== PER INTERVAL / MODE ===`);
  for (const [k, o] of [...byTfMode.entries()].sort((a, b) => b[1].n - a[1].n)) {
    console.log(`  ${k.padEnd(20)} n=${String(o.n).padStart(5)}  wr=${(o.w / o.n * 100).toFixed(1)}%  lb=${(Core.wilsonLowerBound(o.w, o.n) * 100).toFixed(1)}%`);
  }

  for (const level of [1, 2]) {
    const { table, rows } = walkForward(records, level, days);
    console.log(`\n=== WALK-FORWARD (level ${level}) ===`);
    console.log(`minN  LB>=   selected  coverage   testWR   perDay`);
    for (const x of rows) {
      if (x.n >= 20) console.log(`  ${String(x.minN).padStart(3)}  ${x.lbMin.toFixed(2)}  ${String(x.n).padStart(8)}  ${(x.coverage * 100).toFixed(1).padStart(6)}%  ${(x.winrate * 100).toFixed(1).padStart(6)}%  ${x.perDay.toFixed(1)}`);
    }
    const top = [...table.values()].filter((o) => o.n >= 30).sort((a, b) => b.wr - a.wr).slice(0, 15);
    console.log(`  -- top conditions (n>=30) --`);
    for (const o of top) console.log(`     n=${String(o.n).padStart(4)}  wr=${(o.wr * 100).toFixed(1).padStart(5)}%  lb=${(o.lb * 100).toFixed(1).padStart(5)}%  ${o.key}`);
  }

  const l1 = buildTable(records, 1);
  const gate = [...l1.values()].filter((o) => o.n >= 30 && o.lb >= 0.6)
    .map((o) => ({ key: o.key, n: o.n, wr: +o.wr.toFixed(4), lb: +o.lb.toFixed(4) }))
    .sort((a, b) => b.lb - a.lb);
  fs.writeFileSync(path.join(OUT, "signals.json"), JSON.stringify(records));
  fs.writeFileSync(path.join(OUT, "gate.json"), JSON.stringify({ generated: new Date().toISOString(), days: +days.toFixed(2), baseline: +(wins / records.length).toFixed(4), total: records.length, gate }, null, 2));
  console.log(`\nWrote out/signals.json, out/gate.json  (${((Date.now() - t0ms) / 1000).toFixed(1)}s)  gate conditions: ${gate.length}`);
}

main();
