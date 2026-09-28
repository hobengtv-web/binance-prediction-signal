/* Download Binance spot klines for backtesting. Usage: node backtest/fetch.js [days] */
const fs = require("fs");
const path = require("path");

const SYMBOLS = { BTC: "BTCUSDT", ETH: "ETHUSDT" };
const INTERVALS = ["1m", "5m", "15m", "1h"];
const HOST = "https://data-api.binance.vision";
const OUT_DIR = path.join(__dirname, "data");

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function getJSON(url, tries) {
  tries = tries || 6;
  for (let i = 0; i < tries; i++) {
    try {
      const res = await fetch(url);
      if (res.status === 429 || res.status === 418) { await sleep(2000 * (i + 1)); continue; }
      if (!res.ok) throw new Error("HTTP " + res.status);
      return await res.json();
    } catch (e) {
      if (i === tries - 1) throw e;
      await sleep(800 * (i + 1));
    }
  }
}

async function fetchKlines(binSym, interval, days) {
  const startMs = Date.now() - days * 86400000;
  let endTime = Date.now();
  const out = [];
  let req = 0;
  while (endTime > startMs) {
    const url = `${HOST}/api/v3/klines?symbol=${binSym}&interval=${interval}&limit=1000&endTime=${endTime}`;
    const rows = await getJSON(url);
    if (!rows || !rows.length) break;
    for (const r of rows) {
      out.push({ time: Math.floor(r[0] / 1000), open: +r[1], high: +r[2], low: +r[3], close: +r[4], vol: +r[5] });
    }
    endTime = rows[0][0] - 1;
    req++;
    if (req % 20 === 0) process.stdout.write(`    ${binSym} ${interval}: ${out.length}\n`);
    await sleep(110);
  }
  out.sort((a, b) => a.time - b.time);
  const seen = new Set();
  const dedup = [];
  for (const c of out) { if (seen.has(c.time)) continue; seen.add(c.time); dedup.push(c); }
  return dedup.filter((c) => c.time * 1000 >= startMs);
}

(async () => {
  const days = Number(process.argv[2] || 90);
  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });
  console.log(`Downloading ${days} days to ${OUT_DIR}`);
  for (const [sym, binSym] of Object.entries(SYMBOLS)) {
    for (const tf of INTERVALS) {
      const data = await fetchKlines(binSym, tf, days);
      const file = path.join(OUT_DIR, `${sym}_${tf}.json`);
      fs.writeFileSync(file, JSON.stringify(data));
      console.log(`  ${sym} ${tf}: ${data.length} candles -> ${path.basename(file)}`);
    }
  }
  console.log("Done.");
})();
