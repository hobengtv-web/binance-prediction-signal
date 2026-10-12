/* Fetch 30 hari klines spot (data-api) untuk BTC/ETH/BNB.
   Format 1s: {t,o,h,l,c,v} ; tf lain: {t,o,h,l,c,v}. Simpan ke backtest/data30/<SYM>_<tf>.json
   Usage: node backtest/fetch30.js [days] */
const fs = require("fs");
const path = require("path");
const HOST = "https://data-api.binance.vision";
const SYMS = { BTC: "BTCUSDT", ETH: "ETHUSDT", BNB: "BNBUSDT" };
const TFS = ["1s", "1m", "5m", "15m", "1h"];
const DAYS = Number(process.argv[2] || 30);
const OUT = path.join(__dirname, "data30");
fs.mkdirSync(OUT, { recursive: true });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function get(url, tries) {
  tries = tries || 6;
  for (let i = 0; i < tries; i++) {
    try {
      const res = await fetch(url);
      if (res.status === 429 || res.status === 418) { await sleep(1500 * (i + 1)); continue; }
      if (!res.ok) throw new Error("HTTP " + res.status);
      return await res.json();
    } catch (e) { if (i === tries - 1) throw e; await sleep(500 * (i + 1)); }
  }
}

async function fetchTf(bin, tf) {
  const startMs = Date.now() - DAYS * 86400000;
  let endTime = Date.now();
  const out = [];
  let req = 0;
  while (endTime > startMs) {
    const url = `${HOST}/api/v3/klines?symbol=${bin}&interval=${tf}&limit=1000&endTime=${endTime}`;
    const rows = await get(url);
    if (!rows || !rows.length) break;
    for (const r of rows) out.push({ t: Math.floor(r[0] / 1000), o: +r[1], h: +r[2], l: +r[3], c: +r[4], v: +r[5] });
    endTime = rows[0][0] - 1;
    req++;
    if (rows.length < 1000) break;
    if (req % 40 === 0) process.stdout.write(`  ${bin} ${tf}: ${out.length} candles...\n`);
  }
  out.sort((a, b) => a.t - b.t);
  return out;
}

(async () => {
  const t0 = Date.now();
  for (const sym of Object.keys(SYMS)) {
    for (const tf of TFS) {
      const f = path.join(OUT, `${sym}_${tf}.json`);
      if (fs.existsSync(f) && Date.now() - fs.statSync(f).mtimeMs < 6 * 3600e3) { console.log("skip (fresh):", f); continue; }
      const arr = await fetchTf(SYMS[sym], tf);
      fs.writeFileSync(f, JSON.stringify(arr));
      console.log("saved", f, arr.length, "candles", new Date(arr[0].t * 1000).toISOString(), "->", new Date(arr[arr.length - 1].t * 1000).toISOString());
    }
  }
  console.log("DONE in", ((Date.now() - t0) / 1000).toFixed(0), "s");
})();
