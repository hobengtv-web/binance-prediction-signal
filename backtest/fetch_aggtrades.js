/* Download Binance spot aggTrades daily dumps and aggregate per minute (order-flow).
   Usage: node backtest/fetch_aggtrades.js [days]
   Output: backtest/data/{SYM}_ofi.json  -> [{t, buy, sell, n}]  (t = minute start, seconds) */
const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");
const readline = require("readline");

const SYMS = { BTC: "BTCUSDT", ETH: "ETHUSDT" };
const OUT = path.join(__dirname, "data");
const TMP = path.join(require("os").tmpdir(), "aggzips");
const HOST = "https://data.binance.vision/data/spot/daily/aggTrades";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const dstr = (d) => d.toISOString().slice(0, 10);

async function download(url, file) {
  const res = await fetch(url);
  if (!res.ok) return false;
  const buf = Buffer.from(await res.arrayBuffer());
  fs.writeFileSync(file, buf);
  return true;
}

function aggregateZip(zipFile, entryName) {
  return new Promise((resolve, reject) => {
    const perMin = new Map();
    // Extract ONLY the aggTrades member (the zip also contains a metrics CSV).
    const un = spawn("unzip", ["-p", zipFile, entryName]);
    const rl = readline.createInterface({ input: un.stdout });
    rl.on("line", (ln) => {
      if (!ln) return;
      const p = ln.split(",");
      if (p.length < 7) return;
      const qty = +p[2], raw = +p[5], m = p[6].toLowerCase() === "true";
      if (!isFinite(qty) || !isFinite(raw)) return;
      // Timestamps arrive in microseconds in current dumps; detect the unit robustly.
      const sec = raw > 1e15 ? Math.floor(raw / 1e6) : raw > 1e12 ? Math.floor(raw / 1e3) : Math.floor(raw);
      const min = Math.floor(sec / 60) * 60;
      let o = perMin.get(min);
      if (!o) { o = { buy: 0, sell: 0, n: 0 }; perMin.set(min, o); }
      if (m) o.sell += qty; else o.buy += qty;
      o.n++;
    });
    un.on("error", reject);
    rl.on("close", () => resolve([...perMin.entries()].map(([t, o]) => ({ t, buy: +o.buy.toFixed(4), sell: +o.sell.toFixed(4), n: o.n }))));
  });
}

(async () => {
  const days = Number(process.argv[2] || 30);
  if (!fs.existsSync(OUT)) fs.mkdirSync(OUT, { recursive: true });
  if (!fs.existsSync(TMP)) fs.mkdirSync(TMP, { recursive: true });
  const today = new Date(Date.now() - 2 * 86400000); // dumps lag ~1 day
  for (const [sym, binSym] of Object.entries(SYMS)) {
    const all = [];
    let ok = 0;
    for (let i = 0; i < days; i++) {
      const d = new Date(today.getTime() - i * 86400000);
      const ds = dstr(d);
      const url = `${HOST}/${binSym}/${binSym}-aggTrades-${ds}.zip`;
      const zip = path.join(TMP, `${binSym}-${ds}.zip`);
      try {
        if (!(await download(url, zip))) { console.log(`  ${binSym} ${ds}: 404`); continue; }
        const mins = await aggregateZip(zip, `${binSym}-aggTrades-${ds}.csv`);
        for (const m of mins) all.push(m);
        ok++;
        fs.unlinkSync(zip);
        if (ok % 5 === 0) console.log(`  ${binSym}: ${ok} days, ${all.length} minutes`);
      } catch (e) { console.log(`  ${binSym} ${ds}: ${e.message}`); }
      await sleep(120);
    }
    all.sort((a, b) => a.t - b.t);
    fs.writeFileSync(path.join(OUT, `${sym}_ofi.json`), JSON.stringify(all));
    console.log(`${sym}: ${ok} days -> ${all.length} minutes -> ${sym}_ofi.json`);
  }
})();
