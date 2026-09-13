/* Local Node server: serves static files + /api/snapshot (polled) + /api/stream (SSE, realtime).
   The SSE endpoint opens a Binance trade stream on the SERVER and pushes every trade to the
   browser, so price moves are realtime (not once-per-second). Falls back to REST polling if WS fails. */
const http = require("http");
const fs = require("fs");
const path = require("path");
const { getSnapshot, getKlines } = require("./snapshot");

const PORT = process.env.PORT || 8000;
const MIME = { ".html": "text/html", ".js": "text/javascript", ".css": "text/css", ".json": "application/json" };

const clients = new Set();
let bnWs = null, bnPollTimer = null, bnHostIdx = 0;
const BN_HOSTS = ["wss://data-stream.binance.vision", "wss://stream.binance.com:9443"];

const INTERVAL_MS = { "5m": 300000, "15m": 900000, "1h": 3600000 };
const lockPrices = { BTC: {}, ETH: {} };

function getRoundStart(tf, now) {
  const dur = INTERVAL_MS[tf] || 300000;
  return Math.floor(now / dur) * dur;
}

function broadcast(type, data) {
  const payload = `event: ${type}\ndata: ${JSON.stringify(data)}\n\n`;
  clients.forEach((res) => { try { res.write(payload); } catch (_) {} });
}

function startBinance() {
  if (bnWs || typeof WebSocket === "undefined") { fallbackPoll(); return; }
  const url = `${BN_HOSTS[bnHostIdx % BN_HOSTS.length]}/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade/btcusdt@ticker/ethusdt@ticker`;
  let ws;
  try { ws = new WebSocket(url); } catch (e) { bnHostIdx++; setTimeout(startBinance, 2000); return; }
  ws.onopen = () => { if (bnPollTimer) { clearInterval(bnPollTimer); bnPollTimer = null; } };
  ws.onmessage = (ev) => {
    try {
      const m = JSON.parse(ev.data); const d = m.data; if (!d) return;
      if (d.e === "aggTrade") {
        const sym = d.s === "BTCUSDT" ? "BTC" : "ETH";
        const now = Date.now();
        
        // Capture lock price at session boundaries for all timeframes
        for (const tf of Object.keys(INTERVAL_MS)) {
          const roundStart = getRoundStart(tf, now);
          const key = `${tf}_${roundStart}`;
          if (!(key in lockPrices[sym])) {
            lockPrices[sym][key] = { price: +d.p, ts: d.T, roundStart };
            console.log(`[LOCK] ${sym} ${tf} lock price captured: ${+d.p} at ${new Date(d.T).toISOString()}`);
          }
        }
        
        broadcast("trade", { sym, price: +d.p, qty: +d.q, ts: d.T });
      } else if (d.e === "24hrTicker") {
        const sym = d.s === "BTCUSDT" ? "BTC" : "ETH";
        broadcast("ticker", { sym, chg: +d.P, last: +d.c });
      }
    } catch (_) {}
  };
  ws.onclose = () => { bnWs = null; setTimeout(startBinance, 2000); fallbackPoll(); };
  ws.onerror = () => { try { ws.close(); } catch (_) {} };
  bnWs = ws;
}

function fallbackPoll() {
  if (bnPollTimer) return;
  bnPollTimer = setInterval(async () => {
    try {
      const s = await getSnapshot(false);
      for (const k of ["BTC", "ETH"]) {
        const ones = s.candles[k]["1s"]; const last = ones[ones.length - 1];
        if (last) broadcast("trade", { sym: k, price: last.close, qty: last.vol || 0, ts: last.time * 1000 });
        const t = s.ticker[k]; broadcast("ticker", { sym: k, chg: t.chg, last: t.last });
      }
    } catch (_) {}
  }, 1000);
}

http.createServer(async (req, res) => {
  const u = new URL(req.url, "http://x");

  if (u.pathname === "/api/snapshot") {
    try {
      const snap = await getSnapshot(u.searchParams.get("history") === "1");
      res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store", "Access-Control-Allow-Origin": "*" });
      res.end(JSON.stringify(snap));
    } catch (e) {
      res.writeHead(502, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: String(e) }));
    }
    return;
  }

  if (u.pathname === "/api/stream") {
    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      "Connection": "keep-alive",
      "Access-Control-Allow-Origin": "*",
    });
    res.write("\n");
    try {
      const snap = await getSnapshot(true);
      res.write(`event: snapshot\ndata: ${JSON.stringify(snap)}\n\n`);
    } catch (e) {
      res.write(`event: error\ndata: ${JSON.stringify({ error: String(e) })}\n\n`);
    }
    clients.add(res);
    startBinance();
    req.on("close", () => clients.delete(res));
    return;
  }

  if (u.pathname === "/api/klines") {
    try {
      const sym = u.searchParams.get("symbol") || "BTC";
      const tf = u.searchParams.get("tf") || "1s";
      const before = parseInt(u.searchParams.get("before") || "0", 10) || 0;
      const limit = Math.min(parseInt(u.searchParams.get("limit") || "600", 10), 1000);
      const candles = await getKlines(sym, tf, before, limit);
      res.writeHead(200, { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*", "Cache-Control": "no-store" });
      res.end(JSON.stringify({ candles }));
    } catch (e) {
      res.writeHead(502, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: String(e) }));
    }
    return;
  }

  if (u.pathname === "/api/lockprice") {
    try {
      const sym = u.searchParams.get("symbol") || "BTC";
      const tf = u.searchParams.get("tf") || "5m";
      const clientRoundStart = u.searchParams.get("roundStart");
      const now = Date.now();
      
      // If client provides roundStart, use it; otherwise calculate from server time
      let roundStart = clientRoundStart ? parseInt(clientRoundStart, 10) : getRoundStart(tf, now);
      const key = `${tf}_${roundStart}`;
      let lock = lockPrices[sym] && lockPrices[sym][key];
      
      // If exact match not found, look for most recent lock price for this timeframe
      if (!lock) {
        const allLocks = lockPrices[sym] || {};
        let bestKey = null;
        let bestTime = 0;
        for (const k of Object.keys(allLocks)) {
          if (k.startsWith(tf + '_')) {
            const lockTime = allLocks[k].ts;
            if (lockTime > bestTime) {
              bestTime = lockTime;
              bestKey = k;
            }
          }
        }
        if (bestKey) {
          lock = allLocks[bestKey];
        }
      }
      
      if (lock) {
        res.writeHead(200, { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*", "Cache-Control": "no-store" });
        res.end(JSON.stringify({ lockPrice: lock.price, ts: lock.ts, roundStart: lock.roundStart }));
      } else {
        res.writeHead(404, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Lock price not yet captured for this session" }));
      }
    } catch (e) {
      res.writeHead(502, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: String(e) }));
    }
    return;
  }

  let p = u.pathname === "/" ? "/index.html" : u.pathname;
  const fp = path.join(__dirname, decodeURIComponent(p));
  if (!fp.startsWith(__dirname)) { res.writeHead(403); res.end(); return; }
  fs.readFile(fp, (err, data) => {
    if (err) { res.writeHead(404); res.end("not found"); return; }
    res.writeHead(200, { "Content-Type": MIME[path.extname(fp)] || "application/octet-stream", "Cache-Control": "no-store" });
    res.end(data);
  });
}).listen(PORT, () => console.log("Serving on http://localhost:" + PORT));
