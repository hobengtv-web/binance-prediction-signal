/* Local Node server: serves static files + /api/snapshot (polled) + /api/stream (SSE, realtime).
   The SSE endpoint opens a Binance trade stream on the SERVER and pushes every trade to the
   browser, so price moves are realtime (not once-per-second). Falls back to REST polling if WS fails. */
const http = require("http");
const fs = require("fs");
const path = require("path");
const { getSnapshot, getKlines } = require("./snapshot");

const PORT = process.env.PORT || 8000;
const MIME = { ".html": "text/html", ".js": "text/javascript", ".css": "text/css", ".json": "application/json" };

/* ===== PHASE 2: SIGNAL LEDGER =====
   Menyimpan setiap sinyal + vektor fitur lengkap + hasilnya, supaya learner punya data
   jangka panjang (Binance hanya menyediakan 1s klines 7 hari, jadi fitur skala detik
   harus dikumpulkan sendiri dari sekarang).
   Penyimpanan: JSONL append-only. Di Railway ditulis ke volume (mount /data) supaya
   bertahan antar deploy; secara lokal ke ./ledger. */
const LEDGER_DIR = process.env.LEDGER_DIR || (fs.existsSync("/data") ? "/data/ledger" : path.join(__dirname, "ledger"));
const LEDGER_FILE = path.join(LEDGER_DIR, "signals.jsonl");
let ledger = new Map();      // k -> record
let ledgerDirty = 0;
const CORS = { "Access-Control-Allow-Origin": "*", "Cache-Control": "no-store" };
function loadLedger() {
  try {
    fs.mkdirSync(LEDGER_DIR, { recursive: true });
    if (!fs.existsSync(LEDGER_FILE)) { console.log(`[LEDGER] new ledger at ${LEDGER_FILE}`); return; }
    const lines = fs.readFileSync(LEDGER_FILE, "utf8").split("\n");
    for (const l of lines) {
      if (!l.trim()) continue;
      try { const r = JSON.parse(l); if (r && r.k) ledger.set(r.k, r); } catch (_) {}
    }
    console.log(`[LEDGER] loaded ${ledger.size} records from ${LEDGER_FILE}`);
  } catch (e) { console.warn("[LEDGER] load failed:", e.message); }
}
function appendLedger(rec) {
  try { fs.appendFileSync(LEDGER_FILE, JSON.stringify(rec) + "\n"); } catch (e) { console.warn("[LEDGER] append failed:", e.message); }
}
function compactLedger() {   // satu baris per key (versi terakhir menang)
  try { fs.writeFileSync(LEDGER_FILE, [...ledger.values()].map((r) => JSON.stringify(r)).join("\n") + "\n"); }
  catch (e) { console.warn("[LEDGER] compact failed:", e.message); }
}
function ledgerStats() {
  const byKey = {};
  let withRes = 0, won = 0, touch = 0, touchN = 0;
  for (const r of ledger.values()) {
    const kk = `${r.asset}_${r.interval}`;
    (byKey[kk] = byKey[kk] || { n: 0, withRes: 0, won: 0 });
    byKey[kk].n++;
    if (r.res) { withRes++; byKey[kk].withRes++; if (r.res.won === 1) { won++; byKey[kk].won++; } if (r.res.touch != null) { touchN++; touch += r.res.touch; } }
  }
  return {
    total: ledger.size, withRes,
    winrate: withRes ? +(won / withRes).toFixed(4) : null,
    touchRate: touchN ? +(touch / touchN).toFixed(4) : null,
    byKey, dir: LEDGER_DIR, file: LEDGER_FILE,
    persistent: LEDGER_DIR.startsWith("/data"),
  };
}
loadLedger();
setInterval(() => { if (ledgerDirty > 0) { compactLedger(); ledgerDirty = 0; } }, 60000);

/* Self-healing: lengkapi hasil ronde yang belum tercatat (mis. browser ditutup sebelum ronde
   selesai) memakai klines 1m Binance. Arah/jenis hasil = tepat; sentuh-lock/MFE/MAE dihitung
   dari candle MENIT ke-2 dst (menit pertama memuat detik sinyal, jadi sengaja dikecualikan)
   -> ditandai src:"server-1m" agar tidak tertukar dengan hasil dari klien (jalur 1s). */
const DUR_S = { "5m": 300, "15m": 900, "1h": 3600 };
let resolving = false;
async function resolveMissing() {
  if (resolving) return;
  resolving = true;
  const now = Math.floor(Date.now() / 1000);
  let done = 0;
  try {
    for (const r of [...ledger.values()]) {
      if (r.res || !r.sig || !r.t0) continue;
      const dir = r.sig.dir;
      if (dir !== "up" && dir !== "down") continue;
      const dur = DUR_S[r.interval];
      if (!dur) continue;
      if (now < r.t0 + dur + 5) continue;                  // ronde belum berakhir
      if (now > r.t0 + dur + 86400 * 85) continue;         // di luar jangkauan 1m klines (90d)
      try {
        const bars = await getKlines(r.asset, "1m", r.t0 + dur, Math.ceil(dur / 60) + 2);
        const sess = bars.filter((b) => b.time >= r.t0 && b.time < r.t0 + dur);
        if (sess.length < 2) continue;
        const lock = sess[0].open, close = sess[sess.length - 1].close;
        const path = sess.slice(1);
        const v = (p) => (dir === "up" ? (p - lock) / lock * 100 : (lock - p) / lock * 100);
        let mfe = -Infinity, mae = Infinity, touch = 0, tTouch = null;
        for (const b of path) {
          const fHi = dir === "up" ? v(b.high) : v(b.low);
          const fLo = dir === "up" ? v(b.low) : v(b.high);
          if (fHi > mfe) mfe = fHi;
          if (fLo < mae) mae = fLo;
          if (!touch && fHi >= 0) { touch = 1; tTouch = b.time; }
        }
        const actual = close >= lock ? "up" : "down";
        const merged = Object.assign({}, r, {
          res: {
            lock: +lock, close: +close, actual, won: dir === actual ? 1 : 0,
            touch, tTouchSec: tTouch,
            mfeFav: isFinite(mfe) ? +mfe.toFixed(4) : null,
            maeFav: isFinite(mae) ? +mae.toFixed(4) : null,
            endFav: +v(close).toFixed(4), bars: path.length, src: "server-1m",
          },
          upd: Date.now(),
        });
        ledger.set(r.k, merged); appendLedger(merged); ledgerDirty++; done++;
      } catch (_) { /* coba lagi pada siklus berikutnya */ }
    }
  } finally { resolving = false; }
  if (done) console.log(`[LEDGER] resolved ${done} outcome(s) dari 1m klines · total ${ledger.size}`);
}
setTimeout(() => resolveMissing().catch(() => {}), 20000);
setInterval(() => resolveMissing().catch(() => {}), 5 * 60 * 1000);

const clients = new Set();
let bnWs = null, bnPollTimer = null, bnHostIdx = 0;

// Active visitor tracking
const activeVisitors = new Map();  // id -> { lastSeen, userAgent }
const VISITOR_TIMEOUT = 60000;     // consider visitor inactive after 60s

setInterval(() => {
  const now = Date.now();
  for (const [id, v] of activeVisitors) {
    if (now - v.lastSeen > VISITOR_TIMEOUT) activeVisitors.delete(id);
  }
}, 30000);

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
        
        // `m` = isBuyerMaker: true means the aggressor was the seller (taker sell).
        broadcast("trade", { sym, price: +d.p, qty: +d.q, ts: d.T, m: d.m });
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

  if (u.pathname === "/api/ledger") {
    if (req.method === "OPTIONS") { res.writeHead(204, CORS); res.end(); return; }
    if (req.method === "POST") {
      let body = "", tooBig = false;
      req.on("data", (ch) => { body += ch; if (body.length > 4e6) { tooBig = true; req.destroy(); } });
      req.on("end", () => {
        if (tooBig) { res.writeHead(413, CORS); res.end('{"error":"too big"}'); return; }
        let saved = 0;
        try {
          const j = JSON.parse(body || "{}");
          const recs = Array.isArray(j.records) ? j.records : (j.record ? [j.record] : []);
          for (const r of recs) {
            if (!r || typeof r.k !== "string") continue;
            const prev = ledger.get(r.k) || { k: r.k };
            const merged = Object.assign({}, prev, r);
            // jangan menimpa hasil yang sudah tercatat dengan record sinyal yang lebih baru
            if (prev.res && !r.res) merged.res = prev.res;
            if (prev.sig && !r.sig) merged.sig = prev.sig;
            merged.upd = Date.now();
            ledger.set(r.k, merged);
            appendLedger(merged);
            saved++;
          }
          if (saved) ledgerDirty += saved;
        } catch (e) { console.warn("[LEDGER] bad POST:", e.message); }
        res.writeHead(200, Object.assign({ "Content-Type": "application/json" }, CORS));
        res.end(JSON.stringify({ saved, total: ledger.size }));
      });
      return;
    }
    const st = ledgerStats();
    res.writeHead(200, Object.assign({ "Content-Type": "application/json" }, CORS));
    if (u.searchParams.get("dump") === "1") {
      res.end(JSON.stringify({ stats: st, records: [...ledger.values()] }));
    } else {
      res.end(JSON.stringify(st));
    }
    return;
  }

  if (u.pathname === "/api/stats") {
    res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store", "Access-Control-Allow-Origin": "*" });
    res.end(JSON.stringify({ activeUsers: activeVisitors.size }));
    return;
  }

  if (u.pathname === "/api/visit") {
    const id = u.searchParams.get("id") || (Date.now().toString(36) + Math.random().toString(36).slice(2, 8));
    activeVisitors.set(id, { lastSeen: Date.now(), userAgent: req.headers["user-agent"] || "" });
    res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store", "Access-Control-Allow-Origin": "*" });
    res.end(JSON.stringify({ id, activeUsers: activeVisitors.size }));
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
