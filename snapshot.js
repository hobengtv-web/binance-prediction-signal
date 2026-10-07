/* Shared Binance snapshot fetcher — runs server-side (Vercel function or local Node).
   Fetches from Binance on the SERVER, so the browser only talks to our own origin.
   This bypasses regional/browser blocking of Binance & TradingView. */
const SYMS = { BTC: "BTCUSDT", ETH: "ETHUSDT", BNB: "BNBUSDT" };
// 1s = base for 5-second candles (aggregated client-side); 5m/15m/1h = trend contexts.
const KLINES = {
  "1s": { hist: 1000, live: 6 },
  "5m": { hist: 160, live: 2 },
  "15m": { hist: 160, live: 2 },
  "1h": { hist: 160, live: 2 },
};
const REST = [
  "https://data-api.binance.vision",
  "https://api.binance.com",
  "https://api1.binance.com",
  "https://api2.binance.com",
  "https://api3.binance.com",
  "https://api4.binance.com",
];
const FUTURES_REST = [
  "https://fapi.binance.com",
];

// ===== CACHE + KOALESENSI + CIRCUIT-BREAKER =====
// Semua beban Binance kini di SERVER (klien 100% via server). Tanpa cache, banyak klien + engine
// menembak Binance bertubi -> 418 (IP ban). Cache TTL + dedupe in-flight + backoff saat 418/429.
const _cache = new Map();        // path -> { t, v }
const _inflight = new Map();     // path -> Promise (dedupe request bersamaan)
let _banUntil = 0;               // cooldown global saat Binance balas 418/429
// BATAS CACHE (cegah OOM): path memuat endTime/before -> tiap window lazy-history menambah entri baru.
// Tanpa eviction, Map tumbuh tak terbatas (masing-masing menyimpan array klines) -> heap habis.
const _CACHE_MAX = Number(process.env.CACHE_MAX || 400);
function _capMap(m, max) { if (m.size > max) { const over = m.size - max; let i = 0; for (const k of m.keys()) { m.delete(k); if (++i >= over) break; } } }
function ttlFor(path) {
  if (path.includes("/klines")) return 3000;
  if (path.includes("/ticker")) return 5000;
  if (path.includes("/depth")) return 3000;
  if (path.includes("/time")) return 2000;
  return 2000;
}
async function getJSON(path) {
  const now = Date.now();
  const c = _cache.get(path);
  if (c && now - c.t < ttlFor(path)) return c.v;
  if (now < _banUntil) {
    if (c) return c.v;                                  // pakai data lama agar UI tetap hidup
    throw new Error("binance cooldown aktif (418/429)");
  }
  if (_inflight.has(path)) return _inflight.get(path);
  const p = (async () => {
    let lastErr;
    for (const h of REST) {
      try {
        const r = await fetch(h + path, { signal: AbortSignal.timeout(8000) });
        if (r.status === 418 || r.status === 429) {
          const ra = Number(r.headers.get("retry-after") || 0);
          _banUntil = Date.now() + Math.max(60000, ra * 1000);   // backoff (min 60s)
          throw new Error("HTTP " + r.status);
        }
        if (!r.ok) throw new Error("HTTP " + r.status);
        const v = await r.json();
        _cache.set(path, { t: Date.now(), v }); _capMap(_cache, _CACHE_MAX);
        return v;
      } catch (e) { lastErr = e; }
    }
    if (c) return c.v;                                  // gagal total -> data lama (fail-soft)
    throw lastErr;
  })();
  _inflight.set(path, p);
  try { return await p; } finally { _inflight.delete(path); }
}

const _futCache = new Map();
async function getFuturesJSON(path) {
  const now = Date.now();
  const c = _futCache.get(path);
  if (c && now - c.t < 15000) return c.v;
  if (now < _banUntil) { if (c) return c.v; throw new Error("binance cooldown aktif"); }
  let lastErr;
  for (const h of FUTURES_REST) {
    try {
      const r = await fetch(h + path, { signal: AbortSignal.timeout(8000) });
      if (!r.ok) throw new Error("HTTP " + r.status);
      const v = await r.json();
      _futCache.set(path, { t: Date.now(), v }); _capMap(_futCache, 100);
      return v;
    } catch (e) { lastErr = e; }
  }
  if (c) return c.v;
  throw lastErr;
}

function mapKline(r) {
  return {
    time: Math.floor(r[0] / 1000),
    open: +r[1], high: +r[2], low: +r[3], close: +r[4],
    vol: +r[5], trades: +r[8],
    // r[9] = taker BUY base volume. Dipakai menghitung executed order flow (OFI) tanpa WS:
    // di Railway stream aggTrade Binance tidak bisa dibuka, sedangkan REST kline jalan.
    tb: +r[9],
    openTime: r[0], closeTime: r[6],
  };
}

async function getSnapshot(history) {
  const out = { candles: {}, ticker: {}, mark: {}, serverTime: 0, orderbook: {} };
  try {
    const t = await getJSON("/api/v3/time");
    out.serverTime = t.serverTime;
  } catch (_) {}
  await Promise.all(Object.keys(SYMS).map(async (k) => {
    out.candles[k] = {};
    await Promise.all(Object.keys(KLINES).map(async (tf) => {
      const limit = history ? KLINES[tf].hist : KLINES[tf].live;
      const rows = await getJSON(`/api/v3/klines?symbol=${SYMS[k]}&interval=${tf}&limit=${limit}`);
      out.candles[k][tf] = rows.map(mapKline);
    }));
    // Orderbook (depth) — top 5 levels for buy/sell pressure bar.
    // Use spot REST (data-api.binance.vision is CORS-friendly & not geo-blocked)
    // rather than fapi.binance.com (often blocked from serverless)
    try {
      const ob = await getJSON(`/api/v3/depth?symbol=${SYMS[k]}&limit=5`);
      out.orderbook[k] = { bids: ob.bids, asks: ob.asks };
    } catch (_) { out.orderbook[k] = null; }
  }));
  await Promise.all(Object.keys(SYMS).map(async (k) => {
    const t = await getJSON(`/api/v3/ticker/24hr?symbol=${SYMS[k]}`);
    out.ticker[k] = { last: +t.lastPrice, chg: +t.priceChangePercent };
  }));
  await Promise.all(Object.keys(SYMS).map(async (k) => {
    try {
      const m = await getFuturesJSON(`/fapi/v1/premiumIndex?symbol=${SYMS[k]}`);
      out.mark[k] = { price: +m.markPrice, funding: +m.lastFundingRate };
    } catch (_) {
      out.mark[k] = null;
    }
  }));
  return out;
}

async function getKlines(symbol, tf, beforeSec, limit) {
  const s = SYMS[symbol] || symbol;
  const path = `/api/v3/klines?symbol=${s}&interval=${tf}&limit=${limit}` +
    (beforeSec ? `&endTime=${beforeSec * 1000}` : "");
  const rows = await getJSON(path);
  return rows.map(mapKline);
}

module.exports = { getSnapshot, getKlines };
