/* Shared Binance snapshot fetcher — runs server-side (Vercel function or local Node).
   Fetches from Binance on the SERVER, so the browser only talks to our own origin.
   This bypasses regional/browser blocking of Binance & TradingView. */
const SYMS = { BTC: "BTCUSDT", ETH: "ETHUSDT" };
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
];
const FUTURES_REST = [
  "https://fapi.binance.com",
];

async function getJSON(path) {
  let lastErr;
  for (const h of REST) {
    try {
      const r = await fetch(h + path);
      if (!r.ok) throw new Error("HTTP " + r.status);
      return await r.json();
    } catch (e) { lastErr = e; }
  }
  throw lastErr;
}

async function getFuturesJSON(path) {
  let lastErr;
  for (const h of FUTURES_REST) {
    try {
      const r = await fetch(h + path);
      if (!r.ok) throw new Error("HTTP " + r.status);
      return await r.json();
    } catch (e) { lastErr = e; }
  }
  throw lastErr;
}

function mapKline(r) {
  return {
    time: Math.floor(r[0] / 1000),
    open: +r[1], high: +r[2], low: +r[3], close: +r[4],
    vol: +r[5], trades: +r[8],
    openTime: r[0], closeTime: r[6],
  };
}

async function getSnapshot(history) {
  const out = { candles: {}, ticker: {}, mark: {}, serverTime: 0 };
  // Fetch Binance server time FIRST (authoritative for lock price & timer)
  try {
    const t = await getJSON("/api/v3/time");
    out.serverTime = t.serverTime;
  } catch (_) { /* fall back to candle-based inference in client */ }
  await Promise.all(Object.keys(SYMS).map(async (k) => {
    out.candles[k] = {};
    await Promise.all(Object.keys(KLINES).map(async (tf) => {
      const limit = history ? KLINES[tf].hist : KLINES[tf].live;
      const rows = await getJSON(`/api/v3/klines?symbol=${SYMS[k]}&interval=${tf}&limit=${limit}`);
      out.candles[k][tf] = rows.map(mapKline);
    }));
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
