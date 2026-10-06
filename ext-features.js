/* ============================================================================
   ext-features.js — Sumber data EKSTERNAL (Batch 1) untuk mempertajam/memperbanyak sinyal.
   OBSERVASIONAL dulu: hanya di-REKAM ke ledger (sig.ext) + diprobe via /api/ext-probe.
   Tidak mengubah accepted/reject sampai ada bukti lift (pola recorder -> backtest -> gate).

   Fitur (per simbol BTC/ETH/BNB):
     - funding      : lastFundingRate (fapi premiumIndex)
     - basisPct     : (markPrice-indexPrice)/indexPrice*100  (perp vs index)
     - oi           : openInterest (kontrak)
     - oiDelta5m    : (OI_now - OI_5m_lalu)/OI_5m_lalu*100   (openInterestHist)
     - lsrGlobal    : globalLongShortAccountRatio (retail)
     - lsrTop       : topLongShortPositionRatio  (top trader)
     - takerLS      : takerlongshortRatio.buySellRatio
     - depthImb     : (ΣbidNotional-ΣaskNotional)/(Σbid+Σask)  depth spot 20 level
   Endpoint FAIL-OPEN: field null bila sumber tidak dapat diakses (mis. fapi geo-block).
   ============================================================================ */
const FUT = ["https://fapi.binance.com"];
const SPOT = ["https://data-api.binance.vision", "https://api.binance.com", "https://api.binance.com", "https://api1.binance.com", "https://api2.binance.com"];
const SYMS = { BTC: "BTCUSDT", ETH: "ETHUSDT", BNB: "BNBUSDT" };

async function jget(hosts, path, ms = 6000) {
  let last;
  for (const h of hosts) {
    const t0 = Date.now();
    try {
      const r = await fetch(h + path, { signal: AbortSignal.timeout(ms) });
      if (!r.ok) throw new Error("HTTP " + r.status);
      return { data: await r.json(), ms: Date.now() - t0, host: h };
    } catch (e) { last = e; }
  }
  throw last || new Error("fetch failed");
}
const num = (x) => (x != null && isFinite(Number(x)) ? Number(x) : null);

async function fetchSym(k) {
  const s = SYMS[k];
  const out = { sym: k, ok: true, errors: [], t: Date.now() };
  // premiumIndex: funding + basis
  try {
    const { data } = await jget(FUT, `/fapi/v1/premiumIndex?symbol=${s}`);
    out.funding = num(data.lastFundingRate);
    const mk = num(data.markPrice), ix = num(data.indexPrice);
    out.basisPct = (mk != null && ix) ? +(((mk - ix) / ix) * 100).toFixed(5) : null;
  } catch (e) { out.errors.push("premium:" + (e && e.message)); }
  // openInterest now + hist 5m
  try { const { data } = await jget(FUT, `/fapi/v1/openInterest?symbol=${s}`); out.oi = num(data.openInterest); }
  catch (e) { out.errors.push("oi:" + (e && e.message)); }
  try {
    const { data } = await jget(FUT, `/futures/data/openInterestHist?symbol=${s}&period=5m&limit=2`);
    if (Array.isArray(data) && data.length >= 2) {
      const now = num(data[data.length - 1].sumOpenInterest), prev = num(data[0].sumOpenInterest);
      out.oiDelta5m = (now != null && prev) ? +(((now - prev) / prev) * 100).toFixed(4) : null;
    }
  } catch (e) { out.errors.push("oiHist:" + (e && e.message)); }
  // long/short ratios (5m)
  try { const { data } = await jget(FUT, `/futures/data/globalLongShortAccountRatio?symbol=${s}&period=5m&limit=1`); out.lsrGlobal = Array.isArray(data) && data[0] ? num(data[0].longShortRatio) : null; }
  catch (e) { out.errors.push("lsrG:" + (e && e.message)); }
  try { const { data } = await jget(FUT, `/futures/data/topLongShortPositionRatio?symbol=${s}&period=5m&limit=1`); out.lsrTop = Array.isArray(data) && data[0] ? num(data[0].longShortRatio) : null; }
  catch (e) { out.errors.push("lsrT:" + (e && e.message)); }
  try { const { data } = await jget(FUT, `/futures/data/takerlongshortRatio?symbol=${s}&period=5m&limit=1`); out.takerLS = Array.isArray(data) && data[0] ? num(data[0].buySellRatio) : null; }
  catch (e) { out.errors.push("taker:" + (e && e.message)); }
  // spot depth imbalance (top 20)
  try {
    const { data } = await jget(SPOT, `/api/v3/depth?symbol=${s}&limit=20`);
    const sum = (arr) => (arr || []).reduce((a, r) => a + (+r[0]) * (+r[1]), 0);
    const b = sum(data.bids), a = sum(data.asks);
    out.depthImb = (b + a > 0) ? +(((b - a) / (b + a)).toFixed(4)) : null;
  } catch (e) { out.errors.push("depth:" + (e && e.message)); }
  out.ok = out.errors.length === 0;
  return out;
}

const cache = {};           // sym -> feature object
let lastRefresh = 0, lastMs = null;

async function refreshAll() {
  const t0 = Date.now();
  await Promise.all(Object.keys(SYMS).map(async (k) => { try { cache[k] = await fetchSym(k); } catch (e) { cache[k] = { sym: k, ok: false, errors: [String(e && e.message)] , t: Date.now()}; } }));
  lastRefresh = Date.now(); lastMs = lastRefresh - t0;
  return cache;
}

// sync getter untuk computeSignal (dari cache; null bila belum ada)
function get(sym) {
  const c = cache[sym];
  if (!c) return null;
  return {
    funding: c.funding ?? null, basisPct: c.basisPct ?? null, oi: c.oi ?? null, oiDelta5m: c.oiDelta5m ?? null,
    lsrGlobal: c.lsrGlobal ?? null, lsrTop: c.lsrTop ?? null, takerLS: c.takerLS ?? null, depthImb: c.depthImb ?? null,
    ageMs: Date.now() - (c.t || 0),
  };
}
function probe() { return { lastRefresh, lastMs, at: new Date().toISOString(), cache }; }

module.exports = { refreshAll, get, probe, fetchSym, SYMS };
