/* ============================================================================
   ext-features.js — Sumber data EKSTERNAL (Batch 1) untuk mempertajam/memperbanyak sinyal.
   OBSERVASIONAL dulu: direkam ke ledger (sig.ext) + diprobe via /api/ext-probe.
   Tidak mengubah accepted/reject sampai ada bukti lift.

   MULTI-SUMBER (fallback berurutan) karena fapi.binance.com ter-geo-block (HTTP 451) di Railway:
     funding/basis : Binance fapi premiumIndex -> Bybit tickers -> OKX (funding-rate+mark-price)
     OI            : Binance openInterest -> Bybit open-interest -> OKX open-interest
     oiDelta5m     : Binance openInterestHist -> Bybit open-interest(limit2,5min)
     LSR           : Binance global/top -> Bybit account-ratio -> OKX rubik long-short
     takerLS       : Binance takerlongshortRatio -> OKX taker-volume
     depthImb      : Binance spot depth -> Bybit orderbook -> OKX books
   Fail-open: nilai null bila semua sumber gagal; menyimpan `src` per fitur.
   ============================================================================ */
const SPOT = ["https://data-api.binance.vision", "https://api.binance.com", "https://api1.binance.com"];
const FUT = ["https://fapi.binance.com"];
const BYBIT = ["https://api.bybit.com"];
const OKX = ["https://www.okx.com"];
const SYMS = { BTC: "BTCUSDT", ETH: "ETHUSDT", BNB: "BNBUSDT" };
const OKX_SWAP = { BTC: "BTC-USDT-SWAP", ETH: "ETH-USDT-SWAP", BNB: "BNB-USDT-SWAP" };
const OKX_SPOT = { BTC: "BTC-USDT", ETH: "ETH-USDT", BNB: "BNB-USDT" };
const BY_SYM = { BTC: "BTCUSDT", ETH: "ETHUSDT", BNB: "BNBUSDT" };

async function jget(hosts, path, ms = 6000) {
  let last;
  for (const h of hosts) {
    try { const r = await fetch(h + path, { signal: AbortSignal.timeout(ms) }); if (!r.ok) throw new Error("HTTP " + r.status); return await r.json(); }
    catch (e) { last = e; }
  }
  throw last || new Error("fetch failed");
}
const num = (x) => (x != null && isFinite(Number(x)) ? Number(x) : null);
// coba serangkaian sumber, kembalikan hasil pertama yang non-null
async function first(srcs) {
  const errs = [];
  for (const [name, fn] of srcs) {
    try { const v = await fn(); if (v != null && v !== undefined) return { v, src: name }; }
    catch (e) { errs.push(name + ":" + String((e && e.message) || e).slice(0, 24)); }
  }
  return { v: null, src: null, errs };
}

async function fetchSym(k) {
  const s = SYMS[k], bs = BY_SYM[k], os = OKX_SWAP[k], osp = OKX_SPOT[k];
  const out = { sym: k, t: Date.now(), src: {} };

  const fb = await first([
    ["bnc-fapi", async () => { const d = await jget(FUT, `/fapi/v1/premiumIndex?symbol=${s}`); const mk = num(d.markPrice), ix = num(d.indexPrice); return { funding: num(d.lastFundingRate), basisPct: mk != null && ix ? +(((mk - ix) / ix) * 100).toFixed(5) : null }; }],
    ["bybit", async () => { const d = await jget(BYBIT, `/v5/market/tickers?category=linear&symbol=${bs}`); const r = d.result && d.result.list && d.result.list[0]; if (!r) return null; const mk = num(r.markPrice), ix = num(r.indexPrice); return { funding: num(r.fundingRate), basisPct: mk != null && ix ? +(((mk - ix) / ix) * 100).toFixed(5) : null }; }],
    ["okx", async () => { const [fr, mp, ix] = await Promise.all([jget(OKX, `/api/v5/public/funding-rate?instId=${os}`), jget(OKX, `/api/v5/public/mark-price?instType=SWAP&instId=${os}`), jget(OKX, `/api/v5/market/index-tickers?instId=${osp}`)]); const f = fr.data && fr.data[0], m = mp.data && mp.data[0], x = ix.data && ix.data[0]; const mk = m ? num(m.markPx) : null, idx = x ? num(x.idxPx) : null; return { funding: f ? num(f.fundingRate) : null, basisPct: (mk != null && idx) ? +(((mk - idx) / idx) * 100).toFixed(5) : null }; }],
  ]);
  if (fb.v) { out.funding = fb.v.funding ?? null; out.basisPct = fb.v.basisPct ?? null; }
  out.src.funding = fb.src;

  const oi = await first([
    ["bnc-fapi", async () => { const d = await jget(FUT, `/fapi/v1/openInterest?symbol=${s}`); return num(d.openInterest); }],
    ["bybit", async () => { const d = await jget(BYBIT, `/v5/market/open-interest?category=linear&symbol=${bs}&intervalTime=5min&limit=1`); const r = d.result && d.result.list && d.result.list[0]; return r ? num(r.openInterest) : null; }],
    ["okx", async () => { const d = await jget(OKX, `/api/v5/public/open-interest?instType=SWAP&instId=${os}`); const r = d.data && d.data[0]; return r ? num(r.oi) : null; }],
  ]);
  out.oi = oi.v; out.src.oi = oi.src;

  const oid = await first([
    ["bnc-fapi", async () => { const d = await jget(FUT, `/futures/data/openInterestHist?symbol=${s}&period=5m&limit=2`); if (!Array.isArray(d) || d.length < 2) return null; const a = num(d[0].sumOpenInterest), b = num(d[d.length - 1].sumOpenInterest); return a && b != null ? +(((b - a) / a) * 100).toFixed(4) : null; }],
    ["bybit", async () => { const d = await jget(BYBIT, `/v5/market/open-interest?category=linear&symbol=${bs}&intervalTime=5min&limit=2`); const l = (d.result && d.result.list) || []; if (l.length < 2) return null; const a = num(l[0].openInterest), b = num(l[l.length - 1].openInterest); return a && b != null ? +(((b - a) / a) * 100).toFixed(4) : null; }],
    ["okx", async () => { const d = await jget(OKX, `/api/v5/rubik/stat/contracts/open-interest-volume?ccy=${k}&period=5m`); const a = d.data; if (!Array.isArray(a) || a.length < 2) return null; const p = num(a[0][1]), q = num(a[a.length - 1][1]); return p && q != null ? +(((q - p) / p) * 100).toFixed(4) : null; }],
  ]);
  out.oiDelta5m = oid.v; out.src.oiDelta5m = oid.src;

  const lsr = await first([
    ["bnc-fapi", async () => { const g = await jget(FUT, `/futures/data/globalLongShortAccountRatio?symbol=${s}&period=5m&limit=1`); const t = await jget(FUT, `/futures/data/topLongShortPositionRatio?symbol=${s}&period=5m&limit=1`); return { lsrGlobal: Array.isArray(g) && g[0] ? num(g[0].longShortRatio) : null, lsrTop: Array.isArray(t) && t[0] ? num(t[0].longShortRatio) : null }; }],
    ["bybit", async () => { const d = await jget(BYBIT, `/v5/market/account-ratio?category=linear&symbol=${bs}&period=5min&limit=1`); const r = d.result && d.result.list && d.result.list[0]; return r ? { lsrGlobal: num(r.buyRatio) != null ? +(num(r.buyRatio) / (num(r.sellRatio) || 1)).toFixed(4) : null, lsrTop: null } : null; }],
    ["okx", async () => { const [g, t] = await Promise.all([jget(OKX, `/api/v5/rubik/stat/contracts/long-short-account-ratio?ccy=${k}&period=5m`).catch(() => null), jget(OKX, `/api/v5/rubik/stat/contracts/top-trader-long-short-account-ratio?ccy=${k}&period=5m`).catch(() => null)]); const gr = g && g.data && g.data[0], tr = t && t.data && t.data[0]; if (!gr && !tr) return null; return { lsrGlobal: gr ? num(gr[1]) : null, lsrTop: tr ? num(tr[1]) : null }; }],
  ]);
  if (lsr.v) { out.lsrGlobal = lsr.v.lsrGlobal ?? null; out.lsrTop = lsr.v.lsrTop ?? null; }
  out.src.lsr = lsr.src;

  const tk = await first([
    ["bnc-fapi", async () => { const d = await jget(FUT, `/futures/data/takerlongshortRatio?symbol=${s}&period=5m&limit=1`); return Array.isArray(d) && d[0] ? num(d[0].buySellRatio) : null; }],
    ["okx-c", async () => { const d = await jget(OKX, `/api/v5/rubik/stat/taker-volume-contract?ccy=${k}&period=5m`); const r = d.data && d.data[0]; if (!r) return null; const sell = num(r[1]), buy = num(r[2]); return sell ? +(buy / sell).toFixed(4) : null; }],
    ["okx-s", async () => { const d = await jget(OKX, `/api/v5/rubik/stat/taker-volume?ccy=${k}&period=5m`); const r = d.data && d.data[0]; if (!r) return null; const sell = num(r[1]), buy = num(r[2]); return sell ? +(buy / sell).toFixed(4) : null; }],
  ]);
  out.takerLS = tk.v; out.src.takerLS = tk.src;

  const dp = await first([
    ["bnc-spot", async () => { const d = await jget(SPOT, `/api/v3/depth?symbol=${s}&limit=20`); const sum = (arr) => (arr || []).reduce((a, r) => a + (+r[0]) * (+r[1]), 0); const b = sum(d.bids), a = sum(d.asks); return b + a > 0 ? +(((b - a) / (b + a)).toFixed(4)) : null; }],
    ["bybit", async () => { const d = await jget(BYBIT, `/v5/market/orderbook?category=spot&symbol=${bs}&limit=20`); const sum = (arr) => (arr || []).reduce((a, r) => a + (+r[0]) * (+r[1]), 0); const b = sum(d.result && d.result.b), a = sum(d.result && d.result.a); return b + a > 0 ? +(((b - a) / (b + a)).toFixed(4)) : null; }],
    ["okx", async () => { const d = await jget(OKX, `/api/v5/market/books?instId=${osp}&sz=20`); const r = d.data && d.data[0]; if (!r) return null; const sum = (arr) => (arr || []).reduce((a, x) => a + (+x[0]) * (+x[1]), 0); const b = sum(r.bids), a = sum(r.asks); return b + a > 0 ? +(((b - a) / (b + a)).toFixed(4)) : null; }],
  ]);
  out.depthImb = dp.v; out.src.depthImb = dp.src;

  out.ok = [out.funding, out.oi, out.oiDelta5m, out.lsrGlobal, out.takerLS, out.depthImb].some((x) => x != null);
  return out;
}

const cache = {};
let lastRefresh = 0, lastMs = null;
async function refreshAll() {
  const t0 = Date.now();
  await Promise.all(Object.keys(SYMS).map(async (k) => { try { cache[k] = await fetchSym(k); } catch (e) { cache[k] = { sym: k, ok: false, t: Date.now() }; } }));
  lastRefresh = Date.now(); lastMs = lastRefresh - t0;
  return cache;
}
function get(sym) {
  const c = cache[sym];
  if (!c) return null;
  return { funding: c.funding ?? null, basisPct: c.basisPct ?? null, oi: c.oi ?? null, oiDelta5m: c.oiDelta5m ?? null,
    lsrGlobal: c.lsrGlobal ?? null, lsrTop: c.lsrTop ?? null, takerLS: c.takerLS ?? null, depthImb: c.depthImb ?? null,
    src: c.src || null, ageMs: Date.now() - (c.t || 0) };
}
function probe() { return { lastRefresh, lastMs, at: new Date().toISOString(), cache }; }

// DEBUG: coba beberapa kandidat endpoint OKX rubik, kembalikan status + body mentah (untuk kalibrasi).
async function debugOkx(ccy = "BTC") {
  const cands = [
    `/api/v5/rubik/stat/taker-volume?ccy=${ccy}&period=5m`,
    `/api/v5/rubik/stat/taker-volume-contract?ccy=${ccy}&period=5m`,
    `/api/v5/rubik/stat/contracts/taker-volume?ccy=${ccy}&period=5m`,
    `/api/v5/rubik/stat/contracts/top-trader-long-short-account-ratio?ccy=${ccy}&period=5m`,
    `/api/v5/rubik/stat/contracts/top-trader-long-short-position-ratio?ccy=${ccy}&period=5m`,
    `/api/v5/rubik/stat/contracts/long-short-account-ratio?ccy=${ccy}&period=5m`,
    `/api/v5/rubik/stat/taker-volume?ccy=${ccy}&period=1H`,
  ];
  const out = [];
  for (const p of cands) {
    try { const r = await fetch("https://www.okx.com" + p, { signal: AbortSignal.timeout(6000) }); const t = await r.text(); out.push({ p, status: r.status, body: t.slice(0, 260) }); }
    catch (e) { out.push({ p, status: 0, err: String((e && e.message) || e) }); }
  }
  return out;
}

module.exports = { refreshAll, get, probe, fetchSym, debugOkx, SYMS };
