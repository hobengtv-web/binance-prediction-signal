/* ============================================================
   Binance Prediction Signal — BTC / ETH
   Real-time candles + active-candle projection (Up/Down)
   Data: Binance public REST + WebSocket (no API key needed)
   ============================================================ */

const SYMBOLS = {
  BTC: "btcusdt",
  ETH: "ethusdt",
};
const OB_SYMBOLS = { BTC: "BTCUSDT", ETH: "ETHUSDT" };  // Binance spot REST symbol format
// Prediction round durations (lock period). Candle size is fixed 5s (aggregated from 1s).
const INTERVALS = ["5m", "15m", "1h"];
const INTERVAL_MS = { "5m": 300_000, "15m": 900_000, "1h": 3_600_000 };
const CANDLE_SEC = 5; // each chart candle = 5 seconds (default)
const CHART_INTERVALS = ["5s", "30s", "1m"];
const HISTORY = 160;
const HISTORY_LOAD = 600;       // 1s candles per lazy fetch (~10 minutes)
const HISTORY_CAP_1S = 1200;   // ~10 menit 1s candles (scrollable window for lazy load)
const HISTORY_CAP_5S = 250;     // cap on stored 5s candles (~20 min, scrollable)

// GOAL — "penguat" sinyal yang diinginkan (counter-trend / momentum reversal).
// Kita HANYA mau sinyal yang BERLAWANAN arah dengan gerakan harga (fade peak):
//   • REVERSAL↑ = masuk UP  setelah harga dari bawah  (peak bawah) → sesuai GOAL
//   • REVERSAL↓ = masuk DOWN setelah harga dari atas   (peak atas)  → sesuai GOAL
//   • CONT searah (Down saat down / Up saat up) di-SUPPRESS → NETRAL
// mode: "REVERSAL_ONLY" (filter aktif) | "ANY" (biarkan semua sinyal seperti semula)
const GOAL = {
  mode: "REVERSAL_ONLY",
  boost: 10, // tambahan confidence (%) saat sinyal sesuai GOAL (counter-trend)
};
// WARMUP — 3 menit pertama ronde dianggap kurang presisi (harga masih bisa berubah banyak
// hingga close). Semua sinyal di-suppress → NETRAL agar tidak memicu entry terlalu dini.
const WARMUP_MS = 3 * 60 * 1000;
// VOL_TYPICAL — volume "normal" per candle 5s tiap aset (kalibrasi: rata-rata 240 mnt / 12).
// Dipakai sebagai referensi ABSOLUT agar pasar sepi/flat (vol rendah di mana pun) tetap terdeteksi
// sebagai likuiditas rendah, bukan cuma dibandingkan ke candle sebelumnya (yang ikut sepi → relatif ~1).
const VOL_TYPICAL = { BTC: 0.515, ETH: 8.22 };
// EXTEND — fraksi dari OPEN yg dianggap "fully extended" (harga sudah lari jauh dr open).
// Dipakai sbg skala penalti jarak: BTC cenderung bergerak % lebih kecil dr ETH, hence beda nilai.
const EXTEND = { BTC: 0.02, ETH: 0.025 };
const TREND_SESSIONS = 3;  // akumulasi trend dari N sesi terakhir interval yg aktif (3 sesi 5m = 15m, dst)
let confMode = "SIGNAL";  // mode CONFIDENCE: SIGNAL = otomatis counter-trend, UP/DOWN = paksa arah

// Endpoint fallbacks — `binance.vision` is Binance's public data service,
// usually not geo-blocked and CORS-friendly (common fix for ID/region blocks).
const REST_HOSTS = [
  "https://api.binance.com",
  "https://data-api.binance.vision",
  "https://api1.binance.com",
];
const WS_HOSTS = [
  "wss://stream.binance.com:9443",
  "wss://data-stream.binance.vision",
  "wss://stream1.binance.com:9443",
];

const state = {
  asset: "BTC",
  interval: "5m",
  chartInterval: "5s",
  type: "candle",
  // cache[symbol][interval] = { candles: [...], meta: {...} }
  cache: {},
  ticker: { BTC: null, ETH: null },
  orderbook: { BTC: null, ETH: null },
  // executed order flow per minute: flow[sym][minuteStartSec] = { buy, sell, n }
  flow: { BTC: {}, ETH: {} },
  // previous live price for gap sync
  prevPrice: { BTC: null, ETH: null },
  connected: false,
  viaProxy: false,
};

let serverTimeOffset = 0;  // client now → server now correction (ms)
let lastTimeSync = 0;      // Date.now() of last successful time sync
let _lastTradeOffsetAt = 0; // throttle for trade-derived offset updates
function serverNow() { return Date.now() + serverTimeOffset; }
// Re-sync Binance time if it's been stale for >3s (catches missed syncs)
function ensureTimeSync() { const now = Date.now(); if (!lastTimeSync || now - lastTimeSync > 3000) { lastTimeSync = now; fetchBinanceTime(); } }
async function fetchBinanceTime() {
  try {
    const r = await fetch("https://data-api.binance.vision/api/v3/time", { cache: "no-store" });
    if (r.ok) { const j = await r.json(); if (j.serverTime) { serverTimeOffset = j.serverTime - Date.now(); lastTimeSync = Date.now(); } }
  } catch (_) {}
}

// cache[symbol][series] = { candles, meta }; series: 1s (raw), 5s (chart), 5m/15m/1h (trend)
const CANDLE_SERIES = ["1s", "5s", "30s", "1m", "5m", "15m", "1h"];

INTERVALS.forEach((tf) => {
  state.cache.BTC = state.cache.BTC || {};
  state.cache.ETH = state.cache.ETH || {};
  CANDLE_SERIES.forEach((s) => {
    state.cache.BTC[s] = { candles: [], meta: null };
    state.cache.ETH[s] = { candles: [], meta: null };
  });
});

/* ----------------------- Chart setup ----------------------- */
let chart;
let lastSignalState = null;  // Track previous signal untuk trigger alarm otomatis

function setSrc(label) {
  const txt = "SRC " + (label || "—");
  const srcEl = document.getElementById("src");
  if (srcEl) { srcEl.textContent = txt; srcEl.title = txt; }
  // Label sumber bisa sangat panjang ("langsung (Binance/ TradingView)"). Di layar sempit
  // #src disembunyikan (lihat styles.css) supaya header tetap SATU baris — nilainya tetap
  // bisa dibaca di tooltip dot status koneksi:
  const connEl = document.getElementById("conn");
  if (connEl) connEl.title = "Status koneksi · sumber data: " + (label || "—");
}
function showErr(msg) {
  const el = document.getElementById("err");
  if (!msg) { el.hidden = true; el.textContent = ""; return; }
  el.hidden = false; el.textContent = msg;
}
function setStatus(msg) {
  const el = document.getElementById("status");
  if (!msg) { el.classList.add("hidden"); return; }
  el.classList.remove("hidden");
  el.textContent = msg;
}
function hideStatus() { setStatus(null); }

async function fetchJSON(url, tries = REST_HOSTS.length) {
  let lastErr;
  for (let i = 0; i < REST_HOSTS.length; i++) {
    const u = url.replace(/^https?:\/\/[^/]+/, REST_HOSTS[i]);
    const ctrl = new AbortController();
    const to = setTimeout(() => ctrl.abort(), 6000);
    try {
      const r = await fetch(u, { signal: ctrl.signal });
      clearTimeout(to);
      if (!r.ok) throw new Error("HTTP " + r.status);
      setSrc(REST_HOSTS[i].replace("https://", "").replace("wss://", ""));
      return await r.json();
    } catch (e) { clearTimeout(to); lastErr = e; }
  }
  throw lastErr;
}

function fmt(n, d = 2) {
  if (n == null || isNaN(n)) return "—";
  return Number(n).toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d });
}
function fmtPrice(n) {
  if (n == null || isNaN(n)) return "—";
  const d = n >= 1000 ? 2 : n >= 1 ? 3 : 5;
  return Number(n).toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d });
}

function initChart() {
  chart = new CanvasChart(document.getElementById("chart"));
  setReach();
  chart.onCrosshair = (info) => {
    if (info && info.candle) updateLegend(info.candle);
    else updateLegendFromState();
  };
}

function updateLegend(c) {
  if (!c) return;
  document.getElementById("legend-price").textContent = fmtPrice(c.close);
  document.getElementById("legend-ohlc").textContent =
    `O ${fmtPrice(c.open)}  H ${fmtPrice(c.high)}  L ${fmtPrice(c.low)}  C ${fmtPrice(c.close)}`;
}

function updateLegendFromState() {
  const c = activeCandles();
  if (!c.length) return;
  updateLegend(c[c.length - 1]);
}

function activeCandles() {
  return state.cache[state.asset][state.chartInterval].candles;
}

/* ----- 1s -> chart interval aggregation (5s/30s/1m) ----- */
const CHART_SEC = { "5s": 5, "30s": 30, "1m": 60 };
function aggregateInterval(ones, sec) {
  const sorted = ones.slice().sort((a, b) => a.time - b.time);
  const out = [];
  for (const b of sorted) {
    const t = Math.floor(b.time / sec) * sec;
    const last = out[out.length - 1];
    if (last && last.time === t) {
      last.high = Math.max(last.high, b.high);
      last.low = Math.min(last.low, b.low);
      last.close = b.close;
      last.vol = (last.vol || 0) + (b.vol || 0);
    } else {
      out.push({ time: t, open: b.open, high: b.high, low: b.low, close: b.close, vol: b.vol || 0, openTime: t * 1000, closeTime: (t + sec) * 1000 });
    }
  }
  return out;
}
function rebuild5s(sym) {
  const ones = state.cache[sym]["1s"].candles;
  for (const tf of CHART_INTERVALS) {
    const sec = CHART_SEC[tf];
    const arr = aggregateInterval(ones, sec).slice(-HISTORY_CAP_5S);
    state.cache[sym][tf].candles = arr;
    state.cache[sym][tf].meta = arr[arr.length - 1] || null;
  }
}
function aggregate5s(ones) {
  return aggregateInterval(ones, CANDLE_SEC);
}
let CHART_UTILS = null;   // diisi dari dalam IIFE: linreg/detectSwings/avg/stdev/clamp + window per-koin

function sessionBounds(durMs, now) {
  const start = Math.floor(now / durMs) * durMs;
  return { start, end: start + durMs };
}
function pruneFlow(sym, keepFromSec) {
  const f = state.flow[sym];
  if (!f) return;
  for (const k in f) if (+k < keepFromSec - 3600) delete f[k];   // keep ~1h
}
// Cumulative executed order-flow imbalance over [t0Sec, nowSec). Returns null if no data.
function sessionOFI(sym, t0Sec, nowSec) {
  const f = state.flow[sym];
  if (!f) return null;
  let buy = 0, sell = 0, mins = 0;
  for (let t = t0Sec; t < nowSec; t += 60) {
    const o = f[t];
    if (o) { buy += o.buy; sell += o.sell; mins++; }
  }
  if (!mins) return null;
  const tot = buy + sell;
  return tot > 0 ? (buy - sell) / tot : 0;
}
/* PLAN TRADE ASSISTANT dari SERVER (diproses di server memakai trade-plan.js).
   Ambil hanya bila stream /api/live segar, tf-nya sama, dan sinyal snapshot masih sesi berjalan
   yang sama; kalau tidak -> null (klien menghitung sendiri sebagai cadangan offline). */
function serverPlanFor(asset, tf) {
  try {
    if (typeof LIVE === "undefined" || !LIVE.fresh()) return null;
    if (LIVE.tf !== tf) return null;
    const snap = LIVE.snap;
    const a = snap && snap.assets ? snap.assets[asset] : null;
    if (!a || !a.plan || !a.signal) return null;
    const dur = INTERVAL_MS[tf] || 300000;
    const t0 = Math.floor(serverNow() / dur) * dur / 1000;
    if (a.signal.t0 !== t0) return null;              // snapshot beda sesi -> abaikan
    return a.plan;
  } catch (_) { return null; }
}

/* OFI untuk TAMPILAN = metrik LIVE sesi berjalan (BUKAN nilai beku saat sinyal dikunci —
   sinyal dikunci ~2 detik setelah sesi mulai, saat itu flow masih kosong sehingga selalu null).
   Urutan sumber:
     1) angka LIVE dari snapshot server  -> paling benar & identik di semua device,
     2) hitungan flow lokal dari stream trade di browser ini (rumus sama), dan
     3) null -> UI menampilkan "—" (artinya belum ada data, bukan 0). */
function ofiForDisplay(asset, t0Sec, nowSec) {
  try {
    if (typeof LIVE !== "undefined" && LIVE.fresh()) {
      const a = LIVE.snap && LIVE.snap.assets ? LIVE.snap.assets[asset] : null;
      if (a && a.ofi != null) { const v = Number(a.ofi); if (isFinite(v)) return v; }
    }
  } catch (_) {}
  return sessionOFI(asset, t0Sec, nowSec);
}
function sessionLock(sym, durMs, now) {
  const startSec = Math.floor(sessionBounds(durMs, now).start / 1000);
  const arr = state.cache[sym]["5s"].candles;
  const exact = arr.find((c) => c.time === startSec);
  if (exact) return exact.open;
  const after = arr.find((c) => c.time >= startSec);
  if (after) return after.open;
  const last = arr[arr.length - 1];
  return last ? last.close : null;
}

/* ----------------------- History (REST) ----------------------- */
async function loadHistory() {
  const fetches = [];
  for (const sym of Object.keys(SYMBOLS)) {
    for (const tf of INTERVALS) {
      fetches.push(
        fetchJSON(`https://api.binance.com/api/v3/klines?symbol=${SYMBOLS[sym]}&interval=${tf}&limit=${HISTORY}`)
          .then((rows) => {
            const candles = rows.map((r) => ({
              time: Math.floor(r[0] / 1000),
              open: +r[1], high: +r[2], low: +r[3], close: +r[4],
              vol: +r[5], trades: +r[8],
              openTime: r[0], closeTime: r[6],
            }));
            state.cache[sym][tf].candles = candles;
            state.cache[sym][tf].meta = candles[candles.length - 1] || null;
          })
          .catch(() => {})
      );
    }
  }
  await Promise.all(fetches);
  renderActive();
  updateGap();
}

/* ----------------------- Lazy history (older 5s candles) ----------------------- */
// Load older 1m candles (expanded to 1s) when user scrolls chart to the left.
// Uses /api/klines proxy to avoid geo-blocking; fallback to direct Binance.
const HISTORY_COOLDOWN = 800; // ms antar trigger lazy-load
let historyLoading = { BTC: false, ETH: false };
let historyExhausted = { BTC: false, ETH: false };
let lastLoadAt = { BTC: 0, ETH: 0 };

function mergeOlder(sym, ones) {
  if (!ones || !ones.length) return 0;
  let added = 0;
  const s1 = state.cache[sym]["1s"];
  const have1 = new Set(s1.candles.map((c) => c.time));
  const fresh1 = ones.filter((c) => !have1.has(c.time));
  added += fresh1.length;
    s1.candles = fresh1.concat(s1.candles);
    if (s1.candles.length > HISTORY_CAP_1S * 12) s1.candles = s1.candles.slice(-HISTORY_CAP_1S * 12);  // keep ~240 min 1s for rebuild5s
    return added;
}

async function fetchOlder(sym, beforeSec, limit) {
  // Prefer REAL 1s klines (Binance spot supports interval=1s). Only fall back to
  // expanding 1m candles into synthetic 1s when the source returns coarse data.
  const histLimit = Math.min(limit, 1000);
  const toOnes = (candles) => {
    if (!candles || candles.length < 2) return candles || [];
    const gap = candles[1].time - candles[0].time;
    return gap > 1 ? expandTo1s(candles) : candles;
  };
  // 1) proxy server (same-origin)
  try {
    const r = await fetch(`/api/klines?symbol=${sym}&tf=1s&before=${beforeSec}&limit=${histLimit}`);
    if (r.ok) {
      const j = await r.json();
      if (j && Array.isArray(j.candles) && j.candles.length) return toOnes(j.candles);
    }
  } catch (e) { console.warn("[HISTORY] proxy fetch failed:", e); }
  // 2) fallback: direct Binance
  try {
    const rows = await fetchJSON(`https://api.binance.com/api/v3/klines?symbol=${SYMBOLS[sym]}&interval=1s&limit=${histLimit}&endTime=${beforeSec * 1000 - 1000}`);
    const candles = rows.map((r) => ({
      time: Math.floor(r[0] / 1000),
      open: +r[1], high: +r[2], low: +r[3], close: +r[4],
      vol: +r[5], trades: +r[8],
      openTime: r[0], closeTime: r[6],
    }));
    return toOnes(candles);
  } catch (e) { console.warn("[HISTORY] direct Binance fetch failed:", e); throw e; }
}

// Expand 1m candles into 60 one-second candles (same OHLC per second within the minute)
function expandTo1s(candles) {
  const ones = [];
  for (const c of candles) {
    const baseTime = c.time;       // start of minute (in seconds)
    for (let s = 0; s < 60; s++) {
      const t = baseTime + s;
      ones.push({ time: t, open: c.open, high: c.high, low: c.low, close: c.close, vol: (c.vol || 0) / 60, openTime: t * 1000, closeTime: (t + 1) * 1000 });
    }
  }
  return ones.sort((a, b) => a.time - b.time);
}

async function loadOlderCandles(sym, limit) {
  if (historyLoading[sym] || historyExhausted[sym]) return Promise.resolve();
  const store = state.cache[sym]["1s"];
  const oldest = store.candles.length ? store.candles[0].time : Math.floor(Date.now() / 1000);
  historyLoading[sym] = true;
  lastLoadAt[sym] = Date.now();
    setStatus("Loading previous session…");
  try {
    const ones = await fetchOlder(sym, oldest, limit);
    const added = mergeOlder(sym, ones);
    if (added === 0) historyExhausted[sym] = true;
    rebuild5s(sym);  // <-- rebuild 5s candles from updated 1s cache
    if (sym === state.asset) renderActive();
    hideStatus();
  } catch (e) {
    console.warn("[HISTORY] loadOlderCandles error:", e);
    hideStatus();
  } finally {
    historyLoading[sym] = false;
  }
}

// Install callback to chart: called when user reaches the left edge (needs older data)
function setReach() {
  if (!chart) return;
  chart.onReachStart = (done) => {
    const now = Date.now();
    if (historyExhausted[state.asset] || now - (lastLoadAt[state.asset] || 0) < HISTORY_COOLDOWN) {
      if (done) done();
      return;
    }
    loadOlderCandles(state.asset, HISTORY_LOAD).finally(() => { if (done) done(); });
  };
}

/* ----------------------- WebSocket ----------------------- */
let ws, wsRetry = 0, wsHostIdx = 0, wsGotData = false, usingTV = false, tvClient = null, binanceTries = 0;
function connectWS() {
  if (usingTV) return; // TradingView fallback already active
  const streams = [];
  for (const sym of Object.keys(SYMBOLS)) {
    streams.push(`${sym}@kline_1s`);
    for (const tf of INTERVALS) streams.push(`${sym}@kline_${tf}`);
  }
  for (const sym of Object.keys(SYMBOLS)) streams.push(`${sym}@ticker`);

  const host = WS_HOSTS[wsHostIdx % WS_HOSTS.length];
  const url = `${host}/stream?streams=${streams.join("/")}`;
  setSrc(host.replace("wss://", ""));
    setStatus(`Connecting to Binance (${host.replace("wss://", "")})…`);
  showErr(null);
  try { ws = new WebSocket(url); }
  catch (e) {
    scheduleWSRetry();
    return;
  }

  // open timeout: blackholed connections never fire onopen/onclose
  const openTimer = setTimeout(() => { try { ws.close(); } catch (_) {} }, 5000);
  // data timeout: opened but silent (no kline/ticker delivered)
  let dataTimer = null;

  ws.onopen = () => {
    clearTimeout(openTimer);
    state.connected = true; wsRetry = 0; wsGotData = false;
    setConn(true);
    dataTimer = setTimeout(() => { if (!wsGotData) { try { ws.close(); } catch (_) {} } }, 6000);
  };
  ws.onmessage = (ev) => {
    if (dataTimer) clearTimeout(dataTimer);
    wsGotData = true;
    const msg = JSON.parse(ev.data);
    const d = msg.data;
    if (!d) return;
    if (d.e === "kline") handleKline(d);
    else if (d.e === "24hrTicker") handleTicker(d);
  };
  ws.onclose = () => {
    clearTimeout(openTimer);
    if (dataTimer) clearTimeout(dataTimer);
    state.connected = false; setConn(false);
    scheduleWSRetry();
  };
  ws.onerror = () => {
    try { ws.close(); } catch (_) {}
  };
}
function scheduleWSRetry() {
  wsRetry++;
  binanceTries++;
  if (wsRetry % WS_HOSTS.length === 1) wsHostIdx++; // rotate host each full cycle
  if (usingTV) return; // already on TradingView fallback
  // After trying each Binance host with no data, fall back to TradingView
  if (!wsGotData && binanceTries >= WS_HOSTS.length) {
    startTV();
    return;
  }
  if (!wsGotData) {
    setStatus(`Binance gagal (${WS_HOSTS[wsHostIdx % WS_HOSTS.length].replace("wss://", "")}). Mencoba endpoint lain…`);
  }
  setTimeout(connectWS, Math.min(1500 * wsRetry, 6000));
}

function startTV() {
  usingTV = true;
  setSrc("tradingview (BINANCE)");
    setStatus("Binance blocked. Connecting to TradingView (BINANCE)…");
  showErr(null);
  let tvGotData = false;
  tvClient = connectTradingView({
    onStatus: (s) => setSrc(s),
    onHistory: (symKey, tf, candles) => {
      tvGotData = true; hideStatus();
      state.cache[symKey][tf].candles = candles;
      state.cache[symKey][tf].meta = candles[candles.length - 1] || null;
   if (tf === "1s") rebuild5s(symKey);
   hideStatus();

      if (symKey === state.asset && tf === "1s") { renderActive(); }
      updateGap();
    },
    onUpdate: (symKey, tf, candle) => {
      tvGotData = true; hideStatus();
      feedCandle(symKey, tf, candle);
    },
    onQuote: (symKey, info) => {
      tvGotData = true; hideStatus();
      state.prevPrice[symKey] = state.ticker[symKey] ? state.ticker[symKey].last : null;
      state.ticker[symKey] = { last: info.last, chg: info.chgPct };
      updateHeader();
      updateGap();
    },
  });
  // If TradingView also fails (blackholed), surface it after a grace period
  setTimeout(() => {
    if (!tvGotData) showErr("TradingView also failed to connect. Network blocks both sources — try VPN, or check internet connection.");
  }, 9000);
}
function setConn(on) {
  state.connected = !!on;   // single source of truth for every transport (WS, SSE, polling)
  const el = document.getElementById("conn");
  el.className = "conn " + (on ? "conn--on" : "conn--off");
}

function handleKline(d) {
  const symKey = d.s === "btcusdt" ? "BTC" : "ETH";
  const tf = d.k.i;
  const k = d.k;
  const candle = {
    time: Math.floor(k.t / 1000),
    open: +k.o, high: +k.h, low: +k.l, close: +k.c,
    vol: +k.v, trades: +k.n,
    openTime: k.t, closeTime: k.T,
  };
  feedCandle(symKey, tf, candle);
}

// Unified candle feeder for direct (Binance WS) and TradingView paths
function feedCandle(symKey, tf, candle) {
  const store = state.cache[symKey][tf];
  const arr = store.candles;
  const last = arr[arr.length - 1];
  if (last && last.time === candle.time) arr[arr.length - 1] = candle;
  else if (!last || candle.time > last.time) { arr.push(candle); if (arr.length > HISTORY_CAP_1S) arr.shift(); }
  store.meta = candle;
   if (tf === "1s") rebuild5s(symKey);

  if (symKey === state.asset && tf === "1s") { scheduleRender(); updateMobilePrediction(); }
  updateGap();
}

function handleTicker(d) {
  const symKey = d.s === "btcusdt" ? "BTC" : "ETH";
  state.prevPrice[symKey] = state.ticker[symKey] ? state.ticker[symKey].last : null;
  state.ticker[symKey] = { last: +d.c, chg: +d.P };
  hideStatus();
  updateHeader();
  updateGap();
}

/* ----------------------- Render active view ----------------------- */
function renderActive() {
  const candles = activeCandles();
  chart.setData(candles);
  applyType();
  updateProjection();
  updateLegendFromState();
}

function updateActiveSeries(candle) {
  chart.updateLast(candle);
}

function applyType() {
  chart.setType(state.type);
}

/* ----------------------- Projection (prediction session) ----------------------- */
  /* ----- momentum helpers (tailored to Binance Prediction: settlement price vs LOCK) ----- */
  function clamp(v, a, b) { return Math.max(a, Math.min(b, v)); }
  function avg(a) { return a.reduce((x, y) => x + y, 0) / a.length; }
  function stdev(a) { const m = avg(a); return Math.sqrt(avg(a.map((x) => (x - m) * (x - m)))); }

  function linreg(candles) {
    const n = candles.length;
    if (n < 2) return null;
    let sx = 0, sy = 0, sxx = 0, sxy = 0;
    for (const c of candles) { sx += c.time; sy += c.close; sxx += c.time * c.time; sxy += c.time * c.close; }
    const d = n * sxx - sx * sx;
    if (d === 0) return null;
    const b = (n * sxy - sx * sy) / d;   // slope: price / second
    const a = (sy - b * sx) / n;
    return { a, b };
  }
  function detectSwings(candles, left) {
    const L = left || 2, highs = [], lows = [];
    for (let i = L; i < candles.length - L; i++) {
      let isH = true, isL = true;
      for (let j = i - L; j <= i + L; j++) {
        if (j === i) continue;
        if (candles[j].high >= candles[i].high) isH = false;
        if (candles[j].low <= candles[i].low) isL = false;
      }
      if (isH) highs.push({ i, time: candles[i].time, price: candles[i].high });
      if (isL) lows.push({ i, time: candles[i].time, price: candles[i].low });
    }
    return { highs, lows };
  }
  // RSI on an arbitrary series (used on 5m candles → round-relevant & stable)
  function rsiFromSeries(candles, period) {
    return SignalCore.rsiFromSeries(candles, period);
  }
  // ANALYIS = window candle 5s terakhir (bukan lagi "entry window" ronde)
  const ANALYSIS_CANDLES = { "5m": 24, "15m": 60, "1h": 120 };
  function analysisWindow() {
    const five = state.cache[state.asset]["5s"].candles;
    const n = ANALYSIS_CANDLES[state.interval] || 120;
    return five.slice(-n);
  }
  // Ekspor ke scope modul -> dipakai buildOverlay() agar chart kolom DUAL (layar lebar)
  // menggambar overlay yang IDENTIK dengan chart utama (proyeksi/tren/marker).
  CHART_UTILS = {
    linreg, detectSwings, avg, stdev, clamp,
    analysisWindowFor: (asset) => {
      const five = state.cache[asset]?.["5s"]?.candles || [];
      const n = ANALYSIS_CANDLES[state.interval] || 120;
      return five.slice(-n);
    },
    swingLookbackFor: (asset) => {
      const five = state.cache[asset]?.["5s"]?.candles || [];
      const want = Math.round((ANALYSIS_CANDLES[state.interval] || 120) * 0.3);
      return Math.max(4, Math.min(want, Math.floor(five.length / 3)));
    },
  };
  // swing lookback = ~30% dari window analisis, dibatasi agar pivot tetap valid
  function swingLookback() {
    const five = state.cache[state.asset]["5s"].candles;
    const want = Math.round((ANALYSIS_CANDLES[state.interval] || 120) * 0.3);
    return Math.max(4, Math.min(want, Math.floor(five.length / 3)));
  }

// Generate dynamic entry reason based on analysis criteria
    function generateEntryReason(o) {
    // Locked reason (plain text) is always available from the engine.
    if (o.reason) return o.reason;

    if (o.verdict === "flat") {
      if (o.mode === "MENUNGGU") return "No entry. Waiting for the new session to start.";
      if (o.mode === "LOWVOL") return "No entry. Volume is below the minimum, entry too risky.";
      if (o.mode === "WARMUP") return "No entry. Warmup in progress, not enough data yet.";
      if (o.mode === "FILTERED-REVERSAL") return "No entry. A reversal was detected but not confirmed.";
      if (o.mode === "BLOCKED-GOAL") return "No entry. Continuation mode is blocked by policy.";
      if (o.zone && o.zone.indexOf("PEAK") >= 0) {
        return "No entry. Price is at " + o.zone + ", waiting for a " +
          (o.peakDir === "top" ? "down" : "up") + " reversal confirmation.";
      }
      return "No entry. Waiting for a qualifying signal.";
    }

    const reasons = [];
    if (o.mode && o.mode.indexOf("REVERSAL") === 0) reasons.push("Reversal at a detected peak");
    else if (o.mode === "CLOSE") reasons.push("Near settlement, price versus session open");
    else if (o.mode === "CONT") reasons.push("Trend continuation");
    else if (o.mode === "HIST-PREDICT") reasons.push("Historical trend of the last 50 sessions");
    else if (o.mode === "TREND") reasons.push("Price direction away from session open");
    else if (o.mode === "MOMENTUM") reasons.push("Momentum confirmed after warmup");

    if (o.peakPrice != null && o.peakDir) {
      reasons.push((o.peakDir === "top" ? "Top peak at " : "Bottom peak at ") + fmtPrice(o.peakPrice));
    }
    if (o.rsi != null) {
      if (o.rsi >= 70) reasons.push("RSI " + o.rsi.toFixed(1) + " overbought");
      else if (o.rsi <= 30) reasons.push("RSI " + o.rsi.toFixed(1) + " oversold");
      else reasons.push("RSI " + o.rsi.toFixed(1) + " neutral");
    }
    if (o.reward > 0) reasons.push("Reward " + (o.reward >= 0 ? "+" : "") + o.reward.toFixed(2) + " percent");
    if (o.vol5m != null) reasons.push("Volume 5m " + (o.vol5m >= 10 ? "at least 10x" : o.vol5m.toFixed(1) + "x"));

    const dirWord = o.verdict === "up" ? "UP" : "DOWN";
    return "Entry " + dirWord + ". " + reasons.join(". ") + ".";
  }

   
   function updateSignal(o) {
    const z = document.getElementById("s-zone");
    const m = document.getElementById("s-momentum");
    const r = document.getElementById("s-rsi");
    const pk = document.getElementById("s-peak");
    const rv = document.getElementById("s-reversal");
    const rw = document.getElementById("s-reward");
    const rec = document.getElementById("s-rec");
    const volEl = document.getElementById("s-vol");
    const vol5sEl = document.getElementById("s-vol5s");
    const liqEl = document.getElementById("s-liq");
    const ofiEl = document.getElementById("s-ofi");
    const confEl = document.getElementById("s-conf");
    const confBar = document.getElementById("s-conf-bar");
    const reasonEl = document.getElementById("entryReason");
    const statusEl = document.getElementById("calcStatus");
    
    // Live calculation status (when and how the signal is produced) — diringkas 1 baris.
    // Teks aslinya disimpan di tooltip supaya detail teknis tidak hilang.
    if (statusEl) {
      const cs = String(o.calcStatus || "");
      statusEl.textContent = cs
        .replace(/^Signal locked (\d+)s after session open · mode /, "lock +$1s · ")
        .replace(/^Evaluating session, open \+(\d+)s · /, "evaluasi +$1s · ");
      statusEl.title = cs;
    }
    // REASON ringkas: 1 baris (dot berwarna arah + label mode + vol/RSI/OFI).
    // Teks lengkap versi generateEntryReason() tetap dipasang sebagai tooltip.
    if (reasonEl) {
      reasonEl.innerHTML = shortReason(o);
      reasonEl.title = generateEntryReason(o);
    }
    if (z) { z.textContent = o.zone; z.className = o.zone.indexOf("ATAS") >= 0 ? "down" : o.zone.indexOf("BAWAH") >= 0 ? "up" : ""; }
    if (m) { m.textContent = o.momentum; m.className = o.momentum === "BULLISH" ? "up" : o.momentum === "BEARISH" ? "down" : ""; }
    if (r) {
      if (o.rsi == null) { r.textContent = "—"; r.className = ""; }
      else if (o.rsi >= 70) { r.textContent = "Overbought — Risk Down"; r.className = "down"; }
      else if (o.rsi <= 30) { r.textContent = "Oversold — Risk Up"; r.className = "up"; }
      else { r.textContent = "Neutral"; r.className = ""; }
    }
    if (pk) {
      if (o.peakPrice != null) { pk.textContent = (o.peakDir === "top" ? "▲ " : "▼ ") + fmtPrice(o.peakPrice); pk.className = o.peakDir === "top" ? "down" : "up"; }
      else { pk.textContent = "—"; pk.className = ""; }
    }
    if (rv) {
      if (o.verdict !== "flat" && o.mode.indexOf("REVERSAL") === 0) { rv.textContent = o.verdict === "up" ? "FADE UP" : "FADE DOWN"; rv.className = o.verdict === "up" ? "up" : "down"; }
      else { rv.textContent = "—"; rv.className = ""; }
    }
    if (rw) {
      if (o.reward && o.reward !== 0) { rw.textContent = (o.reward >= 0 ? "+" : "") + o.reward.toFixed(2) + "%"; rw.className = o.reward >= 0 ? "up" : "down"; }
      else { rw.textContent = "—"; rw.className = ""; }
    }
    if (confEl) confEl.textContent = o.verdict !== "flat" ? (o.conf || 0) + "%" : "—";
    if (confBar) {
      confBar.style.width = (o.verdict !== "flat" ? (o.conf || 0) : 0) + "%";
      confBar.className = "signal-conf-bar " + (o.verdict === "up" ? "up" : o.verdict === "down" ? "down" : "");
    }
    const fmtVol = (v) => v == null ? "—" : v >= 10 ? "≥10×" : v.toFixed(1) + "×";
    if (volEl) { volEl.textContent = fmtVol(o.vol5m); volEl.className = "vol-val " + volClass(o.vol5m); }      // 5m traded-volume pace (drives the grade)
    if (vol5sEl) { vol5sEl.textContent = fmtVol(o.vol5s); vol5sEl.className = "vol-val " + volClass(o.vol5s); }  // 5s-window traded volume vs ~5 min baseline
    if (liqEl) {
      const liq = o.liquidity || "NORMAL";
      liqEl.textContent = liq;
      liqEl.className = liq === "LOW" ? "down" : liq === "THIN" ? "warn" : liq === "—" ? "" : "up";
    }
    // OFI (order flow eksekusi) — ditonjolkan sejajar LIQUIDITY & VOL seperti di desktop.
    // Angka = OFI sesi; arah diwarnai (positif hijau = tekanan beli, negatif merah = jual).
    if (ofiEl) {
      const ov = o.ofi;
      ofiEl.textContent = ov != null ? (ov >= 0 ? "+" : "") + (ov * 100).toFixed(0) + "%" : "—";
      ofiEl.className = ov == null ? "na" : (ov >= 0 ? "up" : "down");
      const os = (() => { try { const e = (typeof LIVE !== "undefined") ? LIVE.entryFor(state.asset, state.interval) : null; return e && e.ofiShort != null ? e.ofiShort : null; } catch (_) { return null; } })();
      ofiEl.parentElement.title = ov == null
        ? "Order flow belum ada data"
        : `Order flow (eksekusi taker) sesi: ${(ov * 100).toFixed(1)}%` + (os != null ? ` · 2 menit terakhir: ${(os * 100).toFixed(1)}%` : "") +
          " — positif = tekanan BELI, negatif = tekanan JUAL";
    }
    if (rec) {
      if (o.analyzing) {
        rec.textContent = "Sedang Menganalisa";
        rec.className = "signal-rec flat";
      } else if (o.volWait) {
        rec.textContent = `Menunggu volume ${o.vol5m != null ? o.vol5m.toFixed(2) + "×" : "—"} / ${o.volNeed}×`;
        rec.className = "signal-rec flat";
      } else if (o.verdict === "flat") {
        rec.textContent = "No entry for this round";
        rec.className = "signal-rec flat";
      } else {
        const dirWord = o.verdict === "up" ? "UP" : "DOWN";
        rec.textContent = `Recommendation: ${dirWord}`;
        rec.className = "signal-rec " + (o.verdict === "up" ? "up" : o.verdict === "down" ? "down" : "flat");
      }
    }
    // Alert badge next to the recommendation (Masih Sesuai / Awas Melemah / Waspada / Sudah Berbalik)
    const recStatusEl = document.getElementById("rec-status");
    if (recStatusEl) {
      recStatusEl.textContent = o.recStatus || "";
      recStatusEl.className = "rec-status" + (o.recStatusClass ? " " + o.recStatusClass : "");
    }
    // TRADE ASSISTANT panel (entry zone / averaging levels / hold / close)
    const tpAction = document.getElementById("tp-action");
    const tpKey = document.getElementById("tp-key");
    const tpFull = document.getElementById("tp-full");
    const tpLevels = document.getElementById("tp-levels");
    const tpMeta = document.getElementById("tp-meta");
    const tpStEntry = document.getElementById("tp-st-entry");
    const tpStClose = document.getElementById("tp-st-close");
    const tpBias = document.getElementById("tp-bias");

    if (tpAction) {
      const tp = o.tradePlan;
      const se = tp && tp.statusEntry, sc = tp && tp.statusClose;
      // Chip harus muat 1 baris di mobile: isi dipendekkan, detail lengkap masuk tooltip (title).
      if (tpStEntry) {
        const ok = !!(se && se.ok);
        tpStEntry.className = "tp-st" + (ok ? " ok" : " wait");
        tpStEntry.innerHTML = `ENTRY: <b>${ok ? "SUCCESS" : "WAITING…"}</b>`;
        tpStEntry.title = ok
          ? `ENTRY SUCCESS · ${fmtClock(se.at)}${se.price != null ? " @ " + fmtPrice(se.price) : ""}`
          : `Menunggu entry — ${se && se.waiting ? se.waiting : "menunggu sinyal"}`;
      }
      if (tpStClose) {
        const ok = !!(sc && sc.ok);
        tpStClose.className = "tp-st" + (ok ? " ok" : " wait");
        tpStClose.innerHTML = `EARLY CLOSE: <b>${ok ? "SUCCESS" : "WAITING…"}</b>`;
        tpStClose.title = ok
          ? `EARLY CLOSE SUCCESS · ${fmtClock(sc.at)}`
          : (se && se.ok ? "Posisi terbuka — menunggu sinyal close" : "Belum ada posisi");
      }
      if (!tp) {
        tpAction.textContent = "—"; tpAction.className = "tp-action wait";
        if (tpKey) { tpKey.textContent = ""; tpKey.className = "tp-key"; }
        if (tpFull) tpFull.textContent = "";
        if (tpBias) { tpBias.textContent = ""; tpBias.className = "tp-bias"; }
        if (tpLevels) tpLevels.textContent = "";
        if (tpMeta) tpMeta.textContent = "";
      } else {
        // Sinyal (arah close sesi) vs arah trade (contra-lock: selalu menuju LOCK) — supaya
        // tidak membingungkan ketika keduanya berlawanan arah.
        if (tpBias) {
          const sd = String(o.verdict || "flat").toUpperCase();
          const isUpSig = o.verdict === "up";
          const lockTxt = fmtPrice(tp.levels ? tp.levels.target : null);
          tpBias.className = "tp-bias " + (o.verdict || "flat");
          tpBias.innerHTML = `SINYAL <b>${esc(sd)}</b> · entry <b>contra-lock</b>: tunggu harga <b>${isUpSig ? "DI BAWAH" : "DI ATAS"} LOCK</b> (${isUpSig ? "beli" : "jual"}) · close setelah melewati LOCK`;
          tpBias.title = `Arah posisi = arah rekomendasi (sumber tunggal: main signal). LOCK ${lockTxt}. ENTRY hanya saat harga CONTRA-LOCK (${isUpSig ? "di bawah" : "di atas"} LOCK), CLOSE saat harga sudah searah rekomendasi & melewati LOCK (${isUpSig ? "di atas" : "di bawah"} LOCK).`;
        }
        tpAction.textContent = shortAction(tp.action);
        tpAction.className = "tp-action " + (tp.cls || "wait");
        const tpTitle = [tp.action, tp.reason, tp.why && tp.why.length ? "alasan: " + tp.why.join(", ") : ""]
          .filter(Boolean).join(" — ");
        if (tpTitle) tpAction.title = tpTitle;
        // Baris harga kunci: 1 baris, hanya 2 angka yang benar-benar dipakai user.
        if (tpKey) {
          const lv = tp.levels || {};
          const inPos = tp.entryPrice != null;
          const rw = lv.rNow != null ? ` <i>(+${lv.rNow.toFixed(2)}% ke target)</i>` : "";
          tpKey.className = "tp-key " + (tp.cls || "wait");
          tpKey.innerHTML = inPos
            ? `<span>POSISI <b>@${fmtPrice(tp.entryPrice)}</b>${o.recPnl != null ? ` <i>(${o.recPnl >= 0 ? "+" : ""}${o.recPnl.toFixed(2)}%)</i>` : ""}</span>` +
              `<span>TARGET <b>${fmtPrice(lv.target)}</b>${rw}</span>`
            : `<span>ENTRY <b>${fmtPrice(lv.l1)}</b></span>` +
              `<span>TARGET <b>${fmtPrice(lv.target)}</b>${rw}</span>`;
          tpKey.title = inPos
            ? `Posisi aktif @${fmtPrice(tp.entryPrice)} · target = LOCK (harga open sesi) ${fmtPrice(lv.target)}`
            : `Entry contra-lock di sekitar ${fmtPrice(lv.l1)} · target = LOCK (harga open sesi) ${fmtPrice(lv.target)}`;
        }
        if (tpFull) tpFull.textContent = tp.action;
        if (tpLevels) {
          tpLevels.innerHTML = tp.levels
            ? `<span title="Zona entry (${tp.levels.r1.toFixed(2)}% dari LOCK, sisi contra) — entry boleh dilakukan begitu harga melewati LOCK, tidak perlu menunggu level ini">ENTRY L1 <b>${fmtPrice(tp.levels.l1)}</b> <i>(+${tp.levels.r1.toFixed(2)}%)</i></span>` +
              `<span title="Zona TAMBAH 1 (${tp.levels.r2.toFixed(2)}% dari LOCK) — hanya bila harga turun/naik lebih jauh">TAMBAH L2 <b>${fmtPrice(tp.levels.l2)}</b> <i>(+${tp.levels.r2.toFixed(2)}%)</i></span>` +
              `<span title="Zona TAMBAH 2 (${tp.levels.r3.toFixed(2)}% dari LOCK)">TAMBAH L3 <b>${fmtPrice(tp.levels.l3)}</b> <i>(+${tp.levels.r3.toFixed(2)}%)</i></span>` +
              `<span title="Target = LOCK (harga open sesi)">TARGET (LOCK) <b>${fmtPrice(tp.levels.target)}</b></span>`
            : "";
        }
        if (tpMeta) {
          const ofiTxt = o.ofi != null ? `OFI ${(o.ofi * 100).toFixed(0)}%` : "OFI —";
          const adv = tp.adverseStd != null ? `${tp.adverseStd >= 0 ? "-" : "+"}${Math.abs(tp.adverseStd).toFixed(2)}σ` : "—";
          const pos = tp.entryPrice != null
            ? ` · posisi @${fmtPrice(tp.entryPrice)} (${o.recPnl != null ? (o.recPnl >= 0 ? "+" : "") + o.recPnl.toFixed(2) + "%" : "—"})`
            : " · belum ada posisi";
          const rwNow = tp.levels && tp.levels.rNow != null ? ` · reward saat ini +${tp.levels.rNow.toFixed(2)}%` : "";
          tpMeta.textContent = `Momentum ${tp.fs} · jarak lock ${adv}${rwNow} · ${ofiTxt}${pos}${tp.why.length ? " · " + tp.why.join(", ") : ""}`;
        }
      }
    }
    
    // Trigger alarm otomatis ketika sinyal entry muncul. Dedupe PER SESI (key) supaya satu
    // sinyal hanya berbunyi sekali walau verdict sempat berubah-ubah dalam sesi yang sama.
    const alertKey = `${state.asset}_${state.interval}_${o.roundStart || ""}`;
    // PRIMING pengamatan pertama: kalau sesi ini baru pertama kali terlihat (mis. halaman baru
    // dibuka padahal sinyalnya sudah terkunci beberapa detik lalu), JANGAN bunyikan alarm —
    // kartu sudah menampilkannya. Alarm hanya untuk sinyal yang MUNCUL selagi user ada.
    const sigFirstSight = !_sigFirstSight.has(alertKey);
    _sigFirstSight.add(alertKey);
    if (sigFirstSight) {
      // Pengamatan PERTAMA untuk sesi ini (mis. halaman baru dibuka padahal sinyalnya sudah
      // terkunci beberapa detik lalu): tandai "sudah diberitahukan" tanpa bunyi, supaya tick-tick
      // berikutnya tidak ikut berbunyi untuk sinyal yang sudah tampil di kartu sejak awal.
      if (o.verdict !== "flat" && o.highConf) _mainSigAlerted.add(alertKey);
    } else if (o.verdict !== "flat" && confMode === "SIGNAL" && o.highConf && !_mainSigAlerted.has(alertKey)) {
      _mainSigAlerted.add(alertKey);
      playSoundAlert();
      flashCard(state.asset, "signal");
      console.log("[ALERT] High-confidence signal:", o.verdict, o.mode, o.gateWr);
    }
    lastSignalState = { verdict: o.verdict, mode: o.mode, highConf: !!o.highConf };
  }

  function updateConfidenceDisplay(dir, val) {
    const dirEl = document.getElementById("conf-dir");
    const valEl = document.getElementById("conf-val");
    const track = document.getElementById("conf-track");
    if (!dirEl || !track) return;
    if (!dir) {
      dirEl.textContent = "—"; if (valEl) valEl.textContent = "—";
      clearLedBar(track);
      return;
    }
    dirEl.textContent = dir === "down" ? "DOWN" : "UP";
    if (valEl) valEl.textContent = val + "%";
    paintLedBar(track, val);   // implementasi bersama (lihat paintLedBar di scope global)
  }

  // Distribusi per-candle volume trailing (~15 menit) utk threshold LIQUIDITAS dinamis.
  function volDistribution(sym, five, winLen) {
    const N = 180; // ~15 menit (candle 5s)
    const start = Math.max(0, five.length - N - winLen);
    return five.slice(start, five.length - winLen)
      .map((c) => c.vol || 0)
      .filter((v) => v > 0);
  }
  // Persentil ke-p dari array (0..100). Array kosong -> 0.
  function percentile(arr, p) {
    if (!arr.length) return 0;
    const s = arr.slice().sort((a, b) => a - b);
    const idx = Math.min(s.length - 1, Math.max(0, Math.floor((p / 100) * (s.length - 1))));
    return s[idx];
  }

  // Coalesce the hottest render path (per-trade / per-1s-candle) so updateProjection
  // runs at most ~4x/second instead of on every event. User actions still call
  // updateProjection() directly for instant feedback.
  let _renderTimer = null;
  function scheduleRender() {
    if (_renderTimer) return;
    _renderTimer = setTimeout(() => {
      _renderTimer = null;
      chart.setData(activeCandles());
      updateProjection();
      renderDual();          // layar lebar: kolom dual ikut real-time (throttle internal 250ms)
    }, 250);
  }

  function updateProjection() {
    const dur = INTERVAL_MS[state.interval];
    const now = serverNow();
    const bounds = sessionBounds(dur, now);
    const t0 = bounds.start, T = bounds.end;
    const O = sessionLock(state.asset, dur, now);
    const five = state.cache[state.asset]["5s"].candles;
    const last = five[five.length - 1];
    if (!O || !last) return;
    const C = last.close;
    const elapsed = now - t0;
    const remaining = T - now;
    const total = T - t0;

    // Lock (open) line + session dividers
    chart.setDecision(O);
    chart.setSessionDuration(dur);
    const liveStatus = C > O ? "up" : C < O ? "down" : "flat";

    // ----- window analisis = candle 5s terakhir (tanpa gating "entry window") -----
    const win = analysisWindow();                   // window candle 5s terakhir
    // ----- volume / likuiditas (realtime dari klines) -----
    const winLen = win.length;
    const winVol = win.reduce((a, c) => a + (c.vol || 0), 0);
    const trail = five.slice(-(60 + winLen), -winLen);     // ~60 candle sebelum window
    const useTrail = trail.length >= 10 ? trail : five.slice(-60);
    const baseMean = avg(useTrail.map((c) => c.vol || 0));
    const hasVolData = baseMean > 0 || winVol > 0;          // false bila sumber tdk kirim volume (mis. TV fallback)
    const baseWinVol = baseMean * winLen;
    const volRel = baseWinVol > 0 && hasVolData ? winVol / baseWinVol : 1;
    // baseline absolut per-aset: agar pasar sepi/flat (vol rendah di mana pun) tetap terdeteksi
    const typ = VOL_TYPICAL[state.asset];
    const winVolPerCandle = winVol / winLen;
    const absRel = typ ? winVolPerCandle / typ : 1;
    const rel = hasVolData ? Math.min(volRel, absRel) : 1;   // paling konservatif; 1 bila tdk ada data
    // Threshold LIQUIDITAS dinamis (persentil rolling per-candle vol) — adaptif per aset & regime,
    // dibarengi floor absolut (VOL_TYPICAL) agar pasar mati tetap terflag LOW.
    let liquidity = "NORMAL";
    if (hasVolData) {
      const series = volDistribution(state.asset, five, winLen);  // per-candle vol ~15 menit terakhir
      // ambang = persentil dinamis, dibatasi (cap) agar tak over-suppress di regime volatil tinggi
      const lowThresh = Math.min(Math.max(percentile(series, 15), typ * 0.3), typ * 1.5);
      const thinThresh = Math.min(Math.max(percentile(series, 40), typ * 0.6), typ * 2.5);
      if (winVolPerCandle < lowThresh) liquidity = "LOW";
      else if (winVolPerCandle < thinThresh) liquidity = "THIN";
    } else {
      liquidity = "—";   // volume tdk tersedia → jangan menekan sinyal
    }
    const reg = linreg(win);
    const slope = reg ? reg.b : 0;                 // price / second
    const closes = win.map((c) => c.close);
    const mean = avg(closes);
    const std = stdev(closes) || 1;
    const z = (C - mean) / std;                    // how stretched price is within the window

    const nowSec = Math.floor(now / 1000);
    const closeSec = Math.floor(T / 1000);

    // auto trend line (reversal-aware) dihitung SETELAH peak terdeteksi — lihat blok di bawah

    // ----- proyeksi + tren + marker (SATU sumber bersama kolom dual: buildOverlay) -----
    const ov = buildOverlay(win, { C, O, std, slope, nowSec, closeSec, remainingMs: remaining, swingLookback: swingLookback() });
    const projectedClose = ov ? ov.projectedClose : C;
    const contDir = ov ? ov.contDir : "flat";
    let peak = ov ? ov.peak : null;
    if (ov) { chart.setProjection(ov.projection); chart.setTrendFit(ov.trendFit); chart.setMarkers(ov.markers); }



    // ----- signal components -----
    const momentum = reg ? (slope > 0 ? "BULLISH" : slope < 0 ? "BEARISH" : "FLAT") : "FLAT";
    const rsi = rsiFromSeries(state.cache[state.asset]["5m"].candles, 14);

    const tol = Math.max(O * 0.001, std * 0.5);
    const winHigh = Math.max.apply(null, win.map((c) => c.high));
    const winLow = Math.min.apply(null, win.map((c) => c.low));
    // only the ACTUAL top/bottom of the decision window counts as a peak to fade
    const isTopPeak = peak && peak.dir === "top" && peak.price >= winHigh - 1e-6;
    const isBotPeak = peak && peak.dir === "bot" && peak.price <= winLow + 1e-6;
    const nearTop = isTopPeak && Math.abs(C - peak.price) < tol;
    const nearBot = isBotPeak && Math.abs(C - peak.price) < tol;
    const droppedFromPeak = isTopPeak && C <= peak.price - tol;   // already turning down off the top
    const roseFromPeak = isBotPeak && C >= peak.price + tol;       // already turning up off the bottom
    const overbought = z > 1.6;
    const oversold = z < -1.6;
    const rollOver = slope < 0;
    const turnUp = slope > 0;

    // ----- konfirmasi 2-3 candle setelah peak bergerak searah pembalikan -----
    // Menyaring false peak (spike 1 candle): butuh >=2 dari 3 candle pasca-peak
    // yang benar-benar bergerak ke arah reversal (close<open utk turun, sebaliknya).
    let peakConf = false;
    if (peak) {
      const pIdx = win.findIndex((c) => c.time === peak.time);
      if (pIdx >= 0 && pIdx < win.length - 1) {
        const last3 = win.slice(pIdx + 1).slice(-3);
        let sameDir = 0;
        for (const c of last3) {
          const bear = c.close < c.open, bull = c.close > c.open;
          if (peak.dir === "top" && bear) sameDir++;
          else if (peak.dir === "bot" && bull) sameDir++;
        }
        peakConf = last3.length >= 2 && sameDir >= 2;
      }
    }

    // TREND (3 sesi interval aktif) sbg bias tren — jangan fade kalau trend berlawanan arah
    const tr = sessionTrend(state.asset, state.interval, TREND_SESSIONS);
    const trendBias = tr === "bullish" ? "up" : tr === "bearish" ? "down" : "flat";
    
    // Historical trend analysis (50 sessions) - for confidence boost & prediction override
    const histTrend = analyzeHistoricalTrend(state.asset, state.interval, 50);
    
    let zone = "NETRAL";
    if (isTopPeak) zone = nearTop ? "TOP PEAK" : "NEAR TOP PEAK";
    else if (isBotPeak) zone = nearBot ? "BOTTOM PEAK" : "NEAR BOTTOM PEAK";
    else if (z > 1.2) zone = "NEAR TOP PEAK";
    else if (z < -1.2) zone = "NEAR BOTTOM PEAK";

    // ----- decision: FADE the CONFIRMED peak (titik balik) — never force a signal -----
    let verdict = "flat", mode = "CONT", peakPrice = null, reward = 0;
    if (isTopPeak && rollOver && (nearTop || droppedFromPeak || overbought) && peakConf && trendBias !== "up") { verdict = "down"; mode = "REVERSAL↓"; peakPrice = peak.price; }
    else if (isBotPeak && turnUp && (nearBot || roseFromPeak || oversold) && peakConf && trendBias !== "down") { verdict = "up"; mode = "REVERSAL↑"; peakPrice = peak.price; }
    else { verdict = contDir; mode = "CONT"; }

    // reward = move captured by fading the peak to the projected close
    if (peakPrice != null && verdict !== "flat") {
      reward = verdict === "down" ? (peakPrice - projectedClose) / peakPrice * 100
                                : (projectedClose - peakPrice) / peakPrice * 100;
    }

    // near settlement the literal win condition (price vs LOCK now) dominates
    if (remaining < 20000) {
      verdict = C > O ? "up" : C < O ? "down" : verdict;
      mode = "CLOSE";
    }

    // ----- AGGRESSIVE ENTRY FILTER -----
    const aligned = trendBias !== "flat" && ((trendBias === "up" && slope > 0) || (trendBias === "down" && slope < 0));
    
    // Simpan verdict & mode asli untuk race-condition-free GOAL logic
    const verdictBeforeFilter = verdict;
    const modeBeforeFilter = mode;
    
    const isReversalMode = mode.indexOf("REVERSAL") === 0;
    const isHistPredictMode = mode === "HIST-PREDICT";
    const hasPeakConf = peakConf === true;
    const histAligns = histTrend && histTrend.predictDir === verdict;
    const hasVolumeSpikes = (typeof rel === 'number' && rel > 1.3) || (typeof volRel === 'number' && volRel > 1.3);
    const hasRsiExtreme = rsi !== null && (rsi < 35 || rsi > 75);
    
    // QUALIFICATION: REVERSAL requires strong conditions, HIST-PREDICT can be lighter
    let isQualified;
    if (isReversalMode) {
      isQualified = hasPeakConf && (
        (histTrend && histTrend.strength > 70 && histTrend.momentum && histAligns) ||
        (hasVolumeSpikes && hasRsiExtreme)
      );
    } else if (isHistPredictMode) {
      // HIST-PREDICT: just needs valid signal with minimum strength
      isQualified = histTrend && histTrend.predictDir !== "flat" && histTrend.strength > 35;
    } else {
      // Continuation mode - always qualified
      isQualified = true;
    }
    
    let finalVerdict;
    if (isQualified) {
      finalVerdict = verdict;
    } else {
      finalVerdict = "flat";
      if (isReversalMode) mode = "FILTERED-REVERSAL";
    }

    // ----- GOAL: penguat sinyal hanya untuk counter-trend -----
    let goalHit = false;
    if (GOAL.mode === "REVERSAL_ONLY" && finalVerdict !== "flat") {
      if (modeBeforeFilter.indexOf("REVERSAL") === 0) {
        goalHit = true;
      } else {
        const isContinuation = finalVerdict === liveStatus;
        if (isContinuation) {
          finalVerdict = "flat";
          mode = "BLOCKED-GOAL";
        } else {
          goalHit = true;
        }
      }
    }
    
    // ----- WARMUP -----
    if (elapsed < WARMUP_MS && finalVerdict !== "flat") {
      finalVerdict = "flat";
      mode = "WARMUP";
    }

// ----- LOW VOLUME: likuiditas tipis → entry berisiko, tekan ke NETRAL
  if (liquidity === "LOW" && finalVerdict !== "flat") {
    finalVerdict = "flat";
    mode = "LOWVOL";
  }
  
  // Historical trend override - hanya bila trend ekstrem + momentum presence
  if (histTrend && histTrend.strength > 85 && histTrend.momentum) {
    if (histTrend.predictDir !== 'flat' && finalVerdict === "flat") {
      finalVerdict = histTrend.predictDir;
      mode = "HIST-PREDICT";
    }
  }
  
  // ----- confidence: distance from LOCK + TREND (3 sesi) + momentum -----
  // Hitung confidence SEBELUM membuat _deskSig agar tersedia untuk lock
  let conf = 0;
  if (finalVerdict !== "flat") {
    const edge = Math.abs(C - O) / std;
    const edgeScore = clamp(edge / 2, 0, 1) * 35;
    const htfScore = (trendBias === "flat") ? 0 : 20;
    const remSec = Math.max(0, remaining / 1000);   // sisa detik sesi (dipakai momScore; blok fade punya salinannya sendiri)
    const momScore = clamp(Math.abs(slope) * remSec / std, 0, 1) * 15;
    conf = Math.round(edgeScore + htfScore + momScore);
    
    // Historical trend momentum boost (max +20)
    if (histTrend && histTrend.momentum && histTrend.strength > 60) {
      const boost = Math.min(20, Math.round(histTrend.strength / 10));
      conf = Math.min(100, conf + boost);
    }
    
    if (mode === "CLOSE") conf = Math.min(100, conf + 10);
    if (mode.indexOf("REVERSAL") === 0) conf = Math.min(100, conf + 5);
    if (goalHit) conf = Math.min(100, conf + GOAL.boost);
  }

  // ----- SESSION-START LOCK: signal hanya dihitung saat sesi dimulai, kemudian lock -----
  // Reset _deskSig ketika sesi berubah (asal sudah cukup data, >15s)
  const sessionStart = t0;
  const sessionChanged = !_deskSig || _deskSig.roundStart !== sessionStart || _deskSig.asset !== state.asset || _deskSig.interval !== state.interval;
  
  if (sessionChanged && elapsed > 15000) {
    // Plain-text reason via shared builder (desktop fallback path)
    const reason = SignalCore.buildReason({
      verdict: finalVerdict,
      mode: mode,
      rsi: rsi,
      volRel: typeof rel === "number" ? rel : null,
      strength: histTrend?.strength,
      momentum: histTrend?.momentum,
      elapsedSec: elapsed / 1000,
    });
    
    _deskSig = {
      roundStart: sessionStart,
      asset: state.asset,
      interval: state.interval,
      verdict: finalVerdict,
      mode: mode,
      reason: reason,
      conf: conf,
      timestamp: now,
    };
  }
  
  // Recommendation source: the calibrated universal engine ONLY, which is also the source
  // that gets captured into history. This guarantees the displayed verdict always matches
  // the entry that is later scored (no desktop-engine mismatch).
  let gateInfo = null;
  const uniKey = `${state.asset}_${state.interval}_${sessionStart}`;
  let uni = _deskSigCache[uniKey];                // graded EARLY lock lokal (cadangan)
  const srvSig = LIVE.signalFor(state.asset, state.interval);   // SUMBER KEBENARAN: sinyal server
  if (srvSig) uni = Object.assign({}, uni || {}, srvSig, { verdict: srvSig.dir });
  const liveSig = uni || _deskSigLive[uniKey] || null;
  if (uni && uni.verdict !== "flat") {
    finalVerdict = uni.verdict;
    mode = uni.mode;
    conf = uni.conf;
    gateInfo = gateLookup(gateKey(state.interval, uni.mode, uni.verdict, uni.rsi, uni.histStrength));
  } else {
    finalVerdict = "flat";
    mode = liveSig ? liveSig.mode : "MENUNGGU";
    conf = 0;
  }
  const elapsedSec = Math.round((now - sessionStart) / 1000);
  // Signal HEALTH (multi-criteria) shown as a badge next to the recommendation.
  let recStatus = "", recStatusClass = "";
  let tradePlan = null;
  if (uni) {
    const isUp = uni.verdict === "up";
    const st = computeLockStatus(uniKey, uni.verdict, O, C, now);
    // price position and margin, normalised by volatility
    const margin = isUp ? (C - O) : (O - C);                       // >0 = still on our side
    const marginStd = std > 0 ? Math.abs(C - O) / std : null;
    // momentum + deceleration from the 5s window
    const seg = win.slice(-Math.max(3, Math.ceil(winLen / 3)));
    const regRecent = seg.length >= 3 ? linreg(seg) : null;
    const slopeRecent = regRecent ? regRecent.b : slope;
    // executed order flow: session cumulative + short (last 2 minutes) window
    const nowSec2 = Math.floor(now / 1000);
    const ofiShort = sessionOFI(state.asset, nowSec2 - 120, nowSec2);
    // share of counter-direction volume in the last ~60s of 5s candles (1.0 = neutral)
    const recentC = win.slice(-12);
    const counterVol = recentC.filter((c) => (isUp ? c.close < c.open : c.close > c.open)).reduce((a, c) => a + (c.vol || 0), 0);
    const totVol = recentC.reduce((a, c) => a + (c.vol || 0), 0);
    const volAgainst = totVol > 0 ? (counterVol / totVol) / 0.5 : null;
    const health = computeSignalHealth(uni.verdict, {
      margin, marginStd, slope, slopeRecent,
      ofi: uni.ofi, ofiShort, volAgainst,
      rsi, z,
      peakAgainst: isUp ? (isTopPeak && peakConf) : (isBotPeak && peakConf),
      histTrend,
    });
    const dwell = (st && st.state === "AGAINST") ? ` ${Math.round((now - st.since) / 1000)}s` : "";
    recStatus = `${health.label}${health.score ? ` ${health.score}` : ""}${dwell}`;
    recStatusClass = healthClass(health.label);
    // TRADE ASSISTANT plan (entry zone / averaging / hold / close) for the active combo.
    // PENTING: seluruh input TA memakai arah TRADE (contra-lock = menuju LOCK), bukan arah
    // sinyal. Sinyal hanya menentukan SISI harga (di bawah/atas LOCK) -> arah trade kebalikannya.
    const taBiasNow = tradeDirOf(uni.verdict);   // arah posisi = arah rekomendasi
    const taUp = taBiasNow === "up";
    const pk = _tradePeak[uniKey] || -Infinity;
    const favorNow = taUp ? (C - O) : (O - C);
    const peakFavor = Math.max(pk, favorNow);
    _tradePeak[uniKey] = peakFavor;
    const retreat = peakFavor > 0 && (peakFavor - favorNow) >= 0.25 * Math.max(std, 1e-9);
    const entryRec = _tradeEntered[uniKey];
    const wasEntered = !!(entryRec && entryRec.entered);
    // Confirmation evidence + how long it has persisted (dwell), so signals are neither
    // too fast (single noisy tick) nor too late.
    const turn = turnEvidence(taUp, { slope, slopeRecent, ofiShort, win });
    const fade = fadeEvidence(taUp, { slope, slopeRecent, ofiShort, retreat, rsi });
    const dw = _tradeDwell[uniKey] || (_tradeDwell[uniKey] = { turnSince: null, fadeSince: null });
    dw.turnSince = turn.count >= 2 ? (dw.turnSince || now) : null;
    dw.fadeSince = fade.count >= 2 ? (dw.fadeSince || now) : null;
    const dwellTurnMs = dw.turnSince ? now - dw.turnSince : 0;
    const dwellFadeMs = dw.fadeSince ? now - dw.fadeSince : 0;
    const taBias = taBiasNow;                      // = arah rekomendasi (entry contra-lock)
    // ===== SUMBER PLAN: SERVER LEBIH DULU =====
    // Kartu (ENTRY/EARLY CLOSE) dan riwayat harus membaca state yang SAMA. Sebelumnya jalur ini
    // hanya menghitung plan LOKAL, sehingga kartu bisa menampilkan 'ENTRY SUCCESS' dari state
    // lokal padahal server (yang dipakai panel riwayat/ledger) tidak pernah entry -> riwayat
    // tampil abu sementara kartu hijau. Sekarang server jadi acuan; lokal hanya cadangan.
    const srvPlan = taBias ? serverPlanFor(state.asset, state.interval) : null;
    if (srvPlan) {
      tradePlan = srvPlan;
      if (tradePlan.entryPrice != null && !_tradeEntered[uniKey]) {
        _tradeEntered[uniKey] = { entered: true, since: tradePlan.statusEntry ? tradePlan.statusEntry.at : now, price: tradePlan.entryPrice };
      }
      if (!tradePlan.entered) { delete _tradeEntered[uniKey]; delete _tradeClosed[uniKey]; _closeSounded.delete(uniKey); }
      if (tradePlan.statusClose && tradePlan.statusClose.ok && !_tradeClosed[uniKey]) {
        _tradeClosed[uniKey] = { at: tradePlan.statusClose.at, price: tradePlan.statusClose.price };
      }
    } else {
    const trail = taBias ? trailOf(state.asset, Math.floor(sessionStart / 1000), Math.floor(now / 1000), O, taBias === "up") : null;
    tradePlan = taBias ? computeTradePlan(taBias, {
      tf: state.interval, lock: O, price: C, std, slope, slopeRecent, rsi, z,
      ofi: uni.ofi, ofiShort, retreat, health, entered: wasEntered,
      turn, fade, dwellTurnMs, dwellFadeMs, histTrend, trail,
    }) : null;
    if (tradePlan && !tradePlan.entered) { delete _tradeEntered[uniKey]; delete _tradeClosed[uniKey]; _closeSounded.delete(uniKey); }   // no position yet / stand down
    if (tradePlan) {
    if (tradePlan.entered && !wasEntered) {
      _tradeEntered[uniKey] = { entered: true, since: now, price: C };
      console.log(`[TRADE] position opened ${state.asset}/${state.interval} @ ${fmtPrice(C)}`);
    }
    tradePlan.entryPrice = _tradeEntered[uniKey] ? _tradeEntered[uniKey].price : null;
    // Catat kapan early-close (state CLOSE) pertama kali muncul pada sesi ini, lalu tempelkan
    // STATUS ke plan supaya panel Trade Assistant bisa menampilkan ENTRY / EARLY CLOSE:
    //   SUCCESS (sudah terjadi, dengan jam) atau WAITING (belum).
    if (tradePlan.state === "CLOSE" && !_tradeClosed[uniKey]) {
      _tradeClosed[uniKey] = { at: now, price: C };
      console.log(`[TRADE] early close disinyalkan ${state.asset}/${state.interval} @ ${fmtPrice(C)}`);
    }
    {
      const ent = _tradeEntered[uniKey], clo = _tradeClosed[uniKey];
      tradePlan.statusEntry = {
        ok: !!ent, at: ent ? ent.since : null, price: ent ? ent.price : null,
        waiting: tradePlan.state === "STAND_DOWN"
          ? "reversal terdeteksi — tunggu setup baru"
          : (tradePlan.state === "WAIT" && /TUNGGU PEAK/.test(tradePlan.action) ? "konfirmasi peak contra (2/4 bagian + 4s)" : "harga belum contra / belum kembali ke lock"),
      };
      tradePlan.statusClose = { ok: !!clo, at: clo ? clo.at : null, price: clo ? clo.price : null };
    }
    }
    }
    // State-transition alerts with DISTINCT sounds:
    //   entry/average -> rising chirp ; close/cut -> descending chime ; signal entry -> pulsing (existing)
    // First observation of a combo is silent, so switching tabs does not replay a sound.
    const prevTradeState = _tradeLastState[uniKey];
    const prevTradeSrc = _tradeLastSrc[uniKey];
    const planSrc = srvPlan ? "server" : "local";
    _tradeLastState[uniKey] = tradePlan.state;
    _tradeLastSrc[uniKey] = planSrc;
    // Transisi hanya dihitung bila SUMBER plan tidak berubah. Saat halaman baru dibuka, plan
    // sempat dihitung lokal (snapshot server belum tiba) lalu berganti ke plan server — dulu itu
    // terdeteksi sebagai transisi sehingga muncul notif Trade Assistant "palsu" tepat setelah load.
    if (prevTradeState !== undefined && prevTradeSrc === planSrc && tradePlan.state !== prevTradeState) {
      if (tradePlan.state === "ENTRY" || tradePlan.state === "AVERAGE") {
        const sk = `${uniKey}|${tradePlan.state}`;
        if (!_entrySounded.has(sk)) { _entrySounded.add(sk); playTradeEntrySound(); }
        flashCard(state.asset, "entry");
        console.log(`[TRADE][SOUND-ENTRY] ${tradePlan.state} ${state.asset}/${state.interval}: ${tradePlan.action}`);
        flashTitle(`▶ ${tradePlan.action} ${state.asset}/${state.interval}`);
      } else if (tradePlan.state === "CLOSE" || tradePlan.state === "STAND_DOWN") {
        if (!_closeSounded.has(uniKey)) {
          _closeSounded.add(uniKey);
          playCloseSound();
          console.log(`[TRADE][SOUND-CLOSE] ${tradePlan.state} ${state.asset}/${state.interval}: ${tradePlan.action}`);
        }
        flashCard(state.asset, "exit");
        flashTitle(`■ ${tradePlan.action} ${state.asset}/${state.interval}`);
      }
    }
    // High-risk alert: notify once per session when the health score is critical.
    const warnFirstSight = !_warnPrimed.has(uniKey);
    _warnPrimed.add(uniKey);
    if (warnFirstSight) {
      if (health.score >= 75) _warnedKeys.add(uniKey);       // sudah kritis saat halaman dibuka -> jangan notif
    } else if (health.score >= 75 && !_warnedKeys.has(uniKey)) {
      _warnedKeys.add(uniKey);
      console.warn(`[WASPADA] ${state.asset}/${state.interval} ${uni.verdict.toUpperCase()} — ${health.label} (score ${health.score}): ${health.fired.join(", ")}`);
      flashTitle(`⚠ ${health.label} ${uni.verdict.toUpperCase()} ${state.asset}/${state.interval}`);
      try {
        if (typeof Notification !== "undefined" && Notification.permission === "granted" && document.hidden) {
          new Notification(`${health.label} · ${state.asset}/${state.interval}`, {
            body: `Signal ${uni.verdict.toUpperCase()} · score ${health.score} · ${health.fired.join(", ")}`,
            tag: `warn_${uniKey}`,
          });
        }
      } catch (_) {}
    }
  }
  const calcStatus = uni
    ? `Signal locked ${elapsedSec - Math.round((now - uni.lockedAt) / 1000)}s after session open · mode ${uni.mode}`
    : `Evaluating session, open +${elapsedSec}s · ${liveSig ? liveSig.mode : "collecting data"}`;
  // Analysing only during the first seconds; after that a flat verdict means "no entry".
  const analyzing = !uni && elapsedSec < 15;

    // Sync: confidence tetap realtime, verdict tetap di filter desktop
    const mob = _mobilePredSession;
    const mobLocked = mob && mob.prediction !== "flat" && mob.mode !== "MENUNGGU" && mob.mode !== "LOADING";
    
    // Konsistensi mobile-desktop: jika desktop filter flat, override mobile prediction ke flat
    if (mob && mob.prediction !== "flat" && finalVerdict === "flat" && confMode === "SIGNAL") {
      mob.prediction = "flat";
      mob.confidence = 0;
      mob.mode = "BLOCKED";
      try { sessionStorage.setItem(MOBILE_PRED_SESSION_KEY, JSON.stringify(mob)); } catch (_) {}
    }

    // Reason: locked signal first, then live evaluation, then desktop fallback
    let currentReason = "";
    if (liveSig && liveSig.reason) currentReason = liveSig.reason;
    else if (_deskSig && _deskSig.reason) currentReason = _deskSig.reason;
    if (!currentReason) {
      const fbMode = finalVerdict !== "flat" ? mode : ((now - t0) < WARMUP_MS ? "WARMUP" : mode);
      currentReason = SignalCore.buildReason({
        verdict: finalVerdict, mode: fbMode, rsi: rsi,
        volRel: hasVolData ? rel : null, strength: histTrend?.strength,
        momentum: histTrend?.momentum, elapsedSec: elapsed / 1000,
        tf: state.interval, volMin: fairMinVol(state.interval),
      });
    }
    // Keep the quality grade visible in the reason (it no longer fits in the short recommendation).
    const grade = uni && uni.grade ? uni.grade : null;
    // OFI: server lebih dulu, else flow lokal (lihat ofiForDisplay)
    const ofiNow = ofiForDisplay(state.asset, Math.floor(sessionStart / 1000), Math.floor(now / 1000));
    const ofiTxt = ofiNow != null
      ? ` Order flow (OFI) ${(ofiNow * 100).toFixed(0)} percent, ${(liveSig && liveSig.ofiAgree) ? "agreeing" : "against"}.`
      : "";
    if (finalVerdict !== "flat") {
      // Angka winrate harus jelas sumbernya: kalau profil gate BUKAN strict, kalibrasi 90d
      // lama tidak lagi menggambarkan ambang yang dipakai -> sebut sumbernya, dan pakai
      // winrate hasil uji ambang aktif bila learner sudah memilikinya.
      const learnedWR = (GATES && GATES.metrics && GATES.metrics.takenWinrate != null) ? GATES.metrics.takenWinrate : null;
      const strictMode = (GATES && GATES.mode === "strict");
      const wr = (learnedWR != null) ? `winrate uji ambang aktif ${(learnedWR * 100).toFixed(0)} percent`
        : (uni && uni.expectedWR != null) ? `${(uni.expectedWR * 100).toFixed(0)} percent${strictMode ? "" : " (kalibrasi ambang lama)"}` : null;
      const minTxt = uni && uni.minuteIn ? ` (entry minute ${uni.minuteIn}${uni.late ? ", LATE — little time left" : ""})` : "";
      const head = grade
        ? `EARLY ${grade}${minTxt}${wr ? `, measured hit rate ${wr}` : ""}. `
        : (gateInfo ? `Backtested winrate ${(gateInfo.wr * 100).toFixed(0)} percent${strictMode ? "" : " (kalibrasi ambang lama)"}. ` : "Watchlist. ");
      currentReason = head + currentReason + ofiTxt;
    }
    
    updateSignal({
      zone, momentum, rsi, verdict: finalVerdict, mode, trendBias, aligned, conf,
      peakPrice: peakPrice != null ? peakPrice : (peak ? peak.price : null),
      peakDir: peak ? peak.dir : null,
      reward: reward,
      vol5s: hasVolData ? rel : null, liquidity: liquidity,
      // tampilkan metrik yang SAMA dengan gate (volRel2), agar angka di UI = angka yang dinilai
      vol5m: liveSig && liveSig.volRel2 != null ? liveSig.volRel2 : (liveSig && liveSig.volRel != null ? liveSig.volRel : null),
      volWait: !uni && finalVerdict === "flat" && (elapsed / dur) < 0.6 &&
        !!liveSig && (liveSig.mode === "LOWVOL" || liveSig.mode === "WARMUP" || liveSig.mode === "MENUNGGU"),
      volNeed: fairMinVol(state.interval),
      reason: currentReason,
      highConf: !!gateInfo,
      gateWr: gateInfo ? gateInfo.wr : null,
      ofi: ofiNow,
      tradePlan,
      recPnl: (tradePlan && tradePlan.entryPrice != null && tradePlan.tradeDir)
        ? (((tradePlan.tradeDir === "up" ? (C - tradePlan.entryPrice) : (tradePlan.entryPrice - C)) / tradePlan.entryPrice) * 100)
        : null,
      calcStatus,
      recStatus,
      recStatusClass,
      roundStart: sessionStart,     // dipakai updateSignal utk dedupe alarm per sesi
      analyzing,
    });

    // Confidence level LED bar: direction ikut mobile prediction pada tab SIGNAL (value tetap realtime)
    const fadeDir = (mobLocked && confMode === "SIGNAL" && mob.prediction !== "flat")
      ? (mob.prediction === "down" ? "down" : "up")
      : (confMode === "SIGNAL"
        ? (liveStatus === "up" ? "down" : liveStatus === "down" ? "up" : null)
        : confMode.toLowerCase());

    // Referensi TREND saat ini (akumulasi 3 sesi terakhir interval aktif).
    const curTrend = sessionTrend(state.asset, state.interval, TREND_SESSIONS);
    const curTrendDir = curTrend === "bullish" ? "up" : curTrend === "bearish" ? "down" : "flat";
    // SEARAH trend  -> kalkulasi dari 3 SESI SEBELUMNYA (bukti trend benar-benar berlanjut).
    // BERLAWANAN    -> kalkulasi dari SESI AKTIF saat ini (logika lama / counter-trend).
    const alignWithTrend = !!fadeDir && curTrendDir !== "flat" && curTrendDir === fadeDir;

    // Dihitung sebagai FUNGSI atas sebuah arah, supaya LED bar bisa dihitung untuk arah
    // sinyal utama (bukan hanya arah fade internal).
    const confForDirLocal = (dir) => {
      if (!dir) return 0;
      const dn = dir === "down";
      const alignThis = !!curTrendDir && curTrendDir !== "flat" && curTrendDir === dir;
      let base;
      // Tab SIGNAL yang sync mobile prediction: gunakan analisis 3-4 sesi sebelumnya
      if (alignThis || (mobLocked && confMode === "SIGNAL")) {
        base = confidenceFromPastSessions(state.asset, state.interval, dn);
      } else {
        // BERLAWANAN trend (counter-trend): keyakinan dasar dari SESI AKTIF saat ini (live 5s window).
        base = 0;
        // 1) pola candle / struktur peak (paling berbobot)
        if (dn ? isTopPeak : isBotPeak) {
          if (peakConf) base += 25;                                  // 2-3 candle konfirmasi pasca-peak
          if (dn ? droppedFromPeak : roseFromPeak) base += 15;       // harga sudah menjauh dr peak
          else if (dn ? nearTop : nearBot) base += 8;               // msh persis di ujung peak
        }
        // 2) indikator "stretch" (RSI + z-score) — digabung jadi 1 score 0..1 agar tak double-count
        const zComp = (dn ? overbought : oversold) ? 1 : (dn ? z > 1.2 : z < -1.2) ? 0.5 : 0;
        let rsiComp = 0;
        if (rsi != null) rsiComp = (dn ? rsi >= 70 : rsi <= 30) ? 1 : (dn ? rsi >= 60 : rsi <= 40) ? 0.5 : 0;
        const stretch = Math.min(1, (zComp + rsiComp) / 2);   // 0..1
        base += stretch * 25;
        // 3) momentum berbalik (slope)
        if (dn ? rollOver : turnUp) base += 12;
        // 4) TREND (akumulasi 3 sesi interval aktif) — searah fade -> boost, berlawanan -> penalti
        const sTrend = sessionTrend(state.asset, state.interval, TREND_SESSIONS);
        if (sTrend === (dn ? "bearish" : "bullish")) base += 10;
        else if (sTrend === (dn ? "bullish" : "bearish")) base -= 10;
        // 5) likuiditas / volume — tipis = berisiko
        if (liquidity === "LOW") base -= 12;
        else if (liquidity === "THIN") base -= 6;
        if (hasVolData && rel >= 1) base += 5;
      }

      // FEASIBILITAS: peluang harga mencapai target (LOCK / OPEN) — murni DERIVED dari data live,
      // TANPA ambang waktu hardcode. Saat remSec -> 0, jangkauan volMove -> 0 -> reach -> 0 otomatis
      // & realtime tiap tick (recompute di updateProjection yg jalan tiap 1 detik).
      const adverse = dn ? Math.max(0, C - O) : Math.max(0, O - C);
      const remSec = Math.max(0, remaining / 1000);
      const driftToward = dn ? -slope * remSec : slope * remSec;   // proyeksi gerak ke arah target (+ = bagus)
      const residual = Math.max(0, adverse - Math.max(0, driftToward));
      let reach;
      if (residual <= 0) {
        reach = 1;                                       // drift akan bawa ke target
      } else {
        const volMove = std * (remSec / 5) * 2;          // jangkauan sisa waktu (linier thd sisa detik)
        reach = clamp(1 - residual / Math.max(volMove, 1e-9), 0, 1);
      }
      return clamp(Math.round(base * reach), 0, 100);
    };

    // ===== MODEL CONFIDENCE = DIPROSES SERVER =====
    // Angka untuk arah up/down (dua basis: value = sesuai keselarasan trend, past = 3 sesi
    // sebelumnya) sudah dihitung server (confidence.js) dan dikirim lewat
    // assets[sym].all[tf].conf. Klien HANYA memilih angka yang sesuai konteks tab — tidak
    // menjalankan modelnya. confForDirLocal di atas tinggal cadangan bila server tak tersedia.
    const srvConf = (() => {
      try {
        const e = (typeof LIVE !== "undefined") ? LIVE.entryFor(state.asset, state.interval) : null;
        return (e && e.conf) ? e.conf : null;
      } catch (_) { return null; }
    })();
    const confForDir = (dir) => {
      if (!dir) return 0;
      const c = srvConf && srvConf[dir];
      if (c) return (c.align || (mobLocked && confMode === "SIGNAL")) ? c.past : c.value;
      return confForDirLocal(dir);
    };
    const fadeConf = confForDir(fadeDir);

    // indikator sumber kalkulasi confidence (SEARAH -> 3 sesi lalu, BERLAWANAN -> sesi aktif)
    const srcEl = document.getElementById("conf-src");
    if (srcEl) {
      if (!fadeDir) { srcEl.textContent = "—"; srcEl.className = ""; }
      else if (alignWithTrend || (mobLocked && confMode === "SIGNAL")) { srcEl.textContent = "3 SESSIONS PRIOR"; srcEl.className = "src-prev"; }
      else { srcEl.textContent = "ACTIVE SESSION"; srcEl.className = "src-cur"; }
    }
    // ----- TREND (akumulasi N sesi terakhir interval aktif) + PERSENTASE kekuatan trend -----
    const stEl = document.getElementById("conf-trend");
    if (stEl) {
      const t = sessionTrend(state.asset, state.interval, TREND_SESSIONS);
      const mainDir = t === "bullish" ? 1 : t === "bearish" ? -1 : 0;
      let pct = 0;
      if (mainDir !== 0) {
        const candles = state.cache[state.asset][state.interval].candles;
        // run-length: candle berurutan di ujung yg searah trend -> makin lama bertahan, % makin tinggi
        const dirs = candles.map((c) => (c.close > c.open ? 1 : c.close < c.open ? -1 : 0));
        let last = dirs.length - 1;
        while (last >= 0 && dirs[last] === 0) last--;   // maju ke candle terakhir yg punya arah
        if (last >= 0 && dirs[last] === mainDir) {
          let run = 0;
          for (let i = last; i >= 0; i--) {
            if (dirs[i] === mainDir) run++;
            else if (dirs[i] === 0) continue;            // abaikan doji
            else break;
          }
          pct = Math.min(100, run * 14);
          // reversal: puncak berlawanan arah dgn trend terdeteksi -> persentase turun
          const rev = (t === "bullish" && peak && peak.dir === "top") ||
                      (t === "bearish" && peak && peak.dir === "bot");
          if (rev) pct = Math.max(0, pct - (peakConf ? 40 : 20));
        }
      }
      const label = t === "bullish" ? "BULLISH" : t === "bearish" ? "BEARISH" : "FLAT";
      stEl.textContent = pct > 0 ? `${label} ${pct}%` : label;
      stEl.className = t === "bullish" ? "up" : t === "bearish" ? "down" : "";
    }

    // ===== LED bar INLINE dengan MAIN SIGNAL =====
    // KUNCI hanya berlaku di tab SIGNAL: di tab itu LED WAJIB searah rekomendasi utama.
    // Tab UP/DOWN = pilihan MANUAL user -> LED mengikuti arah tab (fadeDir sudah = arah tab),
    // jadi tab tetap berfungsi walau ada rekomendasi berjalan.
    let ledDir = fadeDir, ledConf = fadeConf;
    if (confMode === "SIGNAL" && (finalVerdict === "up" || finalVerdict === "down")) {
      ledDir = finalVerdict;
      ledConf = confForDir(finalVerdict);
      const ledAlign = !!curTrendDir && curTrendDir !== "flat" && curTrendDir === ledDir;
      if (srcEl) {
        if (ledAlign || (mobLocked && confMode === "SIGNAL")) { srcEl.textContent = "3 SESSIONS PRIOR"; srcEl.className = "src-prev"; }
        else { srcEl.textContent = "ACTIVE SESSION"; srcEl.className = "src-cur"; }
      }
    }
    updateConfidenceDisplay(ledDir, ledConf);

    // akurasi: bekukan prediksi di momen entry, evaluasi saat round berakhir
     // Pakai arah yang BENAR-BENAR DITAMPILKAN (ledDir), bukan arah fade internal, supaya
     // log konsisten dengan rekomendasi/led bar yang dilihat user.
     captureConfidenceRound(t0, O, C, typeof ledDir !== "undefined" ? ledDir : fadeDir, typeof ledConf !== "undefined" ? ledConf : fadeConf, curTrendDir, confMode, state);
     

     // Universal background: update all coin/interval signal cache setiap tick
     updateProjectionUniversal();

     // akurasi mobile prediksi: evaluasi di setiap tick
     // Pastikan _mobilePredSession fresh sebelum capture (hindari race condition)
     updateMobilePrediction();
     captureDesktopSignal();

    // price zone overlay (entry zone / sell TP) — must follow the RECOMMENDATION (what the
    // user acts on), NOT the live price direction. Using liveStatus made the zones flip
    // whenever price crossed the lock (e.g. after switching tabs, an UP signal showed the
    // sell zone below the lock). Fallbacks below it: mobile prediction, then live direction.
    const zoneKey = `${state.asset}_${state.interval}_${t0}`;
    const recSig = _deskSigCache[zoneKey] || _deskSigLive[zoneKey] || null;
    const pred = _mobilePredSession;
    const mobOk = pred && pred.prediction !== "flat" && pred.asset === state.asset &&
      pred.interval === state.interval && pred.lockPrice != null && Math.abs(pred.lockPrice - O) < 1e-9;
    const predDir = (recSig && recSig.verdict !== "flat") ? recSig.verdict
      : (mobOk ? pred.prediction : liveStatus);
    chart.setPrediction(predDir !== "flat" ? predDir : null, O);
    chart.setCurrentPrice(C);

    // Store session bounds for smooth rAF timer (client-time reference to avoid serverTimeOffset jitter)
    const sk = state.asset + ":" + dur + ":" + t0;
    _sessionT0 = t0; _sessionT = T; _sessionO = O; _sessionC = C; _sessionDur = dur;
    // Only recompute _sessionT_client when session boundary changes (prevents flicker)
    if (sk !== _sessionKey) { _sessionKey = sk; _sessionT_client = T - serverTimeOffset; _sessionOffsetAtSet = serverTimeOffset; _lastTimerSec = -1; }

  }

  let _lastTimerSec = -1, _sessionT0 = 0, _sessionT = 0, _sessionO = 0, _sessionC = 0, _sessionDur = 0, _sessionT_client = 0;
  let _sessionKey = "";  // guards against flicker: recompute _sessionT_client only when session changes
  let _sessionOffsetAtSet = 0;  // serverTimeOffset used when _sessionT_client was anchored
  function updateTimerDisplay() {
    const now = Date.now();  // pure client time — no serverTimeOffset jitter
    ensureTimeSync();  // re-sync if stale (>3s since last sync)
    // Re-anchor the client-facing round timer if the server-time offset has drifted
    // materially since the session boundary (otherwise the countdown is wrong all round).
    if (_sessionT_client && Math.abs(serverTimeOffset - _sessionOffsetAtSet) > 300) {
      _sessionT_client = _sessionT - serverTimeOffset;
      _sessionOffsetAtSet = serverTimeOffset;
      _lastTimerSec = -1;
    }
    const remaining = _sessionT_client - now;
    const elapsed = now - (_sessionT_client - _sessionDur);
    const total = _sessionDur;
    if (!_sessionT_client || !total) { requestAnimationFrame(updateTimerDisplay); return; }
    const sec = Math.max(0, Math.floor(remaining / 1000));
    if (sec === _lastTimerSec) { requestAnimationFrame(updateTimerDisplay); return; }
    _lastTimerSec = sec;
    const mm = String(Math.floor(sec / 60)).padStart(2, "0");
    const ss = String(sec % 60).padStart(2, "0");
    const liveStatus = _sessionC > _sessionO ? "up" : _sessionC < _sessionO ? "down" : "flat";
    const tfEl = document.getElementById("round-tf");
    const tmrEl = document.getElementById("round-timer");
    const stEl = document.getElementById("round-status");
    if (tfEl) tfEl.textContent = state.interval;
    if (tmrEl) tmrEl.textContent = `${mm}:${ss}`;
    // Chip di samping countdown: HARGA LIVE + selisihnya dari LOCK dalam dolar (sama seperti
    // header kolom dual di layar lebar). Menggantikan label "LIVE UP/DOWN".
    if (stEl) {
      const px = _sessionC, lock = _sessionO;
      const hasPx = px != null && isFinite(px) && px > 0;
      const dUsd = (hasPx && lock != null && isFinite(lock)) ? (px - lock) : null;
      const dir = dUsd != null ? (dUsd > 0 ? "up" : dUsd < 0 ? "down" : "flat") : liveStatus;
      const txt = hasPx ? `${fmtPrice(px)}${dUsd != null ? "  " + fmtUsdDelta(dUsd) : ""}` : "—";
      if (stEl.textContent !== txt) stEl.textContent = txt;
      const cls = "round-status " + dir;
      if (stEl.className !== cls) stEl.className = cls;
      const tip = hasPx ? `Harga live ${fmtPrice(px)}${lock != null ? ` · LOCK ${fmtPrice(lock)} · selisih ${fmtUsdDelta(dUsd)}` : ""}` : "";
      if (stEl.title !== tip) stEl.title = tip;
    }
    const rbf = document.getElementById("round-bar-fill");
    if (rbf) rbf.style.width = Math.min(100, (elapsed / total) * 100) + "%";
     requestAnimationFrame(updateTimerDisplay);
   }

  /* ----------------------- Orderbook bar (real-time buy/sell pressure) ----------------------- */
  function updateOrderbook(sym) {
    const ob = state.orderbook && state.orderbook[sym];
    const askEl = document.getElementById("ob-ask");
    const bidEl = document.getElementById("ob-bid");
    const sellPctEl = document.getElementById("ob-sell-pct");
    const buyPctEl = document.getElementById("ob-buy-pct");
    if (!ob || !ob.bids || !ob.asks || !askEl || !bidEl) {
      if (askEl) askEl.style.width = "50%";
      if (bidEl) bidEl.style.width = "50%";
      if (sellPctEl) sellPctEl.textContent = "—";
      if (buyPctEl) buyPctEl.textContent = "—";
      return;
    }
    // Aggregate total bid (buy) and ask (sell) SIZE (USD value = price × qty) across top 5 levels
    const bidVol = ob.bids.reduce((a, [p, s]) => a + +p * +s, 0);
    const askVol = ob.asks.reduce((a, [p, s]) => a + +p * +s, 0);
    const total = bidVol + askVol;
    if (total <= 0) {
      askEl.style.width = "50%";
      bidEl.style.width = "50%";
      if (sellPctEl) sellPctEl.textContent = "0%";
      if (buyPctEl) buyPctEl.textContent = "0%";
      return;
    }
    const askPct = (askVol / total) * 100;
    const bidPct = 100 - askPct;  // normalize: always sums to 100% (no gaps, no shift)
    askEl.style.width = askPct + "%";
    bidEl.style.width = bidPct + "%";
    if (sellPctEl) sellPctEl.textContent = askPct.toFixed(0) + "%";
    if (buyPctEl) buyPctEl.textContent = bidPct.toFixed(0) + "%";
   }

  /* ----------------------- Real-time orderbook poller (browser-level, 200ms) ----------------------- */
  let _obTimer = null;
  let _obBusy = false;
  function startOrderbookPoll() {
    if (_obTimer) return;
    _obTimer = setInterval(async () => {
      if (_obBusy) return;              // avoid piling up requests if a fetch is slow
      _obBusy = true;
      try {
        // Di layar lebar, kartu dual menampilkan orderbook KEDUA koin. Sebelumnya hanya koin
        // AKTIF yang dipoll (500ms) sedangkan koin lain hanya mendapat data dari snapshot awal
        // -> bar orderbook koin non-aktif MACET. Sekarang keduanya dipoll saat tampilan dual.
        const wide = typeof window.matchMedia === "function" && window.matchMedia("(min-width: 1100px)").matches;
        const syms = wide ? ["BTC", "ETH"] : [state.asset];
        await Promise.all(syms.map(async (sym) => {
          const binanceSym = OB_SYMBOLS[sym];
          if (!binanceSym) return;
          try {
            const r = await fetch(`https://data-api.binance.vision/api/v3/depth?symbol=${binanceSym}&limit=5`, { cache: "no-store" });
            if (!r.ok) return;
            const ob = await r.json();
            if (ob.bids && ob.asks) state.orderbook[sym] = { bids: ob.bids, asks: ob.asks, at: Date.now() };
          } catch (_) {}
        }));
        // Elemen single-coin (ob-ask/ob-bid/…) TIDAK punya prefiks aset -> hanya ditulis utk koin AKTIF.
        if (syms.includes(state.asset)) updateOrderbook(state.asset);
      } catch (_) {} finally { _obBusy = false; }
    }, 500);
  }

// Mayoritas sesi naik -> BULLISH, mayoritas turun -> BEARISH, sisanya FLAT.
// Dipakai utk TREND display & sbg bias tren di confidence / keputusan sinyal.
function sessionTrend(sym, tf, n) {
  return SignalCore.sessionTrend(state.cache[sym]?.[tf]?.candles, n);
}

/* ----------------------- Confidence dari 3 sesi sebelumnya ----------------------- */
// Dipakai bila opsi yg dipilih SEARAH dgn trend: keyakinan dihitung dari akumulasi
// 3 SESI SEBELUMNYA (exclude sesi aktif) sbg bukti trend benar-benar berlanjut.
function trendOfCandles(candles) {
  if (!candles || candles.length < 2) return "flat";
  let bull = 0, bear = 0;
  for (const c of candles) {
    const d = c.close - c.open;
    if (d > 0) bull++;
    else if (d < 0) bear++;
  }
  const need = Math.ceil(candles.length / 2);
  if (bull > bear && bull >= need) return "bullish";
  if (bear > bull && bear >= need) return "bearish";
  return "flat";
}
function confidenceFromPastSessions(sym, tf, dn) {
  // Guard: cache candle bisa belum terisi saat render pertama -> kembalikan 0, bukan crash.
  const candles = (state.cache[sym] && state.cache[sym][tf]) ? state.cache[sym][tf].candles : null;
  if (!candles || candles.length < TREND_SESSIONS + 1) return 0;
  const prev = candles.slice(-1 - TREND_SESSIONS, -1);   // 3 sesi sebelumnya (exclude sesi aktif)
  if (prev.length < 2) return 0;
  const closes = prev.map((k) => k.close);
  const lo = Math.min.apply(null, closes), hi = Math.max.apply(null, closes);
  const range = (hi - lo) || 1;
  let c = 0;
  // 1) konsistensi arah: berapa dari 3 sesi searah dn
  let same = 0;
  for (const k of prev) if (dn ? k.close < k.open : k.close > k.open) same++;
  c += same * 12;                                        // 0..36
  // 2) tren mayoritas 3 sesi: searah -> boost, berlawanan -> penalti
  const t = trendOfCandles(prev);
  if (t === (dn ? "bearish" : "bullish")) c += 25;
  else if (t === (dn ? "bullish" : "bearish")) c -= 15;
  // 3) momentum: arah & kekuatan pergerakan 3 sesi (close terakhir vs open pertama)
  const firstO = prev[0].open, lastC = prev[prev.length - 1].close;
  const mom = lastC - firstO;
  if ((dn ? mom < 0 : mom > 0)) c += Math.round(Math.min(1, Math.abs(mom) / range) * 20);
  // 4) stretch: z-score penutupan terakhir thd rata-rata 3 sesi
  const mean = avg(closes);
  const std = stdev(closes) || 1;
  const z = (lastC - mean) / std;
  if ((dn ? z < -0.5 : z > 0.5)) c += 12;
  else if ((dn ? z < 0 : z > 0)) c += 6;
  return clamp(Math.round(c), 0, 100);
}

/* ----------------------- Header + Gap ----------------------- */
function updateHeader() {
  for (const sym of ["BTC", "ETH"]) {
    const t = state.ticker[sym];
    if (!t) continue;
    const p = document.getElementById(sym.toLowerCase() + "-price");
    const c = document.getElementById(sym.toLowerCase() + "-chg");
    if (!p || !c) continue;
    p.textContent = fmtPrice(t.last);
    const up = t.chg >= 0;
    c.className = "asset-chg " + (up ? "up" : "down");
    c.textContent = (up ? "▲ " : "▼ ") + (up ? "+" : "") + t.chg.toFixed(2) + "%";
  }
}

function updateGap() {
  const b = state.ticker.BTC, e = state.ticker.ETH;
  const bPrev = state.prevPrice.BTC, ePrev = state.prevPrice.ETH;
  if (!b || !e) {
    // fallback to last close from history
    const bc = lastClose("BTC"), ec = lastClose("ETH");
    if (bc && ec) renderGap(bc, ec, null, null);
    return;
  }
  renderGap(b.last, e.last, bPrev, ePrev);
}
function lastClose(sym) {
  const c = state.cache[sym][state.interval].candles;
  return c.length ? c[c.length - 1].close : null;
}
function renderGap(btc, eth, bPrev, ePrev) {
  const gr = document.getElementById("gap-ratio");
  const gs = document.getElementById("gap-sync");
  if (!gr || !gs) return;
  const ratio = btc / eth;
    gr.textContent = "1 BTC = " + ratio.toFixed(2) + " ETH";

  let sync = "—";
  if (bPrev != null && ePrev != null) {
    const bDir = btc >= bPrev ? 1 : -1;
    const eDir = eth >= ePrev ? 1 : -1;
    if (bDir === eDir && bDir === 1) sync = "SYNC ↑";
    else if (bDir === eDir && bDir === -1) sync = "SYNC ↓";
    else if (btc / eth > (bPrev / ePrev)) sync = "BTC > ETH";
    else sync = "ETH > BTC";
  }
  gs.textContent = sync;
}

/* ----------------------- Server proxy (Vercel / local Node) ----------------------- */
let proxyFail = 0;
async function probeProxy() {
  const ctrl = new AbortController();
  const to = setTimeout(() => ctrl.abort(), 6000);
  try {
    // Sync Binance server time BEFORE snapshot (so updateProjection uses correct offset)
    let timeSynced = false;
    try {
      const timeResp = await fetch("https://data-api.binance.vision/api/v3/time", { signal: ctrl.signal });
      if (timeResp.ok) {
        const t = await timeResp.json();
        serverTimeOffset = t.serverTime - Date.now();
        lastTimeSync = Date.now();
        timeSynced = true;
      }
    } catch (_) {}
    const r = await fetch("/api/snapshot?history=1", { signal: ctrl.signal });
    clearTimeout(to);
    if (!r.ok) return null;
    const j = await r.json();
    // Fallback: snapshot serverTime if direct fetch failed
    if (!timeSynced && j.serverTime) {
      serverTimeOffset = j.serverTime - Date.now();
    }
    if (j && j.candles && j.candles.BTC && j.candles.BTC["5m"] && j.candles.BTC["5m"].length) return j;
    return null;
  } catch (_) { clearTimeout(to); return null; }
}

function applySnapshot(snap, isHistory) {
  for (const k of Object.keys(snap.candles)) {
    for (const tf of Object.keys(snap.candles[k])) {
      const bars = snap.candles[k][tf];
      const store = state.cache[k][tf];
      if (isHistory) {
        store.candles = bars.slice(-1000);
      } else {
        bars.forEach((b) => {
          const arr = store.candles;
          const last = arr[arr.length - 1];
          if (last && last.time === b.time) arr[arr.length - 1] = b;
          else if (!last || b.time > last.time) { arr.push(b); if (arr.length > 1000) arr.shift(); }
        });
      }
      store.meta = store.candles[store.candles.length - 1] || null;
      if (tf === "1s") rebuild5s(k);
    }
  }
  if (snap.ticker) {
    for (const k of Object.keys(snap.ticker)) {
      state.prevPrice[k] = state.ticker[k] ? state.ticker[k].last : null;
      state.ticker[k] = snap.ticker[k];
    }
    updateHeader();
  }
  if (snap.orderbook) {
    for (const k of Object.keys(snap.orderbook)) {
      if (snap.orderbook[k]) state.orderbook[k] = snap.orderbook[k];
    }
  }
   updateGap();
   if (state.asset && state.interval) renderActive();
   updateOrderbook(state.asset);
   hideStatus();
}

async function pollProxy() {
  try {
    // Fetch Binance server time DIRECTLY from browser (accurate to ~50ms network latency)
    // This is more accurate than j.serverTime from Vercel snapshot (which is stale by ~1s
    // while Vercel fetches all candles).
    let timeSynced = false;
    try {
      const timeResp = await fetch("https://data-api.binance.vision/api/v3/time", { cache: "no-store" });
      if (timeResp.ok) {
        const timeJ = await timeResp.json();
        serverTimeOffset = timeJ.serverTime - Date.now();
        lastTimeSync = Date.now();
        timeSynced = true;
      }
    } catch (_) { /* may be blocked by CORS/ad-block — fall back below */ }
    const r = await fetch("/api/snapshot", { cache: "no-store" });
    if (!r.ok) throw new Error("HTTP " + r.status);
    const j = await r.json();
    // Fallback: use snapshot serverTime if direct fetch failed (stale by ~1s but better than nothing)
    if (!timeSynced && j.serverTime) {
      serverTimeOffset = j.serverTime - Date.now();
    }
    applySnapshot(j, false);
    proxyFail = 0;
  } catch (e) {
    proxyFail++;
    if (proxyFail > 3) showErr("Gagal mengambil data dari server (Vercel). Periksa deploy/log.");
  }
}

let trendTimer = 0;
// ----- realtime SSE path (local Node server): trade pushed on every fill -----
function updateLiveTrade(d) {
  const sym = d.sym, price = +d.price, ts = +d.ts, qty = +d.qty || 0;
  // Accumulate per-minute executed order flow (OFI) — pulled automatically with every trade.
  // `m` (isBuyerMaker) true = taker sell; false = taker buy.
  if (typeof d.m === "boolean" && isFinite(qty)) {
    const min = Math.floor(ts / 60000) * 60;
    const f = state.flow[sym] || (state.flow[sym] = {});
    let b = f[min]; if (!b) { b = f[min] = { buy: 0, sell: 0, n: 0 }; pruneFlow(sym, min); }
    if (d.m) b.sell += qty; else b.buy += qty;
    b.n++;
  }
  // Sync client ↔ Binance server time from the trade timestamp, throttled + smoothed.
  // Writing on EVERY trade makes serverNow() jitter by network latency, which can flip
  // the session boundary (and therefore sessionLock / sessionStart) near the edge.
  const _nowMs = Date.now();
  if (!_lastTradeOffsetAt || _nowMs - _lastTradeOffsetAt > 2000) {
    const raw = ts - _nowMs;
    serverTimeOffset = serverTimeOffset ? Math.round(serverTimeOffset * 0.8 + raw * 0.2) : raw;
    _lastTradeOffsetAt = _nowMs;
    lastTimeSync = _nowMs;
  }

  // Aggregate into 1s candles (real-time) — needed by mobile prediction
  const t1s = Math.floor(ts / 1000);
  const ones = state.cache[sym]["1s"].candles;
  let one = ones[ones.length - 1];
  if (one && one.time === t1s) {
    one.high = Math.max(one.high, price); one.low = Math.min(one.low, price); one.close = price;
    one.vol = (one.vol || 0) + qty;
  } else if (!one || t1s > one.time) {
    one = { time: t1s, open: price, high: price, low: price, close: price, vol: qty, openTime: t1s * 1000, closeTime: (t1s + 1) * 1000 };
    ones.push(one); if (ones.length > HISTORY_CAP_1S) ones.shift();
  }
  state.cache[sym]["1s"].meta = one;
  
   // Aggregate into chart intervals (5s/30s/1m) via live trade stream
   for (const tf of CHART_INTERVALS) {
     const sec = CHART_SEC[tf];
     const t = Math.floor(ts / 1000 / sec) * sec;
     const arr = state.cache[sym][tf].candles;
     let last = arr[arr.length - 1];
     if (last && last.time === t) {
       last.high = Math.max(last.high, price); last.low = Math.min(last.low, price); last.close = price;
       last.vol = (last.vol || 0) + qty;
     } else if (!last || t > last.time) {
       last = { time: t, open: price, high: price, low: price, close: price, vol: qty, openTime: t * 1000, closeTime: (t + sec) * 1000 };
       arr.push(last); if (arr.length > HISTORY_CAP_5S) arr.shift();
     }
     state.cache[sym][tf].meta = last;
   }
   state.ticker[sym] = state.ticker[sym] || {}; state.ticker[sym].last = price;
   updateHeader(); updateGap();
   if (sym === state.asset) { scheduleRender(); }
}
function updateLiveTicker(d) {
  const t = state.ticker[d.sym] || (state.ticker[d.sym] = {});
  state.prevPrice[d.sym] = t.last; t.last = +d.last; t.chg = +d.chg;
  if (d.sym === state.asset) chart.setCurrentPrice(t.last);
  updateHeader(); updateGap();
}
async function refreshTrends() {
  try {
    const r = await fetch("/api/snapshot", { cache: "no-store" });
    if (!r.ok) return;
    const j = await r.json();
    for (const k of ["BTC", "ETH"]) for (const tf of ["5m", "15m", "1h"]) {
      const bars = j.candles[k][tf]; const store = state.cache[k][tf];
      bars.forEach((b) => {
        const arr = store.candles; const last = arr[arr.length - 1];
        if (last && last.time === b.time) arr[arr.length - 1] = b;
        else if (!last || b.time > last.time) { arr.push(b); if (arr.length > 500) arr.shift(); }
      });
      store.meta = store.candles[store.candles.length - 1] || null;
    }
  } catch (_) {}
}

async function startData() {
  if (typeof EventSource !== "undefined") { trySSE(); return; }
  startPolling();
}
function trySSE() {
    setStatus("Connecting to realtime stream…");
  let es;
  try { es = new EventSource("/api/stream"); } catch (e) { startPolling(); return; }
  let got = false;
  const to = setTimeout(() => { if (!got) { try { es.close(); } catch (_) {} startPolling(); } }, 6000);
   es.addEventListener("snapshot", (e) => {
     got = true; clearTimeout(to); state.viaProxy = true; setConn(true);
     setSrc("stream ↻ live");
     const snap = JSON.parse(e.data);
     // Fast candle-based sync (immediate, ~500ms accuracy)
     const ones = snap.candles && snap.candles.BTC && snap.candles.BTC["1s"];
     if (ones && ones.length) { serverTimeOffset = (ones[ones.length - 1].time + 1) * 1000 - Date.now(); }
     // Refine with authoritative Binance time (more accurate, may fail if blocked)
     fetch("https://data-api.binance.vision/api/v3/time", { cache: "no-store" })
       .then(r => r.ok && r.json()).then(t => { if (t && t.serverTime) { serverTimeOffset = t.serverTime - Date.now(); lastTimeSync = Date.now(); } })
       .catch(() => {});
     applySnapshot(snap, true); hideStatus();
    if (!trendTimer) trendTimer = setInterval(refreshTrends, 10000);
  });
  es.addEventListener("trade", (e) => updateLiveTrade(JSON.parse(e.data)));
  es.addEventListener("ticker", (e) => updateLiveTicker(JSON.parse(e.data)));
  es.onerror = () => { if (!got) { try { es.close(); } catch (_) {} startPolling(); } };
}
function startPolling() {
    setStatus("Connecting to proxy (1s)…");
  probeProxy().then((snap) => {
    if (snap) {
      state.viaProxy = true; setConn(true); setSrc("proxy ↻ 1s"); applySnapshot(snap, true); hideStatus();
      setInterval(pollProxy, 1000);
    } else {
      setSrc("langsung (Binance/ TradingView)");
      loadHistory().then(connectWS).catch(connectWS);
    }
  }).catch(() => {
    setSrc("langsung (Binance/ TradingView)");
    loadHistory().then(connectWS).catch(connectWS);
  });
}

/* ============ CONFIDENCE ACCURACY LOGGER (debug) ============ */
/* Metodologi: prediksi di tab SIGNAL dinamis (counter-trend live price), jadi kita
   BEKUkan prediksi di MOMEN ENTRY (tick pertama ronde di mana sinyal muncul, fadeDir
   !== null), lalu evaluasi vs hasil akhir (close >= lock) saat round berakhir.
   Mengukur di tick terakhir akan rusak: saat settlement liveStatus = hasil akhir,
   sehingga fadeDir = lawan hasil & reach -> 0 (confidence ~0). */

const CONF_LOG_KEY = "bps_conf_log_v1";
const MOBILE_PRED_LOG_KEY = "bps_mobile_pred_log_v1";
const MOBILE_PRED_SESSION_KEY = "bps_mobile_pred_session_v1";

const ConfLog = (() => {
  let log = [];
  try { log = JSON.parse(localStorage.getItem(CONF_LOG_KEY) || "[]"); } catch (_) { log = []; }
  const save = () => { try { localStorage.setItem(CONF_LOG_KEY, JSON.stringify(log)); } catch (_) {} };
  return {
    add(r) { log.push(r); if (log.length > 8000) log = log.slice(-8000); save(); },
    data() { return log; },
    clear() { log = []; save(); },
    size() { return log.length; },
  };
})();

const MobilePredLog = (() => {
  let log = [];
  try { log = JSON.parse(localStorage.getItem(MOBILE_PRED_LOG_KEY) || "[]"); } catch (_) { log = []; }
  const save = () => { try { localStorage.setItem(MOBILE_PRED_LOG_KEY, JSON.stringify(log)); } catch (_) {} };
  return {
    add(r) { log.push(r); if (log.length > 8000) log = log.slice(-8000); save(); },
    data() { return log; },
    clear() { log = []; save(); },
    size() { return log.length; },
    save() { save(); },
    replaceAll(arr) { log = arr.slice(); if (log.length > 8000) log = log.slice(-8000); save(); },
  };
})();

// SignalLog alias - untuk desktop signal capture (source of truth)
const SignalLog = MobilePredLog;

// O(1) index of history keys `${asset}_${interval}_${t0}` so capture/finalize do not
// scan the whole log (up to 8000 entries) on every tick.
const _loggedKeys = new Set();
function logKeyOf(r) { return `${r.asset}_${r.interval}_${r.t0}`; }
function rebuildLoggedIndex() {
  _loggedKeys.clear();
  for (const e of SignalLog.data()) _loggedKeys.add(logKeyOf(e));
}

// Live status of a LOCKED signal (display only — never changes the locked direction).
// Tracks when price first moved to the opposite side of the lock (session open).
const _lockStatusSince = {};
function computeLockStatus(key, dir, lock, price, now) {
  if (!key || (dir !== "up" && dir !== "down") || lock == null || price == null) return null;
  const against = (dir === "up" && price < lock) || (dir === "down" && price > lock);
  const aligned = (dir === "up" && price > lock) || (dir === "down" && price < lock);
  if (against) {
    if (!_lockStatusSince[key]) _lockStatusSince[key] = now;
    return { state: "AGAINST", since: _lockStatusSince[key] };
  }
  _lockStatusSince[key] = null;
  return { state: aligned ? "ALIGNED" : "AT_LOCK", since: null };
}
/* Signal HEALTH score for a LOCKED signal — multi-criteria, deliberately NOT driven by a
   single "price vs lock" sign (which is noisy / whipsaws). Each dimension is independent:
   price position+margin, momentum + deceleration, executed order flow (session and short
   window), counter-direction volume, RSI/z stretch, opposite peak confirmation, higher-tf
   trend flip, and how long price has been against. Advisory only. */
const _warnedKeys = new Set();   // one high-risk alert per combo per session
const _tradeLastState = {};      // key -> last trade-assistant state (drives transition alerts)
function computeSignalHealth(dir, ctx) {
  if (dir !== "up" && dir !== "down") return { score: 0, label: "—", fired: [], confirmedReversal: false };
  const isUp = dir === "up";
  let score = 0; const fired = [];
  const add = (cond, w, label) => { if (cond) { score += w; fired.push(label); } };
  const ofiAgainst = ctx.ofi != null && (isUp ? ctx.ofi < -0.05 : ctx.ofi > 0.05);
  const ofiShortAgainst = ctx.ofiShort != null && (isUp ? ctx.ofiShort < -0.15 : ctx.ofiShort > 0.15);
  const histAgainst = !!ctx.histTrend && ctx.histTrend.predictDir !== "flat" && ctx.histTrend.predictDir !== dir && ctx.histTrend.strength >= 35;

  add(ctx.margin != null && ctx.margin < 0, 15, "harga di sisi lawan lock");
  add(ctx.margin != null && ctx.margin >= 0 && ctx.marginStd != null && ctx.marginStd < 0.5, 10, "margin tipis");
  add(ctx.slope != null && (isUp ? ctx.slope < 0 : ctx.slope > 0), 18, "momentum melawan");
  add(ctx.slope != null && ctx.slopeRecent != null && (isUp ? (ctx.slopeRecent < 0 && ctx.slopeRecent < ctx.slope) : (ctx.slopeRecent > 0 && ctx.slopeRecent > ctx.slope)), 10, "momentum melambat");
  add(ofiAgainst, 18, "OFI melawan");
  add(ofiShortAgainst, 12, "OFI jangka pendek melawan");
  add(ctx.volAgainst != null && ctx.volAgainst >= 1.5, 10, "volume lawan dominan");
  add(ctx.rsi != null && (isUp ? ctx.rsi >= 70 : ctx.rsi <= 30), 8, "RSI ekstrem");
  add(ctx.z != null && (isUp ? ctx.z >= 1.6 : ctx.z <= -1.6), 8, "harga stretch");
  add(!!ctx.peakAgainst, 15, "peak lawan terkonfirmasi");
  add(histAgainst, 12, "trend historis berbalik");

  score = Math.min(100, score);
  // "SUDAH BERBALIK" now requires price against AND independent confirmation — not price alone.
  const confirmedReversal = ctx.margin != null && ctx.margin < 0 && (ofiAgainst || ofiShortAgainst || !!ctx.peakAgainst || histAgainst);
  let label;
  if (confirmedReversal) label = "SUDAH BERBALIK";
  else if (score >= 75) label = "HAMPIR PASTI BERBALIK";
  else if (score >= 55) label = "WASPADA BERBALIK ARAH";
  else if (score >= 30) label = "AWAS MELEMAH";
  else label = "MASIH SESUAI";
  return { score, label, fired, confirmedReversal };
}
function healthClass(label) {
  if (label === "MASIH SESUAI") return "ok";
  if (label === "AWAS MELEMAH") return "warn";
  return "bad";
}

/* Confirmation evidence for the Trade Assistant.
   turnEvidence: signs that price is ABOUT TO move toward the bias (used for entry/average).
   fadeEvidence: signs the favourable move is EXHAUSTING (used for close).
   At least 2 independent parts must agree, and they must persist for a dwell time, so a
   single noisy tick cannot trigger a signal (too fast) and waiting never drags on (too late). */
const DWELL_ENTRY_MS = 4000;    // entry: peak/turn must hold ~4s (fast, but not a single tick)
const DWELL_AVG_MS = 15000;     // averaging: hold ~15s (be more careful adding)
const DWELL_CLOSE_MS = 10000;   // close: fade must hold ~10s
function turnEvidence(isUp, ctx) {
  const p = {};
  p.momentum = ctx.slope != null && (isUp ? ctx.slope > 0 : ctx.slope < 0);
  p.flow = ctx.ofiShort != null && (isUp ? ctx.ofiShort > 0.08 : ctx.ofiShort < -0.08);
  const w = ctx.win || [];
  if (w.length >= 12) {
    if (isUp) {
      const recent = Math.min(...w.slice(-6).map((c) => c.low));
      const prior = Math.min(...w.slice(-12, -6).map((c) => c.low));
      p.structure = recent > prior;                   // higher low = down move stalling
    } else {
      const recent = Math.max(...w.slice(-6).map((c) => c.high));
      const prior = Math.max(...w.slice(-12, -6).map((c) => c.high));
      p.structure = recent < prior;                   // lower high = up move stalling
    }
  } else p.structure = false;
  p.decel = ctx.slope != null && ctx.slopeRecent != null &&
    (isUp ? ctx.slopeRecent > ctx.slope : ctx.slopeRecent < ctx.slope);   // adverse move easing
  const count = Object.values(p).filter(Boolean).length;
  return { count, parts: p };
}
function fadeEvidence(isUp, ctx) {
  const p = {};
  // momentum in favour is weakening
  p.decel = ctx.slope != null && ctx.slopeRecent != null &&
    (isUp ? (ctx.slopeRecent < ctx.slope) : (ctx.slopeRecent > ctx.slope));
  // short-window flow no longer supports the move
  p.flowFade = ctx.ofiShort == null ? false : (isUp ? ctx.ofiShort < 0.05 : ctx.ofiShort > -0.05);
  p.retreat = !!ctx.retreat;
  p.rsi = ctx.rsi != null && (isUp ? ctx.rsi >= 70 : ctx.rsi <= 30);
  const count = Object.values(p).filter(Boolean).length;
  return { count, parts: p };
}
function partList(parts) {
  return Object.keys(parts).filter((k) => parts[k]).join(", ");
}
// Volume emphasis: colour grade by magnitude (bigger = greener), shared by the VOL fields
// and the volume token inside the REASON text.
function volClass(v) {
  return v == null ? "v-na" : v < 1.0 ? "v-low" : v < 1.5 ? "v-mid" : v < 2.5 ? "v-good" : v < 4 ? "v-strong" : "v-hot";
}
/* ===== LED bar confidence (SATU implementasi untuk panel confidence mobile & kartu dual) =====
   Segmen dibuat sekali per elemen lalu hanya kelas/warnanya yang diperbarui, supaya render
   250ms tidak membangun ulang DOM. Warna: merah (0%) -> hijau (100%) seperti versi mobile. */
const LED_SEGMENTS = 20;
const _ledSegCache = new WeakMap();
function paintLedBar(trackEl, val) {
  if (!trackEl) return;
  let segs = _ledSegCache.get(trackEl);
  if (!segs) {
    segs = [];
    trackEl.innerHTML = "";
    for (let i = 0; i < LED_SEGMENTS; i++) {
      const s = document.createElement("i");
      s.className = "conf-seg off";
      trackEl.appendChild(s);
      segs.push(s);
    }
    _ledSegCache.set(trackEl, segs);
  }
  const v = Math.max(0, Math.min(100, Number(val) || 0));
  for (let i = 0; i < LED_SEGMENTS; i++) {
    const seg = segs[i];
    const p = (i + 0.5) / LED_SEGMENTS;              // posisi 0..1 sepanjang gradasi
    const hue = Math.round(p * 120);                 // 0 merah -> 120 hijau
    const lit = ((i + 1) / LED_SEGMENTS) * 100 <= v;
    seg.style.background = "hsl(" + hue + ", 85%, 50%)";
    seg.className = "conf-seg " + (lit ? "on" : "off");
  }
}
function clearLedBar(trackEl) {
  if (!trackEl) return;
  paintLedBar(trackEl, 0);
}

// Wrap only the volume ratio that follows a "volume" phrase, leaving RSI/strength numbers alone.
function emphasizeVolume(text, vol) {
  const esc = String(text).replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c]));
  const cls = "vol-val " + volClass(vol);
  return esc.replace(/(volume[^.]*?)(\d+(?:\.\d+)?x|≥10×)/i, `$1<span class="${cls}">$2</span>`);
}

/* TRADE ASSISTANT — position-aware, matching a mean-reversion entry + momentum exit:
   PHASE 1 (no position): WAIT until price goes CONTRA the bias (below lock for UP),
     then WAIT_TURN until confirmed evidence the move is turning back, then ENTRY.
     It never says HOLD/CLOSE before a position exists.
   PHASE 2 (in position): manage the position — AVERAGE deeper on a confirmed turn,
     HOLD while momentum stays with us, CLOSE when momentum fades or price retreats. */
const _tradePeak = {};     // key -> max favourable excursion after recovery
const _tradeEntered = {};  // key -> { entered, since, price }
const _tradeClosed = {};   // key -> { at, price }  (kapan EARLY CLOSE pertama kali disinyalkan)
const _wideSigSounded = new Set();  // key koin yg sudah dibunyikan di tampilan dual (hindari dobel)
const _mainSigAlerted = new Set();  // key sesi yg alarm sinyalnya sudah dibunyikan (jalur utama)
const _closeSounded = new Set();    // key sesi yg nada close-nya sudah dibunyikan (cegah berulang)
const _entrySounded = new Set();    // key "sesi|state" yg nada entry/average-nya sudah dibunyikan
const _sigFirstSight = new Set();   // key sesi yg sudah pernah dilihat (PRIMING: sinyal yang sudah
                                    // ada saat halaman dibuka TIDAK dibunyikan, karena kartu sudah
                                    // menampilkannya — hanya sinyal yang muncul setelah user ada)
const _sigFirstDual = new Set();    // priming TERPISAH untuk jalur dual: renderDual juga dipanggil
                                    // saat resize (bisa lebih dulu dari updateProjection) sehingga
                                    // memakai set bersama bisa "memakan" priming jalur utama ->
                                    // alert utama berbunyi palsu.
const _warnPrimed = new Set();      // idem untuk peringatan health (skor kritis)
const _tradeLastSrc = {};           // key -> "server" | "local" (asal plan pada pengamatan terakhir)
const _tradeDwell = {};    // key -> { turnSince, fadeSince }
function computeTradePlan(bias, ctx) {
  if (bias !== "up" && bias !== "down") {
    return { state: "NO_SIGNAL", action: "Tidak ada sinyal — tunggu bias sesi", cls: "wait", levels: null, fs: 0, adverseStd: null, why: [], entered: false };
  }
  const isUp = bias === "up";
  const sigma = Math.max(ctx.std || 0, 1e-9);
  const adverse = isUp ? (ctx.lock - ctx.price) : (ctx.price - ctx.lock);   // >0 = against the bias
  const adverseStd = adverse / sigma;
  const favor = -adverse;                                                   // >0 = on the bias side
  // Entry levels are defined by REWARD = price distance from the lock (in %), because the
  // payout comes from recapturing the lock. sigma is kept only as volatility context.
  // Level zona entry/average (% jarak dari LOCK, di sisi CONTRA). Nilai lama 0.10/0.25/0.50
  // terlalu jauh: terukur (BTC+ETH, 7d, gate bootstrap) kedalaman contra setelah melewati LOCK
  //   p10 0.011% · p25 0.033% · p50 0.079% · p75 0.161% · p90 0.279%
  // sehingga L1=0.10% hanya tercapai ~43% (L2/L3 jauh lebih jarang) -> entry terasa mustahil.
  // Nilai baru dipilih agar L1 ~90% tercapai, L2 ~75%, L3 ~50%:
  const RLV = [0.01, 0.03, 0.08];
  // Reward only counts while price is CONTRA the bias (below the lock for UP): that is the
  // distance it must travel back to recapture the lock.
  const rewardOf = (px) => Math.max(0, isUp ? (ctx.lock - px) : (px - ctx.lock)) / px * 100;
  const lvlPrice = (pct) => isUp ? ctx.lock * (1 - pct / 100) : ctx.lock * (1 + pct / 100);
  const levels = { l1: lvlPrice(RLV[0]), l2: lvlPrice(RLV[1]), l3: lvlPrice(RLV[2]), target: ctx.lock };
  levels.rNow = rewardOf(ctx.price);
  levels.r1 = RLV[0]; levels.r2 = RLV[1]; levels.r3 = RLV[2];
  const inZone2 = levels.rNow >= RLV[1];
  const h = ctx.health || {};
  const biasAtRisk = h.label === "HAMPIR PASTI BERBALIK" || h.label === "SUDAH BERBALIK";
  // NOTE: "price is contra the lock" is the ENTRY OPPORTUNITY in this workflow, not a risk —
  // so the health label (built for managing an open position) must NOT veto PHASE 1.
  // The only genuine reason to hold back is a real reversal: higher-tf trend flipped against
  // the bias AND strong opposing flow, while the move is still going against us.
  const histFlipped = !!ctx.histTrend && ctx.histTrend.predictDir !== "flat" && ctx.histTrend.predictDir !== bias && ctx.histTrend.strength >= 35;
  const ofiStrongAgainst = ctx.ofi != null && (isUp ? ctx.ofi < -0.25 : ctx.ofi > 0.25);
  const realReversal = histFlipped && ofiStrongAgainst;
  // Evidence the move against the bias is about to turn back toward it.
  // Confirmation: >=2 independent evidence parts AND a minimum dwell time, so a single
  // noisy tick cannot trigger (too fast) and waiting never drags on (too late).
  const turn = ctx.turn || { count: 0, parts: {} };
  const fade = ctx.fade || { count: 0, parts: {} };
  const dwellTurn = ctx.dwellTurnMs || 0;
  const dwellFade = ctx.dwellFadeMs || 0;
  const avgReady = turn.count >= 2 && dwellTurn >= DWELL_AVG_MS;
  const closeReady = fade.count >= 2 && dwellFade >= DWELL_CLOSE_MS;

  // momentum in favour (only used in PHASE 2)
  let fs = 0; const why = [];
  const add = (c, w, l) => { if (c) { fs += w; why.push(l); } };
  add(ctx.slope != null && (isUp ? ctx.slope > 0 : ctx.slope < 0), 25, "momentum searah");
  add(ctx.slope != null && ctx.slopeRecent != null && (isUp ? (ctx.slopeRecent > 0 && ctx.slopeRecent >= ctx.slope) : (ctx.slopeRecent < 0 && ctx.slopeRecent <= ctx.slope)), 20, "momentum menguat");
  add(ctx.ofi != null && (isUp ? ctx.ofi > 0.05 : ctx.ofi < -0.05), 20, "OFI searah");
  add(ctx.ofiShort != null && (isUp ? ctx.ofiShort > 0.1 : ctx.ofiShort < -0.1), 10, "OFI pendek searah");
  add(favor >= 0, 10, "harga sudah kembali ke lock");
  add(!ctx.retreat, 10, "tidak mundur dari puncak");
  add(ctx.rsi != null && !(isUp ? ctx.rsi >= 75 : ctx.rsi <= 25), 5, "RSI belum ekstrem");
  fs = Math.min(100, fs);
  // Continuation potential: how much further price typically runs after reaching the lock,
  // so the user can exit at the peak instead of straight away.
  const histAligned = !!ctx.histTrend && ctx.histTrend.predictDir !== "flat" && ctx.histTrend.predictDir === bias && ctx.histTrend.strength >= 35;
  const ofiShortAligned = ctx.ofiShort != null && (isUp ? ctx.ofiShort > 0.1 : ctx.ofiShort < -0.1);
  const cp = Math.min(100, fs + (ofiShortAligned ? 10 : 0) + (histAligned ? 10 : 0));
  const cont = favor >= 0 ? continuationOf(ctx.tf, cp, isUp, ctx.price) : null;
  const contTxt = cont
    ? ` · sisa potensi ~${cont.est.toFixed(2)}% (peluang ${(cont.prob * 100).toFixed(0)}%${cont.peakMin != null ? `, puncak ±mnt ${cont.peakMin}` : ""}) → target ${fmtPrice(cont.target)}`
    : "";

  const entered = !!ctx.entered;
  let state, action, cls, nowEntered = entered;
  const rNowTxt = levels.rNow.toFixed(2) + "%";

  if (!entered) {
    // ---------------- PHASE 1: no position ----------------
    // Behaviour: wait for the CONTRA PEAK (the adverse move exhausting), enter there, then sell
    // when price returns to the lock. A confirmed peak needs >=2 evidence parts held ~4s — fast
    // enough to catch the turn, slow enough not to buy a falling knife on one noisy tick.
    // A genuine reversal (higher-tf trend flipped AND strong opposing flow) is the main source of
    // the tail losses, so it blocks the entry.
    if (favor >= 0) {
      state = "WAIT"; cls = "wait";
      action = `TUNGGU — tunggu harga contra ke ${fmtPrice(ctx.lock)}`;
    } else if (realReversal) {
      state = "STAND_DOWN"; cls = "exit";
      action = `JANGAN ENTRY — tren historis berbalik & arus kuat melawan (kemungkinan reversal nyata)`;
    } else if (turn.count >= 2 && dwellTurn >= DWELL_ENTRY_MS) {
      state = "ENTRY"; cls = "entry";
      nowEntered = true;
      action = `ENTRY SEKARANG ${bias.toUpperCase()} — peak contra terkonfirmasi (${rNowTxt}, ${partList(turn.parts)})`;
    } else {
      state = "WAIT"; cls = "wait";
      action = `TUNGGU PEAK — harga contra ${rNowTxt}; konfirmasi pembalikan ${turn.count}/4 · ${Math.round(dwellTurn / 1000)}s/${DWELL_ENTRY_MS / 1000}s`;
    }
  } else {
    // ---------------- PHASE 2: position open ----------------
    if (biasAtRisk) {
      state = "STAND_DOWN"; cls = "exit";
      action = "CUT SEKARANG — sinyal berbalik terkonfirmasi";
    } else if (favor >= 0) {
      // Behaviour: sell when price reaches/exceeds the lock. Only hold when momentum is clearly
      // strong and there is measurable extra room.
      // Exit is the main lever (backtest: selling AT the lock captures only ~5% of the potential
      // move; a target slightly ABOVE the lock captures 2-4x more with a still-high win rate).
      const exAll = (TIERS && TIERS.locktouch && TIERS.locktouch.exitTargets) ? TIERS.locktouch.exitTargets : null;
      const ex = exAll && exAll.extended ? exAll.extended.find((e) => e.t === 0.01) : null;
      const lockWin = exAll ? exAll.lockWin : null;
      const exTxt = ex
        ? ` · opsi target lanjutan ${fmtPrice(isUp ? ctx.lock * 1.0001 : ctx.lock * 0.9999)} (+0.01%, win ${(ex.win * 100).toFixed(0)}%)`
        : "";
      const trailTxt = (ctx.trail && ctx.trail.armed)
        ? ` · TRAIL puncak-halus ${fmtPrice(ctx.trail.smaPeak)} → exit bila mundur ke ${fmtPrice(ctx.trail.exitPrice)} (win 64%, E+0.014%)`
        : "";
      // PARTIAL LADDER EXIT (max 3 legs) — backtest BTC+ETH: 40% lock / 30% lock+0.02% / 30% trail
      // captures ~8x more than selling everything at the lock (median $5.07 vs $0.61) at win 70%.
      const l2Price = isUp ? ctx.lock * 1.0002 : ctx.lock * 0.9998;
      const ladderTxt = (ctx.trail && ctx.trail.armed)
        ? `LADDER (3 level, win ~70%): 40% di lock ${fmtPrice(ctx.lock)} · 30% di ${fmtPrice(l2Price)} (+0.02%) · 30% TRAIL puncak-halus ${fmtPrice(ctx.trail.smaPeak)} → exit ${fmtPrice(ctx.trail.exitPrice)}`
        : `TAHAN — tunggu harga menyentuh lock ${fmtPrice(ctx.lock)} untuk mulai ladder`;
      if (ctx.retreat || closeReady || fs < 65) {
        state = "CLOSE"; cls = "exit";
        const why = ctx.retreat ? "harga mundur dari puncak" : closeReady ? "momentum melemah" : "momentum mulai lemah";
        action = `JUAL SEMUA SEKARANG${lockWin != null ? ` (WIN ${(lockWin * 100).toFixed(0)}%)` : ""} — ${why}${trailTxt}${exTxt}`;
      } else {
        state = "HOLD"; cls = "entry";
        action = ladderTxt;
      }
    } else if (inZone2 && avgReady) {
      state = "AVERAGE"; cls = "entry";
      action = `TAMBAH ENTRY SEKARANG ${bias.toUpperCase()} — harga ${rNowTxt} (average terkonfirmasi)`;
    } else if (inZone2) {
      state = "HOLD_POS"; cls = "wait";
      action = `SIAP TAMBAH ENTRY — konfirmasi ${turn.count}/4 · ${Math.round(dwellTurn / 1000)}s/${DWELL_AVG_MS / 1000}s`;
    } else {
      state = "HOLD_POS"; cls = "wait";
      action = `TUNGGU — harga baru ${rNowTxt} dari lock (zona tambah entry ${RLV[1]}%)`;
    }
  }
  const CMD = { ENTRY: "ENTRY SEKARANG", AVERAGE: "TAMBAH ENTRY SEKARANG", HOLD: "HOLD", CAUTION: "SIAP CLOSE", CLOSE: "CLOSE SEKARANG", STAND_DOWN: "CUT SEKARANG", WAIT: "TUNGGU", HOLD_POS: "TUNGGU", NO_SIGNAL: "—" };
  return { state, action, cls, tradeDir: bias, cmd: CMD[state] || state, levels, fs, cp, cont, adverseStd, favor, why, entered: nowEntered, turn, fade, dwellTurnMs: dwellTurn, dwellFadeMs: dwellFade };
}

/* Signals locked during a running session are held here (persisted) and only written to
   history when the round ENDS. Prevents history entries appearing before a round finishes,
   and prevents duplicates across page reloads. */
const PENDING_SIG_KEY = "bps_pending_sig_v1";
const PendingSig = (() => {
  let map = {};
  try { map = JSON.parse(localStorage.getItem(PENDING_SIG_KEY) || "{}"); } catch (_) { map = {}; }
  const save = () => { try { localStorage.setItem(PENDING_SIG_KEY, JSON.stringify(map)); } catch (_) {} };
  const keyOf = (r) => `${r.asset}_${r.interval}_${r.t0}`;
  return {
    // Immutable: keep the FIRST locked signal for a round. A later re-computation
    // (e.g. after a page reload) must not silently replace an already-locked signal.
    add(r) { const k = keyOf(r); if (!(k in map)) { map[k] = r; save(); } },
    all() { return Object.values(map); },
    remove(r) { delete map[keyOf(r)]; save(); },
    has(k) { return !!map[k]; },
    clear() { map = {}; save(); },
    size() { return Object.keys(map).length; },
  };
})();

/* ===== Backtest-calibrated quality gate (see backtest/replay.js) ===== */
let GATE = null;
let GATE_STATUS = "loading";   // loading | ok | missing | error
const GATE_MAP = new Map();
function gateRsiBucket(r) { return r == null ? "na" : r < 30 ? "<30" : r < 40 ? "30-40" : r <= 60 ? "40-60" : r <= 70 ? "60-70" : ">70"; }
function gateStrBucket(s) { return s < 35 ? "<35" : s < 50 ? "35-50" : s < 70 ? "50-70" : ">=70"; }
function gateKey(tf, mode, dir, rsi, strength) {
  return `${tf}|${mode}|${dir}|rsi:${gateRsiBucket(rsi)}|str:${gateStrBucket(strength)}`;
}
function gateLookup(key) { return GATE_MAP.get(key) || null; }
async function loadGate() {
  try {
    const res = await fetch("/backtest/out/gate.json", { cache: "no-store" });
    if (!res.ok) { GATE_STATUS = "missing"; console.warn("[GATE] gate.json not available — all signals will be marked watchlist"); renderConfidenceReport(); return; }
    GATE = await res.json();
    GATE_MAP.clear();
    for (const g of (GATE.gate || [])) GATE_MAP.set(g.key, g);
    GATE_STATUS = "ok";
    console.log(`[GATE] loaded ${GATE_MAP.size} high-confidence conditions · baseline ${(GATE.baseline * 100).toFixed(1)}% · ${GATE.days}d`);
    renderConfidenceReport();
  } catch (e) { GATE_STATUS = "error"; console.warn("[GATE] load failed — all signals will be marked watchlist:", e.message); renderConfidenceReport(); }
}

/* ===== Calibration tables (see backtest/out/early_tiers.json, tiers.json) =====
   TABEL HISTORIS: dipakai hanya sebagai angka pembanding, dan key-nya masih memakai
   definisi kalibrasi lama. Ambang yang BENAR-BENAR berlaku sekarang ada di
   gates.js / /api/model/gates (profil bootstrap / learned) — lihat gatesSummary().
   Tiers yang dipakai live: STRONG / GOOD / FAIR dari profil gate tersebut. */
let TIERS = null;
let TIER_STATUS = "loading";
async function loadTiers() {
  try {
    const res = await fetch("/backtest/out/early_tiers.json", { cache: "no-store" });
    if (!res.ok) { TIER_STATUS = "missing"; return; }
    TIERS = await res.json();
    // Per-minute accuracy: entries that appear later in the session are measurably better.
    try {
      const r2 = await fetch("/backtest/out/tier_by_minute.json", { cache: "no-store" });
      if (r2.ok) { const j = await r2.json(); TIERS.byMinute = j.tiers || null; }
    } catch (_) {}
    // Continuation potential: how much further price typically runs after touching the lock.
    try {
      const r3 = await fetch("/backtest/out/continuation.json", { cache: "no-store" });
      if (r3.ok) { const j = await r3.json(); TIERS.continuation = j || null; }
    } catch (_) {}
    // 2-second signal tiers (7d of 1s klines).
    try {
      const r4 = await fetch("/backtest/out/early2s.json", { cache: "no-store" });
      if (r4.ok) { const j = await r4.json(); TIERS.early2s = j || null; }
    } catch (_) {}
    // LOCK-TOUCH strategy calibration (the ~90% winrate path).
    try {
      const r5 = await fetch("/backtest/out/locktouch.json", { cache: "no-store" });
      if (r5.ok) { const j = await r5.json(); TIERS.locktouch = j || null; }
    } catch (_) {}
    TIER_STATUS = "ok";
    console.log(`[TIERS] early tiers loaded · ${TIERS.windowDays}d window · byMinute ${TIERS.byMinute ? "yes" : "no"}`);
  } catch (e) { TIER_STATUS = "error"; console.warn("[TIERS] load failed:", e.message); }
}

/* ===== LIVE: sinyal dari SERVER (satu sumber kebenaran untuk semua device) =====
   Sebelumnya sinyal dihitung di tiap browser -> device bisa beda (data pasar, offset jam,
   dan sinyal hanya terkunci bila device terbuka saat detik ke-2 sesi). Sekarang server
   menghitung & mengunci sinyal per sesi (engine.js) dan mengirimnya lewat SSE /api/live.
   Klien memakai sinyal server bila segar; kalau stream mati/basi, otomatis jatuh ke mesin
   lokal (fallback) supaya aplikasi tetap jalan. */
const LIVE = (() => {
  let es = null, snap = null, at = 0, tfSub = null, err = null, count = 0;
  function connect(tf) {
    if (es) { try { es.close(); } catch (_) {} es = null; }
    snap = null; at = 0; tfSub = tf;
    try {
      es = new EventSource(`/api/live?tf=${encodeURIComponent(tf)}`);
      es.onmessage = (e) => { try { snap = JSON.parse(e.data); at = Date.now(); count++; err = null; } catch (_) {} };
      es.onerror = () => { err = "stream terputus (EventSource akan reconnect)"; };
      console.log("[LIVE] subscribe sinyal server tf=" + tf);
    } catch (e) { err = (e && e.message) || "gagal membuat EventSource"; console.warn("[LIVE]", err); }
  }
  const fresh = (ms = 5000) => !!(snap && Date.now() - at < ms);
  // Sinyal server utk (asset, tf) — hanya bila stream segar dan sesinya cocok.
  function signalFor(asset, tf) {
    if (!fresh() || !snap) return null;
    if (tfSub !== tf) return null;
    const a = snap.assets && snap.assets[asset];
    if (!a || !a.signal) return null;
    const dur = INTERVAL_MS[tf] || 300000;
    const t0 = Math.floor(serverNow() / dur) * dur / 1000;
    if (a.signal.t0 !== t0) return null;                 // snapshot beda sesi -> abaikan
    return Object.assign({}, a.signal, {
      verdict: a.signal.verdict || (a.signal.accepted ? a.signal.dir : "flat"),
      source: "server", roundStart: a.signal.t0,
    });
  }
  const priceFor = (asset) => (fresh() && snap && snap.assets && snap.assets[asset]) ? snap.assets[asset].price : null;
  // Data server untuk (aset, tf) apa pun yang dilayani server (snapshot membawa `all` per tf).
  // Dipakai untuk MEMATIKAN perhitungan sinyal lokal: bila server sudah menyediakan entri tf ini
  // (walau masih "pending"/flat), server yang berwenang -> klien tidak boleh mengarang verdict.
  function entryFor(asset, tf) {
    if (!fresh() || !snap || !snap.assets) return null;
    const a = snap.assets[asset];
    return (a && a.all && a.all[tf]) ? a.all[tf] : null;
  }
  const covers = (asset, tf) => !!entryFor(asset, tf);
  // Hook uji/debug: suntikkan snapshot seolah-olah baru diterima dari stream.
  const inject = (s, tf) => { snap = s; at = Date.now(); tfSub = tf || tfSub; count++; err = null; };
  return { connect, inject, fresh, signalFor, priceFor, entryFor, covers, get snap() { return snap; }, get at() { return at; }, get tf() { return tfSub; }, get err() { return err; }, get n() { return count; } };
})();

/* ===== PROFIL GATE (ambang sinyal) — bisa diganti learner TANPA deploy =====
   Ambang yang menentukan apakah sinyal ditampilkan (tier volRel2/surprise, floor likuiditas,
   lateFrac, plus threshold hasil belajar) disajikan server lewat /api/model/gates. Default =
   profil bootstrap (dilonggarkan untuk mengumpulkan data); begitu learner punya cukup bukti
   uji, profil `learned` menggantikannya. Nilai fallback di bawah hanya dipakai bila server
   tidak terjangkau, supaya aplikasi tetap berjalan. */
let GATES = {
  mode: "bootstrap",
  tiers: { STRONG: { volRel2: 3, surprise: 3 }, GOOD: { volRel2: 1.5, surprise: 2 }, FAIR: { volRel2: 0.3, surprise: 0 } },
  liqFloorMul: 0.12, lateFrac: 0.85, thresholds: [],
  note: "fallback lokal (server tidak terjangkau)",
};
async function loadGates() {
  try {
    const r = await fetch("/api/model/gates", { cache: "no-store" });
    if (r.ok) { const g = await r.json(); if (g && g.tiers) { GATES = g; console.log(`[GATES] profil ${g.mode}${g.thresholds && g.thresholds.length ? " · threshold " + g.thresholds.map((t) => `${t.f}${t.op}${t.t}`).join(" ") : ""}`); } }
  } catch (_) {}
  renderGateLine();
  renderLessons();
  renderLearnerStatus();
}
// Jejak build: berguna untuk memastikan browser memuat JS terbaru (bukan cache lama).
const BUILD = "2026-09-28 17:50 WIB";
// Chip di topbar: status learner sekilas + klik = buka & scroll ke panelnya.
let _learnChipBound = false;
function updateLearnChip() {
  const c = document.getElementById("learn-chip"); if (!c) return;
  const L = LEARNER_STATUS && LEARNER_STATUS.ledger, G = LEARNER_STATUS && LEARNER_STATUS.gates;
  const learned = G && G.mode === "learned";
  const boot = G && G.mode === "bootstrap";
  c.className = "learn-chip" + (learned ? " learned" : boot ? " bootstrap" : "");
  // Teks dipersingkat (LRN n/target) karena topbar mobile sempit: nama mode cukup lewat
  // warna chip (hijau = belajar, merah = bootstrap) + tooltip, bukan teks panjang.
  const modeTxt = learned ? "AMBANG BELAJAR" : boot ? "BOOTSTRAP" : "KONSERVATIF";
  c.textContent = L
    ? `LRN ${L.canonicalWithRes || 0}/${L.target || 120}`
    : "LRN —";
  c.title = L
    ? `STATUS LEARNER · ${L.canonicalWithRes || 0}/${L.target || 120} referensi · ambang ${modeTxt} (klik untuk buka panel)`
    : "Buka panel STATUS LEARNER";
  if (!_learnChipBound) {
    _learnChipBound = true;
    c.addEventListener("click", () => {
      ensureLearnerPanel();
      const d = document.getElementById("learn-status");
      if (d) { d.open = true; if (d.scrollIntoView) d.scrollIntoView({ behavior: "smooth", block: "start" }); }
    });
  }
}

// Tampilkan ambang yang SEDANG BERLAKU di UI (selalu dari satu sumber: GATES).
function renderGateLine() {
  const sum = gatesSummary();
  const a = document.getElementById("help-gates"); if (a) a.textContent = sum;
  const b = document.getElementById("help-gate-mode"); if (b) b.textContent = GATES.mode;
  const c = document.getElementById("gate-line");
  const html = `${GATES.mode === "learned" ? '<span class="lstat-badge ok">AMBANG HASIL BELAJAR</span>' : (GATES.mode === "strict" ? '<span class="lstat-badge def">AMBANG KONSERVATIF</span>' : '<span class="lstat-badge sup">BOOTSTRAP (DILONGGARKAN)</span>')} <b>AMBANG AKTIF:</b> ${sum}`;
  if (c) c.innerHTML = html;   // hanya versi mobile (section.confidence)
}
// Terapkan threshold hasil belajar (lapisan kedua setelah tier ladder).
function gateThresholdsOK(f) {
  const ths = GATES && GATES.thresholds;
  if (!Array.isArray(ths) || !ths.length) return true;
  for (const th of ths) {
    const v = f[th.f];
    if (typeof v !== "number" || !isFinite(v)) return false;
    if (th.op === ">=" ? v < th.t : v > th.t) return false;
  }
  return true;
}

/* ===== PHASE 1 LEARNER — "belajar dari sinyal yang sudah berlalu" =====
   Tabel konteks tervalidasi walk-forward: dilatih pada 70% data paling awal, diuji pada
   30% data paling akhir (bukan random split), dinilai dengan Wilson lower/upper bound —
   lihat backtest/learn.js (arah, 90d), learn_touch90.js (peluang kembali ke lock, 90d),
   dan lessons.json (ringkasan "kenapa sinyal salah").
   Dipakai untuk: (a) menampilkan winrate jujur per konteks, (b) memperingatkan konteks
   yang historis lemah, (c) opsional menahan sinyal pada konteks tersebut (LEARN_BLOCK). */
let LEARN = { gate: null, touch: null, lessons: null, status: "loading" };
let LEARN_BLOCK = false;   // default MATI — tidak mengubah sinyal sampai diaktifkan
// fitur yang maknanya identik antara backtest dan live (vol dikecualikan: definisinya beda)
const LEARN_SAFE = new Set(["interval", "symbol", "mode", "minute", "rsi", "hour", "hist", "trend", "gap", "dir"]);
async function loadLearn() {
  const get = (u) => fetch(u, { cache: "no-store" }).then((r) => (r.ok ? r.json() : null)).catch(() => null);
  try {
    // Model yang sedang dipakai disajikan server (hasil re-fit dari ledger produksi bila ada);
    // bila belum tersedia, jatuh ke tabel statis (hasil backtest 90d).
    let [g, t, l] = await Promise.all([get("/api/model/gate"), get("/api/model/touch"), get("/api/model/lessons")]);
    const src = (g && t) ? "model server" : "tabel statis";
    if (!g) g = await get("/backtest/out/learn_gate.json");
    if (!t) t = await get("/backtest/out/learn_touch90.json");
    if (!l) l = await get("/backtest/out/lessons.json");
    LEARN.gate = g; LEARN.touch = t; LEARN.lessons = l; LEARN.src = src;
    LEARN.meta = await get("/api/model/meta");
    LEARN.status = g ? "ok" : "missing";
    console.log(`[LEARN] ${src} · gate ${g ? "ok" : "-"} · touch ${t ? "ok" : "-"} · lessons ${l ? "ok" : "-"}`);
    renderLessons();
  } catch (e) { LEARN.status = "error"; console.warn("[LEARN] load failed:", e.message); }
}
// Bucket HARUS identik dengan backtest/learn.js & learn_touch90.js — jangan diubah sendiri.
function learnBuckets(o) {
  const vr = (o.volRel == null) ? NaN : o.volRel;
  const h = o.hour == null ? 0 : o.hour;
  return {
    interval: String(o.tf), symbol: String(o.symbol), mode: String(o.mode || ""),
    minute: o.minutesIn <= 1 ? "1" : o.minutesIn <= 3 ? "2-3" : o.minutesIn <= 6 ? "4-6" : "7+",
    rsi: o.rsi == null ? "na" : o.rsi < 30 ? "<30" : o.rsi < 40 ? "30-40" : o.rsi < 60 ? "40-60" : o.rsi < 70 ? "60-70" : ">70",
    vol: !isFinite(vr) ? "na" : vr < 0.7 ? "<0.7" : vr < 1.0 ? "0.7-1" : vr < 1.5 ? "1-1.5" : vr < 2.5 ? "1.5-2.5" : ">2.5",
    hour: h < 4 ? "0-3" : h < 8 ? "4-7" : h < 12 ? "8-11" : h < 16 ? "12-15" : h < 20 ? "16-19" : "20-23",
    hist: o.histStrength == null ? "na" : o.histStrength < 10 ? "<10" : o.histStrength < 20 ? "10-20" : ">20",
    trend: String(o.trend), dir: String(o.dir),
    gap: o.gapPct < 0.005 ? "<0.005" : o.gapPct < 0.01 ? "0.005-0.01" : o.gapPct < 0.02 ? "0.01-0.02" : o.gapPct < 0.035 ? "0.02-0.035" : ">0.035",
  };
}
function learnMatch(key, ctx) {
  return key.split("&").every((p) => {
    const i = p.indexOf("="), f = p.slice(0, i), v = p.slice(i + 1);
    return LEARN_SAFE.has(f) && ctx[f] === v;
  });
}
function learnLookup(o) {
  const ctx = learnBuckets(o);
  const res = { ctx, dirWR: null, dirLb: null, dirN: 0, intervalWR: null, touch: null, touchLb: null, touchN: 0, weak: [], strong: [], blockable: [], label: "NETRAL" };
  const G = LEARN.gate, T = LEARN.touch;
  if (G && G.buckets) {
    // Ambang 200: di bawah itu interval kepercayaan terlalu lebar untuk ditampilkan jujur.
    const mi = G.buckets.minute && G.buckets.minute[ctx.minute];
    if (mi && mi.nTest >= 200) { res.dirWR = mi.wrTest; res.dirLb = mi.lbTest; res.dirN = mi.nTest; }
    const iv = G.buckets.interval && G.buckets.interval[ctx.interval];
    if (iv && iv.nTest >= 200) res.intervalWR = { wr: iv.wrTest, lb: iv.lbTest, n: iv.nTest };
    // hanya aturan interval TUNGGAL (mis. "interval=1h") yang boleh menahan sinyal —
    // aturan gabungan seperti "interval=5m&minute=1" terlalu umum (semua sinyal 2s cocok).
    for (const k of G.suppress || []) if (k.indexOf("interval=") === 0 && k.indexOf("&") === -1 && learnMatch(k, ctx)) res.blockable.push(k);
  }
  if (T && T.buckets) {
    const gb = T.buckets.gap && T.buckets.gap[ctx.gap];
    // tabel statis menyimpan lb sentuh di lbTest; model server di touchLbTest -> dukung keduanya
    if (gb && gb.nTest >= 200) { res.touch = gb.touchTest; res.touchLb = (gb.touchLbTest != null ? gb.touchLbTest : gb.lbTest); res.touchN = gb.nTest; }
    // gap = faktor dominan play reversion; hanya aturan gap TUNGGAL yang boleh menahan sinyal
    for (const k of T.suppress || []) if (k.indexOf("gap=") === 0 && k.indexOf("&") === -1 && learnMatch(k, ctx)) res.blockable.push(k);
  }
  if (res.intervalWR && res.intervalWR.wr < 0.66) res.weak.push(`interval ${ctx.interval} historis ${(res.intervalWR.wr * 100).toFixed(0)}%`);
  if (res.touch != null && res.touch < 0.62) res.weak.push(`kembali-ke-lock ${(res.touch * 100).toFixed(0)}% (gap ${ctx.gap})`);
  if (res.touch != null && res.touch >= 0.72) res.strong.push(`kembali-ke-lock ${(res.touch * 100).toFixed(0)}%`);
  if (res.dirWR != null && res.dirWR >= 0.70) res.strong.push(`arah-close ${(res.dirWR * 100).toFixed(0)}%`);
  res.label = res.weak.length && !res.strong.length ? "LEMAH" : res.strong.length && !res.weak.length ? "KUAT" : res.weak.length ? "CAMPURAN" : "NETRAL";
  return res;
}
function learnNote(L) {
  if (!L) return "";
  const p = [];
  if (L.intervalWR) p.push(`${L.ctx.interval} arah-close ${(L.intervalWR.wr * 100).toFixed(0)}% (n=${L.intervalWR.n})`);
  if (L.dirWR != null) p.push(`entri ${L.ctx.minute === "1" ? "menit-1" : "menit " + L.ctx.minute} arah-close ${(L.dirWR * 100).toFixed(0)}% (n=${L.dirN})`);
  if (L.touch != null) p.push(`kembali-ke-lock ${(L.touch * 100).toFixed(0)}% (gap ${L.ctx.gap}, n=${L.touchN})`);
  if (!p.length) return "";
  const tag = L.label === "KUAT" ? " · konteks KUAT ✔" : L.label === "LEMAH" ? " · ⚠ konteks LEMAH" : L.label === "CAMPURAN" ? " · konteks CAMPURAN" : "";
  // Sumber tabel belajar harus jujur: model hasil ledger, atau tabel backtest 90 hari.
  const src = (LEARN.gate && LEARN.gate.source === "ledger") ? `MODEL BELAJAR${LEARN.gate.version ? " " + String(LEARN.gate.version).slice(0, 16) : ""}` : "BELAJAR 90d (backtest)";
  return `${src} (diuji): ${p.join(" · ")}${tag}.`;
}
function renderLessons() {
  const el = document.getElementById("lessons-body"); if (!el) return;
  const st = document.getElementById("ls-status");
  if (st && LEARN.gate) {
    const bt = LEARN.gate.baseline || {};
    const baseTxt = bt.test != null ? bt.test : (bt.dirTest != null ? bt.dirTest : null);
    const ver = LEARN.meta && LEARN.meta.meta && LEARN.meta.meta.version ? ` · model ${String(LEARN.meta.meta.version).slice(0, 16)}` : "";
    st.textContent = `${LEARN.gate.rules.length} aturan arah · ${LEARN.touch ? LEARN.touch.rules.length : 0} aturan lock-touch · baseline uji ${baseTxt != null ? (baseTxt * 100).toFixed(1) + "%" : "—"} (${LEARN.gate.rows || "?"} sinyal) · sumber: ${LEARN.src || "—"}${ver}`;
  }
  const l = LEARN.lessons;
  if (!l || !l.lessons || !l.lessons.length) { el.innerHTML = '<div class="cd-empty">belum ada data pelajaran</div>'; return; }
  const row = (x) => x.type === "cause"
    ? `<div class="ls-row ls-cause"><span class="ls-k">${x.feature}=${x.bucket}</span><span class="ls-v">muncul ${(x.pLose * 100).toFixed(1)}% di sinyal SALAH vs ${(x.pWin * 100).toFixed(1)}% benar</span></div>`
    : `<div class="ls-row ${x.type === "boost" ? "ls-boost" : "ls-sup"}"><span class="ls-k">${x.rule}</span><span class="ls-v">${(x.wrTest * 100).toFixed(1)}% (n=${x.nTest})</span></div>`;
  const sec = (t, arr, cls) => arr.length ? `<div class="ls-sec ${cls}"><b>${t}</b>${arr.slice(0, 6).map(row).join("")}</div>` : "";
  el.innerHTML =
    sec("✔ Konteks kuat (lolos uji)", l.lessons.filter((x) => x.type === "boost"), "c-boost") +
    sec("⚠ Konteks lemah — hindari", l.lessons.filter((x) => x.type === "suppress"), "c-sup") +
    sec("🔎 Penyebab sinyal salah", l.lessons.filter((x) => x.type === "cause"), "c-cause");
}
window.setLearnBlock = (v) => { LEARN_BLOCK = !!v; console.log("[LEARN] tahan konteks lemah =", LEARN_BLOCK); return LEARN_BLOCK; };
window.learnStatus = () => ({ status: LEARN.status, block: LEARN_BLOCK, gateRules: LEARN.gate ? LEARN.gate.rules.length : 0, touchRules: LEARN.touch ? LEARN.touch.rules.length : 0 });

/* ===== STATUS LEARNER (panel UI): progress, pelajaran, penahan aktif, riwayat penyesuaian =====
   Dihitung dari /api/learner (server) + penghitung sesi ini di browser. Tujuannya agar user bisa
   memantau: (1) sudah berapa data belajar terkumpul, (2) pelajaran apa yang didapat,
   (3) penyesuaian apa yang sedang berlaku pada sinyal. */
const LEARN_STATS = { signals: 0, strong: 0, mixed: 0, weak: 0, wouldBlock: 0 };
let LEARNER_STATUS = null;
let LEARNER_ERR = null;
let _lastModelVersion = null;
async function loadLearnerStatus() {
  try {
    const r = await fetch("/api/learner", { cache: "no-store" });
    if (r.ok) { LEARNER_STATUS = await r.json(); LEARNER_ERR = null; }
    else LEARNER_ERR = "HTTP " + r.status;
  } catch (e) { LEARNER_ERR = e && e.message ? e.message : "gagal memuat"; }
  // --- Sinkronkan profil gate + deteksi model baru ---
  // Supaya angka ambang di SEMUA panel sinyal ikut berubah begitu learner mempromosikan
  // model baru, tanpa perlu reload halaman dan tanpa menunggu interval 30 menit.
  try {
    const S = LEARNER_STATUS;
    if (S && S.gates && S.gates.tiers) {
      GATES = S.gates;
      renderGateLine();
    }
    // Tanda tangan model: versi model konteks + mode/ambang gate. Promosi bisa terjadi pada
    // salah satu saja (mis. ambang belajar menang tapi model konteks belum) — keduanya harus
    // memicu muat ulang supaya panel PELAJARAN dan baris BELAJAR di REASON ikut terupdate.
    const sig = S ? `${S.model ? S.model.version : "-"}|${S.gates ? S.gates.mode : "-"}|${S.gates && S.gates.thresholds ? JSON.stringify(S.gates.thresholds) : "-"}` : null;
    if (sig && sig !== _lastModelVersion) {
      const first = _lastModelVersion === null;
      _lastModelVersion = sig;
      if (!first) {
        console.log("[LEARN] model/ambang berubah ->", sig.slice(0, 80), "· memuat ulang tabel & pelajaran");
        loadLearn();          // tabel konteks + pelajaran (panel PELAJARAN & baris BELAJAR di REASON)
        renderGateLine();
      }
    }
  } catch (_) {}
  renderLearnerStatus();
}
const esc = (s) => String(s == null ? "" : s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
function renderLearnerStatus() {
  updateLearnChip();
  const el = document.getElementById("lstat-body"); if (!el) return;
  if (LEARNER_ERR || !LEARNER_STATUS) { el.innerHTML = `<div class="cd-empty">status learner belum tersedia${LEARNER_ERR ? " (" + esc(LEARNER_ERR) + ")" : ""}</div>`; return; }
  const S = LEARNER_STATUS, L = S.ledger || {}, M = S.model || {}, g = S.gates || {}, C = S.capture || {};
  const pctv = Math.round((L.pct || 0) * 1000) / 10;
  const need = Math.max(0, (L.target || 300) - (L.canonicalWithRes || 0));
  const ctxReady = (L.canonicalWithRes || 0) >= (L.targetCtx || 120);
  const learned = g.mode === "learned";
  // ---- hasil perbaikan (terukur pada jendela uji) ----
  const gm = g.metrics || null;
  const baseT = (g.baselineTest && g.baselineTest.takenWinrate != null) ? g.baselineTest.takenWinrate
    : (g.baselineTest && g.baselineTest.wr != null) ? g.baselineTest.wr : null;
  const hasilTxt = gm
    ? `<div class="lstat-line">sinyal yang <b>diambil</b>: winrate <b>${((gm.takenWinrate || 0) * 100).toFixed(1)}%</b>${baseT != null ? ` (sebelum disaring: ${(baseT * 100).toFixed(1)}%)` : ""} · cakupan <b>${((gm.coverage || 0) * 100).toFixed(0)}%</b></div>
       <div class="lstat-line lstat-dim">artinya: dari semua sinyal, ${((gm.coverage || 0) * 100).toFixed(0)}% tetap diambil dan winrate subset itu ${((gm.takenWinrate || 0) * 100).toFixed(1)}% (diukur pada 30% data paling akhir, tidak dipakai saat melatih).</div>`
    : `<div class="lstat-line lstat-dim">belum ada hasil terukur — masih mengumpulkan data. Setelah cukup, baris ini akan menampilkan perbandingan <b>sebelum vs sesudah</b> penyaringan.</div>`;
  // ---- riwayat + alasan belum diganti ----
  const hist = (S.history || []).slice().reverse();
  const lastKeep = hist.find((h) => !h.promote);
  const histHtml = hist.length ? hist.slice(0, 6).map((h) => `<div class="lstat-row">
      <span class="lstat-badge ${h.promote ? "ok" : "def"}">${h.promote ? "DIPAKAI" : "DITAHAN"}</span>
      <span class="lstat-dim">${h.at ? new Date(h.at).toLocaleString() : ""} · pemicu ${esc(h.trigger || "—")} · data ${h.rows != null ? h.rows : "—"}</span>
      <span>${esc(h.why || "")}</span></div>`).join("") : '<div class="lstat-dim">belum ada keputusan (menunggu data cukup)</div>';
  // ---- aksi selanjutnya ----
  const acts = [];
  if (need > 0) {
    acts.push(`Kumpulkan <b>${need}</b> sinyal kanonik berhasil lagi` + (L.rate24h ? ` — laju sekarang <b>${L.rate24h}/24 jam</b>${L.etaDays != null ? `, perkiraan <b>${L.etaDays} hari</b>` : ""}` : "") + `.`);
    if (!L.rate24h) acts.push(`Belum ada data kanonik baru dalam 24 jam terakhir — capture server perlu menghasilkan data. Cek status capture di bagian 1 (harus <b>AKTIF</b>) dan jam sesi 5m/15m.`);
  } else {
    acts.push(learned ? `Data sudah cukup dan model belajar <b>sudah aktif</b>. Re-fit berikutnya: ${S.nextRefitAt ? new Date(S.nextRefitAt).toLocaleString() : "03:00 jam server"}.`
      : `Target data tercapai. Re-fit otomatis berikutnya <b>${S.nextRefitAt ? new Date(S.nextRefitAt).toLocaleString() : "03:00 jam server"}</b>, atau jalankan <code>POST /api/model/refit</code>. Model hanya dipakai bila <b>menang pada jendela uji</b>.`);
  }
  if (g.mode === "bootstrap") acts.push(`Sedang <b>BOOTSTRAP</b> (ambang dilonggarkan supaya sinyal lebih sering) → winrate yang tampil memang lebih rendah. Ini disengaja sampai learner mengetatkan sendiri. Balik cepat: <code>GATES_MODE=strict</code>.`);
  if (lastKeep && !learned) acts.push(`Keputusan terakhir <b>DITAHAN</b>: ${esc(lastKeep.why || "")}.`);
  if (learned) acts.push(`Aktifkan penahanan konteks lemah (opsional): <code>window.setLearnBlock(true)</code> — sinyal pada konteks tervalidasi lemah akan ditahan (mode <code>LEARN-BLOCK</code>).`);
  const blk = [...((S.blockers || {}).gate || []), ...((S.blockers || {}).touch || [])];
  if (blk.length) acts.push(`Penahan konteks aktif: ${blk.map((k) => `<code>${esc(k)}</code>`).join(" · ")}`);
  const actsHtml = acts.map((a, i) => `<div class="lstat-line">${i + 1}. ${a}</div>`).join("");
  el.innerHTML = `
    <div class="lstat-sec">
      <b>1 · PROGRESS DATA BELAJAR</b>
      <div class="lstat-bar"><i style="width:${Math.min(100, pctv)}%"></i></div>
      <div class="lstat-line"><b>${L.canonicalWithRes || 0}</b> / ${L.target || 300} sinyal kanonik berhasil (${pctv}%)${L.rate24h != null ? ` · laju ~${L.rate24h}/24 jam` : ""}${L.etaDays != null && need > 0 ? ` · perkiraan <b>${L.etaDays} hari</b> lagi` : ""}</div>
      <div class="lstat-line lstat-dim">model <b>konteks</b> sudah bisa dibentuk pada ${L.targetCtx || 120} data ${ctxReady ? "(tercapai ✓)" : ""}; ambang numerik butuh ${L.target || 300} baris berarah.${L.spanHours ? ` Laju dihitung dari ${L.spanHours} jam pengamatan.` : ""}</div>
      <div class="lstat-line">capture otomatis server: ${C.enabled ? '<span class="lstat-badge ok">AKTIF</span>' : '<span class="lstat-badge def">MATI</span>'} · <b>${C.captured || 0}</b> tersimpan · ${C.accepted || 0} lolos gate · ${C.rejected || 0} ditolak (tetap direkam)${C.errors ? ` · <span class="lstat-warn">${C.errors} error</span>` : ""}${C.lastAt ? ` · terakhir ${new Date(C.lastAt).toLocaleTimeString()}` : ""}</div>
      <div class="lstat-grid">
        <span><i>total record</i><b>${L.total || 0}</b></span>
        <span><i>dengan hasil</i><b>${L.withRes || 0}</b></span>
        <span><i>kanonik (detik-2)</i><b>${L.canonical || 0}</b></span>
        <span><i>tengah sesi (dibuang)</i><b>${L.late || 0}</b></span>
        <span><i>data baru sejak promosi</i><b>${L.sincePromote || 0}</b></span>
        <span><i>re-fit berikutnya</i><b>${S.nextRefitAt ? new Date(S.nextRefitAt).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }) : "—"}</b></span>
      </div>
    </div>
    <div class="lstat-sec">
      <b>2 · APA YANG DI-IMPROVE</b>
      <div class="lstat-line">Yang dipelajari sistem: <b>(a) konteks</b> — kombinasi tier/aset/jam/interval yang historis lemah ditahan, yang kuat diunggulkan; <b>(b) ambang</b> — batas numerik (volRel2, surprise, gap, likuiditas) disesuaikan dari data nyata.</div>
      <div class="lstat-line">metode: split <b>70/30 berurutan waktu</b> (latih = data paling awal, uji = 30% paling akhir) + <b>Wilson bound</b>; ambang dicari dengan coordinate-ascent memaksimalkan batas bawah Wilson, dengan syarat cakupan ≥20%. Model hanya dipakai bila <b>menang pada jendela uji</b>.</div>
      <div class="lstat-line">status model: ${learned ? '<span class="lstat-badge ok">AMBANG HASIL BELAJAR AKTIF</span>' : (g.mode === "strict" ? '<span class="lstat-badge def">AMBANG KONSERVATIF</span>' : '<span class="lstat-badge sup">BOOTSTRAP — BELUM ADA AMBANG BELAJAR</span>')}</div>
      <div class="lstat-line">ambang aktif: ${(g.thresholds && g.thresholds.length) ? g.thresholds.map((t) => `<code>${esc(t.f)} ${esc(t.op)} ${esc(t.t)}</code>`).join(" · ") : '<span class="lstat-dim">belum ada (memakai tier ladder saja)</span>'}</div>
      <div class="lstat-line lstat-dim">tier: STRONG volRel2≥${g.tiers ? g.tiers.STRONG.volRel2 : "—"}${g.tiers && g.tiers.STRONG.surprise ? " & surprise≥" + g.tiers.STRONG.surprise : ""} · GOOD ≥${g.tiers ? g.tiers.GOOD.volRel2 : "—"} · FAIR ≥${g.tiers && g.tiers.FAIR ? g.tiers.FAIR.volRel2 : "—"} · floor likuiditas ×${g.liqFloorMul != null ? g.liqFloorMul : "—"} · batas telat ${g.lateFrac != null ? (g.lateFrac * 100).toFixed(0) + "%" : "—"}${learned && g.promotedAt ? ` · dipromosikan ${new Date(g.promotedAt).toLocaleString()}` : ""}</div>
    </div>
    <div class="lstat-sec">
      <b>3 · HASIL PERBAIKAN (terukur pada data uji)</b>
      ${hasilTxt}
    </div>
    <div class="lstat-sec">
      <b>4 · EFEK DI BROWSER INI (sejak halaman dibuka)</b>
      <div class="lstat-line">sinyal diamati <b>${LEARN_STATS.signals}</b> · konteks kuat <b>${LEARN_STATS.strong}</b> · campuran <b>${LEARN_STATS.mixed}</b> · lemah <b>${LEARN_STATS.weak}</b> · <span class="${LEARN_STATS.wouldBlock ? "lstat-warn" : "lstat-dim"}">akan ditahan <b>${LEARN_STATS.wouldBlock}</b></span>${LEARN_BLOCK ? ' <span class="lstat-badge sup">TAHAN AKTIF</span>' : ' <span class="lstat-badge def">TAHAN MATI</span>'}</div>
    </div>
    <div class="lstat-sec">
      <b>5 · RIWAYAT PENYESUAIAN MODEL</b> <span class="lstat-dim">(build JS: ${BUILD})</span>
      ${histHtml}
    </div>
    <div class="lstat-sec">
      <b>6 · AKSI SELANJUTNYA</b>
      ${actsHtml}
    </div>`;
}
/* ===== PHASE 2 LEDGER — catat setiap sinyal + vektor fitur lengkap + hasilnya =====
   Learner butuh data jangka panjang: Binance hanya menyediakan 1s klines 7 hari, jadi
   fitur skala detik (volRel2, surprise, gap, tier, MFE/MAE, waktu-ke-lock) harus
   dikumpulkan sendiri mulai sekarang.
   Alur: addSignal() saat sinyal terkunci -> resolve() saat ronde berakhir (dengan jalur
   harga) -> dikirim ke /api/ledger, server menulisnya ke volume persisten.
   Salinan lokal di localStorage supaya tahan tutup browser & bisa re-sync nanti. */
const LEDGER = (() => {
  const LS_KEY = "bps_ledger_v1";
  const MAX = 6000;                       // batas salinan lokal (server = sumber utama)
  let map = new Map();
  try { const raw = JSON.parse(localStorage.getItem(LS_KEY) || "[]"); for (const r of raw) if (r && r.k) map.set(r.k, r); } catch (_) {}
  let sending = false, uploaded = 0, failed = 0, serverTotal = null, lastAt = null;
  const save = () => { try { localStorage.setItem(LS_KEY, JSON.stringify([...map.values()].slice(-MAX))); } catch (_) {} };
  const keyOf = (asset, interval, t0Sec) => `${asset}_${interval}_${t0Sec}`;
  function upsert(k, part) {
    const prev = map.get(k) || { k, v: 1 };
    const merged = Object.assign({}, prev, part);
    // Vektor fitur = snapshot PERTAMA (detik ke-2). Capture ulang sesi yang sama — mis. setelah
    // user menekan reset di panel akurasi (yang mengosongkan _deskSigMap/_loggedKeys) — TIDAK
    // boleh menimpa dengan snapshot dari menit yang lebih lambat, karena itu merusak data belajar.
    if (prev.sig) merged.sig = prev.sig;          // first write wins
    else if (part.sig) merged.sigSync = false;
    // Hasil boleh diperbarui (mis. perkiraan 1m dari server -> jalur 1s dari klien yang lebih presisi).
    if (part.res) merged.resSync = false;
    if (prev.res && !part.res) merged.res = prev.res;
    merged.upd = Date.now();
    map.set(k, merged); save();
    return merged;
  }
  const pending = () => [...map.values()].filter((r) => !r.sigSync || (r.res && !r.resSync));
  async function flush() {
    if (sending) return;
    const list = pending().slice(0, 150);
    if (!list.length) return;
    sending = true;
    try {
      const res = await fetch("/api/ledger", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ records: list }) });
      const j = await res.json();
      if (j && typeof j.saved === "number") {
        for (const r of list) { r.sigSync = true; if (r.res) r.resSync = true; }
        save(); uploaded += list.length; serverTotal = j.total; lastAt = Date.now();
      }
    } catch (_) { failed++; } finally { sending = false; }
  }
  // ===== Riwayat dari SERVER (sumber utama panel akurasi) =====
  // Server menyimpan setiap sinyal kanonik + hasilnya (res.won/lock/close/actual) di volume
  // persisten. Panel akurasi membacanya dari sini supaya SEMUA device melihat riwayat yang sama,
  // bukan hanya apa yang sempat tercatat di localStorage device ini.
  let srvCache = null, srvAt = 0, srvLoading = false;
  async function server(force) {
    if (!force && srvCache && Date.now() - srvAt < 15000) return srvCache;
    if (srvLoading) return srvCache;
    srvLoading = true;
    try {
      const res = await fetch("/api/ledger?dump=1", { cache: "no-store" });
      const j = await res.json();
      srvCache = { records: (j && j.records) || [], stats: (j && j.stats) || null };
      srvAt = Date.now();
    } catch (_) { /* offline -> panel memakai salinan lokal */ } finally { srvLoading = false; }
    return srvCache;
  }
  return {
    server,
    serverCached: () => srvCache,
    addSignal(rec) { if (!rec || !rec.asset) return; const k = keyOf(rec.asset, rec.interval, rec.t0); upsert(k, { asset: rec.asset, interval: rec.interval, t0: rec.t0, sig: rec }); flush(); },
    resolve(asset, interval, t0Sec, res) {
      if (!asset) return;
      const k = keyOf(asset, interval, t0Sec);
      const prev = map.get(k);
      // Tanpa vektor fitur (sig) hasilnya tidak bisa dipakai belajar -> jangan kirim record kosong.
      if (!prev || !prev.sig) return;
      upsert(k, { asset, interval, t0: t0Sec, res });
      flush();
    },
    flush,
    status: () => ({ local: map.size, pending: pending().length, uploaded, failed, serverTotal, lastAt }),
    _map: map,
  };
})();
// Kirim ulang saat tab kembali aktif / koneksi pulih (jangan menunggu timer).
try {
  document.addEventListener("visibilitychange", () => { if (!document.hidden) LEDGER.flush(); });
  window.addEventListener("online", () => LEDGER.flush());
} catch (_) {}
setInterval(() => LEDGER.flush(), 20000);

// Jalur harga satu sesi (1s diutamakan, fallback 5s) mulai dari detik sinyal (t0+2).
function sessionPath(asset, t0Sec, endSec) {
  const pick = (arr) => (arr || []).filter((c) => c.time >= t0Sec + 2 && c.time < endSec);
  let a = pick(state.cache[asset]?.["1s"]?.candles);
  if (a.length < 20) { const b = pick(state.cache[asset]?.["5s"]?.candles); if (b.length >= 10) a = b; }
  return a;
}
// Hasil satu sesi untuk learner: arah, sentuh-lock, waktu-ke-lock, MFE/MAE (relatif ke lock).
function ledgerOutcome(lock, close, dir, path) {
  const v = (p) => (dir === "up" ? (p - lock) / lock * 100 : (lock - p) / lock * 100);
  let mfe = -Infinity, mae = Infinity, touch = 0, tTouch = null;
  for (const c of path) {
    const H = c.high != null ? c.high : c.h, L = c.low != null ? c.low : c.l;
    const fHi = dir === "up" ? v(H) : v(L);
    const fLo = dir === "up" ? v(L) : v(H);
    if (fHi > mfe) mfe = fHi;
    if (fLo < mae) mae = fLo;
    if (!touch && fHi >= 0) { touch = 1; tTouch = c.time; }
  }
  const actual = close >= lock ? "up" : "down";
  return {
    lock: +lock, close: +close, actual, won: dir === actual ? 1 : 0,
    touch, tTouchSec: tTouch != null ? tTouch : null,
    mfeFav: isFinite(mfe) ? +mfe.toFixed(4) : null,
    maeFav: isFinite(mae) ? +mae.toFixed(4) : null,
    endFav: +v(close).toFixed(4), bars: path.length,
  };
}
window.ledgerStatus = () => LEDGER.status();

// Tampilkan status ledger (data belajar) di panel pelajaran.
function renderLedgerStatus() {
  const el = document.getElementById("ls-ledger"); if (!el) return;
  const s = LEDGER.status();
  el.textContent =
    `${s.local} lokal · ${s.pending} belum terkirim · ${s.uploaded} terkirim sesi ini` +
    (s.serverTotal != null ? ` · server tersimpan ${s.serverTotal}` : " · server —") +
    (s.failed ? ` · ${s.failed} gagal` : "") +
    (s.lastAt ? ` · terakhir ${new Date(s.lastAt).toLocaleTimeString()}` : "");
}
setInterval(renderLedgerStatus, 10000);

// TRAIL on a 15s-smoothed price (1s klines). Backtest (BTC+ETH, n=2651): trailing the smoothed
// price by 0.01% after the lock turns the expectancy POSITIVE (+0.014%/trade, win 64%), while a
// raw 1s trailing stop is whipsawed by noise. This is the practical way to capture more than the lock.
function trailOf(sym, t0Sec, nowSec, lock, isUp) {
  const ones = state.cache[sym]?.["1s"]?.candles || [];
  const sess = ones.filter((c) => c.time >= t0Sec && c.time < nowSec);
  if (sess.length < 5) return null;
  // candle 1s memakai {time,open,high,low,close,vol}; bacaan defensif agar tetap benar
  // bila sumbernya mengirim {t,o,h,l,c,v} (sebelumnya salah baca c.c -> SMA NaN -> ladder
  // tidak pernah aktif).
  const px = (c) => (c.close != null ? c.close : c.c);
  const closes = sess.map(px);
  if (closes.some((v) => typeof v !== "number" || !isFinite(v))) return null;
  const sma = [];
  for (let i = 0; i < closes.length; i++) {
    const w = closes.slice(Math.max(0, i - 14), i + 1);
    sma.push(w.reduce((x, y) => x + y, 0) / w.length);
  }
  const smaNow = sma[sma.length - 1];
  const smaPeak = isUp ? Math.max(...sma) : Math.min(...sma);
  const exitPrice = isUp ? smaPeak * (1 - 0.0001) : smaPeak * (1 + 0.0001);
  return { smaNow, smaPeak, exitPrice, armed: isUp ? smaNow >= lock : smaNow <= lock };
}

// LOCK-TOUCH: at 2s the price sits a small distance from the lock; historically it comes back
// with this probability (5m: 92.8% within 0.005%, 82.6% 0.005-0.01%, 79.5% 0.01-0.02%, ...).
function lockTouchOf(tf, dist, dir) {
  const t = (TIERS && TIERS.locktouch && TIERS.locktouch.tiers) ? TIERS.locktouch.tiers[tf] : null;
  if (!t || !t.buckets) return null;
  for (const b of t.buckets) {
    const hi = b.hi == null ? Infinity : b.hi;
    if (dist >= b.lo && dist < hi) {
      return { dir, dist, rate: b.rate, tMed: b.tMed, ddMed: b.ddMed, tooClose: dist < 0.005, total: t.total };
    }
  }
  return null;
}
// Measured winrate for the 2-second tiers (backtest/out/early2s.json).
function early2sWR(grade) {
  if (!TIERS || !TIERS.early2s || !TIERS.early2s.tiers) return null;
  const t = TIERS.early2s.tiers[grade];
  return t ? t.wr : null;
}
// Measured winrate for the exact minute the entry appeared (falls back to the grade table).
function minuteWR(tf, minuteIn) {
  if (!TIERS || !TIERS.byMinute) return null;
  const t = TIERS.byMinute[tf];
  const o = t && t[String(minuteIn)];
  return o ? o.wr : null;
}
// Continuation potential after the price reaches the lock: how much further it typically runs
// before reversing (measured on 90d). cp = continuation score (0-100).
function continuationOf(tf, cp, isUp, price) {
  const t = (TIERS && TIERS.continuation && TIERS.continuation.tiers) ? TIERS.continuation.tiers[tf] : null;
  if (!t) return null;
  const strong = cp >= 70, mid = cp >= 45;
  const est = strong ? t.mfe.p75 : mid ? t.mfe.p50 : t.mfe.p25;
  const prob = strong ? (t.probAligned ? t.probAligned.ge010 : t.prob.ge010) : t.prob.ge005;
  const target = isUp ? price * (1 + est / 100) : price * (1 - est / 100);
  return {
    cp, est, prob,
    peakMin: t.peakMin ? t.peakMin.p50 : null,
    bucket: strong ? "lanjut kuat" : mid ? "lanjut sedang" : "mulai melemah",
    target,
  };
}
// CATATAN: string di bawah adalah KEY tabel kalibrasi lama (bukan ambang yang berlaku).
// Jangan ditampilkan ke user tanpa label "kalibrasi ambang lama".
function gradeVariant(tf, grade) {
  if (grade === "STRONG") return "OFI strong+vol>=3";
  if (grade === "GOOD") return "OFI agree+vol>=3";
  if (grade === "FAIR") return tf === "5m" ? "OFI agree+vol>=0.5" : tf === "15m" ? "OFI agree+vol>=2" : "OFI agree+vol>=1.5";
  return null;
}
function gradeWR(tf, grade) {
  if (!TIERS || !TIERS.tiers) return null;
  const v = gradeVariant(tf, grade);
  if (!v) return null;
  const o = TIERS.tiers[`${tf}|${v}`];
  return o ? o.wr : null;
}
// Minimum volume pace for the FAIR tier, per interval (calibrated 30d).
// Ambang volume yang SEDANG BERLAKU untuk tier FAIR — dibaca dari profil gate aktif,
// bukan angka tetap, supaya teks UI tidak pernah menyimpang dari gate yang dipakai.
function fairMinVol(tf) {
  const T = GATES && GATES.tiers;
  return (T && T.FAIR && T.FAIR.volRel2 != null) ? T.FAIR.volRel2 : 0.9;
}
// Ringkasan ambang aktif untuk ditampilkan di UI (satu sumber kebenaran).
function gatesSummary() {
  const T = (GATES && GATES.tiers) || null;
  if (!T) return "—";
  const thTxt = (GATES.thresholds && GATES.thresholds.length)
    ? GATES.thresholds.map((t) => `${t.f}${t.op}${t.t}`).join(" & ") : "";
  const learned = GATES.mode === "learned";
  const tierTxt = `${learned ? "label tier" : "tier"} STRONG volRel2≥${T.STRONG.volRel2}${T.STRONG.surprise ? " & surprise≥" + T.STRONG.surprise : ""}` +
    ` · GOOD ≥${T.GOOD.volRel2}${T.GOOD.surprise ? " & surprise≥" + T.GOOD.surprise : ""} · FAIR ≥${T.FAIR.volRel2}`;
  const tail = `floor likuiditas ×${GATES.liqFloorMul} · batas telat ${(GATES.lateFrac * 100).toFixed(0)}% · mode ${GATES.mode}`;
  // Saat mode learned, yang MENGIKAT adalah daftar ambang hasil belajar — tampilkan lebih dulu.
  return learned && thTxt
    ? `ambang efektif: ${thTxt} · ${tierTxt} · ${tail}`
    : `${tierTxt}${thTxt ? " · ambang: " + thTxt : ""} · ${tail}`;
}
// After this fraction of the session the price sits close to the lock, so the reward of a
// recapture is tiny even if the direction is right -> those entries are suppressed.
const LATE_FRAC = 0.7;
function pctile(arr, p) {
  if (!arr || !arr.length) return 0;
  const s = arr.slice().sort((a, b) => a - b);
  const idx = Math.min(s.length - 1, Math.max(0, Math.floor((p / 100) * (s.length - 1))));
  return s[idx];
}
// Self-contained stats helpers (top-level, so any scope can use them safely).
function meanOf(a) { return a && a.length ? a.reduce((x, y) => x + y, 0) / a.length : 0; }
function stdOf(a) { const m = meanOf(a); return a && a.length ? Math.sqrt(a.reduce((s, x) => s + (x - m) * (x - m), 0) / a.length) : 0; }
function slopeOf(candles) {
  const n = candles ? candles.length : 0;
  if (n < 3) return 0;
  let sx = 0, sy = 0, sxx = 0, sxy = 0;
  for (let i = 0; i < n; i++) { const x = i, y = candles[i].close; sx += x; sy += y; sxx += x * x; sxy += x * y; }
  const den = n * sxx - sx * sx;
  return den ? (n * sxy - sx * sy) / den : 0;
}

/* ===== Wide-screen dual-coin monitor =====
   Compact per-coin card (recommendation + trade-assistant action + health) computed for
   BOTH coins so they can be watched side by side. Uses the same engine as the detail view. */
const MON_WINDOW = { "5m": 24, "15m": 60, "1h": 120 };
function analyzeCoin(asset, tf, now) {
  const dur = INTERVAL_MS[tf];
  const t0 = Math.floor(now / dur) * dur;
  const t0Sec = Math.floor(t0 / 1000);
  const five = state.cache[asset]?.["5s"]?.candles || [];
  if (five.length < 5) return null;
  const win = five.slice(-(MON_WINDOW[tf] || 120));
  const C = win[win.length - 1].close;
  const O = sessionLock(asset, dur, now);
  if (O == null || C == null) return null;
  const closes = win.map((c) => c.close);
  const mean = meanOf(closes);
  const std = stdOf(closes);
  const z = std > 0 ? (C - mean) / std : 0;
  const slope = slopeOf(win);
  const seg = win.slice(-Math.max(3, Math.ceil(win.length / 3)));
  const slopeRecent = slopeOf(seg);
  const nowSec = Math.floor(now / 1000);
  const rsi = SignalCore.rsiFromSeries((state.cache[asset]?.["5m"]?.candles || []).filter((c) => c.time < nowSec).slice(-50), 14);
  const ofi = sessionOFI(asset, t0Sec, nowSec);
  const ofiShort = sessionOFI(asset, nowSec - 120, nowSec);
  const histTrend = analyzeHistoricalTrend(asset, tf, 50);
  const key = `${asset}_${tf}_${t0}`;
  // SATU SUMBER KEBENARAN: sama dengan rekomendasi utama (graded EARLY cache, non-flat).
  // Sebelumnya jalur ini fallback ke _deskSigLive sehingga bisa BEDA dengan rekomendasi utama
  // (itu penyebab TA terlihat 'bingung'). liveSig hanya dipakai untuk teks status saat flat.
  const srvSigA = (typeof LIVE !== "undefined") ? LIVE.signalFor(asset, tf) : null;   // sinyal server
  const srvEntry = (typeof LIVE !== "undefined") ? LIVE.entryFor(asset, tf) : null;    // entri per tf (semua tf)
  const sig = srvSigA || _deskSigCache[key] || null;
  // Saat sinyal flat, teks mode diambil dari server (skipped) supaya tidak perlu cache lokal.
  const liveSig = sig || _deskSigLive[key] || (srvEntry && srvEntry.skipped && srvEntry.skipped !== "pending"
    ? { mode: String(srvEntry.skipped).toUpperCase() } : null);
  const isUp = !!(sig && sig.verdict === "up");
  const recentC = win.slice(-12);
  const counterVol = recentC.filter((c) => (isUp ? c.close < c.open : c.close > c.open)).reduce((a, c) => a + (c.vol || 0), 0);
  const totVol = recentC.reduce((a, c) => a + (c.vol || 0), 0);
  const volAgainst = totVol > 0 ? (counterVol / totVol) / 0.5 : null;
  // Arah TRADE (contra-lock) dipakai untuk semua input TA: favor/peak/retreat/turn/fade.
  const taBiasTmp = sig ? tradeDirOf(sig.verdict) : null;   // arah posisi = arah rekomendasi
  const taUp = taBiasTmp === "up";
  const favor = taUp ? (C - O) : (O - C);
  const peakFavor = Math.max(_tradePeak[key] || -Infinity, favor);
  _tradePeak[key] = peakFavor;
  const retreat = peakFavor > 0 && (peakFavor - favor) >= 0.25 * Math.max(std, 1e-9);
  let health = (sig && sig.verdict !== "flat")
    ? computeSignalHealth(sig.verdict, {
        margin: isUp ? (C - O) : (O - C),
        marginStd: std > 0 ? Math.abs(C - O) / std : null,
        slope, slopeRecent, ofi, ofiShort, volAgainst, rsi, z, histTrend,
      })
    : null;
  const turn = turnEvidence(taUp, { slope, slopeRecent, ofiShort, win });
  const fade = fadeEvidence(taUp, { slope, slopeRecent, ofiShort, retreat, rsi });
  const dw = _tradeDwell[key] || (_tradeDwell[key] = { turnSince: null, fadeSince: null });
  dw.turnSince = turn.count >= 2 ? (dw.turnSince || now) : null;
  dw.fadeSince = fade.count >= 2 ? (dw.fadeSince || now) : null;
  const entered = !!(_tradeEntered[key] && _tradeEntered[key].entered);
  const taBias = taBiasTmp;   // contra-lock (menuju LOCK)
  // PLAN = hasil proses SERVER (satu sumber kebenaran). Klien hanya menghitung sendiri
  // sebagai cadangan bila snapshot server belum ada/putus (offline), memakai rumus yang sama.
  const srvPlan = taBias ? serverPlanFor(asset, tf) : null;
  const planSrc = srvPlan ? "server" : "local";
  const plan = srvPlan || (taBias
    ? computeTradePlan(taBias, {
        tf, lock: O, price: C, std, slope, slopeRecent, rsi, z, ofi, ofiShort, retreat, health,
        entered, turn, fade,
        trail: trailOf(asset, t0Sec, nowSec, O, taBias === "up"),
        dwellTurnMs: dw.turnSince ? now - dw.turnSince : 0,
        dwellFadeMs: dw.fadeSince ? now - dw.fadeSince : 0,
        histTrend,
      })
    : null);

  // STATUS ENTRY / EARLY CLOSE untuk kartu DUAL (dan monitor). Sebelumnya status hanya direkam
  // di jalur utama (koin aktif) sehingga kartu dual selalu menampilkan WAITING walau posisi
  // sudah dibuka. Key-nya SAMA (asset_tf_sessionStart-ms) sehingga state dibagi dengan jalur
  // utama; penjaga 'belum ada' mencegah pencatatan ganda.
  if (plan) {
    if (srvPlan) {
      // Plan dari server sudah membawa statusEntry/statusClose/entryPrice. Salin ke state lokal
      // supaya jalur pencatatan (akurasi + ledger) memakai angka yang SAMA dengan yang ditampilkan.
      if (plan.entryPrice != null && !_tradeEntered[key]) {
        _tradeEntered[key] = { entered: true, since: plan.statusEntry ? plan.statusEntry.at : now, price: plan.entryPrice };
      }
      if (!plan.entered) { delete _tradeEntered[key]; delete _tradeClosed[key]; _closeSounded.delete(key); }
      if (plan.statusClose && plan.statusClose.ok && !_tradeClosed[key]) {
        _tradeClosed[key] = { at: plan.statusClose.at, price: plan.statusClose.price };
      }
      if (plan.health) health = plan.health;   // health dari server (dihitung server, sama dgn yg dipakai plan)
    } else {
      if (plan.entered && !_tradeEntered[key]) {
        _tradeEntered[key] = { entered: true, since: now, price: C };
        console.log(`[TRADE] position opened ${asset}/${tf} @ ${fmtPrice(C)}`);
      }
      if (!plan.entered) { delete _tradeEntered[key]; delete _tradeClosed[key]; _closeSounded.delete(key); }
      if (plan.state === "CLOSE" && !_tradeClosed[key]) {
        _tradeClosed[key] = { at: now, price: C };
        console.log(`[TRADE] early close disinyalkan ${asset}/${tf} @ ${fmtPrice(C)}`);
      }
      const _ent = _tradeEntered[key], _clo = _tradeClosed[key];
      plan.entryPrice = _ent ? _ent.price : null;
      plan.statusEntry = {
        ok: !!_ent, at: _ent ? _ent.since : null, price: _ent ? _ent.price : null,
        waiting: plan.state === "STAND_DOWN" ? "reversal terdeteksi — tunggu setup baru"
          : (plan.state === "WAIT" && /TUNGGU PEAK/.test(plan.action) ? "konfirmasi peak contra (2/4 bagian + 4s)" : "harga belum contra / belum kembali ke lock"),
      };
      plan.statusClose = { ok: !!_clo, at: _clo ? _clo.at : null, price: _clo ? _clo.price : null };
    }
  }
  return { key, C, O, std, slope, slopeRecent, rsi, z, sig, health, plan, planSrc, ofi, ofiShort, histTrend,
    liveMode: liveSig ? liveSig.mode : null };   // mode live (utk teks status saat tidak ada sinyal graded)
}

function switchAsset(asset) {
  if (state.asset === asset) return;
  state.asset = asset;
  _deskSig = null;
  const btn = document.querySelector(`#asset-seg [data-asset="${asset}"]`);
  if (btn) segActive("asset-seg", btn);
  renderActive(); updateProjection(); updateMobilePrediction(); updateGap(); renderConfidenceReport();
}

let _monLastAt = 0;
let _monLastHtml = "";
function renderMonitors(force) {
  const el = document.getElementById("monitors");
  if (!el) return;
  if (!force && typeof window.matchMedia === "function" && !window.matchMedia("(min-width: 1100px)").matches) return;
  if (!force && Date.now() - _monLastAt < 1000) return;
  _monLastAt = Date.now();
  const now = serverNow();
  const tf = state.interval;
  const html = ["BTC", "ETH"].map((a) => {
    const m = analyzeCoin(a, tf, now);
    const tick = state.ticker[a] || {};
    const px = m ? m.C : (tick.last || 0);
    const chg = (tick.chg != null && isFinite(tick.chg)) ? +tick.chg : null;
    const sig = m && m.sig;
    const liveMode = m ? m.liveMode : null;     // mode live dari analyzeCoin (scope renderDual tidak punya liveSig)
    const graded = !!(sig && sig.verdict !== "flat");
    const dir = graded ? sig.verdict : "flat";
    const dirWord = dir === "up" ? "UP" : dir === "down" ? "DOWN" : "—";
    const grec = graded
      ? `Recommendation: ${dirWord}${sig.grade ? ` · ${sig.grade}${sig.expectedWR != null ? " " + (sig.expectedWR * 100).toFixed(0) + "%" : ""}` : ""}${sig.minuteIn ? ` · min ${sig.minuteIn}` : ""}`
      : (sig && sig.mode ? `No entry · ${sig.mode}` : "Menunggu…");
    const plan = m && m.plan;
    const hl = m && m.health ? m.health.label : "—";
    const hlCls = m && m.health ? healthClass(m.health.label) : "";
    const liq = sig && sig.liqRatio != null ? (sig.liqRatio * 100).toFixed(0) + "%" : "—";
    const rw = plan && plan.levels ? plan.levels.rNow.toFixed(2) + "%" : "—";
    const ofiTxt = sig && sig.ofi != null ? ` · OFI <b>${(sig.ofi * 100).toFixed(0)}%</b>` : "";
    const l1Txt = plan && plan.levels ? ` · L1 <b>${fmtPrice(plan.levels.l1)}</b>` : "";
    return `<div class="mon-card${a === state.asset ? " active" : ""}" data-mon="${a}">
      <div class="mon-head"><span class="mon-coin">${a}</span><span class="mon-price">${fmtPrice(px)}</span>${chg != null ? `<span class="mon-chg ${chg >= 0 ? "up" : "down"}">${chg >= 0 ? "+" : ""}${chg.toFixed(2)}%</span>` : ""}<span class="mon-sig-badge ${hlCls}">${hl}</span></div>
      <div class="mon-rec ${dir}">${grec}</div>
      <div class="mon-act ${plan ? plan.cls : "wait"}">${plan ? plan.action : "—"}</div>
      <div class="mon-meta">reward <b>${rw}</b> · liq <b>${liq}</b>${ofiTxt}${l1Txt}</div>
    </div>`;
  }).join("");
  if (html !== _monLastHtml) {          // avoid re-rendering (and losing the click target) every second
    _monLastHtml = html;
    el.innerHTML = html;
    el.querySelectorAll("[data-mon]").forEach((c) => { c.onclick = () => switchAsset(c.dataset.mon); });
  }
}

/* ===== Wide-screen DUAL DETAIL: the full per-coin detail for BOTH coins, side by side.
   Everything below the controls (chart, recommendation, trade assistant, levels, volume,
   orderbook, reason) is rendered once per coin so both can be monitored at the same time. */
let dualCharts = null, _dualLastAt = 0;
function candlesFor(asset) {
  const c = state.cache[asset] && state.cache[asset][state.chartInterval];
  return c ? c.candles : [];
}
/* ===== ALASAN RINGKAS (dipakai BERSAMA kartu mobile & kolom dual desktop) =====
   Diletakkan di scope global supaya updateSignal() (mobile) dan renderDual() (desktop)
   memakai sumber kata yang sama -> tidak ada dua versi alasan yang bisa berbeda. */
/* Label pendek per mode (kartu mobile hanya butuh inti "kenapa"). */
const MODE_SHORT = {
  "HIST-PREDICT": "historis 50 sesi",
  "TREND": "tren awal sesi",
  "REVERSAL↑": "reversal naik",
  "REVERSAL↓": "reversal turun",
  "MOMENTUM": "momentum",
  "CLOSE": "menjelang settlement",
  "CONT": "kontinuasi",
  "WEAK-TREND": "tren lemah",
  "FILTERED": "tidak selaras",
  "FILTERED-REVERSAL": "reversal belum konfirmasi",
  "BLOCKED-GOAL": "kontinuasi diblokir",
  "WARMUP": "warmup",
  "LOWVOL": "volume tipis",
  "MENUNGGU": "menunggu candle",
  "NO-SIGNAL": "tanpa sinyal",
};

/* Versi RINGKAS dari generateEntryReason(): satu baris, hanya angka yang benar-benar
   dipakai untuk menilai sesi (vol = angka gate, RSI, OFI). Teks lengkap tetap dibuat
   terpisah dan dipasang sebagai tooltip, jadi tidak ada informasi yang hilang. */
function shortReason(o) {
  const dirCls = o.verdict === "up" ? "up" : o.verdict === "down" ? "down" : "flat";
  const sep = `<span class="rz-sep">·</span>`;
  const bits = [];
  // Maksimal 3 potongan supaya pasti muat 1 baris: mode (inti "kenapa") + vol + RSI.
  // OFI & sisanya tetap tersedia lengkap di tooltip.
  bits.push(`<b>${esc(MODE_SHORT[o.mode] || o.mode)}</b>`);
  if (o.vol5m != null) {
    const v = o.vol5m >= 10 ? "≥10×" : o.vol5m.toFixed(2) + "×";
    const minTxt = o.mode === "LOWVOL" && o.volNeed != null ? `<i>(min ${o.volNeed}×)</i>` : "";
    bits.push(`vol <b class="vol-val ${volClass(o.vol5m)}">${v}</b>${minTxt}`);
  }
  if (o.rsi != null) bits.push(`RSI <b>${o.rsi.toFixed(0)}</b>`);
  return `<span class="rz-dot ${dirCls}"></span><span class="rz-txt">${bits.join(" " + sep + " ")}</span>`;
}

/* Versi PENDEK dari teks aksi Trade Assistant: ambil klausa pertama saja
   (mis. "TUNGGU PEAK — harga contra 0.25%" -> "TUNGGU PEAK"). Dipakai kartu mobile
   DAN kolom dual desktop supaya keduanya menampilkan teks aksi yang identik. */
function shortAction(s) {
  const t = String(s || "").trim();
  if (t.includes(" — ")) return t.split(" — ")[0].trim();
  if (t.includes(":") && t.length > 45) return t.split(":")[0].trim();
  if (t.includes(" · ")) return t.split(" · ")[0].trim();
  return t;
}

function buildDual() {
  const el = document.getElementById("dual");
  if (!el || dualCharts) return;
  /* INFORMATION ARCHITECTURE kartu dual = SALINAN urutan kartu mobile yang sudah nyaman:
       1. HEAD      identitas + harga + konteks sesi (Δ dari LOCK, Δ$, 24h, sisa waktu)
       2. CHART     konteks visual (mobile pun menaruh chart sebelum sinyal)
       3. SINYAL    "apa" (rekomendasi) + "kenapa" (alasan) menempel jadi satu blok
       4. TA        aksi (hero) + 2 harga kunci + status; sisanya di balik "Detail"
       5. PASAR     likuiditas & volume (ditonjolkan) + orderbook bar
       6. METRIK    sesi, indikator & order flow sebagai pasangan label:nilai
     Setiap blok dipisah garis + jarak konsisten supaya mata bisa memindai per blok. */
  el.innerHTML = ["BTC", "ETH"].map((a) => `
    <div class="dual-col" id="dc-${a}-col">
      <div class="dc-head">
        <span class="dc-coin">${a}</span>
        <span class="dc-price" id="dc-${a}-price">—</span>
        <span class="dc-chg" id="dc-${a}-chg"></span>
        <span class="dc-dusd" id="dc-${a}-dusd"></span>
        <span class="dc-chg24" id="dc-${a}-chg24"></span>
        <span class="dc-cd" id="dc-${a}-cd" title="Sisa waktu sesi">--:--</span>
      </div>

      <div class="dc-chart" id="dc-${a}-chart"></div>

      <div class="dc-sec dc-signal">
        <div class="dc-recrow"><span class="dc-rec" id="dc-${a}-rec">—</span><span class="rec-status" id="dc-${a}-badge"></span></div>
        <div class="rz-line" id="dc-${a}-reason" title=""><span class="rz-dot flat"></span><span class="rz-txt">—</span></div>
        <!-- LED bar confidence: HANYA bar + % (tanpa label), arahnya SELALU searah signal -->
        <div class="dc-led" id="dc-${a}-led">
          <div class="conf-track dc-led-track" id="dc-${a}-led-track"></div>
          <span class="dc-led-val na" id="dc-${a}-led-val">—</span>
        </div>
      </div>

      <div class="dc-sec dc-ta">
        <div class="dc-sec-label">TRADE ASSISTANT</div>
        <!-- 2 kolom: kiri = info sinyal (aksi + harga kunci) · kanan = status ENTRY & EARLY CLOSE
             (ditumpuk vertikal). Menghemat 1 baris tinggi kartu tanpa memindahkan informasi. -->
        <div class="dc-ta-row">
          <div class="dc-ta-main">
            <div class="dc-act" id="dc-${a}-act">—</div>
            <div class="tp-key" id="dc-${a}-key"></div>
          </div>
          <div class="tp-status dc-ta-st">
            <span class="tp-st wait" id="dc-${a}-st-entry">ENTRY: <b>WAITING…</b></span>
            <span class="tp-st wait" id="dc-${a}-st-close">EARLY CLOSE: <b>WAITING…</b></span>
          </div>
        </div>
        <details class="tp-detail">
          <summary>Detail</summary>
          <div class="tp-full" id="dc-${a}-full"></div>
          <div class="tp-bias" id="dc-${a}-bias"></div>
          <div class="tp-levels" id="dc-${a}-levels"></div>
          <div class="tp-meta" id="dc-${a}-meta"></div>
        </details>
      </div>

      <!-- BAGIAN BAWAH: 2 kolom di dalam kartu supaya tinggi kartu turun (panel
           DESKTOP SIGNAL ACCURACY jadi terlihat tanpa scroll).
           Kiri = pasar (likuiditas/volume + orderbook) · Kanan = metrik & order flow. -->
      <div class="dc-bottom">
      <div class="dc-sec dc-mkt">
        <div class="dc-volrow">
          <span title="Rasio likuiditas: proyeksi volume sesi dibanding volume 5m typical (>1 = lebih ramai)">LIQUIDITY <b class="vol-val v-na" id="dc-${a}-liq">—</b></span>
          <span title="Pace volume 5m terhadap rata-rata (angka yang dinilai gate)">VOL <b class="vol-val v-na" id="dc-${a}-vol">—</b></span>
        </div>
        <div class="ob-bg"><div class="ob-ask" id="dc-${a}-ask"></div><div class="ob-bid" id="dc-${a}-bid"></div></div>
        <div class="ob-label"><span class="ob-sell-pct" id="dc-${a}-askp">—</span><span class="ob-buy-pct" id="dc-${a}-bidp">—</span></div>
      </div>

      <div class="dc-sec dc-metrics-sec">
        <!-- Judul + toggle "Lihat semua" dalam SATU baris (hemat satu baris tinggi kartu).
             4 metrik terpenting selalu tampil; sisanya di balik tombol. -->
        <div class="dc-metrics-head">
          <span class="dc-sec-label">METRIK &amp; ORDER FLOW</span>
          <button type="button" class="dc-more-btn" id="dc-${a}-more-btn" aria-expanded="false"
                  title="Tampilkan metrik pendukung: RSI, JARAK LOCK, LOCK, PREDIKSI, CONF, REWARD">LIHAT SEMUA ▾</button>
        </div>
        <div class="dc-metrics dc-metrics-main" id="dc-${a}-grid"></div>
        <div class="dc-metrics" id="dc-${a}-rows" hidden></div>
        <div class="dc-metrics" id="dc-${a}-pred" hidden></div>
      </div>
      </div>
    </div>`).join("");
  dualCharts = {};
  for (const a of ["BTC", "ETH"]) {
    dualCharts[a] = new CanvasChart(document.getElementById(`dc-${a}-chart`));
    dualCharts[a].setType(state.type);
    dualCharts[a].fit();
    // Toggle "LIHAT SEMUA": pakai <button> (bukan <details>) supaya bisa sebaris dengan
    // judul blok. Yang disembunyikan hanya metrik pendukung — 4 metrik inti tetap terlihat.
    const btn = document.getElementById(`dc-${a}-more-btn`);
    const bodies = [`dc-${a}-rows`, `dc-${a}-pred`].map((id) => document.getElementById(id)).filter(Boolean);
    if (btn && bodies.length) {
      btn.addEventListener("click", () => {
        const open = btn.getAttribute("aria-expanded") === "true";
        btn.setAttribute("aria-expanded", open ? "false" : "true");
        btn.textContent = open ? "LIHAT SEMUA ▾" : "SEMBUNYIKAN ▴";
        bodies.forEach((b) => { b.hidden = open; });
      });
    }
  }
}
/* Arah POSISI Trade Assistant = ARAH REKOMENDASI (main signal = SATU-SATUNYA sumber kebenaran).
   Aturan main (goals):
     ENTRY = CONTRA-LOCK, yaitu menunggu harga berada di sisi BERLAWANAN dari LOCK:
               rekomendasi UP   -> tunggu harga DI BAWAH LOCK -> BELI  (long)
               rekomendasi DOWN -> tunggu harga DI ATAS  LOCK -> JUAL  (short)
     CLOSE = harga sudah SEARAH rekomendasi dan sudah MELEWATI LOCK:
               rekomendasi UP   -> harga DI ATAS LOCK  -> tutup
               rekomendasi DOWN -> harga DI BAWAH LOCK -> tutup
   Di computeTradePlan(): favor>0 berarti harga sudah di sisi menguntungkan posisi
   (= searah rekomendasi, sudah melewati LOCK) -> cabang WAIT (belum entry) / CLOSE (tutup);
   favor<0 berarti masih contra-lock -> zona entry. */
function tradeDirOf(verdict) {
  return (verdict === "up" || verdict === "down") ? verdict : null;
}

// Selisih harga dari LOCK dalam dolar, mis. -$4.23 / $0.00 / +$1.05
// (desimal menyesuaikan besar nilai: ratusan -> 0 desimal, puluhan -> 1, sisanya -> 2)
function fmtUsdDelta(d) {
  if (d == null || !isFinite(d)) return "";
  const a = Math.abs(d);
  const dec = a >= 100 ? 0 : a >= 10 ? 1 : 2;
  const sign = d > 0 ? "+" : d < 0 ? "-" : "";
  return `${sign}$${a.toFixed(dec)}`;
}

/* Efek VISUAL pada kartu coin ketika ada notifikasi suara (khusus layar lebar/dual):
   supaya terlihat notifikasi itu datang dari coin yang mana.
     kind = "signal" (ada sinyal masuk) | "entry" (Trade Assistant: entry/average) | "exit" (close/cut)
   Kartu berkedip + glow sebentar (~2.6s), lalu kelasnya dilepas. Di mobile langsung keluar
   (tidak ada perubahan perilaku). */
const _FLASH_CLASSES = ["flash-signal", "flash-entry", "flash-exit"];
function flashCard(asset, kind) {
  if (typeof window.matchMedia === "function" && !window.matchMedia("(min-width: 1100px)").matches) return;
  const el = document.getElementById(`dc-${asset}-col`);
  if (!el) return;
  // EKSKLUSIF: hanya kartu yang memicu notifikasi yang menyala. Kartu lain dimatikan dulu
  // supaya tidak ada dua kartu ber-glow bersamaan (permintaan user).
  for (const other of document.querySelectorAll(".dual-col")) {
    if (other === el) continue;
    other.classList.remove(..._FLASH_CLASSES);
    if (other._flashT) { clearTimeout(other._flashT); other._flashT = null; }
  }
  const cls = "flash-" + kind;
  el.classList.remove(..._FLASH_CLASSES);
  void el.offsetWidth;                 // paksa restart animasi
  el.classList.add(cls);
  if (el._flashT) clearTimeout(el._flashT);
  el._flashT = setTimeout(() => el.classList.remove(cls), 1800);   // lebih singkat: tidak tumpang tindih
  console.log(`[WIDE][FLASH] ${asset} ${kind}`);
}

// jam lokal untuk status ENTRY / EARLY CLOSE (dipakai panel TA dan kolom dual)
function fmtClock(t) { return t ? new Date(t).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }) : ""; }
// versi pendek (tanpa detik) untuk chip — supaya ENTRY & EARLY CLOSE muat 1 baris di mobile
function fmtClockShort(t) { return t ? new Date(t).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }) : ""; }

/* ===== OVERLAY CHART (proyeksi ke settlement + garis tren reversal-aware + marker puncak) =====
   SATU sumber untuk chart utama (mobile) DAN chart kolom dual (layar lebar). Sebelumnya kolom
   dual tidak menerima proyeksi/tren/marker sehingga tampilannya tidak selengkap mobile. */
function buildOverlay(win, o) {
  const U = CHART_UTILS;
  if (!U || !win || win.length < 2) return null;
  const { linreg, detectSwings } = U;
  const C = o.C, O = o.O, std = o.std || 1;
  const nowSec = o.nowSec, closeSec = o.closeSec;
  const remSec = Math.max(0, (o.remainingMs || 0) / 1000);
  const projectedClose = C + U.clamp((o.slope || 0) * remSec, -std * 3, std * 3);
  const contDir = projectedClose > O ? "up" : projectedClose < O ? "down" : "flat";
  const projection = [{ time: nowSec, value: C }, { time: closeSec, value: projectedClose }];
  const sw = detectSwings(win, o.swingLookback || 4);
  const lastH = sw.highs[sw.highs.length - 1], lastL = sw.lows[sw.lows.length - 1];
  let peak = null;
  if (lastH && lastL) peak = (lastH.time >= lastL.time) ? { price: lastH.price, dir: "top", time: lastH.time } : { price: lastL.price, dir: "bot", time: lastL.time };
  else if (lastH) peak = { price: lastH.price, dir: "top", time: lastH.time };
  else if (lastL) peak = { price: lastL.price, dir: "bot", time: lastL.time };
  let trendFit = [];
  if (peak) {
    const idx = win.findIndex((c) => c.time === peak.time);
    if (idx >= 0) {
      const seg1 = win.slice(0, idx + 1), seg2 = win.slice(idx);
      const r1 = linreg(seg1), r2 = linreg(seg2);
      const pts = [];
      if (r1 && seg1.length) pts.push({ time: seg1[0].time, value: r1.a + r1.b * seg1[0].time });
      pts.push({ time: peak.time, value: peak.price });
      if (r2 && seg2.length) pts.push({ time: nowSec, value: C });
      if (pts.length >= 2) trendFit = pts;
    }
  }
  if (!trendFit.length) {
    const reg = linreg(win);
    if (reg) { const a = win[0], b = win[win.length - 1]; trendFit = [{ time: a.time, value: reg.a + reg.b * a.time }, { time: b.time, value: reg.a + reg.b * b.time }]; }
  }
  const markers = [];
  if (lastH) markers.push({ time: lastH.time, value: lastH.price, color: "#f6465d", text: "▲P" });
  if (lastL) markers.push({ time: lastL.time, value: lastL.price, color: "#0ecb81", below: true, text: "▼P" });
  markers.push({
    time: closeSec, value: projectedClose,
    color: contDir === "up" ? "#0ecb81" : contDir === "down" ? "#f6465d" : "#848e9c",
    text: (contDir === "up" ? "AKHIR ↑ " : contDir === "down" ? "AKHIR ↓ " : "AKHIR ") + fmtPrice(projectedClose),
  });
  return { projection, trendFit, markers, peak, projectedClose, contDir };
}

function renderDual(force) {
  const el = document.getElementById("dual");
  if (!el) return;
  if (!force && typeof window.matchMedia === "function" && !window.matchMedia("(min-width: 1100px)").matches) return;
  if (!force && Date.now() - _dualLastAt < 250) return;   // 250ms: setara dengan coalesce render mobile
  _dualLastAt = Date.now();
  if (!dualCharts) buildDual();
  const now = serverNow();
  const tf = state.interval;
  const dur = INTERVAL_MS[tf];
  // Countdown sisa durasi sesi (satu nilai untuk kedua koin; di kanan atas kartu, sejajar
  // judul/harga/%). Hanya ditulis bila detiknya berubah supaya tidak thrash tiap 250ms.
  const sbNow = sessionBounds(dur, now);
  const remSec = Math.max(0, Math.round((sbNow.end - now) / 1000));
  const cdTxt = `${String(Math.floor(remSec / 60)).padStart(2, "0")}:${String(remSec % 60).padStart(2, "0")}`;
  for (const a of ["BTC", "ETH"]) {
    const m = analyzeCoin(a, tf, now);
    const tick = state.ticker[a] || {};
    const px = m ? m.C : (tick.last || 0);
    const chg = (tick.chg != null && isFinite(tick.chg)) ? +tick.chg : null;
    const g = (id) => document.getElementById(`dc-${a}-${id}`);
    const sig = m && m.sig;
    const liveMode = m ? m.liveMode : null;   // mode live dari analyzeCoin (liveSig hanya ada di analyzeCoin)
    const graded = !!(sig && sig.verdict !== "flat");
    const dir = graded ? sig.verdict : "flat";
    const plan = m && m.plan;
    // OFI untuk tampilan (metrik LIVE: pakai angka server dulu, else flow lokal). Dihitung di
    // AWAL loop karena dipakai dua tempat: baris "Detail" Trade Assistant DAN chip metrik.
    const t0SecR = Math.floor(Math.floor(now / dur) * dur / 1000);
    const ofiVal = ofiForDisplay(a, t0SecR, Math.floor(now / 1000));

    // ===== SOUND per-koin untuk tampilan DESKTOP (dual) =====
    // Mobile: hanya koin aktif yang berbunyi (jalur utama). Di layar lebar KEDUA koin harus
    // berbunyi. Koin non-aktif ditangani di sini; koin aktif tetap oleh jalur utama supaya
    // tidak dobel. Trade Assistant memakai _tradeLastState bersama -> transisi sama = 1 suara.
    if (m && m.key) {
      // PRIMING: sama seperti jalur utama — sinyal yang sudah ada sebelum halaman dibuka tidak
      // dibunyikan (kartu sudah menampilkannya), hanya sinyal yang muncul selagi user ada.
      const isOtherCoin = state.asset !== a;
      const dualFirstSight = isOtherCoin && !_sigFirstDual.has(m.key);
      if (isOtherCoin) _sigFirstDual.add(m.key);      // hanya koin NON-aktif yang memakai priming dual
      if (isOtherCoin && graded) {
        if (!_wideSigSounded.has(m.key)) {
          _wideSigSounded.add(m.key);
          if (dualFirstSight) {
            // sinyal sudah ada saat halaman dibuka -> tandai sudah dibunyikan, tanpa bunyi
          } else {
            playSoundAlert();
            flashCard(a, "signal");
            flashTitle(`▶ SIGNAL ${dir.toUpperCase()} ${a}/${tf}`);
            console.log(`[WIDE][SOUND-SIGNAL] ${a}/${tf} ${dir}${sig && sig.grade ? " " + sig.grade : ""}`);
          }
        }
      } else if (isOtherCoin && !graded) {
        _wideSigSounded.delete(m.key);      // flat / sesi baru -> siap berbunyi lagi
      }
      // Trade Assistant: entry / average / close (semua koin, termasuk koin aktif — dedupe via state bersama)
      if (plan) {
        const prevW = _tradeLastState[m.key];
        const prevWSrc = _tradeLastSrc[m.key];
        const srcNow = m.planSrc || "local";
        _tradeLastSrc[m.key] = srcNow;
        // Sumber plan harus sama (lihat catatan di jalur utama) supaya pergantian lokal->server
        // setelah load tidak dianggap transisi -> tidak ada notif TA palsu.
        if (prevW !== undefined && prevWSrc === srcNow && plan.state !== prevW) {
          if (plan.state === "ENTRY" || plan.state === "AVERAGE") {
            const sk = `${m.key}|${plan.state}`;
            if (!_entrySounded.has(sk)) { _entrySounded.add(sk); playTradeEntrySound(); }
            flashCard(a, "entry");
            flashTitle(`▶ ${plan.action} ${a}/${tf}`);
            console.log(`[WIDE][SOUND-ENTRY] ${a}/${tf} ${plan.state}: ${plan.action}`);
          } else if (plan.state === "CLOSE" || plan.state === "STAND_DOWN") {
            if (!_closeSounded.has(m.key)) {
              _closeSounded.add(m.key);
              playCloseSound();
              console.log(`[WIDE][SOUND-CLOSE] ${a}/${tf} ${plan.state}: ${plan.action}`);
            }
            flashCard(a, "exit");
            flashTitle(`■ ${plan.action} ${a}/${tf}`);
          }
        }
        _tradeLastState[m.key] = plan.state;
      }
    }

    const pEl = g("price"); if (pEl) pEl.textContent = fmtPrice(px);
    // VOLUME & LIQUIDITY (ditonjolkan) — sumber sama dgn sinyal: server bila segar, else cache lokal
    {
      // m.sig = hasil analyzeCoin (sudah memakai sinyal SERVER bila segar, else cache lokal)
      const srcV = (m && m.sig) || null;
      const vEl = g("vol"), lEl = g("liq");
      const volVal = srcV ? (srcV.volRel2 != null ? srcV.volRel2 : (srcV.volRel != null ? srcV.volRel : null)) : null;
      if (vEl) { vEl.textContent = volVal != null ? volVal.toFixed(2) + "\u00d7" : "\u2014"; vEl.className = "vol-val " + volClass(volVal); }
      const liqVal = srcV && srcV.liqRatio != null ? srcV.liqRatio : null;
      if (lEl) {
        lEl.textContent = liqVal != null ? liqVal.toFixed(2) + "\u00d7" + (srcV && srcV.liqLow ? " LOW" : "") : "\u2014";
        lEl.className = "vol-val " + volClass(liqVal);
      }
    }
    const cdEl = g("cd");
    if (cdEl) {
      if (cdEl.textContent !== cdTxt) cdEl.textContent = cdTxt;
      const cls = "dc-cd" + (remSec <= 60 ? " warn" : "");
      if (cdEl.className !== cls) cdEl.className = cls;
      cdEl.title = `Sisa waktu sesi ${tf}: ${cdTxt}`;
    }
    // dc-chg = selisih harga dari LOCK (%), dc-dusd = selisih yang sama dalam $ (permintaan user),
    // dc-chg24 = perubahan 24 jam dari ticker (dibedakan agar tidak tertukar).
    const cEl = g("chg");
    const lockDeltaUsd = (m && m.O) ? (px - m.O) : null;
    const lockDeltaPct = (m && m.O) ? ((px - m.O) / m.O) * 100 : null;
    if (cEl) {
      cEl.textContent = lockDeltaPct != null ? `${lockDeltaPct >= 0 ? "+" : ""}${lockDeltaPct.toFixed(3)}%` : "";
      cEl.className = "dc-chg " + (lockDeltaPct != null ? (lockDeltaPct >= 0 ? "up" : "down") : "");
      cEl.title = lockDeltaPct != null
        ? `Selisih harga dari LOCK (open sesi): ${fmtUsdDelta(lockDeltaUsd)} (${lockDeltaPct >= 0 ? "+" : ""}${lockDeltaPct.toFixed(3)}%)`
        : "LOCK belum tersedia";
    }
    const dEl = g("dusd");
    if (dEl) {
      dEl.textContent = lockDeltaUsd != null ? fmtUsdDelta(lockDeltaUsd) : "";
      dEl.className = "dc-dusd " + (lockDeltaUsd != null ? (lockDeltaUsd > 0 ? "up" : lockDeltaUsd < 0 ? "down" : "flat") : "");
      dEl.title = lockDeltaUsd != null
        ? `Selisih harga dari LOCK dalam dolar: ${fmtUsdDelta(lockDeltaUsd)} · LOCK ${fmtPrice(m.O)} → sekarang ${fmtPrice(px)}`
        : "LOCK belum tersedia";
    }
    const hEl = g("chg24");
    if (hEl) hEl.textContent = chg != null ? `24h ${chg >= 0 ? "+" : ""}${chg.toFixed(2)}%` : "";
    const rEl = g("rec");
    if (rEl) {
      rEl.textContent = graded
        ? `Recommendation: ${dir.toUpperCase()}${sig.grade ? ` · ${sig.grade}${sig.expectedWR != null ? " " + (sig.expectedWR * 100).toFixed(0) + "%" : ""}` : ""}${sig.minuteIn ? ` · min ${sig.minuteIn}` : ""}`
        : (liveMode ? `No entry · ${liveMode}` : "Menunggu…");
      rEl.className = "dc-rec " + (graded ? dir : "flat");
    }
    const bEl = g("badge");
    if (bEl) { const hl = m && m.health ? m.health.label : ""; bEl.textContent = hl; bEl.className = "rec-status " + (m && m.health ? healthClass(m.health.label) : ""); }
    const aEl = g("act");
    if (aEl) {
      aEl.textContent = plan ? shortAction(plan.action) : "—";
      aEl.className = "dc-act " + (plan ? plan.cls : "wait");
      aEl.title = plan ? plan.action : "";
    }
    // Baris 2 harga kunci (sama seperti kartu mobile): ENTRY/TARGET atau POSISI/TARGET
    const keyEl = g("key");
    if (keyEl) {
      const lv = plan && plan.levels ? plan.levels : null;
      if (!lv) { keyEl.textContent = ""; keyEl.className = "tp-key"; }
      else {
        const inPos = plan.entryPrice != null;
        const rw = lv.rNow != null ? ` <i>(+${lv.rNow.toFixed(2)}% ke target)</i>` : "";
        let pnl = "";
        if (inPos && plan.tradeDir) {
          const v = ((plan.tradeDir === "up" ? (px - plan.entryPrice) : (plan.entryPrice - px)) / plan.entryPrice) * 100;
          if (isFinite(v)) pnl = ` <i>(${v >= 0 ? "+" : ""}${v.toFixed(2)}%)</i>`;
        }
        keyEl.className = "tp-key " + (plan.cls || "wait");
        keyEl.innerHTML = inPos
          ? `<span>POSISI <b>@${fmtPrice(plan.entryPrice)}</b>${pnl}</span><span>TARGET <b>${fmtPrice(lv.target)}</b>${rw}</span>`
          : `<span>ENTRY <b>${fmtPrice(lv.l1)}</b></span><span>TARGET <b>${fmtPrice(lv.target)}</b>${rw}</span>`;
        keyEl.title = inPos
          ? `Posisi aktif @${fmtPrice(plan.entryPrice)} · target = LOCK (harga open sesi) ${fmtPrice(lv.target)}`
          : `Entry contra-lock di sekitar ${fmtPrice(lv.l1)} · target = LOCK (harga open sesi) ${fmtPrice(lv.target)}`;
      }
    }
    // Isi "Detail": teks aksi penuh, penjelasan arah, level ladder, dan metrik teknis
    const fullEl = g("full"); if (fullEl) fullEl.textContent = plan ? plan.action : "";
    const biasEl = g("bias");
    if (biasEl) {
      if (!plan) { biasEl.textContent = ""; biasEl.className = "tp-bias"; }
      else {
        const isUpSig = dir === "up";
        biasEl.className = "tp-bias " + dir;
        biasEl.innerHTML = `SINYAL <b>${esc(String(dir).toUpperCase())}</b> · entry <b>contra-lock</b>: tunggu harga <b>${isUpSig ? "DI BAWAH" : "DI ATAS"} LOCK</b> (${isUpSig ? "beli" : "jual"}) · close setelah melewati LOCK`;
      }
    }
    const metaEl = g("meta");
    if (metaEl) {
      if (!plan) metaEl.textContent = "";
      else {
        const ofiTxt = ofiVal != null ? `OFI ${(ofiVal * 100).toFixed(0)}%` : "OFI —";
        const adv = plan.adverseStd != null ? `${plan.adverseStd >= 0 ? "-" : "+"}${Math.abs(plan.adverseStd).toFixed(2)}σ` : "—";
        const rwNow = plan.levels && plan.levels.rNow != null ? ` · reward +${plan.levels.rNow.toFixed(2)}%` : "";
        const pos = plan.entryPrice != null ? ` · posisi @${fmtPrice(plan.entryPrice)}` : " · belum ada posisi";
        metaEl.textContent = `Momentum ${plan.fs} · jarak lock ${adv}${rwNow} · ${ofiTxt}${pos}${plan.why && plan.why.length ? " · " + plan.why.join(", ") : ""}`;
      }
    }
    // STATUS ENTRY / EARLY CLOSE per koin (sumber sama: state Trade Assistant)
    const seEl = g("st-entry"), scEl = g("st-close");
    const se = plan && plan.statusEntry, sc = plan && plan.statusClose;
    if (seEl) {
      const ok = !!(se && se.ok);
      seEl.className = "tp-st" + (ok ? " ok" : " wait");
      seEl.innerHTML = `ENTRY: <b>${ok ? "SUCCESS" : "WAITING…"}</b>`;
      seEl.title = ok ? `ENTRY SUCCESS · ${fmtClock(se.at)}${se.price != null ? " @ " + fmtPrice(se.price) : ""}` : "Menunggu entry";
    }
    if (scEl) {
      const ok = !!(sc && sc.ok);
      scEl.className = "tp-st" + (ok ? " ok" : " wait");
      scEl.innerHTML = `EARLY CLOSE: <b>${ok ? "SUCCESS" : "WAITING…"}</b>`;
      scEl.title = ok ? `EARLY CLOSE SUCCESS · ${fmtClock(sc.at)}` : "Menunggu early close";
    }
    const lvEl = g("levels");
    if (lvEl) lvEl.innerHTML = (plan && plan.levels)
      ? `<span>ENTRY L1 <b>${fmtPrice(plan.levels.l1)}</b> <i>(+${plan.levels.r1.toFixed(2)}%)</i></span>` +
        `<span>TAMBAH L2 <b>${fmtPrice(plan.levels.l2)}</b> <i>(+${plan.levels.r2.toFixed(2)}%)</i></span>` +
        `<span>TAMBAH L3 <b>${fmtPrice(plan.levels.l3)}</b> <i>(+${plan.levels.r3.toFixed(2)}%)</i></span>` +
        `<span>TARGET (LOCK) <b>${fmtPrice(plan.levels.target)}</b></span>`
      : "";
    // Metrik dirender sebagai pasangan label:nilai (chip) supaya bisa dipindai cepat —
    // teks "LABEL nilai · LABEL nilai" sebelumnya sulit dibaca.
    const chip = (l, v, c, t) => `<span class="dc-m"${t ? ` title="${t}"` : ""}><i>${l}</i><b class="${c || ""}">${v}</b></span>`;
    // INTI (selalu tampil) — 4 informasi paling penting saja: OFI, MOMENTUM, MODE, DELTA.
    const grEl = g("grid");
    if (grEl) {
      if (!m) grEl.innerHTML = "";
      else {
        const ofi = ofiVal != null ? (ofiVal * 100).toFixed(0) + "%" : "—";
        const mom = m.slope > 0 ? "BULLISH" : m.slope < 0 ? "BEARISH" : "FLAT";
        const momCls = m.slope > 0 ? "up" : m.slope < 0 ? "down" : "";
        const delta = m.O > 0 ? (m.C - m.O) / m.O * 100 : 0;
        grEl.innerHTML = chip("OFI", ofi, ofiVal == null ? "" : (ofiVal >= 0 ? "up" : "down"),
            "Order flow (eksekusi taker) sesi berjalan: positif = tekanan BELI, negatif = tekanan JUAL") +
          chip("MOMENTUM", mom, momCls) + chip("MODE", liveMode || "—") +
          chip("DELTA", `${delta >= 0 ? "+" : ""}${delta.toFixed(3)}%`);
      }
    }
    // Detail (di balik "Lihat semua metrik") — pendukung, bukan informasi utama.
    const rwEl = g("rows");
    if (rwEl && m) {
      const dev = m.std > 0 ? (m.C - m.O) / m.std : 0;
      rwEl.innerHTML = chip("RSI", m.rsi != null ? m.rsi.toFixed(1) : "—") +
        chip("JARAK LOCK", `${dev >= 0 ? "+" : ""}${dev.toFixed(2)}σ`) + chip("LOCK", fmtPrice(m.O));
    }
    const prEl = g("pred");
    if (prEl) {
      const rw = plan && plan.levels ? plan.levels.rNow.toFixed(2) + "%" : "—";
      prEl.innerHTML = chip("PREDIKSI", graded ? dir.toUpperCase() : "—", graded ? dir : "") +
        chip("CONF", sig && sig.conf != null ? sig.conf : "—") + chip("REWARD", rw);
    }
    const ch = dualCharts[a];
    if (ch) {
      ch.setSessionDuration(dur);
      ch.setType(state.type);
      ch.setData(candlesFor(a));
      ch.setDecision(m ? m.O : null);
      ch.setPrediction(graded ? dir : null, m ? m.O : null);
      ch.setCurrentPrice(px);
      // OVERLAY LENGKAP seperti chart mobile: garis proyeksi ke settlement + garis tren
      // reversal-aware + marker puncak/akhir. (Sebelumnya kolom dual tidak menerimanya.)
      if (m && CHART_UTILS) {
        const nowMs2 = serverNow();
        const bnd = sessionBounds(INTERVAL_MS[tf], nowMs2);
        const ov2 = buildOverlay(CHART_UTILS.analysisWindowFor(a), {
          C: m.C, O: m.O, std: m.std, slope: m.slope,
          nowSec: Math.floor(nowMs2 / 1000), closeSec: Math.floor(bnd.end / 1000),
          remainingMs: bnd.end - nowMs2, swingLookback: CHART_UTILS.swingLookbackFor(a),
        });
        if (ov2) { ch.setProjection(ov2.projection); ch.setTrendFit(ov2.trendFit); ch.setMarkers(ov2.markers); }
      }
    }
    const ob = state.orderbook[a];
    const obA = g("ask"), obB = g("bid");
    if (ob && ob.bids && ob.asks && obA && obB) {
      const bidVol = ob.bids.reduce((x, l) => x + +l[0] * +l[1], 0);
      const askVol = ob.asks.reduce((x, l) => x + +l[0] * +l[1], 0);
      const tot = bidVol + askVol;
      const askPct = tot > 0 ? askVol / tot * 100 : 50;
      obA.style.width = askPct + "%";
      obB.style.width = (100 - askPct) + "%";
      const ap = g("askp"), bp = g("bidp");
      if (ap) ap.textContent = askPct.toFixed(0) + "%";
      if (bp) bp.textContent = (100 - askPct).toFixed(0) + "%";
    }
    // LED BAR CONFIDENCE — tepat di bawah informasi signal.
    // Nilai = keyakinan untuk arah SIGNAL itu sendiri (signal UP -> bar UP), memakai fungsi
    // yang SAMA dengan panel confidence mobile saat tab SIGNAL terkunci ke rekomendasi
    // (confidenceFromPastSessions: bukti 3 sesi sebelum sesi aktif). Tampilannya hanya
    // bar + persen, tanpa label lain, sesuai permintaan.
    const ledTrack = g("led-track"), ledVal = g("led-val"), ledWrap = g("led");
    if (ledTrack) {
      const confDir = graded ? dir : null;
      if (!confDir) {
        clearLedBar(ledTrack);
        if (ledVal) { ledVal.textContent = "—"; ledVal.className = "dc-led-val na"; }
        if (ledWrap) ledWrap.title = "Belum ada arah signal — LED keyakinan kosong";
      } else {
        // Angka dari SERVER (basis "past" = 3 sesi sebelumnya, sama dengan tab SIGNAL di mobile).
        // Cadangan: hitung lokal bila server belum menyediakan.
        const srvC = (() => {
          try { const e = (typeof LIVE !== "undefined") ? LIVE.entryFor(a, tf) : null; return (e && e.conf) ? e.conf : null; } catch (_) { return null; }
        })();
        const conf = (srvC && srvC[confDir]) ? srvC[confDir].past : confidenceFromPastSessions(a, tf, confDir === "down");
        paintLedBar(ledTrack, conf);
        if (ledVal) { ledVal.textContent = conf + "%"; ledVal.className = "dc-led-val " + confDir; }
        if (ledWrap) ledWrap.title = `Keyakinan arah ${confDir.toUpperCase()} (3 sesi sebelum sesi aktif) — sumber sama dengan tab SIGNAL di mobile`;
      }
    }
    // ALASAN: pakai shortReason() yang SAMA dengan kartu mobile (1 baris, nilai saja).
    // Teks lengkap tetap tersedia di tooltip supaya tidak ada informasi yang hilang.
    const rsEl = g("reason");
    if (rsEl) {
      const volFor = sig ? (sig.volRel2 != null ? sig.volRel2 : (sig.volRel != null ? sig.volRel : null)) : null;
      rsEl.innerHTML = shortReason({
        verdict: graded ? dir : "flat",
        mode: (sig && sig.mode) || liveMode || "NO-SIGNAL",
        vol5m: volFor,
        volNeed: fairMinVol(tf),
        rsi: m && m.rsi != null ? m.rsi : null,
      });
      rsEl.title = (sig && sig.reason) ? sig.reason : "—";
    }
  }
}

// Universal signal cache - untuk background calculation semua coin & interval
let _deskSigCache = {};  // key: `${sym}_${tf}_${roundStart}` -> LOCKED signal (only non-flat entries)
const _deskSigMap = {};  // key: same -> boolean (mark sudah capture)
const _deskSigLive = {}; // key: same -> latest evaluation each tick (for live status display)

let _cap_t0 = null;        // t0 ronde yang sedang di-capture
let _cap_pending = null;   // prediksi entry: { t0, asset, interval, mode, dir, conf, trend }
let _cap_lastClose = null; // C terakhir (close ronde sebelumnya saat boundary)
let _cap_lastLock = null;  // O terakhir (lock ronde sebelumnya)
let _cap_asset = null;     // combo guard: asset of the round being captured
let _cap_interval = null;  // combo guard: interval of the round being captured

// Desktop signal lock - signal hanya dihitung saat sesi dimulai, kemudian lock
let _deskSig = null;       // { roundStart, asset, interval, verdict, mode, reason, conf, timestamp }
let _mob_t0 = null;        // t0 ronde mobile pred yang sedang di-capture

function captureDesktopSignal() {
  const now = serverNow();
  
  for (const sym of ["BTC", "ETH"]) {
    for (const tf of INTERVALS) {
      const dur = INTERVAL_MS[tf];
      const t0 = Math.floor(now / dur) * dur;
      const cacheKey = `${sym}_${tf}_${t0}`;
      
      if (_deskSigMap[cacheKey]) continue;

      // ===== SUMBER KEBENARAN = SAMA DENGAN YANG DITAMPILKAN =====
      // Panel DESKTOP SIGNAL ACCURACY menilai entri ini. Dulu entri selalu memakai verdict
      // LOKAL, sedangkan rekomendasi yang dilihat user kini memakai sinyal SERVER -> bila
      // keduanya berbeda (mis. device dibuka di tengah sesi), log tercatat berlawanan dengan
      // sinyal yang menang (user melihat "D dan benar" tapi tercatat "U/salah").
      const srv = (typeof LIVE !== "undefined") ? LIVE.signalFor(sym, tf) : null;
      const cached = _deskSigCache[cacheKey];
      const shown = srv
        ? { verdict: srv.verdict, mode: srv.mode, conf: srv.conf, grade: srv.grade, rsi: srv.rsi,
            histStrength: srv.histStrength, gateKey: srv.gateKey, highConf: !!srv.accepted, ofi: srv.ofi,
            gateWr: null, reason: cached ? cached.reason : "", lock: srv.lock }
        : (cached ? { verdict: cached.verdict, mode: cached.mode, conf: cached.conf, grade: cached.grade,
            rsi: cached.rsi, histStrength: cached.histStrength, gateKey: cached.gateKey,
            highConf: !!cached.highConf, gateWr: cached.gateWr, reason: cached.reason || "", lock: null } : null);
      if (shown && shown.verdict !== "flat") {
        _deskSigMap[cacheKey] = true;
        const gkey = shown.gateKey || gateKey(tf, shown.mode, shown.verdict, shown.rsi, shown.histStrength);
        const g = shown.highConf ? { wr: shown.gateWr } : gateLookup(gkey);
        // Lock = the session open price, taken from the interval candle itself (matches the chart
        // lock line). Falls back to the 5s series only if that candle is not available.
        const t0Sec = Math.floor(t0 / 1000);
        const sessionCandle = (state.cache[sym]?.[tf]?.candles || []).find((c) => c.time === t0Sec);
        const lock = shown.lock || (sessionCandle ? sessionCandle.open : sessionLock(sym, dur, now));
        const entry = {
          ts: Date.now(),
          t0: t0,
          asset: sym,
          interval: tf,
          mode: shown.mode,
          dir: shown.verdict,          // = arah yang DITAMPILKAN (server bila segar)
          conf: shown.conf,
          lock: lock,
          reason: shown.reason || "",
          gateKey: gkey,
          highConf: !!g,
          gateWr: g ? g.wr : null,
        };
        // Hold in PendingSig until the round ends (avoids early + duplicate history entries).
        const alreadyFinal = _loggedKeys.has(cacheKey);
        if (!alreadyFinal) {
          PendingSig.add(entry);
          // PHASE 2: simpan vektor fitur LENGKAP saat sinyal muncul (dipakai learner).
          // Dilewati bila sinyal untuk tf ini sudah diproduksi server (engine 5m/15m) supaya
          // tidak ada record ganda; 1h tetap direkam klien karena server tidak menyajikannya.
          const srvHas = ["5m", "15m"].includes(tf) && !!(typeof LIVE !== "undefined" && LIVE.fresh() && LIVE.tf === tf);
          if (srvHas) { /* sinyal tf ini diproduksi server -> klien tidak merekam (hindari duplikat) */ }
          else try {
            const L = cached.learn || null;
            LEDGER.addSignal({
              asset: sym, interval: tf, t0: t0Sec,
              // capOffsetMs = umur sesi saat capture. Dipakai server untuk memilih snapshot
              // KANONIK per sesi: yang paling dekat ke detik ke-2 (paling awal) yang menang,
              // bukan yang kebetulan ter-upload lebih dulu. Browser yang dibuka di tengah sesi
              // akan menghasilkan capOffsetMs besar -> disimpan sebagai alternatif, bukan kanonik.
              capOffsetMs: Math.max(0, Math.round(now - t0)),
              capAt: Math.floor(now / 1000),
              dir: cached.verdict, mode: cached.mode, conf: cached.conf, lock: lock,
              grade: cached.grade || null,
              expectedWR: cached.expectedWR != null ? +Number(cached.expectedWR).toFixed(4) : null,
              volRel: cached.volRel != null ? +Number(cached.volRel).toFixed(4) : null,
              volRel2: cached.volRel2 != null ? +Number(cached.volRel2).toFixed(4) : null,
              surprise: cached.surprise != null ? +Number(cached.surprise).toFixed(4) : null,
              mv2: cached.mv2 != null ? +Number(cached.mv2).toFixed(5) : null,
              rsi: cached.rsi != null ? +Number(cached.rsi).toFixed(2) : null,
              histStrength: cached.histStrength != null ? cached.histStrength : null,
              minuteIn: cached.minuteIn != null ? cached.minuteIn : null,
              rewardPct: cached.rewardPct != null ? +Number(cached.rewardPct).toFixed(4) : null,
              liqRatio: cached.liqRatio != null ? +Number(cached.liqRatio).toFixed(3) : null,
              liqLow: !!cached.liqLow,
              ofi: (() => { const v = ofiForDisplay(sym, t0Sec, Math.floor(now / 1000)); return v != null ? +Number(v).toFixed(4) : null; })(),
              touchRate: cached.touch ? +Number(cached.touch.rate).toFixed(4) : null,
              gateKey: gkey, gateWr: g ? +Number(g.wr).toFixed(4) : null,
              learn: L ? { label: L.label, touch: L.touch, dirWR: L.dirWR, intervalWR: L.intervalWR ? L.intervalWR.wr : null, gap: L.ctx ? L.ctx.gap : null, hour: L.ctx ? L.ctx.hour : null, trend: L.ctx ? L.ctx.trend : null, blocking: (L.blockable || []).length > 0 } : null,
            });
          } catch (e) { console.warn("[LEDGER] addSignal failed:", e && e.message); }
          // Hitung efek learner untuk panel STATUS LEARNER (sekali per sesi per combo).
          try {
            const L2 = cached.learn;
            if (L2) {
              LEARN_STATS.signals++;
              if (L2.label === "LEMAH") LEARN_STATS.weak++;
              else if (L2.label === "CAMPURAN") LEARN_STATS.mixed++;
              else if (L2.label === "KUAT") LEARN_STATS.strong++;
              if (L2.blockable && L2.blockable.length) LEARN_STATS.wouldBlock++;
              renderLearnerStatus();
            }
          } catch (_) {}
          console.log("[DESK-SIG] pending:", entry);
        }
        
        if (state.asset === sym && state.interval === tf) {
          renderConfidenceReport();
        }
      }
    }
  }
}

// Universal signal calculator - calculate signal untuk semua coin & interval di background
function updateProjectionUniversal() {
  const now = serverNow();
  
  // Score finished rounds (all combos) before generating new signals
  evaluateUniversalSessions(now);
  
  // Cleanup old cache entries (>3 hours old) untuk prevent memory leak
  const CUTOFF = now - 3 * 3600000;
  for (const k in _deskSigCache) {
    const t = parseInt(k.split("_")[2]);
    if (!isNaN(t) && t < CUTOFF) {
      delete _deskSigCache[k];
      delete _deskSigMap[k];
    }
  }
  // _deskSigLive is populated for EVERY session (including flat), so it must be
  // pruned independently — otherwise it grows without bound.
  for (const k in _deskSigLive) {
    const t = parseInt(k.split("_")[2]);
    if (!isNaN(t) && t < CUTOFF) { delete _deskSigLive[k]; delete _lockStatusSince[k]; }
  }
  for (const k of _alertedKeys) {
    const t = parseInt(k.split("_")[2]);
    if (!isNaN(t) && t < CUTOFF) _alertedKeys.delete(k);
  }
  for (const k in _tradePeak) {
    const t = parseInt(k.split("_")[2]);
    if (!isNaN(t) && t < CUTOFF) delete _tradePeak[k];
  }
  for (const k in _tradeLastState) {
    const t = parseInt(k.split("_")[2]);
    if (!isNaN(t) && t < CUTOFF) delete _tradeLastState[k];
  }
  for (const k in _tradeDwell) {
    const t = parseInt(k.split("_")[2]);
    if (!isNaN(t) && t < CUTOFF) delete _tradeDwell[k];
  }
  for (const k in _tradeClosed) {
    const t = parseInt(k.split("_")[2]);
    if (!isNaN(t) && t < CUTOFF) delete _tradeClosed[k];
  }
  for (const k of _wideSigSounded) {
    const t = parseInt(k.split("_")[2]);
    if (!isNaN(t) && t < CUTOFF) _wideSigSounded.delete(k);
  }
  for (const k of _mainSigAlerted) {
    const t = parseInt(k.split("_")[2]);
    if (!isNaN(t) && t < CUTOFF) _mainSigAlerted.delete(k);
  }
  for (const k of _closeSounded) {
    const t = parseInt(k.split("_")[2]);
    if (!isNaN(t) && t < CUTOFF) _closeSounded.delete(k);
    for (const sk of _entrySounded) { const t2 = parseInt(sk.split("_")[2]); if (!isNaN(t2) && t2 < CUTOFF) _entrySounded.delete(sk); }
    for (const k2 of _sigFirstSight) { const t3 = parseInt(k2.split("_")[2]); if (!isNaN(t3) && t3 < CUTOFF) _sigFirstSight.delete(k2); }
    for (const k2 of _sigFirstDual) { const t3 = parseInt(k2.split("_")[2]); if (!isNaN(t3) && t3 < CUTOFF) _sigFirstDual.delete(k2); }
    for (const k2 of _warnPrimed) { const t3 = parseInt(k2.split("_")[2]); if (!isNaN(t3) && t3 < CUTOFF) _warnPrimed.delete(k2); }
    for (const k2 in _tradeLastSrc) { const t3 = parseInt(k2.split("_")[2]); if (!isNaN(t3) && t3 < CUTOFF) delete _tradeLastSrc[k2]; }
  }
  for (const k of _warnedKeys) {
    const t = parseInt(k.split("_")[2]);
    if (!isNaN(t) && t < CUTOFF) _warnedKeys.delete(k);
  }
  
  for (const sym of ["BTC", "ETH"]) {
    for (const tf of INTERVALS) {
      const dur = INTERVAL_MS[tf];
      const t0 = Math.floor(now / dur) * dur;
      const cacheKey = `${sym}_${tf}_${t0}`;
      
      // Skip jika sudah ada di cache dan sesi masih sama
      if (_deskSigCache[cacheKey]) continue;

      // ===== CADANGAN SAJA =====
      // Bila server sudah menyediakan entri untuk (aset, tf) ini (termasuk saat masih
      // "pending"/flat), maka server yang berwenang. Klien TIDAK menghitung sinyal sendiri,
      // supaya tidak pernah ada dua mesin keputusan yang bisa berbeda.
      if (typeof LIVE !== "undefined" && LIVE.covers(sym, tf)) continue;
      
      // Minimal validation - pastikan ada candle data
      const candles5m = state.cache[sym]["5m"]?.candles || [];
      if (candles5m.length < 50) continue;  // Butuh minimal data untuk historical analysis
      
      // Untuk combo non-aktif, hitung signal berdasarkan historical pattern
      // Ini adalah lightweight calculation - tidak perlu full desktop filter
      const sig = calculateUniversalSignal(sym, tf, t0, now, candles5m);
      if (sig) {
        _deskSigLive[cacheKey] = sig;                 // live status (recomputed each tick)
        if (sig.verdict !== "flat") {                 // graded EARLY signal -> lock immediately
          // Attach gate info at lock time (single place; reused by capture + notifications).
          const gk = gateKey(tf, sig.mode, sig.verdict, sig.rsi, sig.histStrength);
          const g = gateLookup(gk);
          sig.gateKey = gk;
          sig.highConf = !!g;
          sig.gateWr = g ? g.wr : null;
          sig.lockedAt = now;                         // when the session signal was locked
          _deskSigCache[cacheKey] = sig;
          console.log(`[SIGNAL] early ${sym}/${tf} at +${Math.round((now - t0) / 1000)}s:`, sig.grade, sig.mode, sig.verdict, `expected ${sig.expectedWR != null ? (sig.expectedWR * 100).toFixed(1) + "%" : "—"}`);
          // NOTIFIKASI HANYA BILA SERVER TIDAK TERSEDIA (mode cadangan) dan HANYA untuk
          // timeframe yang sedang ditampilkan. Sebelumnya sinyal lokal (yang bisa BEDA dari
          // sinyal server yang dipakai kartu) juga memicu notifikasi -> notif terasa "palsu":
          // muncul padahal kartu tidak menampilkan sinyal apa pun (atau beda arah/tf).
          if ((typeof LIVE === "undefined" || !LIVE.fresh()) && tf === state.interval) {
            notifySignal(sym, tf, sig);
          }
        }
      }
    }
  }
  renderMonitors();   // wide screens: compact BTC/ETH cards (throttled, no-op on mobile)
  renderDual();       // wide screens: full per-coin detail for both coins side by side
}

// Calculate signal untuk coin/interval spesifik (dipakai universal)
function calculateUniversalSignal(sym, tf, t0, now, candles5m) {
  // Use first-candle pattern analysis (lighter than full desktop filter)
  const dur = INTERVAL_MS[tf];
  const tfSec = dur / 1000;
  const t0Sec = Math.floor(t0 / 1000);
  
  // Get session candles
  const sessionCandles = candles5m.filter(c => Math.floor(c.time / tfSec) * tfSec === t0Sec);
  if (sessionCandles.length < 1) {
    // Session baru, belum ada candle
    return {
      roundStart: t0,
      asset: sym,
      interval: tf,
      verdict: "flat",
      mode: "MENUNGGU",
      reason: SignalCore.buildReason({ verdict: "flat", mode: "MENUNGGU" }),
      conf: 0,
    };
  }
  
  // Get lock price + current price
  const lockPrice = sessionLock(sym, dur, now);
  const C = sessionCandles[sessionCandles.length - 1].close;
  if (lockPrice == null || C == null) {
    return null;  // Data belum lengkap
  }
  
  const elapsed = now - t0;
  const nowSecFloorW = Math.floor(now / 1000);
  const sessionOnes = (state.cache[sym]?.["1s"]?.candles || []).filter((c) => c.time >= t0Sec && c.time < nowSecFloorW);

  // WARMUP: the signal is evaluated in the FIRST 2 SECONDS of the session (per request), but we
  // still need at least two completed 1s candles to form a move and a volume sample.
  if (elapsed < 2000 || sessionOnes.length < 2) {
    return {
      roundStart: t0,
      asset: sym,
      interval: tf,
      verdict: "flat",
      mode: "WARMUP",
      reason: SignalCore.buildReason({ verdict: "flat", mode: "WARMUP", elapsedSec: elapsed / 1000 }),
      conf: 0,
    };
  }
  
  // Historical trend analysis (50 sessions) untuk non-active combo
  const histTrend = analyzeHistoricalTrend(sym, tf, 50);
  
  // First candle direction (lightweight signal)
  const firstCandleDir = sessionCandles[0].close > sessionCandles[0].open ? "bullish" : "bearish";
  const currentDir = C > lockPrice ? "up" : C < lockPrice ? "down" : "flat";
  
  // Volume analysis — baseline from completed candles BEFORE the session.
  // Compare volume PACE: project the still-forming candle to a full candle before
  // comparing, so the filter works early in the session (not only near its close).
  const prior = candles5m.filter(c => c.time < t0Sec).slice(-25);
  const baseVol = prior.length ? prior.reduce((a, c) => a + (c.vol || 0), 0) / prior.length : 0;
  const candleSec = 300;
  const nowS = now / 1000;
  const candleStart = Math.floor(nowS / candleSec) * candleSec;
  const frac = Math.min(1, Math.max(0.05, (nowS - candleStart) / candleSec));
  const forming = sessionCandles[sessionCandles.length - 1];
  const volRel = baseVol > 0 ? ((forming.vol || 0) / frac) / baseVol : 1;
  // LIQUIDITY gate — a dead market is the biggest risk, independent of the volume RATIO:
  // relative volume can look "normal" simply because the whole recent period was quiet.
  // Compare the projected full-candle volume to the 15th percentile of the last 50 5m
  // candles AND an absolute per-asset floor (VOL_TYPICAL scaled from 5s to 5m).
  const prior5 = candles5m.filter((c) => c.time < t0Sec).slice(-50).map((c) => c.vol || 0).filter((v) => v > 0);
  const typ5m = (VOL_TYPICAL[sym] || 0) * 60;
  const projVol = (forming.vol || 0) / frac;
  const liqMul = (GATES && GATES.liqFloorMul != null) ? GATES.liqFloorMul : 0.3;
  const liqFloor = Math.max(pctile(prior5, 15), typ5m * liqMul);
  const liqLow = typ5m > 0 && projVol < liqFloor;
  const liqRatio = typ5m > 0 ? projVol / typ5m : 1;   // 1.0 = typical market activity

  // RSI from completed 5m candles (no lookahead)
  const nowSecFloor = Math.floor(now / 1000);
  const rsi = rsiFromSeries(candles5m.filter(c => c.time < nowSecFloor).slice(-50), 14);

  // Decision via shared core — identical rules to the backtest
  const decision = SignalCore.decideSignal({ tf, elapsed, histTrend, firstCandleDir, currentDir, volRel, rsi });
  let verdict = decision.verdict, mode = decision.mode, conf = decision.conf;
  const histStr = histTrend?.strength || 0;
  // Executed order-flow imbalance (OFI) accumulated live from the trade stream.
  const ofi = sessionOFI(sym, t0Sec, nowSecFloor);
  const ofiAgree = (ofi == null || verdict === "flat") ? null : ((ofi >= 0) === (verdict === "up"));
  const ofiStrong = ofi != null && Math.abs(ofi) >= 0.2;
  // ---- 2-SECOND features (the signal is formed from the first 2s of the session) ----
  //  mv2      : the move so far vs the session open, in %
  //  surprise : that move divided by the average 1s range of the previous 60s (a "surprise"
  //             measure — how big the move is relative to recent tick noise)
  //  volRel2  : the first-2s volume projected to a full session candle, vs the prior 25 candles
  const secElapsed = Math.max(2, nowSecFloor - t0Sec);
  const vol2sum = sessionOnes.reduce((a, c) => a + (c.vol || 0), 0);
  const projSession = vol2sum * (tfSec / secElapsed);
  const volRel2 = baseVol > 0 ? projSession / baseVol : 1;
  const preOnes = (state.cache[sym]?.["1s"]?.candles || []).filter((c) => c.time < t0Sec).slice(-60);
  const sigma1s = preOnes.length
    ? preOnes.reduce((a, c) => a + Math.abs((c.high != null ? c.high : c.close) - (c.low != null ? c.low : c.close)), 0) / preOnes.length
    : 0;
  const moveAbs = Math.abs(C - lockPrice);
  const mv2 = lockPrice > 0 ? (moveAbs / lockPrice) * 100 : 0;
  const surprise = sigma1s > 0 ? moveAbs / sigma1s : 0;

  // TIER LADDER sinyal 2 detik. AMBANGNYA TIDAK DITULIS DI SINI — dibaca dari profil gate
  // aktif (/api/model/gates): bootstrap (longgar, fase kumpul data), learned (hasil uji
  // learner), atau strict (konservatif). Lihat gates.js dan gatesSummary().
  // NOTE: akurasi 90% TIDAK mungkin di 2 detik — plafon terukur ~57-62% untuk arah.
  const T = (GATES && GATES.tiers) || { STRONG: { volRel2: 3, surprise: 3 }, GOOD: { volRel2: 1.5, surprise: 2 }, FAIR: { volRel2: 0.3, surprise: 0 } };
  const gapNow = lockPrice > 0 ? Math.abs((C - lockPrice) / lockPrice) * 100 : 0;
  let grade = null;
  if (verdict !== "flat") {
    if (volRel2 >= T.STRONG.volRel2 && surprise >= (T.STRONG.surprise || 0)) grade = "STRONG";
    else if (volRel2 >= T.GOOD.volRel2 && surprise >= (T.GOOD.surprise || 0)) grade = "GOOD";
    else if (volRel2 >= T.FAIR.volRel2 && surprise >= (T.FAIR.surprise || 0)) grade = "FAIR";
    // lapisan kedua: ambang hasil belajar (bila learner sudah punya cukup bukti)
    if (grade && !gateThresholdsOK({ volRel2, surprise, liqRatio, gapPct: gapNow, histStrength: histStr, rsi })) grade = null;
  }
  if (!grade) {
    verdict = "flat";
    mode = volRel2 < T.FAIR.volRel2 ? "LOWVOL" : "FILTERED";
    conf = 0;
  }
  const FAIR_MIN = T.FAIR.volRel2;
  // LATE gate: after LATE_FRAC of the session the price is close to the lock, so the reward
  // is tiny even when accurate. Those entries are suppressed (user avoids them by choice).
  const late = elapsed >= ((GATES && GATES.lateFrac != null) ? GATES.lateFrac : LATE_FRAC) * dur;
  if (late && grade) { grade = null; verdict = "flat"; mode = "LATE"; conf = 0; }
  // LIQUIDITY gate: never signal in a dead market, whatever the ratio says.
  if (liqLow && grade) { grade = null; verdict = "flat"; mode = "NO-LIQ"; conf = 0; }
  // LOCK-TOUCH strategy: the way back to the lock and the measured probability for this distance.
  const d2 = lockPrice > 0 ? ((C - lockPrice) / lockPrice) * 100 : 0;
  const touch = lockTouchOf(tf, Math.abs(d2), d2 < 0 ? "up" : "down");
  const expectedGradeWR = grade ? (early2sWR(grade) != null ? early2sWR(grade) : gradeWR(tf, grade)) : null;
  const minuteIn = Math.floor(elapsed / 60000) + 1;                 // 1-based, matches the calibration
  const sessionMin = Math.round(dur / 60000);
  // Potential reward = distance from the current price to the lock (what is gained on a
  // full recapture). Drives the payout, and is why deep (contra) entries are preferred.
  const rewardPct = Math.abs(lockPrice - C) / C * 100;
  const expectedWR = grade ? (minuteWR(tf, minuteIn) != null ? minuteWR(tf, minuteIn) : expectedGradeWR) : null;
  let reason = SignalCore.buildReason({
    verdict,
    mode,
    rsi, volRel: volRel2, strength: histStr,
    momentum: histTrend?.momentum, elapsedSec: elapsed / 1000,
    tf, volMin: FAIR_MIN,
  });
  if (mode === "OFI contra") reason = "No entry. Executed order flow is against this direction.";
  if (mode === "LATE") reason = `No entry. Late in the session (${Math.round((elapsed / dur) * 100)}% elapsed, batas aktif ${(((GATES && GATES.lateFrac != null) ? GATES.lateFrac : LATE_FRAC) * 100).toFixed(0)}%) — reward too small at this distance from the lock.`;
  if (mode === "NO-LIQ") reason = `No entry. Liquidity too thin — the market is quiet (volume ${(liqRatio * 100).toFixed(0)}% of typical, need above ${((liqFloor / (typ5m || 1)) * 100).toFixed(0)}%).`;
  if (touch) reason += ` TOUCH LOCK: arah ${touch.dir.toUpperCase()} · jarak ${touch.dist.toFixed(3)}% dari lock · peluang historis ${(touch.rate * 100).toFixed(0)}% (median ${touch.tMed}s, dd ${touch.ddMed}%)${touch.tooClose ? " · PERINGATAN: terlalu dekat lock (sentuh hampir instan, reward ~0)" : ""}.`;

  // ---- LEARNER (Phase 1): konteks yang sudah tervalidasi 90 hari (walk-forward) ----
  // Dibungkus try/catch: kegagalan apa pun di modul pembelajaran TIDAK boleh mengganggu sinyal.
  let learn = null;
  try {
    const learnTrend = SignalCore.sessionTrend(((state.cache[sym]?.[tf]?.candles) || []).filter((c) => c.time < nowSecFloor), 3);
    learn = learnLookup({
      tf, symbol: sym, mode, minutesIn: minuteIn, rsi, volRel,
      histStrength: histStr, trend: learnTrend, hour: new Date(t0Sec * 1000).getUTCHours(),
      dir: currentDir, gapPct: Math.abs(d2),
    });
    const lNote = learnNote(learn);
    if (lNote && verdict !== "flat") reason += ` ${lNote}`;
    // Opsional (default MATI): tahan sinyal pada konteks yang historis lemah dan tervalidasi.
    if (LEARN_BLOCK && grade && learn.blockable.length) {
      mode = "LEARN-BLOCK"; conf = 0;
      reason = `No entry. Pelajaran 90d: konteks ini historis lemah (${learn.blockable[0]})` +
        (learn.touch != null ? ` · peluang kembali ke lock hanya ${(learn.touch * 100).toFixed(0)}%` : "") + ".";
      grade = null; verdict = "flat";
    }
  } catch (e) { console.warn("[LEARN] lookup failed:", e && e.message); }

  return {
    roundStart: t0,
    asset: sym,
    interval: tf,
    verdict,
    mode,
    reason,
    conf,
    rsi,
    histStrength: histStr,
    volRel,
    volRel2,
    surprise,
    mv2,
    sigma1s,
    grade,
    expectedWR,
    minuteIn,
    late,
    rewardPct,
    touch,
    liqLow,
    liqRatio,
    ofi,
    ofiAgree,
    learn,
  };
}

function captureConfidenceRound(t0, O, C, fadeDir, fadeConf, trendDir, mode, state) {
  // Guard against asset/interval switches mid-round: the accumulator is global, so a
  // switch would otherwise score the old combo's prediction against the new combo's prices.
  if (_cap_asset !== state.asset || _cap_interval !== state.interval) {
    _cap_t0 = null;
    _cap_pending = null;
    _cap_asset = state.asset;
    _cap_interval = state.interval;
  }
  // boundary: ronde sebelumnya baru saja berakhir (t0 berubah)
  if (_cap_t0 !== null && t0 !== _cap_t0) {
    const pendingMatches = _cap_pending && _cap_pending.asset === state.asset && _cap_pending.interval === state.interval;
    if (pendingMatches && _cap_lastClose != null && _cap_lastLock != null) {
      const actual = _cap_lastClose >= _cap_lastLock ? "up" : "down";
      const won = _cap_pending.dir === actual ? 1 : 0;
      ConfLog.add({
        ts: Date.now(),
        t0: _cap_pending.t0,
        asset: _cap_pending.asset,
        interval: _cap_pending.interval,
        mode: _cap_pending.mode,
        dir: _cap_pending.dir,
        conf: _cap_pending.conf,
        trend: _cap_pending.trend,
        lock: _cap_lastLock,
        close: _cap_lastClose,
        actual,
        won,
      });
      renderConfidenceReport();
    }
    _cap_pending = null;
  }
  _cap_t0 = t0;
  _cap_lastClose = C;
  _cap_lastLock = O;

  // entry capture: tick PERTAMA ronde di mana sinyal muncul (fadeDir !== null)
  if (_cap_pending === null && fadeDir) {
    _cap_pending = {
      t0, asset: state.asset, interval: state.interval, mode,
      dir: fadeDir, conf: fadeConf, trend: trendDir,
    };
  }
}

/* ===== REPARASI HISTORY =====
   Entri yang sudah tercatat bisa salah bila dinilai sebelum candle final (lihat gate di
   evaluateUniversalSessions). Fungsi ini memeriksa ulang entri terakhir memakai candle yang
   sekarang sudah final dan MEMPERBAIKI lock/close/actual/won bila berbeda.
   Dijalankan saat start + berkala; hanya menyentuh entri yang candle-nya sudah final. */
function repairLogOutcomes(limit = 400) {
  const log = SignalLog.data();
  if (!log.length) return 0;
  const now = Date.now();
  const tail = log.slice(-limit);
  let fixed = 0;
  for (const e of tail) {
    if (e.t0 == null || !e.interval) continue;
    const dur = INTERVAL_MS[e.interval];
    if (!dur) continue;
    const candles = state.cache[e.asset]?.[e.interval]?.candles || [];
    const t0Sec = Math.floor(e.t0 / 1000);
    const sc = candles.find((c) => c.time === t0Sec);
    if (!sc) continue;
    const tfSec = dur / 1000;
    const finalNow = candles.some((c) => c.time === t0Sec + tfSec) || (sc.closeTime != null && sc.closeTime <= now) || now >= e.t0 + dur + 30000;
    if (!finalNow) continue;
    const actual = sc.close >= sc.open ? "up" : "down";
    const won = e.dir === actual ? 1 : 0;
    if (e.actual !== actual || e.won !== won || e.lock !== sc.open || e.close !== sc.close) {
      e.actual = actual; e.won = won; e.lock = sc.open; e.close = sc.close; fixed++;
    }
  }
  if (fixed) { SignalLog.replaceAll(log); console.log(`[LOG] perbaiki ${fixed} entri hasil (candle sudah final)`); }
  return fixed;
}

// Finalize locked signals whose round has ENDED: write them to history with the outcome.
// Rounds still running stay in PendingSig, so history never shows an unfinished round.
function evaluateUniversalSessions(now) {
  const pending = PendingSig.all();
  if (!pending.length) return;
  let changed = false;

  for (const p of pending) {
    const dur = INTERVAL_MS[p.interval];
    if (!dur || p.t0 == null) continue;
    if (now < p.t0 + dur) continue;               // round still running

    const candles = state.cache[p.asset]?.[p.interval]?.candles || [];
    const t0Sec = Math.floor(p.t0 / 1000);
    const sc = candles.find((c) => c.time === t0Sec);
    if (!sc) {
      // Cannot score without the candle; drop only if it is far too old to ever resolve.
      if (now > p.t0 + dur + 3 * 3600000) PendingSig.remove(p);
      continue;
    }

    // ===== WAJIB: candle sesi harus SUDAH FINAL sebelum dinilai =====
    // Tanpa ini, penilaian bisa memakai close parsial (candle masih berjalan tepat saat ronde
    // berakhir) sehingga hasil salah PERMANEN -- mis. BTC diprediksi UP dan sesi memang close di
    // atas lock, tapi tercatat merah. Finalitas dinilai dari: (a) candle berikutnya sudah ada,
    // atau (b) closeTime candle sudah lewat, atau (c) cadangan: 30 detik setelah ronde berakhir.
    const tfSec = dur / 1000;
    const nextExists = candles.some((c) => c.time === t0Sec + tfSec);
    const closedByTime = sc.closeTime != null && sc.closeTime <= now;
    if (!nextExists && !closedByTime && now < p.t0 + dur + 30000) continue;

    const lock = sc.open;
    const close = sc.close;
    const actual = close >= lock ? "up" : "down";
    const won = p.dir === actual ? 1 : 0;

    const dup = _loggedKeys.has(logKeyOf(p));
    if (!dup) {
      SignalLog.add({
        ts: Date.now(),
        t0: p.t0,
        asset: p.asset,
        interval: p.interval,
        mode: p.mode,
        dir: p.dir,
        conf: p.conf,
        lock: lock,
        close: close,
        actual: actual,
        won: won,
        reason: p.reason || "",
        gateKey: p.gateKey,
        highConf: !!p.highConf,
        gateWr: p.gateWr != null ? p.gateWr : null,
        lockedAt: p.lockedAt,
      });
      _loggedKeys.add(logKeyOf(p));
    }
    // PHASE 2: hasil ronde -> ledger (jalur harga memberi sentuh-lock, waktu, MFE/MAE).
    try {
      const path = sessionPath(p.asset, t0Sec, t0Sec + dur / 1000);
      LEDGER.resolve(p.asset, p.interval, t0Sec, ledgerOutcome(lock, close, p.dir, path));
    } catch (e) { console.warn("[LEDGER] resolve failed:", e && e.message); }
    PendingSig.remove(p);
    changed = true;
  }

  if (changed) renderConfidenceReport();
}

// Move history entries whose round has NOT finished back into PendingSig.
// They were written by the old capture-at-start logic and must not appear as results yet.
function migrateUnfinished(now) {
  const data = SignalLog.data();
  if (!data.length) return;
  const keep = [];
  let moved = 0;
  for (const e of data) {
    const dur = INTERVAL_MS[e.interval];
    if (!dur || e.t0 == null || now >= e.t0 + dur) { keep.push(e); continue; }
    if (!PendingSig.has(`${e.asset}_${e.interval}_${e.t0}`)) {
      PendingSig.add({
        asset: e.asset, interval: e.interval, t0: e.t0, dir: e.dir, mode: e.mode,
        conf: e.conf, reason: e.reason, gateKey: e.gateKey, highConf: e.highConf,
        gateWr: e.gateWr, lockedAt: e.lockedAt,
      });
    }
    moved++;
  }
  if (moved) {
    SignalLog.replaceAll(keep);
    rebuildLoggedIndex();
    console.log(`[MIGRATE] moved ${moved} unfinished history entries back to pending`);
  }
}

// Remove duplicate history entries for the same (asset, interval, t0), keeping one.
function dedupeLog() {
  const data = SignalLog.data();
  const byKey = new Map();
  for (const e of data) {
    const k = `${e.asset}_${e.interval}_${e.t0}`;
    const prev = byKey.get(k);
    if (!prev) byKey.set(k, e);
    else if (prev.won === undefined && e.won !== undefined) byKey.set(k, e); // prefer scored
  }
  const out = [...byKey.values()].sort((a, b) => a.t0 - b.t0);
  if (out.length !== data.length) {
    SignalLog.replaceAll(out);
    rebuildLoggedIndex();
    console.log(`[DEDUPE] removed ${data.length - out.length} duplicate history entries`);
  }
}

// One-time correction for already-stored entries: re-derive lock/close/actual/won from the
// session candle so any entry scored with the old (5s-fallback) lock gets fixed.
function rescoreAll() {
  const data = SignalLog.data();
  let changed = 0;
  for (const entry of data) {
    const candles = state.cache[entry.asset]?.[entry.interval]?.candles || [];
    if (!candles.length || entry.t0 == null) continue;
    const t0Sec = Math.floor(entry.t0 / 1000);
    const sc = candles.find((c) => c.time === t0Sec);
    if (!sc) continue;                       // candle no longer in cache, cannot verify
    const actual = sc.close >= sc.open ? "up" : "down";
    const won = entry.dir === actual ? 1 : 0;
    if (entry.lock !== sc.open || entry.close !== sc.close || entry.actual !== actual || entry.won !== won) {
      entry.lock = sc.open;
      entry.close = sc.close;
      entry.actual = actual;
      entry.won = won;
      changed++;
    }
  }
  if (changed) {
    SignalLog.save();
    renderConfidenceReport();
    console.log(`[RESCORE] corrected ${changed} history entries`);
  }
}

function renderConfidenceReport() {
  const body = document.getElementById("conf-debug-body");
  const head = document.getElementById("conf-debug-head");
  const countEl = document.getElementById("conf-debug-count");
  if (!body) return;

  // Sumber data: LEDGER SERVER (kanonik, semua device sama). Bila server tidak terjangkau,
  // baru pakai catatan lokal (MobilePredLog) sebagai cadangan.
  const srv = (typeof LEDGER !== "undefined" && LEDGER.serverCached) ? LEDGER.serverCached() : null;
  let rounds = [];
  let srcLabel = "server";
  if (srv && srv.records && srv.records.length) {
    rounds = srv.records.map((r) => {
      const sig = r.sig || {};
      const res = r.res || null;
      const dir = (sig.verdict === "up" || sig.verdict === "down") ? sig.verdict
        : (sig.dir === "up" || sig.dir === "down") ? sig.dir : null;
      if (!dir) return null;
      const tr = (res && res.trade) || null;
      // Tiga keadaan (semuanya tampil ABU, hanya tooltip-nya yang beda):
      //   "pending"        -> sesi belum dinilai (belum ada res)
      //   "lama"           -> sudah dinilai tetapi SEBELUM fitur ini (tidak ada res.trade)
      //   (tercatat)       -> ada res.trade, dipakai untuk menentukan sukses/gagal
      const tradeState = tr ? "ok" : (res ? "lama" : "pending");
      // E (entry): undefined = tidak ada entry (abu); 1 = entry & target LOCK tercapai (hijau);
      //            0 = entry tapi LOCK tidak pernah tersentuh sampai sesi tutup (merah).
      const e = (tr && tr.entered) ? (tr.entryTouch ? 1 : 0) : undefined;
      // C (early close): undefined = tidak ada posisi (abu); 1 = early close ter-signal (hijau);
      //                  0 = posisi terbuka tapi early close tidak pernah ter-signal (merah).
      const c = (tr && tr.entered) ? (tr.closed ? 1 : 0) : undefined;
      return {
        asset: r.asset, interval: r.interval, dir,
        won: (res && res.won != null) ? res.won : undefined,
        lock: res ? res.lock : sig.lock, close: res ? res.close : null,
        actual: res ? res.actual : null, e, c, tradeState,
      };
    }).filter(Boolean);
  } else if (typeof MobilePredLog !== "undefined") {
    rounds = MobilePredLog.data().filter((r) => r.dir);
    srcLabel = "lokal";
  }
  // Minta data terbaru dari server (throttle 15s di dalam modul LEDGER), lalu render ulang bila
  // jumlah sesi berubah (mis. ada sesi baru yang selesai).
  if (typeof LEDGER !== "undefined" && LEDGER.server) {
    LEDGER.server().then((s2) => {
      const n1 = srv && srv.records ? srv.records.length : -1;
      const n2 = s2 && s2.records ? s2.records.length : -1;
      if (n2 !== n1) renderConfidenceReport();
    });
  }

  const tf = state.interval;
  const COINS = ["BTC", "ETH"];
  // Isi sisa lebar kartu (di layar lebar ~31 kolom @21px per blok koin). Kelebihannya bisa
  // di-scroll; di mobile default tampil dari KIRI = sesi TERBARU karena urutannya terbaru->lama.
  const PER_COIN = 40;

  // Satu sesi = SATU KOLOM berisi 3 baris: signal (U/D), entry (E), early close (C).
  // Warna memakai konvensi yang sudah ada: hijau = benar/sukses, merah = salah/gagal,
  // abu = belum ada hasil / tidak ada entry / tidak ada posisi.
  const stack = (r) => {
    const sCls = r.won === undefined ? "dot-pending" : (r.won ? "dot-win" : "dot-lose");
    const sTxt = r.dir === "up" ? "U" : "D";
    const naTxt = r.tradeState === "ok" ? "tidak ada"
      : r.tradeState === "lama" ? "tidak tercatat (data lama)" : "belum ada hasil (pending)";
    const eTxt = r.e === undefined ? naTxt : (r.e ? "sukses" : "gagal");
    const cTxt = r.c === undefined ? naTxt : (r.c ? "sukses" : "gagal");
    const eCls = r.e === undefined ? "dot-pending" : (r.e ? "dot-win" : "dot-lose");
    const cCls = r.c === undefined ? "dot-pending" : (r.c ? "dot-win" : "dot-lose");
    const t = `${r.asset}/${r.interval} · signal ${r.dir.toUpperCase()} ${r.won === undefined ? "(pending)" : (r.won ? "BENAR" : "SALAH")}`
      + ` · entry ${eTxt} · early close ${cTxt}`
      + ` · lock ${r.lock != null ? r.lock : "?"} close ${r.close != null ? r.close : "?"}`;
    return `<div class="sa-stack" title="${t}"><span class="dot ${sCls}">${sTxt}</span>`
      + `<span class="dot ${eCls}">E</span><span class="dot ${cCls}">C</span></div>`;
  };

  let totalShown = 0;
  let html = `<div class="sa-cols">`;
  for (const sym of COINS) {
    const data = rounds.filter((r) => r.asset === sym && r.interval === tf);
    const evald = data.filter((r) => r.won !== undefined);
    const wins = evald.reduce((a, r) => a + r.won, 0);
    const wr = evald.length ? Math.round(wins / evald.length * 100) : null;
    // Ringkasan TRADE ASSISTANT: S = entry sukses & early close sukses; F = ada posisi (entry
    // terjadi) tetapi tidak keduanya sukses. Sesi tanpa posisi tidak dinilai (tidak ada yang
    // bisa sukses/gagal) sehingga tidak masuk hitungan S/F.
    const withPos = data.filter((r) => r.tradeState === "ok" && r.e !== undefined);
    const sOk = withPos.filter((r) => r.e === 1 && r.c === 1).length;
    const fBad = withPos.length - sOk;
    // TERBARU -> TERLAMA (kiri = sesi paling baru). Di mobile scroll default dari kiri = terbaru.
    const tail = data.slice(-PER_COIN).reverse();
    totalShown += data.length;
    const statCls = wr == null ? "" : (wr >= 50 ? "cd-win" : "cd-lose");
    const sfPct = withPos.length ? Math.round(sOk / withPos.length * 100) : null;   // % sukses entry+close
    const sfCls = sfPct == null ? "" : (sfPct >= 50 ? "cd-win" : "cd-lose");
    const sfTxt = withPos.length
      ? `<span class="sa-sf"><span class="sa-s">S:${sOk}</span> <span class="sa-f">F:${fBad}</span>`
        + ` <span class="sa-sfpct ${sfCls}">(${sfPct}%)</span></span>`
      : `<span class="sa-sf sa-na">S:— F:—</span>`;
    html += `<div class="sa-coin">
      <div class="sa-coin-head"><b>${sym}</b> · ${tf}
        <span class="sa-head-right">
          <span class="sa-stat ${statCls}" title="signal: benar/salah">${evald.length ? `${wins}W/${evald.length - wins}L${wr != null ? ` (${wr}%)` : ""}` : "—"}</span>
          <span class="sa-stat-sf" title="Trade Assistant: S = entry &amp; early close sukses · F = ada entry tapi tidak keduanya sukses (${withPos.length} sesi berposisi)">${sfTxt}</span>
        </span>
      </div>
      ${tail.length
        ? `<div class="sa-axis"><span>◀ Terbaru</span><span>Lama ▶</span></div>`
          + `<div class="sa-sessions">${tail.map(stack).join("")}</div>`
        : `<div class="cd-empty">belum ada sesi</div>`}
    </div>`;
  }
  html += `</div><div class="sa-legend">tiap kolom = 1 sesi (terbaru di kiri) · ringkasan <b>S</b>=sukses entry+close · <b>F</b>=gagal (dengan % sukses) · baris 1 <b>S</b> signal U/D (hijau benar · merah salah · abu pending) · baris 2 <b>E</b> entry · baris 3 <b>C</b> early close (hijau sukses · merah gagal · abu = tidak ada entry/posisi atau tidak tercatat pada data lama)</div>`;

  if (head) head.textContent = "DESKTOP SIGNAL ACCURACY · per sesi (signal · entry · early close) · ";
  if (countEl) countEl.textContent = `${totalShown} sesi ${tf} · sumber ${srcLabel}`
    + `${GATE_STATUS === "ok" ? "" : ` · gate ${GATE_STATUS}`}${TIER_STATUS === "ok" ? "" : ` · tiers ${TIER_STATUS}`}`;
  body.innerHTML = html;
}

/* ----------------------- Mobile Prediction ----------------------- */
let _mobilePredSession = null;   // { roundStart, lockPrice, prediction, confidence, mode }
const predCache = { BTC: {}, ETH: {} };  // per-session cache: predCache[sym][interval + roundStart] = pred

function restoreMobilePredSession() {
  try {
    const raw = sessionStorage.getItem(MOBILE_PRED_SESSION_KEY);
    if (!raw) return;
    const saved = JSON.parse(raw);
    if (saved && saved.roundStart && saved.prediction) {
      const dur = INTERVAL_MS[state.interval];
      const now = serverNow();
      const curStart = Math.floor(now / dur) * dur;
      // Hanya restore jika session masih sama DAN asset/interval cocok
      const assetMatch = saved.asset === state.asset;
      const tfMatch = saved.interval === state.interval;
      if (Math.abs(saved.roundStart - curStart) < 1000 && assetMatch && tfMatch) {
        _mobilePredSession = saved;
        sessionStorage.removeItem(MOBILE_PRED_SESSION_KEY);   // consumed; guard prevents wrong restores otherwise
        console.log("[MOBILE-PRED] restored from sessionStorage:", saved);
      }
    }
  } catch (_) {}
}

function predictSessionStart(sym, tf) {
  const dur = INTERVAL_MS[tf];
  const now = serverNow();
  const curStart = Math.floor(now / dur) * dur;
  const curStartSec = Math.floor(curStart / 1000);
  const ticker = state.ticker[sym];
  if (!ticker) return null;

  const C = ticker.last;
  
  // Lock price dari sessionLock yang sama dengan chart
  const lockPrice = sessionLock(sym, dur, now);
  
  // Ambil candle 5m langsung dari cache (sudah ada dari loadHistory)
  const candles5m = state.cache[sym]["5m"].candles || [];
  if (candles5m.length < 5) return null;
  
  // Analisis pola: untuk setiap sesi 5m yang sudah selesai, catat arah candle pertama dan outcome
  const tfSec = dur / 1000;
  const sessions = [];
  for (let i = candles5m.length - 1; i >= 0; i--) {
    const c = candles5m[i];
    const sStart = Math.floor(c.time / tfSec) * tfSec;
    const existing = sessions.find(s => s.startSec === sStart);
    if (existing) {
      existing.close = c.close;
      existing.high = Math.max(existing.high, c.high);
      existing.low = Math.min(existing.low, c.low);
    } else {
      sessions.push({ startSec: sStart, open: c.open, high: c.high, low: c.low, close: c.close });
    }
    if (sessions.length >= 30) break;
  }
  sessions.reverse();
  
  // Hitung win rate berdasarkan first candle direction vs session outcome
  const patterns = [];
  for (let i = 0; i < sessions.length - 1; i++) {
    const s = sessions[i];
    const firstCandleDir = s.close > s.open ? 'bullish' : s.close < s.open ? 'bearish' : 'doji';
    const outcome = s.close >= s.open ? 'up' : 'down';
    patterns.push({ firstCandleDir, outcome });
  }
  
  const bullishFirst = patterns.filter(p => p.firstCandleDir === 'bullish');
  const bearishFirst = patterns.filter(p => p.firstCandleDir === 'bearish');
  
  const bullishWinRate = bullishFirst.length > 0
    ? bullishFirst.filter(p => p.outcome === 'up').length / bullishFirst.length
    : 0.5;
  const bearishWinRate = bearishFirst.length > 0
    ? bearishFirst.filter(p => p.outcome === 'down').length / bearishFirst.length
    : 0.5;
  
  const recentTrend = patterns.slice(-5).filter(p => p.outcome === 'up').length / 5;
  
  // Candle pertama sesi current dari 1s candles
  const ones = state.cache[sym]["1s"].candles || [];
  const sortedOnes = ones.slice().sort((a, b) => a.time - b.time);
  const sessionOnes = sortedOnes.filter(c => c.time >= curStartSec);
  const firstOne = sessionOnes[0];
  let currentFirstDir = null;
  
    if (firstOne && (now - firstOne.time * 1000) >= 2000) {
    currentFirstDir = firstOne.close > firstOne.open ? 'bullish' :
                      firstOne.close < firstOne.open ? 'bearish' : 'doji';
  }
  
  // Fallback: jika 1s candle belum tersedia > 15s, pakai 5m candle pertama sesi untuk arah
  if (!currentFirstDir && (now - curStart) >= 15000) {
    const session5m = candles5m.filter(c => c.time >= curStartSec);
    const first5m = session5m[0];
    if (first5m && first5m.close !== first5m.open) {
      currentFirstDir = first5m.close > first5m.open ? 'bullish' : 'bearish';
      console.log("[MOBILE-PRED] fallback to 5m first candle:", currentFirstDir);
    }
  }
  
  let prediction = 'flat';
  let confidence = 50;
  let mode = "MENUNGGU";
  
   if (!currentFirstDir && (now - curStart) < 15000) {
    prediction = "flat";
    confidence = 50;
    mode = "MENUNGGU";
  } else if (currentFirstDir && currentFirstDir !== 'doji') {
    if (currentFirstDir === 'bullish') {
      const winRate = bullishWinRate;
      prediction = winRate > 0.5 ? 'up' : 'down';
      confidence = Math.round(winRate * 100);
      mode = winRate > 0.6 ? "REVERSAL↑" : "CONT↑";
    } else if (currentFirstDir === 'bearish') {
      const winRate = bearishWinRate;
      prediction = winRate > 0.5 ? 'down' : 'up';
      confidence = Math.round(winRate * 100);
      mode = winRate > 0.6 ? "REVERSAL↓" : "CONT↓";
    }
    } else if (currentFirstDir === 'doji') {
    if (recentTrend > 0.5) {
      prediction = 'up';
      confidence = 55;
      mode = 'CONT↑';
    } else if (recentTrend < 0.5) {
      prediction = 'down';
      confidence = 55;
      mode = 'CONT↓';
    }
  } else if (!currentFirstDir && (now - curStart) >= 15000) {
    // Fallback akhir: pakai recent trend jika tidak ada candle 1s/5m
    if (recentTrend > 0.5) {
      prediction = 'up'; confidence = 55; mode = 'CANDLE';
    } else if (recentTrend < 0.5) {
      prediction = 'down'; confidence = 55; mode = 'CANDLE';
    }
  }

  const priceDelta = lockPrice ? (C - lockPrice) / lockPrice : 0;
  
  return {
    roundStart: curStart,
    asset: sym,
    interval: tf,
    lockPrice,
    prediction,
    confidence,
    mode,
    price: C,
    delta: priceDelta,
    trendBias: recentTrend > 0.5 ? 'bullish' : 'bearish',
  };
}

  function updateMobilePrediction() {
  const ticker = state.ticker[state.asset];
  if (!ticker) {
    console.log("[MOBILE-PRED] updateMobilePrediction: no ticker for", state.asset);
    return;
  }
  // ===== MOBILE PREDICTION = DIPROSES SERVER =====
  // Bila server sudah mengirim hasilnya untuk sesi ini, pakai itu (semua device sama);
  // kalau tidak, jalur lokal di bawah dipakai sebagai cadangan (offline).
  try {
    const e = (typeof LIVE !== "undefined") ? LIVE.entryFor(state.asset, state.interval) : null;
    if (e && e.mobilePred) {
      const sp = e.mobilePred;
      _mobilePredSession = { roundStart: sp.roundStart, asset: state.asset, interval: state.interval,
        lockPrice: sp.lockPrice, prediction: sp.prediction, confidence: sp.confidence, mode: sp.mode };
      const C2 = ticker.last;
      const pd = sp.lockPrice ? (C2 - sp.lockPrice) / sp.lockPrice : 0;
      const g = (id) => document.getElementById(id);
      const lockEl = g("m-lock"), dirEl = g("m-dir"), confEl = g("m-conf"),
            priceEl = g("m-price"), deltaEl = g("m-delta"), modeEl = g("m-mode");
      if (lockEl) lockEl.textContent = fmtPrice(sp.lockPrice);
      if (priceEl) priceEl.textContent = fmtPrice(C2);
      if (deltaEl) deltaEl.textContent = (pd >= 0 ? "+" : "") + (pd * 100).toFixed(2) + "%";
      if (modeEl) modeEl.textContent = sp.mode;
      if (dirEl) {
        dirEl.textContent = sp.prediction === "up" ? "UP ▲" : sp.prediction === "down" ? "DOWN ▼" : "—";
        dirEl.className = sp.prediction === "up" ? "up" : sp.prediction === "down" ? "down" : "";
      }
      if (confEl) {
        confEl.textContent = sp.prediction !== "flat" ? sp.confidence + "%" : "—";
        confEl.className = sp.prediction === "up" ? "up" : sp.prediction === "down" ? "down" : "";
      }
      return;   // sudah ditangani server -> tidak menghitung model lokal
    }
  } catch (_) {}

    const dur = INTERVAL_MS[state.interval];
  const now = serverNow();
  const roundStart = Math.floor(now / dur) * dur;
  const elapsed = now - roundStart;

  // FIX: Sinkronkan mobile prediction dengan desktop filter
  // Jika sesi baru (< 15s), paksa MENUNGGU sampai desktop filter siap analisis
  if (elapsed < 15000) {
    if (!_mobilePredSession || _mobilePredSession.roundStart !== roundStart || _mobilePredSession.asset !== state.asset || _mobilePredSession.interval !== state.interval) {
      _mobilePredSession = {
        roundStart,
        asset: state.asset,
        interval: state.interval,
        lockPrice: sessionLock(state.asset, dur, now),
        prediction: "flat",
        confidence: 50,
        mode: "MENUNGGU",
      };
      try { sessionStorage.setItem(MOBILE_PRED_SESSION_KEY, JSON.stringify(_mobilePredSession)); } catch (_) {}
      console.log("[MOBILE-PRED] Early session guard, forcing MENUNGGU until desktop filter ready");
    }
    // Update DOM dan return sebelum compute prediksi
    const pred = _mobilePredSession;
    const lockPrice = pred.lockPrice;
    const C = ticker.last;
    const priceDelta = lockPrice ? (C - lockPrice) / lockPrice : 0;

    const lockEl = document.getElementById("m-lock");
    const dirEl = document.getElementById("m-dir");
    const confEl = document.getElementById("m-conf");
    const priceEl = document.getElementById("m-price");
    const deltaEl = document.getElementById("m-delta");
    const modeEl = document.getElementById("m-mode");

    if (lockEl) lockEl.textContent = fmtPrice(lockPrice);
    if (priceEl) priceEl.textContent = fmtPrice(C);
    if (deltaEl) deltaEl.textContent = (priceDelta >= 0 ? "+" : "") + (priceDelta * 100).toFixed(2) + "%";
    if (modeEl) modeEl.textContent = pred.mode;
    if (dirEl) dirEl.textContent = "—";
    if (confEl) confEl.textContent = "—";
    return;
  }

  const cacheKey = state.interval + "_" + roundStart;
  const cached = predCache[state.asset][cacheKey];
  const sameSession = _mobilePredSession && _mobilePredSession.roundStart === roundStart && _mobilePredSession.asset === state.asset && _mobilePredSession.interval === state.interval;

  // Hitung ulang prediksi HANYA saat sesi baru, atau masih LOADING (data belum siap)
  const needPredict = !_mobilePredSession
    || _mobilePredSession.roundStart !== roundStart
    || _mobilePredSession.mode === "LOADING"
    || _mobilePredSession.mode === "MENUNGGU"
    || (_mobilePredSession.prediction === "flat" && (now - roundStart) < 30000)
    || _mobilePredSession.asset !== state.asset
    || _mobilePredSession.interval !== state.interval;

  if (needPredict) {
     if (cached && !sameSession && cached.prediction !== "flat") {
       // Switch back to previously computed interval — restore cached prediction to prevent flip
       _mobilePredSession = cached;
       console.log("[MOBILE-PRED] restored from cache:", cacheKey);
     } else {
       // Always (re)compute for new session, MENUNGGU, LOADING, or flat-within-30s
       const pred = predictSessionStart(state.asset, state.interval);
       if (pred) {
         _mobilePredSession = pred;
         if (pred.prediction !== "flat") {
          predCache[state.asset][cacheKey] = pred;
          const ck = Object.keys(predCache[state.asset]);
          if (ck.length > 200) delete predCache[state.asset][ck[0]];   // keep the cache bounded
        }
       } else if (!_mobilePredSession || _mobilePredSession.roundStart !== roundStart) {
         _mobilePredSession = {
           roundStart,
           asset: state.asset,
           interval: state.interval,
           lockPrice: sessionLock(state.asset, dur, now),
           prediction: "flat",
           confidence: 50,
           mode: "LOADING",
         };
       }
     }
    // Simpan ke sessionStorage agar tetap konsisten saat refresh
      try { sessionStorage.setItem(MOBILE_PRED_SESSION_KEY, JSON.stringify(_mobilePredSession)); } catch (_) {}
    }

  
  const pred = _mobilePredSession;
  const lockPrice = pred.lockPrice;
  const C = ticker.last;
  const priceDelta = lockPrice ? (C - lockPrice) / lockPrice : 0;

  // Update DOM
  const lockEl = document.getElementById("m-lock");
  const dirEl = document.getElementById("m-dir");
  const confEl = document.getElementById("m-conf");
  const priceEl = document.getElementById("m-price");
  const deltaEl = document.getElementById("m-delta");
  const modeEl = document.getElementById("m-mode");

  if (lockEl) lockEl.textContent = fmtPrice(lockPrice);
  if (priceEl) priceEl.textContent = fmtPrice(C);
  if (deltaEl) deltaEl.textContent = (priceDelta >= 0 ? "+" : "") + (priceDelta * 100).toFixed(2) + "%";
  if (modeEl) modeEl.textContent = pred.mode;

  if (dirEl) {
    dirEl.textContent = pred.prediction === "up" ? "UP ▲" : pred.prediction === "down" ? "DOWN ▼" : "—";
    dirEl.className = pred.prediction === "up" ? "up" : pred.prediction === "down" ? "down" : "";
  }
  if (confEl) {
    confEl.textContent = pred.prediction !== "flat" ? pred.confidence + "%" : "—";
    confEl.className = pred.prediction === "up" ? "up" : pred.prediction === "down" ? "down" : "";
  }
}

/* ----------------------- Controls ----------------------- */
function bindControls() {
  document.getElementById("asset-seg").addEventListener("click", (e) => {
    const b = e.target.closest("[data-asset]"); if (!b) return;
    state.asset = b.dataset.asset;
    _deskSig = null;  // Reset desktop signal lock on asset change
    segActive("asset-seg", b);
    renderActive(); updateProjection(); updateMobilePrediction(); updateGap();
    renderConfidenceReport();
    historyExhausted[state.asset] = false;
    setReach();
    loadOlderCandles(state.asset, 1000).catch(() => {});
  });
  document.getElementById("tf-seg").addEventListener("click", (e) => {
    const b = e.target.closest("[data-tf]"); if (!b) return;
    state.interval = b.dataset.tf;
    // Reset desktop signal lock karena interval berubah
    _deskSig = null;
    LIVE.connect(state.interval);      // stream sinyal server mengikuti interval aktif
    segActive("tf-seg", b);
    renderActive(); updateProjection(); updateMobilePrediction(); renderConfidenceReport();
  });
  document.getElementById("type-seg").addEventListener("click", (e) => {
    const b = e.target.closest("[data-type]"); if (!b) return;
    state.type = b.dataset.type;
    segActive("type-seg", b);
    applyType();
  });
  document.getElementById("chart-tf-seg").addEventListener("click", (e) => {
    const b = e.target.closest("[data-ctf]"); if (!b) return;
    state.chartInterval = b.dataset.ctf;
    segActive("chart-tf-seg", b);
    renderActive(); updateProjection();
  });
  // Tab arah keyakinan (hanya di mobile: tab SIGNAL/UP/DOWN tidak ditampilkan di layar lebar).
  const applyConfMode = (mode) => {
    confMode = mode;
    const seg = document.getElementById("conf-cta");
    const btn = seg && seg.querySelector(`[data-mode="${mode}"]`);
    if (btn) segActive("conf-cta", btn);
    updateProjection();
  };
  {
    const seg = document.getElementById("conf-cta");
    if (seg) seg.addEventListener("click", (e) => {
      const b = e.target.closest("[data-mode]"); if (!b) return;
      applyConfMode(b.dataset.mode);
    });
  }
  const cdbg = document.getElementById("conf-debug-reset");
  if (cdbg) cdbg.addEventListener("click", () => {
    // PENTING: reset ini SENGAJA hanya mengosongkan tampilan akurasi lokal (MobilePredLog) dan
    // cache in-memory. JANGAN menambahkan localStorage.clear() / LEDGER.clear() di sini —
    // ledger belajar tersimpan di server (volume) dan salinan lokalnya harus tetap utuh.
    MobilePredLog.clear();
    PendingSig.clear();
    // Clear in-memory caches in place (they may be const)
    for (const k in _deskSigCache) delete _deskSigCache[k];
    for (const k in _deskSigMap) delete _deskSigMap[k];
    for (const k in _deskSigLive) delete _deskSigLive[k];
    // Reset active session state
    _mobilePredSession = null;
    sessionStorage.removeItem(MOBILE_PRED_SESSION_KEY);
    renderConfidenceReport();
  });
  const mrefresh = document.getElementById("m-refresh");
  if (mrefresh) mrefresh.addEventListener("click", () => {
    console.log("[MOBILE-PRED] refresh button clicked");
    _mobilePredSession = null;
    sessionStorage.removeItem(MOBILE_PRED_SESSION_KEY);
    renderConfidenceReport();
    updateMobilePrediction();
  });

  // Donate popup
  const dOpen = document.getElementById("donate-cta");
  const dClose = document.getElementById("donate-close");
  const dCopy = document.getElementById("donate-copy");
  const dPopup = document.getElementById("donate-popup");
  if (dOpen) dOpen.addEventListener("click", () => { if (dPopup) dPopup.hidden = false; });
  if (dClose) dClose.addEventListener("click", () => { if (dPopup) dPopup.hidden = true; });
  if (dCopy) dCopy.addEventListener("click", async () => {
    const addr = "0x3bc04EC5b40315ea77eCDe734f310660D52B25E5";
    try {
      await navigator.clipboard.writeText(addr);
      dCopy.textContent = "COPIED!";
      setTimeout(() => { dCopy.textContent = "COPY"; }, 2000);
    } catch (_) {
      const ta = document.createElement("textarea"); ta.value = addr; document.body.appendChild(ta); ta.select();
      document.execCommand("copy"); ta.remove();
      dCopy.textContent = "COPIED!";
      setTimeout(() => { dCopy.textContent = "COPY"; }, 2000);
    }
  });
}
function segActive(segId, btn) {
  document.querySelectorAll(`#${segId} .seg-btn`).forEach((x) => x.classList.remove("active"));
  btn.classList.add("active");
}

/* ======================= Historical Trend Analysis ======================= */
function analyzeHistoricalTrend(sym, tf, sessionCount) {
  return SignalCore.analyzeHistoricalTrend(state.cache[sym]?.[tf]?.candles, sessionCount);
}

/* ======================= Audio Alert (Web Audio API) ======================= */
let audioCtx = null;
let audioBlocked = false;   // true = browser memblokir suara (belum ada gesture user)

// Indikator status suara di topbar: 🔊 aktif / 🔇 diblokir (klik = aktifkan + tes suara)
function updateAudioHint() {
  const el = document.getElementById("audio-hint"); if (!el) return;
  const on = !!(audioCtx && audioCtx.state === "running");
  if (on) audioBlocked = false;
  el.textContent = on ? "🔊" : "🔇";
  el.className = "audio-hint " + (on ? "on" : "off");
  el.title = on
    ? "Suara notifikasi AKTIF (klik untuk tes 3 suara)"
    : "Suara notifikasi DIBLOKIR browser sampai ada interaksi. Klik di sini untuk mengaktifkan.";
  el.setAttribute("aria-label", on ? "Suara notifikasi aktif" : "Suara notifikasi diblokir — klik untuk mengaktifkan");
}

// Shared tone sequencer with a GENTLE timbre: sine/triangle only, a low-pass filter and
// smooth attack/release. A square wave + hard on/off (the old signal sound) is the harshest
// possible waveform for a small speaker and can make the membrane rattle.
function playSequence(notes, opts) {
  try {
    if (!audioCtx) audioCtx = new (window.AudioContext || window.webkitAudioContext)();
    const ctx = audioCtx;
    // Browsers blokir AudioContext sampai ada gesture user. Jangan menjadwalkan nada saat
    // context belum "running": itu membuat warning berulang di console dan suara tetap tak
    // berbunyi. Coba resume sekali; kalau masih belum jalan, keluar (tandai audioBlocked).
    if (ctx.state !== "running") {
      let pr = null;
      try { pr = ctx.resume(); } catch (_) {}
      audioBlocked = true; updateAudioHint();
      if (pr && typeof pr.then === "function") {
        pr.then(() => { if (audioCtx && audioCtx.state === "running") { audioBlocked = false; updateAudioHint(); } }).catch(() => {});
      }
      return;
    }
    audioBlocked = false;
    const master = ctx.createGain();
    master.gain.value = (opts && opts.vol != null) ? opts.vol : 0.55;
    const lp = ctx.createBiquadFilter();
    lp.type = "lowpass";
    lp.frequency.value = (opts && opts.cutoff) || 2000;   // strip the harsh upper harmonics
    lp.Q.value = 0.7;
    master.connect(lp);
    lp.connect(ctx.destination);
    // Optional echo (delay + feedback, damped) — unused by the cockpit alerts, kept for reuse.
    let echoIn = null;
    if (opts && opts.echo) {
      const e = opts.echo;
      const wet = ctx.createGain(); wet.gain.value = e.wet != null ? e.wet : 0.3;
      const dl = ctx.createDelay(1.0); dl.delayTime.value = e.delay || 0.2;
      const fb = ctx.createGain(); fb.gain.value = e.feedback != null ? e.feedback : 0.3;
      const damp = ctx.createBiquadFilter(); damp.type = "lowpass"; damp.frequency.value = 1700;
      wet.connect(dl); dl.connect(damp); damp.connect(fb); fb.connect(dl); dl.connect(lp);
      echoIn = wet;
    }
    for (const n of notes) {
      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.type = n.type || "sine";
      const t0 = ctx.currentTime + n.t;
      osc.frequency.setValueAtTime(n.f, t0);
      // Sonar glide: sweep the pitch to n.f2 across the note.
      if (n.f2) osc.frequency.exponentialRampToValueAtTime(Math.max(1, n.f2), t0 + n.d);
      // Optional vibrato — gives a bell-like ringing character.
      if (n.vib) {
        const lfo = ctx.createOscillator();
        const lfoGain = ctx.createGain();
        lfo.frequency.value = n.vibRate || 6;
        lfoGain.gain.value = n.vib;
        lfo.connect(lfoGain);
        lfoGain.connect(osc.frequency);
        lfo.start(t0);
        lfo.stop(t0 + n.d + 0.06);
      }
      const peak = n.vol != null ? n.vol : 0.22;
      // Sonar ping: very fast attack + long exponential decay (n.ping). Otherwise soft envelope.
      const atk = n.ping ? 0.008 : 0.05;
      const rel = Math.min(0.12, Math.max(0.06, n.d * 0.4));
      gain.gain.setValueAtTime(0.0001, t0);
      gain.gain.exponentialRampToValueAtTime(peak, t0 + atk);
      if (!n.ping) gain.gain.setValueAtTime(peak, t0 + Math.max(atk, n.d - rel));
      gain.gain.exponentialRampToValueAtTime(0.0001, t0 + n.d);
      osc.connect(gain);
      gain.connect(master);
      if (echoIn) gain.connect(echoIn);
      osc.start(t0);
      osc.stop(t0 + n.d + 0.06);
    }
  } catch (e) { console.log("[SOUND] failed:", e.message); }
}
// SIGNAL ENTRY — DANGER ALARM: four identical short beeps "teett . teett . teett . teett"
// Lower pitch + slower spacing than the trade sounds so it reads as an alarm, not a chirp.
function playSoundAlert() {
  playSequence([
    { f: 780, t: 0.00, d: 0.14, type: "triangle", vol: 0.26, ping: true },
    { f: 780, t: 0.26, d: 0.14, type: "triangle", vol: 0.26, ping: true },
    { f: 780, t: 0.52, d: 0.14, type: "triangle", vol: 0.26, ping: true },
    { f: 780, t: 0.78, d: 0.20, type: "triangle", vol: 0.26, ping: true },
  ], { vol: 0.6, cutoff: 3200 });
  console.log("[SOUND] danger alarm (signal) played");
}
// TRADE ASSISTANT entry/average — AUTOPILOT DISCONNECT: three quick high beeps (fewer, higher,
// faster than the danger alarm so the two are not confused).
function playTradeEntrySound() {
  playSequence([
    { f: 1175, t: 0.00, d: 0.08, type: "triangle", vol: 0.26, ping: true },
    { f: 1175, t: 0.16, d: 0.08, type: "triangle", vol: 0.26, ping: true },
    { f: 1175, t: 0.32, d: 0.12, type: "triangle", vol: 0.26, ping: true },
  ], { vol: 0.6, cutoff: 3600 });
}
// TRADE ASSISTANT close/cut — "TING" ala notifikasi iPhone: SATU pukulan nada tunggal,
// attack cepat + decay panjang (~1,6s). Nada oktaf di atasnya dibunyikan BERSAMAAN dengan
// volume kecil hanya untuk memberi karakter "bell"; tidak ada nada kedua/bergantian sehingga
// tidak terasa ramai. (Sebelumnya: dua kali "whoop" turun -> terlalu intens & bersaut-sautan.)
function playCloseSound() {
  playSequence([
    { f: 1318.5, t: 0.00, d: 1.60, type: "sine", vol: 0.30, ping: true },   // E6 — "ting"
    { f: 2637.0, t: 0.00, d: 0.70, type: "sine", vol: 0.085, ping: true },  // oktaf (kilau bell)
  ], { vol: 0.55, cutoff: 6200 });
  console.log("[SOUND] close bell (ting) played");
}

// Preload audio context on first user interaction
const unlockAudio = () => {
  if (!audioCtx) {
    try { audioCtx = new (window.AudioContext || window.webkitAudioContext)(); } catch (e) {}
  }
  if (audioCtx && audioCtx.state !== "running") {
    try {
      const pr = audioCtx.resume();
      if (pr && typeof pr.then === "function") pr.then(() => { audioBlocked = false; updateAudioHint(); }).catch(() => {});
    } catch (_) {}
  }
  audioBlocked = !(audioCtx && audioCtx.state === "running");
  updateAudioHint();
  requestNotifyPermission();
};
// tanpa { once:true } supaya tetap bisa mencoba lagi bila percobaan pertama belum berhasil
document.addEventListener("click", unlockAudio);
document.addEventListener("touchstart", unlockAudio);
document.addEventListener("keydown", unlockAudio);
document.addEventListener("pointerdown", unlockAudio);
// SATU-SATUNYA kontrol suara (chip status di topbar): aktifkan audio + tes 3 nada sekaligus.
// Chip inilah pengganti tombol tes terpisah di baris kontrol, supaya header/controls tidak
// menambah elemen yang mendorong tata letak keluar frame di mobile.
(function bindAudioHint() {
  const b = document.getElementById("audio-hint");
  if (b) b.addEventListener("click", (ev) => {
    ev.stopPropagation();
    unlockAudio();
    setTimeout(() => playSoundAlert(), 60);        // 1) danger alarm (sinyal entry)
    setTimeout(playTradeEntrySound, 1300);         // 2) beep autopilot (entry trade)
    setTimeout(playCloseSound, 2100);              // 3) whoop turun (close)
  });
  updateAudioHint();
  setTimeout(updateAudioHint, 1200);
})();

/* ===== Background alert: desktop notification + tab title flash =====
   Audio stays ACTIVE-COMBO only (see updateSignal). These fire for EVERY combo so a
   signal detected while the tab is not focused is still surfaced. */
let _origTitle = document.title;
let _titleAlert = false;
let _lastNotifAt = 0;
const _alertedKeys = new Set();   // one background alert per combo per session

function requestNotifyPermission() {
  try {
    if (typeof Notification !== "undefined" && Notification.permission === "default") {
      Notification.requestPermission().catch(() => {});
    }
  } catch (_) {}
}
function flashTitle(text) {
  if (!_titleAlert) { _origTitle = document.title.replace(/^🔔.*?·\s*/, ""); _titleAlert = true; }
  document.title = `🔔 ${text} · ${_origTitle}`;
}
function clearTitleFlash() {
  if (_titleAlert) { document.title = _origTitle; _titleAlert = false; }
}
function notifySignal(sym, tf, sig) {
  const key = `${sym}_${tf}_${sig.roundStart}`;
  if (_alertedKeys.has(key)) return;         // once per combo per session
  _alertedKeys.add(key);

  const dir = String(sig.verdict || "").toUpperCase();
  const hidden = typeof document.hidden === "boolean" ? document.hidden : false;
  if (!hidden) return;                         // tab fokus -> UI sudah menampilkan, jangan ganggu
  flashTitle(`${dir} ${sym}/${tf}`);          // terlihat di judul tab saat tab tidak fokus
  const nowMs = Date.now();
  if (nowMs - _lastNotifAt < 5000) return;     // throttle notifications
  _lastNotifAt = nowMs;
  const tier = sig.highConf ? "HIGH CONF" : "watchlist";
  const body = `${sig.mode} · ${tier}${sig.gateWr != null ? ` · ${(sig.gateWr * 100).toFixed(0)}%` : ""}`;
  try {
    if (typeof Notification !== "undefined" && Notification.permission === "granted") {
      new Notification(`Signal ${dir} · ${sym}/${tf}`, { body, tag: key });
    }
  } catch (_) {}
}
document.addEventListener("visibilitychange", () => {
  if (!document.hidden) { clearTitleFlash(); if (audioCtx && audioCtx.state === "suspended") { try { audioCtx.resume(); } catch (_) {} } }
});
window.addEventListener("focus", clearTitleFlash);

/* ----------------------- Boot ----------------------- */
// Diagnostic: status of EVERY coin/interval combo (background engine + accuracy).
// Run `__comboStatus()` in the browser console.
window.__comboStatus = function () {
  const now = serverNow();
  const rows = [];
  for (const sym of ["BTC", "ETH"]) {
    for (const tf of INTERVALS) {
      const dur = INTERVAL_MS[tf];
      const t0 = Math.floor(now / dur) * dur;
      const key = `${sym}_${tf}_${t0}`;
      const live = _deskSigLive[key];
      const locked = _deskSigCache[key];
      const hist = SignalLog.data().filter((e) => e.asset === sym && e.interval === tf);
      const ev = hist.filter((e) => e.won !== undefined);
      const wins = ev.reduce((a, e) => a + e.won, 0);
      let status = "—";
      let risk = "—";
      if (locked) {
        const lock = sessionLock(sym, dur, now);
        const price = (state.cache[sym]?.["5s"]?.candles || []).slice(-1)[0]?.close ?? (state.ticker[sym] ? state.ticker[sym].last : null);
        const five = state.cache[sym]?.["5s"]?.candles || [];
        const w = five.slice(-24);
        const reg = w.length >= 3 ? slopeOf(w) : null;
        const sl = reg ? reg.b : 0;
        const reg2 = w.length >= 3 ? slopeOf(w.slice(-Math.max(3, Math.ceil(w.length / 3)))) : null;
        const slRecent = reg2 ? reg2.b : sl;
        const cl = w.map((c) => c.close);
        const mu = cl.length ? cl.reduce((a, b) => a + b, 0) / cl.length : price;
        const sd = cl.length ? Math.sqrt(cl.reduce((a, b) => a + (b - mu) * (b - mu), 0) / cl.length) : 0;
        const isUp = locked.verdict === "up";
        const mg = isUp ? (price - lock) : (lock - price);
        const nowS = Math.floor(now / 1000);
        const health = computeSignalHealth(locked.verdict, {
          margin: mg,
          marginStd: sd > 0 ? Math.abs(price - lock) / sd : null,
          slope: sl, slopeRecent: slRecent,
          ofi: sessionOFI(sym, t0 / 1000, nowS),
          ofiShort: sessionOFI(sym, nowS - 120, nowS),
          rsi: SignalCore.rsiFromSeries((state.cache[sym]?.["5m"]?.candles || []).filter((c) => c.time < Math.floor(now / 1000)).slice(-50), 14),
          histTrend: analyzeHistoricalTrend(sym, tf, 50),
        });
        status = health.label;
        risk = String(health.score);
      }
      const tierNow = locked ? (locked.grade || "LOCKED") : (live && live.verdict !== "flat" ? (live.grade || "—") : "—");
      const ofiNow = sessionOFI(sym, t0 / 1000, Math.floor(now / 1000));
      rows.push({
        combo: `${sym}/${tf}`,
        elapsed: Math.round((now - t0) / 1000) + "s",
        tier: tierNow,
        liq: live && live.liqRatio != null ? (live.liqRatio * 100).toFixed(0) + "%" : "—",
        live: live ? live.mode + (live.verdict !== "flat" ? " " + live.verdict : "") : "—",
        locked: locked ? locked.mode + " " + locked.verdict : "—",
        ofi: ofiNow == null ? "—" : (ofiNow * 100).toFixed(0) + "%",
        status: status,
        risk: risk,
        pending: PendingSig.has(key) ? "yes" : "no",
        rounds: hist.length,
        evaluated: ev.length,
        winrate: ev.length ? ((wins / ev.length) * 100).toFixed(1) + "%" : "—",
        candles: ((state.cache[sym]?.["5m"]?.candles || []).length) + "/" + ((state.cache[sym]?.[tf]?.candles || []).length),
      });
    }
  }
  console.table(rows);
  console.log(
    "connected:", state.connected,
    "· transport:", state.viaProxy ? "proxy/SSE" : usingTV ? "tradingview" : "ws",
    "· pending:", PendingSig.size(),
    "· history:", SignalLog.size(),
    "· gate:", GATE_STATUS
  );
  return rows;
};

window.addEventListener("error", (e) => {
  const el = document.getElementById("err");
  if (el) { el.hidden = false; el.textContent = "JS Error: " + (e.message || e.error) + (e.filename ? " @ " + e.filename + ":" + e.lineno : ""); }
});

// Pastikan panel learner ADA di DOM. Kalau HTML yang dimuat lebih lama dari app.js
// (tab lama / cache), panel dibuat sendiri di sini sehingga fitur tetap terlihat tanpa
// harus mengandalkan versi HTML terbaru.
function ensureLearnerPanel() {
  // chip akses cepat di topbar (dibuat juga bila HTML lama tidak memilikinya)
  if (!document.getElementById("learn-chip")) {
    const brand = document.querySelector(".brand") || document.querySelector(".topbar");
    if (brand) {
      const b = document.createElement("button");
      b.id = "learn-chip"; b.className = "learn-chip"; b.type = "button";
      b.title = "Buka panel STATUS LEARNER"; b.textContent = "LRN —";
      brand.appendChild(b);
    }
  }
  if (!document.getElementById("learn-status")) {
    const d = document.createElement("details");
    d.className = "learn-status"; d.id = "learn-status"; d.open = true;
    d.innerHTML = `<summary>STATUS LEARNER · PROGRESS, PELAJARAN &amp; PENYESUAIAN ▾</summary>
      <div class="lstat-hint">Panel ini menunjukkan apa yang sedang dipelajari sistem dari sinyal nyata dan
      penyesuaian apa yang sudah/akan diterapkan. Model belajar hanya menggantikan tabel backtest bila
      <b>menang pada jendela uji</b> (split berurutan waktu + Wilson bound). Sesi yang <b>ditolak</b> gate
      tetap direkam supaya ambangnya bisa dipelajari dari data.</div>
      <div id="lstat-body" class="lstat-body">memuat…</div>`;
    const anchor = document.querySelector("details.help") || document.querySelector(".conf-debug");
    if (anchor && anchor.parentNode) anchor.parentNode.insertBefore(d, anchor); else document.body.appendChild(d);
  }
  if (!document.getElementById("lessons-body")) {
    const d = document.createElement("details");
    d.className = "lessons"; d.id = "lessons";
    d.innerHTML = `<summary>PELAJARAN DARI SINYAL LALU · LEARNER 90d ▾</summary>
      <div class="ls-hint">Konteks tervalidasi <b>walk-forward</b>: latih 70% data paling awal, uji 30%
      paling akhir, dinilai Wilson bound. Hanya konteks yang lolos uji yang ditampilkan.</div>
      <div id="lessons-body" class="ls-body"></div>
      <div class="ls-foot"><span id="ls-status">—</span> · tahan sinyal pada konteks lemah: <code>window.setLearnBlock(true)</code></div>
      <div class="ls-foot"><b>LEDGER BELAJAR</b> <span id="ls-ledger">memuat…</span></div>`;
    const anchor = document.querySelector("details.help");
    if (anchor && anchor.parentNode) anchor.parentNode.insertBefore(d, anchor); else document.body.appendChild(d);
  }
}

function start() {
  initChart();
  ensureLearnerPanel();
  bindControls();
  restoreMobilePredSession();
  startData();
  migrateUnfinished(serverNow());
  dedupeLog();
  rebuildLoggedIndex();                     // build O(1) index after migrations
  evaluateUniversalSessions(serverNow());   // finalize rounds that already ended (e.g. after reload)
  renderConfidenceReport();
  LIVE.connect(state.interval);      // sinyal dari server (SSE)
  setTimeout(() => { try { repairLogOutcomes(); } catch (_) {} }, 4000);
  setInterval(() => { try { repairLogOutcomes(); } catch (_) {} }, 120000);
  loadGate();
  loadTiers();
  loadLearn();
  loadGates();
  renderGateLine();
  updateLearnChip();
  loadLearnerStatus();
  setInterval(loadGate, 10 * 60 * 1000);
  setInterval(loadTiers, 10 * 60 * 1000);
  setInterval(loadLearn, 5 * 60 * 1000);    // pelajaran/tabel: cadangan; model baru sudah otomatis terdeteksi tiap 60s
  setInterval(loadLearnerStatus, 60 * 1000);   // status learner di-refresh tiap menit
  setInterval(loadGates, 5 * 60 * 1000);       // profil gate: cadangan (sinkron utama via /api/learner tiap 60s)
  setTimeout(rescoreAll, 8000);        // after history candles are loaded
  setTimeout(rescoreAll, 25000);
  // timers — use rAF for smooth timer, updateProjection only on data events
   requestAnimationFrame(updateTimerDisplay);
   setInterval(updateProjection, 5000);  // heavy projection update every 5s only
   setInterval(() => {  // Universal background: update all combos every 3s
     if (state.connected) {
       updateProjectionUniversal();
       captureDesktopSignal();
     }
   }, 3000);
   startOrderbookPoll();  // real-time orderbook (200ms browser fetch)
  renderMonitors(true);  // wide screens: draw both coin cards immediately
  renderDual();          // wide screens only: builds/draws the dual detail columns
  window.addEventListener("resize", () => { renderMonitors(true); renderDual(true); });

   // Active visitor tracking
  let visitorId = localStorage.getItem("bps_vid") || null;
  const visitorsEl = document.getElementById("visitors");
  function sendVisit() {
    if (!visitorId) {
      fetch("/api/visit").then(r => r.json()).then(d => { visitorId = d.id; localStorage.setItem("bps_vid", visitorId); updateVisitors(); }).catch(() => {});
    } else {
      fetch(`/api/visit?id=${visitorId}`).catch(() => {});
    }
  }
  function updateVisitors() {
    fetch("/api/stats").then(r => r.json()).then(d => {
      if (visitorsEl) visitorsEl.innerHTML = `👁 <b>${d.activeUsers || 0}</b>`;
    }).catch(() => {});
  }
  sendVisit();
  setInterval(sendVisit, 10000);
  setInterval(updateVisitors, 15000);
  updateVisitors();

  // (Tombol tes suara terpisah DIHAPUS: kini tes 3 suara ada di chip status suara topbar,
  //  sehingga hanya ada SATU ikon suara dan baris kontrol tidak meluber di layar sempit.)

  window.addEventListener("resize", () => chart && chart.fit());
}
start();
