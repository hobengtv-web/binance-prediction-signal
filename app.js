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

function setSrc(label) { document.getElementById("src").textContent = "SRC " + (label || "—"); }
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
function sessionBounds(durMs, now) {
  const start = Math.floor(now / durMs) * durMs;
  return { start, end: start + durMs };
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
    if (o.volRel != null) reasons.push("Volume " + (o.volRel >= 10 ? "at least 10x" : o.volRel.toFixed(1) + "x"));

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
    const liqEl = document.getElementById("s-liq");
    const confEl = document.getElementById("s-conf");
    const confBar = document.getElementById("s-conf-bar");
    const reasonEl = document.getElementById("entryReason");
    const statusEl = document.getElementById("calcStatus");
    
    // Live calculation status (when and how the signal is produced)
    if (statusEl) statusEl.textContent = o.calcStatus || "";
    // Generate dynamic entry reason based on analysis
    if (reasonEl) {
      reasonEl.textContent = generateEntryReason(o);
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
    if (volEl) volEl.textContent = (o.volRel == null) ? "—" : (o.volRel >= 10 ? "≥10×" : o.volRel.toFixed(1) + "×");
    if (liqEl) {
      const liq = o.liquidity || "NORMAL";
      liqEl.textContent = liq;
      liqEl.className = liq === "LOW" ? "down" : liq === "THIN" ? "warn" : liq === "—" ? "" : "up";
    }
    if (rec) {
      if (o.analyzing) {
        rec.textContent = "Sedang Menganalisa";
        rec.className = "signal-rec flat";
      } else if (o.verdict === "flat") {
        rec.textContent = "No entry for this round";
        rec.className = "signal-rec flat";
      } else {
        const dirWord = o.verdict === "up" ? "UP" : "DOWN";
        const tier = o.highConf
          ? `HIGH CONFIDENCE, backtested winrate ${(o.gateWr * 100).toFixed(0)} percent`
          : "watchlist only, not filtered for high winrate";
        rec.textContent = `Recommendation: ${dirWord}. Mode ${o.mode}. ${tier}.`;
        rec.className = "signal-rec " + (o.verdict === "up" ? "up" : o.verdict === "down" ? "down" : "flat");
      }
    }
    
    // Trigger alarm otomatis ketika sinyal entry muncul (flat -> up/down transisi)
    const prevVerdict = lastSignalState ? lastSignalState.verdict : "flat";
    if (o.verdict !== "flat" && prevVerdict === "flat" && confMode === "SIGNAL" && o.highConf) {
      playSoundAlert();
      console.log("[ALERT] High-confidence signal:", o.verdict, o.mode, o.gateWr);
    }
    lastSignalState = { verdict: o.verdict, mode: o.mode, highConf: !!o.highConf };
  }

  let confSegs = null;
  const CONF_SEG = 20;
  function updateConfidenceDisplay(dir, val) {
    const dirEl = document.getElementById("conf-dir");
    const valEl = document.getElementById("conf-val");
    const track = document.getElementById("conf-track");
    if (!dirEl || !track) return;
    if (!dir) {
      dirEl.textContent = "—"; valEl.textContent = "—";
      if (confSegs) confSegs.forEach((s) => { s.className = "conf-seg off"; s.style.background = ""; });
      return;
    }
    dirEl.textContent = dir === "down" ? "DOWN" : "UP";
    valEl.textContent = val + "%";
    if (!confSegs) {
      confSegs = [];
      track.innerHTML = "";
      for (let i = 0; i < CONF_SEG; i++) {
        const s = document.createElement("i");
        s.className = "conf-seg off";
        track.appendChild(s);
        confSegs.push(s);
      }
    }
    for (let i = 0; i < CONF_SEG; i++) {
      const seg = confSegs[i];
      const p = (i + 0.5) / CONF_SEG;           // posisi 0..1 sepanjang gradasi
      const hue = Math.round(p * 120);            // 0 merah -> 120 hijau
      const lit = ((i + 1) / CONF_SEG) * 100 <= val;
      seg.style.background = "hsl(" + hue + ", 85%, 50%)";
      seg.className = "conf-seg " + (lit ? "on" : "off");
    }
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

    // ----- projection to settlement, capped so it can't be absurd -----
    const remSec = Math.max(0, remaining / 1000);
    const expMove = slope * remSec;
    const cappedExp = clamp(expMove, -std * 3, std * 3);
    const projectedClose = C + cappedExp;
    const contDir = projectedClose > O ? "up" : projectedClose < O ? "down" : "flat";

    chart.setProjection([
      { time: nowSec, value: C },
      { time: closeSec, value: projectedClose },
    ]);

    // ----- peak (titik balik) detection inside the decision window -----
    const L = swingLookback();
    const sw = detectSwings(win, L);
    const lastH = sw.highs[sw.highs.length - 1];
    const lastL = sw.lows[sw.lows.length - 1];
    // most recent extreme = the peak we fade
    let peak = null;
    if (lastH && lastL) peak = (lastH.time >= lastL.time) ? { price: lastH.price, dir: "top", time: lastH.time } : { price: lastL.price, dir: "bot", time: lastL.time };
    else if (lastH) peak = { price: lastH.price, dir: "top", time: lastH.time };
    else if (lastL) peak = { price: lastL.price, dir: "bot", time: lastL.time };

    // ----- auto trend line (reversal-aware): membelok di titik balik (peak) -----
    // Bukan lagi 1 regresi rata-rata window, sehingga terlihat momentum reversal:
    // naik ke puncak lalu turun (top) atau turun ke dasar lalu naik (bottom).
    let trendFit = [];
    if (peak) {
      const idx = win.findIndex((c) => c.time === peak.time);
      if (idx >= 0) {
        const seg1 = win.slice(0, idx + 1);   // sebelum titik balik
        const seg2 = win.slice(idx);          // sesudah titik balik
        const r1 = linreg(seg1), r2 = linreg(seg2);
        const pts = [];
        if (r1 && seg1.length) pts.push({ time: seg1[0].time, value: r1.a + r1.b * seg1[0].time });
        pts.push({ time: peak.time, value: peak.price });             // titik balik
        if (r2 && seg2.length) pts.push({ time: nowSec, value: C });  // ujung = harga live saat ini
        if (pts.length >= 2) trendFit = pts;
      }
    }
    if (!trendFit.length && reg) {
      // tidak ada peak terdeteksi → fallback garis regresi window (seperti semula)
      const a = win[0], b = win[win.length - 1];
      trendFit = [
        { time: a.time, value: reg.a + reg.b * a.time },
        { time: b.time, value: reg.a + reg.b * b.time },
      ];
    }
    chart.setTrendFit(trendFit);

    const markers = [];
    if (lastH) markers.push({ time: lastH.time, value: lastH.price, color: "#f6465d", text: "▲P" });
    if (lastL) markers.push({ time: lastL.time, value: lastL.price, color: "#0ecb81", below: true, text: "▼P" });
    markers.push({
      time: closeSec, value: projectedClose,
      color: contDir === "up" ? "#0ecb81" : contDir === "down" ? "#f6465d" : "#848e9c",
      text: (contDir === "up" ? "AKHIR ↑ " : contDir === "down" ? "AKHIR ↓ " : "AKHIR ") + fmtPrice(projectedClose),
    });
    chart.setMarkers(markers);

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
  const uni = _deskSigCache[uniKey];              // locked (non-flat only)
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
  const calcStatus = uni
    ? `Signal locked ${elapsedSec - Math.round((now - uni.lockedAt) / 1000)}s after session open · mode ${uni.mode}`
    : `Evaluating session, open +${elapsedSec}s · ${liveSig ? liveSig.mode : "collecting data"}`;
  // Still analyzing until a signal locks OR the warmup window has passed with a verdict.
  const analyzing = !uni && (
    elapsedSec < 15 ||
    !liveSig ||
    liveSig.mode === "MENUNGGU" ||
    liveSig.mode === "WARMUP"
  );

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
      });
    }
    
    updateSignal({
      zone, momentum, rsi, verdict: finalVerdict, mode, trendBias, aligned, conf,
      peakPrice: peakPrice != null ? peakPrice : (peak ? peak.price : null),
      peakDir: peak ? peak.dir : null,
      reward: reward,
      volRel: hasVolData ? rel : null, liquidity: liquidity,
      reason: currentReason,
      highConf: !!gateInfo,
      gateWr: gateInfo ? gateInfo.wr : null,
      calcStatus,
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

    let fadeConf = 0;
    if (fadeDir) {
      const dn = fadeDir === "down";
      let base;
      // Tab SIGNAL yang sync mobile prediction: gunakan analisis 3-4 sesi sebelumnya
      if (alignWithTrend || (mobLocked && confMode === "SIGNAL")) {
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
      fadeConf = clamp(Math.round(base * reach), 0, 100);
    }

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

    updateConfidenceDisplay(fadeDir, fadeConf);

    // akurasi: bekukan prediksi di momen entry, evaluasi saat round berakhir
     captureConfidenceRound(t0, O, C, fadeDir, fadeConf, curTrendDir, confMode, state);
     

     // Universal background: update all coin/interval signal cache setiap tick
     updateProjectionUniversal();

     // akurasi mobile prediksi: evaluasi di setiap tick
     // Pastikan _mobilePredSession fresh sebelum capture (hindari race condition)
     updateMobilePrediction();
     captureDesktopSignal();

    // price zone overlay (entry zone / sell TP)
    const pred = _mobilePredSession;
    const predDir = pred && pred.prediction !== "flat" && pred.lockPrice === O ? pred.prediction : liveStatus;
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
    if (stEl) { stEl.className = "round-status " + liveStatus; stEl.textContent = liveStatus === "up" ? "LIVE ▲ UP" : liveStatus === "down" ? "LIVE ▼ DOWN" : "LIVE —"; }
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
      const sym = state.asset;
      const binanceSym = OB_SYMBOLS[sym];
      if (!binanceSym) { _obBusy = false; return; }
      try {
        const r = await fetch(`https://data-api.binance.vision/api/v3/depth?symbol=${binanceSym}&limit=5`, { cache: "no-store" });
        if (r.ok) {
          const ob = await r.json();
          if (ob.bids && ob.asks) {
            state.orderbook[sym] = { bids: ob.bids, asks: ob.asks };
            updateOrderbook(sym);
          }
        }
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
  const candles = state.cache[sym][tf].candles;
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
      
      const cached = _deskSigCache[cacheKey];
      if (cached && cached.verdict !== "flat") {
        _deskSigMap[cacheKey] = true;
        
        const gkey = gateKey(tf, cached.mode, cached.verdict, cached.rsi, cached.histStrength);
        const g = gateLookup(gkey);
        // Lock = the session open price, taken from the interval candle itself (matches the chart
        // lock line). Falls back to the 5s series only if that candle is not available.
        const t0Sec = Math.floor(t0 / 1000);
        const sessionCandle = (state.cache[sym]?.[tf]?.candles || []).find((c) => c.time === t0Sec);
        const lock = sessionCandle ? sessionCandle.open : sessionLock(sym, dur, now);
        const entry = {
          ts: Date.now(),
          t0: t0,
          asset: sym,
          interval: tf,
          mode: cached.mode,
          dir: cached.verdict,
          conf: cached.conf,
          lock: lock,
          reason: cached.reason || "",
          gateKey: gkey,
          highConf: !!g,
          gateWr: g ? g.wr : null,
        };
        // Hold in PendingSig until the round ends (avoids early + duplicate history entries).
        const alreadyFinal = _loggedKeys.has(cacheKey);
        if (!alreadyFinal) {
          PendingSig.add(entry);
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
    if (!isNaN(t) && t < CUTOFF) delete _deskSigLive[k];
  }
  
  for (const sym of ["BTC", "ETH"]) {
    for (const tf of INTERVALS) {
      const dur = INTERVAL_MS[tf];
      const t0 = Math.floor(now / dur) * dur;
      const cacheKey = `${sym}_${tf}_${t0}`;
      
      // Skip jika sudah ada di cache dan sesi masih sama
      if (_deskSigCache[cacheKey]) continue;
      
      // Minimal validation - pastikan ada candle data
      const candles5m = state.cache[sym]["5m"]?.candles || [];
      if (candles5m.length < 50) continue;  // Butuh minimal data untuk historical analysis
      
      // Untuk combo non-aktif, hitung signal berdasarkan historical pattern
      // Ini adalah lightweight calculation - tidak perlu full desktop filter
      const sig = calculateUniversalSignal(sym, tf, t0, now, candles5m);
      if (sig) {
        _deskSigLive[cacheKey] = sig;                 // live status (recomputed each tick)
        if (sig.verdict !== "flat") {
          sig.lockedAt = now;                         // when the session signal was locked
          _deskSigCache[cacheKey] = sig;              // lock ONLY once a real signal appears
          console.log(`[SIGNAL] locked ${sym}/${tf} at +${Math.round((now - t0) / 1000)}s:`, sig.mode, sig.verdict, `conf ${sig.conf}`);
        }
      }
    }
  }
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
  
  // WARMUP filter - skip if too early
  if (elapsed < 15000) {
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

  // RSI from completed 5m candles (no lookahead)
  const nowSecFloor = Math.floor(now / 1000);
  const rsi = rsiFromSeries(candles5m.filter(c => c.time < nowSecFloor).slice(-50), 14);

  // Decision via shared core — identical rules to the backtest
  const decision = SignalCore.decideSignal({ tf, elapsed, histTrend, firstCandleDir, currentDir, volRel, rsi });
  let verdict = decision.verdict, mode = decision.mode, conf = decision.conf;
  const histStr = histTrend?.strength || 0;
  const reason = SignalCore.buildReason({
    verdict, mode, rsi, volRel, strength: histStr,
    momentum: histTrend?.momentum, elapsedSec: elapsed / 1000,
  });
  
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
  
  // Show universal report for ALL coin/interval combos
  const allData = MobilePredLog.data();
  
  // Count total across all combos
  const totalAll = allData.length;
  const pendingCount = PendingSig.size();
  if (head) head.textContent = `DESKTOP SIGNAL ACCURACY · Universal · `;
  if (countEl) countEl.textContent = `${totalAll} rounds total${pendingCount ? ` · ${pendingCount} waiting to settle` : ""}${GATE_STATUS === "ok" ? "" : ` · gate ${GATE_STATUS}`}`;
  
  if (!totalAll) {
    body.innerHTML = `<div class="cd-empty">no completed rounds yet — ${pendingCount ? pendingCount + " signal(s) waiting to settle" : "let it run a few rounds"}</div>`;
    return;
  }

  // Generate report per combo
  const combos = [["BTC", "5m"], ["ETH", "5m"], ["BTC", "15m"], ["ETH", "15m"], ["BTC", "1h"], ["ETH", "1h"]];
  let html = "";
  
  for (const [sym, tf] of combos) {
    const data = allData.filter(r => r.asset === sym && r.interval === tf);
    const n = data.length;
    if (n < 1) continue;  // Skip empty combos
    
    // Winrate only from rounds that have finished (won defined); rest are pending
    const evaluated = data.filter(r => r.won !== undefined);
    const nEval = evaluated.length;
    const pending = n - nEval;
    const totW = evaluated.reduce((a, r) => a + r.won, 0);
    const overall = nEval > 0 ? (totW / nEval * 100).toFixed(1) : "—";
    const wins = totW, losses = nEval - totW;
    const winClass = nEval > 0 ? (Number(overall) >= 50 ? "cd-win" : "cd-lose") : "";
    
    const dirLetter = (d) => d === "up" ? "U" : d === "down" ? "D" : "?";
    const nums = (r) => `lock ${r.lock != null ? r.lock : "?"} close ${r.close != null ? r.close : "?"} actual ${r.actual || "?"}`;
    const dots = data.slice(-20).map(r => {
      if (r.won === undefined) {
        return `<span class="dot dot-pending" title="${r.dir.toUpperCase()} pending | ${nums(r)}">${dirLetter(r.dir)}</span>`;
      }
      const cls = r.won ? "dot-win" : "dot-lose";
      return `<span class="dot ${cls}" title="${r.dir.toUpperCase()} ${r.won ? 'BENAR' : 'SALAH'} | ${nums(r)}">${dirLetter(r.dir)}</span>`;
    }).join('');
    
    html += `
      <div class="cd-row" style="margin-bottom:6px;">
        <span class="cd-b">${sym}/${tf}</span>
        <span class="cd-c">${nEval}${pending ? ` (+${pending})` : ''}</span>
        <span class="cd-wr ${winClass}">${overall}${nEval > 0 ? '%' : ''}</span>
        <span style="text-align:right;color:${nEval > 0 ? (Number(overall) >= 50 ? 'var(--up)' : 'var(--down)') : 'var(--muted, #888)'}">
          ${wins}W / ${losses}L${pending ? ` · ${pending} pending` : ''}
        </span>
      </div>
      <div class="cd-dots" style="margin-top:4px; margin-bottom:8px;">${dots}</div>
    `;
  }
  
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
  document.getElementById("conf-cta").addEventListener("click", (e) => {
    const b = e.target.closest("[data-mode]"); if (!b) return;
    confMode = b.dataset.mode;
    segActive("conf-cta", b);
    updateProjection();
  });
  const cdbg = document.getElementById("conf-debug-reset");
  if (cdbg) cdbg.addEventListener("click", () => {
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
function playSoundAlert() {
  try {
    if (!audioCtx) audioCtx = new (window.AudioContext || window.webkitAudioContext)();
    const ctx = audioCtx;
    if (ctx.state === 'suspended') ctx.resume();
    
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    
    osc.type = 'square';
    osc.frequency.setValueAtTime(440, ctx.currentTime);
    
    // Pulsing pattern - 8 beats over 2 seconds
    const pattern = [
      [0.00, 0.25, 0.30],
      [0.25, 0.50, 0.30],
      [0.50, 0.75, 0.35],
      [0.75, 1.00, 0.35],
      [1.00, 1.25, 0.40],
      [1.25, 1.50, 0.40],
      [1.50, 1.75, 0.40],
      [1.75, 2.00, 0.40]
    ];
    
    gain.gain.setValueAtTime(0, ctx.currentTime);
    pattern.forEach(([start, end, vol]) => {
      gain.gain.setValueAtTime(0, ctx.currentTime + start);
      gain.gain.linearRampToValueAtTime(vol, ctx.currentTime + start + 0.02);
      gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + end);
    });
    
    osc.connect(gain);
    gain.connect(ctx.destination);
    osc.start(ctx.currentTime);
    osc.stop(ctx.currentTime + 2.00);
    console.log("[SOUND] 2-second pulsing alert played");
  } catch (e) {
    console.log("[SOUND] failed:", e.message);
  }
}

// Preload audio context on first user interaction
const unlockAudio = () => {
  if (!audioCtx) {
    try { audioCtx = new (window.AudioContext || window.webkitAudioContext)(); } catch (e) {}
  }
};
document.addEventListener("click", unlockAudio, { once: true });
document.addEventListener("touchstart", unlockAudio, { once: true });

/* ----------------------- Boot ----------------------- */
window.addEventListener("error", (e) => {
  const el = document.getElementById("err");
  if (el) { el.hidden = false; el.textContent = "JS Error: " + (e.message || e.error) + (e.filename ? " @ " + e.filename + ":" + e.lineno : ""); }
});

function start() {
  initChart();
  bindControls();
  restoreMobilePredSession();
  startData();
  migrateUnfinished(serverNow());
  dedupeLog();
  rebuildLoggedIndex();                     // build O(1) index after migrations
  evaluateUniversalSessions(serverNow());   // finalize rounds that already ended (e.g. after reload)
  renderConfidenceReport();
  loadGate();
  setInterval(loadGate, 10 * 60 * 1000);
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

  // Audio test button
  const audioTestBtn = document.getElementById("audio-test-btn");
  if (audioTestBtn) {
    audioTestBtn.addEventListener("click", () => {
      try {
        playSoundAlert();
        audioTestBtn.textContent = "✓";
      } catch (e) {
        audioTestBtn.textContent = "❌";
      }
      setTimeout(() => { audioTestBtn.textContent = "🔊"; }, 2000);
    });
  }

  window.addEventListener("resize", () => chart && chart.fit());
}
start();
