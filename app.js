/* ============================================================
   Binance Prediction Signal — BTC / ETH
   Real-time candles + active-candle projection (Up/Down)
   Data: Binance public REST + WebSocket (no API key needed)
   ============================================================ */

const SYMBOLS = {
  BTC: "btcusdt",
  ETH: "ethusdt",
};
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
  // previous live price for gap sync
  prevPrice: { BTC: null, ETH: null },
  connected: false,
  viaProxy: false,
};

let serverTimeOffset = 0;  // client now → server now correction (ms)
let lastTimeSync = 0;      // Date.now() of last successful time sync
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
  // Fetch 1m candles (Binance REST supports this) and expand to 1s granularity
  const histLimit = Math.min(limit, 1000);
  // 1) proxy server (same-origin)
  try {
    const r = await fetch(`/api/klines?symbol=${sym}&tf=1m&before=${beforeSec}&limit=${histLimit}`);
    if (r.ok) {
      const j = await r.json();
      if (j && Array.isArray(j.candles) && j.candles.length) return expandTo1s(j.candles);
    }
  } catch (e) { console.warn("[HISTORY] proxy fetch failed:", e); }
  // 2) fallback: direct Binance
  try {
    const rows = await fetchJSON(`https://api.binance.com/api/v3/klines?symbol=${SYMBOLS[sym]}&interval=1m&limit=${histLimit}&endTime=${beforeSec * 1000 - 1000}`);
    const candles = rows.map((r) => ({
      time: Math.floor(r[0] / 1000),
      open: +r[1], high: +r[2], low: +r[3], close: +r[4],
      vol: +r[5], trades: +r[8],
      openTime: r[0], closeTime: r[6],
    }));
    return expandTo1s(candles);
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

  if (symKey === state.asset && tf === "1s") { chart.setData(activeCandles()); updateProjection(); updateMobilePrediction(); }
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
    const cls = candles.map((c) => c.close);
    if (cls.length < period + 1) return null;
    let gain = 0, loss = 0;
    for (let i = cls.length - period; i < cls.length; i++) {
      const d = cls[i] - cls[i - 1];
      if (d >= 0) gain += d; else loss -= d;
    }
    if (loss === 0) return 100;
    const rs = (gain / period) / (loss / period);
    return 100 - 100 / (1 + rs);
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
      if (o.verdict !== "flat" && o.mode.indexOf("REVERSAL") === 0) { rv.textContent = o.verdict === "up" ? "FADE ▲" : "FADE ▼"; rv.className = o.verdict === "up" ? "up" : "down"; }
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
      if (o.verdict === "flat") {
        rec.textContent = "Waiting for signal…";
        rec.className = "signal-rec flat";
      } else {
        rec.textContent = "RECOMMENDATION: " + (o.verdict === "up" ? "UP ▲ (fade peak)" : "DOWN ▼ (fade peak)");
        rec.className = "signal-rec " + (o.verdict === "up" ? "up" : o.verdict === "down" ? "down" : "flat");
      }
    }
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

    const aligned = trendBias !== "flat" && ((trendBias === "up" && slope > 0) || (trendBias === "down" && slope < 0));
    // continuation gated by TREND; peak-reversal & close-standing are independent edges
    let finalVerdict;
    if (mode === "CLOSE" || mode.indexOf("REVERSAL") === 0) finalVerdict = verdict;
    else finalVerdict = (verdict !== "flat" && aligned) ? verdict : "flat";

    // ----- GOAL: penguat sinyal — hanya sinyal COUNTER-TREND (REVERSAL / proyeksi berlawanan) -----
    // Aturan GOAL (sesuai ekspektasi):
    //   • REVERSAL↑ (masuk UP saat harga dari bawah) & REVERSAL↓ (masuk DOWN saat harga dari atas)
    //     → SELALU diizinkan & dikuatkan (ini sinyal momentum yg diinginkan)
    //   • CONT & CLOSE → blokir bila SEARAH harga (Down saat down / Up saat up):
    //       - berlaku sepanjang ronde, termasuk awal ronde (menit 0-3)
    //         dan 20 detik terakhir (mode CLOSE) → DIBLOKIR → NETRAL
    //   • bila berlawanan arah harga live → diizinkan & dikuatkan
    let goalHit = false;
    if (GOAL.mode === "REVERSAL_ONLY" && finalVerdict !== "flat") {
      if (mode.indexOf("REVERSAL") === 0) {
        goalHit = true; // reversal selalu counter-trend → sesuai GOAL
      } else {
        // CONT (kelanjutan tren) & CLOSE (akhir ronde) sama-sama "lanjut tren" vs LOCK
        const isContinuation = finalVerdict === liveStatus; // sama arah dgn harga → tidak diinginkan
        if (isContinuation) {
          finalVerdict = "flat";
          mode = "BLOCKED-GOAL";
        } else {
          goalHit = true; // berlawanan arah → sesuai GOAL
        }
      }
    }

    // ----- WARMUP: 3 menit pertama ronde → signal di-suppress (kurang presisi) -----
    if (elapsed < WARMUP_MS && finalVerdict !== "flat") {
      finalVerdict = "flat";
      mode = "WARMUP";
    }

    // ----- LOW VOLUME: likuiditas tipis → entry berisiko, tekan ke NETRAL -----
    if (liquidity === "LOW" && finalVerdict !== "flat") {
      finalVerdict = "flat";
      mode = "LOWVOL";
    }

    // ----- confidence: distance from LOCK + TREND (3 sesi) + momentum -----
    let conf = 0;
    if (finalVerdict !== "flat") {
      const edge = Math.abs(C - O) / std;
      const edgeScore = clamp(edge / 2, 0, 1) * 35;
      const htfScore = (trendBias === "flat") ? 0 : 20;
      const momScore = clamp(Math.abs(slope) * remSec / std, 0, 1) * 15;
      conf = Math.round(edgeScore + htfScore + momScore);
      if (mode === "CLOSE") conf = Math.min(100, conf + 10);
      if (mode.indexOf("REVERSAL") === 0) conf = Math.min(100, conf + 5);
      if (goalHit) conf = Math.min(100, conf + GOAL.boost);
    }

    // Sync: hanya lock arah UP/DOWN (verdict) ke mobile prediction, confidence level tetap realtime
    const mob = _mobilePredSession;
    const mobLocked = mob && mob.prediction !== "flat" && mob.mode !== "MENUNGGU" && mob.mode !== "LOADING";
    if (mobLocked && confMode === "SIGNAL") {
      finalVerdict = mob.prediction;
      mode = mob.mode;
    }

    updateSignal({
      zone, momentum, rsi, verdict: finalVerdict, mode, trendBias, aligned, conf,
      peakPrice: peakPrice != null ? peakPrice : (peak ? peak.price : null),
      peakDir: peak ? peak.dir : null,
      reward: reward,
      volRel: hasVolData ? rel : null, liquidity: liquidity,
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
    
    // akurasi mobile prediksi: evaluasi di setiap tick
    // Pastikan _mobilePredSession fresh sebelum capture (hindari race condition)
    updateMobilePrediction();
    captureMobilePrediction();

    // price zone overlay (entry zone / sell TP)
    const pred = _mobilePredSession;
    const predDir = pred && pred.prediction !== "flat" && pred.lockPrice === O ? pred.prediction : liveStatus;
     chart.setPrediction(predDir !== "flat" ? predDir : null, O);
    chart.setCurrentPrice(C);

    // Store session bounds for smooth rAF timer (client-time reference to avoid serverTimeOffset jitter)
    const sk = state.asset + ":" + dur + ":" + t0;
    _sessionT0 = t0; _sessionT = T; _sessionO = O; _sessionC = C; _sessionDur = dur;
    // Only recompute _sessionT_client when session boundary changes (prevents flicker)
    if (sk !== _sessionKey) { _sessionKey = sk; _sessionT_client = T - serverTimeOffset; _lastTimerSec = -1; }
  }

  let _lastTimerSec = -1, _sessionT0 = 0, _sessionT = 0, _sessionO = 0, _sessionC = 0, _sessionDur = 0, _sessionT_client = 0;
  let _sessionKey = "";  // guards against flicker: recompute _sessionT_client only when session changes
  function updateTimerDisplay() {
    const now = Date.now();  // pure client time — no serverTimeOffset jitter
    ensureTimeSync();  // re-sync if stale (>3s since last sync)
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

/* ----------------------- Trend (akumulasi N sesi interval aktif) ----------------------- */
// Mayoritas sesi naik -> BULLISH, mayoritas turun -> BEARISH, sisanya FLAT.
// Dipakai utk TREND display & sbg bias tren di confidence / keputusan sinyal.
function sessionTrend(sym, tf, n) {
  const candles = state.cache[sym][tf].candles;
  if (!candles || candles.length < 2) return "flat";
  // exclude candle sesi AKTIF yg masih terbentuk, agar trend stabil & sesuai desain
  // (counter-trend dihitung dari sesi yg SUDAH selesai). Bounce live di sesi aktif
  // tidak boleh membalik trend menjadi SEARAH.
  const last = candles.slice(0, -1).slice(-n);
  let bull = 0, bear = 0;
  for (const c of last) {
    const d = c.close - c.open;
    if (d > 0) bull++;
    else if (d < 0) bear++;
  }
  const need = Math.ceil(n / 2);
  if (bull > bear && bull >= need) return "bullish";
  if (bear > bull && bear >= need) return "bearish";
  return "flat";
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
  updateGap();
  if (state.asset && state.interval) renderActive();
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
  // sync client ↔ Binance server time using trade timestamp
  serverTimeOffset = ts - Date.now();
  lastTimeSync = Date.now();

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
   if (sym === state.asset) { chart.setData(activeCandles()); updateProjection(); }
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
      store.meta = arr[arr.length - 1] || null;
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
  };
})();

let _cap_t0 = null;        // t0 ronde yang sedang di-capture
let _cap_pending = null;   // prediksi entry: { t0, asset, interval, mode, dir, conf, trend }
let _cap_lastClose = null; // C terakhir (close ronde sebelumnya saat boundary)
let _cap_lastLock = null;  // O terakhir (lock ronde sebelumnya)

// Mobile prediction tracking
let _mob_t0 = null;        // t0 ronde mobile pred yang sedang di-capture
let _mob_pending = null;   // prediksi mobile: { t0, asset, interval, mode, dir, conf, lock }
let _mob_lastClose = null; // close ronde sebelumnya
let _mob_lastLock = null;  // lock ronde sebelumnya

function captureMobilePrediction() {
  const pred = _mobilePredSession;
  if (!pred || pred.prediction === "flat") {
    console.log("[MOBILE-PRED] skip capture: prediction is flat or no session");
    return;
  }
  
  const dur = INTERVAL_MS[state.interval];
   const now = serverNow();
   const t0 = Math.floor(now / dur) * dur;
    const O = sessionLock(state.asset, dur, now);
    const five = state.cache[state.asset]["5s"].candles;
    const last = five[five.length - 1];
  const C = last ? last.close : null;
  
  console.log("[MOBILE-PRED] capture check:", { _mob_t0, t0, _mob_pending: !!_mob_pending, predDir: pred.prediction });
  
  // boundary: ronde sebelumnya baru saja berakhir
  if (_mob_t0 !== null && _mob_t0 !== t0) {
    console.log("[MOBILE-PRED] session boundary detected:", _mob_t0, "->", t0);
    if (_mob_pending) {
      // Ambil nilai ACTUAL sesi sebelumnya dari 5m candle sesuai session start
      const prevStart = _mob_pending.t0;
      const prevDur = INTERVAL_MS[_mob_pending.interval] || dur;
      const prevStartSec = Math.floor(prevStart / 1000);
      const tfSec = prevDur / 1000;
      const candles5m = state.cache[_mob_pending.asset]["5m"].candles || [];
      const sessionCandles = candles5m.filter(c => Math.floor(c.time / tfSec) * tfSec === prevStartSec);
      const prevClose = sessionCandles.length ? sessionCandles[sessionCandles.length - 1].close : null;
      const prevLock = _mob_pending.lock || sessionLock(_mob_pending.asset, prevDur, prevStart);
      
      console.log("[MOBILE-PRED] evaluating previous session:", { prevStart, prevClose, prevLock });
      
      if (prevClose != null && prevLock != null) {
        const actual = prevClose >= prevLock ? "up" : "down";
        const won = _mob_pending.dir === actual ? 1 : 0;
        const entry = {
          ts: Date.now(),
          t0: _mob_pending.t0,
          asset: _mob_pending.asset,
          interval: _mob_pending.interval,
          mode: _mob_pending.mode,
          dir: _mob_pending.dir,
          conf: _mob_pending.conf,
          lock: prevLock,
          close: prevClose,
          actual,
          won,
        };
        MobilePredLog.add(entry);
        console.log("[MOBILE-PRED] logged:", entry);
        renderConfidenceReport();
      } else {
        console.log("[MOBILE-PRED] missing data for evaluation:", { prevClose, prevLock });
      }
    }
    _mob_pending = null;
  }
  
  _mob_t0 = t0;
  _mob_lastClose = C;
  _mob_lastLock = O;
  
  // Capture mobile prediction if not already captured for this session
  if (_mob_pending === null && pred && pred.prediction !== "flat") {
    _mob_pending = {
      t0,
      asset: state.asset,
      interval: state.interval,
      mode: pred.mode,
      dir: pred.prediction,
      conf: pred.confidence,
      lock: pred.lockPrice,
    };
    console.log("[MOBILE-PRED] captured prediction:", _mob_pending);
  }
}

function captureConfidenceRound(t0, O, C, fadeDir, fadeConf, trendDir, mode, state) {
  // boundary: ronde sebelumnya baru saja berakhir (t0 berubah)
  if (_cap_t0 !== null && t0 !== _cap_t0) {
    if (_cap_pending && _cap_lastClose != null && _cap_lastLock != null) {
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

function renderConfidenceReport() {
  const body = document.getElementById("conf-debug-body");
  const head = document.getElementById("conf-debug-head");
  const countEl = document.getElementById("conf-debug-count");
  if (!body) return;
  const allData = MobilePredLog.data();
  const data = allData.filter(r => r.interval === state.interval && r.asset === state.asset);
  const n = data.length;
    if (head) head.textContent = `MOBILE PREDICTION ACCURACY (${state.asset}/${state.interval}) · `;
    if (countEl) countEl.textContent = `${n} rounds`;
  
  if (!n) {
    body.innerHTML = `<div class="cd-empty">no evaluation data yet — let it run a few rounds</div>`;
    return;
  }

  const totW = data.reduce((a, r) => a + r.won, 0);
  const overall = (totW / n * 100).toFixed(1);
  
  const dirLetter = (d) => d === "up" ? "U" : "D";
  const dots = data.map(r => {
    const cls = r.won ? "dot-win" : "dot-lose";
    return `<span class="dot ${cls}" title="${r.dir.toUpperCase()} (${r.won ? 'BENAR' : 'SALAH'})">${dirLetter(r.dir)}</span>`;
  }).join('');
  
  body.innerHTML = `
    <div class="cd-row" style="margin-bottom:6px;">
      <span class="cd-b">TOTAL</span>
      <span class="cd-c">${n}</span>
      <span class="cd-wr">${overall}%</span>
      <span style="text-align:right;color:${overall >= 50 ? 'var(--up)' : 'var(--down)'}">
        ${data.reduce((a,r)=>a+r.won,0)}W / ${n - data.reduce((a,r)=>a+r.won,0)}L
      </span>
    </div>
    <div class="cd-dots" style="margin-top:6px;">${dots}</div>
  `;
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
        console.log("[MOBILE-PRED] restored from sessionStorage:", saved);
        // Seed boundary tracking so captureMobilePrediction() can detect next boundary
        _mob_t0 = saved.roundStart;
        _mob_pending = {
          t0: saved.roundStart,
          asset: saved.asset,
          interval: saved.interval,
          mode: saved.mode,
          dir: saved.prediction,
          conf: saved.confidence,
          lock: saved.lockPrice,
        };
      } else {
        // Session berbeda - evaluasi prediksi lama sebelum discard
        console.log("[MOBILE-PRED] stale/mismatched session in storage, evaluating:", saved);
        if (saved.prediction !== "flat") {
          const prevDur = INTERVAL_MS[saved.interval] || dur;
          const prevLock = saved.lockPrice || sessionLock(saved.asset, prevDur, saved.roundStart);
          const prevFive = state.cache[saved.asset]["5s"].candles || [];
          // Cari candle terakhir yang termasuk dalam sesi yang disimpan (bukan candle terakhir saat ini)
          const sessionEnd = saved.roundStart + prevDur;
          const sessionCandles = prevFive.filter(c => c.time * 1000 >= saved.roundStart && c.time * 1000 < sessionEnd);
          const prevClose = sessionCandles.length ? sessionCandles[sessionCandles.length - 1].close : null;
          
          if (prevClose != null && prevLock != null) {
            const actual = prevClose >= prevLock ? "up" : "down";
            const won = saved.prediction === actual ? 1 : 0;
            const entry = {
              ts: Date.now(),
              t0: saved.roundStart,
              asset: saved.asset,
              interval: saved.interval,
              mode: saved.mode,
              dir: saved.prediction,
              conf: saved.confidence,
              lock: prevLock,
              close: prevClose,
              actual,
              won,
            };
            MobilePredLog.add(entry);
            console.log("[MOBILE-PRED] logged restored session:", entry);
            renderConfidenceReport();
          }
        }
        sessionStorage.removeItem(MOBILE_PRED_SESSION_KEY);
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
         if (pred.prediction !== "flat") predCache[state.asset][cacheKey] = pred;
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
  
  // Track mobile prediction for accuracy logging
  captureMobilePrediction();
}

/* ----------------------- Controls ----------------------- */
function bindControls() {
  document.getElementById("asset-seg").addEventListener("click", (e) => {
    const b = e.target.closest("[data-asset]"); if (!b) return;
    state.asset = b.dataset.asset;
    _mob_t0 = null;
    _mob_pending = null;
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
    // Reset mobile prediction tracking karena interval berubah — jangan trigger boundary penilaian palsu
    _mob_t0 = null;
    _mob_pending = null;
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
  if (cdbg) cdbg.addEventListener("click", () => { MobilePredLog.clear(); renderConfidenceReport(); });
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
  renderConfidenceReport();
  // timers — use rAF for smooth timer, updateProjection only on data events
  requestAnimationFrame(updateTimerDisplay);
  setInterval(updateProjection, 5000);  // heavy projection update every 5s only

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

  window.addEventListener("resize", () => chart && chart.fit());
}
start();
