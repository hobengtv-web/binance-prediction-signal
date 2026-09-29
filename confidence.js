/* ============================================================================
   CONFIDENCE — MODUL BERSAMA (server + browser) untuk model keyakinan arah.

   Model ini sebelumnya hanya hidup di dalam updateProjection() di app.js, sehingga angka
   LED/panel confidence dihitung di device masing-masing. Sekarang dihitung di SERVER dan
   dikirim lewat /api/live (assets[sym].all[tf].conf), lalu klien hanya MEMILIH angka yang
   sesuai tab (SIGNAL/UP/DOWN) — tidak lagi menjalankan model.

   Fungsi di bawah diambil PERSIS dari app.js (diextract) dan hanya diubah bentuk
   parameternya (menerima ctx/candles, bukan membaca `state`).

   confidenceFor(input) mengembalikan, untuk tiap arah:
     { live, past, align }  -> live/past = nilai dua basis model, align = arah searah trend
   Klien memilih: past bila (align atau dikunci ke tab SIGNAL), selain itu live.
   ============================================================================ */
(function (root, factory) {
  if (typeof module === "object" && module.exports) module.exports = factory();
  else root.Confidence = factory();
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

  // SignalCore: di Node lewat require, di browser sudah menjadi global (script signal-core.js).
  const SignalCore = (typeof module === "object" && typeof require === "function")
    ? require("./signal-core.js")
    : (root.SignalCore || null);

  const TREND_SESSIONS = 3;
  const VOL_TYPICAL = { BTC: 0.515, ETH: 8.22 };

function avg(a) { return a.reduce((x, y) => x + y, 0) / a.length; }

function stdev(a) { const m = avg(a); return Math.sqrt(avg(a.map((x) => (x - m) * (x - m)))); }

function clamp(v, a, b) { return Math.max(a, Math.min(b, v)); }

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

function percentile(arr, p) {
    if (!arr.length) return 0;
    const s = arr.slice().sort((a, b) => a - b);
    const idx = Math.min(s.length - 1, Math.max(0, Math.floor((p / 100) * (s.length - 1))));
    return s[idx];
  }

function volDistribution(five, winLen) {
    const N = 180; // ~15 menit (candle 5s)
    const start = Math.max(0, five.length - N - winLen);
    return five.slice(start, five.length - winLen)
      .map((c) => c.vol || 0)
      .filter((v) => v > 0);
  }

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

function sessionTrend(tfCandles, n) {
  // rumus sama dengan SignalCore.sessionTrend; pakai SignalCore bila ada supaya identik
  if (SignalCore && SignalCore.sessionTrend) return SignalCore.sessionTrend(tfCandles, n);
  return trendOfCandles((tfCandles || []).slice(0, -1).slice(-n));
}

function confidenceFromPastSessions(tfCandles, dn) {
  // Guard: cache candle bisa belum terisi saat render pertama -> kembalikan 0, bukan crash.
  const candles = tfCandles || [];
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

function confForDir(ctx, dir) {
      if (!dir) return 0;
      const dn = dir === "down";
      const alignThis = !!ctx.curTrendDir && ctx.curTrendDir !== "flat" && ctx.curTrendDir === dir;
      let base;
      // Tab SIGNAL yang sync mobile prediction: gunakan analisis 3-4 sesi sebelumnya
      if (alignThis || (ctx.mobLocked && ctx.confMode === "SIGNAL")) {
        base = confidenceFromPastSessions(ctx.tfCandles, dn);
      } else {
        // BERLAWANAN trend (counter-trend): keyakinan dasar dari SESI AKTIF saat ini (live 5s window).
        base = 0;
        // 1) pola candle / struktur peak (paling berbobot)
        if (dn ? ctx.isTopPeak : ctx.isBotPeak) {
          if (ctx.peakConf) base += 25;                                  // 2-3 candle konfirmasi pasca-peak
          if (dn ? ctx.droppedFromPeak : ctx.roseFromPeak) base += 15;       // harga sudah menjauh dr peak
          else if (dn ? ctx.nearTop : ctx.nearBot) base += 8;               // msh persis di ujung peak
        }
        // 2) indikator "stretch" (RSI + z-score) — digabung jadi 1 score 0..1 agar tak double-count
        const zComp = (dn ? ctx.overbought : ctx.oversold) ? 1 : (dn ? ctx.z > 1.2 : ctx.z < -1.2) ? 0.5 : 0;
        let rsiComp = 0;
        if (ctx.rsi != null) rsiComp = (dn ? ctx.rsi >= 70 : ctx.rsi <= 30) ? 1 : (dn ? ctx.rsi >= 60 : ctx.rsi <= 40) ? 0.5 : 0;
        const stretch = Math.min(1, (zComp + rsiComp) / 2);   // 0..1
        base += stretch * 25;
        // 3) momentum berbalik (slope)
        if (dn ? ctx.rollOver : ctx.turnUp) base += 12;
        // 4) TREND (akumulasi 3 sesi interval aktif) — searah fade -> boost, berlawanan -> penalti
        const sTrend = sessionTrend(ctx.tfCandles, TREND_SESSIONS);
        if (sTrend === (dn ? "bearish" : "bullish")) base += 10;
        else if (sTrend === (dn ? "bullish" : "bearish")) base -= 10;
        // 5) likuiditas / volume — tipis = berisiko
        if (ctx.liquidity === "LOW") base -= 12;
        else if (ctx.liquidity === "THIN") base -= 6;
        if (ctx.hasVolData && ctx.rel >= 1) base += 5;
      }

      // FEASIBILITAS: peluang harga mencapai target (LOCK / OPEN) — murni DERIVED dari data live,
      // TANPA ambang waktu hardcode. Saat remSec -> 0, jangkauan volMove -> 0 -> reach -> 0 otomatis
      // & realtime tiap tick (recompute di updateProjection yg jalan tiap 1 detik).
      const adverse = dn ? Math.max(0, ctx.C - ctx.O) : Math.max(0, ctx.O - ctx.C);
      const remSec = Math.max(0, ctx.remainingMs / 1000);
      const driftToward = dn ? -ctx.slope * remSec : ctx.slope * remSec;   // proyeksi gerak ke arah target (+ = bagus)
      const residual = Math.max(0, adverse - Math.max(0, driftToward));
      let reach;
      if (residual <= 0) {
        reach = 1;                                       // drift akan bawa ke target
      } else {
        const volMove = ctx.std * (remSec / 5) * 2;          // jangkauan sisa waktu (linier thd sisa detik)
        reach = clamp(1 - residual / Math.max(volMove, 1e-9), 0, 1);
      }
      return clamp(Math.round(base * reach), 0, 100);
    }

/* ============================================================================
   confidenceFor(input) — SATU pintu masuk untuk server.
   input = {
     sym, tf, C, O, std, slope, rsi, z, remainingMs, win (candle 5s), five (deret 5s),
     tfCandles (candle tf: 5m/15m/1h), volTypical
   }
   Mengembalikan objek berisi angka untuk arah up/down + konteks (trend, likuiditas).
   ============================================================================ */
function confidenceFor(input) {
  const win = input.win || [];
  if (win.length < 3) return null;
  const C = input.C, O = input.O, std = input.std || 0, slope = input.slope || 0;
  const winLen = win.length;
  const winVol = win.reduce((a, c) => a + (c.vol || 0), 0);
  const five = input.five || [];
  const trailC = five.slice(-(60 + winLen), -winLen);
  const useTrail = trailC.length >= 10 ? trailC : five.slice(-60);
  const baseMean = avg(useTrail.map((c) => c.vol || 0));
  const hasVolData = baseMean > 0 || winVol > 0;
  const baseWinVol = baseMean * winLen;
  const volRel = baseWinVol > 0 && hasVolData ? winVol / baseWinVol : 1;
  const typ = input.volTypical != null ? input.volTypical : VOL_TYPICAL[input.sym];
  const winVolPerCandle = winLen ? winVol / winLen : 0;
  const absRel = typ ? winVolPerCandle / typ : 1;
  const rel = hasVolData ? Math.min(volRel, absRel) : 1;
  let liquidity = "NORMAL";
  if (hasVolData) {
    const series = volDistribution(five, winLen);
    const lowThresh = Math.min(Math.max(percentile(series, 15), typ * 0.3), typ * 1.5);
    const thinThresh = Math.min(Math.max(percentile(series, 40), typ * 0.6), typ * 2.5);
    if (winVolPerCandle < lowThresh) liquidity = "LOW";
    else if (winVolPerCandle < thinThresh) liquidity = "THIN";
  } else liquidity = "—";

  // peak + konfirmasi 2-3 candle (logika sama dengan updateProjection)
  const tol = Math.max(O * 0.001, std * 0.5);
  const winHigh = Math.max.apply(null, win.map((c) => c.high));
  const winLow = Math.min.apply(null, win.map((c) => c.low));
  const sw = detectSwings(win, input.swingLookback || 4);
  const lastH = sw.highs[sw.highs.length - 1], lastL = sw.lows[sw.lows.length - 1];
  let peak = null;
  if (lastH && lastL) peak = (lastH.time >= lastL.time) ? { price: lastH.price, dir: "top", time: lastH.time } : { price: lastL.price, dir: "bot", time: lastL.time };
  else if (lastH) peak = { price: lastH.price, dir: "top", time: lastH.time };
  else if (lastL) peak = { price: lastL.price, dir: "bot", time: lastL.time };
  const isTopPeak = !!(peak && peak.dir === "top" && peak.price >= winHigh - 1e-6);
  const isBotPeak = !!(peak && peak.dir === "bot" && peak.price <= winLow + 1e-6);
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
  const trend = sessionTrend(input.tfCandles, TREND_SESSIONS);
  const curTrendDir = trend === "bullish" ? "up" : trend === "bearish" ? "down" : "flat";
  const ctx = {
    sym: input.sym, tf: input.tf, C, O, std, slope, rsi: input.rsi, z: input.z,
    remainingMs: input.remainingMs, tfCandles: input.tfCandles || [],
    isTopPeak, isBotPeak, peakConf,
    nearTop: isTopPeak && Math.abs(C - peak.price) < tol,
    nearBot: isBotPeak && Math.abs(C - peak.price) < tol,
    droppedFromPeak: isTopPeak && C <= peak.price - tol,
    roseFromPeak: isBotPeak && C >= peak.price + tol,
    overbought: input.z > 1.6, oversold: input.z < -1.6,
    rollOver: slope < 0, turnUp: slope > 0,
    liquidity, hasVolData, rel, curTrendDir,
    mobLocked: false, confMode: "SIGNAL",   // nilai netral: klien yang memilih basisnya
  };
  // Dua varian per arah (keduanya SUDAH termasuk faktor feasibilitas/reach, sama seperti
  // confForDir asli):
  //   value = basis yang dipilih model berdasarkan keselarasan trend (align),
  //   past  = basis "3 sesi sebelumnya" (dipakai bila UI sedang terkunci ke tab SIGNAL).
  // Klien hanya memilih: (align || tab SIGNAL terkunci) ? past : value.
  const ctxPlain = Object.assign({}, ctx, { mobLocked: false, confMode: "SIGNAL" });
  const ctxPast  = Object.assign({}, ctx, { mobLocked: true,  confMode: "SIGNAL" });
  return {
    up:   { value: confForDir(ctxPlain, "up"),   past: confForDir(ctxPast, "up"),   align: curTrendDir === "up" },
    down: { value: confForDir(ctxPlain, "down"), past: confForDir(ctxPast, "down"), align: curTrendDir === "down" },
    trendDir: curTrendDir, liquidity, rel, hasVolData,
    peak: peak ? { dir: peak.dir, price: peak.price, conf: peakConf } : null,
  };
}

return { confidenceFor, confForDir, confidenceFromPastSessions, sessionTrend, trendOfCandles,
         detectSwings, percentile, volDistribution, linreg, avg, stdev, clamp, TREND_SESSIONS, VOL_TYPICAL };
});
