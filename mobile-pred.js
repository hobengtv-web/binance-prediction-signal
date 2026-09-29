/* ============================================================================
   MOBILE-PRED — model prediksi arah sesi (dulu hanya dihitung di browser).

   Dipindah ke modul bersama supaya SERVER yang menghitung, klien hanya menampilkan.
   Fungsi predictSessionStart diambil PERSIS dari app.js (diextract) dan hanya diubah
   bentuk masukannya (menerima input eksplisit: harga, lock, candle 5m, candle 1s, durasi),
   bukan membaca state/DOM.

   Keluaran: { roundStart, tf, lockPrice, prediction, confidence, mode, price, delta, trendBias }
   ============================================================================ */
(function (root, factory) {
  if (typeof module === "object" && module.exports) module.exports = factory();
  else root.MobilePred = factory();
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

function predictSessionStart(input) {
  const tf = input.tf;
  const dur = input.durMs;
  const now = input.now;
  const C = input.price;
  const lockPrice = input.lockPrice;
  const curStart = Math.floor(now / dur) * dur;
  const curStartSec = Math.floor(curStart / 1000);
  const ticker = { last: C };
  if (C == null || lockPrice == null) return null;
  // Ambil candle 5m langsung dari cache (sudah ada dari loadHistory)
  const candles5m = input.candles5m || [];
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
  const ones = input.ones || [];
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
    tf,
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
  return { predictSessionStart };
});
