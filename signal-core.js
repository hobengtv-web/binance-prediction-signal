/* Shared signal core — used by the browser app (app.js) and the Node backtest.
   Pure functions only: no DOM, no globals. Keep in sync with live rules. */
(function (root, factory) {
  if (typeof module === "object" && module.exports) module.exports = factory();
  else root.SignalCore = factory();
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

  const INTERVAL_MS = { "5m": 300000, "15m": 900000, "1h": 3600000 };

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

  function sessionTrend(candles, n) {
    if (!candles || candles.length < 2) return "flat";
    const last = candles.slice(0, -1).slice(-n);
    let bull = 0, bear = 0;
    for (const c of last) {
      const d = c.close - c.open;
      if (d > 0) bull++; else if (d < 0) bear++;
    }
    const need = Math.ceil(n / 2);
    if (bull > bear && bull >= need) return "bullish";
    if (bear > bull && bear >= need) return "bearish";
    return "flat";
  }

  function analyzeHistoricalTrend(candles, sessionCount) {
    if (!candles || candles.length < sessionCount + 1) {
      return { dir: "flat", strength: 0, momentum: false, predictDir: "flat" };
    }
    const recent = candles.slice(0, -1).slice(-sessionCount);
    let bull = 0, bear = 0;
    const totalWeight = (sessionCount * (sessionCount + 1)) / 2;
    for (let i = 0; i < recent.length; i++) {
      const c = recent[i];
      const d = c.close - c.open;
      const weight = (i + 1) / totalWeight;
      if (d > 0) bull += weight; else if (d < 0) bear += weight;
    }
    const strength = Math.abs(bull - bear) * 100;
    const dir = bull > bear ? "up" : bear > bull ? "down" : "flat";
    const momentum = Math.abs(bull - bear) > 0.4;
    const last3 = recent.slice(-3);
    const prev3 = recent.slice(-6, -3);
    let lastBull = 0, prevBull = 0;
    for (const c of last3) { if (c.close > c.open) lastBull++; else if (c.close < c.open) lastBull--; }
    for (const c of prev3) { if (c.close > c.open) prevBull++; else if (c.close < c.open) prevBull--; }
    const predictDir = (dir === "down" && lastBull > 0 && lastBull > prevBull) ? "up" :
                       (dir === "up" && lastBull < 0 && Math.abs(lastBull) > Math.abs(prevBull)) ? "down" : dir;
    return { dir, strength: Math.round(strength), momentum, predictDir };
  }

  // Single source of truth for entry decision (mirrors app.js calculateUniversalSignal).
  // Calibrated on 90d walk-forward: only TREND (5m/15m) and MOMENTUM (fallback) are
  // predictive. HIST-PREDICT (43%) and REVERSAL (19-50%) were removed — they lost money.
  function decideSignal(input) {
    const { tf, elapsed, currentDir, volRel } = input;
    const isHighFreq = tf === "5m" || tf === "15m";
    // Direction gate only (the real entry gate is the grade + liquidity). 5m was lowered
    // to 0.5x per request; 15m/1h keep 1.05x.
    const volOK = tf === "5m" ? volRel >= 0.5 : volRel >= 1.05;
    let verdict = "flat", mode = "CONT", conf = 0;

    if (isHighFreq && volOK && currentDir !== "flat") {
      verdict = currentDir; mode = "TREND"; conf = 65;
    } else if (elapsed >= 30000 && volOK && currentDir !== "flat") {
      verdict = currentDir; mode = "MOMENTUM"; conf = 60;
    } else {
      verdict = "flat";
      if (elapsed < 15000) mode = "WARMUP";
      else if (!volOK) mode = "LOWVOL";
      else mode = "FILTERED";
    }
    return { verdict, mode, conf };
  }

  function wilsonLowerBound(wins, n, z) {
    z = z || 1.96;
    if (!n) return 0;
    const p = wins / n;
    const d = 1 + (z * z) / n;
    const c = p + (z * z) / (2 * n);
    const m = z * Math.sqrt((p * (1 - p)) / n + (z * z) / (4 * n * n));
    return (c - m) / d;
  }

  /* Plain-text, symbol-free reason templates.
     Single source of truth for the wording shown to the user. */
  const MODE_LABEL = {
    "HIST-PREDICT": "historical trend",
    "TREND": "trend",
    "REVERSAL↑": "reversal up",
    "REVERSAL↓": "reversal down",
    "MOMENTUM": "momentum",
    "CLOSE": "close",
    "CONT": "continuation",
    "WARMUP": "warmup",
    "LOWVOL": "low volume",
    "WEAK-TREND": "weak trend",
    "FILTERED": "filtered",
    "FILTERED-REVERSAL": "filtered reversal",
    "BLOCKED-GOAL": "blocked by goal policy",
    "MENUNGGU": "waiting",
    "NO-SIGNAL": "no signal",
  };

  // Catalog of every case the engine can produce, for reference/tests.
  const REASON_CASES = {
    entry: [
      "HIST-PREDICT up/down",
      "TREND up/down (5m and 15m only)",
      "REVERSAL up (up only)",
      "REVERSAL down (down only)",
      "MOMENTUM up/down",
      "CLOSE up/down (desktop near settlement)",
    ],
    noEntry: [
      "MENUNGGU (session just started, no candle yet)",
      "WARMUP (elapsed below 15 seconds, or desktop below 180 seconds)",
      "LOWVOL (volume below the interval minimum: 1.2x 5m / 2x 15m / 1.5x 1h)",
      "WEAK-TREND (trend strength below 35, volume below minimum)",
      "FILTERED (volume ok but direction not aligned)",
      "FILTERED-REVERSAL (reversal not confirmed by peak/trend/volume/RSI)",
      "BLOCKED-GOAL (continuation suppressed by reversal-only policy)",
      "CONT (continuation direction not qualified)",
      "CLOSE (near settlement without qualifying direction)",
    ],
  };

  function buildReason(input) {
    const verdict = input.verdict;
    const mode = input.mode;
    const rsi = input.rsi;
    const volRel = input.volRel;
    const strength = input.strength;
    const momentum = input.momentum;
    const elapsedSec = input.elapsedSec;
    const tf = input.tf;
    const volMin = (input.volMin == null) ? 1.05 : input.volMin;

    const rsiTxt = (rsi == null) ? "not available" : rsi.toFixed(1);
    const volTxt = (volRel == null || !isFinite(volRel)) ? "not available" : volRel.toFixed(2) + "x";
    const strTxt = (strength == null) ? "0" : String(strength);
    const secs = (elapsedSec == null) ? 0 : Math.round(elapsedSec);

    if (verdict === "up" || verdict === "down") {
      const DIR = verdict.toUpperCase();
      const aboveBelow = verdict === "up" ? "above" : "below";
      switch (mode) {
        case "HIST-PREDICT":
          return `Entry ${DIR}. The last 50 completed sessions lean ${verdict}, directional strength ${strTxt} of 100` +
            (momentum ? ", momentum confirmed" : "") + `. RSI ${rsiTxt}. Volume (5m pace) ${volTxt}.`;
        case "TREND":
          return `Entry ${DIR}. Price is already ${aboveBelow} the session open in the first minute, with volume (5m pace) ${volTxt}.`;
        case "REVERSAL↑":
          return `Entry UP. RSI ${rsiTxt} is oversold and the first candle of the session was bullish, with volume (5m pace) ${volTxt}.`;
        case "REVERSAL↓":
          return `Entry DOWN. RSI ${rsiTxt} is overbought and the first candle of the session was bearish, with volume (5m pace) ${volTxt}.`;
        case "MOMENTUM":
          return `Entry ${DIR}. After warmup, price is ${aboveBelow} the session open with volume (5m pace) ${volTxt}.`;
        case "CLOSE":
          return `Entry ${DIR}. Near settlement, price is ${aboveBelow} the session open.`;
        default:
          return `Entry ${DIR}. Signal mode ${MODE_LABEL[mode] || mode}.`;
      }
    }

    switch (mode) {
      case "MENUNGGU":
        return "No entry. Waiting for the first candle of the new session to form.";
      case "WARMUP":
        return `No entry. Warmup in progress: ${secs} seconds elapsed, the minimum is 15 seconds.`;
      case "LOWVOL":
        return `No entry. Volume (5m pace) ${volTxt} is below the ${volMin}x minimum for the ${tf || "this"} session, the market is too thin to trade.`;
      case "WEAK-TREND":
        return `No entry. No clear historical trend: strength ${strTxt} of 100 is below 35, and volume is ${volTxt}.`;
      case "FILTERED":
        return `No entry. Conditions not aligned: trend strength ${strTxt}, RSI ${rsiTxt}, volume ${volTxt}.`;
      case "FILTERED-REVERSAL":
        return "No entry. A reversal was detected but not confirmed by peak confidence, trend strength, volume spike, or RSI extreme.";
      case "BLOCKED-GOAL":
        return "No entry. This is a trend continuation but the current policy allows reversals only.";
      case "CONT":
        return "No entry. Continuation direction was filtered out.";
      case "CLOSE":
        return "No entry. Near settlement without a qualifying direction.";
      default:
        return `No entry. Signal mode ${MODE_LABEL[mode] || mode}.`;
    }
  }

  return { INTERVAL_MS, rsiFromSeries, sessionTrend, analyzeHistoricalTrend, decideSignal, wilsonLowerBound, buildReason, REASON_CASES, MODE_LABEL };
});
