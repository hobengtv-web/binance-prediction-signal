/* ============================================================================
   TRADE-PLAN — MODUL BERSAMA (server + browser) untuk Trade Assistant.

   Semua keputusan Trade Assistant (kapan entry, tambah, hold, close) dihitung di SINI.
   Server memakai modul ini untuk memproses plan tiap tick lalu mengirimkannya lewat
   /api/live; browser memakai modul yang sama sebagai fallback bila snapshot server
   tidak tersedia (offline / stream putus). Fungsi-fungsi di bawah diambil PERSIS dari
   app.js (bukan ditulis ulang) supaya tidak ada dua versi logika yang bisa berbeda.

   Kontrak utama:
     buildPlan(input) -> plan {state, action, cls, cmd, tradeDir, levels, fs, cp, cont,
                               adverseStd, favor, why, entered, entryPrice,
                               statusEntry{ok,at,price}, statusClose{ok,at,price}}
   ============================================================================ */
(function (root, factory) {
  if (typeof module === "object" && module.exports) module.exports = factory();
  else root.TradePlan = factory();
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

function fmtPrice(n) {
  if (n == null || isNaN(n)) return "—";
  const d = n >= 1000 ? 2 : n >= 1 ? 3 : 5;
  return Number(n).toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d });
}

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

function continuationOf(tiers, tf, cp, isUp, price) {
  const t = (tiers && tiers.continuation && tiers.continuation.tiers) ? tiers.continuation.tiers[tf] : null;
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
  const cont = favor >= 0 ? continuationOf(ctx.tiers, ctx.tf, cp, isUp, ctx.price) : null;
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
      const _T = ctx.tiers || null;
      const exAll = (_T && _T.locktouch && _T.locktouch.exitTargets) ? _T.locktouch.exitTargets : null;
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


/* ============================================================================
   buildPlan(input) — SATU pintu masuk: dari data pasar mentah -> plan + status.
   input = {
     bias, tf, lock, price, std, slope, slopeRecent, rsi, z,
     ofi, ofiShort, histTrend, win (candle 5s), sessionCloses (1s, sesi berjalan),
     key, now, tiers, state {peak:{}, dwell:{}, entered:{}, closed:{}}
   }
   Mengembalikan plan ATAU null bila tidak ada bias. State sesi (peak/dwell/entered/
   closed) dimutasi di dalam `state` — server menyimpannya per (asset, tf, sesi).
   ============================================================================ */
function buildPlan(input) {
  const bias = input.bias;
  if (bias !== "up" && bias !== "down") return null;
  const win = input.win || [];
  if (win.length < 5) return null;
  const isUp = bias === "up";
  const state = input.state || (input.state = { peak: {}, dwell: {}, entered: {}, closed: {} });
  const key = input.key;
  const now = input.now;
  const C = input.price, O = input.lock;
  if (C == null || O == null) return null;
  const std = input.std || 0, slope = input.slope, slopeRecent = input.slopeRecent;
  const rsi = input.rsi, z = input.z;
  const ofi = input.ofi, ofiShort = input.ofiShort;
  const histTrend = input.histTrend || null;

  // kontribusi volume dari candle yang melawan arah bias (dipakai health)
  const recentC = win.slice(-12);
  const counterVol = recentC.filter((c) => (isUp ? c.close < c.open : c.close > c.open)).reduce((a, c) => a + (c.vol || 0), 0);
  const totVol = recentC.reduce((a, c) => a + (c.vol || 0), 0);
  const volAgainst = totVol > 0 ? (counterVol / totVol) / 0.5 : null;

  // Arah TRADE (contra-lock) = arah rekomendasi; semua input TA memakai arah ini.
  const taUp = isUp;
  const favor = taUp ? (C - O) : (O - C);
  const peakFavor = Math.max(state.peak[key] == null ? -Infinity : state.peak[key], favor);
  state.peak[key] = peakFavor;
  const retreat = peakFavor > 0 && (peakFavor - favor) >= 0.25 * Math.max(std, 1e-9);

  const health = computeSignalHealth(bias, {
    margin: isUp ? (C - O) : (O - C),
    marginStd: std > 0 ? Math.abs(C - O) / std : null,
    slope, slopeRecent, ofi, ofiShort, volAgainst, rsi, z, histTrend,
  });
  const turn = turnEvidence(taUp, { slope, slopeRecent, ofiShort, win });
  const fade = fadeEvidence(taUp, { slope, slopeRecent, ofiShort, retreat, rsi });
  const dw = state.dwell[key] || (state.dwell[key] = { turnSince: null, fadeSince: null });
  dw.turnSince = turn.count >= 2 ? (dw.turnSince || now) : null;
  dw.fadeSince = fade.count >= 2 ? (dw.fadeSince || now) : null;
  const entered = !!(state.entered[key] && state.entered[key].entered);
  const trail = trailOfCloses(input.sessionCloses || [], O, taUp);
  const plan = computeTradePlan(bias, {
    tf: input.tf, lock: O, price: C, std, slope, slopeRecent, rsi, z, ofi, ofiShort, retreat, health,
    entered, turn, fade, trail,
    dwellTurnMs: dw.turnSince ? now - dw.turnSince : 0,
    dwellFadeMs: dw.fadeSince ? now - dw.fadeSince : 0,
    histTrend, tiers: input.tiers || null,
  });
  if (!plan) return null;

  // STATUS ENTRY / EARLY CLOSE (state dibagi per sesi; penjaga mencegah pencatatan ganda)
  if (plan.entered && !state.entered[key]) {
    state.entered[key] = { entered: true, since: now, price: C };
  }
  if (!plan.entered) { delete state.entered[key]; delete state.closed[key]; }
  if (plan.state === "CLOSE" && !state.closed[key]) {
    state.closed[key] = { at: now, price: C };
  }
  const _ent = state.entered[key], _clo = state.closed[key];
  plan.entryPrice = _ent ? _ent.price : null;
  plan.statusEntry = {
    ok: !!_ent, at: _ent ? _ent.since : null, price: _ent ? _ent.price : null,
    waiting: plan.state === "STAND_DOWN" ? "reversal terdeteksi — tunggu setup baru"
      : (plan.state === "WAIT" && /TUNGGU PEAK/.test(plan.action) ? "konfirmasi peak contra (2/4 bagian + 4s)" : "harga belum contra / belum kembali ke lock"),
  };
  plan.statusClose = { ok: !!_clo, at: _clo ? _clo.at : null, price: _clo ? _clo.price : null };
  plan.health = health;
  return plan;
}

/* trailOf versi SERVER: dari deret close 1s sesi berjalan (klien memakai state DOM,
   tapi rumusnya sama: SMA-14 puncak + exit 0.01% di belakangnya). */
function trailOfCloses(closes, lock, isUp) {
  if (!closes || closes.length < 5) return null;
  if (closes.some((v) => typeof v !== "number" || !isFinite(v))) return null;
  const sma = [];
  for (let i = 0; i < closes.length; i++) {
    const w = closes.slice(Math.max(0, i - 14), i + 1);
    sma.push(w.reduce((x, y) => x + y, 0) / w.length);
  }
  const smaNow = sma[sma.length - 1];
  const smaPeak = isUp ? Math.max.apply(null, sma) : Math.min.apply(null, sma);
  const exitPrice = isUp ? smaPeak * (1 - 0.0001) : smaPeak * (1 + 0.0001);
  return { smaNow, smaPeak, exitPrice, armed: isUp ? smaNow >= lock : smaNow <= lock };
}

/* ===== Statistik jendela (DIAMBIL PERSIS dari app.js) =====
   Dipakai server untuk menghitung std/slope/z dari window candle 5s yang sama dengan
   yang dipakai klien, supaya angka plan tidak berbeda karena rumus yang berbeda. */
const MON_WINDOW = { "5m": 24, "15m": 60, "1h": 120 };

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
/* Ringkasan window: mean/std/z/slope/slopeRecent — sama urutannya dengan analyzeCoin(). */
function statsOfWindow(win) {
  const closes = (win || []).map((c) => c.close);
  const C = closes.length ? closes[closes.length - 1] : null;
  const mean = meanOf(closes), std = stdOf(closes);
  const z = std > 0 && C != null ? (C - mean) / std : 0;
  const slope = slopeOf(win);
  const seg = (win || []).slice(-Math.max(3, Math.ceil((win || []).length / 3)));
  const slopeRecent = slopeOf(seg);
  return { C, mean, std, z, slope, slopeRecent };
}

return { buildPlan, computeTradePlan, statsOfWindow, MON_WINDOW, slopeOf, meanOf, stdOf, turnEvidence, fadeEvidence, computeSignalHealth,
         trailOfCloses, continuationOf, fmtPrice, partList,
         DWELL_ENTRY_MS, DWELL_AVG_MS, DWELL_CLOSE_MS };
});
