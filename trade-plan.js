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
  // PERBAIKAN (permintaan user): konfirmasi berbasis ARUS harus BERTAHAN (dwell >= 10 detik),
  // supaya lonjakan OFI sesaat tidak langsung dinyatakan "SUDAH BERBALIK" dan memicu CUT.
  // Konfirmasi struktural (peak lawan terkonfirmasi / tren historis berbalik) tidak butuh dwell.
  const flowPersist = ctx.ofiAgainstMs == null ? true : ctx.ofiAgainstMs >= 10000;
  const confirmedReversal = ctx.margin != null && ctx.margin < 0 &&
    (((ofiAgainst || ofiShortAgainst) && flowPersist) || !!ctx.peakAgainst || histAgainst);
  let label;
  if (confirmedReversal) label = "SUDAH BERBALIK";
  else if (score >= 75) label = "HAMPIR PASTI BERBALIK";
  else if (score >= 55) label = "WASPADA BERBALIK ARAH";
  else if (score >= 30) label = "AWAS MELEMAH";
  else label = "MASIH SESUAI";
  return { score, label, fired, confirmedReversal };
}

const TA = require("./ta-config.js");
const DWELL_ENTRY_MS = TA.DWELL_ENTRY_MS;    // entry: peak/turn hold (turun dari 4s agar tidak telat)
const ENTRY_RETRACE_PCT = TA.ENTRY_RETRACE_PCT; // entry peak: retrace minimal dari harga EKSTREM contra (%)
const ENTRY_MIN_EXTREME_PCT = TA.ENTRY_MIN_EXTREME_PCT; // kedalaman ekstrem contra minimal sebelum retrace-entry
const DWELL_AVG_MS = TA.DWELL_AVG_MS;     // averaging: hold (be more careful adding)
const DWELL_CLOSE_MS = TA.DWELL_CLOSE_MS;   // close: fade must hold
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
  const hRisk = h.label === "HAMPIR PASTI BERBALIK" || h.label === "SUDAH BERBALIK";
  // ===== KEDALAMAN POSISI (dasar keputusan CUT) =====
  // Diukur dari HARGA ENTRY (P&L posisi), bukan dari LOCK. Untuk sinyal UP, entry memang di
  // BAWAH lock (zona contra) -> "harga di sisi lawan lock" BUKAN risiko selama harga masih di
  // dalam zona entry. CUT hanya bila posisi sudah benar-benar dalam (>= 0.5 sigma dari entry)
  // ATAU rugi sudah >= 1.5 sigma dari harga entry.
  const entPx = ctx.entryPrice != null ? ctx.entryPrice : null;
  const adverseFromEntry = entPx != null ? (isUp ? (entPx - ctx.price) : (ctx.price - entPx)) : null;
  const cutAdverseStd = (adverseFromEntry != null && sigma > 0) ? adverseFromEntry / sigma : null;
  const l3px = levels.l3;
  const beyondZone = isUp ? (ctx.price < l3px) : (ctx.price > l3px);
  const deepEnough = beyondZone || (cutAdverseStd != null && cutAdverseStd >= 1.5);
  const biasAtRisk = hRisk && deepEnough;
  // ===== PROGRESS MENUJU TARGET & ATURAN PROFIT MINIMAL (permintaan user) =====
  // captured = berapa % dari jarak entry->LOCK yang sudah ditempuh (0..1+). User hanya mau
  // early close bila profit sudah >= 50% dari target, KECUALI kondisi darurat (reversal/arus kuat).
  const pnlPct = entPx != null ? (((isUp ? (ctx.price - entPx) : (entPx - ctx.price)) / entPx) * 100) : null;
  const pnlTxt = pnlPct != null ? `${pnlPct >= 0 ? "+" : ""}${pnlPct.toFixed(3)}%` : "";
  const hWhy = (h.fired && h.fired.length) ? h.fired.join(", ") : "arus melawan";
  const histFlippedNow = !!ctx.histTrend && ctx.histTrend.predictDir !== "flat" && ctx.histTrend.predictDir !== bias && ctx.histTrend.strength >= 35;
  // NOTE: "price is contra the lock" is the ENTRY OPPORTUNITY in this workflow, not a risk —
  // so the health label (built for managing an open position) must NOT veto PHASE 1.
  // The only genuine reason to hold back is a real reversal: higher-tf trend flipped against
  // the bias AND strong opposing flow, while the move is still going against us.
  const histFlipped = !!ctx.histTrend && ctx.histTrend.predictDir !== "flat" && ctx.histTrend.predictDir !== bias && ctx.histTrend.strength >= 35;
  const ofiStrongAgainst = ctx.ofi != null && (isUp ? ctx.ofi < -0.25 : ctx.ofi > 0.25);
  const realReversal = histFlipped && ofiStrongAgainst;
  const _simple = !!TA.SIMPLE;   // mode test: entry depth+retrace, exit trailing saja
  // Evidence the move against the bias is about to turn back toward it.
  // Confirmation: >=2 independent evidence parts AND a minimum dwell time, so a single
  // noisy tick cannot trigger (too fast) and waiting never drags on (too late).
  const turn = ctx.turn || { count: 0, parts: {} };
  // ===== GATE WAKTU ENTRY (permintaan user) =====
  // Sinyal UP dengan harga masih contra (di bawah LOCK) tetapi sisa sesi mepet -> JANGAN entry,
  // karena tidak ada cukup waktu untuk harga mencapai LOCK. Pengecualian: reversal EKSTREM
  // (>=3/4 bukti pembalikan DAN arus pendek kuat searah bias) yang bisa langsung melewati LOCK.
  const durMsNow = (ctx.durMs != null) ? ctx.durMs : ((DUR_SEC[ctx.tf] || 300) * 1000);
  const remainSecNow = (ctx.remainMs != null) ? ctx.remainMs / 1000 : null;
  const durSecNow = durMsNow / 1000;
  const minRemainSec = Math.min(300, Math.max(TA.ENTRY_MIN_REMAIN_SEC, 0.35 * durSecNow));   // 5m->120s · 15m/1h->300s
  const distToLockPct = Math.abs(ctx.lock - ctx.price) / (ctx.price || 1) * 100;
  const ofiTowardStrong = ctx.ofiShort != null && (isUp ? ctx.ofiShort > 0.25 : ctx.ofiShort < -0.25);
  const extremeReversal = turn.count >= 3 && ofiTowardStrong;
  const timeTooShort = remainSecNow != null && remainSecNow < minRemainSec;
  const blockLateEntry = timeTooShort && !extremeReversal;
  // Menambah posisi (L2/L3) TIDAK punya pengecualian: menambah di detik-detik akhir sesi tetap
  // berisiko (kasus nyata: sisa <1 menit & harga stabil di contra -> berakhir loss).
  const blockLateAdd = remainSecNow != null && remainSecNow < minRemainSec;
  const fade = ctx.fade || { count: 0, parts: {} };
  const dwellTurn = ctx.dwellTurnMs || 0;
  const dwellFade = ctx.dwellFadeMs || 0;
  const avgReady = turn.count >= 2 && dwellTurn >= DWELL_AVG_MS;
  const closeReady = fade.count >= 2 && dwellFade >= DWELL_CLOSE_MS;
  const retraceFromPeakPct = (ctx.retraceFromPeakPct != null) ? ctx.retraceFromPeakPct : null;
  const extremeDepthPct = (ctx.extremeDepthPct != null) ? ctx.extremeDepthPct : null;
  const exitLeg = ctx.exitLeg || 0;
  const newPeakHigher = !!ctx.newPeakHigher;
  const retrace2Pct = (ctx.retrace2Pct != null) ? ctx.retrace2Pct : null;
  const trailArmed = !!ctx.trailArmed;
  const trailRetracePct = (ctx.trailRetracePct != null) ? ctx.trailRetracePct : null;
  const trailHeldMs = ctx.trailHeldMs || 0;

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

  // Potensi = jarak dari harga ENTRY ke target akhir (target kontinuasi bila ada; jika tidak,
  // asumsi konservatif lock+0,05%). Inilah "100%" yang dimaksud user saat bilang profit 50%.
  const contTarget = (cont && cont.target != null) ? cont.target : (isUp ? ctx.lock * 1.0005 : ctx.lock * 0.9995);
  const targetDist = entPx != null ? Math.abs(contTarget - entPx) : null;
  const traveled = entPx != null ? (isUp ? (ctx.price - entPx) : (entPx - ctx.price)) : null;
  const captured = (targetDist != null && targetDist > ctx.price * 0.00001)
    ? Math.max(0, traveled / targetDist)
    : (traveled != null && traveled > 0 ? 1 : 0);
  const capturedPct = Math.round(captured * 100);
  const fadeCnt = (ctx.fade && ctx.fade.count) || 0;
  const retreatStdNow = (ctx.retreatStd != null) ? ctx.retreatStd : null;
  const emergency = (fadeCnt >= 3 && (ctx.dwellFadeMs || 0) >= DWELL_CLOSE_MS)
    || (retreatStdNow != null && retreatStdNow >= 0.5)
    || hRisk;
  const entered = !!ctx.entered;
  let state, action, cls, nowEntered = entered;
  const rNowTxt = levels.rNow.toFixed(2) + "%";

  // ===== ATURAN CUT (permintaan user) =====
  // Jangan CUT karena "reversal terdeteksi" saat sisa sesi masih panjang. CUT hanya bila:
  //  (1) sisa sesi < 2 menit DAN harga diperkirakan tak kembali ke lock (arus/momentum melawan), ATAU
  //  (2) ESCAPE: reversal KUAT (tren historis berbalik + arus kuat melawan + posisi sudah dalam).
  const timeForCut = remainSecNow != null && remainSecNow < TA.CUT_MIN_REMAIN_SEC;
  const cantReturn = ofiStrongAgainst || (ctx.slope != null && (isUp ? ctx.slope < 0 : ctx.slope > 0));
  const cutAllowed = timeForCut && cantReturn;
  const escapeReversal = histFlipped && ofiStrongAgainst && deepEnough;
  // PEAK-BASED ENTRY (permintaan user): entry hanya bila ada konfirmasi STRUKTURAL puncak contra
  // (higher-low utk bias up / lower-high utk bias down) atau peak lawan terkonfirmasi.
  const hasStructPeak = (turn.parts && turn.parts.structure === true) || !!ctx.peakAgainst;

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
    } else if (blockLateEntry) {
      // Sisa waktu sesi tidak cukup untuk mencapai LOCK -> jangan entry sekarang.
      state = "WAIT"; cls = "wait";
      action = `TUNGGU — sisa sesi ${Math.round(remainSecNow)}s (minimal ${Math.round(minRemainSec)}s untuk capai lock ${fmtPrice(ctx.lock)}), jarak ${distToLockPct.toFixed(2)}%`
        + `; entry hanya bila reversal EKSTREM terdeteksi`;
    } else if (!_simple && levels.rNow < Math.max(RLV[0], TA.ENTRY_MIN_NOW_PCT)) {
      // Kedalaman contra belum mencapai L1 (0,01%) -> entry terlalu dini: harga baru bergerak
      // sangat sedikit, sehingga potensi profitnya pun sangat kecil (kasus nyata ETH: entry hanya
      // ~0,002% dari lock -> close hanya untung 0,002%). Tunggu harga turun/naik minimal ke L1.
      state = "WAIT"; cls = "wait";
      action = `TUNGGU PEAK — kedalaman contra baru ${rNowTxt} (minimal ${Math.max(RLV[0], TA.ENTRY_MIN_NOW_PCT)}% untuk entry)`
        + `; konfirmasi pembalikan ${turn.count}/4 · ${Math.round(dwellTurn / 1000)}s/${DWELL_ENTRY_MS / 1000}s`;
    } else if ((_simple && extremeDepthPct != null && extremeDepthPct >= ENTRY_MIN_EXTREME_PCT && retraceFromPeakPct != null && retraceFromPeakPct >= ENTRY_RETRACE_PCT)
      || (!_simple && extremeDepthPct != null && extremeDepthPct >= ENTRY_MIN_EXTREME_PCT
        && ((retraceFromPeakPct != null && retraceFromPeakPct >= ENTRY_RETRACE_PCT && turn.count >= 1)
          || (turn.count >= 2 && dwellTurn >= DWELL_ENTRY_MS && hasStructPeak)))) {
      state = "ENTRY"; cls = "entry";
      nowEntered = true;
      action = `ENTRY SEKARANG ${bias.toUpperCase()} — peak contra terkonfirmasi (${rNowTxt}, ${partList(turn.parts)})`
        + (timeTooShort ? ` [reversal ekstrem; sisa sesi ${Math.round(remainSecNow)}s]` : "");
    } else {
      state = "WAIT"; cls = "wait";
      action = `TUNGGU PEAK — harga contra ${rNowTxt}; ekstrem ${extremeDepthPct != null ? extremeDepthPct.toFixed(3) : "-"}%/${ENTRY_MIN_EXTREME_PCT}% · retrace ${retraceFromPeakPct != null ? retraceFromPeakPct.toFixed(3) : "-"}%/${ENTRY_RETRACE_PCT}%`;
    }
  } else {
    // ---------------- PHASE 2: position open ----------------
    if (_simple) {
      if (trailArmed && trailRetracePct != null && trailRetracePct >= TA.TRAIL_CB_PCT && trailHeldMs >= TA.TRAIL_MIN_HOLD_MS) {
        state = "CLOSE"; cls = "exit";
        action = `TRAIL EXIT — mundur ${trailRetracePct.toFixed(3)}% dari puncak (cb ${TA.TRAIL_CB_PCT}%) · profit ${capturedPct}% dari potensi`;
      } else {
        state = "HOLD"; cls = "entry";
        action = `TAHAN (TRAIL ${trailArmed ? "armed" : "-"}) — profit ${capturedPct}% dari potensi`;
      }
    } else if (biasAtRisk && pnlPct != null && pnlPct > 0) {
      // Sedang UNTUNG + risiko berbalik -> amankan profit (darurat) = jual SISA/semua.
      state = "CLOSE2"; cls = "exit";
      action = `JUAL SISA SEKARANG — posisi ${pnlTxt} dari target (DARURAT: ${hWhy})`;
    } else if (biasAtRisk && (cutAllowed || escapeReversal)) {
      state = "STAND_DOWN"; cls = "exit";
      // Sebut alasan NYATA (bukan klaim "sinyal berbalik" bila tren historis tidak berbalik).
      action = `CUT SEKARANG — posisi ${pnlTxt}`
        + (cutAdverseStd != null ? ` (${Math.abs(cutAdverseStd).toFixed(1)}σ di bawah entry)` : "")
        + `; ${escapeReversal && !cutAllowed ? "REVERSAL KUAT" : (histFlippedNow ? "tren historis berbalik" : hWhy)}`
        + (timeForCut ? `; sisa sesi ${Math.round(remainSecNow)}s` : "");
    } else if (biasAtRisk) {
      // Risiko tinggi TAPI sisa sesi masih panjang & tidak ada reversal kuat -> TAHAN dulu.
      state = "HOLD_POS"; cls = "wait";
      action = `TAHAN — risiko: ${hWhy}; sisa sesi ${remainSecNow != null ? Math.round(remainSecNow) + "s" : "-"}`
        + ` (cut hanya bila <2 mnt & harga tak kembali ke lock ${fmtPrice(ctx.lock)}, atau reversal kuat)`;
    } else if (hRisk && favor >= 0 && pnlPct != null && pnlPct > 0) {
      // TARGET SUDAH TERSENTUH + risiko berbalik -> amankan profit (aturan DARURAT user).
      // Syarat favor>=0 penting: jangan "close karena untung" saat harga belum sampai lock —
      // kasus nyata ETH entry 2.673,13 -> close 2.673,07 hanya untung 0,002%.
      state = "CLOSE2"; cls = "exit";
      action = `JUAL SISA SEKARANG — amankan profit ${pnlTxt} (DARURAT: ${hWhy})`;
    } else if (hRisk) {
      // Risiko tinggi TAPI posisi belum untung & belum cukup dalam -> tahan dulu, jelaskan.
      state = "HOLD_POS"; cls = "wait";
      action = `TAHAN — masih di zona entry (posisi ${pnlTxt}); risiko: ${hWhy}. Tunggu balik ke lock ${fmtPrice(ctx.lock)}`;
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
      // EARLY CLOSE berbasis PEAK (permintaan user): jangan close hanya karena harga sudah lewat lock.
      // Wajib ada puncak: harga MUNDUR dari puncak (retreat) atau TRAIL puncak tersentuh.
      const trailHit = !!(ctx.trail && ctx.trail.armed && ctx.price != null && (isUp ? ctx.price <= ctx.trail.exitPrice : ctx.price >= ctx.trail.exitPrice));
      const closeSignal = ctx.retreat || trailHit;
      const whyClose = ctx.retreat ? "harga mundur dari puncak" : "trail puncak tersentuh";
      // Reversal NYATA (bukan sekadar "retreat + risiko") yang boleh memaksa leg-1 lebih awal.
      const realRev = histFlippedNow && ofiStrongAgainst;
      if (TA.TRAIL_MODE && trailArmed) {
        // EXIT TRAILING: jual (100%) saat harga mundur >= CB% dari puncak; selama belum -> TAHAN (ikuti puncak).
        if (trailRetracePct != null && trailRetracePct >= TA.TRAIL_CB_PCT && trailHeldMs >= TA.TRAIL_MIN_HOLD_MS) {
          state = "CLOSE"; cls = "exit";
          action = `TRAIL EXIT — mundur ${trailRetracePct.toFixed(3)}% dari puncak (cb ${TA.TRAIL_CB_PCT}%) · profit ${capturedPct}% dari potensi`;
        } else {
          state = "HOLD"; cls = "entry";
          action = `TAHAN (TRAIL armed ${(trailHeldMs/1000).toFixed(0)}s) — ikuti puncak; jual bila tahan >=${TA.TRAIL_MIN_HOLD_MS/1000}s & mundur ${TA.TRAIL_CB_PCT}%`;
        }
      } else if (exitLeg === 0) {
        // LEG-1: jual SEBAGIAN (50%) hanya bila profit sudah >= ambang ATAU reversal NYATA.
        // (Emergency lama tidak lagi menutup posisi prematur -> hindari kehilangan potensi profit besar.)
        if (closeSignal && (capturedPct >= TA.EARLYCLOSE_MIN_CAPTURED_PCT || realRev)) {
          state = "CLOSE"; cls = "exit";
          action = `JUAL SEBAGIAN (50%)${lockWin != null ? ` (WIN ${(lockWin * 100).toFixed(0)}%)` : ""}`
            + ` — ${whyClose} · profit ${capturedPct}% dari potensi`
            + `${trailTxt}${exTxt}`;
        } else if (closeSignal) {
          state = "HOLD"; cls = "entry";
          action = `TAHAN — profit baru ${capturedPct}% dari potensi (minimal ${TA.EARLYCLOSE_MIN_CAPTURED_PCT}% untuk jual sebagian, atau reversal nyata); ${whyClose}`;
        } else {
          state = "HOLD"; cls = "entry";
          action = ladderTxt;
        }
      } else {
        // LEG-2: jual SISA hanya bila puncak BARU lebih tinggi + micro-retrace (puncak terdeteksi, sebelum reversal).
        // Selama belum ada puncak baru / belum terdeteksi pembalikan -> HOLD sampai akhir sesi.
        if (newPeakHigher && retrace2Pct != null && retrace2Pct >= TA.CLOSE2_RETRACE_PCT) {
          state = "CLOSE2"; cls = "exit";
          action = `JUAL SISA SEKARANG — puncak BARU lebih tinggi & mundur ${retrace2Pct.toFixed(3)}% dari puncak`;
        } else {
          state = "HOLD"; cls = "entry";
          action = `TAHAN SISA — tunggu puncak BARU lebih tinggi (belum ada pembalikan)`;
        }
      }
    } else if (inZone2 && avgReady && !blockLateAdd) {
      state = "AVERAGE"; cls = "entry";
      action = `TAMBAH ENTRY SEKARANG ${bias.toUpperCase()} — harga ${rNowTxt} (average terkonfirmasi; sisa sesi ${remainSecNow != null ? Math.round(remainSecNow) + "s" : "-"})`;
    } else if (inZone2 && avgReady && blockLateAdd) {
      // Sisa waktu tidak cukup untuk kembali ke LOCK -> JANGAN menambah posisi (permintaan user).
      state = "HOLD_POS"; cls = "wait";
      action = `TAHAN — tidak menambah posisi: sisa sesi ${Math.round(remainSecNow)}s (butuh minimal ${Math.round(minRemainSec)}s untuk capai lock ${fmtPrice(ctx.lock)})`;
    } else if (inZone2) {
      state = "HOLD_POS"; cls = "wait";
      action = `SIAP TAMBAH ENTRY — konfirmasi ${turn.count}/4 · ${Math.round(dwellTurn / 1000)}s/${DWELL_AVG_MS / 1000}s`;
    } else {
      state = "HOLD_POS"; cls = "wait";
      action = `TUNGGU — harga baru ${rNowTxt} dari lock (zona tambah entry ${RLV[1]}%)`;
    }
  }
  const CMD = { ENTRY: "ENTRY SEKARANG", AVERAGE: "TAMBAH ENTRY SEKARANG", HOLD: "HOLD", CAUTION: "SIAP CLOSE", CLOSE: "CLOSE SEKARANG", CLOSE2: "JUAL SISA SEKARANG", STAND_DOWN: "CUT SEKARANG", WAIT: "TUNGGU", HOLD_POS: "TUNGGU", NO_SIGNAL: "—" };
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
  // berapa sigma harga sudah mundur dari puncak favor -> dipakai untuk kondisi DARURAT
  const retreatStd = (peakFavor > 0 && std > 0) ? (peakFavor - favor) / std : 0;
  // PEAK KONTRA: lacak harga EKSTREM di sisi contra sejak awal sesi (up -> min, down -> max),
  // lalu hitung retrace dari ekstrem. Dipakai untuk ENTRY dekat puncak (bukan menunggu struktur 30s).
  if (!state.adverseExtreme) state.adverseExtreme = {};
  const ext0 = state.adverseExtreme[key];
  const ext = (ext0 == null) ? C : (taUp ? Math.min(ext0, C) : Math.max(ext0, C));
  state.adverseExtreme[key] = ext;
  const retraceFromPeakPct = (C || 0) ? (taUp ? ((C - ext) / C * 100) : ((ext - C) / C * 100)) : 0;
  // kedalaman (contra) dari ekstrem tsb, dalam % — dipakai agar entry menunggu ekstrem yang CUKUP DALAM
  const extremeDepthPct = (ext || 0) ? (taUp ? ((O - ext) / ext * 100) : ((ext - O) / ext * 100)) : 0;
  // ===== STAGE EXIT: leg-1 (CLOSE 50%) -> leg-2 (CLOSE2 = puncak BARU lebih tinggi + micro-retrace) =====
  if (!state.exitLeg) state.exitLeg = {};
  if (!state.peakAtLeg1) state.peakAtLeg1 = {};
  const exitLeg = state.exitLeg[key] || 0;
  const peakAtLeg1 = state.peakAtLeg1[key] || 0;
  const retrace2Pct = (C || 0) ? ((peakFavor - favor) / C * 100) : 0;      // mundur dari puncak (favorable)
  const newPeakPct = (C || 0) ? ((peakFavor - peakAtLeg1) / C * 100) : 0;  // puncak baru vs puncak saat leg-1
  const newPeakHigher = exitLeg >= 1 && newPeakPct >= TA.CLOSE2_MIN_NEW_PEAK_PCT;
  // ===== TRAILING EXIT: arm saat profit>=ARM% ATAU sentuh lock; lalu jual saat mundur >= CB% dari puncak =====
  if (!state.trailArmed) state.trailArmed = {};
  const _ent0 = state.entered[key];
  const entPx = _ent0 ? _ent0.price : null;
  const contT = taUp ? O * 1.0005 : O * 0.9995;
  const pot = (entPx != null) ? Math.abs(contT - entPx) : null;
  const traveled = (entPx != null) ? (taUp ? (C - entPx) : (entPx - C)) : null;
  const capturedPct2 = (pot && pot > C * 0.00001) ? (traveled / pot) * 100 : (traveled > 0 ? 100 : 0);
  const lockTouch = taUp ? (C >= O) : (C <= O);
  const trailArmed = state.trailArmed[key] || (capturedPct2 >= TA.TRAIL_ARM_PCT) || lockTouch;
  state.trailArmed[key] = trailArmed;
  if (!state.trailSince) state.trailSince = {};
  if (trailArmed && !state.trailSince[key]) state.trailSince[key] = now;
  const trailHeldMs = (trailArmed && state.trailSince[key]) ? (now - state.trailSince[key]) : 0;
  const trailRetracePct = (C || 0) ? ((peakFavor - favor) / C * 100) : 0;

  const turn = turnEvidence(taUp, { slope, slopeRecent, ofiShort, win });
  const fade = fadeEvidence(taUp, { slope, slopeRecent, ofiShort, retreat, rsi });
  const dw = state.dwell[key] || (state.dwell[key] = { turnSince: null, fadeSince: null, ofiSince: null });
  dw.turnSince = turn.count >= 2 ? (dw.turnSince || now) : null;
  dw.fadeSince = fade.count >= 2 ? (dw.fadeSince || now) : null;
  // berapa lama arus (OFI sesi) sudah melawan arah bias -> dipakai untuk konfirmasi reversal
  const ofiAgainstNow = ofi != null && (isUp ? ofi < -0.05 : ofi > 0.05);
  dw.ofiSince = ofiAgainstNow ? (dw.ofiSince || now) : null;
  const health = computeSignalHealth(bias, {
    margin: isUp ? (C - O) : (O - C),
    marginStd: std > 0 ? Math.abs(C - O) / std : null,
    slope, slopeRecent, ofi, ofiShort, volAgainst, rsi, z, histTrend,
    ofiAgainstMs: dw.ofiSince ? now - dw.ofiSince : 0,   // persistensi arus melawan
  });
  const entered = !!(state.entered[key] && state.entered[key].entered);
  const trail = trailOfCloses(input.sessionCloses || [], O, taUp);
  const plan = computeTradePlan(bias, {
    tf: input.tf, lock: O, price: C, std, slope, slopeRecent, rsi, z, ofi, ofiShort, retreat, health,
    entered, turn, fade, trail, retreatStd, retraceFromPeakPct, extremeDepthPct,
    exitLeg, newPeakHigher, retrace2Pct, trailArmed, trailRetracePct, trailHeldMs,
    durMs: input.durMs, remainMs: input.remainMs,   // gate waktu entry
    entryPrice: state.entered[key] ? state.entered[key].price : null,   // harga entry posisi (untuk kedalaman CUT)
    dwellTurnMs: dw.turnSince ? now - dw.turnSince : 0,
    dwellFadeMs: dw.fadeSince ? now - dw.fadeSince : 0,
    histTrend, tiers: input.tiers || null,
  });
  if (!plan) return null;

  // STATUS ENTRY / EARLY CLOSE (state dibagi per sesi; penjaga mencegah pencatatan ganda)
  // Simpan KONTEKS entry (kedalaman/retrace/ekstrem/sisa waktu) + alasan close -> untuk learner.
  if (plan.entered && !state.entered[key]) {
    state.entered[key] = {
      entered: true, since: now, price: C,
      rNow: plan.levels ? plan.levels.rNow : null,
      retrace: retraceFromPeakPct, extremeDepth: extremeDepthPct,
      remainSec: (input.remainMs != null ? input.remainMs / 1000 : null),
      taVer: TA.VER,
    };
  }
  if (!plan.entered) {
    delete state.entered[key]; delete state.closed[key];
    if (state.exitLeg) delete state.exitLeg[key];
    if (state.peakAtLeg1) delete state.peakAtLeg1[key];
    if (state.trailArmed) delete state.trailArmed[key];
    if (state.trailSince) delete state.trailSince[key];
  }
  if ((plan.state === "CLOSE" || plan.state === "CLOSE2" || plan.state === "STAND_DOWN") && !state.closed[key]) {
    state.closed[key] = { at: now, price: C, reason: plan.state === "STAND_DOWN" ? "cut" : (plan.state === "CLOSE2" ? "close2" : "close") };
  }
  // majukan stage exit: CLOSE pertama = leg-1 (50%); CLOSE2/CUT = selesai
  if (plan.state === "CLOSE" && exitLeg === 0 && state.exitLeg) { state.exitLeg[key] = 1; if (state.peakAtLeg1) state.peakAtLeg1[key] = peakFavor; }
  if ((plan.state === "CLOSE2" || plan.state === "STAND_DOWN") && state.exitLeg) state.exitLeg[key] = 2;
  const _ent = state.entered[key], _clo = state.closed[key];
  plan.taVer = TA.VER;
  plan.entryPrice = _ent ? _ent.price : null;
  plan.statusEntry = {
    ok: !!_ent, at: _ent ? _ent.since : null, price: _ent ? _ent.price : null,
    waiting: plan.state === "STAND_DOWN" ? "reversal terdeteksi — tunggu setup baru"
      : (plan.state === "WAIT" && /TUNGGU PEAK/.test(plan.action) ? "konfirmasi peak contra (2/4 bagian + 4s)" : "harga belum contra / belum kembali ke lock"),
  };
  plan.statusClose = { ok: !!_clo, at: _clo ? _clo.at : null, price: _clo ? _clo.price : null, reason: _clo ? _clo.reason : null };
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
const DUR_SEC = { "5m": 300, "15m": 900, "1h": 3600 };

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
