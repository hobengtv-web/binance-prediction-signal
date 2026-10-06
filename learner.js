/* ============================================================================
   LEARNER (shared library) — dipakai oleh server (re-fit terjadwal) dan skrip backtest.
   Prinsip anti-overfit yang sama: split berurutan waktu (bukan random), Wilson bound,
   minimal n, dan aturan hanya sah bila lolos jendela UJI.
   Model = FILTER konteks (bukan penebak arah): daftar konteks yang ditahan.
   ============================================================================ */

const wilson = (w, n, z = 1.96) => {
  if (!n) return { lo: 0, hi: 1 };
  const p = w / n, d = 1 + (z * z) / n, c = p + (z * z) / (2 * n);
  const m = z * Math.sqrt((p * (1 - p)) / n + (z * z) / (4 * n * n));
  return { lo: Math.max(0, (c - m) / d), hi: Math.min(1, (c + m) / d) };
};
const stat = (rows, key) => {
  const n = rows.length, w = rows.reduce((a, r) => a + (r[key] === 1 ? 1 : 0), 0);
  const { lo, hi } = wilson(w, n);
  return { n, wr: n ? w / n : 0, w, lb: lo, ub: hi };
};
const mean = (a) => (a.length ? a.reduce((x, y) => x + y, 0) / a.length : 0);

/* ---------- bucket (HARUS identik dengan app.js learnBuckets) ---------- */
const NUM = { "<0.005": 0.002, "0.005-0.01": 0.007, "0.01-0.02": 0.015, "0.02-0.035": 0.027, ">0.035": 0.05 };
const bMinute = (m) => (m <= 1 ? "1" : m <= 3 ? "2-3" : m <= 6 ? "4-6" : "7+");
const bRsi = (r) => (r == null ? "na" : r < 30 ? "<30" : r < 40 ? "30-40" : r < 60 ? "40-60" : r < 70 ? "60-70" : ">70");
const bVol = (v) => (v == null ? "na" : v < 0.7 ? "<0.7" : v < 1.0 ? "0.7-1" : v < 1.5 ? "1-1.5" : v < 2.5 ? "1.5-2.5" : ">2.5");
const bHour = (h) => (h == null ? "na" : h < 4 ? "0-3" : h < 8 ? "4-7" : h < 12 ? "8-11" : h < 16 ? "12-15" : h < 20 ? "16-19" : "20-23");
const bHist = (s) => (s == null ? "na" : s < 10 ? "<10" : s < 20 ? "10-20" : ">20");
const bGap = (g) => (g == null ? "na" : g < 0.005 ? "<0.005" : g < 0.01 ? "0.005-0.01" : g < 0.02 ? "0.01-0.02" : g < 0.035 ? "0.02-0.035" : ">0.035");
const isBucket = (v) => typeof v === "string" && /[<>\-]/.test(v);
/* ---------- bucket khusus TRADE ASSISTANT ---------- */
const bDepth = (v) => (v == null ? "na" : v < 0.02 ? "<0.02" : v < 0.03 ? "0.02-0.03" : v < 0.05 ? "0.03-0.05" : ">=0.05");
const bRetr = (v) => (v == null ? "na" : v < 0.02 ? "<0.02" : v < 0.03 ? "0.02-0.03" : v < 0.05 ? "0.03-0.05" : ">=0.05");
const bRemain = (v) => (v == null ? "na" : v < 60 ? "<60" : v < 120 ? "60-120" : v < 180 ? "120-180" : ">=180");

/* ---------- record ledger -> baris fitur ---------- */
// CANONICAL_MAX_MS: sinyal yang dipakai belajar = capture paling dekat ke detik ke-2.
// Browser yang dibuka di tengah sesi menghasilkan capOffsetMs besar -> DIKECUALIKAN dari
// pelatihan (bukan dihapus; tetap ada di record sebagai `alts` untuk studi entry telat).
const CANONICAL_MAX_MS = 6000;
// Normalisasi mikro-struktur: body 1s/2s dalam satuan sigma 1s (searah sinyal: + = mendukung arah)
function microNum(micro, key, sigma) {
  if (!micro || typeof micro[key] !== "number" || !(sigma > 0)) return null;
  return +(micro[key] / sigma).toFixed(3);
}
// Skor alignment: berapa TF (5m/15m/1h) yang trennya SEARAH sinyal (0..3, null bila tak ada data)
function alignScore(micro, dir) {
  const al = micro && micro.align;
  if (!al) return null;
  let n = 0, tot = 0;
  for (const k of ["5m", "15m", "1h"]) { if (al[k]) { tot++; if (al[k] === dir) n++; } }
  return tot ? n : null;
}

function rowsFrom(records, minT0 = 1700000000, opts = {}) {
  const includeLate = !!opts.includeLate;
  const out = [];
  const skipped = { noSig: 0, noRes: 0, noOutcome: 0, late: 0, badDir: 0, old: 0 };
  for (const r of records || []) {
    if (!r || !r.t0 || r.t0 < minT0) { skipped.old++; continue; }
    if (!r.sig) { skipped.noSig++; continue; }
    if (!r.res) { skipped.noRes++; continue; }
    // WAJIB ada outcome NYATA (won 0/1). Record flat (res.won=null) BUKAN kerugian — sebelumnya
    // ikut dihitung `won:0` (20% data!) -> WR tertekan -> learner memveto berlebihan (snowball).
    if (r.res.won !== 1 && r.res.won !== 0) { skipped.noOutcome++; continue; }
    const s = r.sig, dir = s.dir;
    if (dir !== "up" && dir !== "down") { skipped.badDir++; continue; }
    // kanonik: capOffsetMs ada -> wajib <= 6s; record lama (tanpa capOffsetMs) -> pakai minuteIn
    const off = typeof s.capOffsetMs === "number" ? s.capOffsetMs : null;
    const canonical = off != null ? off <= CANONICAL_MAX_MS : (s.minuteIn == null || s.minuteIn <= 1);
    if (!canonical && !includeLate) { skipped.late++; continue; }
    const gapRaw = (s.learn && isBucket(s.learn.gap)) ? s.learn.gap : bGap(s.rewardPct != null ? Math.abs(s.rewardPct) : null);
    // ===== METRIK TRADE ASSISTANT (dari res.trade; hanya ada pada sesi setelah fitur ini live) =====
    const tr = r.res.trade || {};
    const dirUp = dir === "up";
    const pot = (tr.entryPrice != null && r.res.lock != null) ? (dirUp ? (r.res.lock - tr.entryPrice) : (tr.entryPrice - r.res.lock)) : null;
    const gain = (tr.closed && tr.closePrice != null && tr.entryPrice != null) ? (dirUp ? (tr.closePrice - tr.entryPrice) : (tr.entryPrice - tr.closePrice)) : null;
    const capturePct = (gain != null && pot && pot > 0) ? (gain / pot * 100) : null;
    const pnlPct = (gain != null && tr.entryPrice) ? (gain / tr.entryPrice * 100) : null;
    // PnL $ NYATA BOT (dikirim BOT ke record.bot) -> lebih diutamakan daripada proksi spot pnlPct
    const botRoi = (r.bot && typeof r.bot.roiPct === "number") ? +r.bot.roiPct.toFixed(4) : null;
    out.push({
      t0: r.t0, asset: r.asset, interval: r.interval,
      symbol: r.asset, mode: s.mode || "na",
      grade: (s.grade || null),
      minute: bMinute(s.minuteIn != null ? s.minuteIn : 1),
      rsi: bRsi(s.rsi), vol: bVol(s.volRel2 != null ? s.volRel2 : s.volRel),
      hour: bHour(new Date(r.t0 * 1000).getUTCHours()),
      hist: bHist(s.histStrength),
      trend: (s.learn && s.learn.trend && s.learn.trend !== "na") ? s.learn.trend : "na",
      dir, gap: gapRaw,
      flat: !!(s.skipped),                    // sesi flat (flat-price/flat-noise) — direkam & DIPELAJARI
      silent: !!s.silent,                     // arah "silent" (ditebak utk sesi flat)
      // ===== KONFIRMASI ARAH (backtest 2026-10): tren multi-TF + OFI memperkuat arah mentah =====
      // mAlign = berapa TF (5m/15m/1h) yg trend-nya SEARAH dir. mOfiAgree = OFI mendukung dir.
      mAlign: (s.micro && s.micro.align) ? (["5m", "15m", "1h"].filter((t) => s.micro.align[t] === dir).length) : null,
      mOfiAgree: (typeof s.ofi === "number") ? (((dir === "up" && s.ofi > 0.05) || (dir === "down" && s.ofi < -0.05)) ? 1 : 0) : null,
      macdDir: (s.ind && s.ind.macdDir) ? s.ind.macdDir : null,
      // fitur numerik mentah — dibutuhkan untuk BELAJAR THRESHOLD (bukan hanya bucket)
      volRel2: typeof s.volRel2 === "number" ? s.volRel2 : (typeof s.volRel === "number" ? s.volRel : null),
      surprise: typeof s.surprise === "number" ? s.surprise : null,
      liqRatio: typeof s.liqRatio === "number" ? s.liqRatio : null,
      gapPct: typeof s.rewardPct === "number" ? Math.abs(s.rewardPct) : null,
      histStrength: typeof s.histStrength === "number" ? s.histStrength : null,
      rsi: typeof s.rsi === "number" ? s.rsi : null,
      // apakah profil gate yang SEDANG BERLAKU akan menerima sesi ini (diisi capture/klien)
      accepted: r.gate ? !!r.gate.accepted : null,
      reject: r.gate ? (r.gate.reject || null) : null,
      capOffsetMs: off, canonical,
      won: r.res.won === 1 ? 1 : 0,
      touch: r.res.touch === 1 ? 1 : 0,
      mfeFav: r.res.mfeFav, maeFav: r.res.maeFav,
      // ===== TA =====
      taEntered: !!tr.entered, taClosed: !!tr.closed,
      entryRNow: typeof tr.entryRNow === "number" ? tr.entryRNow : null,
      entryRetrace: typeof tr.entryRetrace === "number" ? tr.entryRetrace : null,
      entryExtremeDepth: typeof tr.entryExtremeDepth === "number" ? tr.entryExtremeDepth : null,
      entryRemainSec: typeof tr.entryRemainSec === "number" ? tr.entryRemainSec : null,
      closeReason: tr.closeReason || null,
      taVer: tr.taVer || null,
      capturePct: capturePct != null ? +capturePct.toFixed(3) : null,
      pnlPct: pnlPct != null ? +pnlPct.toFixed(4) : null,
      botRoi: botRoi,
      botPnl: (r.bot && typeof r.bot.pnl === "number") ? +r.bot.pnl.toFixed(4) : null,
      pnlUse: (botRoi != null ? botRoi : (pnlPct != null ? +pnlPct.toFixed(4) : null)),   // DISPLAY saja (akun, else proksi spot)
      // ===== $ NYATA = uang akun (botRoi) ATAU harga-token Binance nyata (settleRoi, hold-to-settle) =====
      settleRoi: (r.res && typeof r.res.settleRoi === "number") ? +r.res.settleRoi.toFixed(4) : null,
      settleSrc: (r.res && r.res.settleSrc) || null,
      // MENANG-$ (basis keputusan): 1 bila $ nyata > 0, 0 bila <=0, null bila tak ada $ nyata.
      dwin: (botRoi != null ? (botRoi > 0 ? 1 : 0) : ((r.res && typeof r.res.settleRoi === "number") ? (r.res.settleRoi > 0 ? 1 : 0) : null)),
      pnlReal: (botRoi != null ? botRoi : ((r.res && typeof r.res.settleRoi === "number") ? +r.res.settleRoi.toFixed(4) : null)),
      pnlRealSrc: (botRoi != null ? "account" : ((r.res && typeof r.res.settleRoi === "number") ? (r.res.settleSrc || "binance-quote") : null)),
      // $ NYATA utk arah TERBALIK (invert): dari HARGA TOKEN Binance sisi lawan + outcome nyata.
      flipRoi: (function () {
        const o = r.odds; if (!o || (dir !== "up" && dir !== "down")) return null;
        const pf = dir === "up" ? o.down : o.up;                 // harga token sisi LAWAN
        if (typeof pf !== "number" || !(pf > 0) || !(pf < 1)) return null;
        return (r.res.won === 1) ? -100 : +(((1 - pf) / pf) * 100).toFixed(4);
      })(),
      taWin: (pnlPct != null) ? (pnlPct > 0 ? 1 : 0) : null,
      taCapWin: (capturePct != null) ? (capturePct >= 50 ? 1 : 0) : null,
      // ===== MIKRO-STRUKTUR (fitur baru untuk mempertajam arah U/D) =====
      mO1: microNum(s.micro, "o1BodyS", s.micro && s.micro.sigma1s),
      mO2: microNum(s.micro, "o2BodyS", s.micro && s.micro.sigma1s),
      mAgree: (s.micro && s.micro.bodyAgree != null) ? (s.micro.bodyAgree ? 1 : 0) : null,
      mRanPos: (s.micro && typeof s.micro.ranPos === "number") ? +s.micro.ranPos.toFixed(3) : null,
      mAlign: alignScore(s.micro, dir),
    });
  }
  out.sort((a, b) => a.t0 - b.t0);
  out.skipped = skipped;
  return out;
}

const GATE_FEATS = { interval: (r) => r.interval, symbol: (r) => r.symbol, mode: (r) => r.mode, minute: (r) => r.minute, rsi: (r) => r.rsi, vol: (r) => r.vol, hour: (r) => r.hour, hist: (r) => r.hist, trend: (r) => r.trend,
  // mikro-struktur (bucket) — aktif otomatis saat cukup record punya data ini
  mAgree: (r) => r.mAgree == null ? "na" : (r.mAgree ? "agree" : "disagree"),
  mAlign: (r) => r.mAlign == null ? "na" : String(r.mAlign),
  mRanZone: (r) => r.mRanPos == null ? "na" : (r.mRanPos < 0.2 ? "low" : r.mRanPos > 0.8 ? "high" : "mid"),
  mBody: (r) => r.mO2 == null ? "na" : (r.mO2 > 0.5 ? "strong+" : r.mO2 < -0.5 ? "strong-" : "weak"),
};
const TOUCH_FEATS = { interval: (r) => r.interval, symbol: (r) => r.symbol, gap: (r) => r.gap, hour: (r) => r.hour, dir: (r) => r.dir };
const GATE_PAIRS = [["interval", "minute"], ["interval", "vol"], ["rsi", "trend"], ["symbol", "hour"], ["interval", "rsi"], ["hist", "trend"], ["minute", "vol"]];
const TOUCH_PAIRS = [["interval", "gap"], ["symbol", "gap"], ["hour", "gap"], ["dir", "gap"], ["interval", "hour"]];

/* ---------- fitur & OBJEKTIF TRADE ASSISTANT ---------- */
// Objektif TA = keberhasilan TRADE (pnlPct>0), BUKAN arah sesi (`won`).
const TA_FEATS = {
  interval: (r) => r.interval, symbol: (r) => r.symbol, dir: (r) => r.dir, mode: (r) => r.mode,
  minute: (r) => r.minute, rsi: (r) => r.rsi, vol: (r) => r.vol, hour: (r) => r.hour,
  depth: (r) => bDepth(r.entryRNow), retrace: (r) => bRetr(r.entryRetrace),
  remain: (r) => bRemain(r.entryRemainSec), exit: (r) => r.closeReason || "na",
};
const TA_PAIRS = [["interval", "depth"], ["dir", "retrace"], ["interval", "remain"], ["mode", "depth"], ["dir", "exit"], ["interval", "exit"], ["symbol", "depth"], ["minute", "depth"]];
function mineTA(train, test, opts = {}) {
  const o = Object.assign({ minTrain: 20, minTest: 10 }, opts);
  const only = (rows) => rows.filter((r) => r.taEntered && r.taWin != null);
  const bt = only(train), be = only(test);
  const base = bt.length ? bt.reduce((a, r) => a + r.taWin, 0) / bt.length : 0;
  const rules = mineRules(bt, be, TA_FEATS, TA_PAIRS, base, "taWin", o);
  return { base, nTrain: bt.length, nTest: be.length, rules };
}

const groupBy = (rr, kf) => { const m = new Map(); for (const r of rr) { const k = kf(r); if (!m.has(k)) m.set(k, []); m.get(k).push(r); } return m; };
const mkKey = (feats, FEATS) => (r) => feats.map((f) => `${f}=${FEATS[f](r)}`).join("&");

function bucketsOf(train, test, FEATS) {
  const out = {};
  for (const f of Object.keys(FEATS)) {
    if (f === "dir") continue;
    const g1 = groupBy(train, FEATS[f]), g2 = groupBy(test, FEATS[f]);
    out[f] = {};
    for (const [k, rr] of g1) {
      const s = stat(rr, "won"), st = stat(rr, "touch");
      const t = g2.get(k), u = t ? stat(t, "won") : { n: 0, wr: 0, lb: 0, ub: 0 }, u2 = t ? stat(t, "touch") : { n: 0, wr: 0, lb: 0, ub: 0 };
      out[f][k] = { n: s.n, wr: +s.wr.toFixed(4), touch: +st.wr.toFixed(4), nTest: u.n, wrTest: +u.wr.toFixed(4), lbTest: +u.lb.toFixed(4), ubTest: +u.ub.toFixed(4), touchTest: +u2.wr.toFixed(4), touchLbTest: +u2.lb.toFixed(4) };
    }
  }
  return out;
}

function mineRules(train, test, FEATS, PAIRS, base, metric, opts) {
  const MIN_TR = opts.minTrain, MIN_TE = opts.minTest;
  const out = [];
  const run = (feats) => {
    const kf = mkKey(feats, FEATS);
    const g1 = groupBy(train, kf), g2 = groupBy(test, kf);
    for (const [k, rr] of g1) {
      if (rr.length < MIN_TR) continue;
      const t = g2.get(k);
      if (!t || t.length < MIN_TE) continue;
      const s = stat(rr, metric), u = stat(t, metric);
      out.push({ k, n: rr.length, wr: +s.wr.toFixed(4), nTest: t.length, wrTest: +u.wr.toFixed(4), lbTest: +u.lb.toFixed(4), ubTest: +u.ub.toFixed(4), delta: +(u.wr - base).toFixed(4), verdict: u.lb > base ? "boost" : u.ub < base ? "suppress" : "neutral" });
    }
  };
  for (const f of Object.keys(FEATS)) run([f]);
  for (const p of PAIRS) if (p.every((x) => FEATS[x])) run(p);
  out.sort((a, b) => b.delta - a.delta);
  return out;
}

/* ---------- evaluasi model sebagai FILTER (dipakai untuk keputusan promosi) ---------- */
// Aturan penahan yang dipakai app: gate -> HANYA aturan interval tunggal; touch -> HANYA gap tunggal.
// Kunci fitur yang DIKENAL saat lock (bisa diterapkan live oleh capture.js). Harus sama dgn
// yang dievaluasi di sini, supaya metrik model = apa yang benar-benar diterapkan.
const APPLY_KEYS = new Set(["interval", "symbol", "dir", "hour", "gap", "mode"]);
function blockersOf(rules, metric) {
  return rules.filter((r) => r.verdict === "suppress" && r.k.indexOf("&") === -1 &&
    APPLY_KEYS.has(r.k.slice(0, r.k.indexOf("="))));
}
function decide(row, blockers) {
  return !blockers.some((b) => {
    const i = b.k.indexOf("="), f = b.k.slice(0, i), v = b.k.slice(i + 1);
    const rv = f === "gap" ? row.gap : f === "interval" ? row.interval : f === "symbol" ? row.symbol : f === "hour" ? row.hour : f === "dir" ? row.dir : null;
    return rv === v;
  });
}
// skor = winrate dari sinyal yang DIAMBIL x akar(cakupan) — filternya harus berguna,
// bukan sekadar mengambil sedikit sinyal.
function evalModel(rows, gateRules, touchRules) {
  const gb = blockersOf(gateRules.map((r) => ({ ...r, metric: "dwin" })), "dwin");
  const tb = blockersOf(touchRules.map((r) => ({ ...r, metric: "dwin" })), "dwin");
  const taken = rows.filter((r) => decide(r, gb) && decide(r, tb));
  const cov = rows.length ? taken.length / rows.length : 0;
  const wr = taken.length ? mean(taken.map((r) => r.dwin)) : 0;   // metrik = $ (dwin)
  return { n: rows.length, taken: taken.length, coverage: +cov.toFixed(4), takenWinrate: +wr.toFixed(4), score: +(wr * Math.sqrt(cov)).toFixed(4), gateBlockers: gb.map((b) => b.k), touchBlockers: tb.map((b) => b.k) };
}
/* ---------- VALIDASI BERGULIR (rolling / k-fold kronologis) ----------
   Model dinilai pada k jendela uji berurutan, bukan satu split. Dipakai untuk membandingkan
   kandidat vs incumbent pada jendela yang SAMA, sehingga tidak terjebak "insiden lama" yang
   metriknya berasal dari sampel kecil/beda waktu. */
function evalModelRolling(rows, gateRules, touchRules, k = 3) {
  const n = rows.length;
  if (n < k * 25) return evalModel(rows, gateRules, touchRules);
  const size = Math.floor(n / k);
  const parts = [];
  for (let i = 0; i < k; i++) {
    const a = i * size, b = (i === k - 1) ? n : (i + 1) * size;
    parts.push(evalModel(rows.slice(a, b), gateRules, touchRules));
  }
  const m = (f) => parts.reduce((s, p) => s + (p[f] || 0), 0) / parts.length;
  return {
    n, parts, taken: Math.round(m("taken")), coverage: +m("coverage").toFixed(4),
    takenWinrate: +m("takenWinrate").toFixed(4), score: +m("score").toFixed(4),
    scoreMin: +Math.min(...parts.map((p) => p.score)).toFixed(4),
    gateBlockers: parts[0].gateBlockers, touchBlockers: parts[0].touchBlockers,
  };
}
/* ---------- JAM OFF ADAPTIF ----------
   Hitung WR per jam WIB dari data; jam dgn WR < thr & n cukup dijadikan OFF (diperbarui tiap refit). */
function hourVetoes(rows, opts = {}) {
  const minN = opts.minN || 25, thr = opts.thr != null ? opts.thr : 0.50;
  const recentN = opts.recentN || 3, recentWin = opts.recentWin != null ? opts.recentWin : 2;
  const offCap = opts.offCap != null ? opts.offCap : 0.5;   // maks fraksi jam OFF (fail-open): sisakan >=50% jam ON
  const pnlBadThr = opts.pnlBad != null ? opts.pnlBad : -2;   // jam dgn $ < ini -> OFF walau WR ok
  rows = rows.filter((r) => !r.flat);   // KEPUTUSAN TRADE hanya dari sesi tradeable (flat hny utk model arah)
  // PER coin × TF (objektif, tidak digeneralisir): WR tiap jam dihitung utk tiap key sendiri.
  const acc = {};
  for (const r of rows) {
    if (r.won == null || !r.symbol || !r.interval) continue;
    const key = r.symbol + "_" + r.interval;
    const h = new Date((r.t0 + 7 * 3600) * 1000).getUTCHours();
    const g = acc[key] = acc[key] || {};
    (g[h] = g[h] || []).push({ t0: r.t0, w: r.won ? 1 : 0, p: r.pnlReal != null ? r.pnlReal : null });  // $ NYATA (akun / harga-token Binance)
  }
  const keys = {};
  for (const key of Object.keys(acc)) {
    const stats = [];
    for (let h = 0; h < 24; h++) {
      const all = (acc[key][h] || []).sort((a, b) => b.t0 - a.t0);
      if (all.length < minN) continue;                              // sampel kurang -> jangan putuskan
      const wr = all.reduce((s, x) => s + x.w, 0) / all.length;
      const rec = all.slice(0, recentN); const recWins = rec.reduce((s, x) => s + x.w, 0);
      const improving = rec.length >= recentN && recWins >= recentWin;
      const ps = all.filter((x) => x.p != null); const pm = ps.length ? ps.reduce((a, x) => a + x.p, 0) / ps.length : null;
      stats.push({ h, n: all.length, wr: +wr.toFixed(4), recWins, recN: rec.length, improving, pnlN: ps.length, pnl: pm != null ? +pm.toFixed(3) : null });
    }
    // FAIL-OPEN: jam OFF = jam TERBURUK saja, dibatasi offCap (default <=50% jam yg bisa diputuskan).
    // Cegah "OFF 20/24 jam" (over-block) yang membuat key nyaris tanpa sinyal. OBJEKTIF $: jam dgn
    // data $ positif JANGAN di-OFF-kan.
    // KEPUTUSAN 100% BERBASIS $ NYATA: jam OFF bila rata-rata $ akun < ambang (bukan WR).
    const minPnl = opts.minPnl != null ? opts.minPnl : 4;
    const cand = stats.filter((s) => s.pnlN >= minPnl && s.pnl != null && s.pnl < pnlBadThr).sort((a, b) => a.pnl - b.pnl);
    const maxOff = Math.floor(offCap * stats.length);
    const hours = cand.slice(0, maxOff).map((s) => s.h);
    keys[key] = { hours, stats };
  }
  return { keys, thr, minN, recentN, recentWin, offCap };
}

/* ---------- ANALISIS KONTEKS FLAT per coin×TF ----------
   Dari record FLAT (sig.skipped==="flat-price", dir null), ukur distribusi outcome.
   "majorityWR" = WR bila DIPAKSA arah sisi mayoritas. Ini BUKAN sinyal live — hanya uji apakah
   konteks flat punya edge. edge=true hanya bila Wilson-LB > 0.5 dan n cukup (bukan noise). */
function flatStats(records, opts = {}) {
  const minN = opts.minN || 10;
  const g = {};
  for (const r of records || []) {
    if (!r || !r.sig || r.res == null) continue;
    if (r.sig.skipped !== "flat-price") continue;
    const act = r.res.actual;
    if (act !== "up" && act !== "down") continue;
    const k = (r.asset || r.sig.asset) + "_" + (r.interval || r.sig.interval);
    (g[k] = g[k] || []).push(act);
  }
  const keys = {};
  for (const k of Object.keys(g)) {
    const a = g[k], n = a.length;
    const up = a.filter((x) => x === "up").length;
    const pUp = n ? up / n : 0;
    const side = pUp >= 0.5 ? "up" : "down";
    const maj = side === "up" ? up : n - up;
    const wb = n ? wilson(maj, n) : null;
    keys[k] = { n, up, down: n - up, pUp: +pUp.toFixed(4), majoritySide: side, majorityWR: n ? +(maj / n).toFixed(4) : null, wilsonLB: wb ? +wb.lo.toFixed(4) : null, edge: !!(wb && n >= minN && wb.lo > 0.5) };
  }
  return { keys, minN, note: "WR konteks FLAT per coin\u00d7TF (jika dipaksa arah sisi mayoritas). edge=true hanya bila Wilson-LB>0.5 & n>=minN." };
}

/* ---------- TIER LADDER PER coin×TF (dimining dari data per key) ----------
   Cari ambang (volRel2, surprise) per tier yg mencapai target WR, PER key (bukan global). */
function keyTiers(rows, opts = {}) {
  const minN = opts.minN || 20;
  const targets = opts.targets || { FAIR: 0.50, GOOD: 0.55, STRONG: 0.60 };
  const gridV = [0.3, 0.5, 0.7, 1, 1.5, 2, 3, 4];
  const gridS = [0, 0.5, 1, 1.5, 2, 3, 4];
  const groups = {};
  for (const r of rows) {
    if (r.dwin == null || !r.symbol || !r.interval || r.volRel2 == null || r.surprise == null) continue;  // tier dari $ NYATA
    const k = r.symbol + "_" + r.interval; (groups[k] = groups[k] || []).push(r);
  }
  const out = {};
  for (const k of Object.keys(groups)) {
    const a = groups[k]; const tier = {};
    for (const name of ["FAIR", "GOOD", "STRONG"]) {
      const t = targets[name]; let best = null;
      for (const v of gridV) for (const s of gridS) {
        const sub = a.filter((r) => r.volRel2 >= v && r.surprise >= s);
        if (sub.length < minN) continue;
        const w = sub.reduce((x, r) => x + (r.dwin ? 1 : 0), 0) / sub.length;   // fraksi MENANG-$ ($>0)
        // pilih ambang TERENDAH yg masih mencapai target (paling permisif)
        if (w >= t && (!best || (v + s / 2) < (best.volRel2 + best.surprise / 2))) best = { volRel2: v, surprise: s, wr: +w.toFixed(4), n: sub.length };
      }
      if (best) tier[name] = best;
    }
    out[k] = tier;
  }
  return { keys: out, targets };
}

/* Fail-open coverage guard: pilih bin "buruk" (WR<thr, n>=minN) TERBURUK dulu, TAPI
   (a) cakupan sampel yang diblok <= covCap (default 0,60), dan (b) sisanya ("allowed") tetap
   punya >= minAllowedN sampel. Bila aturan jadi terlalu luas (memblok hampir semua) -> kembalikan
   [] (fail-open: JANGAN veto) supaya key TIDAK macet tanpa sinyal. Ini mencegah bug "union bin
   menutup seluruh domain" (mis. rsiBad=[0,200) -> mustahil sinyal). */
function pickBadRanges(bins, valOf, thr, minN, opts, allRows) {
  const covCap = opts.covCap != null ? opts.covCap : 0.6;
  const minAllowedN = (opts.minAllowedN != null && opts.minAllowedN > 0) ? opts.minAllowedN : minN;
  const withFeat = allRows.filter((r) => valOf(r) != null);
  const total = withFeat.length || 1;
  const minPnl = opts.minPnl != null ? opts.minPnl : 4;
  const pnlBadThr = opts.pnlBad != null ? opts.pnlBad : -2;
  const cand = [];
  // KEPUTUSAN 100% BERBASIS $ NYATA (pnlReal): bin buruk bila rata-rata $ < ambang. TIDAK memakai WR.
  // Bila data $ belum cukup -> bin NETRAL (tidak diveto) — konsisten "jangan putuskan tanpa $ nyata".
  for (const [lo, hi] of bins) {
    const s = withFeat.filter((r) => { const v = valOf(r); return v >= lo && v < hi; });
    if (s.length < minN) continue;
    const ps = s.filter((r) => r.pnlReal != null);
    if (ps.length < minPnl) continue;
    const pm = mean(ps.map((r) => r.pnlReal));
    if (pm < pnlBadThr) cand.push({ lo, hi, n: s.length, nPnl: ps.length, pnl: +pm.toFixed(2) });
  }
  if (!cand.length) return { ranges: [], cov: 0, allowedN: total, failOpen: false };
  cand.sort((x, y) => x.pnl - y.pnl);                              // $-terburuk dulu
  const sel = []; let coveredN = 0;
  for (const c of cand) { if ((coveredN + c.n) / total > covCap) continue; sel.push([c.lo, c.hi]); coveredN += c.n; }
  const allowedN = total - coveredN;
  if (!sel.length || allowedN < minAllowedN) return { ranges: [], cov: 0, allowedN: total, failOpen: true };
  return { ranges: sel, cov: coveredN / total, allowedN, failOpen: false };
}

/* ---------- RECLAIM: sesi "tanpa sinyal" yang NYATA WIN -> ON-kan kembali ----------
   Untuk tiap key, cari KONTEKS fitur-tunggal yang menang kuat (Wilson-LB >= reclaimWlb, n >= reclaimMinN),
   TERMASUK konteks yang saat ini ditolak. Konteks ini di-ON-kan di capture (override veto/tier).
   Tujuan: (1) jangan menutup sesi yang sebenarnya profitable; (2) mengimbangi veto agar tak "snowball"
   makin menutup produksi signal. Berbasis OUTCOME nyata (res.won), bukan asumsi. */
const RECLAIM_FEATS = [
  { f: "rsi", bins: [[0, 30], [30, 40], [40, 60], [60, 70], [70, 100]] },
  { f: "volRel2", bins: [[0, 0.7], [0.7, 1], [1, 1.5], [1.5, 2.5], [2.5, 99]] },
  { f: "gapPct", bins: [[0, 0.005], [0.005, 0.01], [0.01, 0.02], [0.02, 0.035], [0.035, 9]] },
  { f: "surprise", bins: [[0, 1], [1, 5], [5, 10], [10, 30], [30, 999]] },
  { f: "histStrength", bins: [[0, 10], [10, 20], [20, 999]] },
  { f: "mAlign", bins: [[0, 2], [2, 3], [3, 4]] },          // keselarasan tren multi-TF (konfirmasi arah)
  { f: "mOfiAgree", bins: [[0, 1], [1, 2]] },               // OFI searah arah mentah
];
const RECLAIM_HOURS = [[0, 4], [4, 8], [8, 12], [12, 16], [16, 20], [20, 24]];
function keyReclaim(rows, opts = {}) {
  const minPnl = opts.reclaimMinPnl || 10, wlb = opts.reclaimWlb != null ? opts.reclaimWlb : 0.52;
  const maxCtx = opts.reclaimMax || 4;
  const covCap = opts.reclaimCovCap != null ? opts.reclaimCovCap : 0.4;   // maks cakupan union reclaim (anti-balik-snowball)
  const groups = {};
  for (const r of rows) { if (r.won == null || !r.symbol || !r.interval) continue; const k = r.symbol + "_" + r.interval; (groups[k] = groups[k] || []).push(r); }
  const matchCtx = (c, r) => {
    if (c.f === "dir") return r.dir === c.v;
    if (c.f === "grade") return r.grade === c.v;
    if (c.f === "hourWIB") { const h = Math.floor(((r.t0 + 7 * 3600) % 86400) / 3600); return h >= c.lo && h < c.hi; }
    const x = r[c.f]; return x != null && x >= c.lo && x < c.hi;
  };
  const out = {};
  for (const k of Object.keys(groups)) {
    const a = groups[k]; const cands = [];
    // KEPUTUSAN 100% BERBASIS $ NYATA: reclaim bila rata-rata $ > 0 DAN meyakinkan di level $ (wilson
    // pada "menang-$" pnlReal>0) DAN dua paruh waktu $-positif. TIDAK memakai WR arah.
    const add = (ctx, s) => {
      const ps = s.filter((r) => r.pnlReal != null);
      if (ps.length < minPnl) return;
      const pm = mean(ps.map((r) => r.pnlReal));
      if (!(pm > 0)) return;
      const w = ps.reduce((t, r) => t + (r.pnlReal > 0 ? 1 : 0), 0);
      const lb = wilson(w, ps.length).lo;
      if (lb < wlb) return;
      const h = Math.floor(ps.length / 2);
      if (h > 0) { const o = ps.slice(0, h), n2 = ps.slice(ps.length - h); if (mean(o.map((r) => r.pnlReal)) <= 0 || mean(n2.map((r) => r.pnlReal)) <= 0) return; }
      cands.push(Object.assign({ n: ps.length, lb: +lb.toFixed(4), pnl: +pm.toFixed(3) }, ctx));
    };
    for (const { f, bins } of RECLAIM_FEATS) for (const [lo, hi] of bins) add({ f, lo, hi }, a.filter((r) => r[f] != null && r[f] >= lo && r[f] < hi));
    for (const [lo, hi] of RECLAIM_HOURS) add({ f: "hourWIB", lo, hi }, a.filter((r) => { const h = Math.floor(((r.t0 + 7 * 3600) % 86400) / 3600); return h >= lo && h < hi; }));
    for (const f of ["dir", "grade"]) { const vals = {}; for (const r of a) { const v = r[f]; if (v == null) continue; (vals[v] = vals[v] || []).push(r); } for (const v of Object.keys(vals)) add({ f, v }, vals[v]); }
    cands.sort((x, y) => y.pnl - x.pnl);                             // $-terbaik dulu
    // Pilih terkuat dulu, TAPI batasi cakupan UNION <= covCap agar reclaim tak jadi "ON-kan semua".
    const sel = [];
    for (const c of cands) {
      if (sel.length >= maxCtx) break;
      const trial = a.filter((r) => sel.some((s) => matchCtx(s, r)) || matchCtx(c, r));
      if (a.length && trial.length / a.length > covCap) continue;
      sel.push(c);
    }
    out[k] = sel;
  }
  return out;
}

/* ---------- INVERT: konteks yg arah mentahnya TERBUKTI biasanya SALAH -> balik rekomendasi ----------
   Berbeda dari veto (block) & reclaim (ON-kan): ini MEMBALIK arah (up<->down) SEBELUM gate.
   HANYA sesi yang COCOK KARAKTERISTIK (indikator sama) yg dibalik — bukan semua sinyal.
   Fitur = 1 ATAU 2 kombinasi (mis. rsi<30 & vol 1.5-2.5 = oversold+volume tinggi -> gerak berbalik).
   Syarat KETAT (anti-overfit, cegah flip sembarangan):
     (1) n >= invertMinN (default 40)
     (2) Wilson-LB(aarah TERBALIK) >= invertWlb (default 0.52)
     (3) terkonfirmasi di DUA paruh waktu (kedua paruh: flipped-WR >= 0.5)
     (4) cakupan union <= invertCovCap (default 0.4)
   Default: kosong bila tak ada bukti -> TIDAK membalik apa pun. Konteks disimpan {and:[part,...]}. */
function keyInvert(rows, opts = {}) {
  const minN = opts.invertMinN || 40, wlb = opts.invertWlb != null ? opts.invertWlb : 0.52;
  const maxCtx = opts.invertMax || 4;
  const covCap = opts.invertCovCap != null ? opts.invertCovCap : 0.4;
  const groups = {};
  for (const r of rows) { if (r.won == null || !r.symbol || !r.interval) continue; const k = r.symbol + "_" + r.interval; (groups[k] = groups[k] || []).push(r); }
  const partMatch = (p, r) => {
    if (p.f === "dir") return r.dir === p.v;
    if (p.f === "grade") return r.grade === p.v;
    if (p.f === "hourWIB") { const h = Math.floor(((r.t0 + 7 * 3600) % 86400) / 3600); return h >= p.lo && h < p.hi; }
    const x = r[p.f]; return x != null && x >= p.lo && x < p.hi;
  };
  const ctxMatch = (c, r) => (c.and || []).every((p) => partMatch(p, r));
  const minPnl = opts.invertMinPnl || 10;
  const out = {};
  for (const k of Object.keys(groups)) {
    const a = groups[k].slice().sort((x, y) => (x.t0 || 0) - (y.t0 || 0));
    // bangun part + baris yang cocok (untuk efisiensi irisan 2-fitur)
    const parts = [];
    for (const { f, bins } of RECLAIM_FEATS) for (const [lo, hi] of bins) parts.push({ p: { f, lo, hi }, rows: a.filter((r) => r[f] != null && r[f] >= lo && r[f] < hi) });
    for (const [lo, hi] of RECLAIM_HOURS) parts.push({ p: { f: "hourWIB", lo, hi }, rows: a.filter((r) => { const h = Math.floor(((r.t0 + 7 * 3600) % 86400) / 3600); return h >= lo && h < hi; }) });
    for (const f of ["dir", "grade"]) { const vals = {}; for (const r of a) { const v = r[f]; if (v == null) continue; (vals[v] = vals[v] || []).push(r); } for (const v of Object.keys(vals)) parts.push({ p: { f, v }, rows: vals[v] }); }
    const cands = [];
    // KEPUTUSAN 100% BERBASIS $ NYATA: invert bila $ arah-terbalik (flipRoi dari harga token lawan)
    // rata-rata > 0, meyakinkan (wilson menang-$), dan dua paruh $-positif. Bukan WR.
    const consider = (plist, s) => {
      const fs = s.filter((r) => r.flipRoi != null);
      if (fs.length < minPnl) return;
      const fm = mean(fs.map((r) => r.flipRoi));
      if (!(fm > 0)) return;
      const wf = fs.reduce((t, r) => t + (r.flipRoi > 0 ? 1 : 0), 0);
      const lbFlip = wilson(wf, fs.length).lo;
      if (lbFlip < wlb) return;
      const h = Math.floor(fs.length / 2);
      if (h > 0) { const o = fs.slice(0, h), n2 = fs.slice(fs.length - h); if (mean(o.map((r) => r.flipRoi)) <= 0 || mean(n2.map((r) => r.flipRoi)) <= 0) return; }
      cands.push({ and: plist, n: fs.length, pnlFlip: +fm.toFixed(3), lbFlip: +lbFlip.toFixed(4) });
    };
    for (const A of parts) consider([A.p], A.rows);                     // 1-fitur
    for (let i = 0; i < parts.length; i++) for (let j = i + 1; j < parts.length; j++) {   // 2-fitur (irisan)
      const A = parts[i], B = parts[j];
      if (A.p.f === B.p.f || !A.rows.length || !B.rows.length) continue;
      const setB = new Set(B.rows); const inter = A.rows.filter((r) => setB.has(r));
      if (inter.length >= minN) consider([A.p, B.p], inter);
    }
    cands.sort((x, y) => y.lbFlip - x.lbFlip);
    const sel = [];
    for (const c of cands) {
      if (sel.length >= maxCtx) break;
      const trial = a.filter((r) => sel.some((s) => ctxMatch(s, r)) || ctxMatch(c, r));
      if (a.length && trial.length / a.length > covCap) continue;
      sel.push(c);
    }
    out[k] = sel;
  }
  return out;
}

/* ---------- CONFIRM: konteks yg MEMPERKUAT arah mentah (tren multi-TF/OFI) -> boost grade ----------
   PER KEY (coin×TF), bukan global. Hasil backtest: arah mentah yg selaras tren multi-TF (mAlign tinggi)
   atau OFI searah punya WR & $ lebih baik. Konteks ini MENAIKKAN grade 1 level di capture (bukan flip,
   bukan override veto). Syarat: Wilson-LB(won)>=confirmWlb, n>=confirmMinN, OOS 2 paruh, cakupan<=cap. */
const CONFIRM_FEATS = [
  { f: "mAlign", bins: [[0, 2], [2, 3], [3, 4]] },
  { f: "mOfiAgree", bins: [[0, 1], [1, 2]] },
];
function keyConfirm(rows, opts = {}) {
  const minN = opts.confirmMinN || 40, wlb = opts.confirmWlb != null ? opts.confirmWlb : 0.56;
  const maxCtx = opts.confirmMax || 3;
  const covCap = opts.confirmCovCap != null ? opts.confirmCovCap : 0.5;
  const groups = {};
  for (const r of rows) { if (r.won == null || !r.symbol || !r.interval) continue; const k = r.symbol + "_" + r.interval; (groups[k] = groups[k] || []).push(r); }
  const partMatch = (p, r) => (r[p.f] != null && r[p.f] >= p.lo && r[p.f] < p.hi);
  const ctxMatch = (c, r) => (c.and || []).every((p) => partMatch(p, r));
  const out = {};
  for (const k of Object.keys(groups)) {
    const a = groups[k].slice().sort((x, y) => (x.t0 || 0) - (y.t0 || 0));
    const parts = [];
    for (const { f, bins } of CONFIRM_FEATS) for (const [lo, hi] of bins) parts.push({ p: { f, lo, hi }, rows: a.filter((r) => r[f] != null && r[f] >= lo && r[f] < hi) });
    const cands = [];
    // KEPUTUSAN 100% BERBASIS $ NYATA: confirm bila $ rata-rata > 0, meyakinkan (wilson menang-$), 2 paruh $>0.
    const minPnl = opts.confirmMinPnl || 10;
    const consider = (plist, s) => {
      const ps = s.filter((r) => r.pnlReal != null);
      if (ps.length < minPnl) return;
      const pm = mean(ps.map((r) => r.pnlReal));
      if (!(pm > 0)) return;
      const wf = ps.reduce((t, r) => t + (r.pnlReal > 0 ? 1 : 0), 0);
      const lb = wilson(wf, ps.length).lo;
      if (lb < wlb) return;
      const h = Math.floor(ps.length / 2);
      if (h > 0) { const o = ps.slice(0, h), n2 = ps.slice(ps.length - h); if (mean(o.map((r) => r.pnlReal)) <= 0 || mean(n2.map((r) => r.pnlReal)) <= 0) return; }
      cands.push({ and: plist, n: ps.length, lb: +lb.toFixed(4), pnl: +pm.toFixed(3) });
    };
    for (const A of parts) consider([A.p], A.rows);
    for (let i = 0; i < parts.length; i++) for (let j = i + 1; j < parts.length; j++) {
      const A = parts[i], B = parts[j];
      if (A.p.f === B.p.f || !A.rows.length || !B.rows.length) continue;
      const setB = new Set(B.rows); const inter = A.rows.filter((r) => setB.has(r));
      if (inter.length >= minN) consider([A.p, B.p], inter);
    }
    cands.sort((x, y) => y.lb - x.lb);
    const sel = [];
    for (const c of cands) { if (sel.length >= maxCtx) break; const trial = a.filter((r) => sel.some((s) => ctxMatch(s, r)) || ctxMatch(c, r)); if (a.length && trial.length / a.length > covCap) continue; sel.push(c); }
    out[k] = sel;
  }
  return out;
}

/* ---------- VETO THRESHOLD PER coin×TF (dari data per key, bukan global) ----------
   Cari ambang "no-edge" utk key ini: rewardMin/liqMin (monoton) + rentang rsi/vol yg buruk.
   Tujuannya: filter ditentukan PER coin & durasi, tidak digeneralisir. */
function keyVetoes(rows, opts = {}) {
  const thr = opts.thr != null ? opts.thr : 0.50, minN = opts.minN || 25;
  rows = rows.filter((r) => !r.flat);   // veto/reclaim/invert/confirm = keputusan trade -> hanya sesi tradeable
  const groups = {};
  for (const r of rows) { if (r.won == null || !r.symbol || !r.interval) continue; const k = r.symbol + "_" + r.interval; (groups[k] = groups[k] || []).push(r); }
  const wr = (a) => (a.length ? a.reduce((s, r) => s + (r.won ? 1 : 0), 0) / a.length : null);
  const out = {};
  const minAllowedCov = opts.minAllowedCov != null ? opts.minAllowedCov : 0.3;   // "lamin produksi" per key
  const reclaimMap = keyReclaim(rows, opts);
  const invertMap = keyInvert(rows, opts);
  const confirmMap = keyConfirm(rows, opts);
  const inRanges = (v, ranges) => v != null && (ranges || []).some(([lo, hi]) => v >= lo && v < hi);
  const allowFrac = (a, rmin, lmin, rb, vb) => {
    const A = a.filter((r) => !inRanges(r.rsi, rb) && !inRanges(r.volRel2, vb) && (rmin <= 0 || (r.gapPct != null && r.gapPct >= rmin)) && (lmin <= 0 || (r.liqRatio != null && r.liqRatio >= lmin)));
    return a.length ? A.length / a.length : 1;
  };
  for (const k of Object.keys(groups)) {
    const a = groups[k];
    let rewardMin = 0;
    for (const e of [0, 0.001, 0.002, 0.003, 0.005, 0.008, 0.012]) { const s = a.filter((r) => r.rewardPct != null && r.rewardPct >= e); if (s.length >= minN && wr(s) >= thr) { rewardMin = e; break; } }
    let liqMin = 0;
    for (const e of [0, 1, 1.5, 2, 3, 5]) { const s = a.filter((r) => r.liqRatio != null && r.liqRatio >= e); if (s.length >= minN && wr(s) >= thr) { liqMin = e; break; } }
    const rsiB = [[0, 30], [30, 40], [40, 60], [60, 70], [70, 200]];
    const volB = [[0, 0.7], [0.7, 1], [1, 1.3], [1.3, 1.8], [1.8, 2.5], [2.5, 99]];
    let rsiR = pickBadRanges(rsiB, (r) => r.rsi, thr, minN, opts, a);
    let volR = pickBadRanges(volB, (r) => r.volRel2, thr, minN, opts, a);
    // ===== ANTI-SNOWBALL: jamin "lamin produksi" — fraksi sesi yg TETAP allowed >= minAllowedCov.
    // Bila veto (rsi/vol/reward/liq) menekan produksi di bawah lamin -> lepas yg paling luas dulu.
    let snowball = false;
    let frac = allowFrac(a, rewardMin, liqMin, rsiR.ranges, volR.ranges);
    if (frac < minAllowedCov) { rsiR = { ranges: [], cov: 0, failOpen: true }; volR = { ranges: [], cov: 0, failOpen: true }; snowball = true; frac = allowFrac(a, rewardMin, liqMin, [], []); }
    if (frac < minAllowedCov) { rewardMin = 0; liqMin = 0; frac = allowFrac(a, 0, 0, [], []); }
    out[k] = { n: a.length, rewardMin, liqMin, rsiBad: rsiR.ranges, volBad: volR.ranges,
      rsiFailOpen: rsiR.failOpen, volFailOpen: volR.failOpen,
      rsiCov: +rsiR.cov.toFixed(3), volCov: +volR.cov.toFixed(3),
      allowFrac: +frac.toFixed(3), snowball, reclaim: reclaimMap[k] || [], invert: invertMap[k] || [], confirm: confirmMap[k] || [] };
  }
  return { keys: out, thr, minN, minAllowedCov };
}

/* ---------- GATE WR PER JAM (jalur responsif utk jam SEKARANG) ----------
   Lihat WR jam WIB saat ini dari K sesi terakhir di jam itu:
   - WR >= thr (mis. 60%) -> ON (boleh trading)
   - WR  < thr            -> OFF untuk sisa jam ini; dievaluasi ulang jam berikutnya. */
function liveHourGate(rows, opts = {}, nowMs = Date.now()) {
  const thr = opts.thr != null ? opts.thr : 0.60, k = opts.k || 6, minN = opts.minN || 3;
  const h = new Date(nowMs + 7 * 3600 * 1000).getUTCHours();
  const a = (rows || [])
    .filter((r) => r.won != null && new Date((r.t0 + 7 * 3600) * 1000).getUTCHours() === h)
    .sort((x, y) => y.t0 - x.t0)
    .slice(0, k);
  const n = a.length;
  const wr = n ? a.reduce((s, x) => s + (x.won ? 1 : 0), 0) / n : null;
  const off = (n >= minN && wr != null && wr < thr);
  return { h, wr: wr != null ? +wr.toFixed(4) : null, n, off, thr, k, minN };
}

/* ---------- OBJEKTIF PnL-TRADE (HANYA $ AKUN NYATA: bot.roiPct) ----------
   Mengukur rata-rata PnL per trade dari sinyal yang DIAMBIL, agar promosi mengejar PROFIT akun,
   bukan hanya winrate. Hanya baris dgn `botRoi` (uang nyata dari akun Binance) yang dihitung —
   proksi spot (pnlPct) TIDAK dipakai untuk keputusan $. */
function evalModelPnl(rows, gateRules, touchRules) {
  const gb = blockersOf(gateRules.map((r) => ({ ...r, metric: "won" })), "won");
  const tb = blockersOf(touchRules.map((r) => ({ ...r, metric: "touch" })), "touch");
  const taken = rows.filter((r) => r.pnlReal != null && decide(r, gb) && decide(r, tb));
  const n = taken.length;
  return {
    n,
    meanPnl: n ? +mean(taken.map((r) => r.pnlReal)).toFixed(4) : 0,
    winRate: n ? +mean(taken.map((r) => (r.pnlReal > 0 ? 1 : 0))).toFixed(4) : 0,
  };
}
// Konteks TA_FEATS dengan PnL terburuk/terbaik (lessons + kandidat aturan PnL) — $ NYATA (akun/harga-token).
function pnlContexts(rows, FEATS) {
  const base = rows.filter((r) => r.pnlReal != null);
  if (!base.length) return { all: 0, n: 0, contexts: [] };
  const all = mean(base.map((r) => r.pnlReal));
  const out = [];
  for (const name of Object.keys(FEATS)) {
    const g = groupBy(base, FEATS[name]);
    for (const [k, arr] of g) {
      if (arr.length < 15) continue;
      const m = mean(arr.map((r) => r.pnlReal));
      out.push({ f: name, k, n: arr.length, meanPnl: +m.toFixed(4), delta: +(m - all).toFixed(4),
        verdict: m < all - 0.02 ? "suppress" : m > all + 0.02 ? "boost" : "neutral" });
    }
  }
  out.sort((a, b) => a.meanPnl - b.meanPnl);
  return { all: +all.toFixed(4), n: base.length, contexts: out };
}

/* ---------- bangun model dari baris fitur ---------- */
function buildModel(rows, opts = {}) {
  const o = Object.assign({ minTrain: 60, minTest: 40, minRows: 120 }, opts);
  // KEPUTUSAN 100% BERBASIS $ NYATA: hanya belajar dari baris yang punya $ nyata (dwin != null).
  // Bila $ belum cukup -> model tidak dibangun (netral), bukan memakai WR.
  rows = rows.filter((r) => r.dwin != null);
  if (rows.length < o.minRows) return { ok: false, reason: `butuh >= ${o.minRows} hasil ber-$, ada ${rows.length}`, rows: rows.length };
  const splitIdx = Math.floor(rows.length * 0.7);
  const train = rows.slice(0, splitIdx), test = rows.slice(splitIdx);
  const dirBase = mean(test.map((r) => r.dwin)), touchBase = dirBase;   // objek = $ (dwin), bukan arah
  const dirTrain = mean(train.map((r) => r.dwin));
  const gateBuckets = bucketsOf(train, test, GATE_FEATS);
  const touchBuckets = bucketsOf(train, test, TOUCH_FEATS);
  const gateRules = mineRules(train, test, GATE_FEATS, GATE_PAIRS, dirBase, "dwin", o);
  const touchRules = mineRules(train, test, TOUCH_FEATS, TOUCH_PAIRS, touchBase, "dwin", o);
  const gateSuppress = gateRules.filter((r) => r.verdict === "suppress").map((r) => r.k);
  const touchSuppress = touchRules.filter((r) => r.verdict === "suppress").map((r) => r.k);
  const metrics = evalModel(test, gateRules, touchRules);
  return {
    ok: true, rows: rows.length, splitIdx,
    baseline: { dirTrain: +dirTrain.toFixed(4), dirTest: +dirBase.toFixed(4), touchTest: +touchBase.toFixed(4) },
    gate: { buckets: gateBuckets, rules: gateRules, suppress: gateSuppress, boost: gateRules.filter((r) => r.verdict === "boost").map((r) => r.k) },
    touch: { buckets: touchBuckets, rules: touchRules, suppress: touchSuppress, boost: touchRules.filter((r) => r.verdict === "boost").map((r) => r.k) },
    lessons: { lessons: lessonsFrom(gateRules, touchRules, test, dirBase, touchBase) },
    metrics,
    // ===== OBJEKTIF PnL (res.trade) =====
    pnl: pnlContexts(train, TA_FEATS),
    pnlTest: evalModelPnl(test, gateRules, touchRules),
  };
}

function lessonsFrom(gateRules, touchRules, test, dirBase, touchBase) {
  const L = [];
  for (const r of gateRules.filter((x) => x.verdict === "boost").slice(0, 12)) L.push({ type: "boost", rule: r.k, nTest: r.nTest, wrTest: +r.wrTest.toFixed(4), lbTest: +r.lbTest.toFixed(4), text: `konteks ${r.k}: winrate uji ${(r.wrTest * 100).toFixed(1)}% (LB ${(r.lbTest * 100).toFixed(1)}%, n=${r.nTest})` });
  for (const r of gateRules.filter((x) => x.verdict === "suppress").slice(0, 12)) L.push({ type: "suppress", rule: r.k, nTest: r.nTest, wrTest: +r.wrTest.toFixed(4), ubTest: +r.ubTest.toFixed(4), text: `konteks ${r.k}: winrate uji ${(r.wrTest * 100).toFixed(1)}% (UB ${(r.ubTest * 100).toFixed(1)}%, n=${r.nTest}) — hindari` });
  for (const r of touchRules.filter((x) => x.verdict === "suppress" && x.k.indexOf("&") === -1).slice(0, 8)) L.push({ type: "suppress", rule: r.k, nTest: r.nTest, wrTest: +r.wrTest.toFixed(4), ubTest: +r.ubTest.toFixed(4), text: `peluang kembali ke lock ${r.k}: ${(r.wrTest * 100).toFixed(1)}% (UB ${(r.ubTest * 100).toFixed(1)}%, n=${r.nTest})` });
  // penyebab: fitur yang lebih sering muncul pada sinyal KALAH
  const WIN = test.filter((r) => r.won === 1), LOSE = test.filter((r) => r.won === 0);
  for (const f of Object.keys(GATE_FEATS)) {
    const gw = groupBy(WIN, GATE_FEATS[f]), gl = groupBy(LOSE, GATE_FEATS[f]);
    for (const k of new Set([...gw.keys(), ...gl.keys()])) {
      const pl = ((gl.get(k) || []).length) / (LOSE.length || 1), pw = ((gw.get(k) || []).length) / (WIN.length || 1);
      if (pl - pw >= 0.06 && (gl.get(k) || []).length >= 10) L.push({ type: "cause", feature: f, bucket: k, pLose: +pl.toFixed(4), pWin: +pw.toFixed(4), text: `faktor "${f}=${k}" lebih sering pada sinyal SALAH (${(pl * 100).toFixed(1)}% vs ${(pw * 100).toFixed(1)}%)` });
    }
  }
  return L.slice(0, 40);
}

/* ---------- BELAJAR THRESHOLD (menyesuaikan ambang tiap kriteria) ----------
   Metode: coordinate-ascent sederhana yang tervalidasi walk-forward.
     - dilatih pada 70% data paling awal, dinilai pada 30% paling akhir
     - kandidat = konjungsi aturan bentuk "fitur >= t" / "fitur <= t" (grid kuantil)
     - objektif = winrate(sinyal diambil) x cakupan^alpha, alpha=0.5
       (supaya "mengambil sangat sedikit sinyal" tidak otomatis dianggap menang)
     - setiap langkah hanya diterima bila menaikkan objektif latih > 1%
   Hasilnya = daftar threshold konkret yang bisa langsung dipakai app, bukan sekadar
   daftar konteks bucket. Aturan bucket (gate/touch) tetap ada sebagai lapisan kedua. */
const TH_FEATS = { volRel2: 1, surprise: 1, liqRatio: 1, gapPct: -1, histStrength: 1, rsi: 1,
  mO2: 1, mO1: 1, mAgree: 1, mRanPos: 1, mAlign: 1 };   // mikro-struktur (aktif saat cukup data)
function evalTaken(rows, thresholds) {
  const taken = rows.filter((r) => applyThresholds2(r, thresholds));
  const cov = rows.length ? taken.length / rows.length : 0;
  const wr = taken.length ? mean(taken.map((r) => r.dwin)) : 0;   // metrik = $ (dwin)
  return { n: rows.length, taken: taken.length, coverage: +cov.toFixed(4), takenWinrate: +wr.toFixed(4), score: +(wr * Math.sqrt(cov)).toFixed(4) };
}
function applyThresholds2(row, thresholds) {
  if (!thresholds || !thresholds.length) return true;
  for (const th of thresholds) {
    const v = row[th.f];
    if (typeof v !== "number" || !isFinite(v)) return false;
    if (th.op === ">=" ? v < th.t : v > th.t) return false;
  }
  return true;
}
function learnThresholds(rows, opts = {}) {
  const o = Object.assign({ minRows: 300, minTaken: 80, minCov: 0.2, grid: 20, rounds: 3, minGain: 0.004 }, opts);
  // KEPUTUSAN 100% BERBASIS $ NYATA: threshold dimining dari baris ber-$ nyata saja (dwin != null).
  const usable = rows.filter((r) => (r.dir === "up" || r.dir === "down") && r.dwin != null);
  if (usable.length < o.minRows) return { ok: false, reason: `butuh >= ${o.minRows} baris ber-$ berarah, ada ${usable.length}` };
  const splitIdx = Math.floor(usable.length * 0.7);
  const train = usable.slice(0, splitIdx), test = usable.slice(splitIdx);
  if (train.length < 100 || test.length < 60) return { ok: false, reason: "jendela latih/uji terlalu kecil" };
  // Objektif = Wilson LOWER BOUND dari winrate sinyal yang diambil, dengan syarat
  // cakupan >= minCov dan jumlah diambil >= minTaken. LB otomatis menghukum sampel
  // kecil, jadi tidak bisa "menang" hanya dengan mengambil 5 sinyal yang kebetulan benar.
  const util = (sel) => {
    const taken = train.filter((r) => applyThresholds2(r, sel));
    if (taken.length < o.minTaken) return { u: -1, taken: taken.length, cov: 0, wr: 0, lb: 0 };
    const st = stat(taken, "dwin");
    const cov = taken.length / train.length;
    if (cov < o.minCov) return { u: -1, taken: taken.length, cov, wr: st.wr, lb: st.lb };
    return { u: st.lb, taken: taken.length, cov, wr: st.wr, lb: st.lb };
  };
  let sel = [];
  let cur = util(sel);
  for (let round = 0; round < o.rounds; round++) {
    let best = null;
    for (const f of Object.keys(TH_FEATS)) {
      const vals = train.map((r) => r[f]).filter((v) => typeof v === "number" && isFinite(v)).sort((a, b) => a - b);
      if (vals.length < 50) continue;
      for (let i = 1; i <= o.grid; i++) {
        const t = vals[Math.min(vals.length - 1, Math.round((i / (o.grid + 1)) * (vals.length - 1)))];
        for (const op of [">=", "<="]) {
          const cand = sel.concat([{ f, op, t: +t.toFixed(6) }]);
          const m = util(cand);
          if (m.u > cur.u + o.minGain && (!best || m.u > best.m.u)) best = { cand, m };
        }
      }
    }
    if (!best) break;
    sel = best.cand; cur = best.m;
  }
  const trainM = evalTaken(train, sel), testM = evalTaken(test, sel);
  const baseTest = evalTaken(test, []);
  const baseLbTest = stat(test, "won").lb;
  const candLbTest = testM.taken ? stat(test.filter((r) => applyThresholds2(r, sel)), "won").lb : 0;
  return {
    ok: sel.length > 0, thresholds: sel, train: trainM, test: testM, baselineTest: baseTest,
    trainLb: cur.lb, testLb: +candLbTest.toFixed(4), baselineLbTest: +baseLbTest.toFixed(4),
    // menang out-of-sample: LB uji lebih tinggi DAN cakupan masih memadai DAN winrate naik
    // WR-first (konsisten dgn shouldPromote): menang bila WR uji naik >=2pp, cakupan memadai, LB tak merosot.
    beatsBaseline: testM.taken >= (o.minTakenTest || 40) && (testM.coverage || 0) >= o.minCov && (testM.takenWinrate - baseTest.takenWinrate) >= 0.02 && candLbTest >= baseLbTest - 0.01,
    note: sel.map((x) => `${x.f} ${x.op} ${x.t}`).join(" & ") || "tidak ada threshold yang menambah nilai",
  };
}

/* ---------- keputusan promosi: hanya bila MENANG pada jendela uji ---------- */
/* ---------- keputusan promosi: WR-FIRST (bukan coverage-first) ----------
   Untuk sinyal ke BOT, yang penting = WINRATE dari sinyal yang DIAMBIL (makin sedikit rugi),
   dengan syarat cakupan masih memadai & stabil di tiap lipatan. Metrik lama (WR x akar(coverage))
   menghukum selektivitas sehingga filter penajam tak pernah bisa promote. */
function shouldPromote(candidate, incumbent, minTake = 40, minCov = 0.35) {   // minCov = MIN_APPLY_COV supaya model yg dipakai PASTI diterapkan
  const c = candidate && candidate.metrics, i = incumbent && incumbent.metrics;
  if (!c) return { promote: false, why: "kandidat tidak valid" };
  if (c.taken < minTake) return { promote: false, why: `sinyal diambil hanya ${c.taken} (< ${minTake}) — bukti belum cukup` };
  if ((c.coverage || 0) < minCov) return { promote: false, why: `cakupan ${((c.coverage || 0) * 100).toFixed(0)}% < ${(minCov * 100).toFixed(0)}% — terlalu selektif` };
  const cp = c.pnl, ip = i && i.pnl;
  const pnlReady = !!(cp && cp.n >= 30);
  // ===== OBJEKTIF UTAMA: $ (PnL). Dipakai lebih dulu bila datanya memadai — bukan sekadar WR. =====
  // PnL mencerminkan trailing TP / close mandiri BOT (uang nyata), beda dari WR arah.
  if (pnlReady) {
    if (!ip || ip.n < 30) return (cp.meanPnl > 0)
      ? { promote: true, why: `$: kandidat meanPnl ${cp.meanPnl}% (n ${cp.n}) > 0; insiden data $ kurang -> adopsi kandidat` }
      : { promote: false, why: `$ kandidat NEGATIF (${cp.meanPnl}%, n ${cp.n}) -> tidak diadopsi` };
    const dPnl = cp.meanPnl - ip.meanPnl;
    if (dPnl >= 0.02) return { promote: true, why: `$ membaik: ${cp.meanPnl}% vs ${ip.meanPnl}% (+${dPnl.toFixed(3)}pp) n ${cp.n}/${ip.n} · WR ${(c.takenWinrate * 100).toFixed(1)}% cov ${((c.coverage || 0) * 100).toFixed(0)}%` };
    const dWr = c.takenWinrate - i.takenWinrate;
    if (dWr >= 0.03 && dPnl >= 0) return { promote: true, why: `$ datar (${cp.meanPnl}% vs ${ip.meanPnl}%) tapi WR +${(dWr * 100).toFixed(1)}pp tanpa turunkan $` };
    return { promote: false, why: `$ tidak membaik: ${cp.meanPnl}% vs insiden ${ip.meanPnl}% (n ${cp.n}/${ip.n})` };
  }
  // ===== Fallback: data $ belum cukup (<30) -> pakai WR, tetap dijaga guard $ bila ada. =====
  if (!i) return { promote: true, why: `belum ada model berjalan (data $ <30) → pakai kandidat (objektif WR ${(c.takenWinrate * 100).toFixed(1)}%)` };
  const cMin = (c.parts && c.parts.length) ? Math.min(...c.parts.map((p) => p.takenWinrate)) : c.takenWinrate;
  const iMin = (i.parts && i.parts.length) ? Math.min(...i.parts.map((p) => p.takenWinrate)) : i.takenWinrate;
  const dWr = c.takenWinrate - i.takenWinrate;
  if (dWr >= 0.02 && cMin >= iMin) {
    if (cp && ip && cp.n >= 30 && ip.n >= 30 && cp.meanPnl < ip.meanPnl - 0.02) return { promote: false, why: `WR naik tapi $ turun (${cp.meanPnl}% < ${ip.meanPnl}%) — ditolak` };
    return { promote: true, why: `WR ${(c.takenWinrate * 100).toFixed(1)}% (+${(dWr * 100).toFixed(1)}pp) cov ${((c.coverage || 0) * 100).toFixed(0)}% (data $ <30 -> fallback WR)` };
  }
  return { promote: false, why: `WR ${(c.takenWinrate * 100).toFixed(1)}% tidak menambah ≥2pp vs insiden ${(i.takenWinrate * 100).toFixed(1)}%` };
}

module.exports = { wilson, stat, mean, rowsFrom, buildModel, evalModel, evalModelRolling, evalModelPnl, pnlContexts, shouldPromote, blockersOf, APPLY_KEYS, hourVetoes, keyVetoes, keyTiers, flatStats, liveHourGate, decide, learnThresholds, evalTaken, applyThresholds: applyThresholds2, CANONICAL_MAX_MS, GATE_FEATS, TOUCH_FEATS, TA_FEATS, TA_PAIRS, mineTA, bDepth, bRetr, bRemain, BUCKETS: { bMinute, bRsi, bVol, bHour, bHist, bGap } };
