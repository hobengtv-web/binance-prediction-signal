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
  const skipped = { noSig: 0, noRes: 0, late: 0, badDir: 0, old: 0 };
  for (const r of records || []) {
    if (!r || !r.t0 || r.t0 < minT0) { skipped.old++; continue; }
    if (!r.sig) { skipped.noSig++; continue; }
    if (!r.res) { skipped.noRes++; continue; }
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
      minute: bMinute(s.minuteIn != null ? s.minuteIn : 1),
      rsi: bRsi(s.rsi), vol: bVol(s.volRel2 != null ? s.volRel2 : s.volRel),
      hour: bHour(new Date(r.t0 * 1000).getUTCHours()),
      hist: bHist(s.histStrength),
      trend: (s.learn && s.learn.trend && s.learn.trend !== "na") ? s.learn.trend : "na",
      dir, gap: gapRaw,
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
      pnlUse: (botRoi != null ? botRoi : (pnlPct != null ? +pnlPct.toFixed(4) : null)),   // ROI $ BOT bila ada, else proksi spot
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
  const gb = blockersOf(gateRules.map((r) => ({ ...r, metric: "won" })), "won");
  const tb = blockersOf(touchRules.map((r) => ({ ...r, metric: "touch" })), "touch");
  const taken = rows.filter((r) => decide(r, gb) && decide(r, tb));
  const cov = rows.length ? taken.length / rows.length : 0;
  const wr = taken.length ? mean(taken.map((r) => r.won)) : 0;
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
  const minN = opts.minN || 8, thr = opts.thr != null ? opts.thr : 0.50;
  const recentN = opts.recentN || 3, recentWin = opts.recentWin != null ? opts.recentWin : 2;
  // PER coin × TF (objektif, tidak digeneralisir): WR tiap jam dihitung utk tiap key sendiri.
  const acc = {};
  for (const r of rows) {
    if (r.won == null || !r.symbol || !r.interval) continue;
    const key = r.symbol + "_" + r.interval;
    const h = new Date((r.t0 + 7 * 3600) * 1000).getUTCHours();
    const g = acc[key] = acc[key] || {};
    (g[h] = g[h] || []).push({ t0: r.t0, w: r.won ? 1 : 0 });
  }
  const keys = {};
  for (const key of Object.keys(acc)) {
    const hours = [], stats = [];
    for (let h = 0; h < 24; h++) {
      const all = (acc[key][h] || []).sort((a, b) => b.t0 - a.t0);
      if (all.length < minN) continue;                              // sampel kurang -> jangan putuskan
      const wr = all.reduce((s, x) => s + x.w, 0) / all.length;
      const rec = all.slice(0, recentN); const recWins = rec.reduce((s, x) => s + x.w, 0);
      const improving = rec.length >= recentN && recWins >= recentWin;
      stats.push({ h, n: all.length, wr: +wr.toFixed(4), recWins, recN: rec.length, improving });
      if (wr < thr && !improving) hours.push(h);
    }
    keys[key] = { hours, stats };
  }
  return { keys, thr, minN, recentN, recentWin };
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

/* ---------- OBJEKTIF PnL-TRADE (pakai res.trade -> pnlPct) ----------
   Mengukur rata-rata PnL per trade dari sinyal yang DIAMBIL, agar promosi mengejar PROFIT,
   bukan hanya winrate. Hanya baris yang punya pnlPct (TA benar-benar masuk & keluar). */
function evalModelPnl(rows, gateRules, touchRules) {
  const gb = blockersOf(gateRules.map((r) => ({ ...r, metric: "won" })), "won");
  const tb = blockersOf(touchRules.map((r) => ({ ...r, metric: "touch" })), "touch");
  const taken = rows.filter((r) => r.pnlUse != null && decide(r, gb) && decide(r, tb));
  const n = taken.length;
  return {
    n,
    meanPnl: n ? +mean(taken.map((r) => r.pnlUse)).toFixed(4) : 0,
    winRate: n ? +mean(taken.map((r) => (r.pnlUse > 0 ? 1 : 0))).toFixed(4) : 0,
  };
}
// Konteks TA_FEATS dengan PnL terburuk/terbaik (lessons + kandidat aturan PnL).
function pnlContexts(rows, FEATS) {
  const base = rows.filter((r) => r.pnlUse != null);
  if (!base.length) return { all: 0, n: 0, contexts: [] };
  const all = mean(base.map((r) => r.pnlUse));
  const out = [];
  for (const name of Object.keys(FEATS)) {
    const g = groupBy(base, FEATS[name]);
    for (const [k, arr] of g) {
      if (arr.length < 15) continue;
      const m = mean(arr.map((r) => r.pnlUse));
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
  if (rows.length < o.minRows) return { ok: false, reason: `butuh >= ${o.minRows} hasil, ada ${rows.length}`, rows: rows.length };
  const splitIdx = Math.floor(rows.length * 0.7);
  const train = rows.slice(0, splitIdx), test = rows.slice(splitIdx);
  const dirBase = mean(test.map((r) => r.won)), touchBase = mean(test.map((r) => r.touch));
  const dirTrain = mean(train.map((r) => r.won));
  const gateBuckets = bucketsOf(train, test, GATE_FEATS);
  const touchBuckets = bucketsOf(train, test, TOUCH_FEATS);
  const gateRules = mineRules(train, test, GATE_FEATS, GATE_PAIRS, dirBase, "won", o);
  const touchRules = mineRules(train, test, TOUCH_FEATS, TOUCH_PAIRS, touchBase, "touch", o);
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
  const wr = taken.length ? mean(taken.map((r) => r.won)) : 0;
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
  const usable = rows.filter((r) => r.dir === "up" || r.dir === "down");
  if (usable.length < o.minRows) return { ok: false, reason: `butuh >= ${o.minRows} baris berarah, ada ${usable.length}` };
  const splitIdx = Math.floor(usable.length * 0.7);
  const train = usable.slice(0, splitIdx), test = usable.slice(splitIdx);
  if (train.length < 100 || test.length < 60) return { ok: false, reason: "jendela latih/uji terlalu kecil" };
  // Objektif = Wilson LOWER BOUND dari winrate sinyal yang diambil, dengan syarat
  // cakupan >= minCov dan jumlah diambil >= minTaken. LB otomatis menghukum sampel
  // kecil, jadi tidak bisa "menang" hanya dengan mengambil 5 sinyal yang kebetulan benar.
  const util = (sel) => {
    const taken = train.filter((r) => applyThresholds2(r, sel));
    if (taken.length < o.minTaken) return { u: -1, taken: taken.length, cov: 0, wr: 0, lb: 0 };
    const st = stat(taken, "won");
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
  if (!i) return { promote: true, why: "belum ada model berjalan → pakai kandidat" };
  const cMin = (c.parts && c.parts.length) ? Math.min(...c.parts.map((p) => p.takenWinrate)) : c.takenWinrate;
  const iMin = (i.parts && i.parts.length) ? Math.min(...i.parts.map((p) => p.takenWinrate)) : i.takenWinrate;
  const dWr = c.takenWinrate - i.takenWinrate;
  // ===== JALUR PROMOSI PnL-TRADE: profit naik jelas (>=0.02pp/trade) walau WR tak naik 2pp =====
  const cp0 = c.pnl, ip0 = i.pnl;
  if (cp0 && ip0 && cp0.n >= 30 && ip0.n >= 30 && (cp0.meanPnl - ip0.meanPnl) >= 0.02) {
    return { promote: true, why: `PnL ${cp0.meanPnl}% (+${(cp0.meanPnl - ip0.meanPnl).toFixed(3)}pp) n ${cp0.n} vs insiden ${ip0.meanPnl}% (n ${ip0.n}) · WR ${(c.takenWinrate * 100).toFixed(1)}% cakupan ${((c.coverage || 0) * 100).toFixed(0)}%` };
  }
  if (dWr >= 0.02 && cMin >= iMin) {
    // GUARD PnL-TRADE: bila data PnL memadai, kandidat TIDAK boleh menurunkan profit.
    const cp = c.pnl, ip = i.pnl;
    if (cp && ip && cp.n >= 30 && ip.n >= 30 && cp.meanPnl < ip.meanPnl - 0.02) {
      return { promote: false, why: `WR naik tapi PnL kandidat ${cp.meanPnl}% < insiden ${ip.meanPnl}% (profit turun) — ditolak` };
    }
    const pnlTxt = (cp && ip) ? ` · PnL ${cp.meanPnl}% vs ${ip.meanPnl}% (n ${cp.n}/${ip.n})` : "";
    return { promote: true, why: `WR ${(c.takenWinrate * 100).toFixed(1)}% (+${(dWr * 100).toFixed(1)}pp) cakupan ${((c.coverage || 0) * 100).toFixed(0)}% · min-fold ${(cMin * 100).toFixed(1)}% vs insiden ${(i.takenWinrate * 100).toFixed(1)}% (min-fold ${(iMin * 100).toFixed(1)}%)${pnlTxt}` };
  }
  return { promote: false, why: `WR ${(c.takenWinrate * 100).toFixed(1)}% (min-fold ${(cMin * 100).toFixed(1)}%) tidak menambah ≥2pp vs insiden ${(i.takenWinrate * 100).toFixed(1)}% (min-fold ${(iMin * 100).toFixed(1)}%)` };
}

module.exports = { wilson, stat, mean, rowsFrom, buildModel, evalModel, evalModelRolling, evalModelPnl, pnlContexts, shouldPromote, blockersOf, APPLY_KEYS, hourVetoes, liveHourGate, decide, learnThresholds, evalTaken, applyThresholds: applyThresholds2, CANONICAL_MAX_MS, GATE_FEATS, TOUCH_FEATS, TA_FEATS, TA_PAIRS, mineTA, bDepth, bRetr, bRemain, BUCKETS: { bMinute, bRsi, bVol, bHour, bHist, bGap } };
