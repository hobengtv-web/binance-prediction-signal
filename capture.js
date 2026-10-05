/* ============================================================================
   CAPTURE / ENGINE SINYAL SERVER-SIDE
   Dua pemakai, SATU rumus:
     - computeSignal()  : fungsi MURNI (tanpa I/O) yang menghitung sinyal sesi kanonik
                          2 detik: lock, C2, vol2sum, sigma1s, volRel2, surprise, tier,
                          gate likuiditas, threshold profil gate, RSI, histStrength.
     - createCapture()  : merekam hasilnya ke ledger (termasuk sesi yang DITOLAK, agar
                          learner bisa belajar dari populasi yang ditolak).
   engine.js (mesin sinyal live untuk semua device) memakai computeSignal() yang sama,
   sehingga sinyal yang tampil di UI = sinyal yang dinilai/dipelajari. Tidak ada duplikasi
   rumus.

   Hasil ronde tetap dilengkapi resolveMissing() di server.js (klines 1m); kalau ada
   klien yang mengirim hasil dari jalur 1s (lebih presisi), hasil itu yang dipakai.
   ============================================================================ */
const LEARNER_BUCKETS = require("./learner.js").BUCKETS;
const GATES_DEF = require("./gates.js");

const VOL_TYPICAL = { BTC: 0.515, ETH: 8.22, BNB: 3.0 };
const DUR_S = { "5m": 300, "15m": 900, "1h": 3600 };   // 1h ikut diproses server
const mean = (a) => (a.length ? a.reduce((x, y) => x + y, 0) / a.length : 0);
function pctile(arr, p) {
  if (!arr || !arr.length) return 0;
  const s = arr.slice().sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.max(0, Math.floor((p / 100) * (s.length - 1))))];
}
const rsiBucket = (r) => (r == null ? "na" : r < 30 ? "<30" : r < 40 ? "30-40" : r < 60 ? "40-60" : r < 70 ? "60-70" : ">70");
const strBucket = (s) => (s == null ? "na" : s < 35 ? "<35" : s <= 50 ? "35-50" : ">50");

/* ===== VETO "NO-EDGE" (default ON; matikan dengan env TA_VETO=0) =====
   Analisa 2.542 sesi live: sesi tanpa edge ber-WR ~48-50% (≈ koin). Veto membuangnya
   (accepted=false) agar BOT tidak entry. Ambang bisa diubah via env tanpa ubah kode. */
const _n = (v, d) => { const x = Number(v); return isFinite(x) ? x : d; };
const TA_VETO = {
  on: process.env.TA_VETO !== "0",
  rewardMin: _n(process.env.TA_VETO_REWARD_MIN, 0.002),
  rsiLo: _n(process.env.TA_VETO_RSI_LO, 30), rsiHi: _n(process.env.TA_VETO_RSI_HI, 40),
  volLo: _n(process.env.TA_VETO_VOL_LO, 1.3), volHi: _n(process.env.TA_VETO_VOL_HI, 1.8),
  hours: String(process.env.TA_VETO_HOURS || "3,5,10,12,14,21,22").split(",").map((x) => parseInt(x, 10)).filter((x) => !isNaN(x)), // WIB — diperbarui dari data 2998 sesi (WR<53%)
  liqMin: _n(process.env.TA_VETO_LIQ_MIN, 2),
};

/* ============================================================================
   computeSignal(o) — INTI PERHITUNGAN (murni, tanpa fetch).
   Input: { sym, tf, t0, tfc, idx, ones, five5m, profile, getModel, SignalCore, nowSec }
     tfc    = klines interval (candle sesi pada t0 harus ada di indeks idx)
     ones   = klines 1s di sekitar t0 (harus memuat t0 dan t0+1)
     five5m = klines 5m yang SUDAH selesai (untuk RSI tanpa lookahead)
   Output: { ok:true, signal:{...} } atau { skipped:"<alasan>" }
   ============================================================================ */
function computeSignal(o) {
  const { sym, tf, t0, tfc, idx, ones, five5m, getModel, SignalCore, nowSec } = o;
  const _key = sym + "_" + tf;
  // PER coin × TF: pakai profil (tiers+thresholds) MILIK key ini (independen), fallback bootstrap.
  let profile = o.profile || GATES_DEF.BOOTSTRAP;
  if (profile && profile.byKey) profile = Object.assign({}, GATES_DEF.BOOTSTRAP, profile.byKey[_key] || {});
  const tfSec = DUR_S[tf];
  if (!tfSec) return { skipped: "bad-tf" };
  if (!SignalCore) return { skipped: "no-core" };
  if (idx < 26) return { skipped: "no-session" };
  const sess = tfc[idx];
  const lock = sess && sess.open;
  if (!lock) return { skipped: "no-lock" };
  const prior = tfc.slice(idx - 25, idx);
  const baseVol = mean(prior.map((c) => c.vol || 0));
  const prior5 = tfc.slice(Math.max(0, idx - 50), idx).map((c) => c.vol || 0).filter((v) => v > 0);
  const histTrend = SignalCore.analyzeHistoricalTrend(tfc.slice(0, idx + 1), 50);
  const o1 = ones.find((c) => c.time === t0), o2 = ones.find((c) => c.time === t0 + 1);
  if (!o1 || !o2) return { skipped: "no-1s" };
  const C2 = o2.close;
  const vol2sum = (o1.vol || 0) + (o2.vol || 0);
  const pre = ones.filter((c) => c.time >= t0 - 60 && c.time < t0);
  const sigma1s = pre.length ? mean(pre.map((c) => c.high - c.low)) : 0;
  // ===== MIKRO-STRUKTUR (untuk model arah U/D; direkam ke ledger) =====
  const preHigh = pre.length ? Math.max(...pre.map((c) => c.high)) : null;
  const preLow = pre.length ? Math.min(...pre.map((c) => c.low)) : null;
  const ranPos = (preHigh != null && preLow != null && preHigh > preLow) ? (C2 - preLow) / (preHigh - preLow) : null;
  const o1Body = (o1.close || 0) - (o1.open || 0), o2Body = (o2.close || 0) - (o2.open || 0);
  const bodyAgree = (o1Body !== 0 && Math.sign(o1Body) === Math.sign(o2Body)) ? 1 : 0;
  const sgn = C2 > lock ? 1 : -1;   // searah arah sinyal?
  const o1BodyS = sgn * o1Body, o2BodyS = sgn * o2Body;
  const align = o.align || null;
  const volRel2 = baseVol > 0 ? (vol2sum * (tfSec / 2)) / baseVol : 1;
  const moveAbs = Math.abs(C2 - lock);
  const mv2 = lock > 0 ? (moveAbs / lock) * 100 : 0;
  const surprise = sigma1s > 0 ? moveAbs / sigma1s : 0;
  let currentDir = C2 > lock ? "up" : C2 < lock ? "down" : "flat";
  // ===== FLAT / NOISE (guard C: gerak minim) =====
  // PENTING: JANGAN return objek ringkas di sini. Dulu flat-price/flat-noise return objek minimal ->
  // seluruh detail (rsi, micro, liqRatio, rewardPct, gate, power, ofi) HILANG, sehingga sesi TANPA
  // sinyal tidak selengkap sesi bersinyal & tak bisa dianalisa penuh oleh learner. Sekarang sinyal
  // dihitung PENUH di bawah, lalu ditandai `flatReason` di akhir (dir=null utk flat-price; dir tetap
  // utk flat-noise) + accepted=false + reject=null. Yang TIDAK disimpan hanya kasus benar-benar tanpa
  // data (bad-tf/no-core/no-session/no-lock/no-1s) di atas.
  // FILTER GERAK-MINIMUM (uji 6 hari): entry hanya bila harga sudah bergerak >= 0,02% dari LOCK pada
  // momen sinyal (rewardPct == mv2). Terbukti mengubah EV dari negatif (~-$146) → positif (+$20) di
  // seluruh rentang harga entry (0,55–0,62); WR 56%→62,5%. Di bawah ambang ini -> "flat-noise" (skip).
  // Bisa dibalik instan via env MIN_MV2_PCT (mis. 0.005). 0 = nonaktif.
  const MIN_MV2_PCT = Number(process.env.MIN_MV2_PCT != null ? process.env.MIN_MV2_PCT : 0.015);
  const flatReason = (currentDir === "flat") ? "flat-price" : ((MIN_MV2_PCT > 0 && mv2 < MIN_MV2_PCT) ? "flat-noise" : null);
  // rsi dari candle 5m yang SUDAH SELESAI (tanpa lookahead)
  let rsi = null;
  try {
    if (five5m && five5m.length) rsi = SignalCore.rsiFromSeries(five5m.filter((c) => c.time + 300 <= nowSec).slice(-50), 14);
  } catch (_) {}
  // ===== FILTER RSI (scalper) — zona tengah 40–70 =====
  // Uji ketat (data nyata, 6 hari): binning -> RSI <40 / >=70 = 51–57% (buruk); RSI 40–70 = 67–80%
  // (60–70 justru 80%). Uji statistik z=2,18 (≈95% signifikan), konsisten di dua paruh waktu.
  // rsi null (data 5m kosong) -> filter dilewati (jangan blokir karena data hilang). Env RSI_MIN/RSI_MAX.
  // DIMATIKAN (Opsi 1, uji 6-hari): filter rsiOK 40-70 ternyata OVERFIT (n kecil) & BERTENTANGAN
  // dengan veto-rsi learner. Data penuh: aturan learner (mis. ETH_5m RSI>=70, WR 55,1%) mengalahkan
  // rsiOK 40-70 (WR 47,6%). RSI kini sepenuhnya diatur veto-rsi learner (per-koin). Konstanta disimpan
  // hanya utk referensi/observabilitas.
  const RSI_MIN = Number(process.env.RSI_MIN != null ? process.env.RSI_MIN : 40);
  const RSI_MAX = Number(process.env.RSI_MAX != null ? process.env.RSI_MAX : 70);
  const rsiOK = true;   // NONAKTIF (jangan blokir) — lihat catatan di atas.
  // ===== RECORDER INDIKATOR TAMBAHAN (untuk uji jendela panjang nanti) =====
  // EMA9/EMA21 (crossover), MACD (12/26/9), dan pola candle 5m terakhir — semua dari candle 5m yang
  // SUDAH SELESAI (tanpa lookahead). Direkam ke ledger agar bisa diuji tanpa menunggu 30 hari lagi.
  let ind = null;
  try {
    const cl = (five5m || []).filter((c) => c.time + 300 <= nowSec);
    if (cl.length >= 30) {
      const closes = cl.map((c) => c.close);
      const ema = (arr, p) => { if (!arr.length) return null; let e = arr[0]; const k = 2 / (p + 1); for (let i = 1; i < arr.length; i++) e = arr[i] * k + e * (1 - k); return e; };
      const seg = closes.slice(-80);
      const e9 = ema(seg, 9), e21 = ema(seg, 21), e12 = ema(seg, 12), e26 = ema(seg, 26);
      // MACD line series -> signal line (EMA9 dari MACD)
      const macdSeries = [];
      for (let i = 26; i <= seg.length; i++) { const s = seg.slice(0, i); macdSeries.push(ema(s, 12) - ema(s, 26)); }
      const macd = macdSeries.length ? macdSeries[macdSeries.length - 1] : (e12 - e26);
      const macdSig = ema(macdSeries.slice(-20), 9);
      const macdHist = (macd != null && macdSig != null) ? (macd - macdSig) : null;
      const last = cl[cl.length - 1];
      const body = last.close - last.open, rng = last.high - last.low;
      const upper = last.high - Math.max(last.open, last.close), lower = Math.min(last.open, last.close) - last.low;
      let pattern = "none";
      if (rng > 0) {
        if (Math.abs(body) <= 0.1 * rng) pattern = "doji";
        else if (lower >= 2 * Math.abs(body) && upper <= 0.3 * rng) pattern = "hammer";
        else if (upper >= 2 * Math.abs(body) && lower <= 0.3 * rng) pattern = "shooting-star";
        else pattern = body > 0 ? "bull-candle" : "bear-candle";
      }
      ind = {
        ema9: e9 != null ? +e9.toFixed(2) : null, ema21: e21 != null ? +e21.toFixed(2) : null,
        emaCross: (e9 != null && e21 != null) ? (e9 > e21 ? "up" : "down") : null,
        emaAgree: (e9 != null && e21 != null) ? ((e9 > e21) === (currentDir === "up") ? 1 : 0) : null,
        macd: macd != null ? +macd.toFixed(4) : null, macdSig: macdSig != null ? +macdSig.toFixed(4) : null,
        macdHist: macdHist != null ? +macdHist.toFixed(4) : null,
        macdDir: macdHist != null ? (macdHist > 0 ? "up" : "down") : null,
        pattern, lastBody: +body.toFixed(2), lastRange: +rng.toFixed(2),
      };
    }
  } catch (_) {}
  // CATATAN: gate `verdict !== flat` milik app memakai volRel yang bergantung waktu
  // (artefak floor `frac` = 0.05 sebelum detik ke-15), sehingga pada 2 detik app sering
  // melaporkan LOWVOL walau ladder tier lolos. Populasi yang dipakai = ladder tier 2 detik
  // yang terdokumentasi (early2s.json) + gate likuiditas; mode/conf seperti sinyal TREND.
  const mode = "TREND", conf = 65;
  const T = profile.tiers || GATES_DEF.BOOTSTRAP.tiers;
  const gateNow = Math.abs((C2 - lock) / lock) * 100;
  // GUARD: profil per-key (dari learner/keyTiers) bisa TIDAK punya semua tier (STRONG/GOOD/FAIR).
  // Tanpa guard ini -> "Cannot read properties of undefined (reading 'volRel2')" mematikan capture
  // key tsb (terbukti: ETH 5m error tiap sesi). Tier yang hilang = fallback ke tier lebih rendah.
  const gS = T.STRONG || {}, gG = T.GOOD || gS, gF = T.FAIR || gG;
  const grade = (gS.volRel2 != null && volRel2 >= gS.volRel2 && surprise >= (gS.surprise || 0)) ? "STRONG"
    : (gG.volRel2 != null && volRel2 >= gG.volRel2 && surprise >= (gG.surprise || 0)) ? "GOOD"
      : (gF.volRel2 != null && volRel2 >= gF.volRel2 && surprise >= (gF.surprise || 0)) ? "FAIR" : null;
  const typ5m = (VOL_TYPICAL[sym] || 0) * 60;
  const proj = vol2sum * (tfSec / 2);
  const liqMul = profile.liqFloorMul != null ? profile.liqFloorMul : 0.3;
  const floor = Math.max(pctile(prior5, 15), typ5m * liqMul);
  const liqLow = typ5m > 0 && proj < floor;
  const liqRatio = typ5m > 0 ? proj / typ5m : 1;
  const thOK = GATES_DEF.applyThresholds({ volRel2, surprise, liqRatio, gapPct: gateNow, histStrength: histTrend.strength, rsi }, profile.thresholds);
  const accepted0 = !!grade && !liqLow && thOK && rsiOK;
  // ===== VETO no-edge: buang cohort WR~50% (reward kecil, RSI 30-40, vol choppy, jam buruk, liqud tipis)
  let veto = null;
  const pv = { reward: false, rsi: false, vol: false, hour: false, liq: false, rewardMin: 0, liqMin: 0, rsiBadRanges: null, volBadRanges: null, offHours: null }; // KOSMETIK: salinan status kriteria utk power bar
  const wibH = Math.floor(((t0 + 7 * 3600) % 86400) / 3600);                     // jam WIB (dipakai veto & reclaim)
  let P = { rewardMin: 0, liqMin: 0, rsiBad: [], volBad: [], reclaim: [] };      // profil veto key (diisi bila TA_VETO on)
  if (TA_VETO.on) {
    // Jam OFF ADAPTIF dari learner (bila ada); fallback ke default kode.
    const vetoM = (() => { try { return (typeof getModel === "function" ? getModel("veto") : null); } catch (_) { return null; } })();
    // Jam OFF PER coin×TF (objektif). Bila file ada tapi key ini belum punya cukup sampel -> [] (jangan blokir).
    const km = vetoM && vetoM.keys && vetoM.keys[`${sym}_${tf}`];
    const vetoHours = vetoM ? ((km && Array.isArray(km.hours)) ? km.hours : []) : TA_VETO.hours;
    // VETO THRESHOLD PER coin×TF (dari data key ini). Fallback global hanya bila key belum punya profil.
    const kp = vetoM && vetoM.prof && vetoM.prof.keys && vetoM.prof.keys[`${sym}_${tf}`];
    P = kp || { rewardMin: TA_VETO.rewardMin, liqMin: TA_VETO.liqMin, rsiBad: [[TA_VETO.rsiLo, TA_VETO.rsiHi]], volBad: [[TA_VETO.volLo, TA_VETO.volHi]], reclaim: [] };
    // KOSMETIK: salin status kriteria (kondisi SAMA, tidak mengubah rantai veto di bawah)
    pv.rewardMin = P.rewardMin || 0; pv.liqMin = P.liqMin || 0; pv.rsiBadRanges = P.rsiBad; pv.volBadRanges = P.volBad; pv.offHours = vetoHours;
    pv.reward = (P.rewardMin || 0) > 0 && gateNow < P.rewardMin;
    pv.rsi = (P.rsiBad || []).some(([lo, hi]) => rsi != null && rsi >= lo && rsi < hi);
    pv.vol = (P.volBad || []).some(([lo, hi]) => volRel2 >= lo && volRel2 < hi);
    pv.hour = vetoHours.indexOf(wibH) >= 0;
    pv.liq = (P.liqMin || 0) > 0 && liqRatio < P.liqMin;
    if ((P.rewardMin || 0) > 0 && gateNow < P.rewardMin) veto = "veto-reward";
    else if ((P.rsiBad || []).some(([lo, hi]) => rsi != null && rsi >= lo && rsi < hi)) veto = "veto-rsi";
    else if ((P.volBad || []).some(([lo, hi]) => volRel2 >= lo && volRel2 < hi)) veto = "veto-vol";
    else if (vetoHours.indexOf(wibH) >= 0) veto = "veto-hour";
    else if ((P.liqMin || 0) > 0 && liqRatio < P.liqMin) veto = "veto-liq";
  }
  // ===== INVERT: konteks yg arah mentahnya TERBUKTI biasanya SALAH -> BALIK arah (up<->down) =====
  // Diterapkan SEBELUM gate, hanya bila konteks tervalidasi ketat (Wilson-LB flipped >=0.55, 2 paruh, n>=40).
  let inverted = null;
  if (!flatReason && (currentDir === "up" || currentDir === "down") && !liqLow && Array.isArray(P.invert) && P.invert.length) {
    const _vi = { rsi, volRel2, gapPct: gateNow, surprise, histStrength: histTrend.strength, hourWIB: wibH };
    for (const c of P.invert) {
      const ok = c.f === "dir" ? (currentDir === c.v) : c.f === "grade" ? (grade === c.v)
        : c.f === "hourWIB" ? (wibH >= c.lo && wibH < c.hi) : (_vi[c.f] != null && _vi[c.f] >= c.lo && _vi[c.f] < c.hi);
      if (ok) { inverted = c; break; }
    }
    if (inverted) currentDir = (currentDir === "up") ? "down" : "up";
  }
  let accepted = accepted0 && !veto;
  let reject = accepted ? null : (veto || (!rsiOK ? "rsi-out" : (!thOK ? "threshold" : !grade ? "tier" : "liq-low")));

  const d2 = ((C2 - lock) / lock) * 100;
  const rewardPct = Math.abs(d2);
  const gapB = LEARNER_BUCKETS.bGap(rewardPct);
  const hourB = LEARNER_BUCKETS.bHour(new Date(t0 * 1000).getUTCHours());
  const trend = SignalCore.sessionTrend(tfc.slice(0, idx + 1), 3);
  // konteks learner (dari model yang sedang dipakai) — sama artinya dengan panel app
  let touchRate = null, learn = { trend, gap: gapB, hour: hourB };
  try {
    if (typeof getModel === "function") {
      const gM = getModel("gate"), tM = getModel("touch");
      const g = (gM && gM.byKey) ? gM.byKey[_key] : gM;      // PER key (independen)
      const t = (tM && tM.byKey) ? tM.byKey[_key] : tM;
      const tb = t && t.buckets && t.buckets.gap ? t.buckets.gap[gapB] : null;
      if (tb && tb.nTest >= 200) touchRate = tb.touchTest;
      const iv = g && g.buckets && g.buckets.interval ? g.buckets.interval[tf] : null;
      const mi = g && g.buckets && g.buckets.minute ? g.buckets.minute["1"] : null;
      const ivWR = iv && iv.nTest >= 200 ? iv.wrTest : null;
      const miWR = mi && mi.nTest >= 200 ? mi.wrTest : null;
      const weak = [], strong = [];
      if (ivWR != null && ivWR < 0.66) weak.push("interval");
      if (touchRate != null && touchRate < 0.62) weak.push("touch");
      if (touchRate != null && touchRate >= 0.72) strong.push("touch");
      if (miWR != null && miWR >= 0.70) strong.push("dir");
      // Terapkan aturan penahan learner yang DIKENAL saat lock (tunggal; kunci: interval/symbol/dir/hour/gap/mode).
      // Dulu hanya `interval=`/`gap=` yang diakui -> aturan `hour=`/`symbol=` hasil mining TAK PERNAH diterapkan.
      // LANGKAH CAKUPAN: bila model memblok terlalu banyak, flag apply=false -> jangan terapkan.
      const applyWhole = (() => { try { return (typeof getModel === "function" ? getModel("apply") : null); } catch (_) { return null; } })();
      const applyM = (applyWhole && applyWhole.byKey) ? applyWhole.byKey[_key] : applyWhole;   // PER key
      const applyBlockers = !applyM || applyM.apply !== false;
      const blocking = (!applyBlockers) ? false : (() => {
        const rules = [].concat((g && g.suppress) || [], (t && t.suppress) || []);
        const featVal = { interval: tf, symbol: sym, dir: currentDir, hour: hourB, gap: gapB, mode };
        return rules.some((k) => {
          if (typeof k !== "string" || k.indexOf("&") !== -1) return false;
          const i = k.indexOf("="); if (i < 0) return false;
          const f = k.slice(0, i);
          return featVal[f] != null && String(featVal[f]) === k.slice(i + 1);
        });
      })();
      learn = Object.assign(learn, {
        touch: touchRate, dirWR: miWR, intervalWR: ivWR, blocking,
        label: weak.length && !strong.length ? "LEMAH" : strong.length && !weak.length ? "KUAT" : weak.length ? "CAMPURAN" : "NETRAL",
      });
    }
  } catch (_) {}
  // ===== TERAPKAN MODEL YANG DIPROMOSIKAN LEARNER =====
  // Sebelumnya `learn.blocking` hanya label (tak memblokir). Sekarang bila model terpromosi
  // menandai konteks ini (mis. gap<0.005) -> tolak sinyal, supaya pembelajaran benar-benar menajamkan.
  if (learn && learn.blocking && accepted) { accepted = false; reject = reject || "learn-block"; }

  // ===== RECLAIM: konteks "tanpa sinyal" yang NYATA WIN (validasi Wilson-LB learner) -> ON-kan kembali.
  // Meng-override veto/tier HANYA bila arah ada & bukan liqLow. Menyeimbangkan veto agar produksi tak menutup.
  let reclaim = null;
  if (!accepted && currentDir && !liqLow && Array.isArray(P.reclaim) && P.reclaim.length) {
    const _v = { rsi, volRel2, gapPct: gateNow, surprise, histStrength: histTrend.strength, hourWIB: wibH };
    for (const c of P.reclaim) {
      if (c.f === "dir") { if (currentDir === c.v) reclaim = c; }
      else if (c.f === "grade") { if (grade === c.v) reclaim = c; }
      else { const x = _v[c.f]; if (x != null && x >= c.lo && x < c.hi) reclaim = c; }
      if (reclaim) break;
    }
    if (reclaim) { accepted = true; reject = null; }
  }

  // ===== POWER SINYAL U/D (KOSMETIK — tidak mengubah accepted/reject) =====
  // 100% = tepat di ambang minimum utk menghasilkan sinyal; >100% = melampaui; <100% = ada kriteria belum terpenuhi.
  let power = null;
  try {
    const parts = [];
    const add = (label, ratio, met) => parts.push({ label, ratio: +Math.max(0, Math.min(3, ratio)).toFixed(3), met: !!met });
    const FV = (T && T.FAIR && T.FAIR.volRel2) || 0, FS = (T && T.FAIR && T.FAIR.surprise) || 0;
    add("tier volRel2", FV > 0 ? volRel2 / FV : (volRel2 > 0 ? 1.001 : 1), volRel2 >= FV);
    add("tier surprise", FS > 0 ? surprise / FS : (surprise > 0 ? 1.001 : 1), surprise >= FS);
    const FVv = { volRel2, surprise, liqRatio, gapPct: gateNow, histStrength: histTrend.strength, rsi };
    for (const th of (profile.thresholds || [])) { const v = FVv[th.f]; if (v == null || !(th.t > 0)) continue; const r = th.op === ">=" ? v / th.t : th.t / v; add("ambang " + th.f, r, th.op === ">=" ? v >= th.t : v <= th.t); }
    if ((pv.rewardMin || 0) > 0) add("reward", gateNow / pv.rewardMin, gateNow >= pv.rewardMin);
    if ((pv.liqMin || 0) > 0) add("likuiditas", liqRatio / pv.liqMin, liqRatio >= pv.liqMin);
    add("rsi", pv.rsi ? 0 : 1, !pv.rsi);
    add("vol choppy", pv.vol ? 0 : 1, !pv.vol);
    add("jam sesi", pv.hour ? 0 : 1, !pv.hour);
    add("liqLow", liqLow ? 0 : 1, !liqLow);
    add("learner-block", (learn && learn.blocking) ? 0 : 1, !(learn && learn.blocking));
    const psum = parts.reduce((a, x) => a + x.ratio, 0);
    const metN = parts.filter((p) => p.met).length;
    let pct = parts.length ? 100 * psum / parts.length : (accepted ? 100 : 0);
    if (parts.length && metN < parts.length) pct = Math.min(pct, 100 * metN / parts.length);  // ada yg belum terpenuhi -> <100%
    power = { pct: Math.round(pct), parts, accepted: !!accepted, allMet: metN === parts.length };
  } catch (_) {}

  return {
    ok: true,
    skipped: flatReason || undefined,
    signal: {
      asset: sym, interval: tf, t0, lock,
      power,
      dir: (flatReason === "flat-price") ? null : currentDir, mode, conf,
      grade: grade || null, accepted: flatReason ? false : accepted, reject: flatReason ? null : reject, thresholdsOK: !!thOK,
      reclaim: (reclaim && !flatReason) ? { f: reclaim.f, v: reclaim.v, lo: reclaim.lo, hi: reclaim.hi, n: reclaim.n, lb: reclaim.lb } : null,
      invert: (inverted && !flatReason) ? { f: inverted.f, v: inverted.v, lo: inverted.lo, hi: inverted.hi, n: inverted.n, flipWR: inverted.flipWR, lbFlip: inverted.lbFlip } : null,
      skipped: flatReason || null,
      volRel2: +volRel2.toFixed(4), surprise: +surprise.toFixed(4), mv2: +mv2.toFixed(5),
      rsi: rsi != null ? +rsi.toFixed(2) : null, histStrength: histTrend.strength,
      ind,   // RECORDER: EMA9/EMA21 (crossover) + MACD + pola candle 5m — untuk uji jendela panjang
      rewardPct: +rewardPct.toFixed(4), liqRatio: +liqRatio.toFixed(3), liqLow: !!liqLow,
      touchRate, gateKey: `${tf}|${mode}|${currentDir}|rsi:${rsiBucket(rsi)}|str:${strBucket(histTrend.strength)}`,
      micro: {
        o1BodyS: +o1BodyS.toFixed(5), o2BodyS: +o2BodyS.toFixed(5), bodyAgree, sigma1s: +sigma1s.toFixed(5),
        preHigh, preLow, ranPos: ranPos != null ? +ranPos.toFixed(3) : null,
        baseVol: +baseVol.toFixed(2), vol2sum: +vol2sum.toFixed(3), proj: +proj.toFixed(2), align: align || null,
      },
      // OFI sesi dari flow server (null = belum ada data). Ikut payload -> semua device
      // menampilkan angka yang SAMA, dan ikut terekam ke ledger/learner.
      ofi: FLOW.sessionOFI(sym, t0, nowSec),
      learn,
    },
  };
}

const FLOW = require("./flow.js");

function createCapture(deps) {
  const { getKlines, SignalCore, getModel, save, log = console.log, getGates } = deps;
  let stats = { enabled: true, captured: 0, accepted: 0, rejected: 0, skipped: 0, errors: 0, lastAt: null, lastErr: null, lastKey: null };

  async function captureOne(sym, tf, t0) {
    const tfSec = DUR_S[tf];
    if (!tfSec) return { skipped: "bad-tf" };
    if (!SignalCore) return { skipped: "no-core" };
    const tfc = await getKlines(sym, tf, t0 + tfSec - 1, 60);
    const idx = tfc.findIndex((c) => c.time === t0);
    if (idx < 26) return { skipped: "no-session" };
    const lock = tfc[idx].open;
    if (!lock) return { skipped: "no-lock" };
    const ones = await getKlines(sym, "1s", t0 + 2, 70);
    FLOW.addKlines(sym, ones);                              // OFI dari REST (dedupe -> aman dipanggil terus)
    const nowSec = Math.floor(Date.now() / 1000);
    let five5m = [];
    try { five5m = await getKlines(sym, "5m", nowSec - 1, 60); } catch (_) {}
    const profile = (typeof getGates === "function" ? getGates() : null) || GATES_DEF.BOOTSTRAP;   // profil gate yang BERLAKU (bootstrap/learned)
    // Alignment multi-TF (arah tren tf lain SEBELUM t0) — fitur model arah U/D (jalur capture juga)
    let align = null;
    try {
      align = {};
      for (const t of Object.keys(DUR_S)) {
        if (sym === "BNB" && t !== "5m") continue;
        const arr = (t === tf ? tfc : await getKlines(sym, t, t0 - 1, 60)).filter((c) => c.time < t0);
        if (arr.length >= 20) align[t] = SignalCore.analyzeHistoricalTrend(arr, 50).predictDir;
      }
    } catch (_) {}
    const r = computeSignal({ sym, tf, t0, tfc, idx, ones, five5m, profile, getModel, SignalCore, nowSec, align });
    if (r.skipped) {
      // Sesi FLAT/NOISE: rekam INFORMASIONAL dgn objek sinyal PENUH (field SAMA seperti sesi bersinyal:
      // rsi/micro/liqRatio/rewardPct/gate/power/ofi). Dulu hanya `flat-price` yg disimpan & objek ringkas
      // -> sesi tanpa sinyal tak lengkap / hilang. Selain yg benar-benar tanpa data (no-1s dll) -> disimpan.
      if ((r.skipped === "flat-price" || r.skipped === "flat-noise") && r.signal) {
        const sig = r.signal;
        const rec = { k: `${sym}_${tf}_${t0}`, asset: sym, interval: tf, t0, src: "server",
          sig: Object.assign({}, sig, { skipped: r.skipped, capOffsetMs: 2000, capturedAt: nowSec, prov: "server", minuteIn: 1, gateWr: null }),
          gate: { grade: sig.grade, liqLow: sig.liqLow, liqRatio: sig.liqRatio, thresholdsOK: sig.thresholdsOK, accepted: false, reject: null, profile: profile.mode } };
        save(rec, "server"); stats.captured++; stats.skipped++; stats.lastAt = Date.now(); stats.lastKey = rec.k;
        return { ok: true, flat: true, rec };
      }
      return { skipped: r.skipped };
    }
    const sig = r.signal;
    const rec = {
      k: `${sym}_${tf}_${t0}`, asset: sym, interval: tf, t0, src: "server",
      sig: Object.assign({}, sig, {
        capOffsetMs: 2000, capturedAt: nowSec, prov: "server", minuteIn: 1, ofi: (sig.ofi != null ? sig.ofi : null), gateWr: null,
      }),
      // Keputusan gate saat perekaman: untuk membandingkan populasi diterima vs ditolak.
      gate: { grade: sig.grade, liqLow: sig.liqLow, liqRatio: sig.liqRatio, thresholdsOK: sig.thresholdsOK, accepted: sig.accepted, reject: sig.reject, reclaim: sig.reclaim || null, invert: sig.invert || null, profile: profile.mode },
    };
    save(rec, "server");
    stats.captured++; stats.lastAt = Date.now(); stats.lastKey = rec.k;
    return { ok: true, rec };
  }

  const done = new Set();
  let busy = false;
  async function tick() {
    if (!stats.enabled || busy) return;
    busy = true;
    try {
      const now = Date.now();
      for (const sym of ["BTC", "ETH", "BNB"]) {
        for (const tf of Object.keys(DUR_S)) {
          if (sym === "BNB" && tf !== "5m") continue;         // BNB hanya 5m (selaras engine/bot)
          const durMs = DUR_S[tf] * 1000;
          const t0 = Math.floor(now / durMs) * durMs;
          const off = now - t0;
          if (off < 3500 || off > 20000) continue;            // hanya dekat awal sesi
          const key = `${sym}_${tf}_${t0 / 1000}`;
          if (done.has(key)) continue;
          done.add(key);
          if (done.size > 4000) { const it = done.values(); for (let i = 0; i < 2000; i++) done.delete(it.next().value); }
          try {
            const r = await captureOne(sym, tf, t0 / 1000);
            if (r && r.skipped) { stats.skipped++; log(`[CAPTURE] ${sym} ${tf} dilewati: ${r.skipped}`); }
            else if (r && r.ok && r.flat) {
              stats.flat = (stats.flat || 0) + 1;
              log(`[CAPTURE] ${sym} ${tf} ${r.rec.k} FLAT (informasional, tanpa sinyal/trade)`);
            }
            else if (r && r.ok) {
              if (r.rec.gate && r.rec.gate.accepted) stats.accepted = (stats.accepted || 0) + 1; else stats.rejected = (stats.rejected || 0) + 1;
              log(`[CAPTURE] ${sym} ${tf} ${r.rec.k} dir=${r.rec.sig.dir} grade=${r.rec.sig.grade || "-"} volRel2=${r.rec.sig.volRel2} surprise=${String(r.rec.sig.surprise).slice(0, 6)} gap=${(r.rec.sig.learn && r.rec.sig.learn.gap) || "-"} ${r.rec.gate.accepted ? "DITERIMA" : "DITOLAK:" + (r.rec.gate.reject || "-")}`);
            }
          } catch (e) { stats.errors++; stats.lastErr = e && e.message; log(`[CAPTURE] gagal ${sym} ${tf}: ${e && e.message}`); }
        }
      }
    } finally { busy = false; }
  }

  function start() {
    if (process.env.CAPTURE === "0") { stats.enabled = false; log("[CAPTURE] dinonaktifkan (CAPTURE=0)"); return; }
    setInterval(() => tick().catch(() => {}), 1000);
    log(`[CAPTURE] aktif — snapshot kanonik 2 detik untuk ${Object.keys(DUR_S).join(", ")}`);
  }
  return { start, tick, captureOne, status: () => stats };
}

module.exports = { createCapture, computeSignal, DUR_S, VOL_TYPICAL };
