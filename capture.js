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
const EXP_GATE = require("./exp-gate.js");
const EXT = require("./ext-features.js");   // fitur eksternal (Batch 1) — direkam ke sig.ext

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

/* ===== ANTI-SNOWBALL: ADAPTIVE COVERAGE FLOOR (ACF) =====
   Mencegah efek snowball (blok menumpuk -> sinyal habis -> data habis -> makin memblok).
   Melacak rasio PENERIMAAN per key dari sesi-sesi terakhir. Bila cakupan jatuh di bawah lantai,
   filter dilonggarkan BERTINGKAT & DINAMIS, lalu pulih otomatis saat cakupan kembali normal:
     level 1 : matikan learn-block + selective-score gate
     level 2 : + matikan VETO (rsi/vol/reward/liq/jam + key-EV/regime)
     level 3 : + buang syarat TIER/threshold (eksplorasi: terima bila arah ada & !liqLow)
   Key tanpa profil (no-key-profile) langsung level 3 (eksplorasi) agar bisa mengumpulkan $ akun.
   Kill-switch: ACF=0. Tuning: ACF_WINDOW (default 24), ACF_MIN (min sesi sebelum aktif, 8). */
const ACF_ON = process.env.ACF !== "0";
const ACF_WINDOW = Math.max(8, parseInt(process.env.ACF_WINDOW || "24", 10));
const ACF_MIN = Math.max(6, parseInt(process.env.ACF_MIN || "8", 10));
const _acf = (() => {
  const hist = {};   // key -> [{t0, a}]
  function level(key, noProfile) {
    if (!ACF_ON) return 0;
    if (noProfile) return 3;
    const h = hist[key] || [];
    const recent = h.slice(-ACF_WINDOW);
    if (recent.length < ACF_MIN) return 0;
    const acc = recent.reduce((s, x) => s + x.a, 0);
    if (acc === 0) return 3;
    const rate = acc / recent.length;
    return rate < 0.02 ? 3 : rate < 0.05 ? 2 : rate < 0.12 ? 1 : 0;
  }
  function record(key, t0, accepted) {
    const h = hist[key] || (hist[key] = []);
    const last = h[h.length - 1];
    if (last && last.t0 === t0) { last.a = accepted ? 1 : 0; return; }   // dedupe (engine & capture)
    h.push({ t0, a: accepted ? 1 : 0 });
    if (h.length > 400) h.splice(0, h.length - 400);
  }
  return { level, record, debug: () => hist };
})();

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
  const { sym, tf, t0, tfc, idx, ones, five5m, one1m, getModel, SignalCore, nowSec } = o;
  const _key = sym + "_" + tf;
  // PER coin × TF — STRICT: profil HANYA milik key ini (TIDAK ada fallback global/BOOTSTRAP).
  // Bila key belum punya profil hasil belajar -> sesi DITOLAK (belum ada bukti untuk key ini),
  // BUKAN memakai ambang global. Refit (<=1 jam) membuat profil per key dari data key itu sendiri.
  const _allProf = o.profile || null;
  let profile = (_allProf && _allProf.byKey) ? (_allProf.byKey[_key] || null) : null;
  // ANTI-SNOWBALL: key tanpa profil TIDAK ditolak total -> mode EKSPLORASI (level 3) supaya bisa
  // mengumpulkan $ akun. Semua filter tetap per-key (bukan ambang global). Refit lalu membangun profil.
  const noProfile = !profile;
  if (noProfile) profile = {};
  const explore = _acf.level(_key, noProfile);   // 0..3 (dinamis; naik saat cakupan rendah, turun saat pulih)
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
  // ADAPTIF PER KEY (learner: learn_mv2.json). Fallback env MIN_MV2_PCT bila model belum ada.
  const _mv2M = (() => { try { const m = (typeof getModel === "function") ? getModel("mv2") : null; return (m && m.byKey && m.byKey[_key]) ? m.byKey[_key] : null; } catch (_) { return null; } })();
  const MIN_MV2_PCT = (_mv2M && typeof _mv2M.minMv2 === "number") ? _mv2M.minMv2 : Number(process.env.MIN_MV2_PCT != null ? process.env.MIN_MV2_PCT : 0.015);
  const flatReason = (currentDir === "flat") ? "flat-price" : ((MIN_MV2_PCT > 0 && mv2 < MIN_MV2_PCT) ? "flat-noise" : null);
  // ===== ARAH "SILENT" (permintaan user): SETIAP sesi tetap punya arah utk direkam & DIPELAJARI — 
  // walau tidak layak entry (flat). Tujuannya learner bisa belajar MANANG/KALAH di tiap sesi.
  // Ini TIDAK mengubah accepted (flat tetap accepted=false) & TIDAK ditampilkan sebagai rekomendasi.
  let silentDir = false;
  if (flatReason === "flat-price") {
    let sd = null;
    if (o1BodyS !== 0) sd = o1BodyS > 0 ? "up" : "down";
    else if (o2BodyS !== 0) sd = o2BodyS > 0 ? "up" : "down";
    else if (ranPos != null) sd = ranPos >= 0.5 ? "up" : "down";
    else if (histTrend && histTrend.predictDir === "up") sd = "up";
    else if (histTrend && histTrend.predictDir === "down") sd = "down";
    else if (align && align["5m"]) sd = align["5m"];
    if (sd === "up" || sd === "down") { currentDir = sd; silentDir = true; }
  }
  // rsi dari candle 5m yang SUDAH SELESAI (tanpa lookahead)
  // ===== RECORDER 1m RSI + Stochastic (pola ind; observasional) — uji hipotesis reversal 1m =====
  let ind1m = null;
  try {
    const c1 = (one1m || []).filter((c) => c.time + 60 <= nowSec);   // candle 1m SUDAH selesai (no lookahead)
    if (c1.length >= 20) {
      const rsi1m = SignalCore.rsiFromSeries(c1.slice(-50), 14);
      const kArr = [];
      for (let i = 13; i < c1.length; i++) { const w = c1.slice(i - 13, i + 1); const hh = Math.max(...w.map((x) => x.high)), ll = Math.min(...w.map((x) => x.low)); kArr.push(hh > ll ? ((c1[i].close - ll) / (hh - ll)) * 100 : 50); }
      const stochK = kArr.length ? kArr[kArr.length - 1] : null;
      const stochD = kArr.length >= 3 ? (kArr.slice(-3).reduce((a, b) => a + b, 0) / 3) : null;
      ind1m = { rsi: rsi1m != null ? +rsi1m.toFixed(2) : null, stochK: stochK != null ? +stochK.toFixed(2) : null, stochD: stochD != null ? +stochD.toFixed(2) : null };
    }
  } catch (_) {}
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
  const T = profile.tiers || {};   // STRICT per-key: tanpa fallback tier global (key tanpa tier -> ditolak sbg "tier")
  const gateNow = Math.abs((C2 - lock) / lock) * 100;
  // GUARD: profil per-key (dari learner/keyTiers) bisa TIDAK punya semua tier (STRONG/GOOD/FAIR).
  // Tanpa guard ini -> "Cannot read properties of undefined (reading 'volRel2')" mematikan capture
  // key tsb (terbukti: ETH 5m error tiap sesi). Tier yang hilang = fallback ke tier lebih rendah.
  const gS = T.STRONG || {}, gG = T.GOOD || gS, gF = T.FAIR || gG;
  let grade = (gS.volRel2 != null && volRel2 >= gS.volRel2 && surprise >= (gS.surprise || 0)) ? "STRONG"
    : (gG.volRel2 != null && volRel2 >= gG.volRel2 && surprise >= (gG.surprise || 0)) ? "GOOD"
      : (gF.volRel2 != null && volRel2 >= gF.volRel2 && surprise >= (gF.surprise || 0)) ? "FAIR" : null;
  const typ5m = (VOL_TYPICAL[sym] || 0) * 60;
  const proj = vol2sum * (tfSec / 2);
  const liqMul = profile.liqFloorMul != null ? profile.liqFloorMul : 0.3;
  const floor = Math.max(pctile(prior5, 15), typ5m * liqMul);
  const liqLow = typ5m > 0 && proj < floor;
  const liqRatio = typ5m > 0 ? proj / typ5m : 1;
  const thOK = GATES_DEF.applyThresholds({ volRel2, surprise, liqRatio, gapPct: gateNow, histStrength: histTrend.strength, rsi }, profile.thresholds);
  // MODE AGRESIF (env GATE_REQUIRE_GRADE=0): buang syarat grade -> terima bila tidak $-diveto.
  // Backtest (hold-to-settle): "veto-saja" compound paling tinggi (bal 382 vs 238 selektif).
  const REQUIRE_GRADE = process.env.GATE_REQUIRE_GRADE !== "0";
  let accepted0 = (REQUIRE_GRADE ? !!grade : true) && !liqLow && thOK && rsiOK;
  if (explore >= 3) accepted0 = !liqLow && rsiOK;   // ACF: eksplorasi (buang syarat tier/threshold)
  // ===== VETO no-edge: buang cohort WR~50% (reward kecil, RSI 30-40, vol choppy, jam buruk, liqud tipis)
  let veto = null;
  const pv = { reward: false, rsi: false, vol: false, hour: false, liq: false, rewardMin: 0, liqMin: 0, rsiBadRanges: null, volBadRanges: null, offHours: null }; // KOSMETIK: salinan status kriteria utk power bar
  const wibH = Math.floor(((t0 + 7 * 3600) % 86400) / 3600);                     // jam WIB (dipakai veto & reclaim)
  let P = { rewardMin: 0, liqMin: 0, rsiBad: [], volBad: [], reclaim: [] };      // profil veto key (diisi bila TA_VETO on)
  if (TA_VETO.on) {
    // Jam OFF ADAPTIF dari learner (per key). STRICT: tanpa jam OFF global.
    const vetoM = (() => { try { return (typeof getModel === "function" ? getModel("veto") : null); } catch (_) { return null; } })();
    // Jam OFF PER coin×TF (objektif). Bila key ini belum punya cukup sampel -> [] (jangan blokir).
    const km = vetoM && vetoM.keys && vetoM.keys[`${sym}_${tf}`];
    const vetoHours = (km && Array.isArray(km.hours)) ? km.hours : [];
    // VETO THRESHOLD PER coin×TF (dari data key ini). STRICT: tanpa veto global (key belum punya profil -> netral).
    const kp = vetoM && vetoM.prof && vetoM.prof.keys && vetoM.prof.keys[`${sym}_${tf}`];
    P = kp || { rewardMin: 0, liqMin: 0, rsiBad: [], volBad: [], reclaim: [] };
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
  // ===== PER-KEY $ EV GATE (rolling): blokir SELURUH key bila EV $ nyata-nya negatif =====
  if (P.keyEvGated) veto = veto || "key-ev";
  // ===== KONTEKS MULTI-TF & OFI (nilai HARUS SAMA dgn rowsFrom learner) =====
  // mAlign = jumlah TF (5m/15m/1h) yg tren-nya searah dir; mOfiAgree = 1/0 OFI mendukung dir.
  // Dipakai reclaim/invert/confirm. Dihitung SEKALI agar rule learner berbasis fitur ini TIDAK "mati"
  // (dulu _v/_vi tak memuatnya -> rule mAlign/mOfiAgree = dead rule saat diterapkan live).
  const _alignNow = align || {};
  const _mAlignNow = ["5m", "15m", "1h"].filter((t) => _alignNow[t] === currentDir).length;
  const _ofiNow = (typeof FLOW !== "undefined" && FLOW.sessionOFI) ? FLOW.sessionOFI(sym, t0, nowSec) : null;
  const _mOfiNow = (_ofiNow != null) ? (((currentDir === "up" && _ofiNow > 0.05) || (currentDir === "down" && _ofiNow < -0.05)) ? 1 : 0) : null;
  // ===== INVERT: konteks yg arah mentahnya TERBUKTI biasanya SALAH -> BALIK arah (up<->down) =====
  // Diterapkan SEBELUM gate, hanya bila konteks tervalidasi ketat (Wilson-LB flipped >=0.55, 2 paruh, n>=40).
  let inverted = null;
  if (!flatReason && (currentDir === "up" || currentDir === "down") && !liqLow && Array.isArray(P.invert) && P.invert.length) {
    const _vi = { rsi, volRel2, gapPct: gateNow, surprise, histStrength: histTrend.strength, hourWIB: wibH, mAlign: _mAlignNow, mOfiAgree: _mOfiNow };
    const partOK = (p) => p.f === "dir" ? (currentDir === p.v) : p.f === "grade" ? (grade === p.v)
      : p.f === "hourWIB" ? (wibH >= p.lo && wibH < p.hi) : (_vi[p.f] != null && _vi[p.f] >= p.lo && _vi[p.f] < p.hi);
    for (const c of P.invert) { if ((c.and || []).every(partOK)) { inverted = c; break; } }
    if (inverted) currentDir = (currentDir === "up") ? "down" : "up";
  }
  // ===== CONFIRM: BOOST GRADE 1 LEVEL bila konteks terkonfirmasi (tren multi-TF/OFI) PER-KEY =====
  // Learner per-coin memvalidasi konteks ini (Wilson-LB + OOS). Hanya MENAIKKAN grade (memperkuat
  // produksi sinyal), BUKAN flip & BUKAN override veto. Kosong bila key belum punya bukti.
  let confirmed = null;
  // GUARD ANTI-KONFLIK: jangan terapkan confirm bila arah sudah di-INVERT (konteks confirm dilatih
  // pada arah MENTAH; mengaplikasikannya ke arah terbalik akan salah arti).
  if (!flatReason && !inverted && (currentDir === "up" || currentDir === "down") && Array.isArray(P.confirm) && P.confirm.length) {
    const _vc = { mAlign: _mAlignNow, mOfiAgree: _mOfiNow };
    for (const c of P.confirm) { if ((c.and || []).every((p) => _vc[p.f] != null && _vc[p.f] >= p.lo && _vc[p.f] < p.hi)) { confirmed = c; break; } }
    if (confirmed) {
      const _prev = grade;
      grade = grade === "STRONG" ? "STRONG" : grade === "GOOD" ? "STRONG" : grade === "FAIR" ? "GOOD" : "FAIR";
      accepted0 = (REQUIRE_GRADE ? !!grade : true) && !liqLow && thOK && rsiOK;
      if (explore >= 3) accepted0 = !liqLow && rsiOK;   // ACF: eksplorasi
      if (grade !== _prev) confirmed = Object.assign({}, confirmed, { from: _prev || null, to: grade });
    }
  }
  let accepted = accepted0 && (explore >= 2 ? true : !veto);   // ACF>=2: abaikan veto (termasuk key-EV/regime)
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
      // Terapkan aturan penahan learner yang DIKENAL saat lock (single-feature; bucket SAMA dgn learner/DECIDE_FIELD).
      // Dulu hanya `interval=`/`gap=`/`hour=`/`symbol=`/`dir=`/`mode=` yang diakui -> aturan rsi/vol/hist/trend
      // hasil mining mean-$ TAK PERNAH diterapkan. Sekarang semua bucket dihitung live agar rule benar-benar berlaku.
      const applyWhole = (() => { try { return (typeof getModel === "function" ? getModel("apply") : null); } catch (_) { return null; } })();
      const applyM = (applyWhole && applyWhole.byKey) ? applyWhole.byKey[_key] : applyWhole;   // PER key
      const applyBlockers = !applyM || applyM.apply !== false;
      // Rule penahan: gate/touch (butuh applyMap.apply) + WONBLOCK (blocker outcome per-key, SELALU berlaku).
      const _won = (() => { try { const m = (typeof getModel === "function") ? getModel("wonblock") : null; return (m && m.byKey && m.byKey[_key] && m.byKey[_key].rules) || []; } catch (_) { return []; } })();
      const _al = align || {};
      const _mO2 = (sigma1s > 0) ? (o2BodyS / sigma1s) : null;
      const featVal = {
        interval: tf, symbol: sym, dir: currentDir, hour: hourB, gap: gapB, mode,
        rsi: LEARNER_BUCKETS.bRsi(rsi), vol: LEARNER_BUCKETS.bVol(volRel2),
        hist: LEARNER_BUCKETS.bHist(histTrend.strength), trend: trend || "na", minute: LEARNER_BUCKETS.bMinute(1),
        mv2: LEARNER_BUCKETS.bMv2(typeof mv2 === "number" ? mv2 : null),
        // mikro-struktur (B): sama dgn bucketing di learner.DECIDE_FIELD (harus IDENTIK agar rule match)
        mAlignB: (align && Object.keys(align).length) ? String(["5m", "15m", "1h"].filter((t) => _al[t] === currentDir).length) : "na",
        mAgreeB: (bodyAgree != null) ? (bodyAgree ? "agree" : "disagree") : "na",
        mRanZone: (ranPos != null) ? (ranPos < 0.2 ? "low" : ranPos > 0.8 ? "high" : "mid") : "na",
        mBody: (_mO2 == null) ? "na" : (_mO2 > 0.5 ? "strong+" : _mO2 < -0.5 ? "strong-" : "weak"),
      };
      // Dukung rule tunggal MAUPUN interaksi "f1=v1&f2=v2" (B13).
      const matchK = (k) => {
        if (typeof k !== "string") return false;
        const parts = k.split("&").map((p) => { const i = p.indexOf("="); return i < 0 ? null : { f: p.slice(0, i), v: p.slice(i + 1) }; }).filter(Boolean);
        if (!parts.length) return false;
        return parts.every((p) => featVal[p.f] != null && String(featVal[p.f]) === p.v);
      };
      const gateBlocked = applyBlockers && [].concat((g && g.suppress) || [], (t && t.suppress) || []).some(matchK);
      const wonBlocked = _won.some((r) => matchK(r && r.k ? r.k : r));
      const blocking = gateBlocked || wonBlocked;
      const blockingSrc = gateBlocked ? "gate" : (wonBlocked ? "won" : "");
      learn = Object.assign(learn, {
        touch: touchRate, dirWR: miWR, intervalWR: ivWR, blocking, wonBlock: wonBlocked, blockSrc: blockingSrc || undefined,
        label: weak.length && !strong.length ? "LEMAH" : strong.length && !weak.length ? "KUAT" : weak.length ? "CAMPURAN" : "NETRAL",
      });
    }
  } catch (_) {}
  // (learn-block diterapkan SETELAH loosen/flatEntry/rsi1m — lihat bawah — agar mengikat; ACF>=1 mematikannya.)

  // ===== RECLAIM: konteks "tanpa sinyal" yang NYATA WIN (validasi Wilson-LB learner) -> ON-kan kembali.
  // Meng-override veto/tier HANYA bila arah ada & bukan liqLow. Menyeimbangkan veto agar produksi tak menutup.
  let reclaim = null;
  // GUARD ANTI-KONFLIK: reclaim (termasuk part `dir`) juga dilatih pada arah MENTAH -> jangan
  // terapkan saat arah sudah di-INVERT.
  if (!accepted && !inverted && currentDir && !liqLow && Array.isArray(P.reclaim) && P.reclaim.length) {
    const _v = { rsi, volRel2, gapPct: gateNow, surprise, histStrength: histTrend.strength, hourWIB: wibH, mAlign: _mAlignNow, mOfiAgree: _mOfiNow };
    for (const c of P.reclaim) {
      if (c.f === "dir") { if (currentDir === c.v) reclaim = c; }
      else if (c.f === "grade") { if (grade === c.v) reclaim = c; }
      else { const x = _v[c.f]; if (x != null && x >= c.lo && x < c.hi) reclaim = c; }
      if (reclaim) break;
    }
    if (reclaim) { accepted = true; reject = null; }
  }

  // (POWER dihitung di AKHIR fungsi — setelah semua kriteria penolakan dievaluasi; lihat bawah.)

  // ===== FLAT_ENTRY: entry utk sesi TANPA signal U/D (flat-noise) dgn filter rsi =====
  // Backtest OOS: flat-noise & rsi<40 -> +42%/trade. Hanya bila env FLAT_ENTRY=1 dan key lolos gate flat ($).
  let flatEntry = false;
  if (process.env.FLAT_ENTRY === "1" && flatReason === "flat-noise" && (currentDir === "up" || currentDir === "down")
      && !liqLow && rsi != null && rsi < Number(process.env.FLAT_ENTRY_RSI || 40) && P.flatOk === true) {
    flatEntry = true; accepted = true; reject = null;   // FIX: accept path harus bersihkan reject (cegah record accepted=true + reject=veto-* yang kontradiktif)
  }
  // ===== LOOSEN ke arah MOM (EKSPERIMEN) — kill switch GATE_LOOSEN_MOM (default OFF) =====
  // MOM = terima SEMUA sesi berarah NON-FLAT (bypass tier/veto), kecuali (opsional) likuiditas tipis.
  // Default OFF -> perilaku produksi TIDAK berubah sampai env diaktifkan. Reversibel tanpa deploy.
  let loosen = false;
  if (process.env.GATE_LOOSEN_MOM === "1" && !flatReason && (currentDir === "up" || currentDir === "down")
      && !(process.env.GATE_LOOSEN_KEEP_LIQ === "1" && liqLow)) {
    accepted = true; reject = null; loosen = true;
  }
  // ===== GATE 1m RSI/Stoch (EKSPERIMEN) — buang momentum saat 1m JENUH-BELI (WR<53%). =====
  // Kill switch RSI1M_GATE (default OFF). Oversold (rsi1m<=30/stochK<=20) tetap lolos (WR ~78-79%).
  let rsi1mGated = false;
  if (process.env.RSI1M_GATE === "1" && !flatReason && (currentDir === "up" || currentDir === "down") && ind1m
      && ((ind1m.rsi != null && ind1m.rsi >= Number(process.env.RSI1M_OB_RSI || 70))
       || (ind1m.stochK != null && ind1m.stochK >= Number(process.env.RSI1M_OB_STOCH || 80)))) {
    accepted = false; reject = "rsi1m-overbought"; rsi1mGated = true;
  }
  // ===== LEARN-BLOCK (authoritative) — dimatikan saat ACF explore>=1 (anti-snowball) =====
  if (explore < 1 && learn && learn.blocking && accepted) { accepted = false; reject = reject || "learn-block"; }
  // ===== WONBLOCK SAFETY-NET (blocker OUTCOME per-key) — SELALU berlaku (juga saat ACF explore) =====
  // BERBEDA dari learn-block di atas: wonblock bersumber pada fakta menang/kalah NYATA (res.won),
  // BUKAN $ akun, dan cakupannya dibatasi covCap saat mining (<=35%) -> TIDAK bisa snowball.
  // Karena itu ia TIDAK dimatikan oleh ACF explore (yang tujuannya mencegah $-blocker menumpuk).
  // Hasilnya: konteks buruk tetap dibuang selama fase transisi, sampai profil $ akun terbentuk.
  if (accepted && learn && learn.wonBlock) { accepted = false; reject = "won-block"; }
  // ===== MODEL LANJUTAN (B) per key — diteruskan ke engine/BOT (tanpa ambang global) =====
  let extras = {};
  try {
    const _gm = (p) => { try { return (typeof getModel === "function") ? getModel(p) : null; } catch (_) { return null; } };
    const _byk = (m) => (m && m.byKey) ? (m.byKey[_key] || null) : null;
    const _sp = _byk(_gm("spread")), _sc = _byk(_gm("score")), _sz = _byk(_gm("sizing")), _ta = _byk(_gm("ta"));
    extras = {
      spreadMaxPct: (_sp && _sp.ok) ? _sp.spreadMaxPct : null,     // B12 (batas spread per key -> dipakai BOT)
      minScorePct: (_sc && _sc.ok) ? _sc.minPct : null,            // B17 (ambang skor selektif)
      stakeMult: _sz ? _sz.mult : null,                            // B19 (mult stake per key -> dipakai BOT)
      taTrailCbPct: (_ta && _ta.best) ? _ta.best.cb : null,        // B14 (callback trailing per key)
    };
  } catch (_) {}
  // ===== POWER SINYAL U/D (KOSMETIK) — part dibangun via fungsi, dipakai utk score gate & final =====
  // Menyertakan SEMUA kriteria penolakan (regime/key-EV, RSI-1m, score) -> power <100% bila ada yg gagal.
  function buildPowerParts() {
    const parts = [];
    const add = (label, ratio, met) => parts.push({ label, ratio: +Math.max(0, Math.min(3, ratio)).toFixed(3), met: !!met });
    const FV = (T && T.FAIR && T.FAIR.volRel2) || 0, FS = (T && T.FAIR && T.FAIR.surprise) || 0;
    add("tier volRel2", FV > 0 ? volRel2 / FV : (volRel2 > 0 ? 1.001 : 1), volRel2 >= FV);
    add("tier surprise", FS > 0 ? surprise / FS : (surprise > 0 ? 1.001 : 1), surprise >= FS);
    const FVv = { volRel2, surprise, liqRatio, gapPct: gateNow, histStrength: histTrend.strength, rsi };
    for (const th of (profile.thresholds || [])) { const v = FVv[th.f]; if (v == null || !(th.t > 0)) continue; const r = th.op === ">=" ? v / th.t : th.t / v; add("ambang " + th.f, r, th.op === ">=" ? v >= th.t : v <= th.t); }
    if ((pv.rewardMin || 0) > 0) add("reward", gateNow / pv.rewardMin, gateNow >= pv.rewardMin);
    if ((pv.liqMin || 0) > 0) add("likuiditas", liqRatio / pv.liqMin, liqRatio >= pv.liqMin);
    add("rsi veto", pv.rsi ? 0 : 1, !pv.rsi);
    add("vol veto", pv.vol ? 0 : 1, !pv.vol);
    add("jam veto", pv.hour ? 0 : 1, !pv.hour);
    add("liqLow", liqLow ? 0 : 1, !liqLow);
    add("regime/key-EV", P.keyEvGated ? 0 : 1, !P.keyEvGated);
    add("learner-block", (learn && learn.blocking) ? 0 : 1, !(learn && learn.blocking));
    return parts;
  }
  const _finalizePower = (parts) => {
    const psum = parts.reduce((a, x) => a + x.ratio, 0);
    const metN = parts.filter((p) => p.met).length;
    let pct = parts.length ? 100 * psum / parts.length : (accepted ? 100 : 0);
    if (parts.length && metN < parts.length) pct = Math.min(pct, 100 * metN / parts.length);
    return { pct: Math.round(pct), parts, accepted: !!accepted, allMet: metN === parts.length };
  };
  // SELECTIVE ENTRY (B17): skor live (power dasar) di bawah ambang per-key -> tolak.
  let scoreGated = false;
  const _basePower = _finalizePower(buildPowerParts());
  if (explore < 1 && extras.minScorePct != null && typeof _basePower.pct === "number" && _basePower.pct < extras.minScorePct) {
    accepted = false; reject = "score-low"; scoreGated = true;
  }
  // POWER final — menyertakan part skor selektif & RSI-1m.
  let power = null;
  try {
    const parts = buildPowerParts();
    if (extras && extras.minScorePct != null) parts.push({ label: "skor selektif", ratio: scoreGated ? 0 : 1, met: !scoreGated });
    if (process.env.RSI1M_GATE === "1") parts.push({ label: "RSI 1m", ratio: rsi1mGated ? 0 : 1, met: !rsi1mGated });
    power = _finalizePower(parts);
  } catch (_) {}
  // ACF: rekam keputusan akhir sesi ini (untuk lantai cakupan adaptif / anti-snowball)
  _acf.record(_key, t0, (flatReason && !flatEntry) ? false : !!accepted);
  return {
    ok: true,
    skipped: (flatReason && !flatEntry) ? flatReason : undefined,
    signal: {
      asset: sym, interval: tf, t0, lock,
      power,
      dir: currentDir, silent: silentDir || undefined, mode, conf, flatEntry: flatEntry || undefined, loosen: loosen || undefined,
      grade: grade || null, accepted: (flatReason && !flatEntry) ? false : accepted, reject: (flatReason && !flatEntry) ? null : reject, thresholdsOK: !!thOK,
      reclaim: (reclaim && !flatReason) ? { f: reclaim.f, v: reclaim.v, lo: reclaim.lo, hi: reclaim.hi, n: reclaim.n, lb: reclaim.lb } : null,
      invert: (inverted && !flatReason) ? { and: inverted.and, n: inverted.n, flipWR: inverted.flipWR, lbFlip: inverted.lbFlip } : null,
      confirm: (confirmed && !flatReason) ? { and: confirmed.and, n: confirmed.n, wr: confirmed.wr, lb: confirmed.lb, from: confirmed.from || null, to: confirmed.to || grade } : null,
      skipped: (flatReason && !flatEntry) ? flatReason : null,
      volRel2: +volRel2.toFixed(4), surprise: +surprise.toFixed(4), mv2: +mv2.toFixed(5), mv2MinPct: +MIN_MV2_PCT.toFixed(4),
      rsi: rsi != null ? +rsi.toFixed(2) : null, histStrength: histTrend.strength,
      ind,   // RECORDER: EMA9/EMA21 (crossover) + MACD + pola candle 5m — untuk uji jendela panjang
      ind1m, // RECORDER: RSI(14)+Stochastic(14,3) 1m (candle 1m selesai) — uji reversal 1m
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
      // Label gate EKSPERIMENTAL (observasional; tidak memengaruhi accepted/reject). Spec: EXPERIMENT.md
      exp: (!flatReason && (currentDir === "up" || currentDir === "down"))
        ? EXP_GATE.cohortOfSignal({ dir: currentDir, mv2, ind }, t0) : [],
      rsi1mGated: rsi1mGated || undefined,
      // ===== MODEL LANJUTAN (B) per key =====
      spreadMaxPct: extras.spreadMaxPct, minScorePct: extras.minScorePct,
      stakeMult: extras.stakeMult, taTrailCbPct: extras.taTrailCbPct, scoreGated: scoreGated || undefined,
      explore: explore || undefined,
      // Fitur EKSTERNAL (Batch 1, observasional): funding/OIΔ/LSR/basis/depthImb dari cache server.
      ext: (() => { try { return (typeof EXT.get === "function") ? EXT.get(sym) : null; } catch (_) { return null; } })(),
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
    let one1m = [];
    try { one1m = await getKlines(sym, "1m", nowSec - 1, 60); } catch (_) {}   // RECORDER 1m RSI+Stoch
    const profile = (typeof getGates === "function" ? getGates() : null) || { mode: "nokey", byKey: {} };   // STRICT per-key (tanpa fallback global)
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
    const r = computeSignal({ sym, tf, t0, tfc, idx, ones, five5m, one1m, profile, getModel, SignalCore, nowSec, align });
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
      gate: { grade: sig.grade, liqLow: sig.liqLow, liqRatio: sig.liqRatio, thresholdsOK: sig.thresholdsOK, accepted: sig.accepted, reject: sig.reject, reclaim: sig.reclaim || null, invert: sig.invert || null, confirm: sig.confirm || null, profile: profile.mode },
    };
    save(rec, "server");
    stats.captured++; stats.lastAt = Date.now(); stats.lastKey = rec.k;
    return { ok: true, rec };
  }

  const done = new Set();
  let busy = false;
  const REC_LATE = process.env.REC_LATE !== "0";   // rekam fitur 60s/90s (validasi & fallback late-signal)
  // REKAM LATE (60s/90s): enrich record yg SAMA dgn px/mv/dir pada detik ke-60 & ke-90.
  // Riset (36h, 1s klines): arah candle-60s -> hasil akhir ~67-69% (subset flat-2s), 72-81% bila mv60>=0.02-0.03.
  async function captureLate(sym, tf, t0, sec) {
    const tfSec = DUR_S[tf]; if (!tfSec) return { skipped: "bad-tf" };
    const tfc = await getKlines(sym, tf, t0 + tfSec - 1, 60);
    const idx = tfc.findIndex((c) => c.time === t0);
    if (idx < 26) return { skipped: "no-session" };
    const lock = tfc[idx].open; if (!lock) return { skipped: "no-lock" };
    const ones = await getKlines(sym, "1s", t0 + sec + 3, 140);
    const c = ones.slice().reverse().find((o) => o.time <= t0 + sec - 1 && o.time >= t0);
    if (!c) return { skipped: "no-1s" };
    const px = c.close, mv = Math.abs(px - lock) / lock * 100, dir = px > lock ? 1 : 0;
    const rec = { k: `${sym}_${tf}_${t0}`, asset: sym, interval: tf, t0, src: "server" };
    rec["late" + sec] = { sec, px: +px, mv: +mv.toFixed(5), dir, lock, at: Date.now() };
    const ok = save(rec, "server");
    if (ok) log(`[CAPTURE] late${sec} ${sym} ${tf} ${rec.k} px=${px} mv=${mv.toFixed(4)}% dir=${dir ? "up" : "down"}`);
    return { ok: !!ok, rec };
  }
  function mark(key, suffix) { const k = key + (suffix || ""); if (done.has(k)) return false; done.add(k); if (done.size > 6000) { const it = done.values(); for (let i = 0; i < 3000; i++) done.delete(it.next().value); } return true; }
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
          const key = `${sym}_${tf}_${t0 / 1000}`;
          // (1) capture kanonik 2 detik
          if (off >= 3500 && off <= 20000) {
            if (!mark(key, "")) continue;
            try {
              const r = await captureOne(sym, tf, t0 / 1000);
              if (r && r.skipped) { stats.skipped++; log(`[CAPTURE] ${sym} ${tf} dilewati: ${r.skipped}`); }
              else if (r && r.ok && r.flat) {
                stats.flat = (stats.flat || 0) + 1;
                log(`[CAPTURE] ${sym} ${tf} ${r.rec.k} FLAT (informasional, tanpa sinyal/trade)`);
              } else if (r && r.ok) {
                if (r.rec.gate && r.rec.gate.accepted) stats.accepted = (stats.accepted || 0) + 1; else stats.rejected = (stats.rejected || 0) + 1;
                log(`[CAPTURE] ${sym} ${tf} ${r.rec.k} dir=${r.rec.sig.dir} grade=${r.rec.sig.grade || "-"} volRel2=${r.rec.sig.volRel2} surprise=${String(r.rec.sig.surprise).slice(0, 6)} gap=${(r.rec.sig.learn && r.rec.sig.learn.gap) || "-"} ${r.rec.gate.accepted ? "DITERIMA" : "DITOLAK:" + (r.rec.gate.reject || "-")}`);
              }
            } catch (e) { stats.errors++; stats.lastErr = e && e.message; log(`[CAPTURE] gagal ${sym} ${tf}: ${e && e.message}`); }
            continue;
          }
          // (2) rekam late 60s
          if (REC_LATE && off >= 56000 && off <= 75000) {
            if (!mark(key, "_L60")) continue;
            try { await captureLate(sym, tf, t0 / 1000, 60); } catch (e) { stats.errors++; stats.lastErr = e && e.message; log(`[CAPTURE] late60 gagal ${sym} ${tf}: ${e && e.message}`); }
            continue;
          }
          // (3) rekam late 90s
          if (REC_LATE && off >= 86000 && off <= 105000) {
            if (!mark(key, "_L90")) continue;
            try { await captureLate(sym, tf, t0 / 1000, 90); } catch (e) { stats.errors++; stats.lastErr = e && e.message; log(`[CAPTURE] late90 gagal ${sym} ${tf}: ${e && e.message}`); }
            continue;
          }
        }
      }
    } finally { busy = false; }
  }

  function start() {
    if (process.env.CAPTURE === "0") { stats.enabled = false; log("[CAPTURE] dinonaktifkan (CAPTURE=0)"); return; }
    setInterval(() => tick().catch(() => {}), 1000);
    log(`[CAPTURE] aktif — snapshot kanonik 2 detik untuk ${Object.keys(DUR_S).join(", ")}${REC_LATE ? " · rekam late 60s/90s" : ""}`);
  }
  return { start, tick, captureOne, status: () => stats };
}

module.exports = { createCapture, computeSignal, DUR_S, VOL_TYPICAL };
