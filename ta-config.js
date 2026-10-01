/* ============================================================================
   TA CONFIG — parameter Trade Assistant yang bisa disetel TANPA ubah kode.
   Dipakai trade-plan.js (sisi perhitungan) dan direkam ke ledger sebagai `taVer`
   agar hasil bisa dibandingkan sebelum/sesudah perubahan parameter.

   Set via env (mis. di Railway) dengan prefix TA_ , contoh:
     TA_ENTRY_RETRACE_PCT=0.025 TA_ENTRY_MIN_EXTREME_PCT=0.04
   ========================================================================== */
const crypto = require("crypto");
const num = (v, d) => (v == null || v === "" || isNaN(Number(v)) ? d : Number(v));

const CFG = {
  DWELL_ENTRY_MS: num(process.env.TA_DWELL_ENTRY_MS, 2000),          // peak/turn harus bertahan (ms)
  DWELL_AVG_MS: num(process.env.TA_DWELL_AVG_MS, 15000),             // averaging: hold (ms)
  DWELL_CLOSE_MS: num(process.env.TA_DWELL_CLOSE_MS, 10000),         // fade close: hold (ms)
  ENTRY_RETRACE_PCT: num(process.env.TA_ENTRY_RETRACE_PCT, 0.025),   // retrace dari ekstrem contra (%) — 0.025 = ambang terpilih (DD 49%->27%)
  ENTRY_MIN_EXTREME_PCT: num(process.env.TA_ENTRY_MIN_EXTREME_PCT, 0.04), // kedalaman ekstrem minimal (%) — 0.04 = ambang terpilih
  CUT_MIN_REMAIN_SEC: num(process.env.TA_CUT_MIN_REMAIN_SEC, 120),   // CUT hanya bila sisa sesi < ini (detik)
  EARLYCLOSE_MIN_CAPTURED_PCT: num(process.env.TA_EARLYCLOSE_MIN_CAPTURED_PCT, 70), // early close min % potensi
  ENTRY_MIN_REMAIN_SEC: num(process.env.TA_ENTRY_MIN_REMAIN_SEC, 120), // entry butuh sisa sesi >= ini
  SIMPLE: num(process.env.TA_SIMPLE, 0),                             // 1 = pakai logika TEST (entry depth+retrace, exit trailing saja)
  TRAIL_ARM_ON_LOCK: num(process.env.TA_TRAIL_ARM_ON_LOCK, 0),       // 0 = JANGAN arm hanya karena menyentuh lock (hindari exit premature)
  HYBRID: num(process.env.TA_HYBRID, 0),                             // 1 = HYBRID: boleh exit < min-hold bila capture tinggi / retrace besar
  HYB_MIN_CAP_PCT: num(process.env.TA_HYB_MIN_CAP_PCT, 80),         // exit lebih awal bila capture >= ini
  HYB_MIN_RETRACE_PCT: num(process.env.TA_HYB_MIN_RETRACE_PCT, 0.1), // exit lebih awal bila retrace dari puncak >= ini
  BE_STOP: num(process.env.TA_BE_STOP, 0),                           // 1 = break-even stop: bila sudah armed & profit kembali <=0 -> EXIT (abaikan min-hold)
  TP_CAP_PCT: num(process.env.TA_TP_CAP_PCT, 0),                     // >0 = hard take-profit: bila capture >= X% -> EXIT segera (abaikan min-hold)
  CLOSE2_MIN_NEW_PEAK_PCT: num(process.env.TA_CLOSE2_MIN_NEW_PEAK_PCT, 0.008), // CLOSE-2: puncak baru harus lebih tinggi >= ini (%)
  CLOSE2_RETRACE_PCT: num(process.env.TA_CLOSE2_RETRACE_PCT, 0.01),  // CLOSE-2: micro-retrace dari puncak baru (%)
  // ===== MODE TRAILING (exit) =====
  TRAIL_MODE: num(process.env.TA_TRAIL_MODE, 1),                     // 1 = pakai exit TRAILING (arm+callback), 0 = leg1/CLOSE2 lama
  TRAIL_ARM_PCT: num(process.env.TA_TRAIL_ARM_PCT, 40),              // arm saat profit >= X% potensi (atau sentuh lock)
  TRAIL_CB_PCT: num(process.env.TA_TRAIL_CB_PCT, 0.02),              // callback: jual saat mundur >= X% dari puncak
  TRAIL_MIN_HOLD_MS: num(process.env.TA_TRAIL_MIN_HOLD_MS, 10000),   // min tahan sejak arm sebelum boleh exit (hindari prematur)
  TRAIL_STD_K: num(process.env.TA_TRAIL_STD_K, 0),                    // >0 = trailing ADAPTIF: ambang = max(CB, k x std) dalam % harga (0 = off)
  TRAIL_GIVEBACK_PCT: num(process.env.TA_TRAIL_GIVEBACK_PCT, 0),      // >0 = keluar bila harga memberi balik >= X% dari PUNCAK PROFIT (0 = off)
  // ===== KELAYAKAN ENTRY DI AKHIR SESI (jarak ke LOCK vs sisa waktu) =====
  LATE_FEASIBILITY: num(process.env.TA_LATE_FEASIBILITY, 1),          // 1 = tolak entry telat bila jarak ke LOCK tidak feasible
  LATE_FEASIBLE_SIGMA: num(process.env.TA_LATE_FEASIBLE_SIGMA, 2.5),  // batas "sigma yang dibutuhkan" (skala dgn sqrt(sisa/300s))
  ENTRY_MIN_NOW_PCT: num(process.env.TA_ENTRY_MIN_NOW_PCT, 0.03),    // kedalaman contra SAAT INI minimal utk entry (jangan entry dekat lock)
};

// ===== OVERRIDE PER TIMEFRAME =====
CFG.PER_TF = {
  "5m":  { ENTRY_MIN_REMAIN_SEC: CFG.ENTRY_MIN_REMAIN_SEC, TRAIL_MIN_HOLD_MS: CFG.TRAIL_MIN_HOLD_MS,
           TRAIL_CB_PCT: num(process.env.TA_5M_TRAIL_CB_PCT, CFG.TRAIL_CB_PCT) },
  "15m": { ENTRY_MIN_REMAIN_SEC: num(process.env.TA_15M_ENTRY_MIN_REMAIN_SEC, 180), TRAIL_MIN_HOLD_MS: num(process.env.TA_15M_TRAIL_MIN_HOLD_MS, 20000),
           TRAIL_CB_PCT: num(process.env.TA_15M_TRAIL_CB_PCT, CFG.TRAIL_CB_PCT) },
  "1h":  { ENTRY_MIN_REMAIN_SEC: num(process.env.TA_1H_ENTRY_MIN_REMAIN_SEC, 300), TRAIL_MIN_HOLD_MS: num(process.env.TA_1H_TRAIL_MIN_HOLD_MS, CFG.TRAIL_MIN_HOLD_MS),
           TRAIL_CB_PCT: num(process.env.TA_1H_TRAIL_CB_PCT, CFG.TRAIL_CB_PCT) },
};

// Versi ringkas (hash) — berubah otomatis bila salah satu nilai berubah.
CFG.VER = "ta" + crypto.createHash("md5").update(JSON.stringify(CFG)).digest("hex").slice(0, 8);


/* ===== JAM TRADE (WIB) =====
   Cerminan OFF_HOURS_WIB milik BOT (entry diblokir pada jam-jam ini; EXIT tetap berjalan).
   Dipakai UI untuk menampilkan rentang ON/OFF. Ubah via env TRADE_OFF_HOURS_WIB="4,22". */
CFG.TRADE_OFF_HOURS_WIB = String(process.env.TRADE_OFF_HOURS_WIB || "4,22")
  .split(",").map((x) => parseInt(x, 10)).filter((n) => Number.isFinite(n) && n >= 0 && n < 24);
CFG.TRADE_HOURS = (() => {
  const off = CFG.TRADE_OFF_HOURS_WIB.slice().sort((a, b) => a - b);
  const set = new Set(off); const on = []; let start = null;
  for (let h = 0; h <= 24; h++) {
    const isOff = h < 24 && set.has(h);
    if (!isOff && start === null) start = h;
    if (isOff && start !== null) { on.push([start, h]); start = null; }
  }
  if (start !== null) on.push([start, 24]);
  const pad = (h) => String(h).padStart(2, "0") + ":00";
  return { tz: "WIB", off, on,
    offRanges: off.map((h) => pad(h) + "–" + pad(h + 1)),
    onRanges: on.map(([a, b]) => pad(a) + "–" + pad(b)) };
})();

module.exports = CFG;
