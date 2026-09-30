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
  ENTRY_RETRACE_PCT: num(process.env.TA_ENTRY_RETRACE_PCT, 0.02),    // retrace dari ekstrem contra (%)
  ENTRY_MIN_EXTREME_PCT: num(process.env.TA_ENTRY_MIN_EXTREME_PCT, 0.03), // kedalaman ekstrem minimal (%)
  CUT_MIN_REMAIN_SEC: num(process.env.TA_CUT_MIN_REMAIN_SEC, 120),   // CUT hanya bila sisa sesi < ini (detik)
  EARLYCLOSE_MIN_CAPTURED_PCT: num(process.env.TA_EARLYCLOSE_MIN_CAPTURED_PCT, 70), // early close min % potensi
  ENTRY_MIN_REMAIN_SEC: num(process.env.TA_ENTRY_MIN_REMAIN_SEC, 120), // entry butuh sisa sesi >= ini
  CLOSE2_MIN_NEW_PEAK_PCT: num(process.env.TA_CLOSE2_MIN_NEW_PEAK_PCT, 0.008), // CLOSE-2: puncak baru harus lebih tinggi >= ini (%)
  CLOSE2_RETRACE_PCT: num(process.env.TA_CLOSE2_RETRACE_PCT, 0.01),  // CLOSE-2: micro-retrace dari puncak baru (%)
  // ===== MODE TRAILING (exit) =====
  TRAIL_MODE: num(process.env.TA_TRAIL_MODE, 1),                     // 1 = pakai exit TRAILING (arm+callback), 0 = leg1/CLOSE2 lama
  TRAIL_ARM_PCT: num(process.env.TA_TRAIL_ARM_PCT, 40),              // arm saat profit >= X% potensi (atau sentuh lock)
  TRAIL_CB_PCT: num(process.env.TA_TRAIL_CB_PCT, 0.02),              // callback: jual saat mundur >= X% dari puncak
  TRAIL_MIN_HOLD_MS: num(process.env.TA_TRAIL_MIN_HOLD_MS, 10000),   // min tahan sejak arm sebelum boleh exit (hindari prematur)
  ENTRY_MIN_NOW_PCT: num(process.env.TA_ENTRY_MIN_NOW_PCT, 0.03),    // kedalaman contra SAAT INI minimal utk entry (jangan entry dekat lock)
};

// Versi ringkas (hash) — berubah otomatis bila salah satu nilai berubah.
CFG.VER = "ta" + crypto.createHash("md5").update(JSON.stringify(CFG)).digest("hex").slice(0, 8);

module.exports = CFG;
