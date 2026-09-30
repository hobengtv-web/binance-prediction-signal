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
  EARLYCLOSE_MIN_CAPTURED_PCT: num(process.env.TA_EARLYCLOSE_MIN_CAPTURED_PCT, 50), // early close min % potensi
  ENTRY_MIN_REMAIN_SEC: num(process.env.TA_ENTRY_MIN_REMAIN_SEC, 120), // entry butuh sisa sesi >= ini
};

// Versi ringkas (hash) — berubah otomatis bila salah satu nilai berubah.
CFG.VER = "ta" + crypto.createHash("md5").update(JSON.stringify(CFG)).digest("hex").slice(0, 8);

module.exports = CFG;
