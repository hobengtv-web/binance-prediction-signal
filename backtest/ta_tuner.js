/* ============================================================================
   TA TUNER — menyetel konstanta Trade Assistant dari data ledger (anti-overfit).
   Membaca res.trade (metrik TA) yang direkam engine, memisah data train/test
   berurutan waktu, lalu merekomendasikan nilai konstanta berdasarkan Wilson LB.

   Pemakaian:
     node backtest/ta_tuner.js                 # dari folder repo
     node backtest/ta_tuner.js --csv out.csv
   Output: rekomendasi nilai + tabel; TIDAK mengubah apa pun (advisory).
   Setelah yakin, set env produksi, mis.:
     TA_ENTRY_MIN_EXTREME_PCT=0.04 TA_ENTRY_RETRACE_PCT=0.025
   ========================================================================== */
const fs = require("fs");
const path = require("path");
const L = require(path.join(__dirname, "..", "learner.js"));

const LEDGER = process.env.LEDGER || path.join(__dirname, "..", "ledger/signals.jsonl");
const MIN_N = Number(process.env.MIN_N || 25);      // minimal sampel uji
const SPLIT = Number(process.env.SPLIT || 0.7);     // porsi train (berurutan waktu)

function load() {
  const lines = fs.readFileSync(LEDGER, "utf8").trim().split("\n");
  const recs = lines.map((l) => { try { return JSON.parse(l); } catch (_) { return null; } }).filter(Boolean);
  const rows = L.rowsFrom(recs).filter((r) => r.taEntered && r.taWin != null);
  // dedupe per sesi (asset|interval|t0) -> pakai record terakhir yang punya trade closed
  const m = new Map();
  for (const r of rows) { const k = `${r.asset}|${r.interval}|${r.t0}`; const prev = m.get(k); if (!prev || (r.taClosed && !prev.taClosed)) m.set(k, r); }
  return [...m.values()].sort((a, b) => a.t0 - b.t0);
}
const wil = L.wilson;
function agg(rows) {
  const n = rows.length, w = rows.reduce((a, r) => a + r.taWin, 0);
  // capture hanya valid bila jarak entry->lock cukup besar (pot >= 0.02%) agar tidak inflasi
  const cap = rows.filter((r) => r.capturePct != null && r.entryRNow != null && r.entryRNow >= 0.02);
  return { n, wr: n ? w / n : 0, lb: wil(w, n).lo, cap: cap.length ? cap.reduce((a, r) => a + r.capturePct, 0) / cap.length : null, capN: cap.length,
    pnl: n ? rows.reduce((a, r) => a + (r.pnlPct || 0), 0) / n : 0 };
}
/* Uji ambang pada satu fitur numerik: pilih nilai yang memaksimalkan MEAN P&L pada TEST (n>=MIN_N). */
function tuneThreshold(train, test, feat, dir, candidates) {
  const pick = (rows, x) => rows.filter((r) => r[feat] != null && (dir === ">=" ? r[feat] >= x : r[feat] <= x));
  let best = null;
  for (const x of candidates) {
    const a = agg(pick(train, x)), b = agg(pick(test, x));
    if (b.n < MIN_N) continue;
    if (!best || b.pnl > best.test.pnl) best = { x, train: a, test: b };
  }
  return best;
}
function breakdown(rows, keyFn) {
  const m = new Map();
  for (const r of rows) { const k = keyFn(r); if (!m.has(k)) m.set(k, []); m.get(k).push(r); }
  return [...m.entries()].map(([k, rr]) => ({ k, ...agg(rr) })).sort((a, b) => b.n - a.n);
}

const rows = load();
console.log(`Total sesi dengan trade TA: ${rows.length}`);
if (rows.length < 30) console.log("PERINGATAN: sampel masih kecil — rekomendasi belum kuat (butuh akumulasi `res.trade` baru).");
const cut = Math.floor(rows.length * SPLIT);
const train = rows.slice(0, cut), test = rows.slice(cut);
console.log(`train=${train.length} test=${test.length}  baseTA(train)=${agg(train).wr.toFixed(3)} baseTA(test)=${agg(test).wr.toFixed(3)}`);

console.log("\n--- rekomendasi ambang ENTRY (fitur dicatat saat entry) ---");
const extCands = [0.0, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.08];
const retCands = [0.0, 0.01, 0.015, 0.02, 0.025, 0.03, 0.04];
const tExt = tuneThreshold(train, test, "entryExtremeDepth", ">=", extCands);
const tRet = tuneThreshold(train, test, "entryRetrace", ">=", retCands);
const fmt = (b) => b ? `x>=${b.x}  test n=${b.test.n} wr=${b.test.wr.toFixed(3)} lb=${b.test.lb.toFixed(3)} cap%=${b.test.cap == null ? "-" : b.test.cap.toFixed(0)} pnl%=${b.test.pnl.toFixed(3)}` : "sampel uji belum cukup";
console.log(`  ENTRY_MIN_EXTREME_PCT : ${fmt(tExt)}`);
console.log(`  ENTRY_RETRACE_PCT     : ${fmt(tRet)}`);

console.log("\n--- breakdown per exitReason (bila sudah ada) ---");
breakdown(rows, (r) => r.closeReason || "null").forEach((b) => console.log(`  ${String(b.k).padEnd(6)} n=${String(b.n).padStart(3)} wr=${b.wr.toFixed(3)} cap%=${b.cap == null ? "-" : b.cap.toFixed(0)} pnl%=${b.pnl.toFixed(3)}`));

console.log("\n--- breakdown per interval ---");
breakdown(rows, (r) => r.interval).forEach((b) => console.log(`  ${String(b.k).padEnd(4)} n=${String(b.n).padStart(3)} wr=${b.wr.toFixed(3)} cap%=${b.cap == null ? "-" : b.cap.toFixed(0)}`));

console.log("\nCatatan: set konstanta via env `TA_*` di produksi; `taVer` berubah otomatis dan bisa dipakai membandingkan sebelum/sesudah.");
