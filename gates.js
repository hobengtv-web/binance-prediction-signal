/* ============================================================================
   PROFIL GATE — satu tempat untuk semua ambang yang menentukan apakah sebuah sinyal
   ditampilkan. Dipakai bersama oleh app.js (live) dan capture.js (perekaman), dan
   DISAJIKAN SERVER lewat /api/model/gates sehingga bisa diganti tanpa deploy.

   Mengapa ada: sebelumnya ambang ini tersebar sebagai angka tetap di dalam kode
   (volRel2 >= 0.9, surprise >= 2, liqFloor * 0.3, LATE_FRAC 0.7) sehingga learner
   TIDAK BISA menyesuaikannya. Sekarang learner dapat menghasilkan profil `learned`
   dengan threshold hasil uji walk-forward (lihat learner.js learnThresholds).
   ============================================================================ */

// Perilaku semula (konservatif) — dipakai sebagai pembanding saat memutuskan promosi.
const STRICT = {
  mode: "strict",
  tiers: { STRONG: { volRel2: 3, surprise: 3 }, GOOD: { volRel2: 1.5, surprise: 2 }, FAIR: { volRel2: 0.9, surprise: 0 } },
  liqFloorMul: 0.3,
  lateFrac: 0.7,
  note: "ambang awal (hasil kalibrasi backtest 90d)",
};

// Profil untuk MENGUMPULKAN DATA: criteria dilonggarkan agar sinyal jauh lebih sering
// muncul dan populasinya luas. Ini sengaja lebih longgar dari STRICT; learner akan
// mengetatkan sendiri begitu datanya cukup (lewat threshold hasil uji).
const BOOTSTRAP = {
  mode: "bootstrap",
  tiers: { STRONG: { volRel2: 3, surprise: 3 }, GOOD: { volRel2: 1.5, surprise: 2 }, FAIR: { volRel2: 0.3, surprise: 0 } },
  liqFloorMul: 0.12,
  lateFrac: 0.85,
  note: "bootstrap: kriteria dilonggarkan untuk mempercepat pengumpulan data; learner akan mengetatkan sendiri",
};

// Threshold generik hasil belajar, contoh bentuk:
//   [{ f: "volRel2", op: ">=", t: 0.42 }, { f: "gapPct", op: "<=", t: 0.031 }]
function applyThresholds(row, thresholds) {
  if (!Array.isArray(thresholds) || !thresholds.length) return true;
  for (const th of thresholds) {
    const v = row[th.f];
    if (typeof v !== "number" || !isFinite(v)) return false;   // fitur tidak ada -> tidak lolos
    if (th.op === ">=" ? v < th.t : v > th.t) return false;
  }
  return true;
}

// Profil dari threshold hasil belajar (mode: learned). Tier dipakai hanya untuk LABEL.
function fromThresholds(thresholds, meta = {}) {
  return {
    mode: "learned",
    tiers: { STRONG: { volRel2: 3, surprise: 3 }, GOOD: { volRel2: 1.5, surprise: 2 }, FAIR: { volRel2: 0.3, surprise: 0 } },
    liqFloorMul: meta.liqFloorMul != null ? meta.liqFloorMul : BOOTSTRAP.liqFloorMul,
    lateFrac: meta.lateFrac != null ? meta.lateFrac : BOOTSTRAP.lateFrac,
    thresholds,
    metrics: meta.metrics || null,
    note: meta.note || "ambang hasil belajar dari data nyata (walk-forward)",
  };
}

module.exports = { STRICT, BOOTSTRAP, applyThresholds, fromThresholds };
