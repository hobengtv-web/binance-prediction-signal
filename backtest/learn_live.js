/* ============================================================================
   LEARNER LIVE — jalankan metodologi learner (learner.js) atas LEDGER produksi.
   Data nyata, bukan replay: fitur skala detik dikumpulkan aplikasi sendiri karena
   Binance hanya menyediakan 1s klines 7 hari.
   Pemakaian:
     node backtest/learn_live.js                                  (localhost:8000)
     node backtest/learn_live.js --url <prod>/api/ledger?dump=1
     node backtest/learn_live.js --file ledger/signals.jsonl
     node backtest/learn_live.js --write                          (tulis learn_live.json)
   ============================================================================ */
const fs = require("fs"), path = require("path");
const L = require("../learner.js");
const OUT = path.join(__dirname, "out");
const argv = process.argv.slice(2);
const argOf = (n) => { const i = argv.indexOf(n); return i >= 0 ? argv[i + 1] : null; };
const URL_ARG = argOf("--url") || "http://localhost:8000/api/ledger?dump=1";
const FILE_ARG = argOf("--file");
const WRITE = argv.includes("--write");

async function loadLedger() {
  if (FILE_ARG) {
    const txt = fs.readFileSync(FILE_ARG, "utf8");
    if (FILE_ARG.endsWith(".jsonl")) return txt.split("\n").filter(Boolean).map((l) => JSON.parse(l));
    const j = JSON.parse(txt);
    return Array.isArray(j) ? j : (j.records || []);
  }
  const res = await fetch(URL_ARG);
  const j = await res.json();
  return j.records || [];
}

async function main() {
  const raw = await loadLedger();
  const rows = L.rowsFrom(raw);
  const sk = rows.skipped || {};
  const withRes = rows.length, all = raw.filter((r) => r && r.sig).length;
  const alts = raw.reduce((a, r) => a + ((r.alts && r.alts.length) || 0), 0);
  console.log(`=== LEARNER LIVE (dari ledger) ===`);
  console.log(`record: ${raw.length} · punya fitur: ${all} · siap dipakai (kanonik + ada hasil): ${withRes}`);
  console.log(`dikecualikan: ${sk.late || 0} capture tengah sesi (capOffsetMs > ${L.CANONICAL_MAX_MS}ms) · ${sk.noRes || 0} belum ada hasil · ${sk.noSig || 0} tanpa fitur · ${sk.badDir || 0} arah tidak jelas · ${sk.old || 0} di luar rentang`);
  if (alts) console.log(`snapshot alternatif tersimpan (capture lain di sesi yang sama): ${alts}`);
  const model = L.buildModel(rows);
  if (!model.ok) {
    console.log(`\nBelum cukup data: ${model.reason}.`);
    console.log(`Ledger mengumpulkan otomatis dari sinyal nyata; jalankan lagi nanti.`);
    if (WRITE) fs.writeFileSync(path.join(OUT, "learn_live.json"), JSON.stringify({ generated: new Date().toISOString(), ready: false, rows: withRes, reason: model.reason }, null, 1));
    return;
  }
  const b = model.baseline;
  console.log(`\nbaseline  arah: latih ${(b.dirTrain * 100).toFixed(1)}% -> uji ${(b.dirTest * 100).toFixed(1)}% · sentuh-lock uji ${(b.touchTest * 100).toFixed(1)}%`);
  console.log(`sebagai FILTER: cakupan ${(model.metrics.coverage * 100).toFixed(0)}% · winrate diambil ${(model.metrics.takenWinrate * 100).toFixed(1)}% · skor ${model.metrics.score}`);
  console.log(`penahan aktif: ${[...model.metrics.gateBlockers, ...model.metrics.touchBlockers].join(", ") || "(tidak ada)"}`);
  console.log(`\naturan terkuat (lolos uji):`);
  for (const r of model.gate.rules.filter((x) => x.verdict !== "neutral").slice(0, 8)) console.log(`  arah  ${r.k.padEnd(30)} ${r.verdict.padEnd(9)} uji ${(r.wrTest * 100).toFixed(1)}% (LB ${(r.lbTest * 100).toFixed(1)}%, n=${r.nTest})`);
  for (const r of model.touch.rules.filter((x) => x.verdict !== "neutral").slice(0, 6)) console.log(`  sentuh${"".padEnd(2)}${r.k.padEnd(30)} ${r.verdict.padEnd(9)} uji ${(r.wrTest * 100).toFixed(1)}% (LB ${(r.lbTest * 100).toFixed(1)}%, n=${r.nTest})`);
  console.log(`\npenyebab kalah (dari jendela uji):`);
  for (const c of model.lessons.lessons.filter((x) => x.type === "cause").slice(0, 6)) console.log(`  ${c.text}`);
  if (WRITE) {
    fs.writeFileSync(path.join(OUT, "learn_live.json"), JSON.stringify({
      generated: new Date().toISOString(), ready: true, rows: withRes, baseline: model.baseline, metrics: model.metrics,
      gate: model.gate, touch: model.touch, lessons: model.lessons,
    }, null, 1));
    console.log(`\n-> ditulis: backtest/out/learn_live.json`);
  } else {
    console.log(`\n(tambahkan --write untuk menyimpan hasil ke backtest/out/learn_live.json)`);
  }
}
main().catch((e) => { console.error("gagal:", e.message); process.exit(1); });
