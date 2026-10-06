/* ============================================================================
   CLI: laporan gate EKSPERIMENTAL "MOMENTUM × TREND-AGREE" (observasional).
   Logika cohort ada di ../exp-gate.js (dipakai bersama engine).

   Pakai:
     node backtest/exp_gate.js                 # baca ledger lokal
     node backtest/exp_gate.js --remote        # ambil /api/ledger?dump=1 produksi
     node backtest/exp_gate.js --remote --save # + append ringkasan ke backtest/out/exp_gate_report.jsonl
   ============================================================================ */
const fs = require("fs");
const path = require("path");
const EXP = require("../exp-gate.js");

const ARGS = process.argv.slice(2);
const REMOTE = ARGS.includes("--remote") || !!process.env.SIGNAL_BASE;
const SAVE = ARGS.includes("--save");
const BASE = (process.env.SIGNAL_BASE || "https://binance-prediction-signal-production.up.railway.app").replace(/\/$/, "");
const LEDGER_FILE = process.env.LEDGER_FILE || path.join(__dirname, "..", "ledger", "signals.jsonl");
const OUT = path.join(__dirname, "out", "exp_gate_report.jsonl");

const fmt = (x, d = 3, sign = false) => (x == null ? "-" : ((sign && x >= 0 ? "+" : "") + x.toFixed(d)));
const pad = (s, n) => String(s).padStart(n);

async function load() {
  if (REMOTE) {
    const r = await fetch(`${BASE}/api/ledger?dump=1`);
    return (await r.json()).records || [];
  }
  return fs.readFileSync(LEDGER_FILE, "utf8").trim().split("\n")
    .map((l) => { try { return JSON.parse(l); } catch { return null; } }).filter(Boolean);
}

(async () => {
  const recs = await load();
  const ev = EXP.evaluate(recs);
  console.log(`sumber: ${REMOTE ? BASE + "/api/ledger" : LEDGER_FILE}`);
  console.log(`record berarah+hasil: ${ev.rows}  (${(ev.range || {}).from || "-"} .. ${(ev.range || {}).to || "-"})`);
  if (!ev.rows) return;

  console.log(`\n${"cohort".padEnd(22)} ${pad("n", 5)} ${pad("WR", 6)} ${pad("LB", 6)} ${pad("nOdds", 6)} ${pad("avgPx", 6)} ${pad("EVraw", 7)} ${pad("EVnet", 7)} ${pad("eqGross", 8)} ${pad("eqNet", 7)} ${pad("ddNet", 6)}`);
  for (const [name, s] of Object.entries(ev.cohorts)) {
    if (!s) { console.log(`${name.padEnd(22)} (kosong)`); continue; }
    console.log(`${name.padEnd(22)} ${pad(s.n, 5)} ${pad(s.wr.toFixed(1) + "%", 6)} ${pad(s.lb.toFixed(1) + "%", 6)} ${pad(s.nOd, 6)} ${pad(fmt(s.avgPx), 6)} ${pad(fmt(s.ev, 3, true), 7)} ${pad(fmt(s.evNet, 3, true), 7)} ${pad(fmt(s.eq, 1), 8)} ${pad(fmt(s.netEq, 1), 7)} ${pad(fmt(s.netMaxDD, 1), 6)}`);
  }

  console.log(`\nwalk-forward WR 70/30 & EV ODDS 60/40:`);
  console.log(`${"cohort".padEnd(22)} ${pad("WRtr", 7)} ${pad("WRte", 7)} ${pad("EVtr", 7)} ${pad("EVte", 7)} ${pad("eqTest", 8)}`);
  for (const [name, s] of Object.entries(ev.cohorts)) {
    if (!s) continue;
    console.log(`${name.padEnd(22)} ${pad(fmt(s.wf.wrTrain, 1), 7)} ${pad(fmt(s.wf.wrTest, 1), 7)} ${pad(fmt(s.wfOdds.evTrain, 3, true), 7)} ${pad(fmt(s.wfOdds.evTest, 3, true), 7)} ${pad(fmt(s.wfOdds.eqTest, 1), 8)}`);
  }

  if (SAVE) {
    try { fs.mkdirSync(path.dirname(OUT), { recursive: true }); fs.appendFileSync(OUT, JSON.stringify(ev) + "\n"); console.log(`\n[SAVE] -> ${OUT}`); }
    catch (e) { console.log("gagal simpan:", e.message); }
  }
})();
