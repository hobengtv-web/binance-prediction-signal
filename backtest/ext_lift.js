/* ============================================================================
   backtest/ext_lift.js — ukur LIFT fitur EKSTERNAL (sig.ext) terhadap hasil sesi.
   Fitur bersifat point-in-time -> hanya sesi SEJAK recorder aktif yang punya ext.
   Bandingkan WR/EV per bucket fitur (funding, basisPct, oiDelta5m, lsrGlobal, lsrTop, takerLS, depthImb).

   Pakai:
     node backtest/ext_lift.js            # ledger lokal
     node backtest/ext_lift.js --remote   # /api/ledger?dump=1 produksi
   ============================================================================ */
const fs = require("fs");
const path = require("path");
const ARGS = process.argv.slice(2);
const REMOTE = ARGS.includes("--remote") || !!process.env.SIGNAL_BASE;
const BASE = (process.env.SIGNAL_BASE || "https://binance-prediction-signal-production.up.railway.app").replace(/\/$/, "");
const LEDGER = process.env.LEDGER_FILE || path.join(__dirname, "..", "ledger", "signals.jsonl");

function wilson(k, n, z = 1.96) { if (!n) return 0; const p = k / n, d = 1 + z * z / n; const c = (p + z * z / (2 * n)) / d; const h = z * Math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d; return Math.max(0, c - h); }
const mean = (a) => (a.length ? a.reduce((x, y) => x + y, 0) / a.length : 0);

async function load() {
  if (REMOTE) return (await (await fetch(`${BASE}/api/ledger?dump=1`)).json()).records || [];
  return fs.readFileSync(LEDGER, "utf8").trim().split("\n").map((l) => { try { return JSON.parse(l); } catch { return null; } }).filter(Boolean);
}
const roiOf = (r) => { const res = r.res || {}; if (typeof res.settleRoi === "number") return res.settleRoi; const p = (r.odds || {})[(r.sig || {}).dir]; return (typeof p === "number" && p > 0 && p < 1) ? (res.won === 1 ? (1 - p) / p * 100 : -100) : null; };

const FEATURES = {
  funding: [[-1e9, 0], [0, 1e-4], [1e-4, 1e9]],
  basisPct: [[-1e9, -0.02], [-0.02, 0.02], [0.02, 1e9]],
  oiDelta5m: [[-1e9, -1], [-1, 1], [1, 1e9]],
  lsrGlobal: [[0, 0.9], [0.9, 1.1], [1.1, 1e9]],
  lsrTop: [[0, 0.9], [0.9, 1.1], [1.1, 1e9]],
  takerLS: [[0, 0.95], [0.95, 1.05], [1.05, 1e9]],
  depthImb: [[-1e9, -0.2], [-0.2, 0.2], [0.2, 1e9]],
};

(async () => {
  const recs = await load();
  const rows = recs.filter((r) => { const s = r.sig || {}; return (s.dir === "up" || s.dir === "down") && !s.skipped && (r.res && (r.res.won === 0 || r.res.won === 1)); });
  const withExt = rows.filter((r) => r.sig && r.sig.ext);
  const tot = rows.length, k = withExt.reduce((a, r) => a + r.res.won, 0);
  console.log(`sumber: ${REMOTE ? BASE : LEDGER}`);
  console.log(`sesi berarah+hasil: ${tot} · punya sig.ext: ${withExt.length} (${(100 * withExt.length / (tot || 1)).toFixed(1)}%)`);
  if (withExt.length) console.log(`WR dasar (punya ext): ${(100 * k / withExt.length).toFixed(1)}%  · LB ${(100 * wilson(k, withExt.length)).toFixed(1)}%`);

  if (withExt.length < 20) { console.log("\n(sampel ext masih kecil — biarkan recorder berjalan beberapa hari, lalu jalankan lagi)"); }

  for (const [feat, edges] of Object.entries(FEATURES)) {
    const vals = withExt.map((r) => ({ r, v: r.sig.ext[feat] })).filter((x) => typeof x.v === "number");
    if (!vals.length) { continue; }
    console.log(`\n== ${feat} (n=${vals.length}) ==`);
    for (const [lo, hi] of edges) {
      const sub = vals.filter((x) => x.v >= lo && x.v < hi);
      if (!sub.length) continue;
      const n = sub.length, w = sub.reduce((a, x) => a + x.r.res.won, 0);
      const wr = 100 * w / n, lb = 100 * wilson(w, n);
      const rois = sub.map((x) => roiOf(x.r)).filter((x) => typeof x === "number");
      const net = rois.length ? mean(rois) - 3 : null;
      const label = `${lo === -1e9 ? "-inf" : lo} .. ${hi === 1e9 ? "+inf" : hi}`;
      console.log(`  [${label.padEnd(16)}] n=${String(n).padStart(4)} WR=${wr.toFixed(1).padStart(5)}% LB=${lb.toFixed(1).padStart(5)}% EVnet=${net == null ? "-" : (net >= 0 ? "+" : "") + net.toFixed(1)}%`);
    }
  }
})();
