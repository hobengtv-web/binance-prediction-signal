/* Local Node server: serves static files + /api/snapshot (polled) + /api/stream (SSE, realtime).
   The SSE endpoint opens a Binance trade stream on the SERVER and pushes every trade to the
   browser, so price moves are realtime (not once-per-second). Falls back to REST polling if WS fails. */
const http = require("http");
const fs = require("fs");
const path = require("path");
const { getSnapshot, getKlines } = require("./snapshot");
const FLOW = require("./flow.js");   // akumulasi executed order flow (OFI) sisi server

const PORT = process.env.PORT || 8000;
const MIME = { ".html": "text/html", ".js": "text/javascript", ".css": "text/css", ".json": "application/json" };

/* ===== PHASE 2: SIGNAL LEDGER =====
   Menyimpan setiap sinyal + vektor fitur lengkap + hasilnya, supaya learner punya data
   jangka panjang (Binance hanya menyediakan 1s klines 7 hari, jadi fitur skala detik
   harus dikumpulkan sendiri dari sekarang).
   Penyimpanan: JSONL append-only. Di Railway ditulis ke volume (mount /data) supaya
   bertahan antar deploy; secara lokal ke ./ledger. */
const LEDGER_DIR = process.env.LEDGER_DIR || (fs.existsSync("/data") ? "/data/ledger" : path.join(__dirname, "ledger"));
const LEDGER_FILE = path.join(LEDGER_DIR, "signals.jsonl");
let ledger = new Map();      // k -> record
let ledgerDirty = 0;
const CORS = { "Access-Control-Allow-Origin": "*", "Cache-Control": "no-store" };
function loadLedger() {
  try {
    fs.mkdirSync(LEDGER_DIR, { recursive: true });
    if (!fs.existsSync(LEDGER_FILE)) { console.log(`[LEDGER] new ledger at ${LEDGER_FILE}`); return; }
    const lines = fs.readFileSync(LEDGER_FILE, "utf8").split("\n");
    for (const l of lines) {
      if (!l.trim()) continue;
      try { const r = JSON.parse(l); if (r && r.k) ledger.set(r.k, r); } catch (_) {}
    }
    console.log(`[LEDGER] loaded ${ledger.size} records from ${LEDGER_FILE}`);
  } catch (e) { console.warn("[LEDGER] load failed:", e.message); }
}
function appendLedger(rec) {
  try { fs.appendFileSync(LEDGER_FILE, JSON.stringify(rec) + "\n"); } catch (e) { console.warn("[LEDGER] append failed:", e.message); }
}
function compactLedger() {   // satu baris per key (versi terakhir menang)
  try { fs.writeFileSync(LEDGER_FILE, [...ledger.values()].map((r) => JSON.stringify(r)).join("\n") + "\n"); }
  catch (e) { console.warn("[LEDGER] compact failed:", e.message); }
}
function ledgerStats() {
  const byKey = {};
  let withRes = 0, won = 0, touch = 0, touchN = 0;
  for (const r of ledger.values()) {
    const kk = `${r.asset}_${r.interval}`;
    (byKey[kk] = byKey[kk] || { n: 0, withRes: 0, won: 0 });
    byKey[kk].n++;
    if (r.res) { withRes++; byKey[kk].withRes++; if (r.res.won === 1) { won++; byKey[kk].won++; } if (r.res.touch != null) { touchN++; touch += r.res.touch; } }
  }
  return {
    total: ledger.size, withRes,
    winrate: withRes ? +(won / withRes).toFixed(4) : null,
    touchRate: touchN ? +(touch / touchN).toFixed(4) : null,
    byKey, dir: LEDGER_DIR, file: LEDGER_FILE,
    persistent: LEDGER_DIR.startsWith("/data"),
  };
}
loadLedger();
setInterval(() => { if (ledgerDirty > 0) { compactLedger(); ledgerDirty = 0; } }, 300000);   // kompaksi tiap 5 menit

/* Self-healing: lengkapi hasil ronde yang belum tercatat (mis. browser ditutup sebelum ronde
   selesai) memakai klines 1m Binance. Arah/jenis hasil = tepat; sentuh-lock/MFE/MAE dihitung
   dari candle MENIT ke-2 dst (menit pertama memuat detik sinyal, jadi sengaja dikecualikan)
   -> ditandai src:"server-1m" agar tidak tertukar dengan hasil dari klien (jalur 1s). */
const DUR_S = { "5m": 300, "15m": 900, "1h": 3600 };
let resolving = false;

// Info TRADE (entry & early close) untuk satu sesi: diambil dari state engine + dihitung
// entryTouch (apakah LOCK tersentuh SETELAH entry) dari jalur 1m. Dipakai dua tempat: resolusi
// hasil baru dan BACKFILL untuk record yang sudah punya `res` tetapi belum punya `trade`.
// Cek presisi: apakah LOCK tersentuh SETELAH entry — memakai klines 1 DETIK.
// Versi lama memakai bar 1 MENIT, sehingga sentuhan yang terjadi di dalam bar yang sama dengan
// entry (entry di detik 12, sentuhan di detik 20 pada menit yang sama) tidak terhitung -> baris E
// salah tampil MERAH padahal target tercapai. Mengembalikan { touch, at } atau null bila gagal.
async function entryTouch1s(asset, t0, dur, entryAt, lock, dir) {
  try {
    const bars = await getKlines(asset, "1s", t0 + dur, Math.min(1000, dur + 5));
    const from = Math.floor(entryAt / 1000);
    const after = (bars || []).filter((b) => b.time >= from);
    if (!after.length) return null;
    let at = null;
    for (const b of after) { if (dir === "up" ? b.high >= lock : b.low <= lock) { at = b.time; break; } }
    return { touch: at != null ? 1 : 0, at };
  } catch (_) { return null; }
}

function tradeInfoFor(asset, interval, t0, dir, lock, path) {
  const tr = (typeof engine !== "undefined" && engine.tradeFor) ? engine.tradeFor(asset, interval, t0) : null;
  if (!tr) return null;
  let entryTouch = null;
  if (tr.entered && tr.entryAt) {
    const fromSec = Math.floor(tr.entryAt / 1000);
    const after = (path || []).filter((b) => b.time >= fromSec);
    entryTouch = after.some((b) => (dir === "up" ? b.high >= lock : b.low <= lock)) ? 1 : 0;
  }
  return { entered: !!tr.entered, entryTouch, entryPrice: tr.entryPrice, entryAt: tr.entryAt,
           entryRNow: tr.entryRNow, entryRetrace: tr.entryRetrace, entryExtremeDepth: tr.entryExtremeDepth, entryRemainSec: tr.entryRemainSec,
           closed: !!tr.closed, closePrice: tr.closePrice, closeAt: tr.closeAt, closeReason: tr.closeReason,
           taVer: tr.taVer };
}

async function resolveMissing() {
  if (resolving) return;
  resolving = true;
  const now = Math.floor(Date.now() / 1000);
  let done = 0;
  try {
    const all = [...ledger.values()];
    // ===== PASS 1 (PRIORITAS): sesi yang BELUM punya hasil =====
    // Dijalankan lebih dulu supaya hasil sesi baru tidak pernah tertunda oleh pekerjaan backfill.
    for (const r of all) {
      if (r.res || !r.sig || !r.t0) continue;
      const dir = r.sig.dir;
      if (dir !== "up" && dir !== "down") continue;
      const dur = DUR_S[r.interval];
      if (!dur) continue;
      if (now < r.t0 + dur + 5) continue;                  // ronde belum berakhir
      if (now > r.t0 + dur + 86400 * 85) continue;         // di luar jangkauan 1m klines (90d)
      try {
        const bars = await getKlines(r.asset, "1m", r.t0 + dur, Math.ceil(dur / 60) + 2);
        const sess = bars.filter((b) => b.time >= r.t0 && b.time < r.t0 + dur);
        if (sess.length < 2) continue;
        const lock = sess[0].open, close = sess[sess.length - 1].close;
        const path = sess.slice(1);
        const v = (p) => (dir === "up" ? (p - lock) / lock * 100 : (lock - p) / lock * 100);
        let mfe = -Infinity, mae = Infinity, touch = 0, tTouch = null;
        for (const b of path) {
          const fHi = dir === "up" ? v(b.high) : v(b.low);
          const fLo = dir === "up" ? v(b.low) : v(b.high);
          if (fHi > mfe) mfe = fHi;
          if (fLo < mae) mae = fLo;
          if (!touch && fHi >= 0) { touch = 1; tTouch = b.time; }
        }
        const actual = close >= lock ? "up" : "down";
        // STATUS TRADE ASSISTANT: entry & early close (engine) + entryTouch dari jalur 1m
        const trade = tradeInfoFor(r.asset, r.interval, r.t0, dir, lock, path);
        if (trade && trade.entered && trade.entryAt) {
          const t1 = await entryTouch1s(r.asset, r.t0, dur, trade.entryAt, lock, dir);
          if (t1) { trade.entryTouch = t1.touch; trade.entryTouchAt = t1.at; trade.touchSrc = "1s"; }
        }
        const merged = Object.assign({}, r, {
          res: {
            lock: +lock, close: +close, actual, won: dir === actual ? 1 : 0,
            touch, tTouchSec: tTouch,
            mfeFav: isFinite(mfe) ? +mfe.toFixed(4) : null,
            maeFav: isFinite(mae) ? +mae.toFixed(4) : null,
            endFav: +v(close).toFixed(4), bars: path.length, src: "server-1m",
            trade,
          },
          upd: Date.now(),
        });
        ledger.set(r.k, merged); appendLedger(merged); ledgerDirty++; done++;
      } catch (_) { /* coba lagi pada siklus berikutnya */ }
    }
    // ===== PASS 2: BACKFILL status trade (DIBATASI) =====
    // Melengkapi `trade` untuk record yang sudah punya hasil (mis. hasil lebih dulu dikirim klien).
    // Dulu loop ini berjalan untuk SEMUA record lama -> ratusan permintaan klines tiap siklus dan
    // resolusi sesi baru tertunda (13s -> ~100s). Sekarang: hanya sesi <= 45 menit terakhir,
    // maksimum 10 percobaan per siklus, dan maksimum 3 percobaan per record (dicatat di trTry).
    let tried = 0;
    for (const r of all) {
      if (tried >= 10) break;
      if (!r.res || r.res.trade || !r.sig || !r.t0) continue;
      const dir = r.sig.dir;
      if (dir !== "up" && dir !== "down") continue;
      const dur = DUR_S[r.interval];
      if (!dur) continue;
      if (now < r.t0 + dur + 5) continue;
      if (now > r.t0 + dur + 45 * 60) continue;              // hanya sesi baru (state engine masih ada)
      if ((r.trTry || 0) >= 3) continue;                     // sudah dicoba 3x -> berhenti
      tried++;
      let got = null;
      try {
        const barsB = await getKlines(r.asset, "1m", r.t0 + dur, Math.ceil(dur / 60) + 2);
        const sessB = barsB.filter((b) => b.time >= r.t0 && b.time < r.t0 + dur);
        if (sessB.length >= 2) {
          got = tradeInfoFor(r.asset, r.interval, r.t0, dir, sessB[0].open, sessB.slice(1));
          if (got && got.entered && got.entryAt) {
            const t1b = await entryTouch1s(r.asset, r.t0, dur, got.entryAt, sessB[0].open, dir);
            if (t1b) { got.entryTouch = t1b.touch; got.entryTouchAt = t1b.at; got.touchSrc = "1s"; }
          }
        }
      } catch (_) { /* dicoba lagi siklus berikutnya */ }
      const next = Object.assign({}, r, { trTry: (r.trTry || 0) + 1 });
      if (got) { next.res = Object.assign({}, r.res, { trade: got }); done++; }
      next.upd = Date.now();
      ledger.set(r.k, next); appendLedger(next); ledgerDirty++;
    }
    // ===== PASS 3: perbaiki entryTouch yang dihitung dari bar 1m (presisi rendah) =====
    // Record dengan posisi tetapi touchSrc bukan "1s" dihitung ulang memakai klines 1 detik.
    // Dibatasi seperti backfill: hanya sesi <= 45 menit terakhir, maks 10 per siklus.
    let fixed = 0;
    for (const r of all) {
      if (fixed >= 10) break;
      const tr = r.res && r.res.trade;
      if (!tr || !tr.entered || tr.touchSrc === "1s") continue;
      const dur = DUR_S[r.interval];
      if (!dur || !r.t0) continue;
      if (now < r.t0 + dur + 5) continue;
      if (now > r.t0 + dur + 45 * 60) continue;
      const t1 = await entryTouch1s(r.asset, r.t0, dur, tr.entryAt, r.res.lock, r.sig.dir);
      if (!t1) continue;
      fixed++;
      const tr2 = Object.assign({}, tr, { entryTouch: t1.touch, entryTouchAt: t1.at, touchSrc: "1s" });
      const next = Object.assign({}, r, { res: Object.assign({}, r.res, { trade: tr2 }), upd: Date.now() });
      ledger.set(r.k, next); appendLedger(next); ledgerDirty++; done++;
    }
  } finally { resolving = false; }
  if (done) {
    console.log(`[LEDGER] resolved ${done} outcome(s) dari 1m klines · total ${ledger.size}`);
    // PUSH realtime: beri tahu klien bahwa ada hasil baru supaya panel akurasi langsung memuat ulang.
    broadcast("ledger", { resolved: done, total: ledger.size, at: Date.now() });
  }
}
// ===== OFI SUPER-REALTIME (default 300 ms) =====
// Loop engine berjalan tiap 2 detik, sehingga OFI terasa lambat. Poller ini KHUSUS OFI dan
// berjalan jauh lebih cepat: mengambil klines 1 DETIK terakhir (forming candle ikut ter-update
// di dalam detik) lalu menambahkannya ke akumulator flow dan mengirim event SSE ringan `ofi`.
// Hanya berjalan bila ada klien (SSE) sehingga kuota Binance tetap hemat.
// Kuota: 2 aset x (1000/OFI_POLL_MS) req/s; pada 300 ms = ~6,7 req/s (weight ~13/s) — jauh di
// bawah limit Binance (6000 weight/menit). Set OFI_POLL_MS untuk mengubah.
const OFI_POLL_MS = Math.max(200, parseInt(process.env.OFI_POLL_MS || "300", 10));
async function ofiFastTick() {
  try {
    // kedua aset diambil PARALEL supaya durasi tick singkat (~50ms) dan cadence mendekati 300 ms
    const ks = await Promise.all(["BTC", "ETH", "BNB"].map((sym) => getKlines(sym, "1s", undefined, 6)));
    ["BTC", "ETH", "BNB"].forEach((sym, i) => FLOW.addKlines(sym, ks[i]));
    const nowS = Math.floor(Date.now() / 1000);
    // OFI = AKUMULASI SESI (buy-sell)/(buy+sell) dari awal sesi sampai sekarang, dengan definisi
    // sesi SESUAI TIMEFRAME. Sebelumnya jalur cepat ini selalu memakai sesi 5 menit, sehingga
    // untuk tf 15m/1h angkanya BEDA dengan snapshot (yang memakai sesi tf tsb). Kini ketiga tf
    // dikirim sekaligus (murah: hanya menjumlah bucket menit) dan klien memilih sesuai tf aktif.
    const TFSEC = { "5m": 300, "15m": 900, "1h": 3600 };
    const assets = {};
    for (const sym of ["BTC", "ETH", "BNB"]) {
      const byTf = {};
      for (const tf of Object.keys(TFSEC)) {
        const t0tf = Math.floor(nowS / TFSEC[tf]) * TFSEC[tf];
        byTf[tf] = {
          ofi: FLOW.sessionOFI(sym, t0tf, nowS),
          ofiShort: FLOW.sessionOFI(sym, nowS - 120, nowS),
        };
      }
      assets[sym] = { ofi: byTf["5m"].ofi, ofiShort: byTf["5m"].ofiShort, byTf };
    }
    broadcast("ofi", { at: Date.now(), poll: OFI_POLL_MS, assets });
  } catch (_) { /* lewati tick ini */ }
}
setInterval(() => { if (clients.size) ofiFastTick(); }, OFI_POLL_MS);

// Resolver hasil sesi: dijalankan 8 detik setelah boot lalu SETIAP 15 DETIK supaya riwayat
// (benar/salah + entry/early close) muncul maksimal ~20 detik setelah sesi berakhir — sebelumnya
// siklus 5 menit membuat panel akurasi terasa sangat lambat terupdate.
// Aman dijalankan sering: hanya record yang SESINYA SUDAH BERAKHIR dan belum punya hasil yang
// diproses (sisanya `continue`), jadi tidak ada permintaan klines berulang-ulang.
setTimeout(() => resolveMissing().catch(() => {}), 8000);
setInterval(() => resolveMissing().catch(() => {}), 15 * 1000);

/* ===== PHASE 3: MODEL SERVING + RE-FIT TERJADWAL =====
   Model belajar (gate/touch/lessons) disajikan dari volume; app mengambilnya lewat
   /api/model/*. Re-fit dijalankan otomatis sekali sehari: bangun kandidat dari ledger,
   uji pada jendela uji, dan HANYA ganti model bila kandidat menang out-of-sample
   (lihat learner.js shouldPromote). Kalau tidak menang, model lama tetap dipakai. */
/* Gabungkan satu record ke ledger. Aturan penting:
   - slot `sig` diisi oleh capture yang paling dekat ke detik ke-2 (capOffsetMs terkecil),
     BUKAN yang ter-upload lebih dulu; snapshot lain disimpan di `alts` (audit).
   - `res` tidak pernah ditimpa oleh record tanpa res; hasil yang lebih baru boleh menggantikan
     (mis. perkiraan 1m server -> jalur 1s klien yang lebih presisi). */
function mergeRecord(r) {
  if (!r || typeof r.k !== "string") return false;
  const prev = ledger.get(r.k) || { k: r.k };
  if (!r.sig && !prev.sig) return false;                 // tanpa fitur -> tidak berguna
  const merged = Object.assign({}, prev, r);
  const off = (s) => (s && typeof s.capOffsetMs === "number" ? s.capOffsetMs : Infinity);
  // PENTING: `sig` dan `gate` HARUS berasal dari SUMBER YANG SAMA. Sebelumnya `Object.assign`
  // selalu memakai `gate` dari record masuk, padahal `sig` bisa menang dari record lama ->
  // 87/1684 record punya sig.accepted != gate.accepted (label learner jadi tidak konsisten).
  if (prev.sig && r.sig) {
    if (off(r.sig) < off(prev.sig)) {
      merged.sig = r.sig;
      if (r.gate) merged.gate = r.gate;                     // pasangkan gate dgn sig yang menang
      merged.alts = (prev.alts || []).concat([{ capOffsetMs: off(prev.sig), sig: prev.sig, gate: prev.gate }]).slice(-6);
    } else {
      merged.sig = prev.sig;
      if (prev.gate) merged.gate = prev.gate;               // pasangkan gate dgn sig yang menang
      merged.alts = (prev.alts || []).concat([{ capOffsetMs: off(r.sig), sig: r.sig, gate: r.gate }]).slice(-6);
    }
  } else if (prev.sig) { merged.sig = prev.sig; if (prev.gate) merged.gate = prev.gate; }
  if (prev.res && !r.res) merged.res = prev.res;
  // JANGAN BUANG status Trade Assistant. Klien juga mengirim `res` (hasil dari jalur 1s yang
  // lebih presisi) TANPA field `trade`, sehingga saat res klien menimpa res server, catatan
  // entry/early close hilang -> baris E/C di panel riwayat jadi abu walau posisinya nyata.
  if (r.res && prev.res && prev.res.trade && !r.res.trade) {
    merged.res = Object.assign({}, r.res, { trade: prev.res.trade });
  }
  merged.upd = Date.now();
  ledger.set(r.k, merged);
  appendLedger(merged);
  ledgerDirty++;
  return true;
}

const LEARNER = require("./learner");
const { createCapture } = require("./capture");
const MODEL_DIR = path.join(LEDGER_DIR, "..", "models");
const MODEL_CUR = path.join(MODEL_DIR, "current");
const MODEL_LOG = path.join(MODEL_DIR, "promote.jsonl");
const DEFAULT_OUT = path.join(__dirname, "backtest", "out");
const GATES_DEF = require("./gates.js");
const MODEL_FILES = { gate: "learn_gate.json", touch: "learn_touch90.json", lessons: "lessons.json", gates: "gates.json", pnl: "learn_pnl.json", apply: "learn_apply.json", veto: "learn_veto.json" };
let gatesMeta = { mode: GATES_DEF.BOOTSTRAP.mode, promotedAt: null };
let modelMeta = { version: "default", promotedAt: null, metrics: null };

function ensureModelDirs() { try { fs.mkdirSync(MODEL_CUR, { recursive: true }); } catch (_) {} }
function readModelPart(part) {
  const f = MODEL_FILES[part]; if (!f) return null;
  try { const p = path.join(MODEL_CUR, f); if (fs.existsSync(p)) return JSON.parse(fs.readFileSync(p, "utf8")); } catch (_) {}
  try { const p = path.join(DEFAULT_OUT, f); if (fs.existsSync(p)) return JSON.parse(fs.readFileSync(p, "utf8")); } catch (_) {}
  return null;
}
function loadModelMeta() {
  try {
    const p = path.join(MODEL_CUR, "meta.json");
    if (fs.existsSync(p)) modelMeta = JSON.parse(fs.readFileSync(p, "utf8"));
  } catch (_) {}
}
// Profil gate yang dipakai app: hasil belajar (volume) bila ada, kalau belum -> bootstrap.
function readGates() {
  try {
    const p = path.join(MODEL_CUR, "gates.json");
    if (fs.existsSync(p)) return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch (_) {}
  // Belum ada ambang hasil belajar. Default = bootstrap (dilonggarkan, untuk mengumpulkan data);
  // set GATES_MODE=strict untuk kembali ke ambang konservatif TANPA deploy ulang.
  const mode = String(process.env.GATES_MODE || "bootstrap").toLowerCase();
  return mode === "strict" ? GATES_DEF.STRICT : GATES_DEF.BOOTSTRAP;
}
function incumbentModel() {          // bentuk {metrics, gate:{rules}, touch:{rules}} untuk perbandingan
  const g = readModelPart("gate"), t = readModelPart("touch");
  if (!g || !g.metrics) return null;
  return { metrics: g.metrics, gate: { rules: g.rules || [] }, touch: { rules: (t && t.rules) || [] } };
}
let refitting = false;
async function refit(trigger = "manual") {
  if (refitting) return { ok: false, why: "re-fit sedang berjalan" };
  refitting = true;
  try {
    const records = [...ledger.values()];
    const rows = LEARNER.rowsFrom(records);
    const cand = LEARNER.buildModel(rows);
    const res = { trigger, at: new Date().toISOString(), rows: rows.length, ok: cand.ok };
    if (!cand.ok) { res.why = cand.reason; res.promote = false; }
    else {
      const inc = incumbentModel();
      // FIX: bandingkan kandidat vs incumbent pada JENDELA UJI SEKARANG yang SAMA, dengan
      // validasi BERGULIR (3 lipatan). Sebelumnya incumbent dinilai dari metrik lama (jendela
      // kecil 3 hari lalu) sehingga kandidat bagus tak pernah bisa promote.
      const splitIdxNow = Math.floor(rows.length * 0.7);
      const testNow = rows.slice(splitIdxNow);
      const candEval = LEARNER.evalModelRolling(testNow, cand.gate.rules, cand.touch.rules, 3);
      const incEval = inc ? LEARNER.evalModelRolling(testNow, inc.gate.rules, inc.touch.rules, 3) : null;
      // OBJEKTIF PnL-TRADE (res.trade): agar promosi mengejar PROFIT, bukan hanya winrate.
      const candPnl = LEARNER.evalModelPnl(testNow, cand.gate.rules, cand.touch.rules);
      const incPnl = inc ? LEARNER.evalModelPnl(testNow, inc.gate.rules, inc.touch.rules) : null;
      const dec = LEARNER.shouldPromote(
        { metrics: Object.assign({}, candEval, { pnl: candPnl }) },
        incEval ? { metrics: Object.assign({}, incEval, { pnl: incPnl }) } : null);
      res.promote = dec.promote; res.why = dec.why;
      res.candidate = candEval;
      res.candidateSingle = cand.metrics;
      res.incumbent = incEval;
      res.incumbentStored = inc ? inc.metrics : null;
      res.rollFold = { k: 3, candScoreMin: candEval.scoreMin, incScoreMin: incEval ? incEval.scoreMin : null };
      res.pnl = { candidate: candPnl, incumbent: incPnl, allMeanPnl: cand.pnl && cand.pnl.all, n: cand.pnl && cand.pnl.n };
      ensureModelDirs();
      const ver = new Date().toISOString().replace(/[:.]/g, "-");
      const hist = path.join(MODEL_DIR, "v-" + ver);
      try { fs.mkdirSync(hist, { recursive: true }); } catch (_) {}
      const write = (name, obj) => {
        fs.writeFileSync(path.join(MODEL_CUR, name), JSON.stringify(obj, null, 1));
        try { fs.writeFileSync(path.join(hist, name), JSON.stringify(obj, null, 1)); } catch (_) {}
      };

      // ---- BELAJAR THRESHOLD: sesuaikan ambang kriteria/filter dari data nyata ----
      // Hanya dipakai bila pada jendela UJI dia benar-benar lebih baik (Wilson LB naik,
      // cakupan masih memadai, winrate naik). Kalau tidak, profil gate lama tetap berlaku.
      const th = LEARNER.learnThresholds(rows);
      res.thresholds = th.ok
        ? { note: th.note, train: th.train, test: th.test, baselineTest: th.baselineTest, testLb: th.testLb, baselineLbTest: th.baselineLbTest, beatsBaseline: th.beatsBaseline }
        : { ok: false, reason: th.reason };
      if (th.ok && th.beatsBaseline) {
        const gates = GATES_DEF.fromThresholds(th.thresholds, { metrics: th.test, note: th.note });
        gates.generated = new Date().toISOString(); gates.version = ver; gates.rows = rows.length;
        gates.train = th.train; gates.baselineTest = th.baselineTest; gates.testLb = th.testLb; gates.baselineLbTest = th.baselineLbTest;
        write("gates.json", gates);
        gatesMeta = { mode: "learned", promotedAt: gates.generated, version: ver, thresholds: th.thresholds, metrics: th.test, note: th.note };
        res.gatesPromoted = true;
      } else res.gatesPromoted = false;

      // Tulis konteks PnL-trade SETIAP refit (informasional: konteks TA_FEATS paling untung/rugi)
      try { write("learn_pnl.json", Object.assign({ generated: new Date().toISOString(), version: ver, rows: cand.rows, test: cand.pnlTest }, cand.pnl)); } catch (_) {}
      // ===== LANGKAH CAKUPAN: jangan terapkan blocker bila model memblok terlalu banyak (mis. seluruh BTC/down) =====
      const liveEval = dec.promote ? candEval : (incEval || candEval);
      const minApplyCov = Number(process.env.MIN_APPLY_COV != null ? process.env.MIN_APPLY_COV : 0.35);
      const applyBlockers = !!liveEval && (liveEval.coverage || 1) >= minApplyCov;
      try { write("learn_apply.json", { apply: applyBlockers, coverage: liveEval ? liveEval.coverage : null, minApplyCov, at: new Date().toISOString(), note: applyBlockers ? "blocker diterapkan" : `cakupan ${(100 * (liveEval ? liveEval.coverage : 0)).toFixed(0)}% < ${(minApplyCov * 100).toFixed(0)}% -> blocker TIDAK diterapkan (cegah agresif)` }); } catch (_) {}
      res.applyBlockers = applyBlockers;
      // ===== JAM OFF ADAPTIF: hitung WR per jam WIB dari data, tulis tiap refit =====
      try {
        const hv = LEARNER.hourVetoes(rows, { minN: Number(process.env.VETO_MIN_N || 30), thr: Number(process.env.VETO_WR_THR || 0.53), recentN: Number(process.env.VETO_RECENT_N || 3), recentWin: Number(process.env.VETO_RECENT_WIN || 2) });
        write("learn_veto.json", Object.assign({ generated: new Date().toISOString(), trigger: "refit", version: ver }, hv));
        res.vetoHours = hv.hours;
      } catch (e) { res.vetoErr = String(e && e.message); }
      if (dec.promote) {
        write("learn_gate.json", Object.assign({ generated: new Date().toISOString(), source: "ledger", version: ver, rows: cand.rows, metrics: cand.metrics, baseline: cand.baseline }, cand.gate));
        write("learn_touch90.json", Object.assign({ generated: new Date().toISOString(), source: "ledger", version: ver, rows: cand.rows, metrics: cand.metrics, baseline: cand.baseline }, cand.touch));
        write("lessons.json", Object.assign({ generated: new Date().toISOString(), version: ver }, cand.lessons));
        const meta = { version: ver, promotedAt: new Date().toISOString(), trigger, rows: cand.rows, metrics: candEval, metricsSingle: cand.metrics, baseline: cand.baseline, why: dec.why, pnl: { test: cand.pnlTest, allMeanPnl: cand.pnl && cand.pnl.all, n: cand.pnl && cand.pnl.n }, gatesMode: gatesMeta.mode, gatesThresholds: gatesMeta.thresholds || null, gatesMetrics: gatesMeta.metrics || null };
        write("meta.json", meta);
        modelMeta = meta;
        res.version = ver;
      }
    }
    try { fs.appendFileSync(MODEL_LOG, JSON.stringify(res) + "\n"); } catch (_) {}
    console.log(`[REFIT] ${res.promote ? "PROMOTE" : "KEEP"} · ${res.why || ""} · rows ${rows.length} · gates ${res.gatesPromoted ? "BELAJAR-DIPAKAI" : "tetap"}`);
    return res;
  } finally { refitting = false; }
}
ensureModelDirs(); loadModelMeta();

// ---- Capture kanonik server-side: snapshot 2 detik tiap sesi 5m/15m tanpa perlu browser ----
const capture = createCapture({
  getKlines, SignalCore: require("./signal-core.js"), learner: LEARNER,
  getModel: (part) => readModelPart(part),
  getGates: () => readGates(),
  save: (rec) => {
    const ok = mergeRecord(rec);
    // beri tahu klien ada sesi/record baru (pending) supaya riwayat ikut ter-update
    try { if (ok) broadcast("ledger", { added: true, total: ledger.size, at: Date.now() }); } catch (_) {}
    return ok;
  },
  log: console.log,
});
capture.start();

// ---- ENGINE sinyal server-side: satu sumber kebenaran untuk semua device ----
const { createEngine } = require("./engine.js");
const engine = createEngine({
  // state Trade Assistant disimpan di volume yang sama dengan ledger supaya tidak hilang
  // saat container restart (deploy) -> chip ENTRY/EARLY CLOSE tidak "menghilang" lagi.
  stateFile: path.join(LEDGER_DIR, "trade_state.json"),
  getKlines,
  getModel: (part) => readModelPart(part),
  getGates: () => readGates(),
  log: console.log,
  onEvent: (ev) => broadcastTA(ev),
});
engine.start();
// Jadwal: re-fit otomatis tiap REFIT_INTERVAL_HOURS (default 6 jam) — lebih responsif dari 1x/hari.
const REFIT_INTERVAL_H = Math.max(1, parseFloat(process.env.REFIT_INTERVAL_HOURS || "3"));   // refit tiap 3 jam (default)
const REFIT_CHECK_MIN = Math.max(1, parseInt(process.env.REFIT_CHECK_MIN || "10", 10));
// Kapan re-fit terakhir berjalan? Dibaca dari promote.jsonl supaya tahan restart container.
function lastRefitTime() {
  try {
    if (!fs.existsSync(MODEL_LOG)) return 0;
    const lines = fs.readFileSync(MODEL_LOG, "utf8").trim().split("\n");
    for (let i = lines.length - 1; i >= 0; i--) {
      try { const j = JSON.parse(lines[i]); if (j && j.at) return Date.parse(j.at); } catch (_) {}
    }
  } catch (_) {}
  return 0;
}
setInterval(() => {
  const last = lastRefitTime();
  const due = last === 0 || (Date.now() - last) > REFIT_INTERVAL_H * 3600000;
  if (due) {
    console.log(`[REFIT] memulai re-fit otomatis (interval ${REFIT_INTERVAL_H} jam)`);
    refit("jadwal").catch(() => {});
  }
}, REFIT_CHECK_MIN * 60 * 1000);
console.log(`[REFIT] otomatis: cek tiap ${REFIT_CHECK_MIN} menit · re-fit tiap ${REFIT_INTERVAL_H} jam`);

// ===== JAM OFF RESPONSIF =====
// Cek tiap VETO_CHECK_MIN (default 2 mnt): bila jam yang diblokir mulai MEMBAIK
// (mis. 3 sesi terakhir jam itu >=2 menang), jam tsb DIBUKA segera (tak menunggu refit 3 jam).
const VETO_CHECK_MIN = Math.max(1, parseInt(process.env.VETO_CHECK_MIN || "2", 10));
let lastVetoKey = null;
function computeVetoNow() {
  const rows = LEARNER.rowsFrom([...ledger.values()]);
  return LEARNER.hourVetoes(rows, {
    minN: Number(process.env.VETO_MIN_N || 30), thr: Number(process.env.VETO_WR_THR || 0.53),
    recentN: Number(process.env.VETO_RECENT_N || 3), recentWin: Number(process.env.VETO_RECENT_WIN || 2),
  });
}
function refreshVeto(tag) {
  try {
    const hv = computeVetoNow();
    const key = JSON.stringify(hv.hours);
    if (key !== lastVetoKey) {
      lastVetoKey = key;
      ensureModelDirs();
      fs.writeFileSync(path.join(MODEL_CUR, "learn_veto.json"), JSON.stringify(Object.assign({ generated: new Date().toISOString(), trigger: tag }, hv), null, 1));
      console.log(`[VETO] jam OFF diperbarui (${tag}): [${hv.hours.join(",")}]`);
    }
  } catch (e) { console.log(`[VETO] error: ${e && e.message}`); }
}
setTimeout(() => refreshVeto("awal"), 15000);
setInterval(() => refreshVeto("periodik"), VETO_CHECK_MIN * 60 * 1000);
console.log(`[VETO] cek jam OFF tiap ${VETO_CHECK_MIN} menit (buka cepat bila jam membaik >=${Number(process.env.VETO_RECENT_WIN || 2)}/${Number(process.env.VETO_RECENT_N || 3)} sesi terakhir)`);
setTimeout(() => { try { const l = lastRefitTime(); if (l) console.log(`[REFIT] re-fit terakhir: ${new Date(l).toISOString()}`); } catch (_) {} }, 3000);

const clients = new Set();
let bnWs = null, bnPollTimer = null, bnHostIdx = 0;

// Active visitor tracking
const activeVisitors = new Map();  // id -> { lastSeen, userAgent }
const VISITOR_TIMEOUT = 60000;     // consider visitor inactive after 60s

setInterval(() => {
  const now = Date.now();
  for (const [id, v] of activeVisitors) {
    if (now - v.lastSeen > VISITOR_TIMEOUT) activeVisitors.delete(id);
  }
}, 30000);

const BN_HOSTS = ["wss://data-stream.binance.vision", "wss://stream.binance.com:9443"];

const INTERVAL_MS = { "5m": 300000, "15m": 900000, "1h": 3600000 };
const lockPrices = { BTC: {}, ETH: {}, BNB: {} };

function getRoundStart(tf, now) {
  const dur = INTERVAL_MS[tf] || 300000;
  return Math.floor(now / dur) * dur;
}

function broadcast(type, data) {
  const payload = `event: ${type}\ndata: ${JSON.stringify(data)}\n\n`;
  clients.forEach((res) => { try { res.write(payload); } catch (_) {} });
}
// Klien stream khusus EVENT Trade Assistant (dipakai bot untuk eksekusi latensi rendah).
const taClients = new Set();
function broadcastTA(ev) {
  const payload = `event: ta\ndata: ${JSON.stringify(ev)}\n\n`;
  taClients.forEach((res) => { try { res.write(payload); } catch (_) {} });
}

function startBinance() {
  if (bnWs || typeof WebSocket === "undefined") { fallbackPoll(); return; }
  const url = `${BN_HOSTS[bnHostIdx % BN_HOSTS.length]}/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade/btcusdt@ticker/ethusdt@ticker`;
  let ws;
  try { ws = new WebSocket(url); } catch (e) { bnHostIdx++; setTimeout(startBinance, 2000); return; }
  ws.onopen = () => { if (bnPollTimer) { clearInterval(bnPollTimer); bnPollTimer = null; } };
  ws.onmessage = (ev) => {
    try {
      const m = JSON.parse(ev.data); const d = m.data; if (!d) return;
      if (d.e === "aggTrade") {
        const sym = d.s === "BTCUSDT" ? "BTC" : d.s === "ETHUSDT" ? "ETH" : "BNB";
        const now = Date.now();
        
        // Capture lock price at session boundaries for all timeframes
        for (const tf of Object.keys(INTERVAL_MS)) {
          const roundStart = getRoundStart(tf, now);
          const key = `${tf}_${roundStart}`;
          if (!(key in lockPrices[sym])) {
            lockPrices[sym][key] = { price: +d.p, ts: d.T, roundStart };
            console.log(`[LOCK] ${sym} ${tf} lock price captured: ${+d.p} at ${new Date(d.T).toISOString()}`);
          }
        }
        
        // `m` = isBuyerMaker: true means the aggressor was the seller (taker sell).
        // CATATAN OFI: TIDAK diakumulasi dari sini. Stream aggTrade Binance diblok di sebagian
        // host (terbukti di Railway: log [LOCK] tidak pernah muncul), sedangkan REST kline selalu
        // jalan -> OFI dibangun dari kline (FLOW.addKlines di engine/capture) supaya SATU sumber
        // angka yang sama di semua environment (kalau dua-duanya diisi, volume jadi dobel).
        broadcast("trade", { sym, price: +d.p, qty: +d.q, ts: d.T, m: d.m });
      } else if (d.e === "24hrTicker") {
        const sym = d.s === "BTCUSDT" ? "BTC" : d.s === "ETHUSDT" ? "ETH" : "BNB";
        broadcast("ticker", { sym, chg: +d.P, last: +d.c });
      }
    } catch (_) {}
  };
  ws.onclose = () => { bnWs = null; setTimeout(startBinance, 2000); fallbackPoll(); };
  ws.onerror = () => { try { ws.close(); } catch (_) {} };
  bnWs = ws;
}

function fallbackPoll() {
  if (bnPollTimer) return;
  bnPollTimer = setInterval(async () => {
    try {
      const s = await getSnapshot(false);
      for (const k of ["BTC", "ETH", "BNB"]) {
        const ones = s.candles[k]["1s"]; const last = ones[ones.length - 1];
        if (last) broadcast("trade", { sym: k, price: last.close, qty: last.vol || 0, ts: last.time * 1000 });
        const t = s.ticker[k]; broadcast("ticker", { sym: k, chg: t.chg, last: t.last });
      }
    } catch (_) {}
  }, 1000);
}

http.createServer(async (req, res) => {
  const u = new URL(req.url, "http://x");

  if (u.pathname === "/api/snapshot") {
    try {
      const snap = await getSnapshot(u.searchParams.get("history") === "1");
      res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store", "Access-Control-Allow-Origin": "*" });
      res.end(JSON.stringify(snap));
    } catch (e) {
      res.writeHead(502, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: String(e) }));
    }
    return;
  }

  if (u.pathname === "/api/stream") {
    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      "Connection": "keep-alive",
      "Access-Control-Allow-Origin": "*",
    });
    res.write("\n");
    try {
      const snap = await getSnapshot(true);
      res.write(`event: snapshot\ndata: ${JSON.stringify(snap)}\n\n`);
    } catch (e) {
      res.write(`event: error\ndata: ${JSON.stringify({ error: String(e) })}\n\n`);
    }
    clients.add(res);
    startBinance();
    req.on("close", () => clients.delete(res));
    return;
  }

  if (u.pathname === "/api/klines") {
    try {
      const sym = u.searchParams.get("symbol") || "BTC";
      const tf = u.searchParams.get("tf") || "1s";
      const before = parseInt(u.searchParams.get("before") || "0", 10) || 0;
      const limit = Math.min(parseInt(u.searchParams.get("limit") || "600", 10), 1000);
      const candles = await getKlines(sym, tf, before, limit);
      res.writeHead(200, { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*", "Cache-Control": "no-store" });
      res.end(JSON.stringify({ candles }));
    } catch (e) {
      res.writeHead(502, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: String(e) }));
    }
    return;
  }

  if (u.pathname === "/api/lockprice") {
    try {
      const sym = u.searchParams.get("symbol") || "BTC";
      const tf = u.searchParams.get("tf") || "5m";
      const clientRoundStart = u.searchParams.get("roundStart");
      const now = Date.now();
      
      // If client provides roundStart, use it; otherwise calculate from server time
      let roundStart = clientRoundStart ? parseInt(clientRoundStart, 10) : getRoundStart(tf, now);
      const key = `${tf}_${roundStart}`;
      let lock = lockPrices[sym] && lockPrices[sym][key];
      
      // If exact match not found, look for most recent lock price for this timeframe
      if (!lock) {
        const allLocks = lockPrices[sym] || {};
        let bestKey = null;
        let bestTime = 0;
        for (const k of Object.keys(allLocks)) {
          if (k.startsWith(tf + '_')) {
            const lockTime = allLocks[k].ts;
            if (lockTime > bestTime) {
              bestTime = lockTime;
              bestKey = k;
            }
          }
        }
        if (bestKey) {
          lock = allLocks[bestKey];
        }
      }
      
      if (lock) {
        res.writeHead(200, { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*", "Cache-Control": "no-store" });
        res.end(JSON.stringify({ lockPrice: lock.price, ts: lock.ts, roundStart: lock.roundStart }));
      } else {
        res.writeHead(404, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Lock price not yet captured for this session" }));
      }
    } catch (e) {
      res.writeHead(502, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: String(e) }));
    }
    return;
  }

  // ---- SSE: sinyal live dari SERVER (dipakai semua device -> konsisten) ----
  if (u.pathname === "/api/live") {
    const tfArg = u.searchParams.get("tf");
    const tf = ["5m", "15m", "1h"].includes(tfArg) ? tfArg : "5m";
    res.writeHead(200, {
      "Content-Type": "text/event-stream; charset=utf-8",
      "Cache-Control": "no-cache, no-transform",
      "Connection": "keep-alive",
      "Access-Control-Allow-Origin": "*",
      "X-Accel-Buffering": "no",
    });
    res.write("retry: 3000\n\n");
    const release = engine.addSubscriber();
    const send = () => { try { res.write(`data: ${JSON.stringify(engine.snapshot(tf))}\n\n`); } catch (_) {} };
    send();                                   // snapshot pertama langsung
    const iv = setInterval(send, 1000);       // lalu tiap detik (harga live + sinyal terkunci)
    const ka = setInterval(() => { try { res.write(": keep-alive\n\n"); } catch (_) {} }, 20000);
    const stop = () => { clearInterval(iv); clearInterval(ka); release(); };
    req.on("close", stop); req.on("error", stop); res.on("close", stop);
    return;
  }
  // ===== STREAM EVENT TA (untuk bot: ENTRY/EXIT tepat saat transisi) =====
  // Sengaja TANPA snapshot: koneksi ringan & persisten, hanya menerima event transisi.
  if (u.pathname === "/api/ta-stream") {
    res.writeHead(200, {
      "Content-Type": "text/event-stream; charset=utf-8",
      "Cache-Control": "no-cache, no-transform",
      "Connection": "keep-alive",
      "Access-Control-Allow-Origin": "*",
      "X-Accel-Buffering": "no",
    });
    res.write("retry: 1000\n\n");
    const release = engine.addSubscriber();                       // paksa engine polling penuh
    try { res.write(`event: hello\ndata: ${JSON.stringify({ at: Date.now() })}\n\n`); } catch (_) {}
    taClients.add(res);
    const ka = setInterval(() => { try { res.write(": ka\n\n"); } catch (_) {} }, 15000);
    const stop = () => { clearInterval(ka); taClients.delete(res); release(); };
    req.on("close", stop); req.on("error", stop); res.on("close", stop);
    return;
  }

  // Snapshot sinyal sebagai JSON (fallback non-SSE + untuk debugging/monitoring)
  if (u.pathname === "/api/signal") {
    const tfArg = u.searchParams.get("tf");
    const tf = ["5m", "15m", "1h"].includes(tfArg) ? tfArg : "5m";
    engine.touch();                       // penuhi permintaan: engine refresh data segera
    res.writeHead(200, Object.assign({ "Content-Type": "application/json" }, CORS));
    res.end(JSON.stringify(engine.snapshot(tf)));
    return;
  }
  if (u.pathname === "/api/engine") {
    res.writeHead(200, Object.assign({ "Content-Type": "application/json" }, CORS));
    res.end(JSON.stringify(engine.status()));
    return;
  }

  // favicon: sebagian browser masih meminta /favicon.ico secara otomatis. Layani dengan SVG
  // (Chrome/Safari menerima SVG di jalur ini) supaya console tidak penuh 404.
  if (u.pathname === "/favicon.ico" || u.pathname === "/favicon.svg") {
    try {
      const svg = fs.readFileSync(path.join(__dirname, "favicon.svg"));
      res.writeHead(200, { "Content-Type": "image/svg+xml", "Cache-Control": "public, max-age=86400" });
      res.end(svg);
    } catch (_) { res.writeHead(204); res.end(); }
    return;
  }

  if (u.pathname === "/api/ledger") {
    if (req.method === "OPTIONS") { res.writeHead(204, CORS); res.end(); return; }
    if (req.method === "POST") {
      let body = "", tooBig = false;
      req.on("data", (ch) => { body += ch; if (body.length > 4e6) { tooBig = true; req.destroy(); } });
      req.on("end", () => {
        if (tooBig) { res.writeHead(413, CORS); res.end('{"error":"too big"}'); return; }
        let saved = 0;
        try {
          const j = JSON.parse(body || "{}");
          const recs = Array.isArray(j.records) ? j.records : (j.record ? [j.record] : []);
          for (const r of recs) { if (mergeRecord(r)) saved++; }
          if (saved) { /* ledgerDirty sudah ditambah di mergeRecord */ }
        } catch (e) { console.warn("[LEDGER] bad POST:", e.message); }
        res.writeHead(200, Object.assign({ "Content-Type": "application/json" }, CORS));
        res.end(JSON.stringify({ saved, total: ledger.size }));
      });
      return;
    }
    const st = ledgerStats();
    res.writeHead(200, Object.assign({ "Content-Type": "application/json" }, CORS));
    if (u.searchParams.get("dump") === "1") {
      res.end(JSON.stringify({ stats: st, records: [...ledger.values()] }));
    } else {
      res.end(JSON.stringify(st));
    }
    return;
  }

  // ---- PHASE 3: model belajar yang sedang dipakai ----
  if (u.pathname.startsWith("/api/model")) {
    const part = u.pathname.replace("/api/model", "").replace(/^\//, "");
    if (part === "refit") {
      if (req.method !== "POST") { res.writeHead(405, CORS); res.end('{"error":"POST only"}'); return; }
      refit("manual").then((r) => {
        res.writeHead(200, Object.assign({ "Content-Type": "application/json" }, CORS));
        res.end(JSON.stringify(r));
      }).catch((e) => { res.writeHead(500, CORS); res.end(JSON.stringify({ error: String(e) })); });
      return;
    }
    if (part === "" || part === "meta") {
      res.writeHead(200, Object.assign({ "Content-Type": "application/json" }, CORS));
      res.end(JSON.stringify({ meta: modelMeta, served: { gate: !!readModelPart("gate"), touch: !!readModelPart("touch"), lessons: !!readModelPart("lessons") }, pending: (() => { let n = 0; for (const r of ledger.values()) if (r.res && !r.sig) n++; return n; })() }));
      return;
    }
    if (part === "gates") {
      const g = readGates();
      res.writeHead(200, Object.assign({ "Content-Type": "application/json" }, CORS));
      res.end(JSON.stringify(g));
      return;
    }
    if (MODEL_FILES[part]) {
      const m = readModelPart(part);
      if (!m) { res.writeHead(404, CORS); res.end('{"error":"no model"}'); return; }
      res.writeHead(200, Object.assign({ "Content-Type": "application/json" }, CORS));
      res.end(JSON.stringify(m));
      return;
    }
    res.writeHead(404, CORS); res.end('{"error":"unknown model part"}');
    return;
  }

  // ---- STATUS LEARNER (dipakai panel UI agar user bisa memantau proses belajar) ----
  if (u.pathname === "/api/learner") {
    const CANON_MS = 6000, TARGET = 300, TARGET_CTX = 120;   // 300 = ambang bisa dipelajari; 120 = model konteks sudah bisa
    let total = 0, withSig = 0, withRes = 0, canon = 0, canonRes = 0, late = 0, sincePromote = 0, rate24h = 0;
    let recentRes = 0, firstUpd = Infinity;
    const dayAgo = Date.now() - 86400000;
    const promotedTs = modelMeta && modelMeta.promotedAt ? Date.parse(modelMeta.promotedAt) : null;
    let lastUpd = 0;
    for (const r of ledger.values()) {
      total++;
      if (r.upd && r.upd > lastUpd) lastUpd = r.upd;
      if (promotedTs && r.upd && r.upd > promotedTs) sincePromote++;
      if (!r.sig) continue;
      withSig++;
      if (r.res) withRes++;
      const s = r.sig;
      const off = typeof s.capOffsetMs === "number" ? s.capOffsetMs : null;
      const isCanon = off != null ? off <= CANON_MS : (s.minuteIn == null || s.minuteIn <= 1);
      if (r.upd && r.upd < firstUpd) firstUpd = r.upd;
      if (isCanon) { canon++; if (r.res) { canonRes++; if (r.upd && r.upd > dayAgo) recentRes++; } } else late++;
    }
    const g = readModelPart("gate"), t = readModelPart("touch");
    const single = (rules, prefix) => (rules || []).filter((k) => typeof k === "string" && k.indexOf("&") === -1 && k.indexOf(prefix) === 0);
    let history = [];
    try {
      if (fs.existsSync(MODEL_LOG)) {
        history = fs.readFileSync(MODEL_LOG, "utf8").split("\n").filter(Boolean).slice(-8).map((l) => { try { return JSON.parse(l); } catch (_) { return null; } }).filter(Boolean);
      }
    } catch (_) {}
    const learned = !!(modelMeta && modelMeta.version && modelMeta.version !== "default");
    res.writeHead(200, Object.assign({ "Content-Type": "application/json" }, CORS));
    res.end(JSON.stringify({
      ledger: (() => {
        // Laju per 24 jam dihitung terhadap RENTANG OBSERVASI sebenarnya: kalau capture baru
        // berjalan 40 menit, membagi dengan 24 jam membuat laju tampak 30x lebih kecil.
        const spanH = Math.max(0.25, (Date.now() - (isFinite(firstUpd) ? firstUpd : Date.now())) / 3600000);
        rate24h = Math.round(recentRes / Math.min(24, spanH) * 24);
        return {
          total, withSig, withRes, canonical: canon, canonicalWithRes: canonRes, late,
          target: TARGET, targetCtx: TARGET_CTX, pct: +Math.min(1, canonRes / TARGET).toFixed(3),
          lastUpd: lastUpd || null, sincePromote, rate24h, spanHours: +spanH.toFixed(2),
          etaDays: rate24h > 0 ? +Math.max(0, (TARGET - canonRes) / rate24h).toFixed(2) : null,
        };
      })(),
      // jadwal re-fit berikutnya (jam server) supaya user tahu kapan model bisa berubah
      nextRefitAt: (() => { const l = lastRefitTime(); const base = l > 0 ? l : Date.now(); return new Date(base + REFIT_INTERVAL_H * 3600000).toISOString(); })(),
      model: {
        source: learned ? "learned" : "default",
        version: modelMeta.version || "default", promotedAt: modelMeta.promotedAt || null, trigger: modelMeta.trigger || null,
        rows: modelMeta.rows || (g && g.rows) || null,
        metrics: modelMeta.metrics || (g && g.metrics) || null,
        baseline: (g && g.baseline) || null,
        why: modelMeta.why || null,
      },
      blockers: { gate: single(g && g.suppress, "interval="), touch: single(t && t.suppress, "gap=") },
      gates: (() => { const gg = readGates(); return { mode: gg.mode, thresholds: gg.thresholds || [], liqFloorMul: gg.liqFloorMul, lateFrac: gg.lateFrac, note: gg.note, metrics: gg.metrics || null, promotedAt: gatesMeta.promotedAt || null }; })(),
      capture: capture.status(),
      history,
    }));
    return;
  }

  if (u.pathname === "/api/stats") {
    res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store", "Access-Control-Allow-Origin": "*" });
    res.end(JSON.stringify({ activeUsers: activeVisitors.size }));
    return;
  }

  if (u.pathname === "/api/visit") {
    const id = u.searchParams.get("id") || (Date.now().toString(36) + Math.random().toString(36).slice(2, 8));
    activeVisitors.set(id, { lastSeen: Date.now(), userAgent: req.headers["user-agent"] || "" });
    res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store", "Access-Control-Allow-Origin": "*" });
    res.end(JSON.stringify({ id, activeUsers: activeVisitors.size }));
    return;
  }

  let p = u.pathname === "/" ? "/index.html" : u.pathname;
  const fp = path.join(__dirname, decodeURIComponent(p));
  if (!fp.startsWith(__dirname)) { res.writeHead(403); res.end(); return; }
  fs.readFile(fp, (err, data) => {
    if (err) { res.writeHead(404); res.end("not found"); return; }
    res.writeHead(200, { "Content-Type": MIME[path.extname(fp)] || "application/octet-stream", "Cache-Control": "no-store" });
    res.end(data);
  });
}).listen(PORT, () => console.log("Serving on http://localhost:" + PORT));
