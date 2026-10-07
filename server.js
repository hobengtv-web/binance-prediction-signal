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
      if (!r.sig || !r.t0) continue;
      // Resolve record baru ATAU re-resolve FLAT lama yg belum punya skor arah (backfill "arah silent").
      const needScore = !r.res || (r.res.flat === true && r.res.won == null && (r.sig.dir === "up" || r.sig.dir === "down"));
      if (!needScore) continue;
      const dir = r.sig.dir;
      const flatSig = (r.sig.skipped === "flat-price" || r.sig.skipped === "flat-noise");
      const isFlat = flatSig || (dir !== "up" && dir !== "down");
      if (isFlat && !flatSig) continue;   // resolve record FLAT/NOISE informasional (flat-price & flat-noise)
      const dur = DUR_S[r.interval];
      if (!dur) continue;
      if (now < r.t0 + dur + 5) continue;                  // ronde belum berakhir
      if (now > r.t0 + dur + 86400 * 85) continue;         // di luar jangkauan 1m klines (90d)
      try {
        const bars = await getKlines(r.asset, "1m", r.t0 + dur, Math.ceil(dur / 60) + 2);
        const sess = bars.filter((b) => b.time >= r.t0 && b.time < r.t0 + dur);
        if (sess.length < 2) continue;
        const lock = sess[0].open, close = sess[sess.length - 1].close;
        if (isFlat) {
          // Record FLAT: lock/close/actual + SKOR arah (permintaan user: tiap sesi direkam & DIPELAJARI,
          // termasuk flat). won diisi bila ada arah (flat-noise / arah "silent" flat-price), else null.
          const factual = (close >= lock ? "up" : "down");
          const fdir = (dir === "up" || dir === "down") ? dir : null;
          const sroi = fdir ? settleRoiPct(fdir, factual, r.odds) : null;
          const merged = Object.assign({}, r, { res: { lock: +lock, close: +close, actual: factual, dir: fdir, won: fdir ? (fdir === factual ? 1 : 0) : null, flat: true, silent: !!(r.sig && r.sig.silent), settleRoi: sroi, settleSrc: sroi != null ? "binance-quote" : null, bars: sess.length - 1, src: "server-1m" }, upd: Date.now() });
          ledger.set(r.k, merged); appendLedger(merged); ledgerDirty++; done++;
          continue;
        }
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
        const sroi = settleRoiPct(dir, actual, r.odds);   // $ "jika di-entry" dari HARGA TOKEN Binance nyata + outcome nyata
        const merged = Object.assign({}, r, {
          res: {
            lock: +lock, close: +close, actual, won: dir === actual ? 1 : 0,
            touch, tTouchSec: tTouch,
            mfeFav: isFinite(mfe) ? +mfe.toFixed(4) : null,
            maeFav: isFinite(mae) ? +mae.toFixed(4) : null,
            endFav: +v(close).toFixed(4), bars: path.length, src: "server-1m",
            settleRoi: sroi, settleSrc: sroi != null ? "binance-quote" : null,
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
/* $ "JIKA SESI DI-ENTRY" dari HARGA TOKEN prediksi Binance NYATA + outcome NYATA (bukan perkiraan).
   Token prediksi settle biner (menang=1, kalah=0). p = harga token sisi rekomendasi (saat t0, dari BOT).
   ROI% hold-to-settle = menang ? (1-p)/p*100 : -100. p berasal dari BOT (harga Binance nyata). */
function settleRoiPct(dir, actual, odds) {
  if (!odds || (dir !== "up" && dir !== "down")) return null;
  const p = dir === "up" ? odds.up : odds.down;
  if (typeof p !== "number" || !(p > 0) || !(p < 1)) return null;
  return dir === actual ? +(((1 - p) / p) * 100).toFixed(4) : -100;
}

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
  // ODDS harga token prediksi NYATA (dikirim BOT) — field TERPISAH dari `bot` (PnL) agar tidak saling timpa.
  if (r.odds) merged.odds = Object.assign({}, prev.odds, r.odds);
  merged.upd = Date.now();
  ledger.set(r.k, merged);
  appendLedger(merged);
  ledgerDirty++;
  // B18: adaptasi online — refit key ini (debounced) saat ada $ akun baru.
  if (r.bot && typeof r.bot.roiPct === "number") { try { scheduleOnlineRefit(r.k); } catch (_) {} }
  return true;
}

const LEARNER = require("./learner");
const { createCapture } = require("./capture");
const MODEL_DIR = path.join(LEDGER_DIR, "..", "models");
const MODEL_CUR = path.join(MODEL_DIR, "current");
const MODEL_LOG = path.join(MODEL_DIR, "promote.jsonl");
const DEFAULT_OUT = path.join(__dirname, "backtest", "out");
const EXP_GATE = require("./exp-gate.js");
const EXT = require("./ext-features.js");   // sumber data eksternal (Batch 1) — observasional
const MODEL_FILES = { gate: "learn_gate.json", touch: "learn_touch90.json", lessons: "lessons.json", gates: "gates.json", pnl: "learn_pnl.json", apply: "learn_apply.json", veto: "learn_veto.json", meta: "meta.json", flat: "learn_flat.json", exp: "exp.json", rolling: "learn_rolling.json", spread: "learn_spread.json", score: "learn_score.json", sizing: "learn_sizing.json", ta: "learn_ta.json" };
let gatesMeta = { mode: "perkey", promotedAt: null };
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
  // STRICT per-key: TIDAK ada profil global (BOOTSTRAP/STRICT) sebagai fallback.
  // Bila gates.json belum ada -> profil kosong; semua key ditolak ("no-key-profile") sampai
  // refit otomatis (<=1 jam) menulis gates.json per coin×TF.
  return { mode: "nokey", byKey: {}, note: "belum ada profil per-key — jalankan refit" };
}
function incumbentModel() {          // bentuk {metrics, gate:{rules}, touch:{rules}} untuk perbandingan
  const g = readModelPart("gate"), t = readModelPart("touch");
  if (!g || !g.metrics) return null;
  return { metrics: g.metrics, gate: { rules: g.rules || [] }, touch: { rules: (t && t.rules) || [] } };
}
let refitting = false;
async function refit(trigger = "manual", onlyKey = null) {   // onlyKey: refit HANYA 1 coin×TF
  if (refitting) return { ok: false, why: "re-fit sedang berjalan", busy: true };
  refitting = true;
  try {
    const records = [...ledger.values()];
    const rows = LEARNER.rowsFrom(records);
    ensureModelDirs();
    const ver = new Date().toISOString().replace(/[:.]/g, "-");
    const hist = path.join(MODEL_DIR, "v-" + ver);
    try { fs.mkdirSync(hist, { recursive: true }); } catch (_) {}
    const write = (name, obj) => {
      fs.writeFileSync(path.join(MODEL_CUR, name), JSON.stringify(obj, null, 1));
      try { fs.writeFileSync(path.join(hist, name), JSON.stringify(obj, null, 1)); } catch (_) {}
    };
    // ===== PER coin × TF (INDEPENDEN, tidak digeneralisir) =====
    const prevGate = readModelPart("gate") || {}, prevTouch = readModelPart("touch") || {};
    const prevGates = readGates() || {}, prevMeta = readModelPart("meta") || {};
    const prevApply = readModelPart("apply") || {}, prevLessons = readModelPart("lessons") || {}, prevPnl = readModelPart("pnl") || {};
    const allKeys = [...new Set(rows.map((r) => r.symbol + "_" + r.interval))].sort();
    // ANTI-SNOWBALL: HAPUS config learner utk key yg SUDAH TIDAK ADA di data terbaru.
    // Tanpa ini, aturan lama (suppress/threshold/blocker) menumpuk -> makin banyak sinyal diblok.
    const keySet = new Set(allKeys);
    const keepOnly = (m) => { for (const k of Object.keys(m)) if (!keySet.has(k)) delete m[k]; return m; };
    const gateMap = keepOnly(Object.assign({}, prevGate.byKey || {}));
    const touchMap = keepOnly(Object.assign({}, prevTouch.byKey || {}));
    const gatesMap = keepOnly(Object.assign({}, prevGates.byKey || {}));
    const applyMap = keepOnly(Object.assign({}, prevApply.byKey || {}));
    const lessonsMap = keepOnly(Object.assign({}, prevLessons.byKey || {}));
    const pnlMap = keepOnly(Object.assign({}, prevPnl.byKey || {}));
    const metaMap = keepOnly(Object.assign({}, prevMeta.byKey || {}));
    // ===== IMPROVEMENT LANJUTAN (B): peta model baru per key =====
    const prevSpread = readModelPart("spread") || {};
    const spreadMap = keepOnly(Object.assign({}, prevSpread.byKey || {}));
    const prevScore = readModelPart("score") || {};
    const scoreMap = keepOnly(Object.assign({}, prevScore.byKey || {}));
    const prevSizing = readModelPart("sizing") || {};
    const sizingMap = keepOnly(Object.assign({}, prevSizing.byKey || {}));
    const prevTa = readModelPart("ta") || {};
    const taMap = keepOnly(Object.assign({}, prevTa.byKey || {}));
    const keys = onlyKey ? [onlyKey] : allKeys;   // per-key trigger -> proses key itu saja
    const minApplyCov = Number(process.env.MIN_APPLY_COV != null ? process.env.MIN_APPLY_COV : 0.15);
    const keyRes = {}; let anyPromote = false, anyGates = false;
    const nowRef = Math.floor(Date.now() / 1000);
    const learnWindow = Number(process.env.LEARN_WINDOW_SEC || 0);   // 0 = seluruh histori; >0 = jendela regime (mis. 86400 = 24 jam)
    // Metrik jendela bergulir (regime) berbasis $ — dipakai kill-switch per key & panel.
    const rolling = LEARNER.rollingStats(rows, { now: nowRef, windows: [3 * 3600, 6 * 3600, 12 * 3600, 24 * 3600] });
    for (const key of keys) {
      const kr = rows.filter((r) => r.symbol + "_" + r.interval === key);
      const tfNow = key.slice(key.indexOf("_") + 1);
      const poolRows = rows.filter((r) => r.interval === tfNow);   // B16: pool antar-coin utk TF yg sama
      const cand = LEARNER.buildModel(kr, {
        minTrain: Number(process.env.LEARN_MIN_TRAIN || 12), minTest: Number(process.env.LEARN_MIN_TEST || 6), minRows: Number(process.env.LEARN_MIN_ROWS || 15),
        pnlMinN: Number(process.env.LEARN_PNL_MIN_N || 8), pnlMinDelta: Number(process.env.LEARN_PNL_MIN_DELTA || 3),
        windowSec: learnWindow, now: nowRef, poolRows,
      });
      if (!cand.ok) {
        // data tak cukup -> JANGAN simpan config lama (snowball). Hapus agar key bebas dari blocker usang.
        keyRes[key] = { n: kr.length, ok: false, why: cand.reason };
        delete gateMap[key]; delete touchMap[key]; delete applyMap[key]; delete metaMap[key]; delete gatesMap[key];
        delete pnlMap[key]; delete lessonsMap[key];   // cegah data $/$pelajaran BASI (proxy lama) tampil di panel
        delete spreadMap[key]; delete scoreMap[key]; delete taMap[key];
        continue;
      }
      // PENTING: jendela uji HARUS sejajar dengan split model (baris ber-$ saja). Bila model di-POOL,
      // uji juga memakai himpunan pool agar selaras dgn splitIdx model.
      const krPnl = kr.filter((r) => r.dwin != null);
      const poolPnl = poolRows.filter((r) => r.dwin != null);
      const srcPnl = (krPnl.length >= Number(process.env.LEARN_MIN_ROWS || 15)) ? krPnl : poolPnl;
      const splitIdxNow = (cand.splitIdx != null) ? cand.splitIdx : Math.floor(srcPnl.length * 0.7);
      const testNow = srcPnl.slice(splitIdxNow);
      const inc = gateMap[key] ? { gate: { rules: gateMap[key].rules || [] }, touch: { rules: (touchMap[key] && touchMap[key].rules) || [] }, metrics: (metaMap[key] && metaMap[key].metrics) || null } : null;
      const candEval = LEARNER.evalModelRolling(testNow, cand.gate.rules, cand.touch.rules, 3);
      const incEval = inc ? LEARNER.evalModelRolling(testNow, inc.gate.rules, inc.touch.rules, 3) : null;
      const candPnl = LEARNER.evalModelPnl(testNow, cand.gate.rules, cand.touch.rules);
      const incPnl = inc ? LEARNER.evalModelPnl(testNow, inc.gate.rules, inc.touch.rules) : null;
      const baselinePnl = LEARNER.evalModelPnl(testNow, [], []);   // take-all = pembanding nyata (bukan insiden $-identik)
      // Blocker EFEKTIF (single-feature APPLY_KEYS) — bukan sekadar jumlah suppress mentah.
      const candBlockersEff = LEARNER.blockersOf(cand.gate.rules, "dwin").length + LEARNER.blockersOf(cand.touch.rules, "dwin").length;
      const dec = LEARNER.shouldPromote(
        { metrics: Object.assign({}, candEval, { pnl: candPnl }) },
        incEval ? { metrics: Object.assign({}, incEval, { pnl: incPnl }) } : null,
        { minCov: Number(process.env.LEARN_MIN_COV || 0.10), minTake: Number(process.env.LEARN_MIN_TAKE || 6), minDelta: Number(process.env.LEARN_MIN_DELTA || 2), baselinePnl, hasBlockers: candBlockersEff > 0 });
      const liveEval = dec.promote ? candEval : (incEval || candEval);
      // FIX inkonsistensi: apply HANYA bermakna bila memang ADA aturan blocker efektif (gate/touch).
      const gateRules = (cand.gate && cand.gate.suppress) || [];
      const touchRules = (cand.touch && cand.touch.suppress) || [];
      const hasBlockers = candBlockersEff > 0;
      // PENTING: rule live dibaca capture dari learn_gate.json per key TANPA menunggu "promote".
      // Karena itu penerapan digerbangi oleh: tidak merusak $ vs baseline take-all (cegah model yg OOS-nya lebih buruk).
      const improvesBaseline = !!(candPnl && baselinePnl && candPnl.n >= 6 && candPnl.meanPnl >= baselinePnl.meanPnl);
      const applyBlockers = hasBlockers && improvesBaseline && !!liveEval && (liveEval.coverage || 1) >= minApplyCov;
      applyMap[key] = { apply: applyBlockers, hasBlockers, improvesBaseline, nRules: gateRules.length + touchRules.length, coverage: liveEval ? liveEval.coverage : null, minApplyCov,
        candPnl: candPnl ? candPnl.meanPnl : null, basePnl: baselinePnl ? baselinePnl.meanPnl : null, candN: candPnl ? candPnl.n : null,
        note: !hasBlockers ? "tak ada aturan blocker (model ambil-semua) -> blocker TIDAK diterapkan"
          : !improvesBaseline ? ((candPnl && candPnl.n < 6)
            ? `sampel $ model terlalu kecil (n ${candPnl.n} < 6) -> blocker belum diterapkan (bukti belum cukup)`
            : `$ model ${candPnl ? candPnl.meanPnl : "-"}% < baseline ${baselinePnl ? baselinePnl.meanPnl : "-"}% -> blocker TIDAK diterapkan (cegah perburukan)`)
          : applyBlockers ? "blocker diterapkan"
          : `cakupan ${(100 * (liveEval ? liveEval.coverage : 0)).toFixed(0)}% < ${(minApplyCov * 100).toFixed(0)}% -> blocker TIDAK diterapkan` };
      const th = LEARNER.learnThresholds(kr);
      const gatesPromoted = !!(th.ok && th.beatsBaseline);
      // TIER LADDER: utamakan tier MILIK key; bila key kekurangan data -> fallback tier POOL TF-level
      // (semua coin TF sama digabung). Per-key tetap prioritas; pool hanya warm-start (tanpa ambang global).
      const ktOwn = LEARNER.keyTiers(kr, { minN: Number(process.env.TIER_MIN_N || 8) });
      let gt = (ktOwn.keys[key] && Object.keys(ktOwn.keys[key]).length) ? ktOwn.keys[key] : null;
      let tiersPooled = false;
      if (!gt) {
        const poolTiersRows = poolRows.map((r) => (r.symbol === "POOL" ? r : Object.assign({}, r, { symbol: "POOL" })));
        const ktP = LEARNER.keyTiers(poolTiersRows, { minN: Number(process.env.TIER_MIN_N || 8) });
        const _ktKey = `POOL_${tfNow}`;
        if (ktP.keys[_ktKey] && Object.keys(ktP.keys[_ktKey]).length) { gt = ktP.keys[_ktKey]; tiersPooled = true; }
      }
      // GANTI SELALU (jangan merge config lama) -> tak ada threshold usang yg menahan sinyal.
      const gEntry = {
        mode: "perkey", liqFloorMul: Number(process.env.LIQ_FLOOR_MUL != null ? process.env.LIQ_FLOOR_MUL : 0.12),
        lateFrac: Number(process.env.LATE_FRAC != null ? process.env.LATE_FRAC : 0.85),
        generated: new Date().toISOString(), version: ver, rows: kr.length,
      };
      if (gt) gEntry.tiers = gt;
      if (tiersPooled) gEntry.tiersPooled = true;
      if (gatesPromoted) { gEntry.thresholds = th.thresholds; gEntry.thMetrics = th.test; gEntry.train = th.train; gEntry.baselineTest = th.baselineTest; anyGates = true; }
      gatesMap[key] = gEntry;
      pnlMap[key] = Object.assign({ test: cand.pnlTest }, cand.pnl);
      // ===== IMPROVEMENT LANJUTAN (B) per key =====
      spreadMap[key] = LEARNER.mineSpread(kr);            // B12 batas spread (per key)
      scoreMap[key] = LEARNER.mineScore(kr);              // B17 ambang skor selektif
      sizingMap[key] = LEARNER.mineSizing(rolling, key);  // B19 stake mult dari edge $
      taMap[key] = LEARNER.learnTA(kr);                   // B14 tuning exit TA (replay path akun)
      // LESSONS ditulis SELALU (informatif), terlepas dari promote. Dulu hanya saat `dec.promote` true ->
      // karena tak ada key yang promote, panel "pelajaran" selalu kosong. Lessons = insight konteks,
      // TIDAK bergantung adopsi model.
      if (cand && cand.lessons) lessonsMap[key] = cand.lessons;
      // GANTI SELALU dengan hasil learner TERBARU (anti-snowball: config lama JANGAN disimpan).
      // `dec.promote` tetap dicatat untuk audit (apakah kandidat lebih baik dari insiden).
      gateMap[key] = cand.gate; touchMap[key] = cand.touch;
      metaMap[key] = { version: ver, promotedAt: new Date().toISOString(), n: kr.length, rows: cand.rows, metrics: candEval, why: dec.why, promoted: !!dec.promote };
      if (dec.promote) anyPromote = true;
      keyRes[key] = { n: kr.length, promote: dec.promote, why: dec.why, coverage: liveEval ? liveEval.coverage : null, gatesPromoted,
        blockers: candBlockersEff, candPnl: candPnl ? candPnl.meanPnl : null, basePnl: baselinePnl ? baselinePnl.meanPnl : null, baseN: baselinePnl ? baselinePnl.n : null };
    }
    write("learn_gate.json", { generated: new Date().toISOString(), source: "ledger", version: ver, byKey: gateMap });
    write("learn_touch90.json", { generated: new Date().toISOString(), source: "ledger", version: ver, byKey: touchMap });
    write("gates.json", { generated: new Date().toISOString(), version: ver, mode: "perkey", byKey: gatesMap });
    write("learn_apply.json", { generated: new Date().toISOString(), version: ver, byKey: applyMap });
    write("lessons.json", { generated: new Date().toISOString(), version: ver, byKey: lessonsMap });
    write("learn_pnl.json", { generated: new Date().toISOString(), version: ver, byKey: pnlMap });
    // ===== METRIK JENDELA BERGULIR (regime, berbasis $) =====
    // per key: mean-$/WR pada 3h/6h/12h/24h terakhir. Dipakai kill-switch & panel.
    write("learn_rolling.json", Object.assign({ generated: new Date().toISOString(), version: ver, learnWindow }, rolling));
    // ===== MODEL LANJUTAN (B) =====
    write("learn_spread.json", { generated: new Date().toISOString(), version: ver, byKey: spreadMap });
    write("learn_score.json", { generated: new Date().toISOString(), version: ver, byKey: scoreMap });
    write("learn_sizing.json", { generated: new Date().toISOString(), version: ver, byKey: sizingMap });
    write("learn_ta.json", { generated: new Date().toISOString(), version: ver, byKey: taMap });
    // Terapkan tuning exit TA PER KEY ke modul trade-plan (dibaca live oleh engine).
    try {
      const PER = {};
      for (const k of Object.keys(taMap)) { const b = taMap[k] && taMap[k].best; if (b && b.cb > 0) PER[k] = { TRAIL_CB_PCT: b.cb }; }
      setTaPerKey(PER);
    } catch (_) {}
    write("meta.json", { version: ver, promotedAt: new Date().toISOString(), trigger, byKey: metaMap });
    // ===== GATE EKSPERIMENTAL (OBSERVASIONAL) — tidak mengubah accepted/reject produksi =====
    try { write("exp.json", EXP_GATE.evaluate(records)); } catch (e) { console.log("[EXP] gagal evaluasi:", e.message); }
    try {
      const hv = LEARNER.hourVetoes(rows, { minN: Number(process.env.VETO_MIN_N || 25), thr: Number(process.env.VETO_WR_THR || 0.50), recentN: Number(process.env.VETO_RECENT_N || 3), recentWin: Number(process.env.VETO_RECENT_WIN || 2), offCap: Number(process.env.VETO_HOUR_OFF_CAP || 0.5), pnlBad: Number(process.env.VETO_PNL_BAD != null ? process.env.VETO_PNL_BAD : -2) });
      const kv = LEARNER.keyVetoes(rows, { thr: Number(process.env.VETO_WR_THR || 0.50), minN: Number(process.env.VETO_MIN_N || 25), covCap: Number(process.env.VETO_COV_CAP || 0.6), minAllowedN: Number(process.env.VETO_MIN_ALLOWED_N || 0), minAllowedCov: Number(process.env.VETO_MIN_ALLOWED_COV || 0.3), reclaimMinN: Number(process.env.VETO_RECLAIM_MIN_N || 30), reclaimWlb: Number(process.env.VETO_RECLAIM_WLB || 0.52), reclaimMax: Number(process.env.VETO_RECLAIM_MAX || 4), reclaimCovCap: Number(process.env.VETO_RECLAIM_COV || 0.4), pnlBad: Number(process.env.VETO_PNL_BAD != null ? process.env.VETO_PNL_BAD : -2), invertMinN: Number(process.env.INVERT_MIN_N || 25), invertWlb: Number(process.env.INVERT_WLB || 0.52), invertMax: Number(process.env.INVERT_MAX || 3), invertCovCap: Number(process.env.INVERT_COV || 0.4), confirmMinN: Number(process.env.CONFIRM_MIN_N || 40), confirmWlb: Number(process.env.CONFIRM_WLB || 0.52), confirmMax: Number(process.env.CONFIRM_MAX || 3), confirmCovCap: Number(process.env.CONFIRM_COV || 0.6), keyEvGate: process.env.KEY_EV_CRUDE === "1", keyEvN: Number(process.env.KEY_EV_N || 20), keyEvMin: Number(process.env.KEY_EV_MIN || 0), keyEvWin: Number(process.env.KEY_EV_WIN || 40), flatEvN: Number(process.env.FLAT_EV_N || 20) });
      // ===== KILL-SWITCH REGIME per key (berbasis $ jendela bergulir) =====
      // Pause key bila $ 6h DAN konfirmasi 12h sama-sama negatif (histeresis -> hindari whipsaw).
      // Data: pada 30m $ = noise (persist 0.50); persistensi baru terukur di 3-6 jam -> jendela minimum 3h.
      if (process.env.KEY_EV_GATE === "1") {
        const rpOpts = { fast: Number(process.env.REGIME_FAST_SEC || 21600), confirm: Number(process.env.REGIME_CONFIRM_SEC || 43200), minN: Number(process.env.REGIME_MIN_N || 8), thrPct: Number(process.env.REGIME_THR_PCT || 0) };
        for (const k of Object.keys((kv && kv.keys) || {})) {
          const rp = LEARNER.regimePause(rolling, k, rpOpts);
          kv.keys[k].regime = rp;
          if (rp.pause) kv.keys[k].keyEvGated = true;
        }
      }
      write("learn_veto.json", Object.assign({ generated: new Date().toISOString(), trigger: "refit", version: ver }, hv, { prof: kv }));
    } catch (_) {}
    // ===== ANALISIS KONTEKS FLAT per coin×TF (dari record flat informasional) =====
    try { const fstat = LEARNER.flatStats(records, { minN: Number(process.env.FLAT_MIN_N || 10) }); write("learn_flat.json", Object.assign({ generated: new Date().toISOString(), version: ver }, fstat)); res.flat = Object.keys(fstat.keys).map((k) => ({ k, n: fstat.keys[k].n, wr: fstat.keys[k].majorityWR, lb: fstat.keys[k].wilsonLB, edge: fstat.keys[k].edge })); } catch (_) {}
    const res = { trigger, at: new Date().toISOString(), rows: rows.length, ok: true, perKey: true, keys: keyRes, promote: anyPromote, gatesPromoted: anyGates,
      why: Object.keys(keyRes).map((k) => `${k}:${keyRes[k].ok === false ? "data-kurang" : (keyRes[k].promote ? "PROMOTE" : "keep")}`).join(" · ") };
    try { fs.appendFileSync(MODEL_LOG, JSON.stringify(res) + "\n"); } catch (_) {}
    console.log(`[REFIT] per-key · rows ${rows.length} · ${res.why}`);
    return res;
  } finally { refitting = false; }
}
// ===== B18: ADAPTASI ONLINE — refit PER-KEY (debounced) begitu BOT melaporkan $ akun baru =====
// Kill-switch ONLINE_REFIT. Debounce ONLINE_REFIT_MS agar refit tidak beruntun; tetap per key (tanpa global).
const _onlineRefit = {};
function keyFromRec(k) { const m = /^([A-Za-z0-9]+)_([0-9a-z]+)_\d+$/.exec(String(k || "")); return m ? `${m[1]}_${m[2]}` : null; }
function scheduleOnlineRefit(k) {
  if (process.env.ONLINE_REFIT !== "1") return;
  const key = keyFromRec(k); if (!key) return;
  if (_onlineRefit[key]) return;
  const run = () => {
    _onlineRefit[key] = null;
    try {
      // Bila refit lain sedang berjalan (busy), JADWALKAN ULANG (jangan sampai hilang).
      Promise.resolve(refit("online", key)).then((r) => { if (r && r.busy) scheduleOnlineRefit(k); }).catch(() => {});
    } catch (_) {}
  };
  _onlineRefit[key] = setTimeout(run, Number(process.env.ONLINE_REFIT_MS || 20000));
}
ensureModelDirs(); loadModelMeta();
// Terapkan tuning exit TA PER KEY ke modul trade-plan (dibaca live engine) + refresh VER.
function setTaPerKey(PER) {
  try {
    const ta = require("./ta-config.js");
    ta.PER_KEY = PER || {};
    ta.VER = "ta" + require("crypto").createHash("md5").update(JSON.stringify(ta)).digest("hex").slice(0, 8);
  } catch (_) {}
}
try {
  const taM = readModelPart("ta") || {};
  const PER = {};
  for (const k of Object.keys(taM.byKey || {})) { const b = taM.byKey[k] && taM.byKey[k].best; if (b && b.cb > 0) PER[k] = { TRAIL_CB_PCT: b.cb }; }
  setTaPerKey(PER);
  if (Object.keys(PER).length) console.log(`[TA] per-key exit tuning dimuat: ${Object.keys(PER).join(", ")}`);
} catch (_) {}

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
// Jadwal: re-fit otomatis tiap REFIT_INTERVAL_HOURS (default 1 jam) — adaptif (<=3 jam).
const REFIT_INTERVAL_H = Math.max(1, parseFloat(process.env.REFIT_INTERVAL_HOURS || "1"));   // refit tiap 1 jam (default)
const REFIT_CHECK_MIN = Math.max(1, parseInt(process.env.REFIT_CHECK_MIN || "5", 10));
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
  const hv = LEARNER.hourVetoes(rows, {
    minN: Number(process.env.VETO_MIN_N || 25), thr: Number(process.env.VETO_WR_THR || 0.50),
    recentN: Number(process.env.VETO_RECENT_N || 3), recentWin: Number(process.env.VETO_RECENT_WIN || 2),
    offCap: Number(process.env.VETO_HOUR_OFF_CAP || 0.5),
    pnlBad: Number(process.env.VETO_PNL_BAD != null ? process.env.VETO_PNL_BAD : -2),
  });
  // VETO THRESHOLD per key (reward/rsi/vol/liq) — dari data per coin×TF (bukan global).
  const kv = LEARNER.keyVetoes(rows, { thr: Number(process.env.VETO_WR_THR || 0.50), minN: Number(process.env.VETO_MIN_N || 25), covCap: Number(process.env.VETO_COV_CAP || 0.6), minAllowedN: Number(process.env.VETO_MIN_ALLOWED_N || 0), minAllowedCov: Number(process.env.VETO_MIN_ALLOWED_COV || 0.3), reclaimMinN: Number(process.env.VETO_RECLAIM_MIN_N || 30), reclaimWlb: Number(process.env.VETO_RECLAIM_WLB || 0.52), reclaimMax: Number(process.env.VETO_RECLAIM_MAX || 4), reclaimCovCap: Number(process.env.VETO_RECLAIM_COV || 0.4), pnlBad: Number(process.env.VETO_PNL_BAD != null ? process.env.VETO_PNL_BAD : -2), invertMinN: Number(process.env.INVERT_MIN_N || 25), invertWlb: Number(process.env.INVERT_WLB || 0.52), invertMax: Number(process.env.INVERT_MAX || 3), invertCovCap: Number(process.env.INVERT_COV || 0.4), confirmMinN: Number(process.env.CONFIRM_MIN_N || 40), confirmWlb: Number(process.env.CONFIRM_WLB || 0.52), confirmMax: Number(process.env.CONFIRM_MAX || 3), confirmCovCap: Number(process.env.CONFIRM_COV || 0.6), keyEvGate: process.env.KEY_EV_CRUDE === "1", keyEvN: Number(process.env.KEY_EV_N || 20), keyEvMin: Number(process.env.KEY_EV_MIN || 0), keyEvWin: Number(process.env.KEY_EV_WIN || 40), flatEvN: Number(process.env.FLAT_EV_N || 20) });
  // Kill-switch REGIME (jendela $ 6h & 12h negatif) — dipasang di sini agar tidak terhapus refreshVeto 2-menit.
  if (process.env.KEY_EV_GATE === "1") {
    try {
      const rolling = LEARNER.rollingStats(rows, { now: Math.floor(Date.now() / 1000), windows: [3 * 3600, 6 * 3600, 12 * 3600, 24 * 3600] });
      const rpOpts = { fast: Number(process.env.REGIME_FAST_SEC || 21600), confirm: Number(process.env.REGIME_CONFIRM_SEC || 43200), minN: Number(process.env.REGIME_MIN_N || 8), thrPct: Number(process.env.REGIME_THR_PCT || 0) };
      for (const k of Object.keys((kv && kv.keys) || {})) {
        const rp = LEARNER.regimePause(rolling, k, rpOpts);
        kv.keys[k].regime = rp;
        if (rp.pause) kv.keys[k].keyEvGated = true;
      }
    } catch (_) {}
  }
  return Object.assign({}, hv, { prof: kv });
}
function refreshVeto(tag) {
  try {
    const hv = computeVetoNow();                              // {keys:{key:{hours,stats}}, prof:{key:{rewardMin,...}}, ...}
    const key = JSON.stringify([hv.keys, hv.prof && hv.prof.keys]);
    if (key !== lastVetoKey) {
      lastVetoKey = key;
      ensureModelDirs();
      fs.writeFileSync(path.join(MODEL_CUR, "learn_veto.json"), JSON.stringify(Object.assign({ generated: new Date().toISOString(), trigger: tag }, hv), null, 1));
      const summary = Object.keys(hv.keys).map((k) => `${k}:[${(hv.keys[k].hours || []).join(",")}]`).join(" ");
      console.log(`[VETO] jam OFF per coin/TF (${tag}): ${summary}`);
    }
  } catch (e) { console.log(`[VETO] error: ${e && e.message}`); }
}
setTimeout(() => refreshVeto("awal"), 15000);
setInterval(() => refreshVeto("periodik"), VETO_CHECK_MIN * 60 * 1000);
// Sumber data EKSTERNAL (Batch 1): refresh berkala ke cache (dipakai recorder sig.ext + /api/ext-probe).
const EXT_REFRESH_MS = Number(process.env.EXT_REFRESH_MS || 20000);
setTimeout(() => { EXT.refreshAll().then(() => console.log(`[EXT] refresh awal ${EXT.probe().lastMs}ms`)).catch(() => {}); }, 8000);
setInterval(() => { EXT.refreshAll().catch(() => {}); }, EXT_REFRESH_MS);
console.log(`[EXT] refresh data eksternal tiap ${EXT_REFRESH_MS / 1000}s`);
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
  }, 3000);   // cadence santai (cache snapshot.js menahan beban; cegah 418)
}

/* ===== PROXY REST + RELAY WS BINANCE =====
   Agar KLIEN tidak pernah memanggil Binance langsung — semua data pasar lewat SERVER (100% server-based).
   - /api/v3/*      : proxy transparan REST Binance (time/klines/depth/ticker) dgn fallback host.
   - /api/market-stream : relay WS Binance (kline/ticker) -> SSE; satu upstream dibagi ke semua klien. */
const BN_REST_HOSTS = ["https://data-api.binance.vision", "https://api.binance.com", "https://api1.binance.com"];
// Cache + koalesensi + backoff 418/429 (proxy ini dipakai klien DAN fallback relay -> jangan boros).
const _pxCache = new Map(), _pxInflight = new Map();
let _pxBanUntil = 0;
function _pxTtl(p) { return p.includes("klines") ? 3000 : p.includes("ticker") ? 5000 : p.includes("depth") ? 3000 : p.includes("time") ? 2000 : 2000; }
async function proxyBinanceV3(pathname, search) {
  const key = pathname + (search || "");
  const now = Date.now();
  const c = _pxCache.get(key);
  if (c && now - c.t < _pxTtl(key)) return c.r;
  if (now < _pxBanUntil) return c ? c.r : { status: 429, body: JSON.stringify({ error: "binance cooldown (418/429)" }), ctype: "application/json" };
  if (_pxInflight.has(key)) return _pxInflight.get(key);
  const p = (async () => {
    let lastErr = null;
    for (const h of BN_REST_HOSTS) {
      try {
        const r = await fetch(h + key, { cache: "no-store", signal: AbortSignal.timeout(8000) });
        const body = await r.text();
        if (r.status === 418 || r.status === 429) { const ra = Number(r.headers.get("retry-after") || 0); _pxBanUntil = Date.now() + Math.max(60000, ra * 1000); }
        const res = { status: r.status, body, ctype: r.headers.get("content-type") || "application/json" };
        if (r.ok) _pxCache.set(key, { t: Date.now(), r: res });
        return res;
      } catch (e) { lastErr = e; }
    }
    return c ? c.r : { status: 502, body: JSON.stringify({ error: "proxy gagal: " + String(lastErr) }), ctype: "application/json" };
  })();
  _pxInflight.set(key, p);
  try { return await p; } finally { _pxInflight.delete(key); }
}
const BN_WS_HOSTS = ["wss://stream.binance.com:9443", "wss://data-stream.binance.vision"];
let _mstreamClients = new Set(), _mstreamWs = null, _mstreamIdx = 0, _mstreamGotData = false, _mstreamPoll = null, _mstreamPollN = 0;
function _mstreamUrl() {
  const streams = [];
  for (const s of ["btcusdt", "ethusdt", "bnbusdt"]) {
    streams.push(`${s}@kline_1s`);
    for (const tf of ["5m", "15m", "1h"]) streams.push(`${s}@kline_${tf}`);
    streams.push(`${s}@ticker`);
  }
  return `${BN_WS_HOSTS[_mstreamIdx % BN_WS_HOSTS.length]}/stream?streams=${streams.join("/")}`;
}
function _mstreamEmit(obj) {
  const payload = `data: ${JSON.stringify(obj)}\n\n`;
  for (const c of _mstreamClients) { try { c.write(payload); } catch (_) {} }
}
// FALLBACK: bila WS Binance diblok (umum di Railway), poll REST (/api/v3) & emit pesan ala Binance
// (klien tak perlu tahu sumbernya). Berhenti otomatis begitu WS mulai mengirim data.
function _mstreamPollEmit() {
  if (_mstreamPoll) return;
  _mstreamPoll = setInterval(async () => {
    if ((_mstreamWs && _mstreamGotData) || _mstreamClients.size === 0) { clearInterval(_mstreamPoll); _mstreamPoll = null; return; }
    try {
      _mstreamPollN++;
      const alsoTf = (_mstreamPollN % 12 === 0);   // tiap ~30s, segarkan seri 5m/15m/1h
      for (const symBin of ["BTCUSDT", "ETHUSDT", "BNBUSDT"]) {
        const low = symBin.toLowerCase();
        let rows = [];
        try { const k = await proxyBinanceV3("/api/v3/klines", `?symbol=${symBin}&interval=1s&limit=2`); rows = JSON.parse(k.body); } catch (_) {}
        const r = Array.isArray(rows) ? rows[rows.length - 1] : null;
        if (r) _mstreamEmit({ stream: `${low}@kline_1s`, data: { e: "kline", E: Date.now(), s: symBin, k: { t: r[0] / 1000, T: r[6] / 1000, s: symBin, i: "1s", o: r[1], c: r[4], h: r[2], l: r[3], v: r[5], x: true } } });
        if (alsoTf) {
          for (const tf of ["5m", "15m", "1h"]) {
            try {
              const kk = await proxyBinanceV3("/api/v3/klines", `?symbol=${symBin}&interval=${tf}&limit=2`);
              const rr = JSON.parse(kk.body); const c = Array.isArray(rr) ? rr[rr.length - 1] : null;
              if (c) _mstreamEmit({ stream: `${low}@kline_${tf}`, data: { e: "kline", E: Date.now(), s: symBin, k: { t: c[0] / 1000, T: c[6] / 1000, s: symBin, i: tf, o: c[1], c: c[4], h: c[2], l: c[3], v: c[5], x: false } } });
            } catch (_) {}
          }
        }
        if (_mstreamPollN % 2 === 0) {   // ticker tiap ~5s (lebih ringan)
          try {
            const t = await proxyBinanceV3("/api/v3/ticker/24hr", `?symbol=${symBin}`);
            const tj = JSON.parse(t.body);
            if (tj && tj.lastPrice != null) _mstreamEmit({ stream: `${low}@ticker`, data: { e: "24hrTicker", s: symBin, c: tj.lastPrice, P: tj.priceChangePercent } });
          } catch (_) {}
        }
      }
    } catch (_) {}
  }, 2500);
}
function _startMstream() {
  if (_mstreamWs || typeof WebSocket === "undefined") { if (typeof WebSocket === "undefined") _mstreamPollEmit(); return; }
  try { _mstreamWs = new WebSocket(_mstreamUrl()); } catch (_) { _mstreamIdx++; _mstreamPollEmit(); setTimeout(_startMstream, 2000); return; }
  _mstreamGotData = false;
  _mstreamWs.onopen = () => { setTimeout(() => { if (!_mstreamGotData) _mstreamPollEmit(); }, 4000); };
  _mstreamWs.onmessage = (ev) => { _mstreamGotData = true; for (const c of _mstreamClients) { try { c.write(`data: ${ev.data}\n\n`); } catch (_) {} } };
  _mstreamWs.onclose = () => { _mstreamWs = null; if (_mstreamClients.size) { _mstreamPollEmit(); setTimeout(_startMstream, 3000); } };
  _mstreamWs.onerror = () => { try { _mstreamWs.close(); } catch (_) {} };
}

// Jam ON/OFF utk panel UI — diambil dari hasil learner terbaru (learn_veto.json), BUKAN hardcode.
function tradeHoursNow() {
  const v = readModelPart("veto");
  if (!v || !v.keys) return null;
  const keys = {};
  for (const key of Object.keys(v.keys)) {
    const off = (v.keys[key].hours || []).slice().sort((a, b) => a - b);
    const on = []; let s = null;
    for (let h = 0; h < 24; h++) {
      const isOff = off.indexOf(h) >= 0;
      if (!isOff && s === null) s = h;
      if ((isOff || h === 23) && s !== null) { on.push([s, isOff ? h : 24]); s = null; }
    }
    keys[key] = { off, on };
  }
  return { tz: "WIB", keys, updatedAt: v.generated || null, trigger: v.trigger || null, thr: v.thr, minN: v.minN, src: "learner" };
}

http.createServer(async (req, res) => {
  const u = new URL(req.url, "http://x");

  if (u.pathname === "/api/snapshot") {
    try {
      const snap = await getSnapshot(u.searchParams.get("history") === "1");
      snap.tradeHours = tradeHoursNow();
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
      snap.tradeHours = tradeHoursNow();
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

  // PROXY REST BINANCE — klien memakai /api/v3/* (same-origin), bukan memanggil Binance langsung.
  if (u.pathname.startsWith("/api/v3/")) {
    const pr = await proxyBinanceV3(u.pathname, u.search);
    res.writeHead(pr.status, { "Content-Type": pr.ctype, "Access-Control-Allow-Origin": "*", "Cache-Control": "no-store" });
    res.end(pr.body);
    return;
  }
  // RELAY WS BINANCE -> SSE — klien tidak membuka WS ke Binance.
  if (u.pathname === "/api/market-stream") {
    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      "Connection": "keep-alive",
      "Access-Control-Allow-Origin": "*",
      "X-Accel-Buffering": "no",
    });
    res.write("retry: 2000\n\n");
    _mstreamClients.add(res);
    _startMstream();
    setTimeout(() => { if (!_mstreamGotData) _mstreamPollEmit(); }, 4500);   // fallback REST bila WS diblok/silent
    const ka = setInterval(() => { try { res.write(": ka\n\n"); } catch (_) {} }, 20000);
    const stop = () => { clearInterval(ka); _mstreamClients.delete(res); };
    req.on("close", stop); req.on("error", stop); res.on("close", stop);
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
    const send = () => { try { const s = engine.snapshot(tf); s.tradeHours = tradeHoursNow(); res.write(`data: ${JSON.stringify(s)}\n\n`); } catch (_) {} };
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
  // Gate eksperimental (observasional): perbandingan cohort vs gate produksi. Diperbarui tiap refit.
  if (u.pathname === "/api/exp") {
    res.writeHead(200, Object.assign({ "Content-Type": "application/json" }, CORS));
    res.end(JSON.stringify(readModelPart("exp") || { note: "belum ada; jalankan /api/model/refit" }));
    return;
  }
  // Sumber data EKSTERNAL (Batch 1): status reachability + nilai terkini (untuk uji kelayakan & lift).
  if (u.pathname === "/api/ext-probe") {
    res.writeHead(200, Object.assign({ "Content-Type": "application/json" }, CORS));
    res.end(JSON.stringify(EXT.probe()));
    return;
  }
  // DEBUG kalibrasi endpoint OKX (respons mentah) — dipakai untuk memperbaiki takerLS/lsrTop.
  if (u.pathname === "/api/ext-debug") {
    EXT.debugOkx(u.searchParams.get("ccy") || "BTC").then((r) => {
      res.writeHead(200, Object.assign({ "Content-Type": "application/json" }, CORS));
      res.end(JSON.stringify(r, null, 1));
    }).catch((e) => { res.writeHead(500, CORS); res.end(JSON.stringify({ error: String(e) })); });
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
    } else if (u.searchParams.get("n")) {
      // ENDPOINT RINGAN: N record terakhir, DITRIM ke field yg dipakai panel akurasi saja.
      // (record penuh ~1,7KB berisi micro/ind/learn → 500 record = 0,8MB; trim → ~0,12MB.)
      const n = Math.max(1, Math.min(3000, Number(u.searchParams.get("n")) || 600));
      // PENTING: ambil N record ber-ARAH (sig.dir up/down) terakhir — BUKAN N record terakhir apa saja.
      // Di pasar sepi, ratusan record terakhir = flat/noise (dir null) → panel akurasi jadi kehilangan
      // sesi ber-sinyal (data lama seperti hilang). Filter dir dulu, baru slice N terakhir.
      // Kirim N sesi yang PUNYA SINYAL ARAH & BUKAN ditolak (accepted !== false) — persis yg ditampilkan
      // panel akurasi. Rejected/flat/lebih-lama tak dipakai panel, jadi jangan dihitung ke kuota N.
      const arr = [...ledger.values()].filter((r) => r.sig
        && (r.sig.dir === "up" || r.sig.dir === "down" || r.sig.verdict === "up" || r.sig.verdict === "down")
        && r.sig.accepted !== false);
      const trim = (r) => ({ k: r.k, t0: r.t0, asset: r.asset, interval: r.interval,
        sig: r.sig ? { verdict: r.sig.verdict, dir: r.sig.dir, accepted: r.sig.accepted, grade: r.sig.grade, flatEntry: r.sig.flatEntry || null } : null,
        res: r.res ? { won: r.res.won, actual: r.res.actual, lock: r.res.lock, close: r.res.close, trade: r.res.trade } : null });
      res.end(JSON.stringify({ stats: st, records: arr.slice(-n).map(trim) }));
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
      refit("manual", u.searchParams.get("key") || null).then((r) => {
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
        byKey: (readModelPart("meta") || {}).byKey || {},   // METRIK/why PER coin×TF
      },
      blockers: (() => {
        const A = LEARNER.APPLY_KEYS;
        const pick = (list) => (list || []).filter((k) => typeof k === "string" && k.indexOf("&") === -1 && k.indexOf("=") > 0 && A.has(k.slice(0, k.indexOf("="))));
        const gM = readModelPart("gate") || {}, tM = readModelPart("touch") || {};
        const byKey = {};
        for (const k of Object.keys(gM.byKey || {})) byKey[k] = { gate: pick(gM.byKey[k] && gM.byKey[k].suppress), touch: pick(tM.byKey && tM.byKey[k] && tM.byKey[k].suppress) };
        return { byKey };
      })(),
      apply: readModelPart("apply"),          // {byKey: {key:{apply,coverage,...}}}
      veto: readModelPart("veto"),            // {keys:{key:{hours,stats}}, ...}
      rolling: readModelPart("rolling"),      // {keys:{key:{<windowSec>:{n,meanPnl,winrate,lb}}}, windows}
      gates: (() => { const gg = readGates() || {}; return { mode: gg.mode, byKey: gg.byKey || {}, thresholds: gg.thresholds || [], liqFloorMul: gg.liqFloorMul, lateFrac: gg.lateFrac, note: gg.note }; })(),
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
