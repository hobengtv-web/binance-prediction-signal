/* ============================================================================
   CAPTURE / ENGINE SINYAL SERVER-SIDE
   Dua pemakai, SATU rumus:
     - computeSignal()  : fungsi MURNI (tanpa I/O) yang menghitung sinyal sesi kanonik
                          2 detik: lock, C2, vol2sum, sigma1s, volRel2, surprise, tier,
                          gate likuiditas, threshold profil gate, RSI, histStrength.
     - createCapture()  : merekam hasilnya ke ledger (termasuk sesi yang DITOLAK, agar
                          learner bisa belajar dari populasi yang ditolak).
   engine.js (mesin sinyal live untuk semua device) memakai computeSignal() yang sama,
   sehingga sinyal yang tampil di UI = sinyal yang dinilai/dipelajari. Tidak ada duplikasi
   rumus.

   Hasil ronde tetap dilengkapi resolveMissing() di server.js (klines 1m); kalau ada
   klien yang mengirim hasil dari jalur 1s (lebih presisi), hasil itu yang dipakai.
   ============================================================================ */
const LEARNER_BUCKETS = require("./learner.js").BUCKETS;
const GATES_DEF = require("./gates.js");

const VOL_TYPICAL = { BTC: 0.515, ETH: 8.22, BNB: 3.0 };
const DUR_S = { "5m": 300, "15m": 900, "1h": 3600 };   // 1h ikut diproses server
const mean = (a) => (a.length ? a.reduce((x, y) => x + y, 0) / a.length : 0);
function pctile(arr, p) {
  if (!arr || !arr.length) return 0;
  const s = arr.slice().sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.max(0, Math.floor((p / 100) * (s.length - 1))))];
}
const rsiBucket = (r) => (r == null ? "na" : r < 30 ? "<30" : r < 40 ? "30-40" : r < 60 ? "40-60" : r < 70 ? "60-70" : ">70");
const strBucket = (s) => (s == null ? "na" : s < 35 ? "<35" : s <= 50 ? "35-50" : ">50");

/* ===== VETO "NO-EDGE" (default ON; matikan dengan env TA_VETO=0) =====
   Analisa 2.542 sesi live: sesi tanpa edge ber-WR ~48-50% (≈ koin). Veto membuangnya
   (accepted=false) agar BOT tidak entry. Ambang bisa diubah via env tanpa ubah kode. */
const _n = (v, d) => { const x = Number(v); return isFinite(x) ? x : d; };
const TA_VETO = {
  on: process.env.TA_VETO !== "0",
  rewardMin: _n(process.env.TA_VETO_REWARD_MIN, 0.002),
  rsiLo: _n(process.env.TA_VETO_RSI_LO, 30), rsiHi: _n(process.env.TA_VETO_RSI_HI, 40),
  volLo: _n(process.env.TA_VETO_VOL_LO, 1.3), volHi: _n(process.env.TA_VETO_VOL_HI, 1.8),
  hours: String(process.env.TA_VETO_HOURS || "2,3,5,12,21").split(",").map((x) => parseInt(x, 10)).filter((x) => !isNaN(x)), // WIB
  liqMin: _n(process.env.TA_VETO_LIQ_MIN, 2),
};

/* ============================================================================
   computeSignal(o) — INTI PERHITUNGAN (murni, tanpa fetch).
   Input: { sym, tf, t0, tfc, idx, ones, five5m, profile, getModel, SignalCore, nowSec }
     tfc    = klines interval (candle sesi pada t0 harus ada di indeks idx)
     ones   = klines 1s di sekitar t0 (harus memuat t0 dan t0+1)
     five5m = klines 5m yang SUDAH selesai (untuk RSI tanpa lookahead)
   Output: { ok:true, signal:{...} } atau { skipped:"<alasan>" }
   ============================================================================ */
function computeSignal(o) {
  const { sym, tf, t0, tfc, idx, ones, five5m, getModel, SignalCore, nowSec } = o;
  const profile = o.profile || GATES_DEF.BOOTSTRAP;
  const tfSec = DUR_S[tf];
  if (!tfSec) return { skipped: "bad-tf" };
  if (!SignalCore) return { skipped: "no-core" };
  if (idx < 26) return { skipped: "no-session" };
  const sess = tfc[idx];
  const lock = sess && sess.open;
  if (!lock) return { skipped: "no-lock" };
  const prior = tfc.slice(idx - 25, idx);
  const baseVol = mean(prior.map((c) => c.vol || 0));
  const prior5 = tfc.slice(Math.max(0, idx - 50), idx).map((c) => c.vol || 0).filter((v) => v > 0);
  const histTrend = SignalCore.analyzeHistoricalTrend(tfc.slice(0, idx + 1), 50);
  const o1 = ones.find((c) => c.time === t0), o2 = ones.find((c) => c.time === t0 + 1);
  if (!o1 || !o2) return { skipped: "no-1s" };
  const C2 = o2.close;
  const vol2sum = (o1.vol || 0) + (o2.vol || 0);
  const pre = ones.filter((c) => c.time >= t0 - 60 && c.time < t0);
  const sigma1s = pre.length ? mean(pre.map((c) => c.high - c.low)) : 0;
  const volRel2 = baseVol > 0 ? (vol2sum * (tfSec / 2)) / baseVol : 1;
  const moveAbs = Math.abs(C2 - lock);
  const mv2 = lock > 0 ? (moveAbs / lock) * 100 : 0;
  const surprise = sigma1s > 0 ? moveAbs / sigma1s : 0;
  const currentDir = C2 > lock ? "up" : C2 < lock ? "down" : "flat";
  if (currentDir === "flat") return { skipped: "flat-price" };
  // rsi dari candle 5m yang SUDAH SELESAI (tanpa lookahead)
  let rsi = null;
  try {
    if (five5m && five5m.length) rsi = SignalCore.rsiFromSeries(five5m.filter((c) => c.time + 300 <= nowSec).slice(-50), 14);
  } catch (_) {}
  // CATATAN: gate `verdict !== flat` milik app memakai volRel yang bergantung waktu
  // (artefak floor `frac` = 0.05 sebelum detik ke-15), sehingga pada 2 detik app sering
  // melaporkan LOWVOL walau ladder tier lolos. Populasi yang dipakai = ladder tier 2 detik
  // yang terdokumentasi (early2s.json) + gate likuiditas; mode/conf seperti sinyal TREND.
  const mode = "TREND", conf = 65;
  const T = profile.tiers || GATES_DEF.BOOTSTRAP.tiers;
  const gateNow = Math.abs((C2 - lock) / lock) * 100;
  const grade = (volRel2 >= T.STRONG.volRel2 && surprise >= (T.STRONG.surprise || 0)) ? "STRONG"
    : (volRel2 >= T.GOOD.volRel2 && surprise >= (T.GOOD.surprise || 0)) ? "GOOD"
      : (volRel2 >= T.FAIR.volRel2 && surprise >= (T.FAIR.surprise || 0)) ? "FAIR" : null;
  const typ5m = (VOL_TYPICAL[sym] || 0) * 60;
  const proj = vol2sum * (tfSec / 2);
  const liqMul = profile.liqFloorMul != null ? profile.liqFloorMul : 0.3;
  const floor = Math.max(pctile(prior5, 15), typ5m * liqMul);
  const liqLow = typ5m > 0 && proj < floor;
  const liqRatio = typ5m > 0 ? proj / typ5m : 1;
  const thOK = GATES_DEF.applyThresholds({ volRel2, surprise, liqRatio, gapPct: gateNow, histStrength: histTrend.strength, rsi }, profile.thresholds);
  const accepted0 = !!grade && !liqLow && thOK;
  // ===== VETO no-edge: buang cohort WR~50% (reward kecil, RSI 30-40, vol choppy, jam buruk, liqud tipis)
  let veto = null;
  if (TA_VETO.on) {
    const wibH = new Date((t0 + 7 * 3600) * 1000).getUTCHours();
    if (gateNow < TA_VETO.rewardMin) veto = "veto-reward";
    else if (rsi != null && rsi >= TA_VETO.rsiLo && rsi < TA_VETO.rsiHi) veto = "veto-rsi";
    else if (volRel2 >= TA_VETO.volLo && volRel2 < TA_VETO.volHi) veto = "veto-vol";
    else if (TA_VETO.hours.indexOf(wibH) >= 0) veto = "veto-hour";
    else if (liqRatio < TA_VETO.liqMin) veto = "veto-liq";
  }
  const accepted = accepted0 && !veto;
  const reject = accepted ? null : (veto || (!thOK ? "threshold" : !grade ? "tier" : "liq-low"));

  const d2 = ((C2 - lock) / lock) * 100;
  const rewardPct = Math.abs(d2);
  const gapB = LEARNER_BUCKETS.bGap(rewardPct);
  const hourB = LEARNER_BUCKETS.bHour(new Date(t0 * 1000).getUTCHours());
  const trend = SignalCore.sessionTrend(tfc.slice(0, idx + 1), 3);
  // konteks learner (dari model yang sedang dipakai) — sama artinya dengan panel app
  let touchRate = null, learn = { trend, gap: gapB, hour: hourB };
  try {
    if (typeof getModel === "function") {
      const g = getModel("gate"), t = getModel("touch");
      const tb = t && t.buckets && t.buckets.gap ? t.buckets.gap[gapB] : null;
      if (tb && tb.nTest >= 200) touchRate = tb.touchTest;
      const iv = g && g.buckets && g.buckets.interval ? g.buckets.interval[tf] : null;
      const mi = g && g.buckets && g.buckets.minute ? g.buckets.minute["1"] : null;
      const ivWR = iv && iv.nTest >= 200 ? iv.wrTest : null;
      const miWR = mi && mi.nTest >= 200 ? mi.wrTest : null;
      const weak = [], strong = [];
      if (ivWR != null && ivWR < 0.66) weak.push("interval");
      if (touchRate != null && touchRate < 0.62) weak.push("touch");
      if (touchRate != null && touchRate >= 0.72) strong.push("touch");
      if (miWR != null && miWR >= 0.70) strong.push("dir");
      const blocking = (() => {
        const gb = (g && g.suppress) || [], tbs = (t && t.suppress) || [];
        return gb.indexOf(`interval=${tf}`) >= 0 || tbs.indexOf(`gap=${gapB}`) >= 0;
      })();
      learn = Object.assign(learn, {
        touch: touchRate, dirWR: miWR, intervalWR: ivWR, blocking,
        label: weak.length && !strong.length ? "LEMAH" : strong.length && !weak.length ? "KUAT" : weak.length ? "CAMPURAN" : "NETRAL",
      });
    }
  } catch (_) {}

  return {
    ok: true,
    signal: {
      asset: sym, interval: tf, t0, lock,
      dir: currentDir, mode, conf,
      grade: grade || null, accepted, reject, thresholdsOK: !!thOK,
      volRel2: +volRel2.toFixed(4), surprise: +surprise.toFixed(4), mv2: +mv2.toFixed(5),
      rsi: rsi != null ? +rsi.toFixed(2) : null, histStrength: histTrend.strength,
      rewardPct: +rewardPct.toFixed(4), liqRatio: +liqRatio.toFixed(3), liqLow: !!liqLow,
      touchRate, gateKey: `${tf}|${mode}|${currentDir}|rsi:${rsiBucket(rsi)}|str:${strBucket(histTrend.strength)}`,
      // OFI sesi dari flow server (null = belum ada data). Ikut payload -> semua device
      // menampilkan angka yang SAMA, dan ikut terekam ke ledger/learner.
      ofi: FLOW.sessionOFI(sym, t0, nowSec),
      learn,
    },
  };
}

const FLOW = require("./flow.js");

function createCapture(deps) {
  const { getKlines, SignalCore, getModel, save, log = console.log, getGates } = deps;
  let stats = { enabled: true, captured: 0, accepted: 0, rejected: 0, skipped: 0, errors: 0, lastAt: null, lastErr: null, lastKey: null };

  async function captureOne(sym, tf, t0) {
    const tfSec = DUR_S[tf];
    if (!tfSec) return { skipped: "bad-tf" };
    if (!SignalCore) return { skipped: "no-core" };
    const tfc = await getKlines(sym, tf, t0 + tfSec - 1, 60);
    const idx = tfc.findIndex((c) => c.time === t0);
    if (idx < 26) return { skipped: "no-session" };
    const lock = tfc[idx].open;
    if (!lock) return { skipped: "no-lock" };
    const ones = await getKlines(sym, "1s", t0 + 2, 70);
    FLOW.addKlines(sym, ones);                              // OFI dari REST (dedupe -> aman dipanggil terus)
    const nowSec = Math.floor(Date.now() / 1000);
    let five5m = [];
    try { five5m = await getKlines(sym, "5m", nowSec - 1, 60); } catch (_) {}
    const profile = (typeof getGates === "function" ? getGates() : null) || GATES_DEF.BOOTSTRAP;   // profil gate yang BERLAKU (bootstrap/learned)
    const r = computeSignal({ sym, tf, t0, tfc, idx, ones, five5m, profile, getModel, SignalCore, nowSec });
    if (r.skipped) return { skipped: r.skipped };
    const sig = r.signal;
    const rec = {
      k: `${sym}_${tf}_${t0}`, asset: sym, interval: tf, t0, src: "server",
      sig: Object.assign({}, sig, {
        capOffsetMs: 2000, capturedAt: nowSec, prov: "server", minuteIn: 1, ofi: (sig.ofi != null ? sig.ofi : null), gateWr: null,
      }),
      // Keputusan gate saat perekaman: untuk membandingkan populasi diterima vs ditolak.
      gate: { grade: sig.grade, liqLow: sig.liqLow, liqRatio: sig.liqRatio, thresholdsOK: sig.thresholdsOK, accepted: sig.accepted, reject: sig.reject, profile: profile.mode },
    };
    save(rec, "server");
    stats.captured++; stats.lastAt = Date.now(); stats.lastKey = rec.k;
    return { ok: true, rec };
  }

  const done = new Set();
  let busy = false;
  async function tick() {
    if (!stats.enabled || busy) return;
    busy = true;
    try {
      const now = Date.now();
      for (const sym of ["BTC", "ETH", "BNB"]) {
        for (const tf of Object.keys(DUR_S)) {
          if (sym === "BNB" && tf !== "5m") continue;         // BNB hanya 5m (selaras engine/bot)
          const durMs = DUR_S[tf] * 1000;
          const t0 = Math.floor(now / durMs) * durMs;
          const off = now - t0;
          if (off < 3500 || off > 20000) continue;            // hanya dekat awal sesi
          const key = `${sym}_${tf}_${t0 / 1000}`;
          if (done.has(key)) continue;
          done.add(key);
          if (done.size > 4000) { const it = done.values(); for (let i = 0; i < 2000; i++) done.delete(it.next().value); }
          try {
            const r = await captureOne(sym, tf, t0 / 1000);
            if (r && r.skipped) { stats.skipped++; log(`[CAPTURE] ${sym} ${tf} dilewati: ${r.skipped}`); }
            else if (r && r.ok) {
              if (r.rec.gate && r.rec.gate.accepted) stats.accepted = (stats.accepted || 0) + 1; else stats.rejected = (stats.rejected || 0) + 1;
              log(`[CAPTURE] ${sym} ${tf} ${r.rec.k} dir=${r.rec.sig.dir} grade=${r.rec.sig.grade || "-"} volRel2=${r.rec.sig.volRel2} surprise=${String(r.rec.sig.surprise).slice(0, 6)} gap=${r.rec.sig.learn.gap} ${r.rec.gate.accepted ? "DITERIMA" : "DITOLAK:" + r.rec.gate.reject}`);
            }
          } catch (e) { stats.errors++; stats.lastErr = e && e.message; log(`[CAPTURE] gagal ${sym} ${tf}: ${e && e.message}`); }
        }
      }
    } finally { busy = false; }
  }

  function start() {
    if (process.env.CAPTURE === "0") { stats.enabled = false; log("[CAPTURE] dinonaktifkan (CAPTURE=0)"); return; }
    setInterval(() => tick().catch(() => {}), 1000);
    log(`[CAPTURE] aktif — snapshot kanonik 2 detik untuk ${Object.keys(DUR_S).join(", ")}`);
  }
  return { start, tick, captureOne, status: () => stats };
}

module.exports = { createCapture, computeSignal, DUR_S, VOL_TYPICAL };
