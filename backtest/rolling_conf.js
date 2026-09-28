/* Bedah "68%" versi produksi LAMA (arsip live-railway-20260928-1420).
   Logika lama (app.js:1740-1802 predictSessionStart):
     - arah = candle 1s PERTAMA sesi (>= 2000ms)  -> "sinyal sangat awal 1s"
     - hitung win rate empiris pola itu atas 30 sesi 5m TERAKHIR (rolling)
     - prediksi = continuation bila rolling winrate > 0.5, REVERSAL bila < 0.5
     - confidence = round(rolling winrate * 100)   <-- inilah angka 68% yang terlihat
   Uji: seberapa jujur angka itu? (walk-forward, 7d 1s) */
const fs = require("fs"), path = require("path");
const DATA = path.join(__dirname, "data"), DUR = 300;
const load = (s, t) => JSON.parse(fs.readFileSync(path.join(DATA, `${s}_${t}.json`), "utf8"));
const pct = (a, p) => a.length ? a.slice().sort((x, y) => x - y)[Math.min(a.length - 1, Math.floor(p / 100 * (a.length - 1)))] : 0;
const mean = (a) => a.length ? a.reduce((x, y) => x + y, 0) / a.length : 0;

const rows = [];
for (const sym of ["BTC", "ETH"]) {
  const ones = load(sym === "BTC" ? "BTCUSDT" : "ETHUSDT", "1s");
  const five = load(sym, "5m");
  const om = new Map(ones.map((c) => [c.t, c]));
  const t0min = ones[0].t, t0max = ones[ones.length - 1].t;
  for (const c of five) {
    const t0 = c.time;
    if (t0 < t0min + 120 || t0 + DUR > t0max) continue;
    const f1 = om.get(t0); if (!f1) continue;
    const firstDir = f1.c > f1.o ? "bull" : f1.c < f1.o ? "bear" : null;
    if (!firstDir) continue;
    const outcome = c.close >= c.open ? "up" : "down";
    rows.push({ sym, t0, firstDir, outcome, won: firstDir === "bull" ? (outcome === "up" ? 1 : 0) : (outcome === "down" ? 1 : 0) });
  }
}
rows.sort((a, b) => a.t0 - b.t0);
console.log(`=== REPLIKASI "sinyal 1s" versi LAMA — BTC+ETH 7d, n=${rows.length} sesi 5m ===\n`);

const rawWin = mean(rows.map((r) => r.won));
console.log(`1) Winrate MENTAH pola "candle 1s pertama menentukan arah close":`);
console.log(`   continuation dari sinyal 1s = ${(rawWin * 100).toFixed(1)}%  (n=${rows.length})`);
console.log(`   -> di 1 detik arah nyaris lempar koin; ini batas atas sebenarnya.\n`);

// walk-forward meniru versi lama: pakai 30 sesi terakhir per-aset
const W = 30;
const bySym = { BTC: rows.filter((r) => r.sym === "BTC"), ETH: rows.filter((r) => r.sym === "ETH") };
const preds = [], conf = [];
for (const s of ["BTC", "ETH"]) {
  const R = bySym[s];
  for (let i = W; i < R.length; i++) {
    const hist = R.slice(i - W, i);
    const bull = hist.filter((r) => r.firstDir === "bull"), bear = hist.filter((r) => r.firstDir === "bear");
    const bwr = bull.length ? mean(bull.map((r) => r.won)) : 0.5;
    const swr = bear.length ? mean(bear.map((r) => r.won)) : 0.5;
    const wr = R[i].firstDir === "bull" ? bwr : swr;
    const dir = wr > 0.5 ? (R[i].firstDir === "bull" ? "up" : "down") : (R[i].firstDir === "bull" ? "down" : "up");
    const actual = R[i].outcome;
    preds.push({ won: dir === actual ? 1 : 0, conf: Math.round(wr * 100), cont: wr > 0.5 });
    conf.push(Math.round(wr * 100));
  }
}
const acc = mean(preds.map((p) => p.won)) * 100;
console.log(`2) Akurasi ATURAN LAMA out-of-sample (rolling 30 sesi, flip bila winrate<50%):`);
console.log(`   akurasi = ${acc.toFixed(1)}%  (n=${preds.length})  <- ini winrate riil yang bisa didapat\n`);

console.log(`3) Distribusi angka "confidence" yang ditampilkan (rolling winrate x100) — INILAH angka yang dilihat user:`);
console.log(`   p10 ${pct(conf, 10)}% · p25 ${pct(conf, 25)}% · median ${pct(conf, 50)}% · p75 ${pct(conf, 75)}% · p90 ${pct(conf, 90)}% · max ${Math.max(...conf)}%`);
const ge = (t) => (conf.filter((c) => c >= t).length / conf.length * 100).toFixed(1);
console.log(`   tampil >= 60% : ${(ge(60))}% waktu ·  >= 68% : ${ge(68)}% waktu · >= 70% : ${ge(70)}% waktu · >= 80% : ${ge(80)}% waktu`);
console.log(`\n   -> dengan jendela hanya 30 sesi, winrate bisa terbaca 68-85% padahal underlyingnya ${rawWin * 100 < 50 ? (100 - rawWin * 100).toFixed(1) : (rawWin * 100).toFixed(1)}%`);
console.log(`   -> confidence tinggi = NOISE jendela pendek, bukan edge.\n`);

for (const t of [60, 65, 70, 75, 80]) {
  const sel = preds.filter((p) => p.conf >= t);
  console.log(`   saat confidence >= ${t}%: n=${sel.length} akurasi nyata ${sel.length ? (mean(sel.map((p) => p.won)) * 100).toFixed(1) : "—"}%`);
}
