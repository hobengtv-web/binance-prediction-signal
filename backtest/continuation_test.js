/* Measure the CONTINUATION POTENTIAL after price touches the lock.
   Question: when the price recovers to the lock (the moment the assistant says CLOSE),
   how much further does it typically run before the session ends, and when is the peak?
   Usage: node backtest/continuation_test.js */
const fs = require("fs"), path = require("path");
const DATA = path.join(__dirname, "data");
const MS = { "5m": 300, "15m": 900 };
const load = (s, t) => JSON.parse(fs.readFileSync(path.join(DATA, `${s}_${t}.json`), "utf8"));
const pct = (a, p) => a.length ? a.slice().sort((x, y) => x - y)[Math.min(a.length - 1, Math.floor(p / 100 * (a.length - 1)))] : 0;
const OUT = {};

for (const tf of ["5m", "15m"]) {
  const step = MS[tf] / 60;
  const rows = [];
  for (const sym of ["BTC", "ETH"]) {
    const one = load(sym, "1m"), tfC = load(sym, tf);
    const idx = new Map(one.map((c, i) => [c.time, i]));
    for (const c of tfC) {
      const i0 = idx.get(c.time); if (i0 == null) continue;
      const lock = c.open;
      const m = []; for (let k = 0; k < step; k++) { const x = one[i0 + k]; if (!x) break; m.push(x); }
      if (m.length !== step) continue;
      // two cases: UP (dipped below then recovers to lock) and DOWN (spiked above then returns)
      for (const dir of ["up", "down"]) {
        let contra = -1, touch = -1;
        for (let j = 0; j < m.length; j++) {
          const below = m[j].close < lock, above = m[j].close > lock;
          if (contra < 0 && (dir === "up" ? below : above)) contra = j;
          if (contra >= 0 && j > contra && (dir === "up" ? m[j].close >= lock : m[j].close <= lock)) { touch = j; break; }
        }
        if (touch < 0) continue;
        // continuation after the touch: max favourable excursion (use highs/lows) in %
        let best = 0, bestAt = 0;
        for (let j = touch; j < m.length; j++) {
          const exc = dir === "up" ? (m[j].high - lock) / lock * 100 : (lock - m[j].low) / lock * 100;
          if (exc > best) { best = exc; bestAt = j; }
        }
        // momentum at the touch: slope of the last 2 minutes + last candle direction
        const prev = one[i0 + touch - 1], cur = m[touch];
        const lastUp = cur.close > cur.open;
        const momUp = prev ? cur.close > prev.close : lastUp;
        const aligned = dir === "up" ? (lastUp && momUp) : (!lastUp && !momUp);
        rows.push({ dir, touch, minutesLeft: m.length - 1 - touch, mfe: best, peakMin: bestAt, aligned, size: (m[bestAt].high - m[bestAt].low) / lock * 100 });
      }
    }
  }
  const W = (a) => a.length ? (a.reduce((x, r) => x + r.mfe, 0) / a.length) : 0;
  const all = rows, al = rows.filter(r => r.aligned), na = rows.filter(r => !r.aligned);
  console.log(`\n=== ${tf}: setelah harga menyentuh lock (n=${rows.length}) ===`);
  console.log(`  MFE (gerak lanjut maksimum) persentil: p25=${pct(all.map(r=>r.mfe),25).toFixed(3)}%  median=${pct(all.map(r=>r.mfe),50).toFixed(3)}%  p75=${pct(all.map(r=>r.mfe),75).toFixed(3)}%  p90=${pct(all.map(r=>r.mfe),90).toFixed(3)}%`);
  console.log(`  rata-rata MFE: semua=${W(all).toFixed(3)}%  momentum searah=${W(al).toFixed(3)}% (n=${al.length})  melawan=${W(na).toFixed(3)}% (n=${na.length})`);
  console.log(`  probabilitas lanjut >= 0.05%: ${(all.filter(r=>r.mfe>=0.05).length/all.length*100).toFixed(1)}%   >= 0.10%: ${(all.filter(r=>r.mfe>=0.10).length/all.length*100).toFixed(1)}%   >= 0.20%: ${(all.filter(r=>r.mfe>=0.20).length/all.length*100).toFixed(1)}%`);
  console.log(`  menit ke puncak: median=${pct(all.map(r=>r.peakMin),50)}  p75=${pct(all.map(r=>r.peakMin),75)}  sisa waktu rata2=${(all.reduce((a,r)=>a+r.minutesLeft,0)/all.length).toFixed(1)} menit`);
  console.log(`  (momentum searah) prob >=0.05%: ${(al.filter(r=>r.mfe>=0.05).length/al.length*100).toFixed(1)}%  >=0.10%: ${(al.filter(r=>r.mfe>=0.10).length/al.length*100).toFixed(1)}%  >=0.20%: ${(al.filter(r=>r.mfe>=0.20).length/al.length*100).toFixed(1)}%`);
  OUT[tf] = {
    n: all.length,
    mfe: { p25: +pct(all.map(r=>r.mfe),25).toFixed(4), p50: +pct(all.map(r=>r.mfe),50).toFixed(4), p75: +pct(all.map(r=>r.mfe),75).toFixed(4), p90: +pct(all.map(r=>r.mfe),90).toFixed(4) },
    aligned: { n: al.length, p50: +pct(al.map(r=>r.mfe),50).toFixed(4), p75: +pct(al.map(r=>r.mfe),75).toFixed(4), avg: +W(al).toFixed(4) },
    against: { n: na.length, p50: +pct(na.map(r=>r.mfe),50).toFixed(4), avg: +W(na).toFixed(4) },
    prob: { ge005: +(all.filter(r=>r.mfe>=0.05).length/all.length).toFixed(4), ge010: +(all.filter(r=>r.mfe>=0.10).length/all.length).toFixed(4), ge020: +(all.filter(r=>r.mfe>=0.20).length/all.length).toFixed(4) },
    probAligned: { ge005: +(al.filter(r=>r.mfe>=0.05).length/al.length).toFixed(4), ge010: +(al.filter(r=>r.mfe>=0.10).length/al.length).toFixed(4), ge020: +(al.filter(r=>r.mfe>=0.20).length/al.length).toFixed(4) },
    peakMin: { p50: pct(all.map(r=>r.peakMin),50), p75: pct(all.map(r=>r.peakMin),75) },
  };
}
fs.writeFileSync(path.join(__dirname, "out", "continuation.json"), JSON.stringify({ generated: new Date().toISOString(), windowDays: 90, tiers: OUT }, null, 2));
console.log("\nWrote out/continuation.json");
