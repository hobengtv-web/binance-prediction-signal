/* Studi EXIT: entry di detik ke-2 (arah = menuju lock / mean-reversion sesuai behaviour),
   lalu bandingkan beberapa aturan close vs exit IDEAL (puncak maksimum sebelum berbalik).
   Usage: node backtest/exit_study.js [SYM]   (BTC/ETH/ALL) */
const fs=require("fs"), path=require("path");
const DATA=path.join(__dirname,"data"), DUR=300;
const SYM=process.argv[2]||"ETH";
const load=(s,t)=>JSON.parse(fs.readFileSync(path.join(DATA,`${s}_${t}.json`),"utf8"));
const pct=(a,p)=>a.length?a.slice().sort((x,y)=>x-y)[Math.min(a.length-1,Math.floor(p/100*(a.length-1)))]:0;
const mean=a=>a.length?a.reduce((x,y)=>x+y,0)/a.length:0;

const syms=SYM==="ALL"?["BTC","ETH"]:[SYM];
const rows=[];
for(const sym of syms){
  const ones=load(sym==="BTC"?"BTCUSDT":"ETHUSDT","1s"); const five=load(sym,"5m");
  const om=new Map(ones.map(c=>[c.t,c]));
  const t0min=ones[0].t,t0max=ones[ones.length-1].t;
  for(const c of five){
    const t0=c.time; if(t0<t0min||t0+DUR-1>t0max) continue;
    const c2=om.get(t0+1); if(!c2) continue;
    const lock=c.open, entry=c2.c;
    if(Math.abs(entry-lock)/lock<1e-9) continue;
    // arah = menuju lock (mean-reversion): di bawah lock -> UP, di atas -> DOWN
    const dir = entry<lock ? 1 : -1;
    const path=[]; for(let t=t0+2;t<t0+DUR;t++){ const x=om.get(t); if(x) path.push(x.c); }
    if(path.length<30) continue;
    const fav = p=>dir===1 ? (p-entry)/entry*100 : (entry-p)/entry*100;   // % menguntungkan
    const ser=path.map(fav);
    const mfe=Math.max(...ser);
    rows.push({sym,lock,entry,dir,ser,mfe,gapUsd:Math.abs(entry-lock)});
  }
}
const n=rows.length;
console.log(`\n=== STUDI EXIT — ${SYM} (entry di detik ke-2, arah menuju lock) n=${n} ===`);
const usd=(r,p)=>p/100*r.entry;   // konversi % -> $
const stat=(name,caps)=>{
  const w=caps.filter(x=>x>0).length;
  const med=pct(caps,50), m=mean(caps);
  const effMed=pct(rows.map(r=>r.mfe),50);
  console.log(`  ${name.padEnd(30)} cap-med ${med.toFixed(3)}% (\$${usd(rows[0],med).toFixed(2)})  mean ${m.toFixed(3)}%  win ${(w/n*100).toFixed(1)}%  efisiensi ${(med/effMed*100).toFixed(0)}% vs MFE-med`);
};
// exit IDEAL (puncak) & hold-to-end sebagai pembanding
stat("IDEAL (puncak maksimum)", rows.map(r=>r.mfe));
stat("TAHAN sampai sesi habis", rows.map(r=>r.ser[r.ser.length-1]));
stat("JUAL DI LOCK", rows.map(r=>r.ser.find(v=>v>=0)!==undefined?0:r.ser[r.ser.length-1])); // touch lock -> 0 (di lock), gagal -> minus
// trailing stop: keluar bila mundur R% dari puncak berjalan
for(const R of [0.003,0.005,0.008,0.01,0.015,0.02,0.03]){
  const caps=rows.map(r=>{
    let peak=-1e9;
    for(let i=0;i<r.ser.length;i++){
      const v=r.ser[i]; if(v>peak) peak=v;
      if(peak-v>=R) return v;                 // mundur R dari puncak -> keluar di harga sekarang
    }
    return r.ser[r.ser.length-1];
  });
  stat(`Trailing stop ${R}%`, caps);
}
// target tetap
for(const T of [0.01,0.02,0.03,0.05]){
  const caps=rows.map(r=>{ const i=r.ser.findIndex(v=>v>=T); return i>=0?T:r.ser[r.ser.length-1]; });
  const hit=rows.map(r=>{const i=r.ser.findIndex(v=>v>=T);return i>=0?1:0;}).reduce((a,b)=>a+b,0)/n;
  console.log(`  ${("Target tetap "+T+"%").padEnd(30)} cap ${T.toFixed(3)}% (\$${usd(rows[0],T).toFixed(2)}) (hanya bila kena)  ketemu ${(hit*100).toFixed(1)}%  gagal->akhir sesi`);
}
// trailing + konfirmasi (meniru 'turn evidence': mundur R dan slope 3s melawan)
for(const R of [0.005,0.01,0.015]){
  const caps=rows.map(r=>{
    let peak=-1e9;
    for(let i=0;i<r.ser.length;i++){
      const v=r.ser[i]; if(v>peak) peak=v;
      const slopeAgainst = i>=3 && (r.ser[i]<r.ser[i-3]);
      if(peak-v>=R && slopeAgainst) return v;
    }
    return r.ser[r.ser.length-1];
  });
  stat(`Trailing ${R}% + slope melawan`, caps);
}
console.log(`\n  distribusi MFE (puncak ideal): med ${pct(rows.map(r=>r.mfe),50).toFixed(3)}%  p75 ${pct(rows.map(r=>r.mfe),75).toFixed(3)}%  p90 ${pct(rows.map(r=>r.mfe),90).toFixed(3)}%`);
console.log(`  gap entry->lock: med \$${pct(rows.map(r=>r.gapUsd),50).toFixed(2)}  (contoh harga ETH ~\$${rows[0].entry.toFixed(0)})`);
