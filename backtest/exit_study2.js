/* EXIT STUDY v2 — entry di detik ke-2 (arah menuju lock), ukur berapa banyak "excess" di atas
   lock yang bisa ditangkap oleh berbagai aturan exit, vs ideal (puncak). */
const fs=require("fs"), path=require("path");
const DATA=path.join(__dirname,"data"), DUR=300;
const SYM=process.argv[2]||"ETH";
const load=(s,t)=>JSON.parse(fs.readFileSync(path.join(DATA,`${s}_${t}.json`),"utf8"));
const pct=(a,p)=>a.length?a.slice().sort((x,y)=>x-y)[Math.min(a.length-1,Math.floor(p/100*(a.length-1)))]:0;
const syms=SYM==="ALL"?["BTC","ETH"]:[SYM];
const R=[];
for(const sym of syms){
  const ones=load(sym==="BTC"?"BTCUSDT":"ETHUSDT","1s"); const five=load(sym,"5m");
  const om=new Map(ones.map(c=>[c.t,c])); const t0min=ones[0].t,t0max=ones[ones.length-1].t;
  for(const c of five){
    const t0=c.time; if(t0<t0min||t0+DUR-1>t0max) continue;
    const c2=om.get(t0+1); if(!c2) continue;
    const lock=c.open, entry=c2.c; if(Math.abs(entry-lock)/lock<1e-9) continue;
    const dir=entry<lock?1:-1;
    const gap=Math.abs(lock-entry)/entry*100;
    const ser=[]; for(let t=t0+2;t<t0+DUR;t++){ const x=om.get(t); if(!x) continue;
      ser.push(dir===1?(x.c-entry)/entry*100:(entry-x.c)/entry*100); }
    if(ser.length<30) continue;
    const mfe=Math.max(...ser);
    // crossed = kapan lock tersentuh (favorable >= gap)
    let crossIdx=ser.findIndex(v=>v>=gap);
    R.push({sym,entry,lock,gap,ser,mfe,crossed:crossIdx>=0,crossIdx,excess:mfe-gap});
  }
}
const n=R.length, price=mean(R.map(r=>r.entry));
function mean(a){return a.length?a.reduce((x,y)=>x+y,0)/a.length:0;}
const wr=a=>a.filter(x=>x>0).length/a.length*100;
const show=(name,caps)=>{ const med=pct(caps,50);
  console.log(`  ${name.padEnd(34)} cap-med ${med.toFixed(3)}% ($${(med/100*price).toFixed(2)})  mean ${mean(caps).toFixed(3)}%  win ${wr(caps).toFixed(1)}%  efisiensi ${(med/pct(R.map(r=>r.mfe),50)*100).toFixed(0)}%`); };
console.log(`\n=== EXIT STUDY v2 — ${SYM} (entry detik-2, arah menuju lock) n=${n}, ETH/entryPrice~$${price.toFixed(0)} ===`);
console.log(`  touch lock: ${(R.filter(r=>r.crossed).length/n*100).toFixed(1)}%  gap-med ${pct(R.map(r=>r.gap),50).toFixed(3)}% ($${(pct(R.map(r=>r.gap),50)/100*price).toFixed(2)})`);
console.log(`  MFE-med ${pct(R.map(r=>r.mfe),50).toFixed(3)}%  excess (MFE-gap) med ${pct(R.map(r=>r.excess),50).toFixed(3)}%  p75 ${pct(R.map(r=>r.excess),75).toFixed(3)}%`);
console.log('');
show("IDEAL (puncak)", R.map(r=>r.mfe));
show("TAHAN sampai sesi habis", R.map(r=>r.ser[r.ser.length-1]));
show("JUAL DI LOCK (touch)", R.map(r=>r.crossed?r.gap:r.ser[r.ser.length-1]));
for(const T of [0.01,0.02,0.03,0.05]){
  show(`Target lock+${T}%`, R.map(r=>{const t=r.gap+T; const i=r.ser.findIndex(v=>v>=t); return i>=0?t:r.ser[r.ser.length-1];}));
}
for(const Rr of [0.005,0.01,0.02,0.03,0.05]){
  show(`Trail ${Rr}% setelah lock`, R.map(r=>{
    if(!r.crossed) return r.ser[r.ser.length-1];
    let peak=r.gap;
    for(let i=r.crossIdx;i<r.ser.length;i++){ const v=r.ser[i]; if(v>peak)peak=v; if(peak-v>=Rr) return v; }
    return r.ser[r.ser.length-1];
  }));
}
// trail + slope melawan (meniru turn evidence)
for(const Rr of [0.01,0.02,0.03]){
  show(`Trail ${Rr}% + slope melawan`, R.map(r=>{
    if(!r.crossed) return r.ser[r.ser.length-1];
    let peak=r.gap;
    for(let i=r.crossIdx;i<r.ser.length;i++){ const v=r.ser[i]; if(v>peak)peak=v;
      const slopeAgainst=i>=3&&(r.ser[i]<r.ser[i-3]);
      if(peak-v>=Rr&&slopeAgainst) return v; }
    return r.ser[r.ser.length-1];
  }));
}
