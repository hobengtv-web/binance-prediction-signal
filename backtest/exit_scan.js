/* Scan aturan exit: target di atas lock + hard stop. Cari ekspektansi terbaik. */
const fs=require("fs"), path=require("path");
const DATA=path.join(__dirname,"data"), DUR=300;
const SYM=process.argv[2]||"ETH";
const load=(s,t)=>JSON.parse(fs.readFileSync(path.join(DATA,`${s}_${t}.json`),"utf8"));
const pct=(a,p)=>a.length?a.slice().sort((x,y)=>x-y)[Math.min(a.length-1,Math.floor(p/100*(a.length-1)))]:0;
const mean=a=>a.length?a.reduce((x,y)=>x+y,0)/a.length:0;
const syms=SYM==="ALL"?["BTC","ETH"]:[SYM];
const R=[];
for(const sym of syms){
  const ones=load(sym==="BTC"?"BTCUSDT":"ETHUSDT","1s"); const five=load(sym,"5m");
  const om=new Map(ones.map(c=>[c.t,c])); const t0min=ones[0].t,t0max=ones[ones.length-1].t;
  for(const c of five){
    const t0=c.time; if(t0<t0min||t0+DUR-1>t0max) continue;
    const c2=om.get(t0+1); if(!c2) continue;
    const lock=c.open, entry=c2.c; if(Math.abs(entry-lock)/lock<1e-9) continue;
    const dir=entry<lock?1:-1, gap=Math.abs(lock-entry)/entry*100;
    const ser=[]; for(let t=t0+2;t<t0+DUR;t++){ const x=om.get(t); if(!x)continue;
      ser.push(dir===1?(x.c-entry)/entry*100:(entry-x.c)/entry*100); }
    if(ser.length<30) continue;
    R.push({entry,gap,ser,mfe:Math.max(...ser)});
  }
}
const n=R.length, price=mean(R.map(r=>r.entry));
console.log(`\n=== SCAN EXIT — ${SYM} n=${n} harga~$${price.toFixed(0)} | gap-med ${pct(R.map(r=>r.gap),50).toFixed(3)}% | MFE-med ${pct(R.map(r=>r.mfe),50).toFixed(3)}% ($${(pct(R.map(r=>r.mfe),50)/100*price).toFixed(2)}) ===`);
function run(T,S){
  const caps=R.map(r=>{
    const tgt=r.gap+T;
    for(let i=0;i<r.ser.length;i++){
      const v=r.ser[i];
      if(v>=tgt) return tgt;                 // target kena
      if(S!=null && v<=-S) return -S;        // stop kena
    }
    return r.ser[r.ser.length-1];
  });
  return caps;
}
console.log('  target(lock+)  stop     cap-med (\\$)      mean/ekspektansi   win%   efisiensi');
for(const T of [0.01,0.02,0.03,0.05,0.08,0.12]){
  for(const S of [null,0.05,0.10,0.20]){
    const caps=run(T,S);
    const med=pct(caps,50), m=mean(caps), w=caps.filter(x=>x>0).length/n*100;
    console.log(`  +${T.toFixed(2)}%`.padEnd(14)+String(S==null?'—':'−'+S.toFixed(2)+'%').padEnd(9)+`${med.toFixed(3)}% (\$${(med/100*price).toFixed(2)})`.padEnd(18)+`${m>=0?'+':''}${m.toFixed(3)}%`.padEnd(18)+`${w.toFixed(1)}%`.padEnd(8)+`${(med/pct(R.map(r=>r.mfe),50)*100).toFixed(0)}%`);
  }
}
