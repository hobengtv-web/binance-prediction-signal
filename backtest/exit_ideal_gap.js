/* Seberapa dekat kita bisa mendekati exit IDEAL? Uji aturan yang lebih pintar:
   - trailing pada harga dihaluskan (5s / 15s) untuk membuang noise 1 detik
   - exit saat N detik berturut-turut melawan setelah puncak
   - exit berbasis waktu (median MFE)
   - ladder parsial */
const fs=require("fs"), path=require("path");
const DATA=path.join(__dirname,"data"), DUR=300;
const load=(s,t)=>JSON.parse(fs.readFileSync(path.join(DATA,`${s}_${t}.json`),"utf8"));
const pct=(a,p)=>a.length?a.slice().sort((x,y)=>x-y)[Math.min(a.length-1,Math.floor(p/100*(a.length-1)))]:0;
const mean=a=>a.length?a.reduce((x,y)=>x+y,0)/a.length:0;
const SYM=process.argv[2]||"ALL";
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
    if(ser.length<40) continue;
    R.push({entry,gap,ser,mfe:Math.max(...ser)});
  }
}
const n=R.length, price=mean(R.map(r=>r.entry)), mfeMed=pct(R.map(r=>r.mfe),50);
const show=(name,caps)=>{const med=pct(caps,50);
  console.log(`  ${name.padEnd(32)} cap-med ${med.toFixed(3)}% ($${(med/100*price).toFixed(2)})  mean ${mean(caps).toFixed(3)}%  win ${(caps.filter(x=>x>0).length/n*100).toFixed(1)}%  efisiensi ${(med/mfeMed*100).toFixed(0)}%`);};
const sma=(a,k)=>a.map((_,i)=>mean(a.slice(Math.max(0,i-k+1),i+1)));
console.log(`\n=== Dekati IDEAL — ${SYM} n=${n} harga~$${price.toFixed(0)} | ideal cap-med ${mfeMed.toFixed(3)}% ($${(mfeMed/100*price).toFixed(2)}) ===`);
console.log(`  MFE-dimulai-dalam-60s: ${(R.filter(r=>{let mi=0;r.ser.forEach((v,i)=>{if(v>r.ser[mi])mi=i;});return mi<=58;}).length/n*100).toFixed(1)}%\n`);
show("IDEAL (batas atas)", R.map(r=>r.mfe));
// trailing pada harga dihaluskan (k=5s / 15s) setelah lock tersentuh
for(const K of [5,15,30]){
  for(const Tr of [0.01,0.02,0.03,0.05]){
    const caps=R.map(r=>{
      const sm=sma(r.ser,K); let ci=r.ser.findIndex(v=>v>=r.gap); if(ci<0) return r.ser[r.ser.length-1];
      let peak=sm[ci];
      for(let i=ci;i<sm.length;i++){ if(sm[i]>peak)peak=sm[i]; if(peak-sm[i]>=Tr) return sm[i]; }
      return sm[sm.length-1];
    });
    show(`SMA${K}s trail ${Tr}%`, caps);
  }
}
// exit saat 3 detik berturut-turut melawan setelah puncak
{
  const caps=R.map(r=>{ let ci=r.ser.findIndex(v=>v>=r.gap); if(ci<0) return r.ser[r.ser.length-1];
    let peak=-1e9;
    for(let i=ci;i<r.ser.length;i++){ if(r.ser[i]>peak)peak=r.ser[i];
      if(i>=3&&r.ser[i]<r.ser[i-1]&&r.ser[i-1]<=r.ser[i-2]&&r.ser[i-2]<=r.ser[i-3]&&r.ser[i]<peak) return r.ser[i]; }
    return r.ser[r.ser.length-1]; });
  show("3 detik melawan berturut", caps);
}
// ladder parsial: 25% di lock, 25% +0.01%, 25% +0.03%, 25% trail SMA15 0.02%
{
  const caps=R.map(r=>{ const partial=(t)=>r.ser.some(v=>v>=r.gap+t)?(r.gap+t):null;
    const p1=partial(0)??r.ser[r.ser.length-1];
    const p2=partial(0.01)??r.ser[r.ser.length-1];
    const p3=partial(0.03)??r.ser[r.ser.length-1];
    const sm=sma(r.ser,15); let ci=r.ser.findIndex(v=>v>=r.gap); let p4;
    if(ci<0)p4=r.ser[r.ser.length-1]; else { let pk=sm[ci]; let ex=sm[sm.length-1];
      for(let i=ci;i<sm.length;i++){ if(sm[i]>pk)pk=sm[i]; if(pk-sm[i]>=0.02){ex=sm[i];break;} } p4=ex; }
    return 0.25*p1+0.25*p2+0.25*p3+0.25*p4; });
  show("Ladder 25/25/25/25", caps);
}
