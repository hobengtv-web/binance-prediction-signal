/* Hitung win-rate target lanjutan (lock + T%) utk BTC+ETH, tulis ke out/locktouch.json */
const fs=require("fs"), path=require("path");
const DATA=path.join(__dirname,"data"), DUR=300;
const load=(s,t)=>JSON.parse(fs.readFileSync(path.join(DATA,`${s}_${t}.json`),"utf8"));
const R=[];
for(const sym of ["BTC","ETH"]){
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
    R.push({gap,ser,touch:ser.some(v=>v>=gap)});
  }
}
const n=R.length;
const out={n, lockWin:+(R.filter(r=>r.touch).length/n).toFixed(4), extended:[]};
for(const T of [0.005,0.01,0.02,0.03,0.05]){
  const hit=R.filter(r=>r.ser.some(v=>v>=r.gap+T)).length;
  out.extended.push({t:T, win:+(hit/n).toFixed(4)});
  console.log(`  lock+${T}% -> win ${(hit/n*100).toFixed(1)}%`);
}
console.log(`  lock (touch) -> win ${(out.lockWin*100).toFixed(1)}%   n=${n}`);
const f=path.join(__dirname,"out","locktouch.json"); const j=JSON.parse(fs.readFileSync(f,"utf8"));
j.exitTargets=out;
fs.writeFileSync(f,JSON.stringify(j,null,2));
console.log('Wrote exitTargets into out/locktouch.json');
