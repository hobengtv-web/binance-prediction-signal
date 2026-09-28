/* Sentuh lock dalam batas waktu N detik (barrier + time-limit). */
const fs=require("fs"), path=require("path");
const DATA=path.join(__dirname,"data"), dur=300;
const load=(s,t)=>JSON.parse(fs.readFileSync(path.join(DATA,`${s}_${t}.json`),"utf8"));
const pct=(a,p)=>a.length?a.slice().sort((x,y)=>x-y)[Math.min(a.length-1,Math.floor(p/100*(a.length-1)))]:0;
const rows=[];
for(const [sym,binSym] of [["BTC","BTCUSDT"],["ETH","ETHUSDT"]]){
  const ones=load(binSym,"1s"); const five=load(sym,"5m"); const om=new Map(ones.map(c=>[c.t,c]));
  const t0min=ones[0].t,t0max=ones[ones.length-1].t;
  for(const c of five){
    const t0=c.time; if(t0<t0min||t0+dur-1>t0max) continue;
    const c2=om.get(t0+1); if(!c2) continue;
    const lock=c.open,p2=c2.c,dPct=(p2-lock)/lock*100,dist=Math.abs(dPct);
    if(dist<1e-9) continue;
    const below=dPct<0;
    const touchWithin=(lim)=>{ for(let t=t0+2;t<=t0+lim;t++){ const x=om.get(t); if(!x)continue;
      if(below?x.h>=lock:x.l<=lock) return true; } return false; };
    rows.push({dist, t60:touchWithin(60), t120:touchWithin(120), t300:touchWithin(298)});
  }
}
console.log(`\n=== SENTUH LOCK dalam batas waktu (5m, n=${rows.length}) ===`);
console.log('  jarak       n        sentuh<=60s   <=120s   <=sesi   reward');
for(const [lo,hi] of [[0.003,0.01],[0.01,0.02],[0.02,0.035],[0.035,0.06]]){
  const g=rows.filter(r=>r.dist>=lo&&r.dist<=hi); if(g.length<30) continue;
  const a=g.filter(r=>r.t60).length, b=g.filter(r=>r.t120).length, c=g.filter(r=>r.t300).length;
  console.log(`  ${lo}-${hi}%  ${String(g.length).padStart(5)}    ${(a/g.length*100).toFixed(1)}%        ${(b/g.length*100).toFixed(1)}%    ${(c/g.length*100).toFixed(1)}%    ~${((lo+hi)/2).toFixed(3)}%`);
}
console.log('\n  kesimpulan: sentuh-lock dalam SESI ~80-95% untuk jarak kecil; dalam 60s ~55-80%.');
