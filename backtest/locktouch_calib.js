/* Kalibrasi probabilitas "SENTUH LOCK" per bucket jarak (2s). Output: out/locktouch.json */
const fs=require("fs"), path=require("path");
const DATA=path.join(__dirname,"data");
const load=(s,t)=>JSON.parse(fs.readFileSync(path.join(DATA,`${s}_${t}.json`),"utf8"));
const pct=(a,p)=>a.length?a.slice().sort((x,y)=>x-y)[Math.min(a.length-1,Math.floor(p/100*(a.length-1)))]:0;
const EDGES=[0,0.005,0.01,0.02,0.035,0.06,0.1,1e9];
const OUT={};
for(const tf of ["5m","15m"]){
  const dur=tf==="5m"?300:900; const rows=[];
  for(const [sym,binSym] of [["BTC","BTCUSDT"],["ETH","ETHUSDT"]]){
    const ones=load(binSym,"1s"); const five=load(sym,tf); const om=new Map(ones.map(c=>[c.t,c]));
    const t0min=ones[0].t,t0max=ones[ones.length-1].t;
    for(const c of five){
      const t0=c.time; if(t0<t0min||t0+dur-1>t0max) continue;
      const c2=om.get(t0+1); if(!c2) continue;
      const lock=c.open,p2=c2.c,dPct=(p2-lock)/lock*100,dist=Math.abs(dPct);
      if(dist<1e-9) continue;
      const below=dPct<0;
      let touchAt=-1,dd=0;
      for(let t=t0+2;t<t0+dur;t++){ const x=om.get(t); if(!x)continue;
        if(below){ if(x.h>=lock){touchAt=t;break;} dd=Math.max(dd,(lock-x.l)/lock*100); }
        else { if(x.l<=lock){touchAt=t;break;} dd=Math.max(dd,(x.h-lock)/lock*100); } }
      rows.push({dist,touch:touchAt>=0,tsec:touchAt>=0?touchAt-(t0+2):null,dd});
    }
  }
  OUT[tf]={ buckets:[] };
  for(let i=0;i<EDGES.length-1;i++){
    const lo=EDGES[i], hi=EDGES[i+1];
    const g=rows.filter(r=>r.dist>=lo&&r.dist<hi); if(g.length<25) continue;
    const h=g.filter(r=>r.touch).length;
    OUT[tf].buckets.push({
      lo, hi:hi>1e8?null:hi, n:g.length, rate:+(h/g.length).toFixed(4),
      tMed: pct(g.filter(r=>r.touch).map(r=>r.tsec),50), ddMed:+pct(g.map(r=>r.dd),50).toFixed(4),
      perHour:+(g.length/(7*24)).toFixed(2),
    });
  }
  OUT[tf].total=+((rows.filter(r=>r.touch).length/rows.length)).toFixed(4);
  OUT[tf].n=rows.length;
}
fs.writeFileSync(path.join(__dirname,"out","locktouch.json"),JSON.stringify({generated:new Date().toISOString(),windowDays:7,tiers:OUT},null,2));
for(const tf of ["5m","15m"]){ console.log(`\n${tf}: total sentuh ${(OUT[tf].total*100).toFixed(1)}% (n=${OUT[tf].n})`);
  for(const b of OUT[tf].buckets) console.log(`  ${b.lo}-${b.hi==null?'∞':b.hi}% : rate ${(b.rate*100).toFixed(1)}%  n=${b.n} (${b.perHour}/jam)  t-med ${b.tMed}s  dd-med ${b.ddMed}%`); }
console.log('\nWrote out/locktouch.json');
