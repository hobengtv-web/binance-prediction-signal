/* Sentuh-lock dikondisikan volume/surprise, pada jarak yang masih memberi reward layak. */
const fs=require("fs"), path=require("path");
const DATA=path.join(__dirname,"data");
const load=(s,t)=>JSON.parse(fs.readFileSync(path.join(DATA,`${s}_${t}.json`),"utf8"));
const pct=(a,p)=>a.length?a.slice().sort((x,y)=>x-y)[Math.min(a.length-1,Math.floor(p/100*(a.length-1)))]:0;
for(const tf of ["5m"]){
  const dur=300, rows=[];
  for(const [sym,binSym] of [["BTC","BTCUSDT"],["ETH","ETHUSDT"]]){
    const ones=load(binSym,"1s"); const five=load(sym,tf); const om=new Map(ones.map(c=>[c.t,c]));
    const idx=new Map(five.map((c,i)=>[c.time,i]));
    const t0min=ones[0].t,t0max=ones[ones.length-1].t;
    for(const c of five){
      const t0=c.time; if(t0<t0min||t0+dur-1>t0max) continue;
      const i=idx.get(t0); if(i==null||i<25) continue;
      const c1=om.get(t0),c2=om.get(t0+1); if(!c1||!c2) continue;
      const lock=c.open, p2=c2.c, dPct=(p2-lock)/lock*100, dist=Math.abs(dPct);
      if(dist<1e-9) continue;
      const below=dPct<0;
      let touchAt=-1,dd=0;
      for(let t=t0+2;t<t0+dur;t++){ const x=om.get(t); if(!x)continue;
        if(below){ if(x.h>=lock){touchAt=t;break;} dd=Math.max(dd,(lock-x.l)/lock*100); }
        else { if(x.l<=lock){touchAt=t;break;} dd=Math.max(dd,(x.h-lock)/lock*100); } }
      let rng=0,n=0; for(let k=t0-60;k<t0;k++){const cc=om.get(k); if(cc){rng+=(cc.h-cc.l);n++;}}
      const sigma=n?rng/n:0; const surprise=sigma>0?Math.abs(p2-lock)/sigma:0;
      const prev=five.slice(Math.max(0,i-25),i).map(x=>x.vol); const avgV=prev.length?prev.reduce((a,b)=>a+b,0)/prev.length:0;
      const volRel2=avgV>0?((c1.v+c2.v)*(dur/2))/avgV:1;
      rows.push({dist,touch:touchAt>=0,tsec:touchAt>=0?touchAt-(t0+2):null,dd,volRel2,surprise});
    }
  }
  const R=(f)=>{const g=rows.filter(f); const h=g.filter(r=>r.touch).length;
    if(g.length<25) return `n=${g.length} (sedikit)`;
    return `n=${String(g.length).padStart(4)} (${(g.length/(7*24)).toFixed(1)}/jam)  sentuh=${(h/g.length*100).toFixed(1)}%  reward-med=${pct(g.map(r=>r.dist),50).toFixed(3)}%  dd-med=${pct(g.map(r=>r.dd),50).toFixed(3)}%  t-med=${pct(g.filter(r=>r.touch).map(r=>r.tsec),50)}s`;};
  console.log(`\n=== ${tf}: SENTUH LOCK (jarak 0.005-0.02%, buang sentuh<5s) ===`);
  console.log('  tanpa filter          :', R(r=>r.dist>=0.005&&r.dist<=0.02&&!(r.touch&&r.tsec<5)));
  console.log('  volRel2>=0.9          :', R(r=>r.dist>=0.005&&r.dist<=0.02&&!(r.touch&&r.tsec<5)&&r.volRel2>=0.9));
  console.log('  volRel2>=1.5          :', R(r=>r.dist>=0.005&&r.dist<=0.02&&!(r.touch&&r.tsec<5)&&r.volRel2>=1.5));
  console.log('  surprise>=2           :', R(r=>r.dist>=0.005&&r.dist<=0.02&&!(r.touch&&r.tsec<5)&&r.surprise>=2));
  console.log('  surprise<2 (tenang)   :', R(r=>r.dist>=0.005&&r.dist<=0.02&&!(r.touch&&r.tsec<5)&&r.surprise<2));
  console.log('  vol>=1.5 & surprise>=2:', R(r=>r.dist>=0.005&&r.dist<=0.02&&!(r.touch&&r.tsec<5)&&r.volRel2>=1.5&&r.surprise>=2));
}
