/* Perhalus strategi "sentuh lock": buang kasus degenerate (jarak ~0 & sentuh instan),
   ukur rate, waktu, drawdown, reward, dan expectancy per trade. */
const fs=require("fs"), path=require("path");
const DATA=path.join(__dirname,"data");
const load=(s,t)=>JSON.parse(fs.readFileSync(path.join(DATA,`${s}_${t}.json`),"utf8"));
const pct=(a,p)=>a.length?a.slice().sort((x,y)=>x-y)[Math.min(a.length-1,Math.floor(p/100*(a.length-1)))]:0;
for(const tf of ["5m","15m"]){
  const dur=tf==="5m"?300:900;
  const rows=[];
  for(const [sym,binSym] of [["BTC","BTCUSDT"],["ETH","ETHUSDT"]]){
    const ones=load(binSym,"1s"); const five=load(sym,tf); const om=new Map(ones.map(c=>[c.t,c]));
    const t0min=ones[0].t,t0max=ones[ones.length-1].t;
    for(const c of five){
      const t0=c.time; if(t0<t0min||t0+dur-1>t0max) continue;
      const c2=om.get(t0+1); if(!c2) continue;
      const lock=c.open, p2=c2.c, dPct=(p2-lock)/lock*100, dist=Math.abs(dPct);
      if(dist<1e-9) continue;
      const below=dPct<0;
      let touchAt=-1,dd=0;
      for(let t=t0+2;t<t0+dur;t++){ const x=om.get(t); if(!x)continue;
        if(below){ if(x.h>=lock){touchAt=t;break;} dd=Math.max(dd,(lock-x.l)/lock*100); }
        else { if(x.l<=lock){touchAt=t;break;} dd=Math.max(dd,(x.h-lock)/lock*100); } }
      rows.push({dist,touch:touchAt>=0,tsec:touchAt>=0?touchAt-(t0+2):null,dd});
    }
  }
  console.log(`\n=== ${tf}: strategi SENTUH LOCK (perhalus) ===`);
  const sel=[["dist 0.003-0.02%",r=>r.dist>=0.003&&r.dist<=0.02],
             ["dist 0.003-0.02% & buang sentuh<5s",r=>r.dist>=0.003&&r.dist<=0.02&&!(r.touch&&r.tsec<5)],
             ["dist 0.005-0.03% & buang sentuh<10s",r=>r.dist>=0.005&&r.dist<=0.03&&!(r.touch&&r.tsec<10)],
             ["dist 0.01-0.05% & buang sentuh<10s",r=>r.dist>=0.01&&r.dist<=0.05&&!(r.touch&&r.tsec<10)]];
  for(const [name,f] of sel){
    const g=rows.filter(f); if(g.length<30){console.log(`  ${name}: n=${g.length} (terlalu sedikit)`);continue;}
    const h=g.filter(r=>r.touch).length, rate=h/g.length;
    const rew=pct(g.map(r=>r.dist),50), dd=pct(g.map(r=>r.dd),50);
    // expectancy: menang = reward (jarak), kalah = drawdown pada yang gagal (exit akhir sesi)
    const fail=g.filter(r=>!r.touch); const failLoss=fail.length?(fail.reduce((a,r)=>a+r.dd,0)/fail.length):0;
    const exp=rate*rew - (1-rate)*failLoss;
    console.log(`  ${name.padEnd(32)} n=${String(g.length).padStart(4)} (${(g.length/(7*24)).toFixed(1)}/jam)  sentuh=${(rate*100).toFixed(1)}%  reward-med=${rew.toFixed(3)}%  dd-med=${dd.toFixed(3)}%  t-med=${pct(g.filter(r=>r.touch).map(r=>r.tsec),50)}s  EKSPEKTANSI/trade=${exp>=0?'+':''}${exp.toFixed(3)}%`);
  }
}
