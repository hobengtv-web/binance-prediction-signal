/* STRATEGI BARU: target = "harga menyentuh kembali lock" (bukan arah sesi).
   Sinyal di detik ke-2: harga sedikit contra lock -> ukur peluang kembali menyentuh lock.
   Ini persis trade user (entry saat contra, jual di lock) tapi target-nya barrier, bukan arah.
   Usage: node backtest/locktouch_test.js */
const fs=require("fs"), path=require("path");
const DATA=path.join(__dirname,"data"), DUR=300;
const load=(s,t)=>JSON.parse(fs.readFileSync(path.join(DATA,`${s}_${t}.json`),"utf8"));
const pct=(a,p)=>a.length?a.slice().sort((x,y)=>x-y)[Math.min(a.length-1,Math.floor(p/100*(a.length-1)))]:0;

for(const tf of ["5m","15m"]){
  const dur=tf==="5m"?300:900;
  const rows=[];
  for(const [sym,binSym] of [["BTC","BTCUSDT"],["ETH","ETHUSDT"]]){
    const ones=load(binSym,"1s"); const five=load(sym,tf);
    const om=new Map(ones.map(c=>[c.t,c]));
    const t0min=ones[0].t,t0max=ones[ones.length-1].t;
    for(const c of five){
      const t0=c.time; if(t0<t0min||t0+dur-1>t0max) continue;
      const c2=om.get(t0+1); if(!c2) continue;
      const lock=c.open;
      const p2=c2.c;
      const dPct=(p2-lock)/lock*100;            // signed; >0 = di atas lock, <0 = di bawah
      if(Math.abs(dPct)<1e-9) continue;          // tepat di lock -> tidak ada trade
      const isBelow=dPct<0;
      // apakah menyentuh lock lagi setelah detik ke-2?
      let touchAt=-1, dd=0;
      for(let t=t0+2;t<t0+dur;t++){
        const x=om.get(t); if(!x) continue;
        if(isBelow) { if(x.h>=lock){touchAt=t;break;} dd=Math.max(dd,(lock-x.l)/lock*100); }
        else        { if(x.l<=lock){touchAt=t;break;} dd=Math.max(dd,(x.h-lock)/lock*100); }
      }
      rows.push({sym,dist:Math.abs(dPct),touch:touchAt>=0,tsec:touchAt>=0?(touchAt-(t0+2)):null,dd});
    }
  }
  const n=rows.length; const hit=rows.filter(r=>r.touch).length;
  console.log(`\n=== ${tf}: target = SENTUH LOCK LAGI (n=${n}) ===`);
  console.log(`  total berhasil: ${(hit/n*100).toFixed(1)}%`);
  const B=[0.005,0.01,0.02,0.03,0.05,0.08,0.12,0.2,0.5];
  console.log('  jarak@2s      n      %hari   sentuh-lock   reward   drawdown-med   detik-med');
  let lo=0;
  for(const hi of B){
    const g=rows.filter(r=>r.dist>=lo&&r.dist<hi); lo=hi;
    if(g.length<40) continue;
    const h=g.filter(r=>r.touch).length;
    console.log(`  ${(lo===B[0]?'':'').padStart(0)}< ${hi}%`.padEnd(13)+String(g.length).padStart(6)+String((g.length/(7*24)).toFixed(1)).padStart(7)+`   ${(h/g.length*100).toFixed(1)}%`.padEnd(16)+`${((lo+ (hi-lo)/2)).toFixed(3)}%`.padEnd(10)+`${pct(g.map(r=>r.dd),50).toFixed(3)}%`.padEnd(8)+`${pct(g.filter(r=>r.touch).map(r=>r.tsec),50)}`);
  }
  // kumulatif: hanya ambil jarak <= X
  console.log('  -- kumulatif (jarak <= X) --');
  for(const X of [0.01,0.02,0.03,0.05,0.1]){
    const g=rows.filter(r=>r.dist<=X); const h=g.filter(r=>r.touch).length;
    if(g.length<40) continue;
    console.log(`   jarak<=${X}%  n=${g.length} (${(g.length/(7*24)).toFixed(1)}/jam)  sentuh=${(h/g.length*100).toFixed(1)}%  reward~${X}%  dd-med=${pct(g.map(r=>r.dd),50).toFixed(3)}%`);
  }
}
