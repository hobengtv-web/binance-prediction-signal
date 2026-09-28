/* Uji strategi user: bias sesi -> tunggu harga contra -> entry di PEAK contra (saat gerak lawan
   berhenti) -> jual saat harga kembali menyentuh lock. Ukur: berapa % yang berhasil, reward,
   drawdown, dan waktu.
   Usage: node backtest/strategy_test.js */
const fs=require("fs"), path=require("path");
const MS={"5m":300,"15m":900};
const load=(s,t)=>JSON.parse(fs.readFileSync(path.join(__dirname,"data",`${s}_${t}.json`),"utf8"));
const pct=(a,p)=>a.length?a.slice().sort((x,y)=>x-y)[Math.min(a.length-1,Math.floor(p/100*(a.length-1)))]:0;

for(const tf of ["5m","15m"]){
  const step=MS[tf]/60;
  const rows=[];
  for(const sym of ["BTC","ETH"]){
    const one=load(sym,"1m"), tfC=load(sym,tf);
    const idx=new Map(one.map((c,i)=>[c.time,i]));
    for(const c of tfC){
      const i0=idx.get(c.time); if(i0==null) continue;
      const m=[]; for(let k=0;k<step;k++){const x=one[i0+k]; if(!x) break; m.push(x);} 
      if(m.length!==step) continue;
      const lock=c.open;
      // bias = arah menit pertama (proxy sinyal awal)
      const bias = m[0].close>lock ? "up" : m[0].close<lock ? "down" : null;
      if(!bias) continue;
      const isUp = bias==="up";
      // 1) excursion contra pertama
      let dip=-1;
      for(let j=1;j<m.length;j++){ if(isUp? m[j].close<lock : m[j].close>lock){ dip=j; break; } }
      if(dip<0) continue;                       // tidak pernah contra -> tidak ada setup
      // 2) peak contra = menit pertama setelah dip di mana gerak lawan BERHENTI (close berbalik)
      let entryJ=-1;
      for(let j=dip+1;j<m.length;j++){
        const stop = isUp ? (m[j].close>m[j-1].close) : (m[j].close<m[j-1].close);
        if(stop){ entryJ=j; break; }
      }
      if(entryJ<0) continue;                    // tidak berbalik sampai sesi habis
      const entryPrice=m[entryJ].close;
      const rewardPct=Math.abs(lock-entryPrice)/entryPrice*100;
      const entryMin=entryJ;
      // 3) apakah harga kembali menyentuh lock sebelum sesi berakhir?
      let hitJ=-1;
      for(let j=entryJ;j<m.length;j++){ if(isUp? m[j].high>=lock : m[j].low<=lock){ hitJ=j; break; } }
      // 4) drawdown setelah entry (excursion lawan maksimum)
      let dd=0;
      for(let j=entryJ;j<(hitJ>=0?hitJ+1:m.length);j++){
        const adv = isUp ? (lock-m[j].low) : (m[j].high-lock);
        dd=Math.max(dd, adv/lock*100);
      }
      rows.push({sym,tf,entryMin,rewardPct,hit:hitJ>=0,timeToLock:hitJ>=0?(hitJ-entryJ):null,dd});
    }
  }
  const n=rows.length, hit=rows.filter(r=>r.hit).length;
  const rew=rows.filter(r=>r.hit).map(r=>r.rewardPct);
  console.log(`\n=== ${tf}: strategi (entry di peak contra -> jual di lock) n=${n} ===`);
  console.log(`  BERHASIL menyentuh lock: ${hit}/${n} = ${(hit/n*100).toFixed(1)}%`);
  console.log(`  reward saat entry (semua): median ${pct(rows.map(r=>r.rewardPct),50).toFixed(3)}%  p75 ${pct(rows.map(r=>r.rewardPct),75).toFixed(3)}%`);
  console.log(`  reward pada yang berhasil: median ${pct(rew,50).toFixed(3)}%  mean ${(rew.reduce((a,b)=>a+b,0)/(rew.length||1)).toFixed(3)}%`);
  console.log(`  drawdown setelah entry: median ${pct(rows.map(r=>r.dd),50).toFixed(3)}%  p75 ${pct(rows.map(r=>r.dd),75).toFixed(3)}%`);
  console.log(`  waktu ke lock (menit): median ${pct(rows.filter(r=>r.hit).map(r=>r.timeToLock),50)}  p75 ${pct(rows.filter(r=>r.hit).map(r=>r.timeToLock),75)}`);
  // versi lebih selektif: hanya entry jika reward >= 0.05% / 0.10%
  for(const minR of [0.05,0.10,0.20]){
    const sub=rows.filter(r=>r.rewardPct>=minR); const sh=sub.filter(r=>r.hit).length;
    if(sub.length>30) console.log(`  [reward>=${minR}%] n=${sub.length} berhasil=${(sh/sub.length*100).toFixed(1)}%  reward-median-berhasil=${pct(sub.filter(r=>r.hit).map(r=>r.rewardPct),50).toFixed(3)}%`);
  }
}

// ===== tambahan: ekspektansi P&L (entry di turn, jual di lock; jika tak tercapai keluar di akhir sesi) =====
console.log("\n=== EKSPEKTANSI (entry di peak contra) ===");
for(const tf of ["5m","15m"]){
  const step=MS[tf]/60; let n=0,hit=0,sumWin=0,sumLoss=0,nLoss=0,worst=0;
  for(const sym of ["BTC","ETH"]){
    const one=load(sym,"1m"), tfC=load(sym,tf); const idx=new Map(one.map((c,i)=>[c.time,i]));
    for(const c of tfC){
      const i0=idx.get(c.time); if(i0==null) continue;
      const m=[]; for(let k=0;k<step;k++){const x=one[i0+k]; if(!x)break; m.push(x);} if(m.length!==step)continue;
      const lock=c.open; const bias=m[0].close>lock?"up":m[0].close<lock?"down":null; if(!bias)continue;
      const isUp=bias==="up";
      let dip=-1; for(let j=1;j<m.length;j++){ if(isUp?m[j].close<lock:m[j].close>lock){dip=j;break;} } if(dip<0)continue;
      let eJ=-1; for(let j=dip+1;j<m.length;j++){ const stop=isUp?(m[j].close>m[j-1].close):(m[j].close<m[j-1].close); if(stop){eJ=j;break;} } if(eJ<0)continue;
      const entry=m[eJ].close; let hitJ=-1;
      for(let j=eJ;j<m.length;j++){ if(isUp?m[j].high>=lock:m[j].low<=lock){hitJ=j;break;} }
      const pnl = hitJ>=0 ? Math.abs(lock-entry)/entry*100 : ((isUp?(m[m.length-1].close-entry):(entry-m[m.length-1].close))/entry*100);
      n++; if(hitJ>=0){hit++; sumWin+=pnl;} else {nLoss++; sumLoss+=pnl; worst=Math.min(worst,pnl);}
    }
  }
  const exp=((sumWin+sumLoss)/n);
  console.log(`  ${tf}: n=${n} hit=${(hit/n*100).toFixed(1)}%  avg menang=+${(sumWin/(hit||1)).toFixed(3)}%  avg kalah=${(sumLoss/(nLoss||1)).toFixed(3)}%  worst=${worst.toFixed(2)}%  EKSPEKTANSI/trade=${exp>=0?"+":""}${exp.toFixed(3)}%`);
}
