/* Sweep kombinasi kriteria untuk sinyal di DETIK KE-2 sesi.
   Target: arah sesi (close vs lock). Fitur dari klines 1s (detik 1-2) + konteks sesi sebelumnya.
   Usage: node backtest/early2s_sweep.js [tf]  (default 5m) */
const fs=require("fs"), path=require("path");
const DATA=path.join(__dirname,"data");
const TF=process.argv[2]||"5m";
const DUR=TF==="5m"?300:TF==="15m"?900:3600;
const load=(s,t)=>JSON.parse(fs.readFileSync(path.join(DATA,`${s}_${t}.json`),"utf8"));

function buildSessions(){
  const rows=[];
  for(const [sym,binSym] of [["BTC","BTCUSDT"],["ETH","ETHUSDT"]]){
    const ones=load(binSym,"1s"); const five=load(sym,"5m");
    const om=new Map(ones.map(c=>[c.t,c]));
    const idx=new Map(five.map((c,i)=>[c.time,i]));
    const t0min=ones[0].t, t0max=ones[ones.length-1].t;
    for(const c of five){
      const t0=c.time; if(t0<t0min||t0+DUR-1>t0max) continue;
      const i=idx.get(t0); if(i==null||i<50) continue;
      const c1=om.get(t0), c2=om.get(t0+1);
      if(!c1||!c2) continue;
      const lock=c.open;
      const px2=c2.c;                     // harga pada detik ke-2
      const dir2 = px2>lock?1:px2<lock?0:-1;
      if(dir2<0) continue;
      const mv2=Math.abs(px2-lock)/lock*100;
      const prev=five.slice(Math.max(0,i-25),i).map(x=>x.vol);
      const avgV=prev.length?prev.reduce((a,b)=>a+b,0)/prev.length:0;
      const vol2=(c1.v+c2.v)*(DUR/2);
      const volRel2=avgV>0?vol2/avgV:1;
      const outcome=c.close>=lock?1:0;
      // konteks dari sesi sebelumnya (tanpa lookahead)
      const priorS=five.slice(Math.max(0,i-50),i);
      let bull=0,bear=0; const tw=priorS.length*(priorS.length+1)/2;
      priorS.forEach((x,k)=>{const d=x.close-x.open;const w=(k+1)/tw; if(d>0)bull+=w; else if(d<0)bear+=w;});
      const histDir=bull>bear?1:bear>bull?0:-1;
      const histStr=Math.round(Math.abs(bull-bear)*100);
      const last3=priorS.slice(-3);
      let u3=0,d3=0; last3.forEach(x=>{if(x.close>x.open)u3++;else if(x.close<x.open)d3++;});
      const sessDir3=u3>d3?1:d3>u3?0:-1;   // proxy 15m
      const lastS=priorS[priorS.length-1];
      const prevDir=lastS?(lastS.close>lastS.open?1:0):-1;
      const hour=new Date(t0*1000).getUTCHours();
      // lintasan untuk simulasi trade (1m dari 1s): recapture lock setelah contra
      const mins=[]; for(let k=0;k<DUR;k+=60){ const cc=om.get(t0+k); if(cc) mins.push({o:cc.o,h:cc.h,l:cc.l,c:cc.c}); }
      rows.push({sym,t0,lock,outcome,dir2,mv2,volRel2,histDir,histStr,sessDir3,prevDir,hour,mins});
    }
  }
  rows.sort((a,b)=>a.t0-b.t0);
  return rows;
}
const acc=(a)=>a.length?(a.reduce((x,r)=>x+r.ok,0)/a.length):0;
const pct=(a)=>a.length?(a.map(r=>r.ok?'W':'L').filter(x=>x==='W').length/a.length*100).toFixed(1)+'%':'—';
function lockRate(rows, pick){ // simulasi: entry di turn contra pertama -> apakah menyentuh lock
  let n=0,hit=0;
  for(const r of rows){ if(!pick(r)) continue; const isUp=r.dir2===1; const m=r.mins;
    let dip=-1; for(let j=1;j<m.length;j++){ if(isUp?m[j].c<r.lock:m[j].c>r.lock){dip=j;break;} } if(dip<0)continue;
    let eJ=-1; for(let j=dip+1;j<m.length;j++){ const stop=isUp?(m[j].c>m[j-1].c):(m[j].c<m[j-1].c); if(stop){eJ=j;break;} } if(eJ<0)continue;
    let h=false; for(let j=eJ;j<m.length;j++){ if(isUp?m[j].h>=r.lock:m[j].l<=r.lock){h=true;break;} }
    n++; if(h)hit++;
  }
  return n?`${(hit/n*100).toFixed(1)}% (n=${n})`:'—';
}

const S=buildSessions();
const cut=Math.floor(S.length*0.7), TR=S.slice(0,cut), TE=S.slice(cut);
const F={
  'dir2': r=>r.dir2,
  'histDir50': r=>r.histDir,
  'sessDir3(15m proxy)': r=>r.sessDir3,
  'prevSession': r=>r.prevDir,
};
console.log(`\n=== ${TF}: ${S.length} sesi (7 hari). Target: arah sesi (close vs lock) ===`);
const base=S.filter(r=>r.dir2===r.outcome).length/S.length*100;
console.log(`  BASELINE dir@2s            : ${base.toFixed(1)}% (n=${S.length})`);

// 1) prediktor arah tunggal + kesepakatan dengan dir2
console.log(`\n-- prediktor arah (akurasi memprediksi hasil sesi) --`);
for(const [name,fn] of Object.entries(F)){
  const sub=S.filter(r=>fn(r)===0||fn(r)===1);
  const ok=sub.filter(r=>fn(r)===r.outcome).length;
  console.log(`  ${name.padEnd(22)}: ${(ok/sub.length*100).toFixed(1)}% (n=${sub.length})`);
}

// 2) filter volume & magnitudo pada dir2
console.log(`\n-- dir2 + filter (semua data) --`);
for(const t of [0.6,0.9,1.2,1.5,2,3]){
  const sub=S.filter(r=>r.volRel2>=t); const ok=sub.filter(r=>r.dir2===r.outcome).length;
  console.log(`  dir2 & volRel2>=${t}      : ${sub.length?(ok/sub.length*100).toFixed(1):'—'}% (n=${sub.length})  lock-recapture: ${lockRate(S,r=>r.volRel2>=t)}`);
}
for(const m of [0.02,0.05,0.08,0.12]){
  const sub=S.filter(r=>r.mv2>=m); const ok=sub.filter(r=>r.dir2===r.outcome).length;
  console.log(`  dir2 & |mv2|>=${m}%       : ${sub.length?(ok/sub.length*100).toFixed(1):'—'}% (n=${sub.length})  lock-recapture: ${lockRate(S,r=>r.mv2>=m)}`);
}

// 3) kombinasi (train/test) : dir2 + hist/prev/sess + volume + magnitudo
const conds={
  'histAgree': r=>r.histDir===r.dir2,
  'histAgree&str>=35': r=>r.histDir===r.dir2&&r.histStr>=35,
  'sess3Agree': r=>r.sessDir3===r.dir2,
  'prevAgree': r=>r.prevDir===r.dir2,
  'vol>=0.9': r=>r.volRel2>=0.9,
  'vol>=1.2': r=>r.volRel2>=1.2,
  'mv>=0.05': r=>r.mv2>=0.05,
  'mv>=0.08': r=>r.mv2>=0.08,
  'hour0-7': r=>r.hour<8,
  'hour8-15': r=>r.hour>=8&&r.hour<16,
  'hour16-23': r=>r.hour>=16,
};
const names=Object.keys(conds); const out=[];
function ev(chosen){
  const c=r=>chosen.every(n=>conds[n](r));
  const a=TR.filter(c), b=TE.filter(c);
  if(a.length<150||b.length<80) return;
  const oka=a.filter(r=>r.dir2===r.outcome).length, okb=b.filter(r=>r.dir2===r.outcome).length;
  out.push({c:chosen.join('+'),trN:a.length,trAcc:oka/a.length*100,teN:b.length,teAcc:okb/b.length*100,lock:lockRate(TE,c)});
}
for(let i=0;i<names.length;i++){ ev([names[i]]); for(let j=i+1;j<names.length;j++){ ev([names[i],names[j]]); for(let k=j+1;k<names.length;k++){ if(names[k].startsWith('hour')&&(names[i].startsWith('hour')||names[j].startsWith('hour')))continue; ev([names[i],names[j],names[k]]); } } }
out.sort((x,y)=>y.teAcc-x.teAcc);
console.log(`\n-- TOP kombinasi (train 70% / test 30%) --`);
console.log('  testAcc  testN   trainAcc  trainN   lock-recapture(test)   kondisi');
for(const o of out.slice(0,15)) console.log(`   ${o.teAcc.toFixed(1)}%   ${String(o.teN).padStart(5)}    ${o.trAcc.toFixed(1)}%  ${String(o.trN).padStart(5)}    ${String(o.lock).padEnd(18)} ${o.c}`);
