/* Round 2: fitur BARU di detik ke-2 — gap sesi, volatilitas 1 menit, cross-asset BTC->ETH,
   likuiditas relatif, jam, plus kombinasi. Target: arah sesi 5m (close vs lock). */
const fs=require("fs"), path=require("path");
const DATA=path.join(__dirname,"data");
const DUR=300;
const load=(s,t)=>JSON.parse(fs.readFileSync(path.join(DATA,`${s}_${t}.json`),"utf8"));

const ones={BTC:load("BTCUSDT","1s"),ETH:load("ETHUSDT","1s")};
const five={BTC:load("BTC","5m"),ETH:load("ETH","5m")};
const omap={}, idxs={};
for(const s of ["BTC","ETH"]){ omap[s]=new Map(ones[s].map(c=>[c.t,c])); idxs[s]=new Map(five[s].map((c,i)=>[c.time,i])); }

function featAt(sym,t0,i){
  const c1=omap[sym].get(t0), c2=omap[sym].get(t0+1);
  if(!c1||!c2) return null;
  const lock=five[sym][i].open;
  const dir2=c2.c>lock?1:c2.c<lock?0:-1; if(dir2<0) return null;
  const mv2=Math.abs(c2.c-lock)/lock*100;
  // volatilitas 1s 60 detik terakhir (σ range)
  let rng=0,n=0; for(let k=t0-60;k<t0;k++){ const c=omap[sym].get(k); if(c){rng+=(c.h-c.l);n++;} }
  const sig1s=n?rng/n:0;
  const surprise=sig1s>0?(Math.abs(c2.c-lock)/sig1s):0;
  const prevClose=five[sym][i-1]?five[sym][i-1].close:null;
  const gap=prevClose!=null?(lock-prevClose)/prevClose*100:null;
  const vol2=(c1.v+c2.v)*(DUR/2);
  const prev=five[sym].slice(Math.max(0,i-25),i).map(x=>x.vol);
  const avgV=prev.length?prev.reduce((a,b)=>a+b,0)/prev.length:0;
  const volRel2=avgV>0?vol2/avgV:1;
  return {sym,t0,lock,dir2,mv2,surprise,gap,volRel2,sig1s,outcome:five[sym][i].close>=lock?1:0,hour:new Date(t0*1000).getUTCHours()};
}
const rows=[];
for(const sym of ["BTC","ETH"]){
  for(const c of five[sym]){
    const i=idxs[sym].get(c.time); if(i==null||i<50) continue;
    const t0=c.time; if(t0<ones[sym][0].t||t0+DUR-1>ones[sym][ones[sym].length-1].t) continue;
    const f=featAt(sym,t0,i); if(f) rows.push(f);
  }
}
rows.sort((a,b)=>a.t0-b.t0);
// cross-asset: untuk ETH, arah BTC pada detik yang sama
const btcAt=new Map(rows.filter(r=>r.sym==="BTC").map(r=>[r.t0,r.dir2]));
for(const r of rows){ r.btc2 = r.sym==="ETH" ? (btcAt.has(r.t0)?btcAt.get(r.t0):-1) : r.dir2; }

const S=rows.filter(r=>r.dir2!==-1);
const cut=Math.floor(S.length*0.7), TR=S.slice(0,cut), TE=S.slice(cut);
const accOf=(a,f)=>a.length?(a.filter(f).length/a.length*100).toFixed(1):'—';
console.log(`\n=== ROUND 2 — fitur baru (n=${S.length}, 7 hari) ===`);
console.log(`  baseline dir@2s                 : ${accOf(S,r=>r.dir2===r.outcome)}%`);
// fitur tunggal (arah)
const dirF={ 'btc2 (cross-asset)':r=>r.btc2, 'gap searah':r=>r.gap==null?-1:(r.gap>0?1:0) };
for(const [n,f] of Object.entries(dirF)){ const sub=S.filter(r=>f(r)===0||f(r)===1); console.log(`  ${n.padEnd(28)}: ${sub.length?(sub.filter(r=>f(r)===r.outcome).length/sub.length*100).toFixed(1):'—'}% (n=${sub.length})`); }
// filter
for(const t of [1.0,1.5,2.0,3.0]){ const sub=S.filter(r=>r.surprise>=t); console.log(`  dir2 & surprise>=${t} (=mv/σ1s): ${sub.length?(sub.filter(r=>r.dir2===r.outcome).length/sub.length*100).toFixed(1):'—'}% (n=${sub.length})`); }
for(const t of [0.02,0.05,0.1]){ const sub=S.filter(r=>r.gap!=null&&Math.abs(r.gap)>=t); console.log(`  dir2 & |gap|>=${t}%           : ${sub.length?(sub.filter(r=>r.dir2===r.outcome).length/sub.length*100).toFixed(1):'—'}% (n=${sub.length})`); }
const eth=S.filter(r=>r.sym==="ETH"&&r.btc2!==-1);
console.log(`  ETH & BTC searah (cross)     : ${eth.length?(eth.filter(r=>r.dir2===r.btc2&&r.dir2===r.outcome).length/eth.length*100).toFixed(1):'—'}% (n=${eth.length})`);
console.log(`  ETH & BTC berlawanan          : ${eth.length?(eth.filter(r=>r.dir2!==r.btc2&&r.dir2===r.outcome).length/eth.length*100).toFixed(1):'—'}% (n=${eth.length})`);
// kombinasi train/test
const C={
  'vol>=1.5': r=>r.volRel2>=1.5,
  'vol>=2': r=>r.volRel2>=2,
  'surprise>=1.5': r=>r.surprise>=1.5,
  'surprise>=2': r=>r.surprise>=2,
  'mv>=0.02': r=>r.mv2>=0.02,
  'hour0-7': r=>r.hour<8,
  'hour8-15': r=>r.hour>=8&&r.hour<16,
};
const names=Object.keys(C); const out=[];
function ev(ch){ const c=r=>ch.every(n=>C[n](r)); const a=TR.filter(c),b=TE.filter(c);
  if(a.length<120||b.length<60) return;
  out.push({c:ch.join('+'),trN:a.length,tr:(a.filter(r=>r.dir2===r.outcome).length/a.length*100),teN:b.length,te:(b.filter(r=>r.dir2===r.outcome).length/b.length*100)}); }
for(let i=0;i<names.length;i++){ ev([names[i]]); for(let j=i+1;j<names.length;j++){ ev([names[i],names[j]]); for(let k=j+1;k<names.length;k++){ if(names[k].startsWith('hour')&&(names[i].startsWith('hour')||names[j].startsWith('hour')))continue; ev([names[i],names[j],names[k]]); } } }
out.sort((x,y)=>y.te-x.te);
console.log(`\n  TOP kombinasi (train/test):`);
for(const o of out.slice(0,10)) console.log(`   test ${o.te.toFixed(1)}% (n=${o.teN})  train ${o.tr.toFixed(1)}% (n=${o.trN})  ${o.c}`);
