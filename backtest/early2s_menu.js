/* Menu opsi kriteria untuk sinyal 2 detik: winrate arah + lock-recapture + frekuensi/hari. */
const fs=require("fs"), path=require("path");
const DATA=path.join(__dirname,"data"), DUR=300;
const load=(s,t)=>JSON.parse(fs.readFileSync(path.join(DATA,`${s}_${t}.json`),"utf8"));
const ones={BTC:load("BTCUSDT","1s"),ETH:load("ETHUSDT","1s")};
const five={BTC:load("BTC","5m"),ETH:load("ETH","5m")};
const omap={},idxs={};
for(const s of ["BTC","ETH"]){omap[s]=new Map(ones[s].map(c=>[c.t,c]));idxs[s]=new Map(five[s].map((c,i)=>[c.time,i]));}
const R=[];
for(const sym of ["BTC","ETH"]){
  for(const c of five[sym]){
    const i=idxs[sym].get(c.time); if(i==null||i<50)continue;
    const t0=c.time; if(t0<ones[sym][0].t||t0+DUR-1>ones[sym][ones[sym].length-1].t)continue;
    const c1=omap[sym].get(t0),c2=omap[sym].get(t0+1); if(!c1||!c2)continue;
    const lock=c.open; const dir2=c2.c>lock?1:c2.c<lock?0:-1; if(dir2<0)continue;
    const mv2=Math.abs(c2.c-lock)/lock*100;
    let rng=0,n=0; for(let k=t0-60;k<t0;k++){const cc=omap[sym].get(k); if(cc){rng+=(cc.h-cc.l);n++;}}
    const sig1s=n?rng/n:0; const surprise=sig1s>0?Math.abs(c2.c-lock)/sig1s:0;
    const prev=five[sym].slice(Math.max(0,i-25),i).map(x=>x.vol);
    const avgV=prev.length?prev.reduce((a,b)=>a+b,0)/prev.length:0;
    const volRel2=avgV>0?((c1.v+c2.v)*(DUR/2))/avgV:1;
    const mins=[]; for(let k=0;k<DUR;k+=60){const cc=omap[sym].get(t0+k); if(cc)mins.push({o:cc.o,h:cc.h,l:cc.l,c:cc.c});}
    R.push({sym,t0,lock,dir2,mv2,surprise,volRel2,outcome:c.close>=lock?1:0,mins});
  }
}
R.sort((a,b)=>a.t0-b.t0);
const days=(R[R.length-1].t0-R[0].t0)/86400;
const cut=Math.floor(R.length*0.7), TR=R.slice(0,cut), TE=R.slice(cut);
const wr=a=>a.length?(a.filter(r=>r.dir2===r.outcome).length/a.length*100):0;
function lockRate(a){let n=0,h=0;for(const r of a){const isUp=r.dir2===1,m=r.mins;
  let d=-1;for(let j=1;j<m.length;j++){if(isUp?m[j].c<r.lock:m[j].c>r.lock){d=j;break;}}if(d<0)continue;
  let e=-1;for(let j=d+1;j<m.length;j++){const s=isUp?(m[j].c>m[j-1].c):(m[j].c<m[j-1].c);if(s){e=j;break;}}if(e<0)continue;
  let hit=false;for(let j=e;j<m.length;j++){if(isUp?m[j].h>=r.lock:m[j].l<=r.lock){hit=true;break;}}
  n++;if(hit)h++;}
  return n?(h/n*100):0;}
const OPT=[
  ['baseline (tanpa filter)', r=>true],
  ['vol>=0.9', r=>r.volRel2>=0.9],
  ['vol>=1.2', r=>r.volRel2>=1.2],
  ['vol>=1.5', r=>r.volRel2>=1.5],
  ['vol>=2', r=>r.volRel2>=2],
  ['surprise>=2', r=>r.surprise>=2],
  ['surprise>=3', r=>r.surprise>=3],
  ['mv>=0.02%', r=>r.mv2>=0.02],
  ['vol>=1.5 & surprise>=2', r=>r.volRel2>=1.5&&r.surprise>=2],
  ['vol>=1.5 & surprise>=3', r=>r.volRel2>=1.5&&r.surprise>=3],
  ['vol>=2 & surprise>=2', r=>r.volRel2>=2&&r.surprise>=2],
  ['vol>=3 & surprise>=3', r=>r.volRel2>=3&&r.surprise>=3],
];
console.log(`\n=== MENU OPSI SINYAL 2 DETIK (5m, ${R.length} sesi / ${days.toFixed(0)} hari) ===`);
console.log('  opsi                          n     /hari   WR      testWR   lock-recapture');
for(const [name,f] of OPT){
  const a=R.filter(f), b=TE.filter(f);
  if(a.length<40) continue;
  console.log(`  ${name.padEnd(28)} ${String(a.length).padStart(5)}  ${(a.length/days).toFixed(0).padStart(5)}   ${wr(a).toFixed(1)}%   ${wr(b).toFixed(1)}%    ${lockRate(a).toFixed(1)}%`);
}

// ==== tulis kalibrasi tier 2s untuk app ====
const tiers={
  STRONG:{ n:0,wr:0,testWR:0,recap:0,rule:"volRel2>=3 & surprise>=3" },
  GOOD:{ n:0,wr:0,testWR:0,recap:0,rule:"volRel2>=1.5 & surprise>=2" },
  FAIR:{ n:0,wr:0,testWR:0,recap:0,rule:"volRel2>=0.9" },
};
const defs={ STRONG:r=>r.volRel2>=3&&r.surprise>=3, GOOD:r=>r.volRel2>=1.5&&r.surprise>=2, FAIR:r=>r.volRel2>=0.9 };
for(const k of Object.keys(tiers)){
  const a=R.filter(defs[k]), b=TE.filter(defs[k]);
  tiers[k].n=a.length; tiers[k].wr=+(wr(a)/100).toFixed(4); tiers[k].testWR=+(wr(b)/100).toFixed(4); tiers[k].recap=+(lockRate(a)/100).toFixed(4); tiers[k].perDay=+(a.length/days).toFixed(1);
}
const out={generated:new Date().toISOString(),windowDays:+days.toFixed(2),sessions:R.length,tiers};
fs.writeFileSync(path.join(__dirname,"out","early2s.json"),JSON.stringify(out,null,2));
console.log("\nWrote out/early2s.json:");
console.log(JSON.stringify(tiers,null,1));
