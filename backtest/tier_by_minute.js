const fs=require('fs');
const path=require('path');
const load=(s,t)=>JSON.parse(fs.readFileSync(`backtest/data/${s}_${t}.json`,'utf8'));
const MS={'5m':5,'15m':15}; const FAIR={'5m':1.2,'15m':2.0};
function ofiMap(sym){const a=JSON.parse(fs.readFileSync(`backtest/data/${sym}_ofi.json`,'utf8'));const m=new Map();for(const r of a)m.set(r.t,r);return m;}
function ofiCum(om,t0,endSec){let b=0,s=0,ok=0,need=0;for(let t=t0;t<endSec;t+=60){need++;const o=om.get(t);if(o){b+=o.buy;s+=o.sell;ok++;}}if(!ok||ok<need)return null;const tot=b+s;return tot>0?(b-s)/tot:0;}
const TABLE={};
for(const tf of ['5m','15m']){
  const mins=MS[tf]; const rec=[];
  for(const sym of ['BTC','ETH']){
    const one=load(sym,'1m'), tfC=load(sym,tf); const oi=new Map(one.map((c,i)=>[c.time,i])); const om=ofiMap(sym);
    for(const c of tfC){
      const t0=c.time; if(t0%(mins*60)!==0)continue; const i0=oi.get(t0); if(i0==null)continue;
      const lock=c.open; const outcome=c.close>=lock?1:0;
      for(let j=1;j<=mins;j++){
        const idx=i0+j-1; if(idx>=one.length)break;
        const C=one[idx].close; const dir=C>lock?1:C<lock?0:-1; if(dir<0)continue;
        const prev=one.slice(Math.max(0,idx-25),idx).map(x=>x.vol); const bv=prev.length?prev.reduce((a,b)=>a+b,0)/prev.length:0;
        const volRel=bv>0?one[idx].vol/bv:1;
        const ofi=ofiCum(om,t0,t0+j*60); if(ofi==null)continue;
        if(((ofi>=0?1:0)===dir) && volRel>=FAIR[tf]){ rec.push({min:j, won:dir===outcome?1:0}); break; }
      }
    }
  }
  const n=rec.length; const W=a=>a.length?(a.reduce((x,r)=>x+r.won,0)/a.length*100).toFixed(1)+'%':'—';
  console.log(`\n=== ${tf}: winrate entry berdasarkan menit pertama muncul (n=${n}) ===`);
  let acc=[]; let cum=[];
  for(let j=1;j<=mins;j++){const g=rec.filter(r=>r.min===j); if(!g.length)continue; cum=cum.concat(g);
    console.log(`  menit ${String(j).padStart(2)}: n=${String(g.length).padStart(4)}  WR=${W(g).padStart(6)}   (kumulatif s/d menit ${j}: ${W(cum)})`);}
  console.log(`  TOTAL: ${W(rec)}`);
  const out={}; for(const r of rec){ (out[r.min]=out[r.min]||[]).push(r); }
  TABLE[tf]={}; for(const k of Object.keys(out)){ const g=out[k]; TABLE[tf][k]={ n:g.length, wr:+(g.reduce((a,r)=>a+r.won,0)/g.length).toFixed(4) }; }
}
const OUT=path.join(__dirname,'out'); if(!fs.existsSync(OUT))fs.mkdirSync(OUT,{recursive:true});
fs.writeFileSync(path.join(OUT,'tier_by_minute.json'),JSON.stringify({generated:new Date().toISOString(),windowDays:30,tiers:TABLE},null,2));
console.log('\nWrote out/tier_by_minute.json');
