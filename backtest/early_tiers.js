const fs=require('fs');
const load=(s,t)=>JSON.parse(fs.readFileSync(`backtest/data/${s}_${t}.json`,'utf8'));
const MS={'5m':300000,'15m':900000};
function ofiMap(sym){const a=JSON.parse(fs.readFileSync(`backtest/data/${sym}_ofi.json`,'utf8'));const m=new Map();for(const r of a)m.set(r.t,r);return m;}
const wr=a=>a.length?(a.reduce((x,r)=>x+r.won,0)/a.length*100).toFixed(1)+'%':'—';
const OUT={};
for(const tf of ['5m','15m']){
  const dur=MS[tf]/1000; const rows=[];
  for(const sym of ['BTC','ETH']){
    const one=load(sym,'1m'), tfC=load(sym,tf); const oi=new Map(one.map((c,i)=>[c.time,i])); const om=ofiMap(sym);
    for(const c of tfC){
      const t0=c.time; if(t0%dur!==0)continue; const i1=oi.get(t0); if(i1==null)continue;
      const o=one[i1]; const lock=c.open; const dir=o.close>lock?1:o.close<lock?0:-1; if(dir<0)continue;
      const raw=om.get(t0); if(!raw)continue; const tot=raw.buy+raw.sell; const ofi=tot>0?(raw.buy-raw.sell)/tot:0;
      const prev=one.slice(Math.max(0,i1-25),i1).map(x=>x.vol); const bv=prev.length?prev.reduce((a,b)=>a+b,0)/prev.length:0;
      const volRel=bv>0?o.vol/bv:1;
      const outcome=c.close>=lock?1:0;
      rows.push({t0,dir,ofi,agree:(ofi>=0?1:0)===dir,strong:Math.abs(ofi)>=0.2,volRel,won:dir===outcome?1:0,days:1});
    }
  }
  rows.sort((a,b)=>a.t0-b.t0);
  const days=(rows[rows.length-1].t0-rows[0].t0)/86400;
  const cut=Math.floor(rows.length*0.7);
  const variants=[
    ['base dir', r=>true],
    ['vol>=0.6', r=>r.volRel>=0.6],
    ['vol>=0.8', r=>r.volRel>=0.8],
    ['vol>=1.0', r=>r.volRel>=1.0],
    ['vol>=1.2', r=>r.volRel>=1.2],
    ['vol>=1.5', r=>r.volRel>=1.5],
    ['vol>=2', r=>r.volRel>=2],
    ['vol>=3', r=>r.volRel>=3],
    ['OFI agree', r=>r.agree],
    ['OFI agree+vol>=0.6', r=>r.agree&&r.volRel>=0.6],
    ['OFI agree+vol>=0.8', r=>r.agree&&r.volRel>=0.8],
    ['OFI agree+vol>=1.0', r=>r.agree&&r.volRel>=1.0],
    ['OFI agree+vol>=1.2', r=>r.agree&&r.volRel>=1.2],
    ['OFI agree+vol>=1.5', r=>r.agree&&r.volRel>=1.5],
    ['OFI agree+vol>=2', r=>r.agree&&r.volRel>=2],
    ['OFI agree+vol>=3', r=>r.agree&&r.volRel>=3],
    ['OFI strong+vol>=3', r=>r.agree&&r.strong&&r.volRel>=3],
  ];
  console.log(`\n=== ${tf} early min1 — ${rows.length} sesi / ${days.toFixed(0)} hari ===`);
  console.log('  variant                   n     /hari   WR      | testWR (n)');
  for(const [name,cond] of variants){
    const all=rows.filter(cond), tr=rows.slice(0,cut).filter(cond), te=rows.slice(cut).filter(cond);
    console.log('  '+name.padEnd(24)+String(all.length).padStart(5)+'  '+(all.length/days).toFixed(0).padStart(5)+'   '+wr(all).padStart(7)+' | '+wr(te)+' (n'+te.length+')');
    OUT[`${tf}|${name}`]={n:all.length,wr:+(all.reduce((x,r)=>x+r.won,0)/all.length).toFixed(4),perDay:+(all.length/days).toFixed(1)};
  }
}
fs.writeFileSync('backtest/out/early_tiers.json',JSON.stringify({generated:new Date().toISOString(),windowDays:30,tiers:OUT},null,2));
console.log('\nWrote backtest/out/early_tiers.json');
