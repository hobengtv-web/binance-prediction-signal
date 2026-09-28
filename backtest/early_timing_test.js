/* Apakah menunggu 15 detik membantu? Ukur akurasi arah (harga vs lock) pada detik-detik awal
   sesi 5m memakai klines 1s. Usage: node backtest/early_timing_test.js [days] */
const fs=require("fs"), path=require("path");
const DATA=path.join(__dirname,"data");
const HOST="https://data-api.binance.vision";
const SYMS={BTC:"BTCUSDT",ETH:"ETHUSDT"};
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
const DAYS=Number(process.argv[2]||3);
const DEBUG=[1,2,3,5,10,15,20,30,45,60,120,180,240,299];

async function fetch1s(binSym, days){
  const file=path.join(DATA,`${binSym}_1s.json`);
  if(fs.existsSync(file)){
    const age=(Date.now()-fs.statSync(file).mtimeMs)/3600000;
    if(age<6){ console.log(`  ${binSym}: pakai cache (${age.toFixed(1)} jam)`); return JSON.parse(fs.readFileSync(file,"utf8")); }
  }
  const startMs=Date.now()-days*86400000; let endTime=Date.now(); const out=[]; let req=0;
  while(endTime>startMs){
    const url=`${HOST}/api/v3/klines?symbol=${binSym}&interval=1s&limit=1000&endTime=${endTime}`;
    const rows=await fetch(url).then(r=>r.ok?r.json():[]).catch(()=>[]);
    if(!rows.length) break;
    for(const r of rows) out.push({t:Math.floor(r[0]/1000),o:+r[1],h:+r[2],l:+r[3],c:+r[4],v:+r[5]});
    endTime=rows[0][0]-1; req++;
    if(req%40===0) process.stdout.write(`    ${binSym} ${out.length}\n`);
    await sleep(110);
  }
  out.sort((a,b)=>a.t-b.t);
  const seen=new Set(), dd=[]; for(const c of out){ if(seen.has(c.t))continue; seen.add(c.t); dd.push(c); }
  fs.writeFileSync(file,JSON.stringify(dd));
  console.log(`  ${binSym}: ${dd.length} candle 1s -> ${path.basename(file)}`);
  return dd;
}

(async()=>{
  const perT={}; DEBUG.forEach(d=>perT[d]=[]);
  const flips={}; DEBUG.forEach(d=>flips[d]=0);
  let sessions=0;
  for(const [sym,binSym] of Object.entries(SYMS)){
    const arr=await fetch1s(binSym,DAYS);
    const map=new Map(arr.map(c=>[c.t,c]));
    const first=arr.length?arr[0].t:0, last=arr.length?arr[arr.length-1].t:0;
    for(let t0=Math.floor(first/300)*300; t0+299<=last; t0+=300){
      const c0=map.get(t0), cEnd=map.get(t0+299);
      if(!c0||!cEnd) continue;
      const lock=c0.o; const outcome=cEnd.c>=lock?1:0;
      let ok=0;
      for(const d of DEBUG){
        const cd=map.get(t0+d);
        if(!cd) continue;
        const dir=cd.c>lock?1:cd.c<lock?0:-1;
        if(dir<0) continue;
        perT[d].push(dir===outcome?1:0);
      }
      sessions++;
    }
  }
  const wr=a=>a.length?(a.reduce((x,y)=>x+y,0)/a.length*100).toFixed(1)+'%':'—';
  console.log(`\n=== akurasi arah vs hasil akhir sesi 5m (${sessions} sesi, ${DAYS} hari) ===`);
  console.log('  detik   akurasi   n        (naik berapa % dengan menunggu)');
  let prev=null;
  for(const d of DEBUG){
    const cur=perT[d].length?(perT[d].reduce((x,y)=>x+y,0)/perT[d].length*100):null;
    let delta='';
    if(prev!=null&&cur!=null) delta=` (+${(cur-prev).toFixed(1)} pp vs ${DEBUG[DEBUG.indexOf(d)-1]}s)`;
    console.log(`  ${String(d).padStart(4)}s   ${wr(perT[d]).padStart(6)}   ${String(perT[d].length).padStart(6)}${delta}`);
    if(cur!=null) prev=cur;
  }
  console.log('\nCatatan: akurasi ini memakai arah harga (close vs lock), bukan sinyal bergrade.');
})();
