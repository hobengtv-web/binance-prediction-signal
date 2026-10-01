/* ============================================================================
   FLOW — pelacak executed order flow (OFI) sisi SERVER.

   Kenapa di server: aplikasi memakai sinyal SERVER-SIDE sebagai satu sumber kebenaran
   ("sinyal identik di semua device"). Sebelumnya OFI hanya dihitung di browser dari
   stream trade lokal, sedangkan objek sinyal yang DITAMPILKAN adalah objek server yang
   tidak punya field `ofi` -> UI selalu menampilkan "OFI —" walaupun stream trade jalan.

   Cara kerja (sama rumusnya dengan versi klien di app.js -> sessionOFI):
     - setiap aggTrade Binance membawa flag `m` (isBuyerMaker):
         m = true  -> agresor adalah PENJUAL (taker sell) -> masuk kolom `sell`
         m = false -> agresor adalah PEMBELI (taker buy)  -> masuk kolom `buy`
     - volume diakumulasi per MENIT: flow[sym][menitStartSec] = { buy, sell, n }
     - OFI sesi = (buy - sell) / (buy + sell) dari awal sesi (t0) sampai sekarang, 0..1.

   Nilai balik `null` = belum ada data sama sekali (UI menampilkan "—"), BUKAN 0 — supaya
   "tidak tahu" tidak tertukar dengan "seimbang".
   ============================================================================ */

const KEEP_MIN = 180;                                   // simpan ~3 jam bucket menit
const flow = { BTC: {}, ETH: {}, BNB: {} };   // BNB WAJIB ada: tanpa bucket, addTrade/addKlines BNB di-skip -> OFI BNB selalu null

function prune(s, minSec) {
  const cut = minSec - KEEP_MIN * 60;
  for (const k of Object.keys(s)) if (+k < cut) delete s[k];
}

/* Dipanggil untuk SETIAP aggTrade dari Binance (server.js). */
function addTrade(sym, tsMs, qty, isBuyerMaker) {
  const s = flow[sym];
  if (!s || !isFinite(qty) || qty <= 0) return;
  const min = Math.floor(tsMs / 60000) * 60;
  let b = s[min];
  if (!b) { b = s[min] = { buy: 0, sell: 0, n: 0 }; prune(s, min); }
  if (isBuyerMaker) b.sell += qty; else b.buy += qty;
  b.n++;
}

/* Sumber ALTERNATIF yang selalu tersedia: kline REST Binance.
   Setiap kline 1s punya `tb` = taker BUY volume; sell = vol - tb.

   PENTING (realtime): candle 1s TERAKHIR masih SEDANG BERJALAN, volumenya bertambah tiap detik.
   Versi lama hanya mencatat candle dengan `time > terakhir`, sehingga candle yang sedang berjalan
   tercatat SAAT BARU MUNCUL (volume ~0) dan seluruh volume detik itu hilang -> OFI selalu
   tertinggal dari pergerakan harga. Sekarang candle yang sedang berjalan di-TOPO-UP dengan
   DELTA-nya (buy & sell) setiap kali dipanggil, jadi OFI bergerak secepat datanya (~1 detik). */
const lastKline = { BTC: null, ETH: null };            // { time, tb, vol } candle terakhir yang dilihat
function bucketFor(s, tsSec) {
  const min = Math.floor(tsSec / 60) * 60;
  let b = s[min];
  if (!b) { b = s[min] = { buy: 0, sell: 0, n: 0 }; prune(s, min); }
  return b;
}
function addVol(s, tsSec, buy, sell) {
  if (!(buy > 0) && !(sell > 0)) return;
  const b = bucketFor(s, tsSec);
  b.buy += buy; b.sell += sell; b.n++;
}
function addKlines(sym, candles) {
  const s = flow[sym];
  if (!s || !Array.isArray(candles) || !candles.length) return;
  let last = lastKline[sym];
  for (const c of candles) {
    if (!c || !isFinite(c.time) || !isFinite(c.vol) || !isFinite(c.tb)) continue;
    if (!last || c.time > last.time) {
      addVol(s, c.time, +c.tb, Math.max(0, +c.vol - +c.tb));    // candle baru: volume penuh
      last = { time: c.time, tb: +c.tb, vol: +c.vol };
    } else if (c.time === last.time) {
      // candle berjalan: tambahkan HANYA selisihnya (delta), jangan dihitung dua kali
      const dBuy = +c.tb - last.tb;
      const dSell = (+c.vol - +c.tb) - (last.vol - last.tb);
      addVol(s, c.time, Math.max(0, dBuy), Math.max(0, dSell));
      last = { time: c.time, tb: +c.tb, vol: +c.vol };
    }
    // c.time < last.time -> data lama, abaikan
  }
  lastKline[sym] = last;
}

/* OFI sesi: mulai dari awal sesi (t0sec, dibulatkan ke menit) sampai nowSec. */
function sessionOFI(sym, t0Sec, nowSec) {
  const s = flow[sym];
  if (!s) return null;
  const start = Math.floor(t0Sec / 60) * 60;
  const end = (nowSec == null) ? Math.floor(Date.now() / 1000) : nowSec;
  let buy = 0, sell = 0, mins = 0;
  for (let t = start; t <= end; t += 60) {
    const o = s[t];
    if (o) { buy += o.buy; sell += o.sell; mins++; }
  }
  if (!mins) return null;                               // belum ada trade terekam
  const tot = buy + sell;
  if (!(tot > 0)) return 0;
  return +(((buy - sell) / tot).toFixed(4));
}

module.exports = { addTrade, addKlines, sessionOFI, stats: () => ({ BTC: Object.keys(flow.BTC).length, ETH: Object.keys(flow.ETH).length }) };
