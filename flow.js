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
const flow = { BTC: {}, ETH: {} };

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
   Dipakai karena stream aggTrade WS diblok di sebagian host (mis. Railway), sementara REST jalan.
   Dedupe per aset: hanya candle dengan time > terakhir yang diproses, jadi boleh dipanggil
   berkali-kali (tiap tick) tanpa menghitung ganda. */
const lastKline = { BTC: 0, ETH: 0 };
function addKlines(sym, candles) {
  const s = flow[sym];
  if (!s || !Array.isArray(candles) || !candles.length) return;
  let last = lastKline[sym] || 0;
  for (const c of candles) {
    if (!c || !isFinite(c.time) || c.time <= last) continue;
    if (!isFinite(c.vol) || !isFinite(c.tb)) continue;
    const min = Math.floor(c.time / 60) * 60;
    let b = s[min];
    if (!b) { b = s[min] = { buy: 0, sell: 0, n: 0 }; prune(s, min); }
    b.buy += +c.tb;
    b.sell += Math.max(0, +c.vol - +c.tb);
    b.n++;
    last = c.time;
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
