/* ============================================================================
   PATTERNS — deteksi pola candle (price action) dari deret candle CLOSED.

   Dipakai SERVER untuk:
     - mengirim daftar pola terakhir ke UI (disp.patterns) -> digambar di chart,
     - menambah/mengurangi skor momentum Trade Assistant (faktor pendukung, bukan penentu
       tunggal), dan menyebut polanya di teks aksi.

   Definisi dibuat eksplisit & konservatif supaya tidak "menebak":
     body        = |close - open|
     range       = high - low
     upperWick   = high - max(open, close)
     lowerWick   = min(open, close) - low

   Pola yang dikenali:
     bullish_engulfing : candle sebelumnya bearish, candle terakhir bullish, dan body terakhir
                         MENUTUPI body sebelumnya (open <= prev.close && close >= prev.open).
     bearish_engulfing : kebalikannya.
     hammer            : body kecil di ATAS range, lower wick >= 2x body, upper wick kecil
                         (sinyal pembalikan naik setelah turun).
     shooting_star     : body kecil di BAWAH range, upper wick >= 2x body (pembalikan turun).
     doji              : body <= 10% range (keraguan / titik balik).
     three_white_soldiers : 3 candle bullish berurutan dengan close makin tinggi.
     three_black_crows    : 3 candle bearish berurutan dengan close makin rendah.

   Setiap pola: { name, dir: "up"|"down"|"flat", at (waktu candle terakhir), price (level kunci),
                  label (teks pendek untuk tooltip/chart) }
   ============================================================================ */

function body(c) { return Math.abs(c.close - c.open); }
function range(c) { return Math.max(0, c.high - c.low); }
function upperWick(c) { return c.high - Math.max(c.open, c.close); }
function lowerWick(c) { return Math.min(c.open, c.close) - c.low; }
function isBull(c) { return c.close > c.open; }
function isBear(c) { return c.close < c.open; }

/* Deteksi pola yang BERAKHIR pada candle terakhir dari array (candles harus sudah CLOSED). */
function detectLast(candles) {
  const n = candles ? candles.length : 0;
  if (n < 1) return null;
  const c = candles[n - 1];
  const p = n >= 2 ? candles[n - 2] : null;      // beberapa pola (engulfing) butuh candle sebelumnya
  const r = range(c), b = body(c);
  if (r <= 0) return null;
  const lw = lowerWick(c), uw = upperWick(c);

  // 1) Engulfing (paling kuat secara definisi) — butuh 2 candle
  if (p && isBull(c) && isBear(p) && c.close >= p.open && c.open <= p.close && b > body(p) * 0.9) {
    return { name: "bullish_engulfing", dir: "up", at: c.time, price: c.low, label: "Bullish engulfing", short: "BullEngulf" };
  }
  if (p && isBear(c) && isBull(p) && c.close <= p.open && c.open >= p.close && b > body(p) * 0.9) {
    return { name: "bearish_engulfing", dir: "down", at: c.time, price: c.high, label: "Bearish engulfing", short: "BearEngulf" };
  }
  // 2) Hammer / shooting star (wick panjang = penolakan harga)
  if (lw >= 2 * b && uw <= Math.max(b, r * 0.15) && b / r <= 0.45) {
    return { name: "hammer", dir: "up", at: c.time, price: c.low, label: "Hammer (penolakan bawah)", short: "Hammer" };
  }
  if (uw >= 2 * b && lw <= Math.max(b, r * 0.15) && b / r <= 0.45) {
    return { name: "shooting_star", dir: "down", at: c.time, price: c.high, label: "Shooting star (penolakan atas)", short: "ShootStar" };
  }
  // 3) Tiga prajurit / tiga gagak (butuh 3 candle)
  if (n >= 3) {
    const a = candles[n - 3];
    if (isBull(a) && isBull(p) && isBull(c) && p.close > a.close && c.close > p.close) {
      return { name: "three_white_soldiers", dir: "up", at: c.time, price: c.low, label: "3 white soldiers", short: "3 Soldiers" };
    }
    if (isBear(a) && isBear(p) && isBear(c) && p.close < a.close && c.close < p.close) {
      return { name: "three_black_crows", dir: "down", at: c.time, price: c.high, label: "3 black crows", short: "3 Crows" };
    }
  }
  // 4) Doji (keraguan) — paling lemah, dicek terakhir
  if (b <= r * 0.10) {
    return { name: "doji", dir: "flat", at: c.time, price: (c.high + c.low) / 2, label: "Doji (keraguan)", short: "Doji" };
  }
  return null;
}

/* Ambil beberapa pola terakhir (maks `max`) dari deret candle, paling baru lebih dulu. */
function detect(candles, max) {
  const out = [];
  const lim = max || 3;
  const arr = candles || [];
  for (let end = arr.length; end >= 2 && out.length < lim; end--) {
    const w = arr.slice(0, end);                 // pola yang berakhir di candle ke-(end-1)
    const p = detectLast(w);
    if (p && !out.some((q) => q.at === p.at)) out.push(p);
  }
  return out;
}

module.exports = { detect, detectLast, body, range, upperWick, lowerWick };
