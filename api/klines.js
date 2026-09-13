// Vercel serverless function (Node). Exposes GET /api/klines
// Reuses the shared snapshot module so behavior matches the local Node server.
const { getKlines } = require("../snapshot");

module.exports = async function handler(req, res) {
  try {
    const url = new URL(req.url, "http://x");
    const sym = url.searchParams.get("symbol") || "BTC";
    const tf = url.searchParams.get("tf") || "1s";
    const before = parseInt(url.searchParams.get("before") || "0", 10) || 0;
    const limit = Math.min(parseInt(url.searchParams.get("limit") || "600", 10), 1000);
    const candles = await getKlines(sym, tf, before, limit);
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Cache-Control", "no-store");
    res.status(200).json({ candles });
  } catch (e) {
    res.status(502).json({ error: String(e) });
  }
};
