// Vercel serverless function (Node). Exposes GET /api/snapshot
// Reuses the shared snapshot module so behavior matches the local Node server.
const { getSnapshot } = require("../snapshot");

module.exports = async function handler(req, res) {
  try {
    const snap = await getSnapshot(new URL(req.url, "http://x").searchParams.get("history") === "1");
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Cache-Control", "no-store");
    res.status(200).json(snap);
  } catch (e) {
    res.status(502).json({ error: String(e) });
  }
};
