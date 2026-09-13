/* ============================================================
   TradingView data feed (fallback when Binance is blocked)
   Uses exchange-prefixed symbols BINANCE:BTCUSDT / BINANCE:ETHUSDT
   so the price is identical to Binance's own feed.
   Protocol: socket.io over WebSocket with ~m~<len>~m~ framing.
   ============================================================ */
(function () {
  const TV_RES = { "1s": "1S", "5m": "5", "15m": "15", "1h": "60" };
  const TV_INTSEC = { "1s": 1, "5m": 300, "15m": 900, "1h": 3600 };
  const TV_SYMBOL = { BTC: "BINANCE:BTCUSDT", ETH: "BINANCE:ETHUSDT" };

  function connectTradingView(handlers) {
    const ws = new WebSocket("wss://data.tradingview.com/socket.io/websocket");
    let alive = true;

    function send(obj) {
      if (!alive) return;
      const s = JSON.stringify(obj);
      ws.send("~m~" + s.length + "~m~" + s);
    }
    function parse(buf) {
      let i = 0;
      while (i < buf.length) {
        if (buf.startsWith("~m~", i)) {
          const a = buf.indexOf("~m~", i + 2);
          if (a < 0) break;
          const len = parseInt(buf.substring(i + 3, a), 10);
          const json = buf.substring(a + 3, a + 3 + len);
          try { handle(JSON.parse(json)); } catch (_) {}
          i = a + 3 + len;
        } else if (buf.startsWith("~h~", i)) {
          ws.send("~h~"); i += 3;
        } else { i++; }
      }
    }
    function handle(m) {
      if (!m || !m.m) return;
      if (m.m === "qsd" || m.m === "quote_completed") {
        const data = m.p[1] || {};
        for (const key in data) {
          if (!key.startsWith("s:")) continue;
          const tv = key.slice(2);
          const symKey = tv === TV_SYMBOL.BTC ? "BTC" : tv === TV_SYMBOL.ETH ? "ETH" : null;
          if (!symKey) continue;
          const d = data[key];
          const last = parseFloat(d.lp);
          const chg = parseFloat(d.chp);
          if (!isNaN(last)) handlers.onQuote && handlers.onQuote(symKey, { last, chgPct: isNaN(chg) ? 0 : chg });
        }
      } else if (m.m === "series_data" || m.m === "series_update") {
        const id = m.p[0];
        const mm = /^s_(BTC|ETH)_(1s|5m|15m|1h)$/.exec(id);
        if (!mm) return;
        const symKey = mm[1], tf = mm[2];
        let bars = m.p[1];
        if (bars == null && m.p.length >= 4 && Array.isArray(m.p[3])) bars = [m.p[3]];
        if (!Array.isArray(bars)) return;
        const intSec = TV_INTSEC[tf];
        const toCandle = (b) => ({
          time: b[0], open: +b[1], high: +b[2], low: +b[3], close: +b[4],
          vol: +(b[5] || 0),
          openTime: b[0] * 1000, closeTime: (b[0] + intSec) * 1000,
        });
        if (m.m === "series_data") {
          const candles = bars.map(toCandle).sort((a, b) => a.time - b.time);
          handlers.onHistory && handlers.onHistory(symKey, tf, candles);
        } else {
          bars.map(toCandle).forEach((c) => handlers.onUpdate && handlers.onUpdate(symKey, tf, c));
        }
      }
    }

    ws.onopen = () => {
      send({ m: "set_auth_token", p: ["unauthorized_user_token"] });
      send({ m: "quote_create_session", p: ["qs_1"] });
      send({ m: "quote_add_symbols", p: ["qs_1", [TV_SYMBOL.BTC, TV_SYMBOL.ETH]] });
      for (const symKey of ["BTC", "ETH"]) {
        for (const tf of ["1s", "5m", "15m", "1h"]) {
          send({ m: "create_series", p: [`s_${symKey}_${tf}`, TV_SYMBOL[symKey], TV_RES[tf], tf === "1s" ? 600 : 160] });
        }
      }
      handlers.onStatus && handlers.onStatus("tradingview (BINANCE)");
    };
    ws.onmessage = (ev) => parse(ev.data);
    ws.onclose = () => { alive = false; handlers.onStatus && handlers.onStatus("tradingview: closed"); };
    ws.onerror = () => { try { ws.close(); } catch (_) {} };

    return { close: () => { alive = false; try { ws.close(); } catch (_) {} } };
  }

  window.connectTradingView = connectTradingView;
})();
