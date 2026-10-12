/* tvchart.js — TradingView Lightweight Charts wrapper (API setara CanvasChart).
   Candles + pan/zoom NATIVE (geser/scroll nyaman), overlay custom (zona, pin, session divider,
   marker) digambar di canvas TERSINKRON (timeToCoordinate/priceToCoordinate). SEMUA penanda lama
   dipertahankan: LOCK, guides/TA, zona Entry/Sell (pulse), pin E/C, marker swing, proyeksi, trendFit,
   current price, crosshair, scroll-to-fetch-older. */
(function () {
  const UP = "#26a69a", DOWN = "#ef5350", ACCENT = "#f0b90b";
  const nf = (v, d) => (v == null || !isFinite(v)) ? "—" : (Math.abs(v) >= 1000 ? v.toLocaleString("en-US", { maximumFractionDigits: 2 }) : v.toFixed(d == null ? 2 : d));
  const tstr = (t) => { const d = new Date(t * 1000); const p = (x) => String(x).padStart(2, "0"); return p(d.getHours()) + ":" + p(d.getMinutes()) + ":" + p(d.getSeconds()); };

  function TVChart(el) {
    this.el = el;
    this.type = "candles";
    this.candles = [];
    this.decision = null; this.guides = []; this.projection = []; this.trendFit = [];
    this.markers = []; this.pins = []; this.sessionDuration = 0;
    this.prediction = null; this.currentPrice = null;
    this.visible = 90; this.follow = true;
    this.onReachStart = null; this.onCrosshair = null;
    this._startLock = false;
    this._lockLine = null; this._curLine = null; this._guideLines = [];

    const LCc = window.LightweightCharts;
    const w = el.clientWidth || 600, h = el.clientHeight || 320;
    this.chart = LCc.createChart(el, {
      width: w, height: h,
      layout: { background: { color: "transparent" }, textColor: "#9aa4b2", fontSize: 10, fontFamily: "-apple-system,Segoe UI,Roboto,sans-serif" },
      grid: { vertLines: { color: "rgba(255,255,255,.05)" }, horzLines: { color: "rgba(255,255,255,.05)" } },
      rightPriceScale: { borderColor: "rgba(255,255,255,.10)", scaleMargins: { top: 0.08, bottom: 0.08 } },
      timeScale: { borderColor: "rgba(255,255,255,.10)", timeVisible: true, secondsVisible: true, rightOffset: 3, barSpacing: 7, minBarSpacing: 0.5 },
      crosshair: { mode: LCc.CrosshairMode.Normal, vertLine: { color: "rgba(255,255,255,.14)", width: 1, labelVisible: false }, horzLine: { color: "rgba(255,255,255,.14)", width: 1, labelVisible: true, labelBackgroundColor: "#2962ff" } },
      handleScroll: { mouseWheel: true, pressedMouseMove: true, horzTouchDrag: true, vertTouchDrag: false },
      handleScale: { mouseWheel: true, pinch: true, pressedMouseMove: true, axisPressedMouseMove: true, axisDoubleClickReset: true },
      animationDuration: 0,
    });
    this.series = this.chart.addCandlestickSeries({
      upColor: UP, downColor: DOWN, borderVisible: false, wickUpColor: UP, wickDownColor: DOWN,
      priceLineVisible: false, lastValueVisible: true,
    });
    this.projSeries = this.chart.addLineSeries({ color: UP, lineWidth: 2, lineStyle: LCc.LineStyle.Dashed, priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false });
    this.trendSeries = this.chart.addLineSeries({ color: ACCENT, lineWidth: 1, priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false });

    // canvas overlay (zona, pin, session divider, marker)
    el.style.position = el.style.position || "relative";
    this._ov = document.createElement("canvas");
    this._ov.style.cssText = "position:absolute;left:0;top:0;pointer-events:none;z-index:4";
    el.appendChild(this._ov);
    this._ovc = this._ov.getContext("2d");

    const ts = this.chart.timeScale();
    this._inertial = false;
    // Follow tracking: tiap user scrollback → lepas auto-follow; drag ke ujung kanan → restore.
    // Dilindungi _inertial agar scrollToRealTime() (triggered by setData/updateLast) tidak
    // menimpa flag follow lewat listener ini sendiri.
    ts.subscribeVisibleLogicalRangeChange((lr) => {
      if (!lr) return;
      if (!this._inertial && this.candles.length) {
        const last = this.candles.length - 1;
        const back = last - lr.to;                     // seberapa jauh ujung kanan dr candle terakhir
        if (back > Math.max(1, this.candles.length * 0.05)) this.follow = false;   // scrollback manual
        if (back <= 1) this.follow = true;             // dekat realtime → (re)engage follow
      }
      if (this.onReachStart && this.candles.length) {
        if (lr.from <= 3 && !this._startLock) { this._startLock = true; try { this.onReachStart(() => { this._startLock = false; }); } catch (_) { this._startLock = false; } }
      }
    });
    this.chart.subscribeCrosshairMove((param) => {
      try {
        const p = param && param.point && param.time && param.seriesData && param.seriesData.get(this.series);
        // Hanya info harga (label harga di sumbu kanan sudah dari library). Legend kiri ikut candle hover.
        if (this.onCrosshair) this.onCrosshair(p ? { t: param.time, price: p.close, candle: p, x: param.point.x, y: param.point.y } : null);
      } catch (_) {}
    });
    this._raf();
  }

  TVChart.prototype._raf = function () {
    const loop = () => { try { this._sync(); } catch (_) {} this._rafId = requestAnimationFrame(loop); };
    this._rafId = requestAnimationFrame(loop);
  };
  TVChart.prototype._priceLines = function () {
    return this._lockLine ? 1 : 0;
  };
  TVChart.prototype._setPL = function (prop, line) {
    try { if (line) this.series.removePriceLine(line); } catch (_) {}
    if (line) this[prop] = line;
  };
  TVChart.prototype._line = function (price, color, title, dashed, axisLabel, width) {
    if (price == null || !isFinite(price)) return null;
    return this.series.createPriceLine({
      price, color, lineWidth: width || 1,
      lineStyle: dashed ? window.LightweightCharts.LineStyle.Dashed : window.LightweightCharts.LineStyle.Solid,
      axisLabelVisible: axisLabel !== false, title: title || "",
    });
  };

  TVChart.prototype.resize = function () {
    const w = this.el.clientWidth || 600, h = this.el.clientHeight || 320;
    try { this.chart.applyOptions({ width: w, height: h }); } catch (_) {}
  };
  TVChart.prototype.setType = function (t) { this.type = t; };
   TVChart.prototype.setData = function (candles, opts) {
    opts = opts || {};
    const oldN = this.candles.length;
    const ts = this.chart.timeScale();
    // Saat lazy-load (prepend older candles): simpan posisi viewport (jugi lepas follow) & restore setelah setData.
    const preserve = !!opts.preserveRange && !this.follow && oldN > 0;
    let lr = null;
    if (preserve) { try { lr = ts.getVisibleLogicalRange(); } catch (_) {} }
    this.candles = (candles || []).slice();
    try {
      this.series.setData(this.candles.filter((c) => c && c.time != null && isFinite(c.close)).map((c) => ({ time: c.time, open: c.open, high: c.high, low: c.low, close: c.close })));
      if (preserve && lr) {
        // restore posisi relatif (jumlah candle bertambah ke kiri): pertahankan right-edge logical coordinate.
        const newN = this.candles.length;
        const span = lr.to - lr.from;
        const right = lr.to + (newN - oldN);
        this._inertial = true;
        try { ts.setVisibleLogicalRange({ from: right - span, to: right }); } catch (_) {}
        this._inertial = false;
      } else if (this.follow) { this._inertial = true; try { ts.scrollToRealTime(); } catch (_) {} this._inertial = false; }
    } catch (_) {}
  };
  TVChart.prototype.setLineData = function (candles) { this.setData(candles); };
  TVChart.prototype.updateLast = function (c) {
    if (!c || c.time == null || !isFinite(c.close)) return;
    try { this.series.update({ time: c.time, open: c.open, high: c.high, low: c.low, close: c.close }); } catch (_) {}
    if (this.follow) { try { this.chart.timeScale().scrollToRealTime(); } catch (_) {} }
  };
  TVChart.prototype.fit = function () {
    this.follow = true;
    this._inertial = true;
    try { this.chart.timeScale().scrollToRealTime(); } catch (_) {}
    this._inertial = false;
  };

  TVChart.prototype.setDecision = function (p) {
    this.decision = (p == null || !isFinite(p)) ? null : p;
    try { if (this._lockLine) { this.series.removePriceLine(this._lockLine); this._lockLine = null; } } catch (_) {}
  };
  TVChart.prototype.setCurrentPrice = function (p) { this.currentPrice = (p == null || !isFinite(p)) ? null : p; };
  TVChart.prototype.setPrediction = function (dir, price) { this.prediction = dir; if (price != null && isFinite(price)) this.decision = price; };
  TVChart.prototype.setSessionDuration = function (ms) { this.sessionDuration = ms || 0; };
  TVChart.prototype.setProjection = function (pts) {
    this.projection = (pts || []).filter((x) => x && x.time != null && isFinite(x.value));
    try {
      this.projSeries.setData(this.projection.map((x) => ({ time: x.time, value: x.value })));
      const up = this.projection.length >= 2 && this.projection[1].value > (this.decision != null ? this.decision : this.projection[0].value);
      this.projSeries.applyOptions({ color: up ? UP : DOWN });
    } catch (_) {}
  };
  TVChart.prototype.setTrendFit = function (pts) {
    this.trendFit = (pts || []).filter((x) => x && x.time != null && isFinite(x.value));
    try {
      this.trendSeries.setData(this.trendFit.map((x) => ({ time: x.time, value: x.value })));
      const up = this.trendFit.length >= 2 && this.trendFit[this.trendFit.length - 1].value >= this.trendFit[0].value;
      this.trendSeries.applyOptions({ color: up ? UP : DOWN });
    } catch (_) {}
  };
  TVChart.prototype.setMarkers = function (m) { this.markers = m || []; };
  TVChart.prototype.setPins = function (l) { this.pins = l || []; };
  TVChart.prototype.setGuides = function (list) {
    this.guides = (list || []).filter((g) => g && isFinite(g.p));
    for (const l of this._guideLines) { try { this.series.removePriceLine(l); } catch (_) {} }
    this._guideLines = [];
    for (const g of this.guides) {
      // AKTIF (memicu entry/close Trade Assistant) → KUNING SOLID tebal, label tampil.
      // Non-aktif: S/R abu, level lain putih redup, putus-putus (samakan dgn CanvasChart lama).
      const active = !!g.active;
      const col = active ? "#f0b90b" : "rgba(255,255,255,.42)";
      // Garis saja (tanpa title/axis-label) — label digambar di KIRI oleh overlay _sync.
      const l = this._line(g.p, col, "", !active, false, active ? 2 : 1);
      if (l) this._guideLines.push(l);
    }
  };

  // ===== overlay canvas (sinkron ke chart) =====
  TVChart.prototype._sync = function () {
    const el = this.el, cv = this._ov; if (!cv) return;
    const w = el.clientWidth, h = el.clientHeight, dpr = window.devicePixelRatio || 1;
    if (cv.width !== Math.round(w * dpr) || cv.height !== Math.round(h * dpr)) { cv.width = Math.round(w * dpr); cv.height = Math.round(h * dpr); cv.style.width = w + "px"; cv.style.height = h + "px"; }
    const ctx = this._ovc; ctx.setTransform(dpr, 0, 0, dpr, 0, 0); ctx.clearRect(0, 0, w, h);
    const ts = this.chart.timeScale();
    const xOf = (t) => { const x = ts.timeToCoordinate(t); return x == null ? null : x; };
    const yOf = (p) => { const y = this.series.priceToCoordinate(p); return y == null ? null : y; };

    // Label garis bantu (ENTRY/TARGET/R/S) di KIRI — tidak menutupi candle terbaru di kanan.
    if (this.guides && this.guides.length) {
      ctx.save();
      ctx.font = "10px -apple-system,Segoe UI,Roboto,sans-serif";
      ctx.textAlign = "left"; ctx.textBaseline = "middle";
      for (const g of this.guides) {
        if (!g.label) continue;
        const y = yOf(g.p); if (y == null || y < 8 || y > h - 6) continue;
        const active = !!g.active;
        const col = active ? "#f0b90b" : "rgba(226,232,240,.72)";
        const tw = ctx.measureText(g.label).width;
        ctx.fillStyle = "rgba(15,23,42,.80)"; ctx.fillRect(4, y - 6, tw + 8, 12);
        ctx.fillStyle = col; ctx.fillText(g.label, 8, y);
      }
      ctx.restore();
    }

    // LOCK price level: garis dashed penuh + label kiri (bukan di kanan axis → tak menutupi candle realtime)
    if (this.decision != null) {
      const y = yOf(this.decision);
      if (y != null) {
        ctx.save();
        ctx.strokeStyle = ACCENT; ctx.setLineDash([5, 4]); ctx.lineWidth = 1;
        ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(w, y); ctx.stroke();
        const txt = "LOCK " + nf(this.decision, 2);
        ctx.font = "bold 10px -apple-system,Segoe UI,Roboto,sans-serif";
        const tw = ctx.measureText(txt).width;
        ctx.fillStyle = "rgba(15,23,42,.85)"; ctx.fillRect(4, y - 6, tw + 8, 12);
        ctx.fillStyle = ACCENT; ctx.textAlign = "left"; ctx.textBaseline = "middle"; ctx.fillText(txt, 8, y);
        ctx.setLineDash([]); ctx.restore();
      }
    }

    // session dividers
    if (this.sessionDuration > 0 && this.candles.length) {
      const durSec = this.sessionDuration / 1000;
      const t0 = this.candles[0].time, t1 = this.candles[this.candles.length - 1].time;
      const first = Math.ceil(t0 / durSec) * durSec, last = Math.floor(t1 / durSec) * durSec;
      ctx.save(); ctx.strokeStyle = "rgba(240,185,11,.55)"; ctx.lineWidth = 1; ctx.setLineDash([3, 4]); ctx.fillStyle = "rgba(240,185,11,.9)"; ctx.textAlign = "center"; ctx.textBaseline = "top"; ctx.font = "10px -apple-system,Segoe UI,Roboto,sans-serif";
      for (let t = first; t <= last; t += durSec) {
        const x = xOf(t); if (x == null || x < -2 || x > w) continue;
        ctx.beginPath(); ctx.moveTo(x, 0); ctx.lineTo(x, h); ctx.stroke();
        ctx.fillText(tstr(t).slice(0, 5), x, 1);
      }
      ctx.setLineDash([]); ctx.restore();
    }

    // zona Entry / Sell (pulse) berdasar prediction vs lock(decision)
    if (this.prediction && this.decision != null) {
      const y = yOf(this.decision);
      if (y != null) {
        const ph = (Date.now() % 1000) / 1000;
        const pulse = 0.16 + 0.24 * (0.5 - Math.abs(0.5 - ph)) * 2;
        const inEntry = this.currentPrice != null && (this.prediction === "down" ? this.currentPrice > this.decision : this.currentPrice < this.decision);
        const inSell = this.currentPrice != null && (this.prediction === "down" ? this.currentPrice < this.decision : this.currentPrice > this.decision);
        const eCol = inEntry ? `rgba(59,130,246,${pulse})` : "rgba(59,130,246,.10)";
        const sCol = inSell ? `rgba(16,185,129,${pulse})` : "rgba(16,185,129,.10)";
        ctx.font = "10px -apple-system,Segoe UI,Roboto,sans-serif"; ctx.textAlign = "center"; ctx.textBaseline = "middle";
        if (this.prediction === "down") {
          ctx.fillStyle = eCol; ctx.fillRect(0, 0, w, y); ctx.fillStyle = "#3b82f6"; ctx.fillText("Entry Zone", w / 2, y / 2);
          ctx.fillStyle = sCol; ctx.fillRect(0, y, w, h - y); ctx.fillStyle = "#10b981"; ctx.fillText("Sell Zone (Take Profit)", w / 2, (y + h) / 2);
        } else {
          ctx.fillStyle = eCol; ctx.fillRect(0, y, w, h - y); ctx.fillStyle = "#3b82f6"; ctx.fillText("Entry Zone", w / 2, (y + h) / 2);
          ctx.fillStyle = sCol; ctx.fillRect(0, 0, w, y); ctx.fillStyle = "#10b981"; ctx.fillText("Sell Zone (Take Profit)", w / 2, y / 2);
        }
      }
    }

    // markers (swing hi/lo triangles + text)
    if (this.markers.length) {
      ctx.font = "10px -apple-system,Segoe UI,Roboto,sans-serif";
      for (const m of this.markers) {
        const x = xOf(m.time), y = yOf(m.value); if (x == null || y == null) continue;
        ctx.fillStyle = m.color || ACCENT;
        if (m.below) {
          ctx.beginPath(); ctx.moveTo(x, y + 5); ctx.lineTo(x - 4, y + 12); ctx.lineTo(x + 4, y + 12); ctx.closePath(); ctx.fill();
          ctx.textAlign = "center"; ctx.textBaseline = "top"; ctx.fillText(m.text || "", x, y + 13);
        } else {
          ctx.beginPath(); ctx.moveTo(x, y - 5); ctx.lineTo(x - 4, y - 12); ctx.lineTo(x + 4, y - 12); ctx.closePath(); ctx.fill();
          ctx.textAlign = "center"; ctx.textBaseline = "bottom"; ctx.fillText(m.text || "", x, y - 13);
        }
      }
    }

    // pins E/C (bubble + ekor + label harga/jam)
    if (this.pins.length) {
      ctx.save();
      for (const pin of this.pins) {
        const x = xOf(pin.t), y = yOf(pin.p); if (x == null || y == null) continue;
        const isClose = pin.cls === "pin-close"; const col = isClose ? "#10b981" : "#3b82f6";
        const R = 9, above = y > 60, by = above ? y - (12 + R) : y + (12 + R);
        ctx.strokeStyle = col; ctx.lineWidth = 1.5; ctx.beginPath(); ctx.moveTo(x, y); ctx.lineTo(x, above ? by + R : by - R); ctx.stroke();
        ctx.fillStyle = col; ctx.beginPath(); ctx.arc(x, y, 2.5, 0, Math.PI * 2); ctx.fill();
        ctx.beginPath(); ctx.arc(x, by, R, 0, Math.PI * 2); ctx.fill();
        ctx.strokeStyle = "rgba(0,0,0,.5)"; ctx.lineWidth = 1; ctx.stroke();
        ctx.fillStyle = "#fff"; ctx.font = "bold 10px -apple-system,Segoe UI,Roboto,sans-serif"; ctx.textAlign = "center"; ctx.textBaseline = "middle"; ctx.fillText(pin.label, x, by + 0.5);
        const txt = nf(pin.p) + " · " + tstr(pin.t).slice(0, 5); ctx.font = "10px -apple-system,Segoe UI,Roboto,sans-serif";
        const tw = ctx.measureText(txt).width; const tx = (x + R + 4 + tw <= w) ? x + R + 4 : x - R - 4 - tw;
        ctx.fillStyle = "rgba(0,0,0,.45)"; ctx.fillRect(tx - 2, by - 7, tw + 4, 14); ctx.fillStyle = col; ctx.textAlign = "left"; ctx.fillText(txt, tx, by);
      }
      ctx.restore();
    }
  };

  if (typeof window !== "undefined") window.TVChart = TVChart;
  if (typeof module !== "undefined" && module.exports) module.exports = { TVChart };
})();
