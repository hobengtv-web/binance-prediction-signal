/* ============================================================
   CanvasChart — self-contained candlestick + line chart
   (no external CDN dependency, mobile-friendly, touch + wheel)
   ============================================================ */
(function () {
  const UP = "#0ecb81", DOWN = "#f6465d", GRID = "rgba(43,49,57,.55)",
        TEXT = "#848e9c", BG = "transparent", ACCENT = "#f0b90b";

  function clamp(v, a, b) { return Math.max(a, Math.min(b, v)); }

  function CanvasChart(el) {
    this.el = el;
    this.canvas = document.createElement("canvas");
    this.canvas.style.width = "100%";
    this.canvas.style.height = "100%";
    this.canvas.style.display = "block";
    this.canvas.style.touchAction = "none";
    this.canvas.style.cursor = "grab";
    this.canvas.style.userSelect = "none";
    this.canvas.style.webkitUserSelect = "none";
    this.canvas.style.webkitTapHighlightColor = "transparent";
    el.appendChild(this.canvas);
    this.ctx = this.canvas.getContext("2d");

    this.candles = [];
    this.type = "candle";
    this.decision = null;
    this.projection = [];
    this.markers = [];
    this.visible = 90;
    this.offset = 0;
    this.rightOffset = 8;   // empty bars on the right when following (live edge not glued to frame)
    this.follow = true;     // pin view to latest
    this.anchorTime = null; // left-edge time when paused (time-anchored panning)
    this.cross = null;
    this.onCrosshair = null;
    this.onReachStart = null;   // called when user reaches left edge (lazy load)
    this._loadingStart = false; // flag lazy-load sedang berjalan
    this.sessionDuration = 0; // ms; 0 = no session dividers
    this.decision = null;
    this.projection = [];
    this.trendFit = [];   // auto trend line (regression) drawn across the recent window
    this.markers = [];    // marker.below => drawn under the point (swing lows)

    this._bindEvents();
    this._ro = new ResizeObserver(() => this.resize());
    this._ro.observe(el);
    this.resize();
  }

  CanvasChart.prototype.resize = function () {
    const dpr = window.devicePixelRatio || 1;
    const w = this.el.clientWidth, h = this.el.clientHeight;
    this.canvas.width = Math.max(1, w * dpr);
    this.canvas.height = Math.max(1, h * dpr);
    this.ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    this.W = w; this.H = h;
    this.render();
  };

  CanvasChart.prototype.setType = function (t) { this.type = t; this.render(); };
  CanvasChart.prototype.setData = function (candles) {
    this.candles = candles.map((c) => ({ ...c }));
    this.render();
  };
  CanvasChart.prototype.updateLast = function (c) {
    const a = this.candles;
    if (!a.length || c.time > a[a.length - 1].time) a.push({ ...c });
    else if (c.time === a[a.length - 1].time) a[a.length - 1] = { ...c };
    this.render();
  };
  CanvasChart.prototype.setLineData = function (candles) { this.setData(candles); };
  CanvasChart.prototype.setDecision = function (p) { this.decision = p; this.render(); };
  CanvasChart.prototype.setProjection = function (pts) { this.projection = pts || []; this.render(); };
  CanvasChart.prototype.setTrendFit = function (pts) { this.trendFit = pts || []; this.render(); };
  CanvasChart.prototype.setMarkers = function (m) { this.markers = m || []; this.render(); };
  CanvasChart.prototype.setSessionDuration = function (ms) { this.sessionDuration = ms || 0; this.render(); };
  CanvasChart.prototype.fit = function () { this.offset = 0; this.follow = true; this.anchorTime = null; this.visible = 90; this.render(); };

  CanvasChart.prototype._range = function () {
    const cs = this.candles, n = cs.length;
    if (!n) return null;
    const vis = clamp(this.visible, 20, 400);
    let start, end;
    if (this.follow) {
      // pin to latest; leave rightOffset EMPTY slots after the last candle so it isn't glued to the frame
      start = n + this.rightOffset - vis;
      if (start < 0) start = 0;
      end = n + this.rightOffset;
    } else if (this.anchorTime != null) {
      // time-anchored panning: find first candle with time <= anchorTime
      let lo = 0, hi = n - 1, ans = 0;
      while (lo <= hi) {
        const mid = (lo + hi) >> 1;
        if (cs[mid].time <= this.anchorTime) { ans = mid; lo = mid + 1; } else hi = mid - 1;
      }
      start = ans;
      end = start + vis;   // fixed window; empty future region is shown when panned forward
      if (end - start < vis) start = Math.max(0, end - vis);
    } else {
      start = n + this.rightOffset - vis;
      if (start < 0) start = 0;
      end = n + this.rightOffset;
    }
    return { start, end, vis: end - start };
  };

  // seconds per bar (candles are 5s, but derive it so it's robust)
  CanvasChart.prototype._barSec = function () {
    const cs = this.candles, n = cs.length;
    if (n > 1) return Math.max(1, (cs[n - 1].time - cs[0].time) / (n - 1));
    return 5;
  };

  // time currently at the LEFT edge of the plot
  CanvasChart.prototype._leftTime = function () {
    const r = this._range();
    if (!r) return 0;
    return this.candles[r.start].time;
  };

  CanvasChart.prototype.render = function () {
    const ctx = this.ctx, W = this.W, H = this.H;
    ctx.clearRect(0, 0, W, H);
    const padR = 64, padB = 16, padT = 8, padL = 6;
    const plotL = padL, plotR = W - padR, plotT = padT, plotB = H - padB;
    const plotW = plotR - plotL, plotH = plotB - plotT;
    if (plotW <= 0 || plotH <= 0) return;

    const r = this._range();
    if (!r || r.end - r.start < 2) return;
    const cs = this.candles;
    const n = cs.length;
    const barSec = n > 1 ? (cs[n - 1].time - cs[0].time) / (n - 1) : 5;
    // t1 may sit in the empty padding region (end > n) → extrapolate by barSec
    const t0 = cs[r.start].time;
    const t1 = (r.end - 1 < n) ? cs[r.end - 1].time : cs[n - 1].time + (r.end - 1 - (n - 1)) * barSec;
    const span = Math.max(1, t1 - t0);
    const xOf = (t) => plotL + ((t - t0) / span) * plotW;
    const barW = Math.max(2, (plotW / r.vis) * 0.66);

    // price bounds
    let pMin = Infinity, pMax = -Infinity;
    for (let i = r.start; i < Math.min(r.end, n); i++) {
      if (cs[i].low < pMin) pMin = cs[i].low;
      if (cs[i].high > pMax) pMax = cs[i].high;
    }
    if (this.decision != null) { pMin = Math.min(pMin, this.decision); pMax = Math.max(pMax, this.decision); }
    this.projection.forEach((p) => { pMin = Math.min(pMin, p.value); pMax = Math.max(pMax, p.value); });
    if (!isFinite(pMin)) { pMin = 0; pMax = 1; }
    const pad = (pMax - pMin) * 0.08 || pMax * 0.01;
    pMin -= pad; pMax += pad;
    const yOf = (p) => plotT + (pMax - p) / (pMax - pMin) * plotH;

    // grid + price axis
    ctx.font = "10px -apple-system,Segoe UI,Roboto,sans-serif";
    ctx.textBaseline = "middle";
    const steps = 5;
    for (let i = 0; i <= steps; i++) {
      const p = pMin + (pMax - pMin) * (i / steps);
      const y = yOf(p);
      ctx.strokeStyle = GRID; ctx.lineWidth = 1;
      ctx.beginPath(); ctx.moveTo(plotL, y); ctx.lineTo(plotR, y); ctx.stroke();
      ctx.fillStyle = TEXT; ctx.textAlign = "left";
      ctx.fillText(fmtAxis(p), plotR + 4, y);
    }

    // time axis
    ctx.fillStyle = TEXT; ctx.textAlign = "center";
    const tlabels = 4;
    for (let i = 0; i <= tlabels; i++) {
      const t = t0 + span * (i / tlabels);
      const x = xOf(t);
      ctx.strokeStyle = GRID; ctx.beginPath(); ctx.moveTo(x, plotT); ctx.lineTo(x, plotB); ctx.stroke();
      ctx.fillText(fmtTime(t), x, plotB + 8);
    }

    // session dividers (prediction round boundaries)
    if (this.sessionDuration > 0) {
      const durSec = this.sessionDuration / 1000;
      const first = Math.ceil(t0 / durSec) * durSec;
      ctx.save();
      for (let t = first; t <= t1 + 0.5; t += durSec) {
        const x = xOf(t);
        ctx.strokeStyle = "rgba(240,185,11,.55)";
        ctx.lineWidth = 1; ctx.setLineDash([3, 4]);
        ctx.beginPath(); ctx.moveTo(x, plotT); ctx.lineTo(x, plotB); ctx.stroke();
        ctx.setLineDash([]);
        ctx.fillStyle = "rgba(240,185,11,.9)"; ctx.textAlign = "center"; ctx.textBaseline = "top";
        const d = new Date(t * 1000);
        const p = (n) => String(n).padStart(2, "0");
        ctx.fillText(p(d.getHours()) + ":" + p(d.getMinutes()), x, plotT + 1);
      }
      ctx.restore();
    }

    // candles or line
    if (this.type === "candle") {
      for (let i = r.start; i < Math.min(r.end, n); i++) {
        const c = cs[i];
        const x = xOf(c.time);
        const up = c.close >= c.open;
        ctx.strokeStyle = up ? UP : DOWN; ctx.fillStyle = up ? UP : DOWN;
        ctx.lineWidth = 1;
        ctx.beginPath(); ctx.moveTo(x, yOf(c.high)); ctx.lineTo(x, yOf(c.low)); ctx.stroke();
        const yo = yOf(c.open), yc = yOf(c.close);
        const top = Math.min(yo, yc), bh = Math.max(1, Math.abs(yc - yo));
        ctx.fillRect(x - barW / 2, top, barW, bh);
      }
    } else {
      ctx.strokeStyle = ACCENT; ctx.lineWidth = 1.5; ctx.beginPath();
      for (let i = r.start; i < Math.min(r.end, n); i++) {
        const x = xOf(cs[i].time), y = yOf(cs[i].close);
        i === r.start ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
      }
      ctx.stroke();
    }

    // decision (lock) line
    if (this.decision != null) {
      const y = yOf(this.decision);
      ctx.strokeStyle = ACCENT; ctx.lineWidth = 1; ctx.setLineDash([2, 3]);
      ctx.beginPath(); ctx.moveTo(plotL, y); ctx.lineTo(plotR, y); ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = ACCENT; ctx.textAlign = "left"; ctx.textBaseline = "bottom";
      ctx.fillText("LOCK " + fmtAxis(this.decision), plotL + 4, y - 2);
      ctx.textBaseline = "middle";
    }

    // auto trend line (regression across recent window) — shows momentum direction
    if (this.trendFit.length >= 2) {
      const tf = this.trendFit;
      const col = tf[tf.length - 1].value >= tf[0].value ? UP : DOWN;
      ctx.strokeStyle = col; ctx.globalAlpha = 0.5; ctx.lineWidth = 1.25;
      ctx.beginPath();
      tf.forEach((pt, i) => { const x = xOf(pt.time), y = yOf(pt.value); i ? ctx.lineTo(x, y) : ctx.moveTo(x, y); });
      ctx.stroke(); ctx.globalAlpha = 1;
    }

    // projection line extended to the prediction session end
    if (this.projection.length >= 2) {
      const p = this.projection;
      const col = p[1].value > (this.decision != null ? this.decision : p[0].value) ? UP : DOWN;
      ctx.strokeStyle = col; ctx.lineWidth = 1.5; ctx.setLineDash([5, 4]);
      ctx.beginPath();
      p.forEach((pt, i) => { const x = xOf(pt.time), y = yOf(pt.value); i ? ctx.lineTo(x, y) : ctx.moveTo(x, y); });
      ctx.stroke(); ctx.setLineDash([]);
    }

    // markers (swing highs / lows). marker.below draws under the point.
    this.markers.forEach((m) => {
      const x = xOf(m.time), y = yOf(m.value);
      ctx.fillStyle = m.color || ACCENT;
      if (m.below) {
        ctx.beginPath(); ctx.moveTo(x, y + 5); ctx.lineTo(x - 4, y + 12); ctx.lineTo(x + 4, y + 12); ctx.closePath(); ctx.fill();
        ctx.textAlign = "center"; ctx.textBaseline = "top";
        ctx.fillText(m.text || "", x, y + 13);
        ctx.textBaseline = "middle";
      } else {
        ctx.beginPath(); ctx.moveTo(x, y - 5); ctx.lineTo(x - 4, y - 12); ctx.lineTo(x + 4, y - 12); ctx.closePath(); ctx.fill();
        ctx.textAlign = "center"; ctx.textBaseline = "bottom";
        ctx.fillText(m.text || "", x, y - 13);
        ctx.textBaseline = "middle";
      }
    });

    // last price tag
    const last = cs[Math.min(r.end - 1, n - 1)];
    const ly = yOf(last.close);
    ctx.fillStyle = last.close >= last.open ? UP : DOWN;
    ctx.fillRect(plotR, ly - 8, padR, 16);
    ctx.fillStyle = "#0b0e11"; ctx.textAlign = "left"; ctx.textBaseline = "middle";
    ctx.font = "10px -apple-system,Segoe UI,Roboto,sans-serif";
    ctx.fillText(fmtAxis(last.close), plotR + 4, ly);

    // hint navigasi + indikator lazy-load
    ctx.font = "11px -apple-system,Segoe UI,Roboto,sans-serif";
    ctx.textBaseline = "top"; ctx.textAlign = "left";
    if (this._loadingStart) {
      ctx.fillStyle = "rgba(240,185,11,.95)";
      ctx.fillText("Loading previous session…", plotL + 8, plotT + 6);
    } else if (this.follow) {
      ctx.fillStyle = "rgba(132,142,156,.6)";
      ctx.fillText("← pull to load previous session", plotL + 8, plotT + 6);
    }

    // crosshair
    if (this.cross) {
      const mx = this.cross.x;
      if (mx >= plotL && mx <= plotR) {
        const t = t0 + ((mx - plotL) / plotW) * span;
        let ci = r.start;
        for (let i = r.start; i < Math.min(r.end, n); i++) if (cs[i].time <= t) ci = i;
        const c = cs[ci];
        const cx = xOf(c.time);
        ctx.strokeStyle = "rgba(132,142,156,.5)"; ctx.lineWidth = 1; ctx.setLineDash([3, 3]);
        ctx.beginPath(); ctx.moveTo(cx, plotT); ctx.lineTo(cx, plotB); ctx.stroke();
        ctx.setLineDash([]);
        if (this.onCrosshair) this.onCrosshair({ time: c.time, candle: c });
      }
    }
  };

  CanvasChart.prototype._bindEvents = function () {
    const self = this;

    function pos(e) {
      const rect = self.canvas.getBoundingClientRect();
      const px = (e.touches ? e.touches[0].clientX : e.clientX) - rect.left;
      const py = (e.touches ? e.touches[0].clientY : e.clientY) - rect.top;
      return { x: px, y: py };
    }
    let downX = null, maxMove = 0;
    function down(e) {
      const p = pos(e);
      downX = p.x; maxMove = 0;
      self.follow = false;
      self._panX0 = p.x;                 // fixed reference pointer x
      self._panAnchor0 = self._leftTime(); // fixed left-edge time at grab
      self.canvas.style.cursor = "grabbing";
    }
    function move(e) {
      const p = pos(e);
      self.cross = p;
      if (downX != null) {
        maxMove = Math.max(maxMove, Math.abs(p.x - downX));
        const pxPerBar = (self.W - 70) / self.visible;
        const dxBars = (self._panX0 - p.x) / pxPerBar;   // how many bars the content shifts
        const barSec = self._barSec();
        const n = self.candles.length;
        if (n) {
          const firstT = self.candles[0].time;
          const lastT = self.candles[n - 1].time;
          const liveLeft = self.candles[clamp(n + self.rightOffset - self.visible, 0, n - 1)].time;
          // allow panning FORWARD into the (empty) future; cap so the last candle can reach the left edge
          const maxAnchor = lastT + self.visible * barSec;
          // drag LEFT = older data (anchorTime decreases), drag RIGHT = newer data
          self.anchorTime = clamp(self._panAnchor0 - dxBars * barSec, firstT, maxAnchor);
          // dragging BACK into the live region → resume following
          if (self.anchorTime <= liveLeft) { self.anchorTime = null; self.follow = true; }
          // lazy-load: reached the start of loaded data → request older candles
          if (self.anchorTime != null && self.onReachStart && !self._loadingStart && !self.follow) {
            const leftT = self._leftTime();
            if (leftT <= firstT + barSec) {
              self._loadingStart = true;
              self.onReachStart(function () { self._loadingStart = false; });
            }
          }
        }
      }
      self.render();
      if (e.cancelable) e.preventDefault();
    }
    function up() {
      if (maxMove < 6) { self.follow = true; self.anchorTime = null; self.render(); } // tap (no drag) = back to live
      downX = null;
      self.canvas.style.cursor = "grab";
    }

    self.canvas.addEventListener("mousedown", down);
    self.canvas.addEventListener("mousemove", move);
    window.addEventListener("mouseup", up);
    self.canvas.addEventListener("touchstart", down, { passive: true });
    self.canvas.addEventListener("touchmove", move, { passive: false });
    self.canvas.addEventListener("touchend", up);
    self.canvas.addEventListener("wheel", (e) => {
      const f = e.deltaY > 0 ? 1.15 : 0.87;
      self.visible = clamp(Math.round(self.visible * f), 20, 400);
      self.render(); e.preventDefault();
    }, { passive: false });
  };

  function fmtAxis(p) {
    if (p == null || isNaN(p)) return "—";
    const d = p >= 1000 ? 2 : p >= 1 ? 3 : 5;
    return Number(p).toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d });
  }
  function fmtTime(t) {
    const d = new Date(t * 1000);
    const p = (n) => String(n).padStart(2, "0");
    return p(d.getHours()) + ":" + p(d.getMinutes());
  }

  window.CanvasChart = CanvasChart;
})();
