/* ============================================================================
   PEMERIKSA SCOPE — mencegah bug kelas "ReferenceError: X is not defined" yang hanya muncul
   saat runtime (mis. liveSig dipakai di renderDual, sessionStart dipakai di updateSignal).

   Cara kerja: untuk setiap fungsi (termasuk yang bersarang), hitung identifier yang DILIHAT
   (rantai scope: deklarasi sendiri + fungsi induk + tingkat modul + global) lalu laporkan
   identifier yang:
     - dipakai di dalam fungsi itu, DAN
     - dideklarasikan DI SUATU TEMPAT di file ini (const/let/var/function), TETAPI
     - tidak terlihat dari rantai scope fungsi tersebut.
   Heuristik ini hampir tanpa false positive (identifier harus benar-benar dideklarasi di file).

   CATATAN: alat ini masih memberi false positive pada fungsi bersarang / IIFE (deklarasi di
   dalam blok bersarang belum semuanya tertangkap). Pakai sebagai PETUNJUK saja. Pemeriksaan
   yang otoritatif: jalankan aplikasi di browser nyata lalu tangkap window.onerror/console.error
   (mis. via diag headless) — itu menangkap SEMUA ReferenceError saat runtime.

   Pakai: node backtest/check_scope.js [file]
   ============================================================================ */
const fs = require("fs");
const file = process.argv[2] || "app.js";
const raw = fs.readFileSync(file, "utf8");

// ---- bersihkan komentar & literal string supaya kata di dalamnya tidak dianggap kode ----
const src = raw
  .replace(/`(?:\\[\s\S]|[^`\\])*`/g, (m) => "``" + "\n".repeat((m.match(/\n/g) || []).length))
  .replace(/"(?:\\.|[^"\\\n])*"/g, '""')
  .replace(/'(?:\\.|[^'\\\n])*'/g, "''")
  .replace(/\/\*[\s\S]*?\*\//g, (m) => m.replace(/[^\n]/g, " "))
  .replace(/\/\/[^\n]*/g, "");

const KEYWORDS = new Set("const let var function async await return if else for while do switch case default break continue try catch finally throw new delete void typeof instanceof in of class extends super this import export from as static get set yield debugger with not and or true false null undefined NaN Infinity".split(" "));
const GLOBALS = new Set("window document console Math JSON Date Object Array Number String Boolean Promise Map Set WeakMap WeakSet Intl RegExp Error TypeError RangeError isFinite isNaN parseInt parseFloat setTimeout setInterval clearTimeout clearInterval requestAnimationFrame cancelAnimationFrame requestIdleCallback queueMicrotask localStorage sessionStorage navigator location history Notification AudioContext webkitAudioContext WebSocket EventSource Event CustomEvent getComputedStyle customElements alert confirm prompt btoa atob encodeURIComponent decodeURIComponent structuredClone performance Blob File Image Audio MutationObserver ResizeObserver IntersectionObserver crypto fetch URL URLSearchParams AbortController TextEncoder TextDecoder CanvasRenderingContext2D OffscreenCanvas".split(" "));

// ---- daftar deklarasi tingkat modul + daftar SEMUA deklarasi di file ----
const allDeclared = new Set();
const moduleVisible = new Set();
const reDecl = /(?:\b(?:const|let|var)\s+|^\s*function\s+|\bfunction\s+|^\s*async\s+function\s+|\bclass\s+)([A-Za-z_$][\w$]*)/gm;
for (const m of src.matchAll(reDecl)) { allDeclared.add(m[1]); if (/^(?:const|let|var)\s|^function\s/.test(m[0].trim()) || /^\s+/.test(m[0])) { /* noop */ } }
// deklarasi tingkat modul = baris yang diawali tanpa indentasi
for (const m of src.matchAll(/^(?:const|let|var|function|async function|class)\s+([A-Za-z_$][\w$]*)/gm)) moduleVisible.add(m[1]);
for (const m of src.matchAll(/^(?:const|let|var)\s*\{([^}]*)\}/gm)) m[1].split(",").forEach((x) => moduleVisible.add(x.trim().split(":")[0].trim()));
for (const m of src.matchAll(/^\s*(?:const|let|var|function|async function|class)\s+([A-Za-z_$][\w$]*)/gm)) allDeclared.add(m[1]);
for (const m of src.matchAll(/\b(?:const|let|var)\s*\{([^}]*)\}/g)) m[1].split(",").forEach((x) => { const t = x.trim().split(":")[0].trim(); if (/^[A-Za-z_$][\w$]*$/.test(t)) allDeclared.add(t); });
for (const m of src.matchAll(/\(([^)]*)\)\s*(?:=>|\{)/g)) m[1].split(",").forEach((x) => { const t = x.trim().split("=")[0].trim().replace(/^\.\.\./, ""); if (/^[A-Za-z_$][\w$]*$/.test(t)) allDeclared.add(t); });
for (const m of src.matchAll(/(?:^|[^\w.$])([A-Za-z_$][\w$]*)\s*=>/g)) allDeclared.add(m[1]);
for (const m of src.matchAll(/catch\s*\(\s*([A-Za-z_$][\w$]*)/g)) allDeclared.add(m[1]);
for (const m of src.matchAll(/for\s*\(\s*(?:const|let|var)\s+([A-Za-z_$][\w$]*)/g)) allDeclared.add(m[1]);

// ---- temukan semua fungsi + rentang body ----
const funcs = [];
{
  const re = /\bfunction\s+([A-Za-z_$][\w$]*)?\s*\(([^)]*)\)/g;
  let m;
  while ((m = re.exec(src))) {
    const open = src.indexOf("{", m.index + m[0].length - 1);
    if (open < 0) continue;
    let depth = 0, end = -1;
    for (let i = open; i < src.length; i++) {
      if (src[i] === "{") depth++;
      else if (src[i] === "}") { depth--; if (depth === 0) { end = i; break; } }
    }
    if (end < 0) continue;
    funcs.push({ name: m[1] || "(anon)", params: m[2], start: m.index, bodyStart: open, end });
  }
}

const declIn = (text) => {
  const d = new Set();
  for (const m of text.matchAll(/\b(?:const|let|var|function|async function|class)\s+([A-Za-z_$][\w$]*)/g)) d.add(m[1]);
  for (const m of text.matchAll(/\b(?:const|let|var)\s*\{([^}]*)\}/g)) m[1].split(",").forEach((x) => { const t = x.trim().split(":")[0].trim(); if (/^[A-Za-z_$][\w$]*$/.test(t)) d.add(t); });
  for (const m of text.matchAll(/\b(?:const|let|var)\s*\[([^\]]*)\]/g)) m[1].split(",").forEach((x) => { const t = x.trim().split("=")[0].trim(); if (/^[A-Za-z_$][\w$]*$/.test(t)) d.add(t); });
  // daftar parameter TANPA tanda kurung bersarang (hindari salah tangkap a.reduce((x,y)=>..))
  for (const m of text.matchAll(/\(\s*([A-Za-z_$][\w$]*(?:\s*,\s*[A-Za-z_$][\w$]*)*)\s*\)\s*(?:=>|\{)/g)) m[1].split(",").forEach((x) => { const t = x.trim(); if (t) d.add(t); });
  for (const m of text.matchAll(/(?:^|[^\w.$])([A-Za-z_$][\w$]*)\s*=>/g)) d.add(m[1]);
  for (const m of text.matchAll(/catch\s*\(\s*([A-Za-z_$][\w$]*)/g)) d.add(m[1]);
  for (const m of text.matchAll(/for\s*\(\s*(?:const|let|var)?\s*([A-Za-z_$][\w$]*)\s+(?:of|in)\s/g)) d.add(m[1]);
  return d;
};

let problems = 0;
for (const f of funcs) {
  const body = src.slice(f.bodyStart + 1, f.end);
  // rantai scope: deklarasi fungsi ini + semua fungsi INDUK yang membungkusnya + modul + global
  const visible = new Set([...moduleVisible, ...KEYWORDS, ...GLOBALS, ...declIn(body)]);
  f.params.split(",").forEach((p) => { const t = p.trim().split("=")[0].trim(); if (t) visible.add(t); });
  for (const g of funcs) if (g !== f && g.bodyStart < f.start && g.end > f.end) { for (const d of declIn(src.slice(g.bodyStart + 1, g.end))) visible.add(d); }
  const unknown = [];
  for (const u of body.matchAll(/(?<![\w.$"'`])([A-Za-z_$][\w$]*)(?!\s*:)/g)) {
    const x = u[1];
    if (!visible.has(x) && allDeclared.has(x)) unknown.push(x);
  }
  if (unknown.length) {
    problems++;
    console.log(`  ${f.name}()  ->  ${[...new Set(unknown)].join(", ")}   (ADA di file, tapi TIDAK terlihat dari fungsi ini)`);
  }
}

console.log(`\n${file}: diperiksa ${funcs.length} fungsi · ${problems} fungsi dengan identifier di luar scope`);
process.exit(problems ? 1 : 0);
