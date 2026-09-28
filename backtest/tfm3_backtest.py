#!/usr/bin/env python3.11
"""
BACKTEST TimesFM-3 (zero-shot) vs sinyal yang kita pakai sekarang.

Tugas: untuk setiap sesi 5m, prediksi arah CLOSE vs OPEN (= LOCK):
    actual = "up" bila close >= open   (open sesi = lock, diketahui di t0)
    pred   = "up" bila ramalan TimesFM untuk close sesi > open

Varian:
  V1 univariate   : context = close BTC saja (N candle 5m terakhir sebelum sesi)
  V2 multivariate : context = [close BTC, close ETH] (TimesFM-3 memakai korelasi antar seri)
  V1c/V2c         : hanya prediksi berkeyakinan tinggi (|ramalan-open| besar relatif skala)

Baseline pada data yang sama: momentum (candle lalu), reversal, selalu up.

Zero-shot murni: TIDAK ada fine-tuning.
Output: ringkasan winrate + simpan prediksi ke out/tfm3_pred%s.json" % ("_tail%d" % TAIL if TAIL else "")
"""
import json, math, os, time
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "data")
OUT = os.path.join(HERE, "out")
MODEL_DIR = os.environ.get("TFM_DIR", "/tmp/tfm3")
N_CTX = int(os.environ.get("N_CTX", "512"))
STRIDE = int(os.environ.get("STRIDE", "1"))
BATCH = int(os.environ.get("BATCH", "64"))
LIMIT = int(os.environ.get("LIMIT", "0"))
TAIL = int(os.environ.get("TAIL", "0"))   # 0 = semua; >0 = hanya N sesi terakhir (utk head-to-head 7d)
H = 1


INTERVAL = os.environ.get("INTERVAL", "5m")


def load(sym):
    with open(os.path.join(DATA, f"{sym}_{INTERVAL}.json")) as f:
        return json.load(f)


def ci95(p, n):
    return 1.96 * math.sqrt(max(p * (1 - p), 1e-12) / max(n, 1))


def main():
    btc = load("BTC")
    eth = load("ETH")
    eth_map = {c["time"]: c for c in eth}
    pairs = [(b, eth_map[b["time"]]) for b in btc if b["time"] in eth_map]
    NSEC = {"5m": 300, "15m": 900, "1h": 3600}.get(INTERVAL, 300)
    print(f"sesi 5m berpasangan BTC/ETH: {len(pairs)}")

    idxs = list(range(N_CTX, len(pairs), STRIDE))
    if TAIL:
        idxs = idxs[-TAIL:]
    if LIMIT:
        idxs = idxs[:LIMIT]
    n = len(idxs)
    print(f"titik evaluasi: {n} (N_CTX={N_CTX}, stride={STRIDE})")

    actuals, opens, ctx_btc, ctx_eth = [], [], [], []
    for i in idxs:
        b = pairs[i][0]
        actuals.append(b["close"] >= b["open"])
        opens.append(b["open"])
        ctx_btc.append(np.array([p[0]["close"] for p in pairs[i - N_CTX:i]], dtype=np.float32))
        ctx_eth.append(np.array([p[1]["close"] for p in pairs[i - N_CTX:i]], dtype=np.float32))
    actuals = np.array(actuals)
    opens = np.array(opens, dtype=np.float64)

    # ---------- baseline ----------
    mom = rev = up = 0
    for k, i in enumerate(idxs):
        prev_up = pairs[i - 1][0]["close"] >= pairs[i - 1][0]["open"]
        mom += (prev_up == actuals[k])
        rev += ((not prev_up) == actuals[k])
        up += actuals[k]
    print("\n=== BASELINE (data sama) ===")
    print(f"  momentum (searah candle lalu) : {mom/n*100:.2f}%  ({mom}/{n})")
    print(f"  reversal (kontra candle lalu) : {rev/n*100:.2f}%")
    print(f"  selalu up                     : {up/n*100:.2f}%  (base rate up)")

    if not os.path.exists(os.path.join(MODEL_DIR, "model.safetensors")):
        print(f"\n(bobot belum ada di {MODEL_DIR} — bagian TimesFM dilewati)")
        return

    from timesfm import TimesFM3Forecaster
    t0 = time.time()
    f = TimesFM3Forecaster.from_pretrained(MODEL_DIR, device="cpu")
    cfg = getattr(f, "config", None)
    print(f"\nmodel dimuat {time.time()-t0:.1f}s · config: {getattr(cfg, 'input_patch_length', '?')}/{getattr(cfg, 'output_patch_length', '?')}")

    # cari nama field ramalan titik pada output
    probe = f.predict(ctx_btc[0], horizon=H, return_quantiles=True)
    fields = [a for a in dir(probe) if not a.startswith("_")]
    print("  field output:", fields)
    pfield = next((k for k in ("mean", "point", "forecast", "median", "prediction") if k in fields), None)
    if pfield is None:
        raise RuntimeError("tidak menemukan field ramalan titik")

    def run(contexts, covs=None, label=""):
        preds = np.empty(len(contexts), dtype=np.float64)
        t = time.time()
        for s in range(0, len(contexts), BATCH):
            chunk = contexts[s:s + BATCH]
            cv = covs[s:s + BATCH] if covs is not None else None
            got = 0
            for o in f.predict_batch(chunk, horizon=H, return_quantiles=True, past_future_covariates=cv):
                v = np.asarray(getattr(o, pfield)).ravel()
                preds[s + got] = float(v[0])
                got += 1
            if s % (BATCH * 8) == 0:
                done = min(s + BATCH, len(contexts))
                el = time.time() - t
                print(f"    {label} {done}/{len(contexts)} · {el:.0f}s · {done/max(el,1e-9):.1f} kps", flush=True)
        return preds

    # ---------- V1 univariate ----------
    print("\n=== V1 univariate (context BTC saja) ===")
    p1 = run(ctx_btc, None, "V1")
    w1 = ((p1 > opens) == actuals).mean()
    print(f"  winrate arah: {w1*100:.2f}%  ±{ci95(w1,n)*100:.2f}  (n={n})")

    # ---------- V2 multivariate (bisa dilewati utk run besar) ----------
    SKIP_V2 = os.environ.get("SKIP_V2", "0") == "1"
    if SKIP_V2:
        print("\n(V2 dilewati: SKIP_V2=1)")
        p2 = p1
        w2 = w1
    else:
        print("\n=== V2 multivariate (context [BTC, ETH]) ===")
        multi = [np.stack([a, b]) for a, b in zip(ctx_btc, ctx_eth)]
        p2 = run(multi, None, "V2")
        w2 = ((p2 > opens) == actuals).mean()
        print(f"  winrate arah: {w2*100:.2f}%  ±{ci95(w2,n)*100:.2f}  (n={n})")

    # ---------- filter keyakinan (|ramalan-open| relatif skala ctx) ----------
    scale = np.array([np.std(c[-200:]) for c in ctx_btc], dtype=np.float64)
    conf = np.abs(p2 - opens) / np.maximum(scale, 1e-9)
    for q in (0.5, 0.7, 0.8, 0.9):
        thr = np.quantile(conf, q)
        sel = conf >= thr
        if sel.sum() > 30:
            w = ((p2[sel] > opens[sel]) == actuals[sel]).mean()
            print(f"\n  V2 keyakinan>={q*100:.0f}th percentile (n={sel.sum()}): winrate {w*100:.2f}% ±{ci95(w,sel.sum())*100:.2f}")

    os.makedirs(OUT, exist_ok=True)
    pred_path = os.path.join(OUT, "tfm3_pred" + (f"_{INTERVAL}" if INTERVAL != "5m" else "") + (f"_tail{TAIL}" if TAIL else "") + ".json")
    with open(pred_path, "w") as fh:
        json.dump({
            "n": n, "n_ctx": N_CTX, "stride": STRIDE, "tail": TAIL, "interval": INTERVAL,
            "baseline": {"momentum": mom/n, "reversal": rev/n, "always_up": up/n},
            "v1_uni": w1, "v2_multi": w2,
            "pred_v2": p2.tolist(), "opens": opens.tolist(), "actual_up": actuals.astype(int).tolist(),
            "conf": conf.tolist(),
        }, fh)
    print(f"\n-> prediksi disimpan ke {pred_path}")


if __name__ == "__main__":
    main()
