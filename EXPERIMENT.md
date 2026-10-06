# EXPERIMENT — Gate Eksperimental "MOMENTUM × TREND-AGREE"

Status: **FROZEN SPEC** (jangan ubah definisi selama periode pengumpulan data).
Tujuan: menguji apakah cohort eksperimental menghasilkan `$` net-positif OOS,
**tanpa menyentuh gate produksi**. Hasil di-paper-trade berdampingan (SHADOW).

## Definisi cohort (dibekukan)
Dihitung dari fitur yang tersedia **saat t0** (tanpa look-ahead), hanya untuk sesi
**berarah non-flat** (`skipped == null`, `dir ∈ {up,down}`):

| id | aturan |
|---|---|
| `TREND`    | `ind.emaCross == dir` **dan** `ind.macdDir == dir` |
| `MOM`      | `mv2 ≥ 0.015%` (gerak 2 detik) |
| `CORE`     | `TREND` **dan** `hourWIB ∉ BAD_HOURS` |
| `CORE_MOM` | `CORE` **dan** `mv2 ≥ 0.008%` |

`BAD_HOURS_WIB = {3, 5, 10, 12, 14, 21}` (WR < 52% pada data 9 hari).
Baseline pembanding: `PROD` = `gate.accepted` produksi.

## Model eksekusi (paper)
- Entry: `odds[dir]` saat t0 dikurangi **haircut** `EXP_SPREAD_PCT` (default 3%, mencakup spread+fee).
- Settlement: hold-to-settle memakai hasil nyata sesi (`res.actual`); ROI = `(1/p_eff - 1)` bila arah benar, `-1` bila salah.
- Sizing: **fixed-fractional 5%** per trade (compound) untuk metrik equity; juga laporkan EV per-trade.
- Portofolio tiap cohort **terpisah** (`data/exp.json`), tidak mencampur uang nyata.

## Kriteria lulus (keputusan setelah periode)
Setelah **≥600 sesi per cohort** (≈10–12 hari):
1. Wilson lower-bound WR > `avgEntryPrice`, DAN
2. EV per-trade **net** (setelah haircut) > 0 di paruh **test** (OOS), DAN
3. Equity test > equity baseline PROD pada basis yang sama.

Jika lulus → promosi ke strat/akun kedua (stake nyata kecil), scale bertahap.
Jika gagal → matikan cohort (`EXP_ENABLED=0`).

## Kill switch & reversibilitas
- `EXP_ENABLED=0` menghentikan pelacakan tanpa deploy.
- Semua perubahan Fase 1 **aditif** (field label `exp`), tidak mengubah `accepted`/`reject`.
- Tracker BOT hanya menulis `data/exp.json`; tidak pernah memanggil fungsi order nyata.
