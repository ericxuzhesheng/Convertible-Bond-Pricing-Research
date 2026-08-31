# AGENTS.md — Agent Instructions for Convertible Bond Pricing Research

This file tells Codex how to navigate and work with this codebase.

---

## Project Purpose

Absolute pricing research for Chinese A-share convertible bonds using three models:
- **Black-Scholes (BS)**: closed-form, offensive anchor (equity/vol driven)
- **Zheng-Lin (ZL)**: Monte Carlo optimal stopping, defensive anchor (clause-aware)
- **Least-Squares Monte Carlo (LSM)**: vectorized continuation-value regression and voluntary early conversion

The pipeline goes: raw data → pricing → mispricing signal → long-short strategy.

Current published vintage: **2026-08-28**. Routine weekly work is strictly
incremental; do not run a full-history rebuild unless a maintainer explicitly
requests one.

---

## Repository Layout

```
Convertible-Bond-Pricing-Research/
├── AGENTS.md                          ← you are here
├── README.md                          ← bilingual overview
├── backtest/                          ← PRIMARY working directory
│   ├── data_pipeline.py               ← Tushare data ingestion (run first)
│   ├── B-S_backtest.py                ← BS model pricing + output
│   ├── Z-L_backtest_GPU_prod.py       ← shared ZL driver (CUDA/CPU)
│   ├── Z-L_backtest_CPU_prod.py       ← GitHub CPU incremental entrypoint
│   ├── LSM_backtest.py                 ← vectorized strict-incremental LSM driver
│   ├── lsm_backend.py                  ← batched quadratic Longstaff-Schwartz engine
│   ├── Z-L_backtest_GPU.py            ← disabled legacy entrypoint
│   ├── full_history_rebuild.py        ← fail-closed full-history rebuild
│   ├── regenerate_plots.py            ← 一键重生成 README 图表（无需重跑模型）
│   ├── weekly_update.bat              ← 周更新主入口（数据→模型→图表→Git推送）
│   ├── setup_weekly_task.ps1          ← 一次性注册 Windows 任务计划程序
│   ├── logs/                          ← weekly_update.bat 日志
│   ├── cb_*.csv                       ← wide-format data caches (rows=date, cols=bond)
│   ├── rf_yield_cache.csv             ← risk-free yield curve (tenor format, not wide)
│   ├── bs_volatility_cache.csv        ← 250-day rolling vol for BS
│   ├── BS_Model_*.csv / .xlsx         ← BS model outputs
│   ├── ZL_Model_*.csv / .xlsx         ← ZL model outputs
│   └── LSM_Model_*.csv / .xlsx        ← LSM outputs + independent manifest
├── long-short strategy/
│   └── BS_ZL_LSM_strategy.py          ← three-model monthly rebalancing backtest
├── mispricing factor/
│   ├── B-S_mispricing_factor.py       ← 6-factor BS composite
│   ├── Z-L_mispricing_factor.py       ← 6-factor ZL composite
│   └── LSM_mispricing_factor.py       ← 6-factor LSM composite
├── summary/
│   └── key_findings.md                ← executive summary
└── report/                            ← full research PDF
```

---

## How to Run

### Explicit maintenance only: full-history rebuild

```bash
# Never use this for a routine update. It requires an explicit maintainer request.
# GPU preflight → rebuild observed Tushare/AkShare inputs → BS → GPU ZL → LSM
# → benchmark → factors → strategies → plots
python backtest/full_history_rebuild.py
```

### Regenerate README plots only (no model recomputation)

```bash
# Reads existing XLSX/CSV outputs and regenerates the README images in seconds
python backtest/regenerate_plots.py
```

### Weekly automation (GitHub cloud, with local catch-up)

```powershell
# GitHub Actions runs the complete incremental pipeline every Friday at 17:30
# Asia/Shanghai. Configure the repository TUSHARE_TOKEN secret once.

# Optional one-time local setup: fetch hourly and fast-forward a clean main.
cd backtest
.\setup_main_sync_task.ps1

# Manual local catch-up
.\sync_main_from_github.ps1

# Check local sync logs
Get-Content .\logs\main_sync.log -Tail 30
```

The GitHub `weekly-incremental-cpu.yml` pipeline:
1. `data_pipeline.py`, `B-S_backtest.py`, and `Z-L_backtest_CPU_prod.py` — incrementally update weekly observed inputs and BS/ZL prices
2. `LSM_backtest.py --weekly` — verifies its independent manifest and prices only later ZL dates
3. `long-short strategy/update_benchmark.py` — updates the 000832.CSI benchmark
4. `rebuild_research_outputs.py` — updates three model factors, append-only IC histories, strategies, and README plots
5. validation + `git commit && git push origin main` — publishes only complete verified changes

The Windows `weekly_update.bat` remains available as a manual CUDA path, but
the scheduled cloud update does not depend on the local computer being on.

### Incremental update (most common)

The routine scripts read the verified Manifest cutoff and compute only dates
strictly after it. Historical NaNs are not missing work and must never trigger a
weekly reprice. `data_pipeline.py` accepts `--start` / `--end` for an explicitly
bounded incremental range.

---

## Key Architecture Patterns

### Path convention

All scripts use `PIPELINE_DIR = os.path.dirname(os.path.abspath(__file__))` to
resolve paths. **Never use bare relative paths** like `"bs_volatility_cache.csv"` —
always `os.path.join(PIPELINE_DIR, "bs_volatility_cache.csv")`. This was a
previously fixed bug; do not regress it.

### Cache file formats

| File | Shape | Notes |
|------|-------|-------|
| `cb_price_cache.csv` | rows=trade_date, cols=ts_code | wide format |
| `cb_convert_val_cache.csv` | rows=trade_date, cols=ts_code | wide format |
| `cb_maturity_cache.csv` | rows=trade_date, cols=ts_code | years remaining |
| `cb_stock_mv_cache.csv` | rows=trade_date, cols=ts_code | 万元 |
| `cb_rating_cache.csv` | rows=trade_date, cols=ts_code | forward-filled |
| `rf_yield_cache.csv` | rows=trade_date, cols=tenor (1,3,5,7,10) | **NOT wide bond format** |
| `bs_volatility_cache.csv` | rows=ts_code, cols=trade_date or vice versa | 250-day rolling vol |

`rf_yield_cache.csv` is in tenor format (5 columns). Do **not** reindex it to
bond-column shape; instead interpolate by tenor at call time.

### ZL incremental logic

`Z-L_backtest_GPU_prod.py` reads the verified cutoff from
`ZL_Model_Manifest.json`. In weekly mode, eligible dates must be strictly later
than that cutoff; historical NaNs are legitimate skipped observations and must
not trigger repricing. Within eligible new dates, Monte Carlo runs only for valid
pending cells. When reindexing old cache DataFrames to match the current
`df_price` shape, always use:
```python
df.reindex(index=df_price.index, columns=df_price.columns)
```
Never `.reindex(df_price.index)` alone — that only reindexes rows and leaves old
bond columns intact, causing KeyError for new bonds.

### BS volatility cache

`B-S_backtest.py --weekly --incremental-after YYYY-MM-DD` preserves the complete
existing `bs_volatility_cache.csv` history and merges only new volatility rows.
Never replace the cache with an incremental slice. Missing entries are filled
with `0.40` (40% default).

### LSM incremental logic

`LSM_backtest.py --weekly` requires `LSM_Model_Manifest.json` after the one-time
explicit initialization. It verifies the historical input fingerprint, model
parameters, and summary-workbook SHA-256 before calculating dates strictly later
than its verified cutoff. If there are no later dates, it exits without rewriting
outputs. Never pass `--initialize-history` in routine automation.

### Factor diagnostics incremental logic

Each BS, ZL, and LSM factor run stores period-level IC/Rank IC history, a summary,
and an independent manifest under `mispricing factor/`. The manifest verifies the
method version, historical-input fingerprint, and history-file SHA-256. Once the
baseline exists, routine runs append only holding periods after `last_return_date`.
Do not delete or bypass a factor IC manifest to force a historical rewrite.

### Windows console encoding

All scripts that `print()` Chinese or emoji must include this header:
```python
if hasattr(sys.stdout, "buffer") and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
```

---

## Environment Requirements

- Python 3.9+
- Key packages: `tushare`, `akshare`, `pandas`, `numpy`, `scipy`, `numba==0.62.1`, `numba-cuda`, `openpyxl`, `matplotlib`
- Tushare Pro token: provide `TUSHARE_TOKEN` or the ignored local file `backtest/tushare_token.txt`; never hard-code or commit it
- No extra packages needed for email (uses stdlib `smtplib`)

---

## Data Source

All market data is fetched via **Tushare Pro API**:
- `pro.cb_daily()` — bond prices, conversion values, balance, volume; when `cb_price_chg` is unavailable, calculate it from adjacent closes and optionally correct it with a free quote source
- `pro.cb_basic()` — static info (coupon rate, maturity date, stock mapping)
- `pro.daily_basic()` — stock market cap
- `pro.rating()` — credit ratings
- `pro.fina_indicator()` — BPS (quarterly, forward-filled to daily)
- `akshare.bond_zh_us_rate()` — risk-free yield curve

Tushare has per-minute rate limits. `data_pipeline.py` uses `time.sleep()` between
batch calls. Do not remove these sleeps.

---

## Files NOT to Modify Without Care

- `backtest/Z-L_backtest_GPU.py` — disabled legacy entrypoint; do not use for research output
- `【浙商固收】转债资产端特征数据库【周更新外发】.xlsx` — legacy Excel source, kept for historical comparison
- `backtest/rf_yield_cache.csv` — tenor-format yield curve cache; format differs from other caches

---

## Weekly Automation Architecture

### Pipeline order in `weekly_update.bat`

```
18:00 Friday
  │
  ├─ manifest cutoff          ← 读取已验证截止日，只允许后续新交易周
  ├─ data_pipeline.py         ← 从截止日次日起增量更新真实市场数据
  ├─ B-S_backtest.py          ← 仅更新截止日后的 BS 周度定价
  ├─ Z-L_backtest_GPU_prod.py ← CUDA 仅更新截止日后的 ZL 周度定价
  ├─ LSM_backtest.py          ← 校验独立 Manifest，仅计算新增 ZL 周度截面
  │
  ├─ update_benchmark.py      ← 更新 000832.CSI 基准
  │
  ├─ rebuild_research_outputs.py
  │     ├─ build_observed_factors.py  ← 从 Tushare 日频缓存重建五个非定价因子
  │     ├─ BS/ZL/LSM factor backtests
  │     ├─ monthly long-short strategy
  │     └─ regenerate_plots.py        ← 重生成 README 图表与三模型 IC 对比图
  │
  └─ git add -u               ← 暂存所有已追踪的变更文件
       git commit              ← 仅在有实际变更时提交（跳过空提交）
       git push origin main    ← 推送到 GitHub
```

### `regenerate_plots.py` data sources

| 图表 | 数据来源 | Sheet/列 |
|------|---------|---------|
| Fig1_BS_Price_Time_Series.png | BS_Model_Summary.xlsx | 理论价格, 市场价格, 相对偏差 |
| Fig1_ZL_Price_Time_Series.png | ZL_Model_Summary.xlsx | 理论价格, 市场价格, 相对偏差 |
| Fig1_LSM_Price_Time_Series.png | LSM_Model_Summary.xlsx | 理论价格, 市场价格, 相对偏差 |
| BS_model_performance.png | mispricing factor/B-S_alpha_strategy_results.csv | benchmark_nav, long_nav, bs_deviation_nav |
| ZL_model_performance.png | mispricing factor/Z-L_alpha_strategy_results.csv | benchmark_nav, long_nav, zl_deviation_nav |
| LSM_model_performance.png | mispricing factor/LSM_alpha_strategy_results.csv | benchmark_nav, long_nav, lsm_deviation_nav |
| BS_factor_correlation.png | BS_Model_Summary.xlsx + five observed factor CSVs | Pearson + Spearman, direction-adjusted |
| ZL_factor_correlation.png | ZL_Model_Summary.xlsx + five observed factor CSVs | Pearson + Spearman, direction-adjusted |
| LSM_factor_correlation.png | LSM_Model_Summary.xlsx + five observed factor CSVs | Pearson + Spearman, direction-adjusted |
| factor_ic_comparison.png | BS/ZL/LSM_factor_ic_summary.csv | mean IC + mean Rank IC |

Factor CSV bond code format: `sh110030` → normalized to `110030.SH` by `_standardize_code()`.

### Windows Task Scheduler task details

| 属性 | 值 |
|------|-----|
| 任务名 | ConvertibleBond_WeeklyUpdate |
| 触发 | 每周五 18:00 |
| 执行 | `cmd.exe /c weekly_update.bat` |
| 登录类型 | Interactive（当前用户，需已登录） |
| 网络要求 | 仅在网络可用时运行 |
| 超时 | 4 小时 |

If Friday is a public holiday, `data_pipeline.py` fetches no new completed week,
the models find no dates after the verified cutoff, and
`git diff --cached --quiet` returns 0 — the commit is skipped automatically. No
special holiday handling is needed.

---

## Common Pitfalls

1. **Relative path bug**: Using `"filename.csv"` instead of `os.path.join(PIPELINE_DIR, "filename.csv")` causes cache misses when scripts are run from the project root.
2. **ZL history-repricing bug**: historical NaNs are not missing work. Weekly eligibility must use dates strictly later than the manifest cutoff before constructing the pending-cell mask.
3. **rf_yield tenor mismatch**: If you ever `reindex(columns=df_price.columns)` on the yield cache, all values become NaN and fallback `fillna(0.02)` kicks in silently.
4. **GBK encoding crash**: Chinese characters or emoji in `print()` crash on Windows GBK console; use the `io.TextIOWrapper` header or ASCII-only strings.
5. **Tushare rate limits**: Bulk historical fetches hit per-minute limits; always batch by year and add `time.sleep(0.5)` between calls.
6. **Git push authentication**: Scheduled tasks run in background; configure git credential store (`git config --global credential.helper manager`) or SSH key before registering the task, otherwise `git push` silently fails.
7. **Incremental cache truncation**: never write an incremental volatility/data slice directly over a full cache; merge with existing history first.
