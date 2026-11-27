# AGENTS.md — Agent Instructions for Convertible Bond Pricing Research

This file tells Codex how to navigate and work with this codebase.

---

## Project Purpose

Absolute pricing research for Chinese A-share convertible bonds using four models:
- **Black-Scholes (BS)**: closed-form, offensive anchor (equity/vol driven)
- **ZL (project label)**: clause-stopping Monte Carlo policy baseline, defensive anchor
- **Least-Squares Monte Carlo (LSM)**: vectorized continuation-value regression and voluntary early conversion
- **CCB**: finite-difference pricing with observed curves and issuer-disclosed non-call protection

The pipeline goes: raw data → pricing → mispricing signal → long-short strategy.

Current verified local model cutoff and daily strategy-input cutoff are both **2026-11-27**. The production ensemble equally weights BS, ZL, LSM and CCB on the shared weekly calendar from 2019. The standalone CCB archive also includes 2017 and 2018. Pricing and six-factor text tables dated **2026-08-28** are archived evidence. Routine weekly work is strictly
incremental for market data and pricing. Recalculate derived factors, IC,
strategies, figures and README on every weekly update using the verified pricing
baseline. A full-history pricing rebuild requires an explicit maintainer request.

---

## Repository Layout

```
Convertible-Bond-Pricing-Research/
├── AGENTS.md                          ← you are here
├── README.md                          ← bilingual overview
├── backtest/                          ← PRIMARY working directory
│   ├── data_pipeline.py               ← Tushare data ingestion (run first)
│   ├── B-S_backtest.py                ← BS model pricing + output
│   ├── Z-L_backtest_GPU_prod.py       ← corrected clause-MC entrypoint (CPU)
│   ├── Z-L_backtest_CPU_prod.py       ← GitHub CPU incremental entrypoint
│   ├── LSM_backtest.py                 ← vectorized strict-incremental LSM driver
│   ├── ccb_weekly.py                   ← incremental CCB and CNInfo notice update
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
│   ├── weekly_value_strategy.py      ← independent weekly mean-value + 2% net-margin strategy
│   ├── weekly_value_results/         ← separate daily research and frozen tracking outputs
│   └── BS_ZL_LSM_strategy.py          ← legacy relative-ranking / monthly research
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

### Independent strategy-only backtest

~~~powershell
python "long-short strategy/weekly_value_strategy.py" --start 2019-01-01
~~~

This reads existing BS/ZL/LSM prices and DAILY strategy inputs only. Do not run
pricing, mispricing factors, IC rebuilds, or the full publishing pipeline for a
strategy-only request. Keep weekly_value_results separate from legacy results.
Methodology: [WEEKLY_VALUE_STRATEGY.md](long-short%20strategy/WEEKLY_VALUE_STRATEGY.md).

Defaults: 5bp one-way total friction; 2% valuation margin AFTER round-trip cost;
highest eligible quintile supplies at most floor(N*20%) long slots. Each long is
capped at 10%, so three names imply 30% target exposure. Unused capital stays cash.
Recheck frozen fair-value limits at the next session OPEN. The theoretical short
leg has matched target notional; disclose actual drift and locked positions.
Account DAILY and withhold formal metrics for unexplained marks. Never claim
complete total returns while historical coupons remain unverified. The 2% margin
is a predeclared research parameter, not a calibrated error bound.

AGENTS.md is the repository instruction source; no separate CLAUDE.md is maintained.

### Explicit maintenance only: full-history rebuild

```bash
# Never use this for a routine update. It requires an explicit maintainer request.
# Reprice cached observed inputs only; no data download or research rebuild.
python backtest/reprice_history.py --resume --promote
```

### Regenerate README plots only (no model recomputation)

```bash
# Reads existing XLSX/CSV outputs and regenerates the README images in seconds
python backtest/regenerate_plots.py
```

### Weekly automation (local Windows task)

Routine updates run locally every Friday at 16:30 Asia/Shanghai through
`run_scheduled_weekly.ps1` and `weekly_update.bat`. Register with
`backtest/setup_weekly_task.ps1`. The computer must be on, online and the
user logged in. Windows Task Scheduler triggers hourly from Friday 16:30
through Saturday 04:30. After successful publication the launcher skips the
remaining attempts for that week. Each attempt uses the same validation and
publishing guards. Weekly acquisition prefers Tushare pure-bond values and
fills undercovered dates with same-session Eastmoney valuations checked against
the primary close. It checks coverage before writing market caches and records
accepted alternate values in `backtest/cb_bond_floor_sources.csv`.
When Tushare's trade calendar is unavailable, pricing and strategy acquisition
use Sina's SSE calendar within the dates it explicitly covers.

The pipeline updates weekly inputs and volatility, runs independent BS/ZL/LSM/CCB
incremental pricing, updates the index benchmark, then stages and validates
factors, IC, strategies, reports, figures and README before publication.
`publish_weekly_outputs.py` selects generated outputs, including new reports,
and commits and pushes only after successful completion.

GitHub `weekly-incremental-cpu.yml` is a manual fallback with
`workflow_dispatch`; it has no scheduled trigger. The optional
`setup_main_sync_task.ps1` / `sync_main_from_github.ps1` scripts provide local
catch-up from GitHub.

### Incremental update (most common)

The routine scripts read the verified Manifest cutoff and compute only dates
strictly after it. Historical NaNs are not missing work and must never trigger a
weekly reprice. `data_pipeline.py` accepts `--start` / `--end` for an explicitly
bounded incremental range.

Every update must refresh `cb_basic` and compare the previous and current bond
universe: identify newly issued/listed bonds and bonds whose delisting date has
arrived. Include new bonds in pricing only after listing and observed quotes are
available; retain the strategy's existing listing-age and risk filters. Exclude
delisted bonds from new selections on or after their delisting date. Preserve
their historical prices and portfolio records, and never invent a sale for an
untradeable holding. Verify these changes against daily quotes and report the
actual pricing and daily-data cutoffs before committing or pushing. A midweek
update may extend daily accounting beyond the latest completed weekly pricing
date; do not treat an incomplete trading day as a published daily close.

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

`update_pricing_volatility.py --after YYYY-MM-DD` preserves the complete
existing `bs_volatility_cache.csv` history and merges only new observed weekly
volatility rows after the latest verified cutoff of all independent models.
A model ahead of its peers protects its historical missing cells. Never replace the cache with
an incremental slice or use a constant fallback. At most seven calendar days of
as-of carry is allowed for a suspended underlying; older inputs remain missing.
Historical repairs are explicit maintenance and retain observation dates.

### Corrected pricing v2

BS, ZL and LSM use `cashflow-pricing-v2` manifests, with independent input
fingerprints and artifact integrity checks. All three entrypoints are cache-only
and share contractual cash-flow utilities, not theoretical prices. LSM never
reads ZL model outputs. `update_pricing_volatility.py` refreshes new observed
weekly volatility once before the three independent runs.

Explicit maintainer-authorized historical repricing uses
`python backtest/reprice_history.py --resume --promote`. It builds in a separate
staging folder, verifies all three models, and records the superseded Git commit
before replacing pricing artifacts. Routine runs never initialize or overwrite
history. See [PRICING_METHODS.md](backtest/PRICING_METHODS.md) for model assumptions,
coverage controls and independent commands. The v1 CUDA algorithms are retired.

Every weekly update rebuilds derived research against the verified pricing
baseline. `Research_Migration.json` records pricing sources and the superseded
Git commit. New IC baselines are calculated in staging and validated before
promotion. Existing manifest guards are never deleted or bypassed.
The maintainer authorized a one-off 2026 report backfill on 2026-10-28.
Use `long-short strategy/rebuild_weekly_reports_2026.py` and the separate
`weekly_value_results_pricing_v2_2026` directory. These are retrospective
research reports, not historical frozen signals. Original prospective frozen
signals and confirmed tracking ledger prefixes remain protected during the
separately authorized full derived-research recalculation.

Four-model research reports are stored under `weekly_value_results_pricing_four_{year}`.
Prospective four-model tracking uses `weekly_value_results/tracking_four_models`.
CCB uses the earliest verified BS/ZL/LSM cutoff as its upper bound. It uses
only notices available at each valuation date and validates the archived
pricing and notice prefixes before appending new weeks. The research average
requires all four valid prices.

Pricing-only figures and `Pricing_Metrics.csv` use the identical valid bond-date
sample across models. Compare the ETF with portfolio NAVs in the key strategy
figure, never with cross-sectional model mean prices. Refresh `update_etf_benchmark.py` before full plotting; its
independent 511380.SH cache uses observed closes and same-date adjustment factors,
never pre-listing backfills. Preserve the full strategy NAV history; normalize
each strategy/ETF curve at its own first observed value and leave the ETF empty
before listing. Present this as a broad trend comparison with different starting points.

### Complex-model rebuilds: CUDA preference

For explicitly authorized full-history rebuilds of ZL, LSM and similarly complex
models, evaluate CUDA first when a supported NVIDIA GPU is available, and prefer
the validated GPU backend. Keep a CPU implementation as the correctness reference
and fallback; routine weekly updates remain strictly incremental and may use CPU.

GPU acceleration must preserve the current contractual cash flows, credit/equity
discounting, conversion dates, clause termination and independent LSM training /
valuation policy. Validate matched-path CPU/GPU results where possible, plus
multi-seed sampling-error and convergence checks. Do not trade away numerical
accuracy or silently lower path counts for speed. Record backend, algorithm and
sampling parameters in independent manifests before promoting results. Retired
v1 CUDA routines must not be re-enabled as a shortcut.

The corrected `pricing_cuda.py` port uses float64 path simulation and shares
the CPU continuation-value regression. Use `--lsm-backend cuda` and/or
`--zl-backend cuda` with `reprice_history.py`, or `--backend cuda` with an
independent MC entrypoint. Its xoroshiro128+ pair streams are separated by
2^64 steps and recorded in the model manifest. CPU incremental runs must retain
that recorded sampler when appending to a CUDA baseline.

### Factor diagnostics incremental logic

Each BS, ZL, and LSM factor run stores period-level IC/Rank IC history, a summary,
and an independent manifest under `mispricing factor/`. The manifest verifies the
method version, historical-input fingerprint, and history-file SHA-256. Once the
baseline exists, routine runs append only holding periods after `last_return_date`.
Do not delete or bypass a factor IC manifest to force a historical rewrite.
The complete weekly pipeline instead creates and validates a fresh staging
baseline before promotion, preserving the previous baseline in Git. This keeps
changing partial-month endpoints from accumulating overlapping IC periods.

### Windows console encoding

All scripts that `print()` Chinese or emoji must include this header:
```python
if hasattr(sys.stdout, "buffer") and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
```

---

## Environment Requirements

- Python 3.10+; production cloud uses Python 3.13
- Key packages: `tushare`, `akshare`, `pandas==2.3.3`, `numpy==2.2.6`, `scipy==1.16.3`, `numba==0.62.1`, `openpyxl`, `matplotlib`; corrected pricing does not require CUDA
- Tushare Pro token: provide `TUSHARE_TOKEN` or the ignored local file `backtest/tushare_token.txt`; never hard-code or commit it
- No extra packages needed for email (uses stdlib `smtplib`)

---

## Data Source

Pricing uses **Tushare Pro API** as the main market-data source:
- `pro.cb_daily()` — bond prices, conversion values, balance, volume; when `cb_price_chg` is unavailable, calculate it from adjacent closes and optionally correct it with a free quote source
- `pro.cb_basic()` — static info (coupon rate, maturity date, stock mapping)
- `pro.daily_basic()` — stock market cap
- `pro.rating()` — credit ratings
- `pro.fina_indicator()` — BPS (quarterly, forward-filled to daily)
- `akshare.bond_zh_us_rate()` — risk-free yield curve

The independent strategy additionally caches DAILY OHLCV, financial vintages,
redemption notices, verified cashflows and suspensions in backtest/strategy_inputs.
AkShare-backed Sina, Eastmoney, Jisilu and CNInfo provide cross-checks and event
supplements. Current snapshots must not be projected backward onto old signals.

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
16:30 Friday
  │
  ├─ manifest cutoff          ← 读取已验证截止日，只允许后续新交易周
  ├─ data_pipeline.py         ← 从截止日次日起增量更新真实市场数据
  ├─ update_pricing_volatility.py ← 为三模型补充新增真实周度波动率
  ├─ B-S_backtest.py          ← 仅更新截止日后的 BS 周度定价
  ├─ Z-L_backtest_GPU_prod.py ← CPU 仅更新截止日后的 ZL 周度定价
  ├─ LSM_backtest.py          ← 校验独立 Manifest，独立计算新增市场周度截面
  │
  ├─ update_benchmark.py      ← 更新 000832.CSI 基准
  │
  ├─ research                ← 暂存重算因子、IC、策略与周报，校验后更新图表和 README
  │
  └─ publish_weekly_outputs.py ← 选择本次生成的更新产物
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
| 触发 | 每周五 16:30 |
| 执行 | 隐藏 PowerShell 运行 `run_scheduled_weekly.ps1` |
| 登录类型 | Interactive（当前用户，需已登录） |
| 网络要求 | 仅在网络可用时运行 |
| 超时 | 6 小时 |
| 失败补跑 | 每小时触发，至周六 04:30，成功后跳过本周剩余触发 |

When Friday is a public holiday, the completed pricing date is the last open
session of that week. If there are no new completed sessions, acquisition skips
market-cache writes and the downstream stages can resume from verified inputs.

---

## Common Pitfalls

1. **Relative path bug**: Using `"filename.csv"` instead of `os.path.join(PIPELINE_DIR, "filename.csv")` causes cache misses when scripts are run from the project root.
2. **ZL history-repricing bug**: historical NaNs are not missing work. Weekly eligibility must use dates strictly later than the manifest cutoff before constructing the pending-cell mask.
3. **rf_yield tenor mismatch**: If you ever `reindex(columns=df_price.columns)` on the yield cache, all values become NaN and fallback `fillna(0.02)` kicks in silently.
4. **GBK encoding crash**: Chinese characters or emoji in `print()` crash on Windows GBK console; use the `io.TextIOWrapper` header or ASCII-only strings.
5. **Tushare rate limits**: Bulk historical fetches hit per-minute limits; always batch by year and add `time.sleep(0.5)` between calls.
6. **Git push authentication**: Scheduled tasks run in background; configure git credential store (`git config --global credential.helper manager`) or SSH key before registering the task, otherwise `git push` silently fails.
7. **Incremental cache truncation**: never write an incremental volatility/data slice directly over a full cache; merge with existing history first.
