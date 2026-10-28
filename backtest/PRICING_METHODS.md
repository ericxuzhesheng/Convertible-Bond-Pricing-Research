# Corrected pricing contract v2

BS, ZL and LSM read observed caches independently. None reads another model's
theoretical price. Routine runs append only dates after their own verified
manifest cutoff. An algorithm change requires an explicit new baseline.

## Prices and cash flows

All prices are full exchange quotes per bond. Conversion value, face amount,
contractual maturity redemption, dated coupons and conversion start/end dates
remain separate inputs. Maturity redemption already contains the final coupon;
it is not added twice. YYYYMMDD and YYYYMMDD.0 dates are parsed as calendar
dates rather than nanoseconds since 1970.

The model converts annual effective government yields with `log1p(yield)`
and interpolates these continuous zero-rate proxies to form
`D(t) = exp(-interpolated_continuous_rate(t) * t)`. It does not claim a bootstrapped
discount curve. A nonnegative cash-credit spread is calibrated to the observed
pure-bond value using the same contractual cash flows and discount convention.
No credit spread is fitted to the convertible market price. An observation
above its contractual risk-free cash-flow value is excluded and reported;
it is not silently assigned zero credit spread.

Cash payments use `D(t) * exp(-spread*t)` and conversion into shares uses
`D(t)`. This is a TF-style approximation. It is not an explicit issuer-default,
stock-jump or recovery model. Volatility uses at most 250 adjusted closing-price
observations (249 log returns), at least 60 prices, and square-root-250
annualization. A missing stock quote on the valuation date may use the latest
already observed volatility within seven calendar days; older observations stay
missing. Repair records retain the source observation date and age. No future
quote or constant-volatility fallback is used. Zero dividend yield is a declared
conversion-value-process assumption.

## BS

The European-conversion baseline uses the actual conversion expiry and the
remaining contractual cash value at that expiry, not an arbitrary face-100
strike. Coupons before conversion expiry are separate cash flows. The terminal
cash/equity probabilities are evaluated under the same risk-neutral process.
Zero volatility is evaluated analytically; volatility and maturity are never
artificially increased to avoid division by zero. Issuer call and holder put
options are deliberately outside this European baseline.

## ZL clause-Monte-Carlo baseline

The ZL label is retained for the project's clause-stopping Monte Carlo baseline.
It is not described as an exact reproduction of a published game-option model.
Observed rolling call counts and consecutive put counts seed each simulated
path. Calls cannot qualify before conversion starts. Coupons stop at contract
termination. Cash redemption and share conversion are discounted separately.
Conversion is also evaluated at the last permitted conversion observation;
waiting until a later maturity payment must not erase the holder's final choice.
The current policy assumes the issuer calls after its observed price trigger,
with a disclosed 30-observation notice period, and the holder uses a qualifying
put. These are policy assumptions, not forecasts of board decisions.
An explicit contractual prohibition on price-triggered holder puts is respected;
it is not replaced by a default put trigger. Full contractual text specifying only a change-of-use put likewise creates no price-triggered put. Fixed full put prices (for example 103 including interest) are distinct from par-plus-accrual call prices. Ordinal start years are measured from issue date. Accrual restarts at contractual
payment dates, including semiannual payment dates where applicable.

## LSM

LSM uses the same contractual path rules, with voluntary conversion and optional
put exercise determined by continuation-value regression. Issuer calls and
maturity terminate the path before an exercise decision can restore future
option value. Conversion starts/ends and time-zero conversion are enforced.
Intermediate coupons are included in continuation cash flows.
Only the training cohort determines whether and how a continuation regression
is fitted. Evaluation-path survival never gates training decisions. Voluntary
exercise below the contract's minimum forced cash payment times its worst
remaining cash discount is dominated and excluded; this bound uses contractual
cash flows, not another model's price. Regression runs on the remaining
candidates with separate training/evaluation predictions.

The basis is `1, S/S0, (S/S0)^2, call-count/window, put-count/window,
remaining-notice/notice-length`. Regression is trained on 8,192 antithetic
paths and the resulting policy is valued on a separate 8,192 paths. No
evaluation-path future cash flow enters the fitted regression. There are 192
base exercise intervals, augmented with conversion boundaries and coupon
observations. Clause observations use 252 steps per year. This is a discrete
exercise approximation; path-count and exercise-grid convergence must be
checked rather than claiming an exact American value.

There is no `max(ZL, LSM)` overlay. The only time-zero lower bound enforced is
an actually permitted immediate conversion. Monte Carlo standard errors are
computed from independent antithetic pair means, not from individual correlated
paths. LSM standard errors condition on the fitted exercise policy; they do not
measure training-policy uncertainty, exercise-grid bias or model misspecification.
ZL uses 10,000 paths by default; BS has no Monte Carlo error.

`Pricing_Convergence.csv` compares nine observed 2026-10-23 contracts spanning
the conversion-value distribution. Against 32,768 training plus 32,768 valuation
paths and 384 exercise intervals, the production 8,192/192 setting is checked
with independent seeds; the CPU MT19937 diagnostic's largest observed difference
is 0.329 yuan. The independent CUDA xoroshiro diagnostic's maximum is also
0.329 yuan (see `Pricing_CUDA_Validation.json`).
This is a numerical diagnostic, not a universal
accuracy bound or evidence that a 2% strategy margin is calibrated.

The production sampler uses Numba's thread-local MT19937 stream, seeded per
contract, with disjoint antithetic pairs. A proposed, unpublished v2.1 sampler
was rejected because its shifted per-pair seeds produced overlapping streams.
Across 128 seeded European reference runs, replacing it reduced empirical
sampling dispersion from 2.120 to 0.503 yuan; the production mean reported
standard error is 0.510 yuan. This check and the timing comparison are preserved
in `Pricing_Numerical_Validation.json`; v2.1 staging output is not publishable.
Precomputing deterministic drift/diffusion per time step gives a median 1.162x
speedup over the per-path calculation in ten warm timing cases, with bitwise
identical prices and standard errors; no paths or exercise dates are removed.

## Data limitations and validation

Future discretionary conversion-price resets and unverified waiver periods are
not invented. The present event cache does not establish complete historical
waiver coverage. These limitations also prevent a claim of exact replication
of the broker's full LSM or CCBA model. Verified announcement/history ingestion
is a separate requirement for empirical issuer-behaviour calibration.

The cached private-placement contract 124017.SZ remains incomplete. Its
[2022-11-24 issuer notice](https://static.cninfo.com.cn/finalpage/2022-11-24/1215187048.PDF)
confirms a 100.10 gross maturity payout, but that notice alone does not repair
the historical nonstandard conversion/put terms. Those cells remain excluded
rather than being treated as a standard public convertible contract.

Every observed-input-valid cell must produce a positive finite result; numerical
failures stop publication. Missing/inconsistent sources produce NaNs with
date/code/reason in `*_Pricing_Issues.csv`. Total quoted-universe coverage is
reported separately in `*_Pricing_Coverage.csv`. Publication floors are 95%
for BS and the pre-existing 84% before 2021 / 90% from 2021 for clause models.
These are input-coverage controls, not pricing-accuracy claims.

Fingerprints use platform-independent LF serialization; clause whitespace is
normalized consistently with parsing. Git preserves pricing artifact bytes.
Listing/delisting dates later than the verified cutoff do not alter that past
prefix; changes to events on or before the cutoff still stop incremental runs.
Shared input refreshes begin after the latest cutoff among the independent
models, preserving historical missing cells even when one model has advanced
ahead of the others. A lagging model prices the already cached weeks itself.
Each model has independent prices, market prices, deviations, cash spreads,
Monte Carlo errors, coverage, issues, workbook and manifest. Manifest validation
covers the input prefix, algorithm parameters, workbook hash and CSV artifact
hashes. A no-new-date run does not rewrite output files.

## Operation and migration

```powershell
# Explicit, cache-only full baseline. Old results must be tracked and unchanged.
python backtest/reprice_history.py --resume --promote

# CUDA LSM full rebuild; retain already verified BS/ZL in this staging folder.
python backtest/reprice_history.py --stage backtest/logs/pricing_cuda_final --lsm-backend cuda --workers 4 --resume

# Independent normal weekly pricing; no initialization or history rewrite.
python backtest/update_pricing_volatility.py
python backtest/B-S_backtest.py --weekly
python backtest/Z-L_backtest_CPU_prod.py --weekly
python backtest/LSM_backtest.py --weekly
```

`reprice_history.py` builds in a separate staging directory and verifies all
three models before promotion. `Pricing_Migration.json` records the exact Git
commit containing the superseded prices. There is no data download or report
rebuild in this operation. The corrected engine is CPU-validated; retired v1
CUDA/API entrypoints fail explicitly instead of emitting stale formulas.

The corrected `pricing_cuda.py` backend accelerates path simulation in float64,
with fast math disabled; continuation-value regressions use the same CPU code.
It uses Numba's xoroshiro128+ initialization with pair streams separated by
2^64 steps. Antithetic partners copy the same initial state into separate
thread-owned slots, so differing stopping times cannot race or overlap another
pair's stream. Box-Muller uses float64 uniforms (the legacy Numba normal helper
internally used float32 uniforms). Training and evaluation use disjoint pairs.
The manifest records this sampler separately from the CPU MT19937 sampler.
CPU weekly runs automatically replay the sampler recorded in their own baseline;
switching hardware does not silently change historical model parameters.

`--backend cuda` is available on either independent MC entrypoint. A supported
NVIDIA device, driver and CUDA toolkit are required; CPU runs do not require a
toolkit. The local RTX 5060 Laptop GPU was checked with CUDA 12.8. Matched-draw
CPU/GPU contractual cases and a 128-seed European sampling check accompany
the nine-contract convergence diagnostic in `Pricing_CUDA_Validation.json`.
The CPU-only GitHub weekly workflow remains supported.

## Price diagnostics and ETF reference

`Pricing_Metrics.csv` evaluates all three models on identical positive, finite
bond-date cells. MSE is mean squared price deviation (yuan squared); RMSE and
MAE are in yuan; MAPE is mean absolute percentage deviation; bias is model minus
market price. R-squared uses the market-price mean as its constant baseline.
These describe differences from observed quotes, not known fair-value errors.

`update_etf_benchmark.py` appends observed 511380.SH closes and exact-date
adjustment factors from Tushare `fund_daily` / `fund_adj`. The independent cache
manifest records provenance and byte integrity. No value is backfilled before
the ETF's actual 2020-04-07 listing. Required observed pricing dates must be
covered. The token needs the provider's ETF endpoint permission (currently
5,000 points for `fund_daily`). Pricing-only weekly publication refreshes this
cache before rendering the figures.

The ETF comparison normalizes each series to 100 on its first common observed
week. Cross-sectional mean convertible prices change with the observed bond
universe and are not portfolio-return indices. Distribution whiskers show the
5th/95th percentiles, with every common valid cell entering the statistics.
Annual averages use equal bond-date weights. Monte Carlo error panels show
cross-sectional medians and 95th percentiles, not confidence bands for the whole
pricing model. `regenerate_plots.py --pricing-only` updates these diagnostics
and the metrics table without touching research or weekly reports.

The v1 factor IC manifests, frozen signals, strategy ledgers and weekly reports
are retained unchanged. Their results are not re-labelled as v2 research.
`regenerate_plots.py --pricing-only` refreshes the three pricing figures without
touching research figures. Automatic research rebuilding is paused during the pricing migration; re-enable
it only after an explicit versioned IC/strategy migration. Pricing-only cloud
updates remain incremental. Do not delete an IC manifest to make it accept
different historical model prices.

## Method and exchange sources

- [Longstaff and Schwartz, 2001](https://people.math.ethz.ch/~hjfurrer/teaching/LongstaffSchwartzAmericanOptionsLeastSquareMonteCarlo.pdf)
- [Convertible valuation and credit-model caveats, Ayache/Forsyth/Vetzal](https://cs.uwaterloo.ca/~paforsyt/convert.pdf)
- [SSE convertible trading rules, full-price quotations](https://www.sse.com.cn/lawandrules/sselawsrules2025/bond/convertible/trading/c/c_20250606_10782995.shtml)
- [SZSE bond rules, full-price convertible quotations](https://investor.szse.cn/lawrules/rule/bond/t20220127_591060.html)
