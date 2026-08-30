# CLAUDE.md — Convertible Bond Pricing Research

Follow [`AGENTS.md`](AGENTS.md) as the single source of truth for repository
layout, data contracts, commands, and operational safeguards.

## Current published state

- Data and BS/ZL model outputs are updated through **2026-08-28**.
- The production ZL increment was run with CUDA; the supported local environment
  uses `numba==0.62.1` and `numba-cuda`.
- Routine weekly execution is **incremental only**. `backtest/weekly_update.bat`
  reads `backtest/ZL_Model_Manifest.json`, starts data ingestion on the following
  day, and prices only dates strictly after the verified cutoff.
- Historical NaNs are valid skipped cells, not permission to reprice history.
- `cb_price_chg` may be calculated from adjacent closes when the paid/source field
  is unavailable, with optional correction from a free quote source.
- Load the Tushare token from `TUSHARE_TOKEN` or the ignored
  `backtest/tushare_token.txt`; never hard-code or commit credentials.

## Routine commands

```powershell
# Local weekly CUDA update: data -> BS -> ZL -> benchmark -> factors -> plots -> publish
cd backtest
.\weekly_update.bat

# Rebuild figures from existing verified outputs without repricing
python backtest/regenerate_plots.py
```

`backtest/full_history_rebuild.py` is an explicit maintenance path. Do not run it
for ordinary weekly updates or merely to fill historical NaNs.
