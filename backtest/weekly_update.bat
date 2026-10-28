@echo off
:: Convertible-bond weekly update:
:: market data -> BS/ZL/LSM -> factors/IC -> strategies -> figures/README -> Git publish

setlocal EnableDelayedExpansion

set "BACKTEST_DIR=%~dp0"
set "REPO_DIR=%BACKTEST_DIR%.."
set "LOG_DIR=%BACKTEST_DIR%logs"
set "MPLBACKEND=Agg"
if defined CB_PYTHON (
    set "PYTHON=%CB_PYTHON%"
) else (
    set "PYTHON=python"
)

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

set "PIPELINE_START="
set "MODEL_CUTOFF="
for /f "usebackq tokens=1,2" %%i in (`""%PYTHON%" "%BACKTEST_DIR%update_pricing_volatility.py" --print-bounds"`) do (
    set "PIPELINE_START=%%i"
    set "MODEL_CUTOFF=%%j"
)
if not defined PIPELINE_START (
    echo [ERROR] Unable to resolve incremental pipeline start from model manifests.
    exit /b 1
)
if not defined MODEL_CUTOFF (
    echo [ERROR] Unable to resolve incremental model cutoff from model manifests.
    exit /b 1
)

if "%~1"=="--check" (
    echo [OK] Runtime and incremental bounds: %PIPELINE_START% %MODEL_CUTOFF%
    exit /b 0
)
for /f %%i in ('powershell.exe -NoProfile -Command "Get-Date -Format yyyyMMdd"') do set "DATESTAMP=%%i"
set "LOG_FILE=%LOG_DIR%\weekly_update_%DATESTAMP%.log"

(
echo.
echo ==============================================================
echo Weekly update started: %DATE% %TIME%
echo ==============================================================
) >> "%LOG_FILE%"

echo [1/7] Running data_pipeline.py ... >> "%LOG_FILE%"
"%PYTHON%" "%BACKTEST_DIR%data_pipeline.py" --start "%PIPELINE_START%" --weekly --reuse-clause-cache --reuse-conversion-event-cache >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
    echo [ERROR] data_pipeline.py failed. Stopping. >> "%LOG_FILE%"
    goto :fail
)

echo Refreshing shared observed volatility ... >> "%LOG_FILE%"
"%PYTHON%" "%BACKTEST_DIR%update_pricing_volatility.py" --after "%MODEL_CUTOFF%" >> "%LOG_FILE%" 2>&1
if errorlevel 1 goto :fail

echo [2/7] Running B-S_backtest.py ... >> "%LOG_FILE%"
"%PYTHON%" "%BACKTEST_DIR%B-S_backtest.py" --weekly --incremental-after "%MODEL_CUTOFF%" >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
    echo [ERROR] B-S_backtest.py failed. Stopping. >> "%LOG_FILE%"
    goto :fail
)

echo [3/7] Running Z-L_backtest_GPU_prod.py ... >> "%LOG_FILE%"
"%PYTHON%" "%BACKTEST_DIR%Z-L_backtest_GPU_prod.py" --backend cpu --weekly --offline-inputs >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
    echo [ERROR] Z-L_backtest_GPU_prod.py failed. Stopping. >> "%LOG_FILE%"
    goto :fail
)

echo [4/7] Running LSM_backtest.py ... >> "%LOG_FILE%"
"%PYTHON%" "%BACKTEST_DIR%LSM_backtest.py" --weekly >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
    echo [ERROR] LSM_backtest.py failed. Stopping. >> "%LOG_FILE%"
    goto :fail
)

echo [5/7] Running update_benchmark.py ... >> "%LOG_FILE%"
"%PYTHON%" "%REPO_DIR%\long-short strategy\update_benchmark.py" >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
    echo [ERROR] update_benchmark.py failed. Stopping. >> "%LOG_FILE%"
    goto :fail
)

echo [6/7] Recalculating factors, IC, strategies, weekly reports, figures and README ... >> "%LOG_FILE%"
"%PYTHON%" "%BACKTEST_DIR%rebuild_research_outputs.py" --full-history --refresh-inputs --promote >> "%LOG_FILE%" 2>&1
if errorlevel 1 goto :fail

echo [7/7] Publishing verified outputs ... >> "%LOG_FILE%"
cd /d "%REPO_DIR%"

"%PYTHON%" "%BACKTEST_DIR%publish_weekly_outputs.py" >> "%LOG_FILE%" 2>&1
if errorlevel 1 goto :fail

(
echo ==============================================================
echo Weekly update completed: %DATE% %TIME%
echo ==============================================================
echo.
) >> "%LOG_FILE%"
exit /b 0

:fail
(
echo ==============================================================
echo Weekly update failed: %DATE% %TIME%
echo No later stages were published.
echo ==============================================================
echo.
) >> "%LOG_FILE%"
exit /b 1
