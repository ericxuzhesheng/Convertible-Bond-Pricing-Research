@echo off
:: Convertible-bond weekly update:
:: market data -> BS -> ZL -> benchmark -> research outputs -> Git publish

setlocal EnableDelayedExpansion

set "BACKTEST_DIR=%~dp0"
set "REPO_DIR=%BACKTEST_DIR%.."
set "LOG_DIR=%BACKTEST_DIR%logs"
set "MPLBACKEND=Agg"
set "TORCH_PYTHON=C:\Users\12510\.conda\envs\torch\python.exe"
if exist "%TORCH_PYTHON%" (
    set "PYTHON=%TORCH_PYTHON%"
) else (
    set "PYTHON=python"
)

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

for /f "usebackq delims=" %%i in (`"%PYTHON%" -c "import json; from datetime import date, timedelta; d=date.fromisoformat(json.load(open(r'%BACKTEST_DIR%ZL_Model_Manifest.json', encoding='utf-8'))['input_cutoff']); print((d + timedelta(days=1)).strftime('%%Y%%m%%d'))"`) do set "PIPELINE_START=%%i"
for /f "usebackq delims=" %%i in (`"%PYTHON%" -c "import json; print(json.load(open(r'%BACKTEST_DIR%ZL_Model_Manifest.json', encoding='utf-8'))['input_cutoff'])"`) do set "MODEL_CUTOFF=%%i"
if not defined PIPELINE_START (
    echo [ERROR] Unable to resolve incremental pipeline start from ZL manifest.
    exit /b 1
)
if not defined MODEL_CUTOFF (
    echo [ERROR] Unable to resolve incremental model cutoff from ZL manifest.
    exit /b 1
)

for /f "tokens=2 delims==" %%i in ('wmic os get localdatetime /value') do set DT=%%i
set "DATESTAMP=%DT:~0,8%"
set "LOG_FILE=%LOG_DIR%\weekly_update_%DATESTAMP%.log"

(
echo.
echo ==============================================================
echo Weekly update started: %DATE% %TIME%
echo ==============================================================
) >> "%LOG_FILE%"

echo [1/6] Running data_pipeline.py ... >> "%LOG_FILE%"
"%PYTHON%" "%BACKTEST_DIR%data_pipeline.py" --start "%PIPELINE_START%" --weekly --reuse-clause-cache --reuse-conversion-event-cache >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
    echo [ERROR] data_pipeline.py failed. Stopping. >> "%LOG_FILE%"
    goto :fail
)

echo [2/6] Running B-S_backtest.py ... >> "%LOG_FILE%"
"%PYTHON%" "%BACKTEST_DIR%B-S_backtest.py" --weekly --incremental-after "%MODEL_CUTOFF%" >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
    echo [ERROR] B-S_backtest.py failed. Stopping. >> "%LOG_FILE%"
    goto :fail
)

echo [3/6] Running Z-L_backtest_GPU_prod.py ... >> "%LOG_FILE%"
"%PYTHON%" "%BACKTEST_DIR%Z-L_backtest_GPU_prod.py" --backend cuda --weekly --offline-inputs >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
    echo [ERROR] Z-L_backtest_GPU_prod.py failed. Stopping. >> "%LOG_FILE%"
    goto :fail
)

echo [4/6] Running update_benchmark.py ... >> "%LOG_FILE%"
"%PYTHON%" "%REPO_DIR%\long-short strategy\update_benchmark.py" >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
    echo [ERROR] update_benchmark.py failed. Stopping. >> "%LOG_FILE%"
    goto :fail
)

echo [5/6] Running rebuild_research_outputs.py ... >> "%LOG_FILE%"
"%PYTHON%" "%BACKTEST_DIR%rebuild_research_outputs.py" >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
    echo [ERROR] rebuild_research_outputs.py failed. Stopping. >> "%LOG_FILE%"
    goto :fail
)

echo [6/6] Publishing verified outputs ... >> "%LOG_FILE%"
cd /d "%REPO_DIR%"

git add -u >> "%LOG_FILE%" 2>&1
git add "backtest\regenerate_plots.py" >> "%LOG_FILE%" 2>&1
git add "backtest\weekly_update.bat" >> "%LOG_FILE%" 2>&1
git add "backtest\setup_weekly_task.ps1" >> "%LOG_FILE%" 2>&1
git add "long-short strategy\update_benchmark.py" >> "%LOG_FILE%" 2>&1
git add "long-short strategy\000832_CSI_close_price.csv" >> "%LOG_FILE%" 2>&1

git diff --cached --quiet
if errorlevel 1 (
    git commit -m "chore: weekly update %DATESTAMP%" >> "%LOG_FILE%" 2>&1
    git push origin main >> "%LOG_FILE%" 2>&1
    if errorlevel 1 (
        echo [ERROR] git push failed. Check network and credentials. >> "%LOG_FILE%"
        goto :fail
    )
) else (
    echo [SKIP] No changes to publish. >> "%LOG_FILE%"
)

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
