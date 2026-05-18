@echo off
:: ============================================================
:: 可转债周更新 — 每周最后一个交易日（通常周五）18:00 运行
:: 流程：数据拉取 → 模型定价 → 信号推送 → 重生成图表 → Git 提交推送
:: 在任务计划程序中通过 setup_weekly_task.ps1 一键注册
:: ============================================================

setlocal EnableDelayedExpansion

:: ── 路径设置 ─────────────────────────────────────────────────────────────────
set "BACKTEST_DIR=%~dp0"
set "REPO_DIR=%BACKTEST_DIR%.."
set "LOG_DIR=%BACKTEST_DIR%logs"
set "PYTHON=python"

:: 激活 conda 环境（如需要，取消注释并修改环境名）
:: call conda activate base

:: ── 日志文件 ──────────────────────────────────────────────────────────────────
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

for /f "tokens=2 delims==" %%i in ('wmic os get localdatetime /value') do set DT=%%i
set "DATESTAMP=%DT:~0,8%"
set "LOG_FILE=%LOG_DIR%\weekly_update_%DATESTAMP%.log"

(
echo.
echo ==============================================================
echo 周更新开始: %DATE% %TIME%
echo ==============================================================
) >> "%LOG_FILE%"

:: ── Step 1: 全量更新（数据 + 模型 + 信号推送） ──────────────────────────────
echo [1/3] 运行 daily_signal.py ... >> "%LOG_FILE%"
"%PYTHON%" "%BACKTEST_DIR%daily_signal.py" >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
    echo [警告] daily_signal.py 报告错误，继续执行后续步骤 >> "%LOG_FILE%"
) else (
    echo [完成] daily_signal.py 执行成功 >> "%LOG_FILE%"
)

:: ── Step 2: 重生成 README 全部图表 ───────────────────────────────────────────
echo [2/3] 运行 regenerate_plots.py ... >> "%LOG_FILE%"
"%PYTHON%" "%BACKTEST_DIR%regenerate_plots.py" >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
    echo [警告] regenerate_plots.py 报告错误，继续执行后续步骤 >> "%LOG_FILE%"
) else (
    echo [完成] regenerate_plots.py 执行成功 >> "%LOG_FILE%"
)

:: ── Step 3: Git 提交并推送 ────────────────────────────────────────────────────
echo [3/3] 提交更新到 GitHub ... >> "%LOG_FILE%"
cd /d "%REPO_DIR%"

:: 暂存所有已追踪的变更文件
git add -u >> "%LOG_FILE%" 2>&1

:: 追加暂存本次新增的文件（首次运行或新文件）
git add "backtest\regenerate_plots.py" >> "%LOG_FILE%" 2>&1
git add "backtest\weekly_update.bat" >> "%LOG_FILE%" 2>&1
git add "backtest\setup_weekly_task.ps1" >> "%LOG_FILE%" 2>&1
git add "backtest\top5_*.csv" >> "%LOG_FILE%" 2>&1

:: 检查是否有实际变更
git diff --cached --quiet
if errorlevel 1 (
    git commit -m "chore: weekly update %DATESTAMP%" >> "%LOG_FILE%" 2>&1
    git push origin main >> "%LOG_FILE%" 2>&1
    if errorlevel 1 (
        echo [错误] git push 失败，请检查网络或认证配置 >> "%LOG_FILE%"
    ) else (
        echo [完成] 推送成功 >> "%LOG_FILE%"
    )
) else (
    echo [跳过] 无新变更，跳过本次提交 >> "%LOG_FILE%"
)

(
echo ==============================================================
echo 周更新完成: %DATE% %TIME%
echo ==============================================================
echo.
) >> "%LOG_FILE%"
