@echo off
:: ============================================================
:: 可转债每日信号 — 每个交易日 15:30 后运行
:: 在任务计划程序中指向此文件
:: ============================================================

:: 切换到脚本目录
cd /d "%~dp0"

:: 设置企业微信 webhook (或通过任务计划的"操作→添加参数"传入环境变量)
:: set WXWORK_WEBHOOK_URL=https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY_HERE

:: 激活 conda 环境 (如需要，取消注释并修改环境名)
:: call conda activate base

:: 运行主脚本
python daily_signal.py >> logs\daily_signal.log 2>&1

:: 如果出错，保留窗口 (任务计划中通常不需要)
:: if errorlevel 1 pause
