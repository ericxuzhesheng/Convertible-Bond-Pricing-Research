# setup_weekly_task.ps1
# ============================================================
# 一次性运行此脚本，在 Windows 任务计划程序中注册周更新定时任务
#
# 运行方式（以管理员权限）：
#   右键 PowerShell → 以管理员身份运行 → cd 到 backtest 目录 → .\setup_weekly_task.ps1
#
# 注销任务：
#   Unregister-ScheduledTask -TaskName "ConvertibleBond_WeeklyUpdate" -Confirm:$false
# ============================================================

$ErrorActionPreference = "Stop"

$taskName   = "ConvertibleBond_WeeklyUpdate"
$batchFile  = Join-Path $PSScriptRoot "weekly_update.bat"
$logDir     = Join-Path $PSScriptRoot "logs"

# 确保 logs 目录存在
if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir | Out-Null
}

# ── 校验 batch 文件存在 ──────────────────────────────────────────────────────
if (-not (Test-Path $batchFile)) {
    Write-Error "找不到 weekly_update.bat：$batchFile"
    exit 1
}

# ── 触发器：每周五 18:00 ─────────────────────────────────────────────────────
# 若当周五为法定节假日，任务仍会触发；脚本本身会在无新数据时跳过 git 提交。
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Friday -At "18:00"

# ── 操作：以 cmd.exe 运行 batch ──────────────────────────────────────────────
$schedulerLog = Join-Path $logDir "scheduler.log"
$action = New-ScheduledTaskAction `
    -Execute "cmd.exe" `
    -Argument "/c `"$batchFile`""

# ── 设置项 ───────────────────────────────────────────────────────────────────
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Hours 4) `
    -MultipleInstances IgnoreNew `
    -WakeToRun $false

# ── 注册任务（以当前登录用户身份运行，需要时输入密码） ───────────────────────
$principal = New-ScheduledTaskPrincipal `
    -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) `
    -LogonType Interactive `
    -RunLevel Highest

Register-ScheduledTask `
    -TaskName $taskName `
    -Trigger $trigger `
    -Action $action `
    -Settings $settings `
    -Principal $principal `
    -Description "可转债数据周更新：拉取数据、运行模型、重生成图表、提交推送 GitHub" `
    -Force | Out-Null

Write-Host ""
Write-Host "任务注册成功！" -ForegroundColor Green
Write-Host "  任务名称  : $taskName"
Write-Host "  触发时间  : 每周五 18:00"
Write-Host "  执行脚本  : $batchFile"
Write-Host "  日志目录  : $logDir"
Write-Host ""
Write-Host "查看任务状态: Get-ScheduledTask -TaskName '$taskName'"
Write-Host "立即测试运行: Start-ScheduledTask -TaskName '$taskName'"
Write-Host "注销任务    : Unregister-ScheduledTask -TaskName '$taskName' -Confirm:`$false"
