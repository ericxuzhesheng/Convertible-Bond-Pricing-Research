# setup_weekly_task.ps1
# ============================================================
# 一次性运行此脚本，在 Windows 任务计划程序中注册严格增量周更新任务
#
# 运行方式（当前用户）：
#   cd 到 backtest 目录 → .\setup_weekly_task.ps1
#
# 注销任务：
#   Unregister-ScheduledTask -TaskName "ConvertibleBond_WeeklyUpdate" -Confirm:$false
# ============================================================

param([string]$PythonExe = "$env:USERPROFILE\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe")
$ErrorActionPreference = "Stop"

$taskName   = "ConvertibleBond_WeeklyUpdate"
$batchFile  = Join-Path $PSScriptRoot "weekly_update.bat"
$launcher = Join-Path $PSScriptRoot "run_scheduled_weekly.ps1"
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

# ── 触发器：每周五 16:30（本机时区应为北京时间） ─────────────────────────────
# 若当周五为法定节假日，任务仍会触发；脚本本身会在无新数据时跳过 git 提交。
if ((Get-TimeZone).Id -ne 'China Standard Time') { throw 'Set the intended local timezone before registering the 16:30 task.' }
& $launcher -PythonExe $PythonExe -CheckOnly
if ($LASTEXITCODE -ne 0) { throw 'Weekly runtime preflight failed.' }
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Friday -At "16:30"

# ── 操作：隐藏 PowerShell 窗口，使用已验证环境运行 batch ──────────────────────
$action = New-ScheduledTaskAction `
    -Execute "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe" `
    -Argument "-NoProfile -NonInteractive -WindowStyle Hidden -File `"$launcher`" -PythonExe `"$PythonExe`"" `
    -WorkingDirectory (Split-Path $PSScriptRoot -Parent)

# ── 设置项 ───────────────────────────────────────────────────────────────────
# 注: -WakeToRun 是 switch 参数，默认即为关闭；写成 "-WakeToRun $false" 会导致参数绑定错误
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Hours 6) `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -MultipleInstances IgnoreNew

# ── 注册任务（当前用户需已登录；不存储密码、不要求管理员权限） ──────────────
$principal = New-ScheduledTaskPrincipal `
    -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) `
    -LogonType Interactive `
    -RunLevel Limited

Register-ScheduledTask `
    -TaskName $taskName `
    -Trigger $trigger `
    -Action $action `
    -Settings $settings `
    -Principal $principal `
    -Description "每周五16:30本地更新行情、定价、策略、周报和README；验证通过才提交推送GitHub。" `
    -Force | Out-Null

Write-Host ""
Write-Host "任务注册成功！" -ForegroundColor Green
Write-Host "  任务名称  : $taskName"
Write-Host "  触发时间  : 每周五 16:30"
Write-Host "  执行脚本  : $batchFile"
Write-Host "  日志目录  : $logDir"
Write-Host ""
Write-Host "查看任务状态: Get-ScheduledTask -TaskName '$taskName'"
Write-Host "立即测试运行: Start-ScheduledTask -TaskName '$taskName'"
Write-Host "注销任务    : Unregister-ScheduledTask -TaskName '$taskName' -Confirm:`$false"
