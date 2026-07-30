param(
    [string]$TaskName = "ConvertibleBond_MainSync"
)

$ErrorActionPreference = "Stop"
$syncScript = (Resolve-Path (
    Join-Path $PSScriptRoot "sync_main_from_github.ps1"
)).Path
$powershell = Join-Path $env:SystemRoot (
    "System32\WindowsPowerShell\v1.0\powershell.exe"
)
$arguments = (
    "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden " +
    "-File `"$syncScript`""
)

$action = New-ScheduledTaskAction `
    -Execute $powershell `
    -Argument $arguments
$atLogOn = New-ScheduledTaskTrigger -AtLogOn
$hourly = New-ScheduledTaskTrigger `
    -Once `
    -At (Get-Date).AddMinutes(2) `
    -RepetitionInterval (New-TimeSpan -Hours 1) `
    -RepetitionDuration (New-TimeSpan -Days 3650)
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger @($atLogOn, $hourly) `
    -Settings $settings `
    -Description "Fetch and fast-forward local main after GitHub cloud updates." `
    -Force

Write-Output "Registered task: $TaskName"
