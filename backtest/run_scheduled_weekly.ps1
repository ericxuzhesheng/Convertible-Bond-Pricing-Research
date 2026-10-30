param(
    [Parameter(Mandatory=$true)][string]$PythonExe,
    [switch]$CheckOnly
)
$ErrorActionPreference = 'Stop'
if (-not (Test-Path -LiteralPath $PythonExe -PathType Leaf)) { throw 'Configured Python is missing.' }
$env:CB_PYTHON = $PythonExe
$env:PYTHONPATH = "$PSScriptRoot\logs\research_runtime;$PSScriptRoot\logs\data_runtime;$PSScriptRoot\logs\pricing_runtime;$PSScriptRoot\logs\plot_runtime;$PSScriptRoot"
# Scheduler and interactive preflight must see the same dependencies.
$env:PYTHONNOUSERSITE = '1'
$env:PYTHONIOENCODING = 'utf-8'
$env:OPENBLAS_NUM_THREADS = '1'
$env:OMP_NUM_THREADS = '1'
$env:MPLBACKEND = 'Agg'
$env:GIT_TERMINAL_PROMPT = '0'
$env:GCM_INTERACTIVE = 'Never'
New-Item -ItemType Directory -Force -Path "$PSScriptRoot\logs" | Out-Null
Start-Transcript -Path "$PSScriptRoot\logs\weekly_scheduler.log" -Append | Out-Null
Set-Location (Split-Path $PSScriptRoot -Parent)
& $PythonExe -X utf8 -c "import pandas,numpy,scipy,numba,matplotlib,openpyxl,tushare,akshare,requests,tqdm; from token_loader import load_tushare_token; assert load_tushare_token(), 'Missing Tushare token'; assert pandas.__version__ == '2.3.3'; print('Weekly runtime and token availability verified')"
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
if ($CheckOnly) {
    & "$PSScriptRoot\weekly_update.bat" --check
} else {
    & "$PSScriptRoot\weekly_update.bat"
}
exit $LASTEXITCODE
