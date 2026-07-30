param(
    [string]$RepositoryRoot = (
        Resolve-Path (Join-Path $PSScriptRoot "..")
    ).Path
)

$ErrorActionPreference = "Stop"
$logDirectory = Join-Path $PSScriptRoot "logs"
$logPath = Join-Path $logDirectory "main_sync.log"
New-Item -ItemType Directory -Path $logDirectory -Force | Out-Null

function Write-SyncLog {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Add-Content -LiteralPath $logPath -Value "[$timestamp] $Message"
}

try {
    $resolvedRoot = (Resolve-Path -LiteralPath $RepositoryRoot).Path
    Push-Location -LiteralPath $resolvedRoot
    try {
        git fetch origin main
        if ($LASTEXITCODE -ne 0) {
            throw "git fetch origin main failed"
        }

        $branch = (git branch --show-current).Trim()
        if ($branch -ne "main") {
            Write-SyncLog "Fetched origin/main; merge skipped on branch $branch."
            exit 2
        }

        $dirty = git status --porcelain --untracked-files=no
        if ($LASTEXITCODE -ne 0) {
            throw "git status --porcelain failed"
        }
        if ($dirty) {
            Write-SyncLog "Fetched origin/main; merge skipped because the worktree is dirty."
            exit 2
        }

        git merge --ff-only origin/main
        if ($LASTEXITCODE -ne 0) {
            throw "git merge --ff-only origin/main failed"
        }
        Write-SyncLog "Local main synchronized successfully."
    }
    finally {
        Pop-Location
    }
}
catch {
    Write-SyncLog "Synchronization failed: $($_.Exception.Message)"
    exit 1
}
