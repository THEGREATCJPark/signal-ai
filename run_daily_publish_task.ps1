$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$LogDir = Join-Path $Root "logs"
$LogFile = Join-Path $LogDir "signal_daily_publish.log"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

$Python = if ($env:AI_FRONTIER_PYTHON) { $env:AI_FRONTIER_PYTHON } else { "python" }
$StartedAt = Get-Date -Format "yyyy-MM-dd HH:mm:ss K"
Add-Content -Path $LogFile -Encoding UTF8 -Value "[$StartedAt] windows scheduler starting daily publish"

Push-Location $Root
try {
    & $Python scripts\daily_publish_local.py --platform both --allow-partial *>> $LogFile
    $ExitCode = $LASTEXITCODE
}
finally {
    Pop-Location
}

$FinishedAt = Get-Date -Format "yyyy-MM-dd HH:mm:ss K"
Add-Content -Path $LogFile -Encoding UTF8 -Value "[$FinishedAt] windows scheduler finished daily publish exit=$ExitCode"
exit $ExitCode
