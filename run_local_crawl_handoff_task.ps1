$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$LogDir = Join-Path $Root "logs"
$LogFile = Join-Path $LogDir "signal_local_crawl_handoff.log"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

$Config = if ($env:DISCORD_EXPORT_CONFIG) { $env:DISCORD_EXPORT_CONFIG } else { Join-Path $Root "discord_export_config.env" }
if (Test-Path $Config) {
    Get-Content $Config | ForEach-Object {
        if ($_ -match '^DISCORD_TOKEN=(.*)$') {
            $env:DISCORD_TOKEN = $Matches[1]
        }
    }
}

$Python = if ($env:AI_FRONTIER_PYTHON) { $env:AI_FRONTIER_PYTHON } else { "python" }
$StartedAt = Get-Date -Format "yyyy-MM-dd HH:mm:ss K"
Add-Content -Path $LogFile -Encoding UTF8 -Value "[$StartedAt] windows scheduler starting local crawl handoff"

Push-Location $Root
try {
    & $Python scripts\local_crawl_handoff_gate.py -- $Python scripts\dispatch_local_crawl_handoff.py *>> $LogFile
    $ExitCode = $LASTEXITCODE
}
finally {
    Pop-Location
}

$FinishedAt = Get-Date -Format "yyyy-MM-dd HH:mm:ss K"
Add-Content -Path $LogFile -Encoding UTF8 -Value "[$FinishedAt] windows scheduler finished local crawl handoff exit=$ExitCode"
exit $ExitCode
