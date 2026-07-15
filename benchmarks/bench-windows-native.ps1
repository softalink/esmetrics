# TSBS benchmark harness for native Windows hosts (mirror of bench.sh).
# Usage: powershell -File bench-windows-native.ps1 -Label go-win -ServerExe C:\bench\victoria-metrics-windows-amd64-prod.exe
param(
    [Parameter(Mandatory = $true)][string]$Label,
    [Parameter(Mandatory = $true)][string]$ServerExe
)
$ErrorActionPreference = "Stop"

$Bench = "C:\bench"
$DataDir = "$Bench\data"
$Results = "$Bench\results\$Label"
$Storage = "$Bench\storage-$Label"
$Port = 8428
$Workers = 4

New-Item -ItemType Directory -Force -Path $Results | Out-Null
if (Test-Path $Storage) { Remove-Item -Recurse -Force $Storage }

Write-Host "=== [$Label] starting server ==="
$server = Start-Process -FilePath $ServerExe -ArgumentList @(
    "-storageDataPath=$Storage", "-retentionPeriod=100y", "-httpListenAddr=:$Port"
) -RedirectStandardOutput "$Results\server-out.log" -RedirectStandardError "$Results\server-err.log" -PassThru -NoNewWindow

try {
    $ok = $false
    foreach ($i in 1..100) {
        try {
            Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$Port/health" -TimeoutSec 2 | Out-Null
            $ok = $true; break
        } catch { Start-Sleep -Milliseconds 300 }
    }
    if (-not $ok) { throw "server failed to start" }

    Write-Host "=== [$Label] load benchmark ==="
    # Native commands write progress to stderr; run via cmd /c with file
    # redirection so PowerShell's error preference can't abort the run.
    cmd /c "`"$Bench\tsbs_load_victoriametrics.exe`" --file=`"$DataDir\cpu-only-100h-1d.lp`" --urls=http://127.0.0.1:$Port/write --workers=$Workers --batch-size=10000 --results-file=`"$Results\load.json`" > `"$Results\load.txt`" 2>&1"
    Get-Content "$Results\load.txt" | Select-String 'loaded' | Write-Host

    Start-Sleep -Seconds 5
    try { Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$Port/internal/force_flush" -TimeoutSec 10 | Out-Null } catch {}
    Start-Sleep -Seconds 2

    $qfiles = Get-ChildItem "$DataDir\queries-*.dat" | Where-Object Length -gt 0 | Sort-Object Name
    foreach ($qf in $qfiles) {
        $qt = $qf.BaseName -replace '^queries-', ''
        Write-Host "=== [$Label] query benchmark: $qt ==="
        cmd /c "`"$Bench\tsbs_run_queries_victoriametrics.exe`" --file=`"$($qf.FullName)`" --workers=$Workers --urls=http://127.0.0.1:$Port --results-file=`"$Results\query-$qt.json`" > `"$Results\query-$qt.txt`" 2>&1"
    }
    Write-Host "=== [$Label] done ==="
} finally {
    Stop-Process -Id $server.Id -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 2
    if (Test-Path $Storage) { Remove-Item -Recurse -Force $Storage -ErrorAction SilentlyContinue }
}
