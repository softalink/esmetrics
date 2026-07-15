# TSBS benchmark with resource monitoring on Windows (mirror of bench-monitored.sh).
# load -> settle+flush -> 10 query types -> force_merge, sampling the server
# process (CPU time, working set, peak working set). Expects the C:\bench layout
# of bench-windows-native.ps1 plus data\queries-*.dat. Summarize the samples
# with analyze-resources.py.
# Usage: powershell -ExecutionPolicy Bypass -File bench-monitored.ps1 -Label go1 -ServerExe C:\bench\victoria-metrics-windows-amd64-prod.exe
# Note: over ssh, pass -ExtraArgs via `powershell -Command "& 'C:\bench\bench-monitored.ps1' ... -ExtraArgs @('-a=1','-b=2')"` — the -File form comma-joins the array into a single argument.
param(
    [Parameter(Mandatory = $true)][string]$Label,
    [Parameter(Mandatory = $true)][string]$ServerExe,
    [string[]]$ExtraArgs = @()
)
$ErrorActionPreference = "Stop"

$Bench = "C:\bench"
$DataDir = "$Bench\data"
$Results = "$Bench\results-$Label"
$Storage = "$Bench\storage-$Label"
$Port = 8428
$Workers = 4

if (Test-Path $Results) { Remove-Item -Recurse -Force $Results }
New-Item -ItemType Directory -Force -Path $Results | Out-Null
if (Test-Path $Storage) { Remove-Item -Recurse -Force $Storage }

function Mark($name) {
    $t = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    Add-Content -Path "$Results\phases.txt" -Value "$name $t"
}

Write-Host "=== [$Label] starting server ==="
$server = Start-Process -FilePath $ServerExe -ArgumentList (@(
    "-storageDataPath=$Storage", "-retentionPeriod=100y", "-httpListenAddr=:$Port"
) + $ExtraArgs) -RedirectStandardOutput "$Results\server-out.log" -RedirectStandardError "$Results\server-err.log" -PassThru -NoNewWindow

$sampler = Start-Job -ScriptBlock {
    param($srvPid, $out)
    while ($true) {
        try { $p = Get-Process -Id $srvPid -ErrorAction Stop } catch { break }
        $t = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
        $cpu = $p.TotalProcessorTime.TotalSeconds
        Add-Content -Path $out -Value "$t $cpu $($p.WorkingSet64) $($p.PeakWorkingSet64)"
        Start-Sleep -Milliseconds 200
    }
} -ArgumentList $server.Id, "$Results\samples.txt"

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
    Mark "load_start"
    cmd /c "`"$Bench\tsbs_load_victoriametrics.exe`" --file=`"$DataDir\cpu-only-100h-1d.lp`" --urls=http://127.0.0.1:$Port/write --workers=$Workers --batch-size=10000 --results-file=`"$Results\load.json`" > `"$Results\load.txt`" 2>&1"
    Mark "load_end"
    Get-Content "$Results\load.txt" | Select-String 'loaded' | Write-Host

    Start-Sleep -Seconds 5
    try { Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$Port/internal/force_flush" -TimeoutSec 10 | Out-Null } catch {}
    Start-Sleep -Seconds 2
    Mark "flush_end"

    $types = @("single-groupby-1-1-1","single-groupby-1-1-12","single-groupby-1-8-1",
               "single-groupby-5-1-1","single-groupby-5-8-1","cpu-max-all-1",
               "cpu-max-all-8","double-groupby-1","double-groupby-5","double-groupby-all")
    Mark "query_start"
    foreach ($qt in $types) {
        $qf = "$DataDir\queries-$qt.dat"
        if (-not (Test-Path $qf)) { continue }
        Write-Host "=== [$Label] query benchmark: $qt ==="
        cmd /c "`"$Bench\tsbs_run_queries_victoriametrics.exe`" --file=`"$qf`" --workers=$Workers --urls=http://127.0.0.1:$Port --results-file=`"$Results\query-$qt.json`" > `"$Results\query-$qt.txt`" 2>&1"
    }
    Mark "query_end"

    try { Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$Port/internal/force_merge" -TimeoutSec 10 | Out-Null } catch {}
    $prev = -1
    foreach ($i in 1..30) {
        Start-Sleep -Seconds 2
        $cur = (Get-ChildItem -Recurse -File $Storage | Measure-Object -Sum Length).Sum
        if ($cur -eq $prev) { break }
        $prev = $cur
    }
    Add-Content -Path "$Results\storage.txt" -Value "post_merge $prev"
    Mark "merge_end"
    Write-Host "=== [$Label] done ==="
} finally {
    Stop-Process -Id $server.Id -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 2
    Stop-Job $sampler -ErrorAction SilentlyContinue
    Remove-Job $sampler -Force -ErrorAction SilentlyContinue
    if (Test-Path $Storage) { Remove-Item -Recurse -Force $Storage -ErrorAction SilentlyContinue }
}
