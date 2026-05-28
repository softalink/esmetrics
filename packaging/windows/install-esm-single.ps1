# Installs esm-single.exe as a Windows service.
#
# Run from an elevated PowerShell prompt. Requires the New-Service cmdlet
# (Windows 10+). For older Windows, see sc.exe usage below.
#
# Usage:
#   .\install-esm-single.ps1 -ExePath C:\esmetrics\esm-single.exe `
#                            -DataPath C:\esmetrics\data

param(
    [Parameter(Mandatory=$true)] [string]$ExePath,
    [Parameter(Mandatory=$true)] [string]$DataPath,
    [string]$ListenAddr = "127.0.0.1:8428",
    [string]$ServiceName = "EsMetricsSingle"
)

$cmdline = "`"$ExePath`" --storage-data-path=`"$DataPath`" --http-listen-addr=$ListenAddr"

New-Service `
    -Name $ServiceName `
    -DisplayName "EsMetrics single-node server" `
    -BinaryPathName $cmdline `
    -Description "EsMetrics single-node (drop-in for vmsingle)." `
    -StartupType Automatic

Start-Service $ServiceName
Get-Service $ServiceName

# To uninstall:
#   Stop-Service EsMetricsSingle
#   Remove-Service EsMetricsSingle    (Windows 10+; otherwise: sc.exe delete EsMetricsSingle)
