<#
.SYNOPSIS
    Wipe all Kafka data and start again from an empty cluster.

.DESCRIPTION
    Stops the broker, clears the KRaft log directory, re-formats it with a new
    cluster id, restarts, and recreates the three topics. Everything on the
    topics is destroyed.

.NOTES
    This is deliberately a *data directory* reset rather than a topic deletion.
    Deleting a topic on a running Windows broker is not reliable: Kafka has to
    remove log segment and index files that are still memory-mapped, Windows
    refuses to unlink an open mapped file, and the broker takes the resulting
    IOException as fatal and shuts down (KAFKA-1194, open since 2014). Stopping
    the broker first sidesteps the whole problem.

.EXAMPLE
    .\scripts\reset-kafka.ps1
#>
[CmdletBinding()]
param(
    [string]$DataDir = $(if ($env:KAFKA_DATA_DIR) { $env:KAFKA_DATA_DIR } else { "C:\kafka-data" }),
    [switch]$Force
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot

if (-not $Force) {
    Write-Host "This deletes every message in orders, orders.DLQ and orders.aggregates." -ForegroundColor Yellow
    $answer = Read-Host "Type 'yes' to continue"
    if ($answer -ne "yes") {
        Write-Host "Cancelled." -ForegroundColor Yellow
        exit 0
    }
}

Write-Host "Stopping broker..." -ForegroundColor Cyan
& "$PSScriptRoot\stop-kafka.ps1" | Out-Host

if (Test-Path $DataDir) {
    Write-Host "Clearing $DataDir ..." -ForegroundColor Cyan
    # Remove the contents rather than the directory itself: the folder may be
    # open in another shell, and start-kafka.ps1 expects to find it.
    Get-ChildItem -Path $DataDir -Force | ForEach-Object {
        try {
            Remove-Item $_.FullName -Recurse -Force -ErrorAction Stop
        } catch {
            Write-Host "  could not delete $($_.Name): $($_.Exception.Message)" -ForegroundColor Yellow
        }
    }
    $left = @(Get-ChildItem -Path $DataDir -Force)
    if ($left.Count -gt 0) {
        Write-Error "$($left.Count) item(s) left in $DataDir - is the broker still running, or a file open elsewhere?"
    }
}

Write-Host "Starting a fresh cluster..." -ForegroundColor Cyan
& "$PSScriptRoot\start-kafka.ps1" | Out-Host
if ($LASTEXITCODE -ne 0) { Write-Error "Broker failed to restart." }

Write-Host "Recreating topics..." -ForegroundColor Cyan
Push-Location $projectRoot
try {
    python -m src.create_topics | Out-Host
    if ($LASTEXITCODE -ne 0) { Write-Error "Topic creation failed." }
} finally {
    Pop-Location
}

Write-Host ""
Write-Host "Reset complete - empty cluster, topics recreated." -ForegroundColor Green
