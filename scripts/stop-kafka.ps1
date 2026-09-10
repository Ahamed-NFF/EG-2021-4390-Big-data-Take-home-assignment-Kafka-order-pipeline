<#
.SYNOPSIS
    Stop the local Kafka broker.

.DESCRIPTION
    Kafka ships kafka-server-stop.bat, but it finds the broker with `wmic`,
    which Windows 11 no longer includes, so that script is a no-op on this
    platform. This does the same job through CIM instead.

    The broker is matched on its command line containing `kafka.Kafka`, so
    unrelated Java processes on the machine are never touched. SIGTERM has no
    Windows equivalent for a detached process, so shutdown is a Stop-Process;
    KRaft recovers its log on the next start.
#>
[CmdletBinding()]
param()

function Get-KafkaBroker {
    Get-CimInstance Win32_Process -Filter "Name = 'java.exe'" |
        Where-Object { $_.CommandLine -like "*kafka.Kafka*" }
}

$broker = Get-KafkaBroker
if (-not $broker) {
    Write-Host "Kafka is not running." -ForegroundColor Yellow
    exit 0
}

foreach ($p in @($broker)) {
    Write-Host "Stopping Kafka broker (PID $($p.ProcessId))..." -ForegroundColor Cyan
    Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
}

# Give the JVM a moment to release its file handles on the log directory.
for ($i = 0; $i -lt 10; $i++) {
    Start-Sleep -Milliseconds 500
    if (-not (Get-KafkaBroker)) {
        Write-Host "Kafka stopped." -ForegroundColor Green
        exit 0
    }
}

Write-Host "Kafka is still running after 5s." -ForegroundColor Red
exit 1
