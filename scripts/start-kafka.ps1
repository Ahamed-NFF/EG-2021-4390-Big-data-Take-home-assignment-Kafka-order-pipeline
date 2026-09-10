<#
.SYNOPSIS
    Start the single-node Kafka broker in KRaft mode.

.DESCRIPTION
    Formats the storage directory on first run (KRaft requires a cluster id
    written into the log dir before the broker will start), renders the config
    template, launches the broker in the background, and waits until port 9092
    is actually accepting connections before returning.

.NOTES
    KAFKA_HOME must be a SHORT path with no spaces. kafka-run-class.bat builds
    the classpath by listing every jar in libs/, and a long install path pushes
    that past the 8191-character Windows command-line limit, which surfaces as
    "The input line is too long."
#>
[CmdletBinding()]
param(
    [string]$KafkaHome = $(if ($env:KAFKA_HOME) { $env:KAFKA_HOME } else { "C:\kafka" }),
    [string]$DataDir   = $(if ($env:KAFKA_DATA_DIR) { $env:KAFKA_DATA_DIR } else { "C:\kafka-data" }),
    [int]$TimeoutSeconds = 90
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$logFile = Join-Path $projectRoot "logs\kafka-server.log"

function Invoke-KafkaTool {
    <#
      Kafka's CLI tools print a harmless log4j2 "Reconfiguration failed" line to
      stderr. Under $ErrorActionPreference = 'Stop' PowerShell promotes any
      native stderr output to a terminating NativeCommandError, which would kill
      the script over a warning. Relax the preference for native calls only, and
      judge success by the exit code instead.
    #>
    param([string]$Exe, [string[]]$Arguments)

    $previous = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $output = & $Exe @Arguments 2>&1 | ForEach-Object { $_.ToString() }
        return [pscustomobject]@{ Output = @($output); ExitCode = $LASTEXITCODE }
    } finally {
        $ErrorActionPreference = $previous
    }
}

function Test-KafkaUp {
    try {
        $c = New-Object System.Net.Sockets.TcpClient
        $c.Connect("localhost", 9092)
        $c.Close()
        return $true
    } catch { return $false }
}

if (-not (Test-Path "$KafkaHome\bin\windows\kafka-server-start.bat")) {
    Write-Error "Kafka not found at '$KafkaHome'. Run .\scripts\setup-kafka.ps1 first, or set KAFKA_HOME."
}

if (Test-KafkaUp) {
    Write-Host "Kafka is already running on localhost:9092." -ForegroundColor Green
    exit 0
}

New-Item -ItemType Directory -Force -Path $DataDir, (Split-Path $logFile) | Out-Null

# Render the config template with this machine's data directory. Kafka reads
# properties files with backslash as an escape character, so use forward slashes.
$template = Get-Content "$projectRoot\config\kraft-server.properties.template" -Raw
$config   = Join-Path $DataDir "server.properties"
$template.Replace("{{LOG_DIRS}}", $DataDir.Replace("\", "/")) |
    Set-Content -Path $config -Encoding ascii

# KRaft stores a cluster id in meta.properties; its absence means "never formatted".
if (-not (Test-Path (Join-Path $DataDir "meta.properties"))) {
    Write-Host "First run - formatting KRaft storage at $DataDir" -ForegroundColor Cyan
    $storageBat = "$KafkaHome\bin\windows\kafka-storage.bat"

    $uuidResult = Invoke-KafkaTool -Exe $storageBat -Arguments @("random-uuid")
    # A KRaft cluster id is 22 base64url characters; skip the log4j warning lines.
    $clusterId = $uuidResult.Output |
                 Where-Object { $_.Trim() -match '^[A-Za-z0-9_-]{22}$' } |
                 Select-Object -Last 1
    if (-not $clusterId) {
        $uuidResult.Output | ForEach-Object { Write-Host "  $_" }
        Write-Error "Could not generate a KRaft cluster id."
    }
    $clusterId = $clusterId.Trim()
    Write-Host "  cluster id: $clusterId"

    $fmt = Invoke-KafkaTool -Exe $storageBat `
        -Arguments @("format", "-t", $clusterId, "-c", $config, "--standalone")
    $fmt.Output | Select-Object -Last 5 | ForEach-Object { Write-Host "  $_" }

    if (-not (Test-Path (Join-Path $DataDir "meta.properties"))) {
        Write-Error "Storage format failed - no meta.properties written to $DataDir"
    }
}

Write-Host "Starting Kafka broker (log: $logFile)..." -ForegroundColor Cyan

# Launch through a generated .cmd rather than passing the config path as an
# argument. Two Windows quirks make the direct route unreliable:
#
#   * `cmd /c "a.bat" "b.properties"` hits cmd's quote-stripping rule and the
#     broker either never receives its config or fails outright.
#   * Start-Process redirection combined with a .bat produced an empty log.
#
# Baking both paths into a script file removes every layer of quoting, and the
# redirection happens inside cmd where it is unambiguous. The launcher lives in
# the data directory, which is a short path with no spaces.
$startBat = "$KafkaHome\bin\windows\kafka-server-start.bat"
$launcher = Join-Path $DataDir "run-broker.cmd"
$heapOpts = if ($env:KAFKA_HEAP_OPTS) { $env:KAFKA_HEAP_OPTS } else { "-Xmx1G -Xms1G" }

# KAFKA_HEAP_OPTS is set here because kafka-server-start.bat otherwise sizes
# the heap by shelling out to `wmic`, which Windows 11 no longer ships.
@"
@echo off
rem Generated by scripts/start-kafka.ps1 - edits will be overwritten.
set KAFKA_HEAP_OPTS=$heapOpts
call "$startBat" "$config" > "$logFile" 2>&1
"@ | Set-Content -Path $launcher -Encoding ascii

Start-Process -FilePath "cmd.exe" -ArgumentList @("/c", $launcher) -WindowStyle Hidden

$deadline = (Get-Date).AddSeconds($TimeoutSeconds)
while ((Get-Date) -lt $deadline) {
    if (Test-KafkaUp) {
        Write-Host "Kafka is up on localhost:9092" -ForegroundColor Green
        exit 0
    }
    Start-Sleep -Milliseconds 750
}

Write-Host "Kafka did not come up within $TimeoutSeconds seconds." -ForegroundColor Red
Write-Host "Last 30 log lines from $logFile :" -ForegroundColor Yellow
if (Test-Path $logFile) { Get-Content $logFile -Tail 30 } else { Write-Host "  (no log file written)" }
Write-Host ""
Write-Host "If the log mentions a corrupt or locked log directory, reset it with:" -ForegroundColor Yellow
Write-Host "  .\scripts\reset-kafka.ps1" -ForegroundColor Yellow
exit 1
