<#
.SYNOPSIS
    End-to-end demonstration: produce -> consume -> retry -> DLQ -> aggregate.

.DESCRIPTION
    Runs the whole pipeline in one pass and prints each stage, so the system can
    be demonstrated live from a single command. Producer and consumer are run
    sequentially rather than concurrently so the output stays readable; the
    consumer reads from the topic afterwards, which is exactly the same code
    path as running them side by side in two terminals.

.PARAMETER Reset
    Delete and recreate the topics first, so the run starts from empty offsets
    and the running average begins at zero. Use this for a live demo.

.PARAMETER Transcript
    Also write everything to docs\demo-output-<timestamp>.txt.

.EXAMPLE
    .\scripts\demo.ps1 -Reset
    .\scripts\demo.ps1 -Count 200 -TransientFailureRate 0.3 -Transcript
#>
[CmdletBinding()]
param(
    [int]$Count = 60,
    [double]$Rate = 10,
    [double]$CorruptRate = 0.08,
    [double]$InvalidRate = 0.08,
    [double]$TransientFailureRate = 0.25,
    [ValidateSet("topics", "blocking")]
    [string]$RetryMode = "topics",
    [double]$WindowSeconds = 2,
    [int]$Seed = 42,
    [switch]$Reset,
    [switch]$Transcript
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

if ($Transcript) {
    # Start-Transcript only records PowerShell's own streams, so everything the
    # Python processes print would be missing from the file. Re-run this script
    # as a child process instead and tee its merged output, which does capture
    # the producer and consumer.
    $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $transcriptPath = Join-Path $projectRoot "docs\demo-output-$stamp.txt"
    New-Item -ItemType Directory -Force -Path (Split-Path $transcriptPath) | Out-Null

    $childArgs = @(
        "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $PSCommandPath,
        "-Count", $Count, "-Rate", $Rate,
        "-CorruptRate", $CorruptRate, "-InvalidRate", $InvalidRate,
        "-TransientFailureRate", $TransientFailureRate, "-Seed", $Seed,
        "-RetryMode", $RetryMode, "-WindowSeconds", $WindowSeconds
    )
    if ($Reset) { $childArgs += "-Reset" }

    # Merging stderr into the pipeline turns any native stderr line into an
    # ErrorRecord, and under 'Stop' that aborts the run. librdkafka legitimately
    # warns on stderr while the broker finishes starting up, so judge success by
    # the child's exit code instead.
    $ErrorActionPreference = "Continue"
    & powershell @childArgs 2>&1 |
        ForEach-Object { $_.ToString() } |
        Tee-Object -FilePath $transcriptPath
    $childExit = $LASTEXITCODE
    $ErrorActionPreference = "Stop"

    Write-Host ""
    Write-Host "Transcript saved to $transcriptPath" -ForegroundColor Green
    exit $childExit
}

function Step($number, $title) {
    Write-Host ""
    Write-Host ("#" * 74) -ForegroundColor DarkCyan
    Write-Host "#  STEP $number - $title" -ForegroundColor Cyan
    Write-Host ("#" * 74) -ForegroundColor DarkCyan
    Write-Host ""
}

try {
    if ($Reset) {
        Step 0 "Reset the cluster to an empty state"
        # A data-directory wipe, not a topic delete: deleting topics on a live
        # Windows broker crashes it (KAFKA-1194). See scripts/reset-kafka.ps1.
        & "$PSScriptRoot\reset-kafka.ps1" -Force
        if ($LASTEXITCODE -ne 0) { throw "Reset failed." }
    }

    Step 1 "Start the Kafka broker (KRaft mode, no ZooKeeper)"
    & "$PSScriptRoot\start-kafka.ps1"
    if ($LASTEXITCODE -ne 0) { throw "Kafka failed to start." }

    Step 2 "Create topics: orders, 3 retry tiers, orders.DLQ, orders.aggregates"
    python -m src.create_topics
    if ($LASTEXITCODE -ne 0) { throw "Topic creation failed." }

    Step 3 "Produce $Count Avro-encoded orders (with injected bad messages)"
    python -m src.producer --count $Count --rate $Rate `
        --corrupt-rate $CorruptRate --invalid-rate $InvalidRate --seed $Seed
    if ($LASTEXITCODE -ne 0) { throw "Producer failed." }

    Step 4 "Consume: decode, validate, retry ($RetryMode), aggregate, DLQ the rest"
    python -m src.consumer --retry-mode $RetryMode --max-messages $Count `
        --window-seconds $WindowSeconds --idle-timeout 25 --drain-timeout 8 `
        --transient-failure-rate $TransientFailureRate --seed 7
    if ($LASTEXITCODE -ne 0) { throw "Consumer failed." }

    Step 5 "Inspect the Dead Letter Queue"
    python -m src.dlq_tool inspect

    Step 6 "Show which DLQ records are worth replaying"
    python -m src.dlq_tool replay --dry-run

    Write-Host ""
    Write-Host ("=" * 74) -ForegroundColor Green
    Write-Host "  DEMO COMPLETE" -ForegroundColor Green
    Write-Host ("=" * 74) -ForegroundColor Green
    Write-Host "  Cumulative averages and closed tumbling windows were published to"
    Write-Host "  the 'orders.aggregates' topic. Read them with:"
    Write-Host "    C:\kafka\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 ``"
    Write-Host "        --topic orders.aggregates --from-beginning --timeout-ms 10000"
    Write-Host ""
    Write-Host "  Measure blocking vs non-blocking retry on identical input:"
    Write-Host "    .\scripts\compare-retry-modes.ps1"
    Write-Host ""
    Write-Host "  Stop the broker with:  .\scripts\stop-kafka.ps1"
    Write-Host ""
}
catch {
    Write-Host ""
    Write-Host "DEMO FAILED: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}
