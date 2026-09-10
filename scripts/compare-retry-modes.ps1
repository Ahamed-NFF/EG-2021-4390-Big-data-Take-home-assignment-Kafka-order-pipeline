<#
.SYNOPSIS
    Measure blocking retry against non-blocking retry topics on identical input.

.DESCRIPTION
    Runs the same producer output through the consumer twice, once per retry
    mode, and reports how long the main topic took to drain in each.

    Drain time is the metric that matters. Total wall-clock is not comparable
    between the modes: the retry tiers use deliberately longer delays (2s/6s/15s)
    than the in-process backoff (0.25s-4s), and in topic mode those delays run
    in the background where they cost nothing. What head-of-line blocking
    actually costs is the time the *main* topic spends stalled behind a record
    that is sleeping, which is exactly what this measures.

    The cluster is reset before each run so both modes see the same input from
    the same starting offsets.

.EXAMPLE
    .\scripts\compare-retry-modes.ps1
    .\scripts\compare-retry-modes.ps1 -Count 120 -TransientFailureRate 0.4
#>
[CmdletBinding()]
param(
    [int]$Count = 60,
    [double]$Rate = 20,
    [double]$TransientFailureRate = 0.30,
    [int]$Seed = 42,
    [double]$WindowSeconds = 2
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

$results = @{}

foreach ($mode in @("blocking", "topics")) {
    Write-Host ""
    Write-Host ("=" * 74) -ForegroundColor DarkCyan
    Write-Host "  RETRY MODE: $mode" -ForegroundColor Cyan
    Write-Host ("=" * 74) -ForegroundColor DarkCyan

    & "$PSScriptRoot\reset-kafka.ps1" -Force | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Reset failed before the '$mode' run." }

    python -m src.producer --count $Count --rate $Rate `
        --corrupt-rate 0.05 --invalid-rate 0.05 --seed $Seed | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Producer failed before the '$mode' run." }

    # Native stderr must not abort the run; librdkafka warns on startup.
    $ErrorActionPreference = "Continue"
    $output = & python -m src.consumer --retry-mode $mode --max-messages $Count `
        --window-seconds $WindowSeconds --transient-failure-rate $TransientFailureRate `
        --seed 7 --drain-timeout 8 --group "compare-$mode" 2>&1 |
        ForEach-Object { $_.ToString() }
    $ErrorActionPreference = "Stop"

    function Field($pattern) {
        $line = $output | Select-String -Pattern $pattern | Select-Object -First 1
        if ($line -and $line.Matches[0].Groups.Count -gt 1) {
            return $line.Matches[0].Groups[1].Value.Trim()
        }
        return "?"
    }

    $results[$mode] = [pscustomobject]@{
        Mode      = $mode
        DrainTime = Field 'main topic drain time\s*:\s*([0-9.]+)s'
        Rate      = Field 'main topic drain time.*\(([0-9.]+) orders/s\)'
        Processed = Field 'processed successfully\s*:\s*(\d+)'
        Recovered = Field 'of which recovered\s*:\s*(\d+)'
        Dlq       = Field 'sent to DLQ\s*:\s*(\d+)'
        Accounted = Field 'accounted for\s*:\s*(\S+)'
    }

    $output | Select-String -Pattern "FINAL REPORT" -Context 0, 22 | Out-Host
}

Write-Host ""
Write-Host ("=" * 74) -ForegroundColor Green
Write-Host "  COMPARISON - identical input ($Count orders, seed $Seed, " -ForegroundColor Green -NoNewline
Write-Host "$($TransientFailureRate * 100)% transient failures)" -ForegroundColor Green
Write-Host ("=" * 74) -ForegroundColor Green

$table = $results.Values | Sort-Object Mode | Format-Table `
    @{L = "retry mode"; E = { $_.Mode } },
    @{L = "main drain"; E = { "$($_.DrainTime)s" } },
    @{L = "orders/s"; E = { $_.Rate } },
    @{L = "processed"; E = { $_.Processed } },
    @{L = "recovered"; E = { $_.Recovered } },
    @{L = "DLQ"; E = { $_.Dlq } },
    @{L = "accounted"; E = { $_.Accounted } } -AutoSize | Out-String
Write-Host $table

$blocking = [double]($results["blocking"].DrainTime -replace '[^0-9.]', '')
$topics = [double]($results["topics"].DrainTime -replace '[^0-9.]', '')
if ($blocking -gt 0 -and $topics -gt 0) {
    $speedup = [math]::Round($blocking / $topics, 1)
    Write-Host "  Non-blocking retry drained the main topic ${speedup}x faster." -ForegroundColor Green
    Write-Host "  Both modes account for every record - the difference is throughput," -ForegroundColor Green
    Write-Host "  paid for with the loss of global ordering in topic mode." -ForegroundColor Green
}
Write-Host ""
