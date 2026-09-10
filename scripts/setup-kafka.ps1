<#
.SYNOPSIS
    Download and install Apache Kafka for the assignment demo.

.DESCRIPTION
    Fetches the Kafka binary distribution and extracts it to a short install
    path. Kafka 4.x runs in KRaft mode, so there is no ZooKeeper to install.

.NOTES
    The install path MUST be short and free of spaces, which is why the default
    is C:\kafka rather than somewhere inside this repository.

    kafka-run-class.bat builds the JVM classpath by expanding every jar in
    libs/ into one command line. With ~100 jars, a long install prefix pushes
    that past the 8191-character Windows limit and every Kafka command fails
    with "The input line is too long." Installing under a short path keeps the
    classpath comfortably inside the limit.

.EXAMPLE
    .\scripts\setup-kafka.ps1
    .\scripts\setup-kafka.ps1 -InstallDir D:\kafka -Version 4.1.2
#>
[CmdletBinding()]
param(
    [string]$Version    = "4.1.2",
    [string]$ScalaVersion = "2.13",
    [string]$InstallDir = $(if ($env:KAFKA_HOME) { $env:KAFKA_HOME } else { "C:\kafka" })
)

$ErrorActionPreference = "Stop"
$ProgressPreference    = "SilentlyContinue"

if ($InstallDir -match '\s') {
    Write-Error "Install path '$InstallDir' contains a space. Kafka's Windows scripts cannot handle that - choose a path like C:\kafka."
}

if (Test-Path "$InstallDir\bin\windows\kafka-server-start.bat") {
    Write-Host "Kafka is already installed at $InstallDir" -ForegroundColor Green
    exit 0
}

$java = Get-Command java -ErrorAction SilentlyContinue
if (-not $java) {
    Write-Error "Java not found on PATH. Kafka 4.x requires Java 17 or newer."
}
Write-Host "Java: $((& java -version 2>&1 | Select-Object -First 1))" -ForegroundColor Cyan

$archive = "kafka_$ScalaVersion-$Version.tgz"
$tempDir = Join-Path $env:TEMP "kafka-setup-$Version"
$tarball = Join-Path $tempDir $archive
New-Item -ItemType Directory -Force -Path $tempDir | Out-Null

# dlcdn only carries current releases; archive.apache.org keeps everything.
$sources = @(
    "https://dlcdn.apache.org/kafka/$Version/$archive",
    "https://archive.apache.org/dist/kafka/$Version/$archive"
)

$downloaded = $false
foreach ($url in $sources) {
    try {
        Write-Host "Downloading $url ..." -ForegroundColor Cyan
        Invoke-WebRequest -Uri $url -OutFile $tarball -UseBasicParsing -TimeoutSec 900
        $downloaded = $true
        break
    } catch {
        Write-Host "  failed: $($_.Exception.Message)" -ForegroundColor Yellow
    }
}
if (-not $downloaded) { Write-Error "Could not download Kafka $Version from any mirror." }

Write-Host ("Downloaded {0:N1} MB" -f ((Get-Item $tarball).Length / 1MB)) -ForegroundColor Green

Write-Host "Extracting..." -ForegroundColor Cyan
tar -xzf $tarball -C $tempDir
if ($LASTEXITCODE -ne 0) { Write-Error "tar extraction failed (exit $LASTEXITCODE)." }

$extracted = Join-Path $tempDir "kafka_$ScalaVersion-$Version"
if (-not (Test-Path $extracted)) { Write-Error "Expected '$extracted' after extraction." }

Write-Host "Installing to $InstallDir ..." -ForegroundColor Cyan
New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
robocopy $extracted $InstallDir /E /MOVE /NFL /NDL /NJH /NJS /NP | Out-Null
# robocopy uses exit codes 0-7 for success; 8+ means a real failure.
if ($LASTEXITCODE -ge 8) { Write-Error "robocopy failed (exit $LASTEXITCODE)." }

Remove-Item $tarball -Force -ErrorAction SilentlyContinue

if (-not (Test-Path "$InstallDir\bin\windows\kafka-server-start.bat")) {
    Write-Error "Install verification failed - kafka-server-start.bat not found in $InstallDir."
}

Write-Host ""
Write-Host "Kafka $Version installed at $InstallDir" -ForegroundColor Green
Write-Host "Next:  .\scripts\start-kafka.ps1" -ForegroundColor Green
