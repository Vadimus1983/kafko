# kafka_bench.ps1 -- Benchmark Apache Kafka local broker for comparison vs kafko.
#
# Usage (from project root, in PowerShell):
#   .\scripts\kafka_bench.ps1
#
# Requirements:
#   - Docker Desktop installed and running (steady tray icon)
#   - Windows PowerShell 5.1+ (Pwsh 7 also works)
#
# Output:
#   scripts/tmp/kafka_bench_results_<YYYYMMDD-HHMMSS>.txt
#
# Matrix:
#   4 modes × 6 record sizes = 24 measurements, ~15-20 minutes total.
#
# Config notes:
#   Every Kafka knob except message-size ceilings is left at its default. The
#   message-size knobs (broker message.max.bytes, topic max.message.bytes,
#   producer max.request.size) are all raised to 16 MiB so the 1 MiB record
#   cell does not fail. With defaults (~1 MiB on each), a 1 MiB record serializes
#   to ~1,048,664 bytes (record + framing) and the producer rejects it with
#   RecordTooLargeException. Raising these three to 16 MiB does NOT add any
#   batching or tuning -- it just lifts the size ceiling.

# NOTE: PowerShell 5.1 wraps stderr from native commands as NativeCommandError
# when redirected (e.g., `2>$null`). With $ErrorActionPreference='Stop' this halts
# the script. We use `cmd /c "cmd >nul 2>nul"` for silenced calls instead -- cmd
# handles the redirect, PowerShell only sees the exit code.

# --- Anchor every path to the script's own location ---
$ScriptDir   = $PSScriptRoot
$ProjectRoot = (Resolve-Path (Join-Path $ScriptDir '..')).Path
$TmpDir      = Join-Path $ScriptDir 'tmp'
New-Item -ItemType Directory -Force -Path $TmpDir | Out-Null
Push-Location $ProjectRoot
try {

# --- Config ---
$KafkaImage = 'apache/kafka:3.7.0'
$Container  = 'kafka-bench'
$Topic      = 'bench'
$Sizes      = @(64, 256, 512, 1024, 4096, 1048576)
$Timestamp  = (Get-Date).ToString('yyyyMMdd-HHmmss')
$Results    = Join-Path $TmpDir "kafka_bench_results_$Timestamp.txt"
$ClusterId  = 'ciWo7IWazngRchmPES6q5A'

# --- Cleanup helper ---
function Invoke-Cleanup {
    Write-Host "[cleanup] Stopping and removing container..."
    cmd /c "docker stop $Container >nul 2>nul"
    cmd /c "docker rm   $Container >nul 2>nul"
}

# --- Pre-flight: docker installed? ---
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Host "ERROR: 'docker' not found in PATH. Install Docker Desktop." -ForegroundColor Red
    exit 1
}

# --- Pre-flight: docker daemon running? ---
cmd /c "docker info >nul 2>nul"
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Docker daemon not running. Open Docker Desktop, wait for steady icon, then re-run." -ForegroundColor Red
    exit 1
}

# --- Remove any leftover container from a previous run ---
cmd /c "docker rm -f $Container >nul 2>nul"

# --- Start Kafka in KRaft single-node mode ---
Write-Host "Starting Kafka $KafkaImage in KRaft single-node mode..."

$startArgs = @(
    'run', '-d',
    '--name', $Container,
    '-p', '9092:9092',
    '-p', '9093:9093',
    '-e', 'KAFKA_NODE_ID=1',
    '-e', 'KAFKA_PROCESS_ROLES=broker,controller',
    '-e', 'KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093',
    '-e', 'KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092',
    '-e', 'KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER',
    '-e', 'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT',
    '-e', 'KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093',
    '-e', "CLUSTER_ID=$ClusterId",
    # Raise broker-side message-size ceilings to 16 MiB so the 1 MiB cell works.
    # Defaults (~1 MiB) reject 1 MiB records once framing overhead is added.
    '-e', 'KAFKA_MESSAGE_MAX_BYTES=16777216',
    '-e', 'KAFKA_REPLICA_FETCH_MAX_BYTES=16777216',
    $KafkaImage
)

$containerId = & docker @startArgs
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Failed to start Kafka container" -ForegroundColor Red
    exit 1
}

# --- Wait for Kafka to be ready (up to 60s) ---
Write-Host -NoNewline "Waiting for Kafka to become ready"
$ready = $false
for ($i = 0; $i -lt 60; $i++) {
    cmd /c "docker exec $Container /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >nul 2>nul"
    if ($LASTEXITCODE -eq 0) {
        Write-Host " OK"
        $ready = $true
        break
    }
    Write-Host -NoNewline "."
    Start-Sleep -Seconds 1
}

if (-not $ready) {
    Write-Host " FAILED" -ForegroundColor Red
    Write-Host "Kafka did not become ready in 60s. Last 50 log lines:"
    & docker logs --tail 50 $Container
    Invoke-Cleanup
    exit 1
}

# --- Initialize results file ---
$timestamp = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
$header = @"
Kafka local broker benchmark
Date:  $timestamp
Image: $KafkaImage
Mode:  KRaft single-node (no replication), acks=1
Host:  $env:COMPUTERNAME

"@
$header | Out-File -FilePath $Results -Encoding utf8

# --- Single bench runner ---
function Invoke-PerfRun {
    param(
        [int]$Size,
        [string]$Mode
    )

    # Number of records scales inversely with size
    if     ($Size -ge 1048576) { $numRecords = 5000 }
    elseif ($Size -ge 4096)    { $numRecords = 100000 }
    else                       { $numRecords = 500000 }

    switch ($Mode) {
        'unbatched'    { $linger = 0;  $batchSize = 1;     $compression = 'none' }
        'batched'      { $linger = 10; $batchSize = 65536; $compression = 'none' }
        'batched_lz4'  { $linger = 10; $batchSize = 65536; $compression = 'lz4'  }
        'batched_zstd' { $linger = 10; $batchSize = 65536; $compression = 'zstd' }
        default        { throw "Unknown mode: $Mode" }
    }

    $label  = "size=${Size}B mode=$Mode n=$numRecords"
    $config = "config: acks=1 linger.ms=$linger batch.size=$batchSize compression.type=$compression"

    Write-Host ""
    Write-Host "=== $label ==="
    Write-Host $config

    Add-Content -Path $Results -Value ""
    Add-Content -Path $Results -Value "=== $label ==="
    Add-Content -Path $Results -Value $config

    # Clean topic state per run (silent -- topic might not exist yet)
    cmd /c "docker exec $Container /opt/kafka/bin/kafka-topics.sh --delete --topic $Topic --bootstrap-server localhost:9092 >nul 2>nul"
    Start-Sleep -Seconds 1

    # Create fresh topic with raised max.message.bytes so the 1 MiB cell works
    & docker exec $Container /opt/kafka/bin/kafka-topics.sh `
        --create --topic $Topic --partitions 1 --replication-factor 1 `
        --config "max.message.bytes=16777216" `
        --bootstrap-server localhost:9092 | Out-Null

    # Run the perf test. max.request.size raised to 16 MiB so the producer
    # accepts records >1 MiB after framing overhead is added.
    $perfArgs = @(
        'exec', $Container,
        '/opt/kafka/bin/kafka-producer-perf-test.sh',
        '--topic',         $Topic,
        '--num-records',   "$numRecords",
        '--record-size',   "$Size",
        '--throughput',    '-1',
        '--producer-props',
        'bootstrap.servers=localhost:9092',
        'acks=1',
        "linger.ms=$linger",
        "batch.size=$batchSize",
        "compression.type=$compression",
        'max.request.size=16777216'
    )

    $output = & docker @perfArgs

    # tee-like: print to console AND append to results file
    $output | ForEach-Object {
        Write-Host $_
        Add-Content -Path $Results -Value $_
    }
}

# --- Run the full 4 × 6 matrix ---
try {
    foreach ($mode in @('unbatched', 'batched', 'batched_lz4', 'batched_zstd')) {
        foreach ($size in $Sizes) {
            Invoke-PerfRun -Size $size -Mode $mode
        }
    }
} finally {
    Invoke-Cleanup
}

Add-Content -Path $Results -Value ""
Add-Content -Path $Results -Value "=== DONE -- results in $Results ==="
Write-Host ""
Write-Host "=== DONE -- results in $Results ==="

} finally {
    Pop-Location
}
