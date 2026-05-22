# kafka_bench_max.ps1 -- Kafka local broker tuned for MAX performance.
# Updated to properly handle large (1 MB) records.
#
# Usage (from project root, in PowerShell):
#   .\scripts\kafka_bench_max.ps1
#
# Configuration:
#   Broker:
#     - num.network.threads=8, num.io.threads=8
#     - socket buffers 1 MB, request max 100 MB
#     - message.max.bytes=16 MB, replica.fetch.max.bytes=16 MB    ← NEW
#     - log flush interval 100k messages / 1s (page-cache durability)
#     - JVM heap 1 GB
#   Topic:
#     - max.message.bytes=16 MB                                    ← NEW
#   Producer:
#     - linger.ms=50, batch.size dynamic = max(128 KB, recordSize × 2)  ← UPDATED
#     - buffer.memory = max(128 MB, recordSize × 256)              ← UPDATED
#     - max.request.size=16 MB                                     ← UPDATED
#     - acks=1
#     - tests with no compression / lz4 / zstd
#
# Output:
#   kafka_bench_max_results.txt in the current directory

$KafkaImage           = 'apache/kafka:3.7.0'
$Container            = 'kafka-bench-max'
$Topic                = 'bench'
$Sizes                = @(64, 256, 512, 1024, 4096, 1048576)
$Results              = 'kafka_bench_max_results.txt'
$ClusterId            = 'ciWo7IWazngRchmPES6q5A'
$ConcurrentProducers  = 8
$MaxMessageBytes      = 16777216  # 16 MB -- big enough for 1 MB records + framing

# --- Cleanup helper ---
function Invoke-Cleanup {
    Write-Host "[cleanup] Stopping and removing container..."
    cmd /c "docker stop $Container >nul 2>nul"
    cmd /c "docker rm   $Container >nul 2>nul"
}

# --- Pre-flight ---
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Host "ERROR: 'docker' not found in PATH. Install Docker Desktop." -ForegroundColor Red
    exit 1
}

cmd /c "docker info >nul 2>nul"
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Docker daemon not running. Open Docker Desktop and re-run." -ForegroundColor Red
    exit 1
}

cmd /c "docker rm -f $Container >nul 2>nul"

# --- Start Kafka with PERFORMANCE-TUNED broker config + large-message support ---
Write-Host "Starting Kafka $KafkaImage (perf-tuned + large-message broker)..."

$startArgs = @(
    'run', '-d',
    '--name', $Container,
    '-p', '9092:9092',
    '-p', '9093:9093',
    # KRaft single-node
    '-e', 'KAFKA_NODE_ID=1',
    '-e', 'KAFKA_PROCESS_ROLES=broker,controller',
    '-e', 'KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093',
    '-e', 'KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092',
    '-e', 'KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER',
    '-e', 'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT',
    '-e', 'KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093',
    '-e', "CLUSTER_ID=$ClusterId",
    # Broker performance tuning
    '-e', 'KAFKA_NUM_NETWORK_THREADS=8',
    '-e', 'KAFKA_NUM_IO_THREADS=8',
    '-e', 'KAFKA_SOCKET_SEND_BUFFER_BYTES=1048576',
    '-e', 'KAFKA_SOCKET_RECEIVE_BUFFER_BYTES=1048576',
    '-e', 'KAFKA_SOCKET_REQUEST_MAX_BYTES=104857600',
    '-e', 'KAFKA_LOG_FLUSH_INTERVAL_MESSAGES=100000',
    '-e', 'KAFKA_LOG_FLUSH_INTERVAL_MS=1000',
    '-e', 'KAFKA_NUM_PARTITIONS=1',
    '-e', 'KAFKA_DEFAULT_REPLICATION_FACTOR=1',
    # NEW: large-message support
    '-e', "KAFKA_MESSAGE_MAX_BYTES=$MaxMessageBytes",
    '-e', "KAFKA_REPLICA_FETCH_MAX_BYTES=$MaxMessageBytes",
    # JVM heap
    '-e', 'KAFKA_HEAP_OPTS=-Xms1G -Xmx1G',
    $KafkaImage
)

$containerId = & docker @startArgs
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Failed to start Kafka container" -ForegroundColor Red
    exit 1
}

# --- Wait for readiness ---
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
    Write-Host "Kafka did not become ready. Last 50 log lines:"
    & docker logs --tail 50 $Container
    Invoke-Cleanup
    exit 1
}

# --- Initialize results file ---
$timestamp = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
$header = @"
Kafka local broker -- MAX PERFORMANCE config (with large-message support)
Date:  $timestamp
Image: $KafkaImage
Mode:  KRaft single-node, acks=1, perf-tuned broker
Broker: 8 io/net threads, 1MB sockets, 16MB message.max.bytes, 1GB JVM heap
Producer: linger.ms=50, batch.size = max(128KB, size*2), buffer.memory = max(128MB, size*256)
Host:  $env:COMPUTERNAME

"@
$header | Out-File -FilePath $Results -Encoding utf8

# --- Topic helpers (with max.message.bytes config) ---
function Reset-Topic {
    cmd /c "docker exec $Container /opt/kafka/bin/kafka-topics.sh --delete --topic $Topic --bootstrap-server localhost:9092 >nul 2>nul"
    Start-Sleep -Seconds 1
    & docker exec $Container /opt/kafka/bin/kafka-topics.sh `
        --create --topic $Topic --partitions 1 --replication-factor 1 `
        --config "max.message.bytes=$MaxMessageBytes" `
        --bootstrap-server localhost:9092 | Out-Null
}

# --- Helper: compute producer config sized to record ---
function Get-ProducerConfig {
    param([int]$Size, [string]$Mode)

    switch ($Mode) {
        'unbatched' {
            $linger      = 0
            $batchBase   = 1
            $compression = 'none'
        }
        'max' {
            $linger      = 50
            $batchBase   = 131072
            $compression = 'none'
        }
        'max_lz4' {
            $linger      = 50
            $batchBase   = 131072
            $compression = 'lz4'
        }
        'max_zstd' {
            $linger      = 50
            $batchBase   = 131072
            $compression = 'zstd'
        }
        default { throw "Unknown mode: $Mode" }
    }

    # Dynamic batch.size: must be able to hold at least one record. Add headroom.
    $batchSize = [Math]::Max($batchBase, $Size * 2)
    # Dynamic buffer.memory: enough to queue ~256 records. Floor at 128 MB.
    $bufferMem = [Math]::Max(134217728, $Size * 256)
    # max.request.size: must exceed largest record + overhead.
    $maxReqSize = [Math]::Max(16777216, $Size * 4)

    return @{
        Linger       = $linger
        BatchSize    = $batchSize
        BufferMemory = $bufferMem
        MaxReqSize   = $maxReqSize
        Compression  = $compression
    }
}

# --- Single-producer perf runner ---
function Invoke-PerfRun {
    param(
        [int]$Size,
        [string]$Mode
    )

    if     ($Size -ge 1048576) { $numRecords = 1000 }
    elseif ($Size -ge 4096)    { $numRecords = 200000 }
    else                       { $numRecords = 1000000 }

    $cfg = Get-ProducerConfig -Size $Size -Mode $Mode

    $label  = "size=${Size}B mode=$Mode n=$numRecords"
    $config = "config: acks=1 linger.ms=$($cfg.Linger) batch.size=$($cfg.BatchSize) buffer.memory=$($cfg.BufferMemory) max.request.size=$($cfg.MaxReqSize) compression.type=$($cfg.Compression)"

    Write-Host ""
    Write-Host "=== $label ===" -ForegroundColor Cyan
    Write-Host $config
    Add-Content -Path $Results -Value ""
    Add-Content -Path $Results -Value "=== $label ==="
    Add-Content -Path $Results -Value $config

    Reset-Topic

    # JVM warmup pass -- ~5% of records, output discarded, same config
    $warmupRecords = [Math]::Max(100, [int]($numRecords * 0.05))
    Write-Host "[warmup $warmupRecords records...]"
    & docker exec $Container /opt/kafka/bin/kafka-producer-perf-test.sh `
        --topic $Topic --num-records $warmupRecords --record-size $Size --throughput -1 `
        --producer-props bootstrap.servers=localhost:9092 acks=1 `
            "linger.ms=$($cfg.Linger)" "batch.size=$($cfg.BatchSize)" `
            "buffer.memory=$($cfg.BufferMemory)" "max.request.size=$($cfg.MaxReqSize)" `
            "compression.type=$($cfg.Compression)" | Out-Null

    Reset-Topic

    # Measured run
    $perfArgs = @(
        'exec', $Container,
        '/opt/kafka/bin/kafka-producer-perf-test.sh',
        '--topic',       $Topic,
        '--num-records', "$numRecords",
        '--record-size', "$Size",
        '--throughput',  '-1',
        '--producer-props',
        'bootstrap.servers=localhost:9092',
        'acks=1',
        "linger.ms=$($cfg.Linger)",
        "batch.size=$($cfg.BatchSize)",
        "buffer.memory=$($cfg.BufferMemory)",
        "max.request.size=$($cfg.MaxReqSize)",
        "compression.type=$($cfg.Compression)"
    )

    $output = & docker @perfArgs
    $output | ForEach-Object {
        Write-Host $_
        Add-Content -Path $Results -Value $_
    }
}

# --- Concurrent multi-producer runner ---
function Invoke-ConcurrentRun {
    param(
        [int]$Size,
        [string]$Compression,
        [int]$NumProducers = $ConcurrentProducers,
        [int]$RecordsPerProducer = 250000
    )

    $resolvedMode = if ($Compression -eq 'none') { 'max' } else { "max_$Compression" }
    $cfg = Get-ProducerConfig -Size $Size -Mode $resolvedMode

    $totalRecords = $NumProducers * $RecordsPerProducer
    $label  = "concurrent N=$NumProducers size=${Size}B compression=$Compression total=$totalRecords"
    $config = "config: acks=1 linger.ms=$($cfg.Linger) batch.size=$($cfg.BatchSize) buffer.memory=$($cfg.BufferMemory) compression.type=$($cfg.Compression)"

    Write-Host ""
    Write-Host "=== $label ===" -ForegroundColor Yellow
    Write-Host $config
    Add-Content -Path $Results -Value ""
    Add-Content -Path $Results -Value "=== $label ==="
    Add-Content -Path $Results -Value $config

    Reset-Topic

    $jobs = @()
    $startWall = Get-Date
    for ($i = 0; $i -lt $NumProducers; $i++) {
        $jobs += Start-Job -ScriptBlock {
            param($container, $topic, $size, $records, $cfg)
            & docker exec $container /opt/kafka/bin/kafka-producer-perf-test.sh `
                --topic $topic --num-records $records --record-size $size --throughput -1 `
                --producer-props "bootstrap.servers=localhost:9092" `
                    "acks=1" "linger.ms=$($cfg.Linger)" "batch.size=$($cfg.BatchSize)" `
                    "buffer.memory=$($cfg.BufferMemory)" "max.request.size=$($cfg.MaxReqSize)" `
                    "compression.type=$($cfg.Compression)" "client.id=concurrent-$PID"
        } -ArgumentList $Container, $Topic, $Size, $RecordsPerProducer, $cfg
    }

    Write-Host "Waiting for $NumProducers concurrent producers to finish..."
    $jobs | Wait-Job | Out-Null
    $elapsedSec = ((Get-Date) - $startWall).TotalSeconds

    $jobs | ForEach-Object {
        $jobOutput = Receive-Job -Job $_
        $jobOutput | ForEach-Object {
            Write-Host "  [job] $_"
            Add-Content -Path $Results -Value "  [job] $_"
        }
        Remove-Job -Job $_
    }

    $aggregateOpsSec = [Math]::Round($totalRecords / $elapsedSec, 0)
    $aggregateMBs    = [Math]::Round(($totalRecords * $Size) / $elapsedSec / 1MB, 2)
    $aggregate = "AGGREGATE: $totalRecords records in $([Math]::Round($elapsedSec,3))s = $aggregateOpsSec ops/s, $aggregateMBs MiB/s"
    Write-Host $aggregate -ForegroundColor Green
    Add-Content -Path $Results -Value $aggregate
}

# --- Run the matrix ---
try {
    foreach ($mode in @('unbatched', 'max', 'max_lz4', 'max_zstd')) {
        foreach ($size in $Sizes) {
            Invoke-PerfRun -Size $size -Mode $mode
        }
    }

    Write-Host ""
    Write-Host "═══════════════════════════════════════════════" -ForegroundColor Yellow
    Write-Host " Concurrent multi-producer tests ($ConcurrentProducers producers)"
    Write-Host "═══════════════════════════════════════════════" -ForegroundColor Yellow
    Invoke-ConcurrentRun -Size 256  -Compression 'none'
    Invoke-ConcurrentRun -Size 256  -Compression 'lz4'
    Invoke-ConcurrentRun -Size 4096 -Compression 'lz4'

} finally {
    Invoke-Cleanup
}

Add-Content -Path $Results -Value ""
Add-Content -Path $Results -Value "=== DONE -- results in $Results ==="
Write-Host ""
Write-Host "=== DONE -- results in $Results ===" -ForegroundColor Green
