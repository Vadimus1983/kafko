# kafka_bench_unbatched.ps1 -- Apples-to-apples Kafka bench vs kafko_http.
#
# Forces Kafka into the SAME shape as kafko_http (one record = one network
# request) by configuring the producer with:
#   - linger.ms = 0                            (no time-based batching)
#   - batch.size = size + small overhead       (no size-based accumulation)
#   - max.in.flight.requests.per.connection=1  (no pipelining accidental batching)
#   - acks = 1                                 (matches kafko durability story)
#
# Runs 16 concurrent producer instances (matches kafko_http's -c 16) so the
# parallelism shape is identical too.
#
# The previous "max-tuned" Kafka bench let Kafka batch ~2000 records into one
# network call at small sizes -- not a fair comparison to kafko_http which
# sent one record per request. This script removes that advantage so the
# comparison is purely "protocol overhead per record" for each system.
#
# Output: kafka_bench_unbatched_results.txt
# Usage:  .\scripts\kafka_bench_unbatched.ps1
# Time:   ~12-15 minutes

# --- Force UTF-8 for console + native command IO ---
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding           = [System.Text.Encoding]::UTF8

# --- Config ---
$KafkaImage    = 'apache/kafka:3.7.0'
$Container     = 'kafka-bench-unbatched'
$Topic         = 'bench'
$Sizes         = @(64, 256, 512, 1024, 4096, 1048576)
$Codecs        = @('none', 'lz4', 'zstd')
$NumProducers  = 16
$ResultsFile   = 'kafka_bench_unbatched_results.txt'
$ClusterId     = 'ciWo7IWazngRchmPES6q5A'

# --- Cleanup helper ---
function Invoke-Cleanup {
    Write-Host "[cleanup] Stopping and removing container..."
    cmd /c "docker stop $Container >nul 2>nul"
    cmd /c "docker rm   $Container >nul 2>nul"
}

# --- Pre-flight: docker installed and daemon up ---
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Host "ERROR: 'docker' not found in PATH." -ForegroundColor Red
    exit 1
}
cmd /c "docker info >nul 2>nul"
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Docker daemon not running. Open Docker Desktop and wait for the steady icon." -ForegroundColor Red
    exit 1
}

# --- Remove any leftover container from a previous run ---
cmd /c "docker rm -f $Container >nul 2>nul"

# --- Start Kafka in KRaft single-node mode, max-tuned broker ---
Write-Host "Starting Kafka $KafkaImage with max-tuned broker config..."

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
    # Broker perf tuning (same as max-tuned bench)
    '-e', 'KAFKA_NUM_NETWORK_THREADS=8',
    '-e', 'KAFKA_NUM_IO_THREADS=8',
    '-e', 'KAFKA_SOCKET_SEND_BUFFER_BYTES=1048576',
    '-e', 'KAFKA_SOCKET_RECEIVE_BUFFER_BYTES=1048576',
    '-e', 'KAFKA_MESSAGE_MAX_BYTES=16777216',
    '-e', 'KAFKA_REPLICA_FETCH_MAX_BYTES=16777216',
    '-e', 'KAFKA_LOG_FLUSH_INTERVAL_MESSAGES=100000',
    '-e', 'KAFKA_LOG_FLUSH_INTERVAL_MS=1000',
    '-e', 'KAFKA_HEAP_OPTS=-Xmx1G -Xms1G',
    $KafkaImage
)

$containerId = & docker @startArgs
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Failed to start Kafka container" -ForegroundColor Red
    exit 1
}

# --- Wait for Kafka to be ready (up to 90s) ---
Write-Host -NoNewline "Waiting for Kafka to become ready"
$ready = $false
for ($i = 0; $i -lt 90; $i++) {
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
    Write-Host "Kafka did not become ready in 90s. Last 50 log lines:"
    & docker logs --tail 50 $Container
    Invoke-Cleanup
    exit 1
}

# --- Open results file via StreamWriter (UTF-8 no BOM, held open) ---
$resultsAbsPath = Join-Path (Get-Location) $ResultsFile
if (Test-Path $resultsAbsPath) { Remove-Item -Path $resultsAbsPath -Force }
$utf8NoBom = [System.Text.UTF8Encoding]::new($false)
$writer = [System.IO.StreamWriter]::new($resultsAbsPath, $false, $utf8NoBom)
$writer.AutoFlush = $true

function Write-Result {
    param([string]$Line)
    $writer.WriteLine($Line)
}

$timestamp = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
Write-Result "Kafka unbatched concurrent bench -- apples-to-apples vs kafko_http"
Write-Result "Date:      $timestamp"
Write-Result "Image:     $KafkaImage"
Write-Result "Mode:      KRaft single-node, acks=1, max-tuned broker"
Write-Result "Producer:  linger.ms=0, batch.size=size+1KiB, max.in.flight=1 -> one record per request"
Write-Result "Parallel:  $NumProducers concurrent producer instances (matches kafko_http -c 16)"
Write-Result "Host:      $env:COMPUTERNAME"
Write-Result ""

# --- Single unbatched concurrent run ---
function Invoke-UnbatchedConcurrentRun {
    param([int]$Size, [string]$Compression)

    if     ($Size -ge 1048576) { $totalTarget = 1000   }
    elseif ($Size -ge 4096)    { $totalTarget = 50000  }
    else                       { $totalTarget = 500000 }

    $perProducer  = [int]([math]::Ceiling($totalTarget / $NumProducers))
    $totalRecords = $perProducer * $NumProducers

    # Tiny batch.size relative to record size = one record per batch.
    # Kafka rejects batch.size < 0 but accepts very small values; we use
    # size + 1024 bytes of overhead headroom so the producer can fit exactly
    # one record + its framing into a batch.
    $batchSize = $Size + 1024
    $bufferMem = [math]::Max(67108864, $Size * 256)

    $label = "size=${Size}B codec=$Compression producers=$NumProducers per=$perProducer total=$totalRecords"
    Write-Host ""
    Write-Host "=== $label ===" -ForegroundColor Cyan
    Write-Result ""
    Write-Result "=== $label ==="

    $cfg = "config: acks=1 linger.ms=0 batch.size=$batchSize max.in.flight=1 compression=$Compression"
    Write-Host $cfg
    Write-Result $cfg

    # Recreate topic (silent delete, may not exist)
    cmd /c "docker exec $Container /opt/kafka/bin/kafka-topics.sh --delete --topic $Topic --bootstrap-server localhost:9092 >nul 2>nul"
    Start-Sleep -Milliseconds 500

    & docker exec $Container /opt/kafka/bin/kafka-topics.sh `
        --create --topic $Topic --partitions 1 --replication-factor 1 `
        --config "max.message.bytes=16777216" `
        --bootstrap-server localhost:9092 | Out-Null

    # Build bash one-liner: launch N producers in parallel, wait, dump logs.
    # Bash sees PowerShell-substituted values for $Size, $perProducer, etc.
    # Bash's own $i (loop var) is escaped with `$ so PowerShell leaves it alone.
    $bashCmd = @"
rm -f /tmp/perf-*.log
for i in `$(seq 1 $NumProducers); do
  /opt/kafka/bin/kafka-producer-perf-test.sh \
    --topic $Topic --num-records $perProducer --record-size $Size \
    --throughput -1 \
    --producer-props \
      bootstrap.servers=localhost:9092 \
      acks=1 \
      linger.ms=0 \
      batch.size=$batchSize \
      max.in.flight.requests.per.connection=1 \
      buffer.memory=$bufferMem \
      max.request.size=16777216 \
      compression.type=$Compression \
    > /tmp/perf-`$i.log 2>&1 &
done
wait
cat /tmp/perf-*.log
rm -f /tmp/perf-*.log
"@

    $startTime = Get-Date
    $output = & docker exec $Container bash -c $bashCmd
    $endTime = Get-Date

    $wallClockSec = ($endTime - $startTime).TotalSeconds

    # Parse per-producer summary lines (one per producer)
    # Format: "N records sent, R records/sec (M MB/sec), A ms avg latency, X ms max latency, P50 ms 50th, P95 ms 95th, P99 ms 99th, P999 ms 99.9th."
    $rxFinal = '^\s*(\d+) records sent, ([\d.]+) records/sec \(([\d.]+) MB/sec\), ([\d.]+) ms avg latency, ([\d.]+) ms max latency, (\d+) ms 50th, (\d+) ms 95th, (\d+) ms 99th, (\d+) ms 99\.9th'

    $producerCount   = 0
    $sumThroughput   = 0.0
    $sumMBs          = 0.0
    $sumAvgLatency   = 0.0
    $maxLatency      = 0
    $sumP50          = 0
    $sumP95          = 0
    $sumP99          = 0
    $sumP999         = 0
    $totalRecordsSeen = 0

    foreach ($line in $output) {
        Write-Host $line
        Write-Result "  $line"

        if ($line -match $rxFinal) {
            $producerCount++
            $totalRecordsSeen += [int]$matches[1]
            $sumThroughput    += [double]$matches[2]
            $sumMBs           += [double]$matches[3]
            $sumAvgLatency    += [double]$matches[4]
            $maxThis           = [double]$matches[5]
            if ($maxThis -gt $maxLatency) { $maxLatency = $maxThis }
            $sumP50           += [int]$matches[6]
            $sumP95           += [int]$matches[7]
            $sumP99           += [int]$matches[8]
            $sumP999          += [int]$matches[9]
        }
    }

    if ($producerCount -gt 0) {
        $avgLatency = [math]::Round($sumAvgLatency / $producerCount, 2)
        $avgP50     = [math]::Round($sumP50 / $producerCount, 1)
        $avgP95     = [math]::Round($sumP95 / $producerCount, 1)
        $avgP99     = [math]::Round($sumP99 / $producerCount, 1)
        $avgP999    = [math]::Round($sumP999 / $producerCount, 1)
        $wallTput   = [math]::Round($totalRecordsSeen / $wallClockSec, 0)
        $wallMBs    = [math]::Round(($totalRecordsSeen * $Size) / $wallClockSec / 1048576, 2)

        $summary = @(
            "",
            "AGGREGATE ($producerCount producers, $totalRecordsSeen records):",
            "  wall-clock runtime:   $([math]::Round($wallClockSec, 3))s",
            "  wall-clock throughput: $wallTput rec/s, $wallMBs MiB/s",
            "  sum of per-producer:   $([math]::Round($sumThroughput, 0)) rec/s, $([math]::Round($sumMBs, 2)) MB/s",
            "  avg latency (mean of per-producer means): $avgLatency ms",
            "  max latency (max of per-producer maxes):  $maxLatency ms",
            "  avg p50:  $avgP50 ms",
            "  avg p95:  $avgP95 ms",
            "  avg p99:  $avgP99 ms",
            "  avg p999: $avgP999 ms"
        )
        foreach ($s in $summary) {
            Write-Host $s -ForegroundColor Green
            Write-Result $s
        }
    } else {
        Write-Host "WARNING: no producer summary lines parsed" -ForegroundColor Yellow
        Write-Result "WARNING: no producer summary lines parsed"
    }
}

# --- Matrix: 3 codecs x 6 sizes = 18 runs ---
try {
    foreach ($codec in $Codecs) {
        Write-Host ""
        Write-Host ("=" * 60) -ForegroundColor Yellow
        Write-Host " CODEC: $codec" -ForegroundColor Yellow
        Write-Host ("=" * 60) -ForegroundColor Yellow

        foreach ($size in $Sizes) {
            Invoke-UnbatchedConcurrentRun -Size $size -Compression $codec
        }
    }
} finally {
    Write-Result ""
    Write-Result "=== DONE -- results in $ResultsFile ==="
    $writer.Close()
    $writer.Dispose()
    Invoke-Cleanup
}

Write-Host ""
Write-Host "=== DONE -- results in $ResultsFile ===" -ForegroundColor Green
