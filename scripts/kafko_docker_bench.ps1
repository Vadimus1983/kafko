# kafko_docker_bench.ps1 -- Bench the kafko HTTP server inside Docker.
#
# Mirrors scripts/kafka_bench_max.ps1 exactly:
#   - Builds a Linux container holding both kafko-http and oha
#   - Starts the container, kafko-http listens on 0.0.0.0:9091 inside it
#   - Runs oha via `docker exec` so the load test stays inside the container's
#     network namespace (same shape as kafka-producer-perf-test.sh in the
#     Kafka container -- server + client both on container loopback)
#   - Tears the container down at the end
#
# Output: scripts/tmp/kafko_docker_bench_results_<YYYYMMDD-HHMMSS>.txt
#
# Usage:
#   .\scripts\kafko_docker_bench.ps1
#
# Requires: Docker Desktop running.

# --- Force UTF-8 for console + native command IO (oha prints block chars) ---
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding           = [System.Text.Encoding]::UTF8

# --- Anchor every path to the script's own location ---
$ScriptDir   = $PSScriptRoot
$ProjectRoot = (Resolve-Path (Join-Path $ScriptDir '..')).Path
$TmpDir      = Join-Path $ScriptDir 'tmp'
New-Item -ItemType Directory -Force -Path $TmpDir | Out-Null
Push-Location $ProjectRoot
try {

# --- Config ---
#
# $Sizes includes 131072 (128 KiB) so we can compare kafko's single-HTTP-request
# throughput at the SAME payload size as Kafka's natural client batch. With
# Kafka's default-tuned producer (batch.size=128 KiB, linger.ms=50), every
# Kafka network call carries ~128 KiB of records. The 131072 cell here puts
# kafko at the same per-network-call payload size and tells us whether kafko
# wins/loses on raw protocol+storage efficiency once Kafka's batching is removed
# from the comparison.
$Image        = 'kafko-http:bench'
$Container    = 'kafko-http-bench'
$Sizes        = @(64, 256, 512, 1024, 4096, 131072, 1048576)
$Codecs       = @('none', 'lz4', 'zstd')
$Concurrency  = 16
$Timestamp    = (Get-Date).ToString('yyyyMMdd-HHmmss')
$ResultsFile  = Join-Path $TmpDir "kafko_docker_bench_results_$Timestamp.txt"
$HostPort     = 9091

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

# --- Build the image ---
Write-Host "Building Docker image $Image..."
& docker build -t $Image -f scripts/Dockerfile .
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: docker build failed" -ForegroundColor Red
    exit 1
}

# --- Remove any leftover container from a previous run ---
cmd /c "docker rm -f $Container >nul 2>nul"

# --- Start the container ---
Write-Host "Starting kafko-http container..."
$containerId = & docker run -d `
    --name $Container `
    -p "${HostPort}:9091" `
    $Image
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: failed to start container" -ForegroundColor Red
    exit 1
}

# From this point, ANY failure must clean up the container. Wrap the rest in
# a single try/finally so neither readiness checks, payload gen, StreamWriter
# construction, nor the matrix can leak a running container.
$writer = $null
try {
    # --- Wait for kafko-http to respond (via host port, up to 30s) ---
    Write-Host -NoNewline "Waiting for kafko-http to become ready"
    $ready = $false
    for ($i = 0; $i -lt 60; $i++) {
        try {
            $resp = Invoke-WebRequest -Uri "http://127.0.0.1:$HostPort/hwm" `
                -UseBasicParsing -TimeoutSec 1 -ErrorAction Stop
            if ($resp.StatusCode -eq 200) { $ready = $true; Write-Host " OK"; break }
        } catch {
            Write-Host -NoNewline "."
            Start-Sleep -Milliseconds 500
        }
    }
    if (-not $ready) {
        Write-Host " FAILED" -ForegroundColor Red
        Write-Host "Container logs:"
        & docker logs --tail 80 $Container
        throw "kafko-http did not become ready in time"
    }

    # --- Generate payload files INSIDE the container (/dev/zero) ---
    Write-Host "Generating payload files inside container..."
    foreach ($size in $Sizes) {
        & docker exec $Container sh -c "head -c $size /dev/zero > /tmp/payload_$size.bin"
        if ($LASTEXITCODE -ne 0) {
            throw "failed to create /tmp/payload_$size.bin in container"
        }
    }

    # --- Open results file via StreamWriter (UTF-8 no BOM, held open) ---
    if (Test-Path $ResultsFile) { Remove-Item -Path $ResultsFile -Force }
    $utf8NoBom = [System.Text.UTF8Encoding]::new($false)
    $writer = [System.IO.StreamWriter]::new($ResultsFile, $false, $utf8NoBom)
    $writer.AutoFlush = $true

    function Write-Result {
        param([string]$Line)
        $writer.WriteLine($Line)
    }

    $headerTs = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
    Write-Result "kafko HTTP bench inside Docker (oha load test, 3 codecs)"
    Write-Result "Date:      $headerTs"
    Write-Result "Host:      $env:COMPUTERNAME"
    Write-Result "Image:     $Image"
    Write-Result "Container: $Container"
    Write-Result "Network:   container loopback (oha + kafko-http in same netns)"
    Write-Result "Client:    oha (in container), $Concurrency concurrent connections"
    Write-Result "Codecs:    none / lz4 / zstd (each on its own kafko topic)"
    Write-Result "Payload:   all-zero bytes (compresses trivially -- same as Kafka bench)"
    Write-Result ""

    function Invoke-DockerOhaBench {
        param([int]$Size, [string]$Codec, [int]$Total)

        $label = "size=${Size}B codec=$Codec concurrency=$Concurrency total=$Total"
        Write-Host ""
        Write-Host "=== $label ===" -ForegroundColor Cyan
        Write-Result ""
        Write-Result "=== $label ==="

        $url = "http://127.0.0.1:9091/produce/$Codec"
        $payload = "/tmp/payload_$Size.bin"

        $output = & docker exec $Container oha `
            -n $Total `
            -c $Concurrency `
            --no-tui `
            -m POST `
            -H "Content-Type: application/octet-stream" `
            -D $payload `
            $url

        foreach ($line in $output) {
            Write-Host $line
            Write-Result $line
        }
    }

    # --- Matrix: 3 codecs x 6 sizes = 18 runs ---
    foreach ($codec in $Codecs) {
        Write-Host ""
        Write-Host ("=" * 60) -ForegroundColor Yellow
        Write-Host " CODEC: $codec" -ForegroundColor Yellow
        Write-Host ("=" * 60) -ForegroundColor Yellow

        foreach ($size in $Sizes) {
            if     ($size -ge 1048576) { $total = 1000   }
            elseif ($size -ge 131072)  { $total = 5000   }   # 128 KiB tier (Kafka batch-equivalent payload)
            elseif ($size -ge 4096)    { $total = 50000  }
            else                       { $total = 500000 }

            Invoke-DockerOhaBench -Size $size -Codec $codec -Total $total
        }
    }

    Write-Result ""
    Write-Result "=== DONE -- results in $ResultsFile ==="
} finally {
    if ($writer) {
        try { $writer.Close(); $writer.Dispose() } catch {}
    }
    Invoke-Cleanup
}

Write-Host ""
Write-Host "=== DONE -- results in $ResultsFile ===" -ForegroundColor Green

} finally {
    Pop-Location
}
