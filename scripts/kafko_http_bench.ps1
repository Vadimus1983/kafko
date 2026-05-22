# kafko_http_bench.ps1 -- Load-test the kafko HTTP server with oha across 3 codecs.
#
# Usage (from project root, in PowerShell):
#   .\scripts\kafko_http_bench.ps1
#
# Requirements:
#   - Rust toolchain (cargo)
#   - oha installed: cargo install oha
#
# Output:
#   kafko_http_bench_results.txt in the current directory
#
# What it does:
#   1. Builds kafko_http binary in release mode with http-server feature
#   2. Generates all-zero payload files (same shape as Kafka bench)
#   3. Starts kafko_http server (3 topics: bench_none / bench_lz4 / bench_zstd)
#   4. Runs oha against POST /produce/:codec for each codec x size
#   5. Stops the server, cleans up
#
# Encoding notes (Windows PowerShell 5.1):
#   - We set [Console]::OutputEncoding to UTF-8 so that bytes printed by `oha`
#     (block chars 0xE2 0x96 0xA0 = U+2588) decode as the real glyph instead of
#     mojibake like `Γûá`.
#   - We hold the results file open via a single StreamWriter for the entire
#     session; rapid Add-Content calls collided with Windows Defender / shadow
#     handle scans and produced "file is being used by another process" errors.

# --- Force UTF-8 for console + native command IO (fixes histogram glyphs) ---
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding           = [System.Text.Encoding]::UTF8

$Sizes        = @(64, 256, 512, 1024, 4096, 1048576)
$Codecs       = @('none', 'lz4', 'zstd')
$Concurrency  = 16
$ResultsFile  = 'kafko_http_bench_results.txt'
$ServerUrl    = 'http://127.0.0.1:9091'
$PayloadDir   = 'bench_payloads'
$ServerLogOut = 'kafko_http.log'
$ServerLogErr = 'kafko_http.err'

# --- Pre-flight: oha installed? ---
if (-not (Get-Command oha -ErrorAction SilentlyContinue)) {
    Write-Host "ERROR: 'oha' not found in PATH." -ForegroundColor Red
    Write-Host "Install with:  cargo install oha"
    exit 1
}

# --- Build the kafko_http binary ---
Write-Host "Building kafko_http (release, http-server feature)..."
& cargo build --release --bin kafko_http --features http-server
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: cargo build failed" -ForegroundColor Red
    exit 1
}

$BinaryPath = '.\target\release\kafko_http.exe'
if (-not (Test-Path $BinaryPath)) {
    $BinaryPath = '.\target\release\kafko_http'
}

# --- Generate payload files (all-zero bytes) ---
Write-Host "Generating payload files in $PayloadDir/..."
New-Item -Type Directory -Force -Path $PayloadDir | Out-Null
foreach ($size in $Sizes) {
    $relPath = Join-Path $PayloadDir "payload_$size.bin"
    if (-not (Test-Path $relPath)) {
        $abs = Join-Path (Get-Location) $relPath
        [System.IO.File]::WriteAllBytes($abs, [byte[]]::new($size))
    }
}

# --- Open results file once via StreamWriter (UTF-8, no BOM) ---
$resultsAbsPath = Join-Path (Get-Location) $ResultsFile
if (Test-Path $resultsAbsPath) { Remove-Item -Path $resultsAbsPath -Force }
$utf8NoBom = [System.Text.UTF8Encoding]::new($false)
$writer = [System.IO.StreamWriter]::new($resultsAbsPath, $false, $utf8NoBom)
$writer.AutoFlush = $true

function Write-Result {
    param([string]$Line)
    $writer.WriteLine($Line)
}

# --- Write header ---
$timestamp = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
Write-Result "kafko HTTP server benchmark (oha load test, 3 codecs)"
Write-Result "Date:    $timestamp"
Write-Result "Host:    $env:COMPUTERNAME"
Write-Result "Server:  axum + kafko, bound to $ServerUrl, 4 worker threads"
Write-Result "Client:  oha, $Concurrency concurrent connections"
Write-Result "Codecs:  none / lz4 / zstd (each on its own kafko topic)"
Write-Result "Payload: all-zero bytes (compresses trivially -- same as Kafka bench)"
Write-Result ""

# --- Start the kafko_http server in background ---
Write-Host "Starting kafko_http server (resetting data dir)..."
$env:KAFKO_RESET    = '1'
$env:KAFKO_DATA_DIR = '.\kafko_http_data'
$env:KAFKO_BIND     = '127.0.0.1:9091'

$serverProcess = Start-Process `
    -FilePath $BinaryPath `
    -PassThru `
    -NoNewWindow `
    -RedirectStandardOutput $ServerLogOut `
    -RedirectStandardError  $ServerLogErr

if (-not $serverProcess) {
    Write-Host "ERROR: Failed to launch kafko_http" -ForegroundColor Red
    $writer.Close()
    exit 1
}

# --- Wait for server to be ready (up to 15s) ---
Write-Host -NoNewline "Waiting for server to respond"
$ready = $false
for ($i = 0; $i -lt 30; $i++) {
    try {
        $resp = Invoke-WebRequest -Uri "$ServerUrl/hwm" -UseBasicParsing -TimeoutSec 1 -ErrorAction Stop
        if ($resp.StatusCode -eq 200) {
            $ready = $true
            Write-Host " OK"
            break
        }
    } catch {
        Write-Host -NoNewline "."
        Start-Sleep -Milliseconds 500
    }
}

if (-not $ready) {
    Write-Host " FAILED" -ForegroundColor Red
    Write-Host "Server did not respond. stderr log:"
    if (Test-Path $ServerLogErr) { Get-Content $ServerLogErr }
    Stop-Process -Id $serverProcess.Id -Force -ErrorAction SilentlyContinue
    $writer.Close()
    exit 1
}

# --- Bench runner ---
function Invoke-OhaBench {
    param([int]$Size, [string]$Codec, [int]$Total)

    $label = "size=${Size}B codec=$Codec concurrency=$Concurrency total=$Total"
    Write-Host ""
    Write-Host "=== $label ===" -ForegroundColor Cyan
    Write-Result ""
    Write-Result "=== $label ==="

    $payloadPath = Join-Path (Get-Location) (Join-Path $PayloadDir "payload_$Size.bin")
    $url = "$ServerUrl/produce/$Codec"

    $output = & oha `
        -n $Total `
        -c $Concurrency `
        --no-tui `
        -m POST `
        -H "Content-Type: application/octet-stream" `
        -D "$payloadPath" `
        $url

    foreach ($line in $output) {
        Write-Host $line
        Write-Result $line
    }
}

# --- Run the matrix: 3 codecs x 6 sizes = 18 runs ---
try {
    foreach ($codec in $Codecs) {
        Write-Host ""
        Write-Host ("=" * 60) -ForegroundColor Yellow
        Write-Host " CODEC: $codec" -ForegroundColor Yellow
        Write-Host ("=" * 60) -ForegroundColor Yellow

        foreach ($size in $Sizes) {
            if     ($size -ge 1048576) { $total = 1000   }
            elseif ($size -ge 4096)    { $total = 50000  }
            else                       { $total = 500000 }

            Invoke-OhaBench -Size $size -Codec $codec -Total $total
        }
    }
} finally {
    Write-Host ""
    Write-Host "Stopping kafko_http server..."
    Stop-Process -Id $serverProcess.Id -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 1

    Write-Result ""
    Write-Result "=== DONE -- results in $ResultsFile ==="
    $writer.Close()
    $writer.Dispose()
}

Write-Host ""
Write-Host "=== DONE -- results in $ResultsFile ===" -ForegroundColor Green
