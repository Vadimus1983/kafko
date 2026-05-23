# kafko_http_bench.ps1 -- Load-test the kafko-http server with oha across 3 codecs.
#
# Usage (from anywhere, in PowerShell):
#   .\scripts\kafko_http_bench.ps1
#   pwsh /path/to/repo/scripts/kafko_http_bench.ps1   (also fine from any cwd)
#
# The script resolves all paths relative to its own location ($PSScriptRoot),
# so the project root is always $PSScriptRoot\.. -- not whatever cwd the user
# happened to be in when they typed the command.
#
# Requirements:
#   - Rust toolchain (cargo)
#   - oha installed: cargo install oha
#
# Output:
#   scripts/tmp/kafko-http_bench_results_<YYYYMMDD-HHMMSS>.txt  (persistent)
#
# Ephemeral run folder (created at start, removed at end):
#   scripts/tmp/run_<YYYYMMDD-HHMMSS>/
#     kafko-http_data/   server WAL + segments
#     payloads/          oha payload .bin files
#     server.log         kafko-http stdout
#     server.err         kafko-http stderr
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

# --- Anchor every path to the script's own location ---
$ScriptDir   = $PSScriptRoot
$ProjectRoot = (Resolve-Path (Join-Path $ScriptDir '..')).Path
$TmpDir      = Join-Path $ScriptDir 'tmp'
New-Item -ItemType Directory -Force -Path $TmpDir | Out-Null

Push-Location $ProjectRoot
try {

$Sizes        = @(64, 256, 512, 1024, 4096, 1048576)
$Codecs       = @('none', 'lz4', 'zstd')
$Concurrency  = 16
$Timestamp    = (Get-Date).ToString('yyyyMMdd-HHmmss')
$ResultsFile  = Join-Path $TmpDir "kafko-http_bench_results_$Timestamp.txt"
$RunFolder    = Join-Path $TmpDir "run_$Timestamp"
$PayloadDir   = Join-Path $RunFolder 'payloads'
$DataDir      = Join-Path $RunFolder 'kafko-http_data'
$ServerLogOut = Join-Path $RunFolder 'server.log'
$ServerLogErr = Join-Path $RunFolder 'server.err'
$ServerUrl    = 'http://127.0.0.1:9091'

# --- State carried into Invoke-Cleanup ---
$serverProcess = $null
$writer        = $null

function Invoke-Cleanup {
    if ($script:serverProcess) {
        try { Stop-Process -Id $script:serverProcess.Id -Force -ErrorAction SilentlyContinue } catch {}
        $script:serverProcess = $null
    }
    if ($script:writer) {
        try { $script:writer.Close(); $script:writer.Dispose() } catch {}
        $script:writer = $null
    }
    Start-Sleep -Milliseconds 300
    if ($RunFolder -and (Test-Path $RunFolder)) {
        Remove-Item -Recurse -Force $RunFolder -ErrorAction SilentlyContinue
    }
}

# --- Pre-flight: oha installed? ---
if (-not (Get-Command oha -ErrorAction SilentlyContinue)) {
    Write-Host "ERROR: 'oha' not found in PATH." -ForegroundColor Red
    Write-Host "Install with:  cargo install oha"
    exit 1
}

# --- Build the kafko-http binary (workspace member) ---
Write-Host "Building kafko-http (release)..."
& cargo build --release --package kafko-http
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: cargo build failed" -ForegroundColor Red
    exit 1
}

# Resolve the binary to an absolute path -- Start-Process with -RedirectStandard*
# is unreliable with relative -FilePath values.
$BinaryRel = 'target\release\kafko-http.exe'
if (-not (Test-Path $BinaryRel)) {
    $BinaryRel = 'target\release\kafko-http'
}
if (-not (Test-Path $BinaryRel)) {
    Write-Host "ERROR: built binary not found at $BinaryRel" -ForegroundColor Red
    exit 1
}
$BinaryPath = (Resolve-Path $BinaryRel).Path

# --- Create the run folder (everything ephemeral goes here) ---
Write-Host "Creating run folder $RunFolder ..."
New-Item -Type Directory -Force -Path $RunFolder | Out-Null
New-Item -Type Directory -Force -Path $PayloadDir | Out-Null

# --- Generate payload files (all-zero bytes) inside the run folder ---
foreach ($size in $Sizes) {
    $abs = Join-Path $PayloadDir "payload_$size.bin"
    [System.IO.File]::WriteAllBytes($abs, [byte[]]::new($size))
}

# --- Open results file via StreamWriter (UTF-8, no BOM) ---
if (Test-Path $ResultsFile) { Remove-Item -Path $ResultsFile -Force }
$utf8NoBom = [System.Text.UTF8Encoding]::new($false)
$writer = [System.IO.StreamWriter]::new($ResultsFile, $false, $utf8NoBom)
$writer.AutoFlush = $true

function Write-Result {
    param([string]$Line)
    $writer.WriteLine($Line)
}

# --- Write header ---
$headerTs = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
Write-Result "kafko HTTP server benchmark (oha load test, 3 codecs)"
Write-Result "Date:    $headerTs"
Write-Result "Host:    $env:COMPUTERNAME"
Write-Result "Server:  axum + kafko, bound to $ServerUrl, 4 worker threads"
Write-Result "Client:  oha, $Concurrency concurrent connections"
Write-Result "Codecs:  none / lz4 / zstd (each on its own kafko topic)"
Write-Result "Payload: all-zero bytes (compresses trivially -- same as Kafka bench)"
Write-Result "Run:     $RunFolder (deleted on exit)"
Write-Result ""

try {
    # --- Start the kafko-http server in background ---
    Write-Host "Starting kafko-http server (data dir = $DataDir)..."
    $env:KAFKO_RESET    = '1'
    $env:KAFKO_DATA_DIR = $DataDir
    $env:KAFKO_BIND     = '127.0.0.1:9091'

    $serverProcess = Start-Process `
        -FilePath $BinaryPath `
        -PassThru `
        -NoNewWindow `
        -RedirectStandardOutput $ServerLogOut `
        -RedirectStandardError  $ServerLogErr

    if (-not $serverProcess) {
        throw "Failed to launch kafko-http"
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
        if (Test-Path $ServerLogErr) {
            Write-Host "Server did not respond. stderr log:"
            Get-Content $ServerLogErr
        }
        throw "Server did not become ready in time"
    }

    function Invoke-OhaBench {
        param([int]$Size, [string]$Codec, [int]$Total)

        $label = "size=${Size}B codec=$Codec concurrency=$Concurrency total=$Total"
        Write-Host ""
        Write-Host "=== $label ===" -ForegroundColor Cyan
        Write-Result ""
        Write-Result "=== $label ==="

        $payloadPath = Join-Path $PayloadDir "payload_$Size.bin"
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

    Write-Result ""
    Write-Result "=== DONE -- results in $ResultsFile ==="
} finally {
    Write-Host ""
    Write-Host "Stopping kafko-http and removing run folder..."
    Invoke-Cleanup
}

Write-Host ""
Write-Host "=== DONE -- results in $ResultsFile ===" -ForegroundColor Green

} finally {
    Pop-Location
}
