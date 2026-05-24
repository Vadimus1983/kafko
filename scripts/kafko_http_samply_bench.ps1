# kafko_http_samply_bench.ps1 -- Profile kafko-http under samply while oha drives load.
#
# Usage (from anywhere, in PowerShell):
#   .\scripts\kafko_http_samply_bench.ps1
#   pwsh /path/to/repo/scripts/kafko_http_samply_bench.ps1
#
# Requirements:
#   - Rust toolchain (cargo)
#   - oha:    cargo install oha
#   - samply: cargo install samply
#
# Output (persisted in scripts/tmp/):
#   kafko-http_samply_results_<ts>.txt       bench output (oha tables)
#   kafko-http_samply_<ts>.profile.json      samply profile
#       View with:  samply load <file>
#       or upload at https://profiler.firefox.com/
#
# Ephemeral run folder (created at start, removed at end):
#   scripts/tmp/run_<ts>/
#     kafko-http_data/   server WAL + segments
#     payloads/          oha payload .bin files
#     server.log         kafko-http stdout (under samply)
#     server.err         kafko-http stderr + samply stderr
#
# Notes:
#   - The kafko-http binary is built in DEBUG mode. Throughput numbers from this
#     script are NOT comparable to kafko_http_bench.ps1 (which uses release).
#     The point here is to capture a profile, not to measure performance.
#   - The matrix is smaller than the release bench so total runtime stays
#     manageable in debug.
#   - Cleanup kills the kafko-http child first (found via parent-PID lookup on
#     samply's process). That lets samply observe child exit and flush the
#     profile to disk before terminating. Killing samply directly can truncate
#     or skip the profile.

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

# --- Config (smaller matrix than release bench: profiling, not benchmarking) ---
$Sizes        = @(64, 256, 1024, 4096, 131072, 1048576)
$Codecs       = @('none', 'lz4', 'zstd')
$Concurrency  = 16
$Timestamp    = (Get-Date).ToString('yyyyMMdd-HHmmss')
$ResultsFile  = Join-Path $TmpDir "kafko-http_samply_results_$Timestamp.txt"
$ProfileFile  = Join-Path $TmpDir "kafko-http_samply_$Timestamp.profile.json"
$RunFolder    = Join-Path $TmpDir "run_$Timestamp"
$PayloadDir   = Join-Path $RunFolder 'payloads'
$DataDir      = Join-Path $RunFolder 'kafko-http_data'
# Logs live OUTSIDE the run folder so they survive cleanup and can be inspected
# when samply misbehaves (e.g. doesn't flush the profile).
$ServerLogOut = Join-Path $TmpDir  "kafko-http_samply_$Timestamp.server.log"
$ServerLogErr = Join-Path $TmpDir  "kafko-http_samply_$Timestamp.server.err"
$ServerUrl    = 'http://127.0.0.1:9091'

# --- State carried into Invoke-Cleanup ---
$samplyProcess = $null
$writer        = $null

function Invoke-Cleanup {
    # Kill the kafko-http child first so samply observes a clean exit and
    # flushes the profile. If we killed samply itself, the profile would be
    # truncated or missing entirely.
    if ($script:samplyProcess -and -not $script:samplyProcess.HasExited) {
        $childrenKilled = 0
        try {
            $children = Get-CimInstance Win32_Process `
                -Filter "ParentProcessId = $($script:samplyProcess.Id)" `
                -ErrorAction SilentlyContinue
            foreach ($child in $children) {
                if ($child.Name -like 'kafko-http*') {
                    Write-Host ("  killing kafko-http child PID {0}" -f $child.ProcessId)
                    Stop-Process -Id $child.ProcessId -Force -ErrorAction SilentlyContinue
                    $childrenKilled++
                }
            }
        } catch {}
        if ($childrenKilled -eq 0) {
            Write-Host "  no kafko-http child found under samply PID $($script:samplyProcess.Id) (already exited?)"
        }

        # Give samply up to 30s to write the profile and exit naturally.
        # Profile flush time depends on profile size; a few-minute debug bench
        # run can produce 10-100 MiB of profile data.
        Write-Host -NoNewline "  waiting for samply to flush profile"
        $waited = 0
        for ($i = 0; $i -lt 60; $i++) {
            if ($script:samplyProcess.HasExited) {
                Write-Host (" exited after {0}s" -f $waited)
                break
            }
            Start-Sleep -Milliseconds 500
            $waited += 0.5
            if (($i % 4) -eq 3) { Write-Host -NoNewline "." }
        }
        if (-not $script:samplyProcess.HasExited) {
            Write-Host " TIMEOUT (force-killing samply; profile likely lost)" -ForegroundColor Yellow
            Stop-Process -Id $script:samplyProcess.Id -Force -ErrorAction SilentlyContinue
        }
        $script:samplyProcess = $null
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

# --- Pre-flight checks ---
if (-not (Get-Command oha -ErrorAction SilentlyContinue)) {
    Write-Host "ERROR: 'oha' not found in PATH." -ForegroundColor Red
    Write-Host "Install with:  cargo install oha"
    exit 1
}
if (-not (Get-Command samply -ErrorAction SilentlyContinue)) {
    Write-Host "ERROR: 'samply' not found in PATH." -ForegroundColor Red
    Write-Host "Install with:  cargo install samply"
    exit 1
}

# --- Build the kafko-http binary (debug, so the profile has symbols) ---
Write-Host "Building kafko-http (debug)..."
& cargo build --package kafko-http
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: cargo build failed" -ForegroundColor Red
    exit 1
}

$BinaryRel = 'target\debug\kafko-http.exe'
if (-not (Test-Path $BinaryRel)) {
    $BinaryRel = 'target\debug\kafko-http'
}
if (-not (Test-Path $BinaryRel)) {
    Write-Host "ERROR: built binary not found at $BinaryRel" -ForegroundColor Red
    exit 1
}
$BinaryPath = (Resolve-Path $BinaryRel).Path

# --- Create the run folder ---
Write-Host "Creating run folder $RunFolder ..."
New-Item -Type Directory -Force -Path $RunFolder | Out-Null
New-Item -Type Directory -Force -Path $PayloadDir | Out-Null

# --- Generate payload files inside the run folder ---
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
Write-Result "kafko-http samply profile run (DEBUG build, oha load test, 3 codecs)"
Write-Result "Date:     $headerTs"
Write-Result "Host:     $env:COMPUTERNAME"
Write-Result "Server:   axum + kafko, $ServerUrl, debug build under samply"
Write-Result "Client:   oha, $Concurrency concurrent connections"
Write-Result "Codecs:   none / lz4 / zstd (each on its own kafko topic)"
Write-Result "Payload:  all-zero bytes"
Write-Result "Profile:  $ProfileFile"
Write-Result "Run:      $RunFolder (deleted on exit)"
Write-Result "Note:     debug build; throughput is NOT comparable to release bench"
Write-Result ""

try {
    # --- Start kafko-http under samply ---
    Write-Host "Starting kafko-http under samply..."
    Write-Host "  profile -> $ProfileFile"
    $env:KAFKO_RESET    = '1'
    $env:KAFKO_DATA_DIR = $DataDir
    $env:KAFKO_BIND     = '127.0.0.1:9091'

    # --save-only: don't spin up the local UI server after recording.
    # --no-open:   don't try to open a browser tab.
    # --verbose:   print samply progress to its stderr (we capture and show on failure).
    $samplyArgs = @(
        'record',
        '--save-only',
        '--no-open',
        '--verbose',
        '-o', $ProfileFile,
        '--',
        $BinaryPath
    )
    Write-Host ("  samply " + ($samplyArgs -join ' '))
    $samplyProcess = Start-Process -FilePath samply `
        -ArgumentList $samplyArgs `
        -PassThru `
        -NoNewWindow `
        -RedirectStandardOutput $ServerLogOut `
        -RedirectStandardError  $ServerLogErr

    if (-not $samplyProcess) {
        throw "Failed to launch samply"
    }
    Write-Host ("  samply PID = {0}" -f $samplyProcess.Id)

    # --- Wait for server to be ready (debug startup is slower; allow 30s) ---
    Write-Host -NoNewline "Waiting for server to respond"
    $ready = $false
    for ($i = 0; $i -lt 60; $i++) {
        try {
            $resp = Invoke-WebRequest -Uri "$ServerUrl/hwm" `
                -UseBasicParsing -TimeoutSec 1 -ErrorAction Stop
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

    # --- Run the matrix (smaller than release bench for reasonable debug runtime) ---
    foreach ($codec in $Codecs) {
        Write-Host ""
        Write-Host ("=" * 60) -ForegroundColor Yellow
        Write-Host " CODEC: $codec" -ForegroundColor Yellow
        Write-Host ("=" * 60) -ForegroundColor Yellow

        foreach ($size in $Sizes) {
            if     ($size -ge 1048576) { $total = 200    }
            elseif ($size -ge 131072)  { $total = 500    }
            elseif ($size -ge 4096)    { $total = 5000   }
            else                       { $total = 50000  }

            Invoke-OhaBench -Size $size -Codec $codec -Total $total
        }
    }

    Write-Result ""
    Write-Result "=== DONE -- results in $ResultsFile ==="
    Write-Result "=== profile in $ProfileFile ==="
} finally {
    Write-Host ""
    Write-Host "Stopping kafko-http (letting samply flush profile)..."
    Invoke-Cleanup
}

Write-Host ""
# samply 0.13 sometimes appends .gz to the -o filename when output is gzipped;
# check both. Use Select-Object so a single-match pipeline doesn't auto-unwrap
# the string and let [0] index into characters.
$actual = $null
foreach ($candidate in @($ProfileFile, "${ProfileFile}.gz")) {
    if (Test-Path -LiteralPath $candidate) {
        $actual = $candidate
        break
    }
}
if ($actual) {
    $profileSizeMiB = (Get-Item -LiteralPath $actual).Length / 1MB
    Write-Host ("Profile saved: {0} ({1:N1} MiB)" -f $actual, $profileSizeMiB) `
        -ForegroundColor Green
    Write-Host "View it:"
    Write-Host ("  samply load `"{0}`"" -f $actual)
    Write-Host "  or upload to https://profiler.firefox.com/"
} else {
    Write-Host "WARNING: profile file not found." -ForegroundColor Yellow
    Write-Host "  expected at: $ProfileFile"
    Write-Host "  or gzipped:  $ProfileFile.gz"
    Write-Host ""
    # Scan TmpDir + CWD + ProjectRoot for any stray profile.json* in case samply
    # wrote it somewhere else.
    $scanDirs = @($TmpDir, (Get-Location).Path, $ProjectRoot) | Select-Object -Unique
    $stray = $scanDirs | ForEach-Object {
        Get-ChildItem -Path $_ -Filter "*profile*.json*" -ErrorAction SilentlyContinue
    } | Where-Object { $_.LastWriteTime -gt (Get-Date).AddMinutes(-30) }
    if ($stray) {
        Write-Host "  found these recent profile-shaped files (may be from samply):" -ForegroundColor Yellow
        $stray | ForEach-Object { Write-Host ("    {0}  ({1:N1} MiB)" -f $_.FullName, ($_.Length / 1MB)) }
    }
    Write-Host ""
    Write-Host "samply stderr (preserved at $ServerLogErr):" -ForegroundColor Yellow
    if ((Test-Path $ServerLogErr) -and ((Get-Item $ServerLogErr).Length -gt 0)) {
        Get-Content $ServerLogErr | Select-Object -Last 50 | ForEach-Object { Write-Host "  $_" }
    } else {
        Write-Host "  (empty)"
    }
    Write-Host ""
    Write-Host "samply stdout (preserved at $ServerLogOut):" -ForegroundColor Yellow
    if ((Test-Path $ServerLogOut) -and ((Get-Item $ServerLogOut).Length -gt 0)) {
        Get-Content $ServerLogOut | Select-Object -Last 50 | ForEach-Object { Write-Host "  $_" }
    } else {
        Write-Host "  (empty)"
    }
}
Write-Host "Results: $ResultsFile" -ForegroundColor Green
Write-Host "Logs:    $ServerLogOut / .err (preserved across runs, gitignored)"

} finally {
    Pop-Location
}
