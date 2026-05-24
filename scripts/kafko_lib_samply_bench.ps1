# kafko_lib_samply_bench.ps1 -- Profile kafko's in-process append matrix under samply.
#
# Usage (from anywhere, in PowerShell):
#   .\scripts\kafko_lib_samply_bench.ps1
#   pwsh /path/to/repo/scripts/kafko_lib_samply_bench.ps1
#
# Requirements:
#   - Rust toolchain (cargo)
#   - samply: cargo install samply
#
# Output (persisted in scripts/tmp/):
#   kafko-lib_samply_results_<ts>.txt           bench stdout+stderr
#   kafko-lib_samply_<ts>.profile.json[.gz]     samply profile
#       View with:  samply load <file>
#       or upload at https://profiler.firefox.com/
#
# Ephemeral run folder (created at start, removed at end):
#   scripts/tmp/run_<ts>/
#     kafko-bench_data/   broker WAL + segments
#
# Notes:
#   - This script wraps `kafko-bench`, a separate workspace binary that opens a
#     Kafko directly and runs the same matrix as the HTTP samply bench but with
#     no HTTP / axum / hyper / oha in the picture. The resulting profile shows
#     only kafko's storage hot path plus tokio task scheduling.
#   - The binary runs the matrix and exits on its own. samply observes the
#     child exit and flushes the profile naturally -- no PID-killing dance.
#   - Built in DEBUG so the profile is symbolicated. Throughput numbers here
#     are NOT comparable to release benches; this is for finding hot spots.

# --- Force UTF-8 for console + native command IO ---
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
$Timestamp    = (Get-Date).ToString('yyyyMMdd-HHmmss')
$ResultsFile  = Join-Path $TmpDir "kafko-lib_samply_results_$Timestamp.txt"
$ProfileFile  = Join-Path $TmpDir "kafko-lib_samply_$Timestamp.profile.json"
$RunFolder    = Join-Path $TmpDir "run_$Timestamp"
$DataDir      = Join-Path $RunFolder 'kafko-bench_data'

# --- State carried into Invoke-Cleanup ---
$samplyProcess = $null

function Invoke-Cleanup {
    # The bench binary should have exited on its own (which causes samply to
    # save and exit). If samply is still running here something went wrong;
    # try the gentle path first, then escalate.
    if ($script:samplyProcess -and -not $script:samplyProcess.HasExited) {
        Write-Host "  samply still running after bench finished; killing child kafko-bench so samply flushes"
        try {
            $children = Get-CimInstance Win32_Process `
                -Filter "ParentProcessId = $($script:samplyProcess.Id)" `
                -ErrorAction SilentlyContinue
            foreach ($child in $children) {
                if ($child.Name -like 'kafko-bench*') {
                    Stop-Process -Id $child.ProcessId -Force -ErrorAction SilentlyContinue
                }
            }
        } catch {}

        for ($i = 0; $i -lt 60; $i++) {
            if ($script:samplyProcess.HasExited) { break }
            Start-Sleep -Milliseconds 500
        }
        if (-not $script:samplyProcess.HasExited) {
            Write-Host "  TIMEOUT (force-killing samply; profile may be lost)" -ForegroundColor Yellow
            Stop-Process -Id $script:samplyProcess.Id -Force -ErrorAction SilentlyContinue
        }
        $script:samplyProcess = $null
    }
    Start-Sleep -Milliseconds 300
    if ($RunFolder -and (Test-Path $RunFolder)) {
        Remove-Item -Recurse -Force $RunFolder -ErrorAction SilentlyContinue
    }
}

# --- Pre-flight: samply installed? ---
if (-not (Get-Command samply -ErrorAction SilentlyContinue)) {
    Write-Host "ERROR: 'samply' not found in PATH." -ForegroundColor Red
    Write-Host "Install with:  cargo install samply"
    exit 1
}

# --- Build kafko-bench (debug, for symbols) ---
Write-Host "Building kafko-bench (debug)..."
& cargo build --package kafko-bench
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: cargo build failed" -ForegroundColor Red
    exit 1
}

$BinaryRel = 'target\debug\kafko-bench.exe'
if (-not (Test-Path $BinaryRel)) {
    $BinaryRel = 'target\debug\kafko-bench'
}
if (-not (Test-Path $BinaryRel)) {
    Write-Host "ERROR: built binary not found at $BinaryRel" -ForegroundColor Red
    exit 1
}
$BinaryPath = (Resolve-Path $BinaryRel).Path

# --- Create run folder for the bench data dir ---
Write-Host "Creating run folder $RunFolder ..."
New-Item -Type Directory -Force -Path $RunFolder | Out-Null

# --- Environment for the bench binary ---
$env:KAFKO_RESET           = '1'
$env:KAFKO_BENCH_DATA_DIR  = $DataDir

try {
    # --- Start kafko-bench under samply ---
    # --save-only: don't spin up the local UI server after recording.
    # --no-open:   don't try to open a browser tab.
    # --verbose:   print samply progress to stderr (captured to results file).
    $samplyArgs = @(
        'record',
        '--save-only',
        '--no-open',
        '--verbose',
        '-o', $ProfileFile,
        '--',
        $BinaryPath
    )
    Write-Host ("Starting samply: samply " + ($samplyArgs -join ' '))
    Write-Host "  profile -> $ProfileFile"
    Write-Host "  results -> $ResultsFile"

    if (Test-Path $ResultsFile) { Remove-Item -Path $ResultsFile -Force }

    $samplyProcess = Start-Process -FilePath samply `
        -ArgumentList $samplyArgs `
        -PassThru `
        -NoNewWindow `
        -RedirectStandardOutput $ResultsFile `
        -RedirectStandardError  "$ResultsFile.err"

    if (-not $samplyProcess) {
        throw "Failed to launch samply"
    }
    Write-Host ("  samply PID = {0}" -f $samplyProcess.Id)

    # --- Wait for the matrix to finish (no manual termination needed) ---
    Write-Host -NoNewline "Waiting for kafko-bench to finish"
    while (-not $samplyProcess.HasExited) {
        Start-Sleep -Seconds 2
        Write-Host -NoNewline "."
    }
    Write-Host ""
    Write-Host ("samply exited with code {0}" -f $samplyProcess.ExitCode)
} finally {
    Write-Host ""
    Write-Host "Cleaning up..."
    Invoke-Cleanup
}

Write-Host ""
# samply 0.13 may append .gz when output is gzipped; check both.
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
    Write-Host "samply stderr (tail):" -ForegroundColor Yellow
    if (Test-Path "$ResultsFile.err") {
        Get-Content "$ResultsFile.err" | Select-Object -Last 30 | ForEach-Object { Write-Host "  $_" }
    }
}
Write-Host ""
Write-Host "Results (bench output): $ResultsFile" -ForegroundColor Green
if (Test-Path "$ResultsFile.err") {
    Write-Host "samply log:             $ResultsFile.err"
}

} finally {
    Pop-Location
}
