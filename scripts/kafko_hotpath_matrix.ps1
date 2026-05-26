# kafko_hotpath_matrix.ps1 -- Run the kafko-bench hotpath measurement matrix.
#
# Builds kafko-bench with the `hotpath` + `hotpath-alloc` features and runs
# each scenario (sequential, concurrent, batch) in its own process so the
# per-function timing and allocation tables hotpath prints at exit are clean
# and isolated per access pattern.
#
# Why three separate processes:
#   hotpath's counters live in process memory and are not reset between runs.
#   Running all three scenarios in one process would conflate their tables.
#   One scenario = one process = one independent measurement.
#
# Why no hotpath-cpu:
#   hotpath-cpu is only supported on macOS/Linux. On Windows we use wall-time
#   (the default) plus allocation tracking (hotpath-alloc).
#
# Usage (PowerShell):
#   .\scripts\kafko_hotpath_matrix.ps1
#
# Output (under scripts/tmp/hotpath_<ts>/):
#   sequential.txt  -- per-function timing + alloc table for the sequential scenario
#   concurrent.txt  -- ditto for the 16-task concurrent scenario
#   batch.txt       -- ditto for the send_batch(1024) scenario
#   summary.txt     -- one-line throughput summary across the three
#
# Each scenario uses a fresh data dir (wiped at start) so segment-rotation
# pressure doesn't leak between runs.

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding           = [System.Text.Encoding]::UTF8

$ScriptDir   = $PSScriptRoot
$ProjectRoot = (Resolve-Path (Join-Path $ScriptDir '..')).Path
$TmpDir      = Join-Path $ScriptDir 'tmp'
New-Item -ItemType Directory -Force -Path $TmpDir | Out-Null

Push-Location $ProjectRoot
try {

$Timestamp = (Get-Date).ToString('yyyyMMdd-HHmmss')
$OutDir    = Join-Path $TmpDir "hotpath_$Timestamp"
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$Features = 'hotpath hotpath-alloc'

Write-Host "kafko hotpath measurement matrix"
Write-Host "  features : $Features"
Write-Host "  output   : $OutDir"
Write-Host ""

# --- Build once ---
Write-Host "Building kafko-bench --release --features `"$Features`" ..."
$buildArgs = @('build', '--release', '--package', 'kafko-bench', '--features', $Features)
& cargo @buildArgs
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: cargo build failed" -ForegroundColor Red
    exit 1
}

$BinaryRel = 'target\release\kafko-bench.exe'
if (-not (Test-Path $BinaryRel)) {
    $BinaryRel = 'target\release\kafko-bench'
}
if (-not (Test-Path $BinaryRel)) {
    Write-Host "ERROR: built binary not found at $BinaryRel" -ForegroundColor Red
    exit 1
}
$BinaryPath = (Resolve-Path $BinaryRel).Path

# --- Run each scenario in its own process ---
$Scenarios = @('sequential', 'concurrent', 'batch')
$summaryLines = @()

foreach ($scenario in $Scenarios) {
    $resultFile = Join-Path $OutDir "$scenario.txt"
    $dataDir    = Join-Path $OutDir "data_$scenario"

    Write-Host ""
    Write-Host "=== scenario: $scenario ==="
    Write-Host "  result -> $resultFile"

    $env:KAFKO_SCENARIO       = $scenario
    $env:KAFKO_BENCH_DATA_DIR = $dataDir
    $env:KAFKO_RESET          = '1'

    # Run the binary, capturing stdout + stderr. Hotpath prints its summary
    # table at process exit; capturing both streams catches it regardless of
    # which one hotpath writes to.
    $proc = Start-Process -FilePath $BinaryPath `
        -PassThru `
        -NoNewWindow `
        -RedirectStandardOutput $resultFile `
        -RedirectStandardError  "$resultFile.err"

    $proc.WaitForExit()
    if ($proc.ExitCode -ne 0) {
        Write-Host ("  scenario '$scenario' exited with code {0}" -f $proc.ExitCode) -ForegroundColor Yellow
    }

    # Merge stderr into the main result file -- hotpath writes to stderr.
    if (Test-Path "$resultFile.err") {
        Add-Content -Path $resultFile -Value "`n--- stderr ---`n"
        Get-Content "$resultFile.err" | Add-Content -Path $resultFile
        Remove-Item -Force "$resultFile.err"
    }

    # Pull the throughput line for the summary.
    $throughput = (Select-String -Path $resultFile -Pattern 'throughput' -SimpleMatch | Select-Object -First 1).Line
    if ($throughput) {
        $summaryLines += "$scenario : $($throughput.Trim())"
    } else {
        $summaryLines += "$scenario : (no throughput line found)"
    }

    # Tear down the data dir for this scenario so disk doesn't fill.
    if (Test-Path $dataDir) {
        Remove-Item -Recurse -Force $dataDir -ErrorAction SilentlyContinue
    }
}

# --- Summary ---
$SummaryFile = Join-Path $OutDir 'summary.txt'
Write-Host ""
Write-Host "=== summary ==="
foreach ($line in $summaryLines) {
    Write-Host "  $line"
}
$summaryLines | Set-Content -Path $SummaryFile -Encoding ascii

Write-Host ""
Write-Host "All scenario results saved under:"
Write-Host "  $OutDir"
Write-Host ""
Write-Host "Per-scenario per-function tables include:"
Write-Host "  - hotpath timing table (mean/median/p95/p99 per measured function)"
Write-Host "  - hotpath alloc table  (alloc count + total bytes per measured function)"
Write-Host ""
Write-Host "Key things to compare across scenarios:"
Write-Host "  Partition::append vs flush_append_batch -- delta = mpsc+oneshot overhead"
Write-Host "  Segment::append mean time              -- the write() syscall cost"
Write-Host "  Record::encode_with mean time          -- the codec+CRC cost"
Write-Host "  SparseIndex::track_append call count   -- index update pressure"

} finally {
    Pop-Location
}
