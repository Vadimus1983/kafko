# kafko_lib_multisize_bench.ps1 -- In-process kafko bench across a size matrix.
#
# Drives kafko-bench's `sequential` / `lz4_sequential` / `zstd_sequential`
# scenarios across a range of record sizes, so the README's
# "Library hot path -- records/sec (in-process, no HTTP)" multi-size table
# can be regenerated.
#
# Each (size, codec) cell runs in its own process with its own data dir, so
# no broker state leaks between cells and the disk-I/O cost of one cell
# doesn't pollute the next.
#
# Usage (PowerShell):
#   .\scripts\kafko_lib_multisize_bench.ps1
#
# Output:
#   scripts/tmp/kafko_lib_multisize_<ts>/
#     cell_<size>_<codec>.txt   per-cell raw kafko-bench output (stderr)
#     results.txt               human-readable results table (size x codec)
#     results.csv               machine-readable: size_bytes,codec,records,rec_per_s,mib_per_s
#
# Build features:
#   hotpath/hotpath-alloc are NOT enabled here -- this script measures clean
#   throughput. Use kafko_hotpath_matrix.ps1 for the alloc/timing tables.

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding           = [System.Text.Encoding]::UTF8

$ScriptDir   = $PSScriptRoot
$ProjectRoot = (Resolve-Path (Join-Path $ScriptDir '..')).Path
$TmpDir      = Join-Path $ScriptDir 'tmp'
New-Item -ItemType Directory -Force -Path $TmpDir | Out-Null

Push-Location $ProjectRoot
try {

$Timestamp = (Get-Date).ToString('yyyyMMdd-HHmmss')
$OutDir    = Join-Path $TmpDir "kafko_lib_multisize_$Timestamp"
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$Features = 'compression-all'

# --- Per-cell record counts ---
#
# Total bytes per cell is roughly constant (~50-200 MiB) so big-value runs
# don't dominate wall-clock time. Smaller sizes get more records to keep the
# measurement above the noise floor (~3.6 us per send).
$RecordsBySize = @{
    64       = 500000
    256      = 200000
    1024     = 100000
    4096     = 50000
    131072   = 10000
    1048576  = 1000
}

$Sizes  = 64, 256, 1024, 4096, 131072, 1048576
$Codecs = 'none', 'lz4', 'zstd'

# Map codec -> KAFKO_SCENARIO name expected by kafko-bench.
$ScenarioByCodec = @{
    'none' = 'sequential'
    'lz4'  = 'lz4_sequential'
    'zstd' = 'zstd_sequential'
}

Write-Host "kafko library multi-size bench"
Write-Host "  features : $Features"
Write-Host "  output   : $OutDir"
Write-Host "  sizes    : $($Sizes -join ', ')"
Write-Host "  codecs   : $($Codecs -join ', ')"
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

# Results: nested hashtable size -> codec -> @{rec_per_s, mib_per_s, records}
$Results = @{}
foreach ($size in $Sizes) {
    $Results[$size] = @{}
}

# --- Run each (size, codec) cell ---
$cellNum = 0
$totalCells = $Sizes.Count * $Codecs.Count
foreach ($size in $Sizes) {
    $records = $RecordsBySize[$size]
    foreach ($codec in $Codecs) {
        $cellNum++
        $scenario = $ScenarioByCodec[$codec]
        $cellFile = Join-Path $OutDir ("cell_{0}_{1}.txt" -f $size, $codec)
        $dataDir  = Join-Path $OutDir ("data_{0}_{1}" -f $size, $codec)

        Write-Host ("[{0}/{1}] size={2} codec={3} records={4}" -f $cellNum, $totalCells, $size, $codec, $records)

        $env:KAFKO_SCENARIO       = $scenario
        $env:KAFKO_VALUE_SIZE     = "$size"
        $env:KAFKO_TOTAL_RECORDS  = "$records"
        $env:KAFKO_BENCH_DATA_DIR = $dataDir
        $env:KAFKO_RESET          = '1'

        $proc = Start-Process -FilePath $BinaryPath `
            -PassThru `
            -NoNewWindow `
            -RedirectStandardOutput $cellFile `
            -RedirectStandardError  "$cellFile.err"
        $proc.WaitForExit()

        # Merge stderr into the main file (kafko-bench writes to stderr).
        if (Test-Path "$cellFile.err") {
            if (Test-Path $cellFile) {
                Add-Content -Path $cellFile -Value "`n--- stderr ---`n"
            }
            Get-Content "$cellFile.err" | Add-Content -Path $cellFile
            Remove-Item -Force "$cellFile.err"
        }

        # ExitCode from Start-Process can be $null on success on Windows --
        # don't gate on it. Always try to parse; warn if parse fails.
        $line = Select-String -Path $cellFile -Pattern 'throughput\s*:\s*([\d.]+)\s+rec/s\s+\(([\d.]+)\s+MiB/s' -AllMatches | Select-Object -First 1
        if ($line) {
            $m = $line.Matches[0]
            $rec_per_s = [double]$m.Groups[1].Value
            $mib_per_s = [double]$m.Groups[2].Value
            $Results[$size][$codec] = @{ rec_per_s = $rec_per_s; mib_per_s = $mib_per_s; records = $records }
            Write-Host ("    -> {0:N0} rec/s ({1:N1} MiB/s)" -f $rec_per_s, $mib_per_s)
        } else {
            Write-Host "    -> WARNING: no throughput line found in $cellFile" -ForegroundColor Yellow
            $Results[$size][$codec] = @{ rec_per_s = 0; mib_per_s = 0; records = $records }
        }

        # Tear down per-cell data dir so disk doesn't fill.
        if (Test-Path $dataDir) {
            Remove-Item -Recurse -Force $dataDir -ErrorAction SilentlyContinue
        }
    }
}

# --- Build results table ---
$ResultsFile = Join-Path $OutDir 'results.txt'
$CsvFile     = Join-Path $OutDir 'results.csv'

$lines = @()
$lines += "kafko library multi-size bench results"
$lines += ("Date:     " + (Get-Date).ToString('yyyy-MM-ddTHH:mm:ssZ'))
$lines += "Features: $Features"
$lines += "Build:    cargo build --release --package kafko-bench --features $Features"
$lines += ""
$lines += "=== records/sec ==="
$lines += "| Size | none | lz4 | zstd |"
$lines += "|---|---:|---:|---:|"
foreach ($size in $Sizes) {
    if     ($size -lt 1024)        { $sizeLabel = "$size B" }
    elseif ($size -lt 1048576)     { $sizeLabel = ("{0} KiB" -f ($size / 1024)) }
    else                           { $sizeLabel = ("{0} MiB" -f ($size / 1048576)) }
    $n = "{0:N0}" -f $Results[$size]['none'].rec_per_s
    $l = "{0:N0}" -f $Results[$size]['lz4'].rec_per_s
    $z = "{0:N0}" -f $Results[$size]['zstd'].rec_per_s
    $lines += ("| {0,-9} | {1,9} | {2,9} | {3,9} |" -f $sizeLabel, $n, $l, $z)
}
$lines += ""
$lines += "=== MiB/s value bytes ==="
$lines += "| Size | none | lz4 | zstd |"
$lines += "|---|---:|---:|---:|"
foreach ($size in $Sizes) {
    if     ($size -lt 1024)        { $sizeLabel = "$size B" }
    elseif ($size -lt 1048576)     { $sizeLabel = ("{0} KiB" -f ($size / 1024)) }
    else                           { $sizeLabel = ("{0} MiB" -f ($size / 1048576)) }
    $n = "{0:N1}" -f $Results[$size]['none'].mib_per_s
    $l = "{0:N1}" -f $Results[$size]['lz4'].mib_per_s
    $z = "{0:N1}" -f $Results[$size]['zstd'].mib_per_s
    $lines += ("| {0,-9} | {1,9} | {2,9} | {3,9} |" -f $sizeLabel, $n, $l, $z)
}

$lines | Set-Content -Path $ResultsFile -Encoding ascii

# CSV
$csvLines = @('size_bytes,codec,records,rec_per_s,mib_per_s')
foreach ($size in $Sizes) {
    foreach ($codec in $Codecs) {
        $r = $Results[$size][$codec]
        $csvLines += ("{0},{1},{2},{3},{4}" -f $size, $codec, $r.records, $r.rec_per_s, $r.mib_per_s)
    }
}
$csvLines | Set-Content -Path $CsvFile -Encoding ascii

Write-Host ""
Write-Host "=== summary ==="
Write-Host ("Results: $ResultsFile")
Write-Host ("CSV    : $CsvFile")
Write-Host ""
Get-Content $ResultsFile | Select-Object -Skip 4 | Write-Host

} finally {
    Pop-Location
    Remove-Item Env:KAFKO_SCENARIO       -ErrorAction SilentlyContinue
    Remove-Item Env:KAFKO_VALUE_SIZE     -ErrorAction SilentlyContinue
    Remove-Item Env:KAFKO_TOTAL_RECORDS  -ErrorAction SilentlyContinue
    Remove-Item Env:KAFKO_BENCH_DATA_DIR -ErrorAction SilentlyContinue
    Remove-Item Env:KAFKO_RESET          -ErrorAction SilentlyContinue
}
