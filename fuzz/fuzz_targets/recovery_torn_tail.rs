#![no_main]

use kafko::{Log, LogConfig};
use libfuzzer_sys::fuzz_target;
use std::io::Write;
use tempfile::TempDir;
use tokio::runtime::Builder;

// Feeds arbitrary bytes as the contents of the active segment file (`00...0.log`)
// and runs the recovery path via `Log::open`. The contract: recovery
// CRC-scans the segment, finds the longest valid record prefix, truncates the
// rest, rebuilds the sparse index from the verified records, and returns a
// usable Log. Recovery must NEVER panic on adversarial bytes — only return an
// error or a successfully-truncated log.
fuzz_target!(|data: &[u8]| {
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    let dir = TempDir::new().unwrap();

    // Synthesize the segment file. The active segment file name format is
    // `00000000000000000000.log` (base offset zero, 20 digits zero-padded).
    let log_path = dir.path().join("00000000000000000000.log");
    {
        let mut f = std::fs::File::create(&log_path).unwrap();
        let _ = f.write_all(data);
    }

    let result = rt.block_on(Log::open(dir.path(), LogConfig::default()));

    if let Ok(mut log) = result {
        // If recovery succeeded, every offset in [0, next_offset) must be
        // readable without error. A successful recovery that produces an
        // unreadable record set would be a state-machine bug.
        let n = log.next_offset();
        for i in 0..n {
            let r = rt.block_on(log.read_record_at(i)).unwrap();
            assert!(r.is_some(), "acked offset {i} unreadable after recovery");
        }
    }
});
