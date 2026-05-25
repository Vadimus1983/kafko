#![no_main]

use kafko::SparseIndex;
use libfuzzer_sys::fuzz_target;
use std::io::Write;
use tempfile::TempDir;
use tokio::runtime::Builder;

// Feeds arbitrary bytes as the contents of a sparse index file (`00...0.index`)
// and exercises both the parser (`SparseIndex::open`) and the lookup path on
// the parsed result. Neither must panic on adversarial input.
//
// The fuzzer's input is split: the first 8 bytes (if present) drive a
// post-parse lookup; the rest is the index file content. This way the same
// fuzz input exercises both the parser AND the lookup against whatever state
// the parser produced.
fuzz_target!(|data: &[u8]| {
    let (lookup_target, file_bytes) = if data.len() >= 8 {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&data[..8]);
        (u64::from_le_bytes(buf), &data[8..])
    } else {
        (0u64, data)
    };

    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    let dir = TempDir::new().unwrap();

    let index_path = dir.path().join("00000000000000000000.index");
    {
        let mut f = std::fs::File::create(&index_path).unwrap();
        let _ = f.write_all(file_bytes);
    }

    if let Ok(index) = rt.block_on(SparseIndex::open(dir.path(), 0, 4096)) {
        // The contract is "no panic", not "any specific return value." We
        // intentionally don't assert on the returned (pos, offset) because the
        // parsed entries are derived from fuzzer input.
        let _ = index.lookup(lookup_target);
        let _ = index.lookup(0);
        let _ = index.lookup(u64::MAX);
    }
});
