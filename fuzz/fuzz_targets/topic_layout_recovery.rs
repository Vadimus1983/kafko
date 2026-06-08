#![no_main]

use arbitrary::Arbitrary;
use kafko::{LogConfig, Topic};
use libfuzzer_sys::fuzz_target;
use std::io::Write;
use tempfile::TempDir;
use tokio::runtime::Builder;

// Feeds an arbitrary on-disk topic layout to the multi-partition recovery path
// (`Topic::open`): a set of partition subdirectories with Arbitrary names (so
// indices may have gaps, duplicates, or be non-contiguous), each holding a
// `00...0.log` of Arbitrary bytes, plus optional non-numeric junk. This composes
// partition discovery + contiguity validation with per-partition segment recovery
// over adversarial bytes.
//
// Contract: recovery must NEVER panic. It returns either Ok (indices were
// contiguous 0..n and every segment recovered) or an error
// (InvalidTopicLayout / IO / decode). When it returns Ok, every offset recovery
// vouches for must be readable.

const MAX_DIRS: usize = 8;
const MAX_SEGMENT_LEN: usize = 4096;

#[derive(Arbitrary, Debug)]
struct PartitionDir {
    index: u8,
    segment: Vec<u8>,
}

#[derive(Arbitrary, Debug)]
struct LayoutCase {
    dirs: Vec<PartitionDir>,
    junk_subdir: bool,
    stray_file: bool,
}

fuzz_target!(|case: LayoutCase| {
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    let dir = TempDir::new().unwrap();
    let topic_dir = dir.path().join("t");
    std::fs::create_dir_all(&topic_dir).unwrap();

    for pd in case.dirs.iter().take(MAX_DIRS) {
        let sub = topic_dir.join(pd.index.to_string());
        std::fs::create_dir_all(&sub).unwrap();
        let seg = sub.join("00000000000000000000.log");
        let mut f = std::fs::File::create(&seg).unwrap();
        let bytes = &pd.segment[..pd.segment.len().min(MAX_SEGMENT_LEN)];
        let _ = f.write_all(bytes);
    }
    if case.junk_subdir {
        let _ = std::fs::create_dir_all(topic_dir.join("not-a-number"));
    }
    if case.stray_file {
        let _ = std::fs::write(topic_dir.join("stray.txt"), b"junk");
    }

    let result = rt.block_on(Topic::open(&topic_dir, "t", LogConfig::default()));

    if let Ok(topic) = result {
        for p in 0..topic.partition_count() {
            let partition = topic.partition(p).expect("partition in range");
            let n = partition.high_water_mark();
            for offset in 0..n {
                let r = rt.block_on(partition.read_record_at(offset)).unwrap();
                assert!(
                    r.is_some(),
                    "recovered offset {offset} unreadable in partition {p}"
                );
            }
        }
        rt.block_on(topic.shutdown());
    }
});
