#![no_main]

use kafko::Kafko;
use libfuzzer_sys::fuzz_target;
use tempfile::TempDir;
use tokio::runtime::Builder;

// Feeds arbitrary bytes as the contents of a consumer group's committed-offset
// file (`<topic>/offsets/<group>`) and runs the parse via the public
// `consumer_for_group` path, which lowers to `OffsetStore::decode`.
//
// Contract: parsing committed offsets must NEVER panic on adversarial bytes. A
// short / bad-CRC / truncated / oversized-count file degrades to "start from
// offset 0", never an error or a crash, and the resumed read position is always
// clamped to the partition's high-water-mark (0 here, since the topic is empty).
fuzz_target!(|data: &[u8]| {
    let dir = TempDir::new().unwrap();

    // Minimal on-disk topic "t" with one (empty) partition + the fuzzed offset
    // file. An empty partition subdir makes Log::open create a fresh segment, so
    // the broker opens cleanly and the only attacker-controlled input is the
    // offset file.
    let topic_dir = dir.path().join("t");
    std::fs::create_dir_all(topic_dir.join("0")).unwrap();
    std::fs::create_dir_all(topic_dir.join("offsets")).unwrap();
    std::fs::write(topic_dir.join("offsets").join("g"), data).unwrap();

    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    rt.block_on(async {
        let broker = match Kafko::open(dir.path()).await {
            Ok(b) => b,
            Err(_) => return,
        };
        // Triggers OffsetStore::open -> decode(data).
        if let Ok(consumer) = broker.consumer_for_group("t", "g").await {
            // Topic is empty (hwm 0), so the resumed cursor must clamp to 0
            // regardless of what the fuzzed file claimed.
            assert_eq!(consumer.position(0), 0, "cursor not clamped to high-water-mark");
        }
        let _ = broker.shutdown().await;
    });
});
