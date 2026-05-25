#![no_main]

use kafko::Record;
use libfuzzer_sys::fuzz_target;

// Record::decode must never panic on arbitrary input — it is the trust
// boundary for everything that comes off disk during recovery. Errors
// (Truncated, CrcMismatch, InvalidLength, UnknownCompression,
// DecompressionFailed) are the contract; a panic is a bug.
fuzz_target!(|data: &[u8]| {
    let mut slice: &[u8] = data;
    let _ = Record::decode(&mut slice);
});
