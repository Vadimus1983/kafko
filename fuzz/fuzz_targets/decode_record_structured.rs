#![no_main]

use arbitrary::Arbitrary;
use kafko::Record;
use libfuzzer_sys::fuzz_target;

// Hand-builds a wire-format record with a chosen compression flag and CHOSEN
// payload bytes (acting as if they were a compressed payload). Fixes up the
// CRC so Record::decode passes the integrity check and reaches the flag-parse
// and decompress paths. Random bytes via the unstructured target almost never
// pass the CRC, so the decompressor wrappers (lz4_flex, zstd) are not actually
// exercised there. This target finds:
//   - any flag value handling that panics instead of returning UnknownCompression
//   - any lz4/zstd input that makes the decompressor panic
//   - any key/value length combination that overflows or wraps
const KEY_NULL_SENTINEL: u32 = u32::MAX;

#[derive(Arbitrary, Debug)]
struct FuzzedRecord {
    flag: u8,
    timestamp_ms: i64,
    key: Option<Vec<u8>>,
    // Value bytes treated as a (possibly malformed) compressed payload when the
    // flag selects a compressed codec. Capped via the fuzzer's input length, not
    // here, so adversarial sizes are reachable.
    payload: Vec<u8>,
}

fuzz_target!(|input: FuzzedRecord| {
    let key_part_len: usize = match &input.key {
        None => 4,
        Some(k) => 4 + k.len(),
    };
    let val_part_len = 4 + input.payload.len();
    let payload_field_len = 1 + 8 + key_part_len + val_part_len;
    let total_len = 4 + payload_field_len;

    let mut buf = Vec::with_capacity(4 + total_len);
    buf.extend_from_slice(&(total_len as u32).to_be_bytes());

    // Reserve 4 bytes for the CRC; filled in after the payload is laid out.
    let crc_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]);

    let payload_start = buf.len();
    buf.push(input.flag);
    buf.extend_from_slice(&input.timestamp_ms.to_be_bytes());
    match &input.key {
        None => buf.extend_from_slice(&KEY_NULL_SENTINEL.to_be_bytes()),
        Some(k) => {
            buf.extend_from_slice(&(k.len() as u32).to_be_bytes());
            buf.extend_from_slice(k);
        }
    }
    buf.extend_from_slice(&(input.payload.len() as u32).to_be_bytes());
    buf.extend_from_slice(&input.payload);

    let crc = crc32fast::hash(&buf[payload_start..]);
    buf[crc_pos..crc_pos + 4].copy_from_slice(&crc.to_be_bytes());

    let mut slice: &[u8] = &buf;
    let _ = Record::decode(&mut slice);
});
