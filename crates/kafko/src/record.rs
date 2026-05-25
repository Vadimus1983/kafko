use crate::compression::Compression;
use crate::error::{KafkoError, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::cell::RefCell;

const KEY_NULL_SENTINEL: u32 = u32::MAX;
const MIN_TOTAL_LEN: usize = 4 + 1 + 8 + 4 + 4; // crc + flags + ts + key_len + val_len

// Per-thread scratch buffer for the compressed-value bytes. Lives at thread scope so the
// hot encode loop (one per partition writer task) reuses a single allocation across every
// record it encodes — the buffer grows to the peak compressed size seen on that thread and
// then stays there. Replaces the per-call Vec<u8> that Compression::compress used to return.
thread_local! {
    static ENCODE_COMPRESS_SCRATCH: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// A single log entry: timestamp, optional key, value bytes.
///
/// Keys and values are [`Bytes`] so the producer can pass owned or zero-copy
/// slices without an extra copy. Records returned by [`Consumer::next_record`]
/// have decompressed values; the on-wire form is an internal detail.
///
/// [`Consumer::next_record`]: crate::Consumer::next_record
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    timestamp_ms: i64,
    key: Option<Bytes>,
    value: Bytes,
}

impl Record {
    /// Constructs a record with the given timestamp (Unix epoch milliseconds),
    /// optional key, and value. [`Producer::send`] is the usual entry point;
    /// use this directly when you need to preserve a timestamp from upstream.
    ///
    /// [`Producer::send`]: crate::Producer::send
    pub fn new(timestamp_ms: i64, key: Option<Bytes>, value: Bytes) -> Self {
        Self {
            timestamp_ms,
            key,
            value,
        }
    }

    /// Returns the record's timestamp in Unix epoch milliseconds.
    pub fn timestamp_ms(&self) -> i64 {
        self.timestamp_ms
    }

    /// Returns the record's key, or `None` if it was produced without one.
    pub fn key(&self) -> Option<&Bytes> {
        self.key.as_ref()
    }

    /// Returns the record's value bytes (decompressed if the topic uses a codec).
    pub fn value(&self) -> &Bytes {
        &self.value
    }

    /// Upper-bound on-wire size assuming no compression. Used by `Log` to size buffers
    /// and check rotation thresholds. The actual encoded size (returned by `encode_with`)
    /// may be smaller when compression is applied.
    pub fn wire_size(&self) -> usize {
        let key_part = match &self.key {
            None => 4,
            Some(k) => 4 + k.len(),
        };
        4 + 4 + 1 + 8 + key_part + 4 + self.value.len()
    }

    /// Equivalent to `encode_with(out, Compression::None)`. Consumes `self`
    /// to enforce single-use. Returns the on-wire byte count appended to `out`.
    pub fn encode(self, out: &mut BytesMut) -> usize {
        self.encode_with(out, Compression::None)
    }

    /// Consumes `self` to enforce single-use. Compresses the value if `compression` is non-`None`.
    /// Returns the actual on-wire byte count appended to `out`.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn encode_with(self, out: &mut BytesMut, compression: Compression) -> usize {
        // The Compression::None branch is zero-copy: encode_inner reads val bytes straight
        // from self.value. The compressed branches route the value through the per-thread
        // scratch buffer so encode_inner sees an already-compressed &[u8] without ever
        // materializing a fresh Vec<u8> on the hot path.
        match compression {
            Compression::None => self.encode_inner(out, compression, None),
            Compression::Lz4 | Compression::Zstd => ENCODE_COMPRESS_SCRATCH.with(|s| {
                let mut scratch = s.borrow_mut();
                compression.compress(&self.value, &mut scratch);
                self.encode_inner(out, compression, Some(&scratch[..]))
            }),
        }
    }

    fn encode_inner(
        self,
        out: &mut BytesMut,
        compression: Compression,
        val_bytes_override: Option<&[u8]>,
    ) -> usize {
        // Wire layout written to `out` (all integers big-endian):
        //
        //   ┌─────────┬─────────┬───────┬─────────┬───────────┬──────────┬───────────┬──────────┐
        //   │   len   │  crc32  │ flags │  ts_ms  │  key_len  │   key    │  val_len  │   val    │
        //   │   u32   │   u32   │   u8  │   i64   │    u32    │  bytes   │    u32    │  bytes   │
        //   └─────────┴─────────┴───────┴─────────┴───────────┴──────────┴───────────┴──────────┘
        //
        //   len     — byte count of everything following this field (crc + payload)
        //   crc32   — checksum over [flags .. val] (everything after this field itself)
        //   flags   — bit 0-1: compression (0=none, 1=lz4); other bits reserved
        //   key_len — u32::MAX sentinel means null key (no key bytes follow)
        //   val     — bytes (compressed iff flags indicates so)

        let start_len = out.len();
        let flag = compression.flag();

        let val_bytes: &[u8] = val_bytes_override.unwrap_or(&self.value);

        let key_part = match &self.key {
            None => 4,
            Some(k) => 4 + k.len(),
        };
        let val_part = 4 + val_bytes.len();
        let payload_len = 1 + 8 + key_part + val_part;
        let total_len = 4 + payload_len;

        out.reserve(4 + total_len);
        out.put_u32(total_len as u32);

        let crc_pos = out.len();
        out.put_u32(0);

        let payload_start = out.len();
        out.put_u8(flag);
        out.put_i64(self.timestamp_ms);
        match &self.key {
            None => out.put_u32(KEY_NULL_SENTINEL),
            Some(k) => {
                out.put_u32(k.len() as u32);
                out.put_slice(k);
            }
        }
        out.put_u32(val_bytes.len() as u32);
        out.put_slice(val_bytes);

        let crc = crc32fast::hash(&out[payload_start..]);
        out[crc_pos..crc_pos + 4].copy_from_slice(&crc.to_be_bytes());

        out.len() - start_len
    }

    /// Decodes one record off the front of `buf`, advancing the slice past the
    /// consumed bytes. Returns [`KafkoError::Truncated`] if `buf` is short,
    /// [`KafkoError::CrcMismatch`] on corruption, [`KafkoError::InvalidLength`]
    /// on a malformed header, or [`KafkoError::UnknownCompression`] /
    /// [`KafkoError::DecompressionFailed`] on a codec issue.
    pub fn decode(buf: &mut &[u8]) -> Result<Self> {
        if buf.remaining() < 4 {
            return Err(KafkoError::Truncated {
                needed: 4 - buf.remaining(),
            });
        }
        let total_len = buf.get_u32() as usize;

        if total_len < MIN_TOTAL_LEN {
            return Err(KafkoError::InvalidLength(total_len as u32));
        }
        if buf.remaining() < total_len {
            return Err(KafkoError::Truncated {
                needed: total_len - buf.remaining(),
            });
        }

        let expected_crc = buf.get_u32();
        let payload_len = total_len - 4;
        let actual_crc = crc32fast::hash(&buf[..payload_len]);
        if actual_crc != expected_crc {
            return Err(KafkoError::CrcMismatch {
                expected: expected_crc,
                actual: actual_crc,
            });
        }

        let flag = buf.get_u8();
        let compression = Compression::from_flag(flag)?;

        let timestamp_ms = buf.get_i64();

        let key_len_raw = buf.get_u32();
        let key = if key_len_raw == KEY_NULL_SENTINEL {
            None
        } else {
            let key_len = key_len_raw as usize;
            if buf.remaining() < key_len {
                return Err(KafkoError::InvalidLength(total_len as u32));
            }
            let bytes = Bytes::copy_from_slice(&buf[..key_len]);
            buf.advance(key_len);
            Some(bytes)
        };

        if buf.remaining() < 4 {
            return Err(KafkoError::InvalidLength(total_len as u32));
        }
        let val_len = buf.get_u32() as usize;
        if buf.remaining() < val_len {
            return Err(KafkoError::InvalidLength(total_len as u32));
        }
        let raw_val: &[u8] = &buf[..val_len];
        let value = match compression {
            Compression::None => Bytes::copy_from_slice(raw_val),
            Compression::Lz4 | Compression::Zstd => Bytes::from(compression.decompress(raw_val)?),
        };
        buf.advance(val_len);

        Ok(Self {
            timestamp_ms,
            key,
            value,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Record {
        Record::new(
            1_700_000_000_000,
            Some(Bytes::from_static(b"key")),
            Bytes::from_static(b"value"),
        )
    }

    #[test]
    fn getters_return_constructor_inputs() {
        let r = sample();
        assert_eq!(r.timestamp_ms(), 1_700_000_000_000);
        assert_eq!(r.key(), Some(&Bytes::from_static(b"key")));
        assert_eq!(r.value(), &Bytes::from_static(b"value"));
    }

    #[test]
    fn roundtrip_with_key() {
        let r = sample();
        let expected = r.clone();
        let expected_size = r.wire_size();

        let mut buf = BytesMut::new();
        let written = r.encode(&mut buf);
        assert_eq!(buf.len(), expected_size);
        assert_eq!(written, expected_size);

        let mut slice: &[u8] = &buf;
        let decoded = Record::decode(&mut slice).unwrap();
        assert_eq!(decoded, expected);
        assert!(slice.is_empty());
    }

    #[test]
    fn roundtrip_null_key() {
        let r = Record::new(0, None, Bytes::from_static(b"v"));
        let expected = r.clone();
        let mut buf = BytesMut::new();
        r.encode(&mut buf);
        let mut slice: &[u8] = &buf;
        let decoded = Record::decode(&mut slice).unwrap();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn empty_key_is_distinct_from_null_key() {
        let with_empty = Record::new(0, Some(Bytes::new()), Bytes::from_static(b"v"));
        let with_null = Record::new(0, None, Bytes::from_static(b"v"));

        let mut buf_a = BytesMut::new();
        let mut buf_b = BytesMut::new();
        with_empty.clone().encode(&mut buf_a);
        with_null.clone().encode(&mut buf_b);
        assert_ne!(buf_a, buf_b);

        let mut slice_a: &[u8] = &buf_a;
        let mut slice_b: &[u8] = &buf_b;
        assert_eq!(Record::decode(&mut slice_a).unwrap(), with_empty);
        assert_eq!(Record::decode(&mut slice_b).unwrap(), with_null);
    }

    #[test]
    fn crc_tampering_detected() {
        let r = sample();
        let mut buf = BytesMut::new();
        r.encode(&mut buf);
        // flip a byte after [len][crc] in the payload
        buf[12] ^= 0xFF;
        let mut slice: &[u8] = &buf;
        match Record::decode(&mut slice) {
            Err(KafkoError::CrcMismatch { .. }) => {}
            other => panic!("expected CrcMismatch, got {:?}", other),
        }
    }

    #[test]
    fn truncated_input_detected() {
        let r = sample();
        let mut buf = BytesMut::new();
        r.encode(&mut buf);
        let truncated = &buf[..buf.len() - 1];
        let mut slice: &[u8] = truncated;
        match Record::decode(&mut slice) {
            Err(KafkoError::Truncated { .. }) => {}
            other => panic!("expected Truncated, got {:?}", other),
        }
    }

    #[test]
    fn empty_input_truncated() {
        let mut slice: &[u8] = &[];
        match Record::decode(&mut slice) {
            Err(KafkoError::Truncated { .. }) => {}
            other => panic!("expected Truncated, got {:?}", other),
        }
    }

    #[test]
    fn encode_consumes_self() {
        // sample() = key 3 bytes + value 5 bytes. Overhead now 25 (was 24 pre-flags).
        let r = sample();
        let mut buf = BytesMut::new();
        r.encode(&mut buf);
        // r.encode(&mut buf);  // <-- would not compile: use of moved value
        assert_eq!(buf.len(), 25 + 3 + 5);
    }

    #[test]
    fn lz4_compression_roundtrip() {
        // A highly compressible value (lots of repeats) should round-trip identically.
        let payload = Bytes::from(vec![0xABu8; 1024]);
        let r = Record::new(1_700_000_000_000, Some(Bytes::from_static(b"k")), payload.clone());

        let mut buf = BytesMut::new();
        let actual_size = r.encode_with(&mut buf, Compression::Lz4);

        // Compressed size should be much smaller than uncompressed
        assert!(
            actual_size < 1024,
            "expected compressed output to be smaller than 1024 bytes, got {}",
            actual_size
        );

        let mut slice: &[u8] = &buf;
        let decoded = Record::decode(&mut slice).unwrap();
        assert_eq!(decoded.value(), &payload);
        assert!(slice.is_empty());
    }

    #[test]
    fn zstd_compression_roundtrip() {
        let payload = Bytes::from(vec![0xABu8; 1024]);
        let r = Record::new(1_700_000_000_000, Some(Bytes::from_static(b"k")), payload.clone());

        let mut buf = BytesMut::new();
        let actual_size = r.encode_with(&mut buf, Compression::Zstd);

        assert!(
            actual_size < 1024,
            "expected zstd-compressed output to be smaller than 1024 bytes, got {}",
            actual_size
        );

        let mut slice: &[u8] = &buf;
        let decoded = Record::decode(&mut slice).unwrap();
        assert_eq!(decoded.value(), &payload);
        assert!(slice.is_empty());
    }

    #[test]
    fn lz4_record_decodes_without_caller_knowing_compression() {
        // Decode is compression-agnostic — it reads the flag from the wire.
        let payload = Bytes::from(vec![0xFFu8; 2048]);
        let r = Record::new(42, None, payload.clone());

        let mut buf = BytesMut::new();
        r.encode_with(&mut buf, Compression::Lz4);

        let mut slice: &[u8] = &buf;
        let decoded = Record::decode(&mut slice).unwrap();
        assert_eq!(decoded.value(), &payload);
    }

    #[test]
    fn decode_rejects_lz4_record_with_oversized_decompressed_size_claim() {
        // End-to-end fuzz regression: a CRC-valid record whose LZ4 value bytes
        // claim a ~4 GiB decompressed size must fail with DecompressionFailed,
        // not OOM the process. Originally found by `decode_record_structured`
        // fuzz target on input `oom-25f853ff8087d2ab14b530825448b7be1d5f045d`.
        const KEY_NULL_SENTINEL_LOCAL: u32 = u32::MAX;
        let hostile_lz4_payload: [u8; 5] = [0x55, 0xFF, 0xFF, 0xFF, 0x00];

        let payload_field_len = 1 + 8 + 4 + (4 + hostile_lz4_payload.len());
        let total_len = 4 + payload_field_len;

        let mut wire = Vec::with_capacity(4 + total_len);
        wire.extend_from_slice(&(total_len as u32).to_be_bytes());
        let crc_pos = wire.len();
        wire.extend_from_slice(&[0u8; 4]);
        let payload_start = wire.len();
        wire.push(Compression::Lz4.flag());
        wire.extend_from_slice(&0i64.to_be_bytes());
        wire.extend_from_slice(&KEY_NULL_SENTINEL_LOCAL.to_be_bytes());
        wire.extend_from_slice(&(hostile_lz4_payload.len() as u32).to_be_bytes());
        wire.extend_from_slice(&hostile_lz4_payload);
        let crc = crc32fast::hash(&wire[payload_start..]);
        wire[crc_pos..crc_pos + 4].copy_from_slice(&crc.to_be_bytes());

        let mut slice: &[u8] = &wire;
        match Record::decode(&mut slice) {
            Err(KafkoError::DecompressionFailed) => {}
            other => panic!("expected DecompressionFailed, got {:?}", other),
        }
    }
}
