use crate::error::{KafkoError, Result};
#[cfg(any(feature = "compression-lz4", feature = "compression-zstd"))]
use std::cell::RefCell;

#[cfg(feature = "compression-zstd")]
const ZSTD_LEVEL: i32 = 3;

// Upper bound on a single record's decompressed size, applied uniformly to both
// LZ4 and Zstd. 16 MiB is comfortably above any realistic per-record payload.
//
// This bound is load-bearing for safety, not just convenience: lz4_flex's
// `decompress_size_prepended` reads the 4-byte LE size prefix and calls
// `Vec::with_capacity(claimed)` *before* validating the compressed bytes. An
// attacker (or an unfortunate torn-tail recovery producing a CRC-valid garbage
// record) can claim ~4 GiB in a tiny payload and OOM the process. We cap the
// claimed size here before delegating. Zstd's bulk Decompressor::decompress
// already takes a max-output-size argument; LZ4 does not, so we enforce it
// ourselves. Discovered by the `decode_record_structured` fuzz target.
#[allow(dead_code)]
const DECOMPRESS_MAX_SIZE: usize = 16 * 1024 * 1024;

#[cfg(feature = "compression-zstd")]
thread_local! {
    static ZSTD_COMPRESSOR: RefCell<Option<zstd::bulk::Compressor<'static>>> =
        const { RefCell::new(None) };
    static ZSTD_DECOMPRESSOR: RefCell<Option<zstd::bulk::Decompressor<'static>>> =
        const { RefCell::new(None) };
}

// Per-thread reusable LZ4 hash table. lz4_flex's default `compress_into` path
// allocates a fresh 8 KiB hash table on every call. `compress_into_with_table`
// (lz4_flex 0.13+) clears a caller-owned table in place instead, so keeping one
// table per encoder thread reduces the per-call alloc to zero after the first
// record. The table self-upgrades Small -> Large the first time it sees an
// input >= 64 KiB (one-time cost per thread, not per record).
#[cfg(feature = "compression-lz4")]
thread_local! {
    static LZ4_TABLE: RefCell<lz4_flex::block::CompressTable> =
        RefCell::new(lz4_flex::block::CompressTable::default());
}

/// Per-topic value compression.
///
/// Set at topic creation via [`LogConfig::compression`]. The compression flag
/// is encoded in each record's header, so a single topic can be decoded
/// correctly even if the broker's configured codec changes between runs.
///
/// All variants are visible regardless of which Cargo features are enabled, so
/// a build without `compression-lz4` / `compression-zstd` can still parse
/// on-disk records written by another build and surface a friendly
/// [`KafkoError::CompressionUnavailable`] instead of crashing.
///
/// See the README "Codec note" for the per-call allocation behaviour of LZ4
/// vs zstd.
///
/// [`LogConfig::compression`]: crate::LogConfig::compression
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Compression {
    /// No compression — record payloads are written verbatim. Always available.
    #[default]
    None,
    /// LZ4 block compression via [`lz4_flex`]. Requires the
    /// `compression-lz4` Cargo feature; otherwise calls return
    /// [`KafkoError::CompressionUnavailable`].
    Lz4,
    /// Zstandard compression at level 3 via the [`zstd`] crate. Requires the
    /// `compression-zstd` Cargo feature; otherwise calls return
    /// [`KafkoError::CompressionUnavailable`].
    Zstd,
}

impl Compression {
    pub(crate) fn flag(self) -> u8 {
        match self {
            Compression::None => 0,
            Compression::Lz4 => 1,
            Compression::Zstd => 2,
        }
    }

    pub(crate) fn from_flag(flag: u8) -> Result<Self> {
        match flag {
            0 => Ok(Compression::None),
            1 => Ok(Compression::Lz4),
            2 => Ok(Compression::Zstd),
            other => Err(KafkoError::UnknownCompression(other)),
        }
    }

    /// Returns `true` iff this build of kafko was compiled with the Cargo
    /// feature backing this variant. `Compression::None` is always available;
    /// `Lz4` requires `compression-lz4` and `Zstd` requires `compression-zstd`.
    ///
    /// Useful for callers that want to pick a codec at runtime based on what
    /// the binary can actually handle.
    pub fn is_available(self) -> bool {
        match self {
            Compression::None => true,
            Compression::Lz4 => cfg!(feature = "compression-lz4"),
            Compression::Zstd => cfg!(feature = "compression-zstd"),
        }
    }

    /// Writes the compressed form of `raw` into `out`. `out` is cleared first; its capacity
    /// is reused across calls so a caller that keeps a long-lived buffer pays at most one
    /// allocation per call's peak size (and zero once the buffer has grown to that size).
    ///
    /// Wire format matches what `decompress` expects:
    /// - `None`: bytes copied verbatim
    /// - `Lz4`: 4-byte little-endian decompressed size, then the lz4 block. Equivalent to
    ///   `lz4_flex::compress_prepend_size` but writes through the caller's buffer.
    /// - `Zstd`: raw zstd frame (zstd carries its own size header internally).
    ///
    /// Returns [`KafkoError::CompressionUnavailable`] when called with a codec
    /// whose Cargo feature is not enabled in this build.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(crate) fn compress(self, raw: &[u8], out: &mut Vec<u8>) -> Result<()> {
        out.clear();
        match self {
            Compression::None => {
                out.extend_from_slice(raw);
                Ok(())
            }
            Compression::Lz4 => lz4_compress(raw, out),
            Compression::Zstd => zstd_compress(raw, out),
        }
    }

    pub(crate) fn decompress(self, compressed: &[u8]) -> Result<Vec<u8>> {
        match self {
            Compression::None => Ok(compressed.to_vec()),
            Compression::Lz4 => lz4_decompress(compressed),
            Compression::Zstd => zstd_decompress(compressed),
        }
    }
}

#[cfg(feature = "compression-lz4")]
fn lz4_compress(raw: &[u8], out: &mut Vec<u8>) -> Result<()> {
    let max_block_size = lz4_flex::block::get_maximum_output_size(raw.len());
    out.resize(4 + max_block_size, 0);
    out[..4].copy_from_slice(&(raw.len() as u32).to_le_bytes());
    let written = LZ4_TABLE
        .with_borrow_mut(|table| {
            lz4_flex::block::compress_into_with_table(raw, &mut out[4..], table)
        })
        .expect(
            "lz4 compress_into_with_table should not fail with get_maximum_output_size-sized buffer",
        );
    out.truncate(4 + written);
    Ok(())
}

#[cfg(not(feature = "compression-lz4"))]
fn lz4_compress(_raw: &[u8], _out: &mut Vec<u8>) -> Result<()> {
    Err(KafkoError::CompressionUnavailable(Compression::Lz4))
}

#[cfg(feature = "compression-zstd")]
fn zstd_compress(raw: &[u8], out: &mut Vec<u8>) -> Result<()> {
    ZSTD_COMPRESSOR.with(|c| {
        let mut c = c.borrow_mut();
        if c.is_none() {
            *c = Some(
                zstd::bulk::Compressor::new(ZSTD_LEVEL)
                    .expect("zstd Compressor::new should not fail at level 3"),
            );
        }
        // compress_to_buffer requires the Vec to have enough spare capacity for the
        // worst-case compressed size; it does NOT grow the Vec on demand. Reserve
        // explicitly so the first call on a fresh thread-local sizes the buffer
        // correctly and subsequent calls below that high-water mark are zero-alloc.
        let bound = zstd::zstd_safe::compress_bound(raw.len());
        out.reserve(bound);
        c.as_mut()
            .expect("compressor initialized by the is_none branch above")
            .compress_to_buffer(raw, out)
            .expect("zstd compress should not fail with compress_bound-sized buffer");
        Ok(())
    })
}

#[cfg(not(feature = "compression-zstd"))]
fn zstd_compress(_raw: &[u8], _out: &mut Vec<u8>) -> Result<()> {
    Err(KafkoError::CompressionUnavailable(Compression::Zstd))
}

#[cfg(feature = "compression-lz4")]
fn lz4_decompress(compressed: &[u8]) -> Result<Vec<u8>> {
    if compressed.len() < 4 {
        return Err(KafkoError::DecompressionFailed);
    }
    let claimed_size = u32::from_le_bytes([
        compressed[0],
        compressed[1],
        compressed[2],
        compressed[3],
    ]) as usize;
    if claimed_size > DECOMPRESS_MAX_SIZE {
        return Err(KafkoError::DecompressionFailed);
    }
    lz4_flex::decompress_size_prepended(compressed).map_err(|_| KafkoError::DecompressionFailed)
}

#[cfg(not(feature = "compression-lz4"))]
fn lz4_decompress(_compressed: &[u8]) -> Result<Vec<u8>> {
    Err(KafkoError::CompressionUnavailable(Compression::Lz4))
}

#[cfg(feature = "compression-zstd")]
fn zstd_decompress(compressed: &[u8]) -> Result<Vec<u8>> {
    ZSTD_DECOMPRESSOR.with(|d| {
        let mut d = d.borrow_mut();
        if d.is_none() {
            *d = Some(
                zstd::bulk::Decompressor::new().expect("zstd Decompressor::new should not fail"),
            );
        }
        d.as_mut()
            .expect("decompressor initialized by the is_none branch above")
            .decompress(compressed, DECOMPRESS_MAX_SIZE)
            .map_err(|_| KafkoError::DecompressionFailed)
    })
}

#[cfg(not(feature = "compression-zstd"))]
fn zstd_decompress(_compressed: &[u8]) -> Result<Vec<u8>> {
    Err(KafkoError::CompressionUnavailable(Compression::Zstd))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "compression-lz4")]
    #[test]
    fn lz4_decompress_rejects_oversized_size_prefix() {
        // Crafted payload that claims ~4 GiB decompressed size in its 4-byte LE
        // prefix. Without the guard, lz4_flex would Vec::with_capacity that much
        // and OOM. With the guard we return DecompressionFailed before the
        // allocation. Found by the `decode_record_structured` fuzz target on
        // input `oom-25f853ff8087d2ab14b530825448b7be1d5f045d`.
        let payload = [0x55u8, 0xFF, 0xFF, 0xFF, 0x00];
        match Compression::Lz4.decompress(&payload) {
            Err(KafkoError::DecompressionFailed) => {}
            other => panic!("expected DecompressionFailed, got {:?}", other),
        }
    }

    #[cfg(feature = "compression-lz4")]
    #[test]
    fn lz4_decompress_rejects_payload_shorter_than_size_prefix() {
        for len in 0..4 {
            let buf = vec![0u8; len];
            match Compression::Lz4.decompress(&buf) {
                Err(KafkoError::DecompressionFailed) => {}
                other => panic!("len={len}: expected DecompressionFailed, got {:?}", other),
            }
        }
    }

    #[cfg(feature = "compression-lz4")]
    #[test]
    fn lz4_decompress_accepts_normally_sized_payload() {
        // Round-trip a moderate payload to confirm the guard doesn't reject
        // legitimate inputs whose claimed size sits below the cap.
        let raw = vec![0xABu8; 4096];
        let mut compressed = Vec::new();
        Compression::Lz4.compress(&raw, &mut compressed).unwrap();
        let decompressed = Compression::Lz4.decompress(&compressed).unwrap();
        assert_eq!(decompressed, raw);
    }

    #[cfg(feature = "compression-zstd")]
    #[test]
    fn zstd_decompress_rejects_oversized_claim() {
        // Zstd's wire format carries the uncompressed size internally; the bulk
        // Decompressor::decompress call already takes a max-output-size argument
        // (DECOMPRESS_MAX_SIZE). A frame claiming a larger output must fail with
        // DecompressionFailed rather than allocating beyond the cap.
        let raw = vec![0u8; DECOMPRESS_MAX_SIZE + 1];
        // Compress with a fresh Compressor (not the thread-local; we want a
        // legitimately-encoded frame that claims more bytes than our cap).
        let mut c = zstd::bulk::Compressor::new(ZSTD_LEVEL).unwrap();
        let frame = c.compress(&raw).unwrap();
        match Compression::Zstd.decompress(&frame) {
            Err(KafkoError::DecompressionFailed) => {}
            other => panic!("expected DecompressionFailed, got {:?}", other),
        }
    }

    #[cfg(not(feature = "compression-lz4"))]
    #[test]
    fn lz4_calls_return_unavailable_when_feature_off() {
        let mut out = Vec::new();
        match Compression::Lz4.compress(b"hello", &mut out) {
            Err(KafkoError::CompressionUnavailable(Compression::Lz4)) => {}
            other => panic!("expected CompressionUnavailable(Lz4), got {:?}", other),
        }
        match Compression::Lz4.decompress(b"\x00\x00\x00\x00") {
            Err(KafkoError::CompressionUnavailable(Compression::Lz4)) => {}
            other => panic!("expected CompressionUnavailable(Lz4), got {:?}", other),
        }
    }

    #[cfg(not(feature = "compression-zstd"))]
    #[test]
    fn zstd_calls_return_unavailable_when_feature_off() {
        let mut out = Vec::new();
        match Compression::Zstd.compress(b"hello", &mut out) {
            Err(KafkoError::CompressionUnavailable(Compression::Zstd)) => {}
            other => panic!("expected CompressionUnavailable(Zstd), got {:?}", other),
        }
        match Compression::Zstd.decompress(b"\x00\x00\x00\x00") {
            Err(KafkoError::CompressionUnavailable(Compression::Zstd)) => {}
            other => panic!("expected CompressionUnavailable(Zstd), got {:?}", other),
        }
    }

    #[test]
    fn is_available_reports_truth_about_this_build() {
        assert!(Compression::None.is_available());
        assert_eq!(
            Compression::Lz4.is_available(),
            cfg!(feature = "compression-lz4")
        );
        assert_eq!(
            Compression::Zstd.is_available(),
            cfg!(feature = "compression-zstd")
        );
    }
}
