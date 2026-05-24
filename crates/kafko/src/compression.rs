use crate::error::{KafkoError, Result};
use std::cell::RefCell;

const ZSTD_LEVEL: i32 = 3;
// Upper bound on a single record's decompressed size. Records larger than this will
// fail decompression. 16 MiB is comfortably above any realistic per-record payload.
const ZSTD_DECOMPRESS_MAX_SIZE: usize = 16 * 1024 * 1024;

thread_local! {
    static ZSTD_COMPRESSOR: RefCell<Option<zstd::bulk::Compressor<'static>>> =
        const { RefCell::new(None) };
    static ZSTD_DECOMPRESSOR: RefCell<Option<zstd::bulk::Decompressor<'static>>> =
        const { RefCell::new(None) };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Compression {
    #[default]
    None,
    Lz4,
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

    /// Writes the compressed form of `raw` into `out`. `out` is cleared first; its capacity
    /// is reused across calls so a caller that keeps a long-lived buffer pays at most one
    /// allocation per call's peak size (and zero once the buffer has grown to that size).
    ///
    /// Wire format matches what `decompress` expects:
    /// - `None`: bytes copied verbatim
    /// - `Lz4`: 4-byte little-endian decompressed size, then the lz4 block. Equivalent to
    ///   `lz4_flex::compress_prepend_size` but writes through the caller's buffer.
    /// - `Zstd`: raw zstd frame (zstd carries its own size header internally).
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(crate) fn compress(self, raw: &[u8], out: &mut Vec<u8>) {
        out.clear();
        match self {
            Compression::None => out.extend_from_slice(raw),
            Compression::Lz4 => {
                let max_block_size = lz4_flex::block::get_maximum_output_size(raw.len());
                out.resize(4 + max_block_size, 0);
                out[..4].copy_from_slice(&(raw.len() as u32).to_le_bytes());
                let written = lz4_flex::compress_into(raw, &mut out[4..]).expect(
                    "lz4 compress_into should not fail with get_maximum_output_size-sized buffer",
                );
                out.truncate(4 + written);
            }
            Compression::Zstd => ZSTD_COMPRESSOR.with(|c| {
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
                    .unwrap()
                    .compress_to_buffer(raw, out)
                    .expect("zstd compress should not fail with compress_bound-sized buffer");
            }),
        }
    }

    pub(crate) fn decompress(self, compressed: &[u8]) -> Result<Vec<u8>> {
        match self {
            Compression::None => Ok(compressed.to_vec()),
            Compression::Lz4 => lz4_flex::decompress_size_prepended(compressed)
                .map_err(|_| KafkoError::DecompressionFailed),
            Compression::Zstd => ZSTD_DECOMPRESSOR.with(|d| {
                let mut d = d.borrow_mut();
                if d.is_none() {
                    *d = Some(
                        zstd::bulk::Decompressor::new()
                            .expect("zstd Decompressor::new should not fail"),
                    );
                }
                d.as_mut()
                    .unwrap()
                    .decompress(compressed, ZSTD_DECOMPRESS_MAX_SIZE)
                    .map_err(|_| KafkoError::DecompressionFailed)
            }),
        }
    }
}
