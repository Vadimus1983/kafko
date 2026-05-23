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

    pub(crate) fn compress(self, raw: &[u8]) -> Vec<u8> {
        match self {
            Compression::None => raw.to_vec(),
            Compression::Lz4 => lz4_flex::compress_prepend_size(raw),
            Compression::Zstd => ZSTD_COMPRESSOR.with(|c| {
                let mut c = c.borrow_mut();
                if c.is_none() {
                    *c = Some(
                        zstd::bulk::Compressor::new(ZSTD_LEVEL)
                            .expect("zstd Compressor::new should not fail at level 3"),
                    );
                }
                c.as_mut()
                    .unwrap()
                    .compress(raw)
                    .expect("zstd compress should not fail on owned context")
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
