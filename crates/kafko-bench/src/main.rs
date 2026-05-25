// kafko-bench -- run kafko's append matrix in-process so the resulting profile
// shows storage + tokio task scheduling without HTTP/axum machinery dominating
// the flame graph.
//
// Workload mirrors the kafko-http samply bench: 6 record sizes x 3 codecs x
// CONCURRENCY tasks per cell, each task awaits its own Producer::send loop.
// The binary exits as soon as the matrix finishes, so a wrapping `samply record`
// observes child exit naturally and flushes the profile without any signal
// gymnastics.
//
// Environment variables:
//   KAFKO_BENCH_DATA_DIR  data dir for the broker (default ./kafko-bench_data)
//   KAFKO_RESET           if set, wipe the data dir at startup
//
// This binary is intentionally test-only (publish = false). It is NOT shipped
// to crates.io and is not part of the kafko library's public API.

use anyhow::Result;
use bytes::Bytes;
use kafko::{Compression, Kafko, LogConfig};
use std::sync::Arc;
use std::time::Instant;

const SIZES: &[usize] = &[64, 256, 1024, 4096, 131_072, 1_048_576];
const CODECS: &[(&str, Compression)] = &[
    ("bench_none", Compression::None),
    ("bench_lz4", Compression::Lz4),
    ("bench_zstd", Compression::Zstd),
];
const CONCURRENCY: usize = 16;

// Records per cell: tuned to match the kafko-http samply bench so profile
// shapes are comparable. Debug build is slow; numbers here are smaller than
// the release HTTP bench on purpose.
fn target_records(size: usize) -> u64 {
    if size >= 1_048_576 {
        200
    } else if size >= 131_072 {
        500
    } else if size >= 4_096 {
        5_000
    } else {
        50_000
    }
}

// hotpath's docs say to write #[tokio::main] above #[hotpath::main]. The
// cfg_attr makes hotpath::main appear only when the feature is on; without
// it the build is identical to a plain tokio::main async fn.
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
#[cfg_attr(feature = "hotpath", hotpath::main)]
async fn main() -> Result<()> {
    let data_dir = std::env::var("KAFKO_BENCH_DATA_DIR")
        .unwrap_or_else(|_| "./kafko-bench_data".to_string());

    if std::env::var("KAFKO_RESET").is_ok() {
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    let broker = Arc::new(Kafko::open(&data_dir).await?);

    for (name, codec) in CODECS {
        let cfg = LogConfig {
            compression: *codec,
            ..Default::default()
        };
        broker.create_topic_with_config(name, cfg).await?;
    }

    eprintln!(
        "kafko in-process bench: {} sizes x {} codecs x {} concurrency, no HTTP",
        SIZES.len(),
        CODECS.len(),
        CONCURRENCY,
    );
    eprintln!("data dir: {data_dir}");
    eprintln!();

    let bench_start = Instant::now();

    for (topic, _codec) in CODECS {
        eprintln!("============================================================");
        eprintln!(" TOPIC: {topic}");
        eprintln!("============================================================");

        for &size in SIZES {
            run_cell(&broker, topic, size).await?;
        }
    }

    let total_elapsed = bench_start.elapsed();
    eprintln!();
    eprintln!("=== DONE in {:.2}s ===", total_elapsed.as_secs_f64());

    // Explicit shutdown so the active segment is fsynced and the data-dir
    // lock is released before the process exits. Lets samply observe a clean
    // process tree termination.
    Arc::try_unwrap(broker)
        .map_err(|_| anyhow::anyhow!("broker still has outstanding clones at shutdown"))?
        .shutdown()
        .await?;

    // When the MCP server is enabled, keep the process alive after the bench
    // matrix completes so an LLM agent (or anyone with an MCP client) can
    // query the accumulated metrics. Hotpath's counters live in process
    // memory; shutting down kafko doesn't clear them, only process exit does.
    #[cfg(feature = "hotpath-mcp")]
    {
        eprintln!();
        eprintln!("------------------------------------------------------------");
        eprintln!(" Hotpath MCP server is running. Bench results above.");
        eprintln!(" Press Ctrl+C to exit when you're done querying.");
        eprintln!("------------------------------------------------------------");
        let _ = tokio::signal::ctrl_c().await;
        eprintln!("Exiting.");
    }

    Ok(())
}

async fn run_cell(broker: &Kafko, topic: &str, size: usize) -> Result<()> {
    let total = target_records(size);
    let per_task = (total / CONCURRENCY as u64).max(1);
    let total_actual = per_task * CONCURRENCY as u64;

    eprintln!();
    eprintln!(
        "=== size={size}B topic={topic} concurrency={CONCURRENCY} per_task={per_task} total={total_actual} ===",
    );

    let producer = broker.producer_for(topic).await?;
    let payload = Bytes::from(vec![0u8; size]);

    let start = Instant::now();
    let mut handles = Vec::with_capacity(CONCURRENCY);
    for _ in 0..CONCURRENCY {
        let p = producer.clone();
        let payload = payload.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..per_task {
                p.send(None, payload.clone()).await?;
            }
            Ok::<(), kafko::KafkoError>(())
        }));
    }
    for h in handles {
        h.await??;
    }
    let elapsed = start.elapsed();

    let rec_per_s = total_actual as f64 / elapsed.as_secs_f64();
    let mib_per_s =
        (total_actual * size as u64) as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0);
    eprintln!(
        "  {:.3}s  ({:.0} rec/s, {:.1} MiB/s)",
        elapsed.as_secs_f64(),
        rec_per_s,
        mib_per_s,
    );

    Ok(())
}
