// kafko-bench -- focused per-scenario harness for profiling kafko's hot path.
//
// Picks ONE scenario via the KAFKO_SCENARIO env var and runs only that, so
// hotpath counters reflect a single access pattern without cross-contamination.
// All scenarios use the same record size (256 B), no key, no compression --
// the baseline cell -- so per-function timing differences across scenarios
// isolate the access pattern, not the codec or payload.
//
// Scenarios:
//   sequential  -- 1 producer, N=100_000 single send() calls in a tight loop.
//                  Answers Gap 1: where does the ~3.7 us per-send go?
//                  Hotpath functions to compare: Partition::append (outer cost)
//                  vs flush_append_batch (writer-side cost). The delta is the
//                  mpsc-send + oneshot-wait overhead.
//
//   concurrent  -- 16 long-lived producer tasks, each doing 6_250 sends.
//                  Answers Gap 2: is natural-batching helping under contention?
//                  Aggregate throughput vs sequential answers "does parallelism
//                  help or hurt with one writer task as the serialization point?"
//                  Look at flush_append_batch's call count + mean per-call cost
//                  vs sequential's -- a higher mean per call = effective batching.
//
//   batch       -- 1 producer, K=97 calls of send_batch(1024). Reference ceiling.
//                  Saturates the writer task and amortizes mpsc cost across 1024
//                  records per round-trip. Hotpath should show append_batch
//                  dominating and Partition::append unused.
//
// Environment variables:
//   KAFKO_SCENARIO        sequential | concurrent | batch   (required)
//   KAFKO_BENCH_DATA_DIR  data dir for the broker             (default ./kafko-bench_data)
//   KAFKO_RESET           if set, wipe the data dir at startup
//
// This binary is intentionally test-only (publish = false).

use anyhow::{Result, anyhow};
use bytes::Bytes;
use kafko::{Compression, Kafko, LogConfig};
use std::sync::Arc;
use std::time::Instant;

const VALUE_SIZE: usize = 256;
const TOTAL_RECORDS_TARGET: u64 = 100_000;

const SEQUENTIAL_TOPIC: &str = "scenario_sequential";
const CONCURRENT_TOPIC: &str = "scenario_concurrent";
const BATCH_TOPIC: &str = "scenario_batch";

const CONCURRENT_TASKS: u64 = 16;
const BATCH_SIZE: u64 = 1024;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
#[cfg_attr(feature = "hotpath", hotpath::main)]
async fn main() -> Result<()> {
    let scenario = std::env::var("KAFKO_SCENARIO").unwrap_or_default();
    if scenario.is_empty() {
        print_usage();
        return Err(anyhow!("KAFKO_SCENARIO is required"));
    }

    let data_dir = std::env::var("KAFKO_BENCH_DATA_DIR")
        .unwrap_or_else(|_| "./kafko-bench_data".to_string());

    if std::env::var("KAFKO_RESET").is_ok() {
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    let broker = Arc::new(Kafko::open(&data_dir).await?);

    eprintln!("kafko-bench scenario={scenario}");
    eprintln!("  data dir : {data_dir}");
    eprintln!("  payload  : {VALUE_SIZE} B, no key, no compression");
    eprintln!("  target   : ~{TOTAL_RECORDS_TARGET} records");
    eprintln!();

    let elapsed = match scenario.as_str() {
        "sequential" => run_sequential(&broker).await?,
        "concurrent" => run_concurrent(&broker).await?,
        "batch" => run_batch(&broker).await?,
        other => {
            print_usage();
            return Err(anyhow!("unknown scenario '{other}'"));
        }
    };

    eprintln!();
    eprintln!("=== scenario '{scenario}' done in {:.3}s ===", elapsed.as_secs_f64());

    Arc::try_unwrap(broker)
        .map_err(|_| anyhow!("broker still has outstanding clones at shutdown"))?
        .shutdown()
        .await?;

    // When the MCP server is enabled, keep the process alive so an MCP client
    // can query the accumulated per-function counters. They live in process
    // memory; only process exit clears them.
    #[cfg(feature = "hotpath-mcp")]
    {
        eprintln!();
        eprintln!("------------------------------------------------------------");
        eprintln!(" Hotpath MCP server is running. Scenario complete.");
        eprintln!(" Press Ctrl+C to exit when you're done querying.");
        eprintln!("------------------------------------------------------------");
        let _ = tokio::signal::ctrl_c().await;
        eprintln!("Exiting.");
    }

    Ok(())
}

fn print_usage() {
    eprintln!("usage: KAFKO_SCENARIO=<scenario> kafko-bench");
    eprintln!();
    eprintln!("scenarios:");
    eprintln!("  sequential   1 producer, {TOTAL_RECORDS_TARGET} sequential send() calls");
    eprintln!("  concurrent   {CONCURRENT_TASKS} producers, ~{TOTAL_RECORDS_TARGET} sends total");
    eprintln!("  batch        1 producer, send_batch({BATCH_SIZE}) repeated to ~{TOTAL_RECORDS_TARGET} records");
}

async fn run_sequential(broker: &Kafko) -> Result<std::time::Duration> {
    broker
        .create_topic_with_config(SEQUENTIAL_TOPIC, default_cfg())
        .await?;
    let producer = broker.producer_for(SEQUENTIAL_TOPIC).await?;
    let payload = Bytes::from(vec![0u8; VALUE_SIZE]);

    let n = TOTAL_RECORDS_TARGET;
    eprintln!("running sequential: 1 task x {n} sends");

    let start = Instant::now();
    for _ in 0..n {
        producer.send(None, payload.clone()).await?;
    }
    let elapsed = start.elapsed();

    report("sequential", n, elapsed, None);
    Ok(elapsed)
}

async fn run_concurrent(broker: &Kafko) -> Result<std::time::Duration> {
    broker
        .create_topic_with_config(CONCURRENT_TOPIC, default_cfg())
        .await?;
    let producer = broker.producer_for(CONCURRENT_TOPIC).await?;
    let payload = Bytes::from(vec![0u8; VALUE_SIZE]);

    let per_task = TOTAL_RECORDS_TARGET / CONCURRENT_TASKS;
    let total = per_task * CONCURRENT_TASKS;
    eprintln!("running concurrent: {CONCURRENT_TASKS} tasks x {per_task} sends each (= {total} total)");

    let start = Instant::now();
    let mut handles = Vec::with_capacity(CONCURRENT_TASKS as usize);
    for _ in 0..CONCURRENT_TASKS {
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

    report("concurrent", total, elapsed, Some(CONCURRENT_TASKS));
    Ok(elapsed)
}

async fn run_batch(broker: &Kafko) -> Result<std::time::Duration> {
    broker
        .create_topic_with_config(BATCH_TOPIC, default_cfg())
        .await?;
    let producer = broker.producer_for(BATCH_TOPIC).await?;
    let payload = Bytes::from(vec![0u8; VALUE_SIZE]);

    let batches = TOTAL_RECORDS_TARGET.div_ceil(BATCH_SIZE);
    let total = batches * BATCH_SIZE;
    eprintln!("running batch: 1 task x {batches} batches of {BATCH_SIZE} (= {total} total)");

    let start = Instant::now();
    for _ in 0..batches {
        let items: Vec<(Option<Bytes>, Bytes)> = (0..BATCH_SIZE)
            .map(|_| (None, payload.clone()))
            .collect();
        producer.send_batch(items).await?;
    }
    let elapsed = start.elapsed();

    report("batch", total, elapsed, None);
    Ok(elapsed)
}

fn report(name: &str, total_records: u64, elapsed: std::time::Duration, tasks: Option<u64>) {
    let secs = elapsed.as_secs_f64();
    let rec_per_s = total_records as f64 / secs;
    let bytes = total_records * VALUE_SIZE as u64;
    let mib_per_s = bytes as f64 / secs / (1024.0 * 1024.0);
    eprintln!();
    eprintln!("scenario {name}:");
    eprintln!("  total records : {total_records}");
    eprintln!("  elapsed       : {secs:.3} s");
    eprintln!("  throughput    : {rec_per_s:.0} rec/s  ({mib_per_s:.1} MiB/s value bytes)");
    if let Some(n_tasks) = tasks {
        let per_task = rec_per_s / n_tasks as f64;
        eprintln!("  per task      : {per_task:.0} rec/s ({n_tasks} tasks)");
    }
}

fn default_cfg() -> LogConfig {
    LogConfig {
        compression: Compression::None,
        ..Default::default()
    }
}
