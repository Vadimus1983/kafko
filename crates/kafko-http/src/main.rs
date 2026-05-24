// kafko-http -- minimal HTTP server exposing kafko partitions over POST /produce/:codec.
//
// Endpoints:
//   POST /produce/none    body = raw bytes  ->  200, body = assigned offset (string)
//   POST /produce/lz4     body = raw bytes  ->  200, body = assigned offset (string)
//   POST /produce/zstd    body = raw bytes  ->  200, body = assigned offset (string)
//   GET  /hwm                                ->  200, body = current high-water-mark
//
// Each /produce/:codec endpoint writes to a different topic configured with
// its compression mode (none / lz4 / zstd). This lets a load tester compare
// the three compression codecs through the same HTTP stack and the same
// kafko runtime.
//
// Environment variables:
//   KAFKO_BIND        address:port to bind   (default 127.0.0.1:9091)
//   KAFKO_DATA_DIR    kafko broker data dir  (default ./kafko-http_data)
//   KAFKO_RESET       if set, wipes the data dir at startup
//
// NOTE: kafko v0.2 does not persist per-topic LogConfig across restarts. The
// compression mode is only applied at topic creation. Use KAFKO_RESET=1 for
// clean bench runs to guarantee the compression mode is what was requested.

use anyhow::Result;
use axum::{
    Router,
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
};
use kafko::{Compression, Kafko, LogConfig, Partition, Producer};
use std::sync::Arc;

#[derive(Clone)]
struct AppState {
    producer_none: Producer,
    producer_lz4: Producer,
    producer_zstd: Producer,
    partition_none: Arc<Partition>,
}

async fn produce_handler(
    State(state): State<AppState>,
    Path(codec): Path<String>,
    body: Bytes,
) -> Result<String, (StatusCode, String)> {
    let producer = match codec.as_str() {
        "none" => &state.producer_none,
        "lz4" => &state.producer_lz4,
        "zstd" => &state.producer_zstd,
        other => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("unknown codec: '{other}'. Expected 'none', 'lz4', or 'zstd'"),
            ));
        }
    };
    producer
        .send(None, body)
        .await
        .map(|offset| offset.to_string())
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

async fn hwm_handler(State(state): State<AppState>) -> String {
    state.partition_none.high_water_mark().to_string()
}

async fn ensure_topic(broker: &Kafko, name: &str, compression: Compression) -> Result<()> {
    if !broker.has_topic(name).await {
        let cfg = LogConfig {
            compression,
            ..Default::default()
        };
        broker.create_topic_with_config(name, cfg).await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let data_dir = std::env::var("KAFKO_DATA_DIR")
        .unwrap_or_else(|_| "./kafko-http_data".to_string());

    if std::env::var("KAFKO_RESET").is_ok() {
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    let broker = Kafko::open(&data_dir).await?;
    ensure_topic(&broker, "bench_none", Compression::None).await?;
    ensure_topic(&broker, "bench_lz4", Compression::Lz4).await?;
    ensure_topic(&broker, "bench_zstd", Compression::Zstd).await?;

    let producer_none = broker.producer_for("bench_none").await?;
    let producer_lz4 = broker.producer_for("bench_lz4").await?;
    let producer_zstd = broker.producer_for("bench_zstd").await?;
    let partition_none = broker
        .topic("bench_none")
        .await
        .expect("topic 'bench_none' exists after create");

    let state = AppState {
        producer_none,
        producer_lz4,
        producer_zstd,
        partition_none,
    };

    let app = Router::new()
        .route("/produce/:codec", post(produce_handler))
        .route("/hwm", get(hwm_handler))
        .with_state(state);

    let bind = std::env::var("KAFKO_BIND").unwrap_or_else(|_| "127.0.0.1:9091".to_string());
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    eprintln!("kafko HTTP server listening on http://{bind}");
    eprintln!("  POST /produce/none  (Compression::None)");
    eprintln!("  POST /produce/lz4   (Compression::Lz4)");
    eprintln!("  POST /produce/zstd  (Compression::Zstd)");

    axum::serve(listener, app).await?;
    Ok(())
}
