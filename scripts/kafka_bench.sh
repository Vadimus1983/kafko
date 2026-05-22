#!/usr/bin/env bash
# kafka_bench.sh — Benchmark Apache Kafka local broker for comparison vs kafko.
#
# Usage:
#   bash scripts/kafka_bench.sh
#
# Requirements:
#   - Docker installed and running
#   - Internet (first run pulls apache/kafka image, ~400 MB)
#
# Output:
#   kafka_bench_results.txt in the current directory
#
# Matrix:
#   4 modes × 6 record sizes = 24 measurements, ~15-20 minutes total.

set -euo pipefail

# ─── Config ──────────────────────────────────────────────────────────────────
KAFKA_IMAGE="apache/kafka:3.7.0"
CONTAINER="kafka-bench"
TOPIC="bench"
SIZES=(64 256 512 1024 4096 1048576)
RESULTS="kafka_bench_results.txt"
CLUSTER_ID="ciWo7IWazngRchmPES6q5A"

# ─── Helpers ─────────────────────────────────────────────────────────────────
cleanup() {
    echo "[cleanup] Stopping and removing container..."
    docker stop "$CONTAINER" 2>/dev/null || true
    docker rm "$CONTAINER" 2>/dev/null || true
}
trap cleanup EXIT

require_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "ERROR: '$1' not found in PATH. Please install it."
        exit 1
    fi
}

# ─── Pre-flight ──────────────────────────────────────────────────────────────
require_cmd docker

if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker daemon not running. Start Docker Desktop and try again."
    exit 1
fi

# ─── Start Kafka in KRaft mode ───────────────────────────────────────────────
echo "Starting Kafka $KAFKA_IMAGE in KRaft single-node mode..."
docker rm -f "$CONTAINER" 2>/dev/null || true
docker run -d --name "$CONTAINER" \
    -p 9092:9092 -p 9093:9093 \
    -e KAFKA_NODE_ID=1 \
    -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
    -e CLUSTER_ID="$CLUSTER_ID" \
    "$KAFKA_IMAGE" >/dev/null

echo -n "Waiting for Kafka to become ready"
for _ in $(seq 1 60); do
    if docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
        echo " ✓"
        break
    fi
    echo -n "."
    sleep 1
done

if ! docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    echo " FAILED"
    echo "Kafka container did not come up in 60s. Last 50 log lines:"
    docker logs --tail 50 "$CONTAINER"
    exit 1
fi

# ─── Initialize results file ─────────────────────────────────────────────────
{
    echo "Kafka local broker benchmark"
    echo "Date:  $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "Image: $KAFKA_IMAGE"
    echo "Mode:  KRaft single-node (no replication), acks=1"
    echo "Host:  $(uname -a)"
} > "$RESULTS"

# ─── Bench runner ────────────────────────────────────────────────────────────
run_perf() {
    local size=$1
    local mode=$2
    local num_records linger batch_size compression

    # Number of records scales inversely with size to keep wall-clock reasonable.
    if   [[ $size -ge 1048576 ]]; then num_records=5000
    elif [[ $size -ge 4096    ]]; then num_records=100000
    else                               num_records=500000
    fi

    case "$mode" in
        unbatched)      linger=0;  batch_size=1;     compression=none ;;
        batched)        linger=10; batch_size=65536; compression=none ;;
        batched_lz4)    linger=10; batch_size=65536; compression=lz4  ;;
        batched_zstd)   linger=10; batch_size=65536; compression=zstd ;;
        *)              echo "Unknown mode: $mode"; exit 1 ;;
    esac

    local label="size=${size}B mode=${mode} n=${num_records}"
    echo ""             | tee -a "$RESULTS"
    echo "=== $label ===" | tee -a "$RESULTS"
    echo "config: acks=1 linger.ms=$linger batch.size=$batch_size compression.type=$compression" \
        | tee -a "$RESULTS"

    # Clean state per run.
    docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
        --delete --topic "$TOPIC" --bootstrap-server localhost:9092 >/dev/null 2>&1 || true
    sleep 1
    docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
        --create --topic "$TOPIC" --partitions 1 --replication-factor 1 \
        --bootstrap-server localhost:9092 >/dev/null

    docker exec "$CONTAINER" /opt/kafka/bin/kafka-producer-perf-test.sh \
        --topic "$TOPIC" \
        --num-records "$num_records" \
        --record-size "$size" \
        --throughput -1 \
        --producer-props \
            bootstrap.servers=localhost:9092 \
            acks=1 \
            linger.ms="$linger" \
            batch.size="$batch_size" \
            compression.type="$compression" \
        2>&1 | tee -a "$RESULTS"
}

# ─── Run the matrix ──────────────────────────────────────────────────────────
for mode in unbatched batched batched_lz4 batched_zstd; do
    for size in "${SIZES[@]}"; do
        run_perf "$size" "$mode"
    done
done

# ─── Done ────────────────────────────────────────────────────────────────────
echo ""                                     | tee -a "$RESULTS"
echo "=== DONE — results in $RESULTS ===" | tee -a "$RESULTS"
