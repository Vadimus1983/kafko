# Dockerfile -- kafko-http server + oha client in a single image.
#
# Purpose: mirror the Kafka bench setup exactly. Both the server and the load
# tester live inside the container so the request path is container-loopback
# only (same shape as kafka-producer-perf-test.sh inside the Kafka container).
#
# Build:   docker build -t kafko-http:bench -f scripts/Dockerfile .
#          (run from project root so the build context contains crates/)
# Run:     see scripts/kafko_docker_bench.ps1

FROM rust:1.89-bookworm AS builder

WORKDIR /build

COPY Cargo.toml Cargo.lock* ./
COPY crates ./crates

RUN cargo build --release --package kafko-http

# oha is the load tester. Built in the same toolchain image; copied as a plain
# binary into the runtime stage so we do not ship Rust.
RUN cargo install oha --version "^1.4"

# ---------------------------------------------------------------------------

FROM debian:bookworm-slim AS runtime

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/kafko-http /usr/local/bin/kafko-http
COPY --from=builder /usr/local/cargo/bin/oha        /usr/local/bin/oha

WORKDIR /data

# Bind to 0.0.0.0 so the host can probe /hwm for readiness. oha calls inside
# the container hit 127.0.0.1 (container-loopback path, same as Kafka bench).
ENV KAFKO_BIND=0.0.0.0:9091
ENV KAFKO_DATA_DIR=/data/kafko
ENV KAFKO_RESET=1

EXPOSE 9091

CMD ["/usr/local/bin/kafko-http"]
