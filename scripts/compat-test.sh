#!/usr/bin/env bash
#
# Cross-language compatibility test: Go producer → Rust consumer
#
# Spins up a MinIO container, runs the Go ingestor to write batches,
# then runs the Rust collector to read and verify them.
#
# Prerequisites:
#   - docker (or podman aliased to docker)
#   - go toolchain
#   - cargo (Rust toolchain)
#
# Usage:
#   ./scripts/compat-test.sh               # uncompressed batches
#   BATCH_COMPRESSION=zstd ./scripts/compat-test.sh  # zstd-compressed batches

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CACHE_DIR="${TMPDIR:-/tmp}/opendata-compat-cache"

# ---------- configuration ----------
MINIO_CONTAINER="ingest-compat-minio"
MINIO_PORT="${MINIO_PORT:-9000}"
MINIO_IMAGE="${MINIO_IMAGE:-minio/minio:RELEASE.2025-09-07T16-13-09Z}"
S3_ENDPOINT="http://localhost:${MINIO_PORT}"
S3_BUCKET="compat-test"
AWS_ACCESS_KEY_ID="test"
AWS_SECRET_ACCESS_KEY="testtesttest"
AWS_REGION="us-east-1"
BATCH_COMPRESSION="${BATCH_COMPRESSION:-none}"

export S3_ENDPOINT S3_BUCKET AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_REGION BATCH_COMPRESSION
export GOCACHE="${GOCACHE:-${CACHE_DIR}/gocache}"
export GOMODCACHE="${GOMODCACHE:-${CACHE_DIR}/gomodcache}"
export GOPATH="${GOPATH:-${CACHE_DIR}/gopath}"
# Rust object_store reads this env var for S3-compatible endpoints.
export AWS_ENDPOINT_URL="${S3_ENDPOINT}"
# Rust object_store needs this to skip loading IMDS credentials.
export AWS_ALLOW_HTTP="true"

# ---------- helpers ----------
cleanup() {
    echo "--- cleanup ---"
    docker rm -f "${MINIO_CONTAINER}" 2>/dev/null || true
}
trap cleanup EXIT

log() { echo "=== $* ==="; }

mkdir -p "${GOCACHE}" "${GOMODCACHE}" "${GOPATH}"

wait_for_minio() {
    local attempts=0
    while ! curl -sf "${S3_ENDPOINT}/minio/health/live" >/dev/null 2>&1; do
        attempts=$((attempts + 1))
        if [ "$attempts" -ge 30 ]; then
            echo "ERROR: MinIO did not become healthy after 30s"
            exit 1
        fi
        sleep 1
    done
}

# ---------- start MinIO ----------
log "starting MinIO"
docker rm -f "${MINIO_CONTAINER}" 2>/dev/null || true
docker run -d \
    --name "${MINIO_CONTAINER}" \
    -p "${MINIO_PORT}:9000" \
    -e "MINIO_ROOT_USER=${AWS_ACCESS_KEY_ID}" \
    -e "MINIO_ROOT_PASSWORD=${AWS_SECRET_ACCESS_KEY}" \
    "${MINIO_IMAGE}" server /data

log "waiting for MinIO to be healthy"
wait_for_minio

# ---------- build ----------
log "building Go producer"
(cd "${PROJECT_DIR}" && go build -o "${PROJECT_DIR}/bin/compat-producer" ./test/compat-producer)

log "building Rust consumer"
(cd "${PROJECT_DIR}/test/compat-consumer" && cargo build 2>&1)

# ---------- run producer ----------
log "running Go producer (compression=${BATCH_COMPRESSION})"
"${PROJECT_DIR}/bin/compat-producer"

# ---------- run consumer ----------
log "running Rust consumer"
"${PROJECT_DIR}/test/compat-consumer/target/debug/compat-consumer"

# ---------- done ----------
log "compatibility test PASSED"
