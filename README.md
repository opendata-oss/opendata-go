# OpenData Go

[![Discord](https://img.shields.io/badge/discord-join-7289DA?style=flat-square&logo=discord)](https://discord.gg/CsAQJ2AJGU)
[![GitHub License](https://img.shields.io/github/license/opendata-oss/opendata?style=flat-square)](LICENSE)

Go bindings for the [OpenData](https://github.com/opendata-oss/opendata) project. Producer for [OpenData Buffer](https://github.com/opendata-oss/opendata/tree/main/buffer), an OTel collector exporter built on it, and the object-store adapters they share.

## Packages

| Package | Description |
|---|---|
| [`buffer`](buffer/) | Pipelined Buffer producer: concurrent encode + upload, manifest-append batching, byte-budgeted backpressure, lifecycle observer |
| [`objstore`](objstore/) | Object-store abstraction with S3 and in-memory implementations |
| [`exporter/opendataexporter`](exporter/opendataexporter/) | OTel collector exporter that writes to a Buffer queue via the producer |
| [`otel-collector`](otel-collector/) | Custom OTel collector distribution that bundles the exporter |
| [`cmd/bench`](cmd/bench/) | Load generator and microbenchmark harness for the producer |

## Producer architecture

The Buffer producer accepts entries, batches them in memory, and persists each batch as one data object plus one manifest append. A single-flight producer (the default config) is correct but caps throughput at the cost of one encode plus one upload plus one CAS manifest write per batch. To push past that, the producer runs a six-stage pipeline:

```
append → rotate → encode → upload → commit → resolve
```

- **append**: callers' `Produce(entries, metadata)` calls enqueue into a bounded accumulator.
- **rotate**: a size or time trigger seals the accumulator into a `Batch` with an assigned ordinal.
- **encode**: N concurrent encoders length-prefix and optionally zstd-compress the batch.
- **upload**: N concurrent uploaders `PUT` the encoded batch object.
- **commit**: a single `ManifestCommitter` coalesces ready ordinals into one `PutIfMatch` against the manifest. Coalescing is opt-in via `ManifestAppendBatchSize` (default 1).
- **resolve**: the durability watcher per batch fires after a successful commit.

Two budgets bound in-flight work per producer:

- `MaxInFlightBatches` (default 64): hard cap on batches that have entered the pipeline and not yet resolved.
- `MaxInFlightBytes` (default 256 MiB): bytes reserved at `appendCh` enqueue and released at commit. The byte budget is the binding backpressure signal; the batch cap is a secondary safety stop.

Retries:

- Upload PUT failures retry up to `UploadMaxAttempts` (default 6) with exponential backoff and jitter starting at `UploadInitialBackoff` (default 100 ms). Exhaustion resolves the batch with a permanent error; later ordinals keep flowing.
- Manifest CAS retries up to `ManifestMaxAttempts` (default 6) for transient write errors. `ErrPreconditionFailed` has its own re-read-and-re-plan loop and is not counted against the attempt budget. Exhaustion halts the producer.

## Configuration

The pipelining knobs are off by default. To run as a pipelined producer, raise `EncodeConcurrency`, `UploadConcurrency`, and `ManifestAppendBatchSize` from the defaults in [`buffer/config.go`](buffer/config.go):

```go
cfg := buffer.DefaultProducerConfig()
cfg.ManifestPath = "ingest/otel/logs/manifest"
cfg.DataPathPrefix = "ingest/otel/logs/data"
cfg.FlushInterval = 100 * time.Millisecond
cfg.FlushSizeBytes = 64 * 1024 * 1024     // 64 MiB

// Pipelining (defaults are 1 / 1 / 1):
cfg.EncodeConcurrency = 4
cfg.UploadConcurrency = 8
cfg.ManifestAppendBatchSize = 16          // coalesce up to 16 ordinals per CAS

cfg.MaxInFlightBytes = 512 * 1024 * 1024  // 512 MiB
cfg.BatchCompression = buffer.CompressionZstd
cfg.Observer = myObserver                 // optional; see Observability
```

A `DefaultProducerConfig()` plus `MaxBufferedInputs` plus an `Observer` is a working serial producer. The pipelining knobs are additive: raise them to opt into parallelism.

## Observability

`buffer.Observer` is the producer's only public observability hook. The interface has hooks at every pipeline stage transition (`OnAccepted`, `OnRotate`, `OnEncodeStart`, `OnUploadStart`, `OnCommitStart`, `OnResolve`, plus error variants) and pipeline-depth gauges per stage. An implementation can leave any hook it doesn't need as a no-op.

The OTel exporter ships an `Observer` that emits the standard `buffer.producer.*` metric family via the collector's metrics SDK. See [`exporter/opendataexporter/`](exporter/opendataexporter/) for the wiring.

## When to use the producer directly vs. via the OTel exporter

The OTel exporter is the right choice if you are already running an OTel collector. It is a normal collector exporter; configure it in your collector config and the producer runs inside the collector process.

Use the Go producer directly when you control the source side and want to push raw bytes into Buffer without an OTel collector hop: a service writing its own custom protobuf, a load generator, an integration test. `cmd/bench` is the worked example.

## Releases

This repository publishes Go modules by git tag, not as prebuilt binaries.

- Root module tags use `vX.Y.Z`. Latest: `v0.4.0`.
- Exporter module tags use `exporter/opendataexporter/vX.Y.Z`. Latest: `v0.5.0`.

The custom OTel collector image is published to `ghcr.io/opendata-oss/otel-collector` by the `Build OTel collector image` workflow. See [`otel-collector/README.md`](otel-collector/README.md) for image tags and pull instructions, and [`RELEASE.md`](RELEASE.md) for the release workflow.
