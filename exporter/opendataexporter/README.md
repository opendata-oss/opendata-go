# opendataexporter

OpenTelemetry Collector exporter that writes OTLP payloads into an [OpenData Buffer](https://github.com/opendata-oss/opendata/tree/main/buffer) queue. Built on the pipelined producer in [`opendata-go/buffer`](../../buffer/).

The exporter supports OTLP **metrics**. [Opendata Timeseries](https://www.opendata.dev/docs/timeseries/ingest) natively integrates with Buffer to ingest metrics into the timeseries database.

## What it does

For one OTLP metrics request:

1. Receives OTLP metrics from the collector pipeline.
2. Serializes the OTLP payload as protobuf, writes it to the Buffer producer as one entry per OTLP request, and attaches the OpenData envelope `{version=1, signal_type, encoding=OTLP, reserved=0}` as the per-entry metadata. `signal_type` is `1` for metrics, set at exporter construction.
3. Returns success to the collector's pipeline only after the configured durability mode is satisfied (see "Durability" below).

## Configuration

```yaml
exporters:
  opendata/metrics:
    object_store:
      type: s3
      bucket: my-otel-bucket
      region: us-west-2
    data_path_prefix: ingest/otel/metrics/data
    manifest_path: ingest/otel/metrics/manifest

    # Flush thresholds (passed through to buffer.ProducerConfig)
    flush_interval: 100ms
    flush_size_bytes: 67108864       # 64 MiB
    compression: zstd                # or "none"

    # Pipelining knobs. Defaults are 1 / 1 / 1 (serial encode + upload +
    # single-ordinal CAS). Raise to opt into the pipelined producer.
    encode_concurrency: 4
    upload_concurrency: 8
    max_inflight_batches: 64
    max_inflight_bytes: 268435456    # 256 MiB
    manifest_append_batch_size: 16   # coalesce up to 16 ordinals per CAS

    # Per-call timeout that exporterhelper applies to the Consume call.
    # 0 inherits exporterhelper's 5s default, which is too tight under
    # high-throughput contention. Set explicitly per deployment.
    timeout: 30s

    # Standard exporterhelper queue. Defaults: enabled, num_consumers=10,
    # queue_size=1000. NumConsumers is the dominant concurrency lever
    # when per-call producer flush is the bottleneck.
    sending_queue:
      enabled: true
      num_consumers: 10
      queue_size: 1000

service:
  pipelines:
    metrics:
      exporters: [opendata/metrics]
```

`AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (or an instance/IRSA role) supply object-store credentials through the standard AWS SDK chain.

### Durability

The exporter returns to the collector's pipeline only after the producer's per-batch durability watcher fires (the batch object was successfully PUT to object storage *and* its ordinal landed in the manifest CAS). A failed retry budget surfaces as a hard error and the collector pipeline marks the batch as failed; the `SendingQueue` then decides whether to retry the in-collector batch.


## Pairing with the producer

The exporter is a thin wrapper around `buffer.ProducerConfig`. The exporter config fields map 1:1 onto producer fields (`encode_concurrency`, `upload_concurrency`, `max_inflight_batches`, `max_inflight_bytes`, `manifest_append_batch_size`); see [`buffer/config.go`](../../buffer/config.go) for defaults and the producer README in [the repo root](../../README.md) for the pipeline diagram.

## Self-monitoring telemetry

The exporter emits the standard `buffer.producer.*` family via the collector's metrics SDK (one stage gauge per pipeline stage, one batch-outcome counter, plus `opendataexporter_durable_wait_duration_seconds`). Wire your collector's own metrics exporter (Prometheus, OTLP, etc.) and these flow out alongside the rest of the collector telemetry.

## Module versioning

Tags are independent from the root module: `exporter/opendataexporter/vX.Y.Z`. Latest is `v0.5.0`, tracking root `v0.4.0`.

## See also

- [Root README](../../README.md): producer architecture, pipeline stages, observer interface.
- [otel-collector image](../../otel-collector/): pre-built collector distribution bundling this exporter.
