# opendataexporter

OpenTelemetry Collector exporter that writes OTLP payloads into an [OpenData Buffer](https://github.com/opendata-oss/opendata/tree/main/buffer) queue. Built on the pipelined producer in [`opendata-go/buffer`](../../buffer/).

Two factories ship: one for OTLP logs, one for OTLP metrics. There are a couple of consumers of OTLP data in Buffer:

1. The [`clickhouse-ingestor`](https://github.com/opendata-oss/opendata-contrib/tree/main/connectors/clickhouse-ingestor) reads the **logs** Buffer and writes rows into ClickHouse.
2. [Opendata Timeseries](https://www.opendata.dev/docs/timeseries/ingest) natively integrates with Buffer to ingest metrics into the timeseries database.

Each named `opendata` exporter handles exactly one signal — the factory you wire into the logs pipeline is a logs exporter, the factory you wire into the metrics pipeline is a metrics exporter. To carry both signals on the same collector, define two named instances with disjoint `manifest_path` / `data_path_prefix` so the two Buffer queues stay independent.

## What it does

For one OTLP request (logs or metrics):

1. Receives OTLP from the collector pipeline (logs and metrics today).
2. **Logs only:** stamps each `ResourceLogs.Resource()` with `_odb_gateway_received_at`, a UTC nanosecond timestamp recording when the gateway received the request. Every `ResourceLogs` in a single Consume call shares the same stamp (per-request granularity, not per-record). The metrics path does not stamp anything.
3. Serializes the OTLP payload as protobuf, writes it to the Buffer producer as one entry per OTLP request, and attaches the OpenData envelope `{version=1, signal_type, encoding=OTLP, reserved=0}` as the per-entry metadata. `signal_type` is `2` for logs, `1` for metrics, set at exporter construction.
4. Returns success to the collector's pipeline only after the configured durability mode is satisfied (see "Durability" below).

## Configuration

```yaml
exporters:
  opendata/logs:
    object_store:
      type: s3
      bucket: my-otel-bucket
      region: us-west-2
    data_path_prefix: ingest/otel/logs/data
    manifest_path: ingest/otel/logs/manifest

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

  # A second instance for metrics writes to its own Buffer queue. Other
  # knobs match the logs instance unless your signal volumes diverge.
  opendata/metrics:
    object_store:
      type: s3
      bucket: my-otel-bucket
      region: us-west-2
    data_path_prefix: ingest/otel/metrics/data
    manifest_path: ingest/otel/metrics/manifest
    flush_interval: 100ms
    flush_size_bytes: 67108864
    compression: zstd

service:
  pipelines:
    logs:
      exporters: [opendata/logs]
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
- [clickhouse-ingestor](https://github.com/opendata-oss/opendata-contrib/tree/main/connectors/clickhouse-ingestor): downstream consumer that reads the logs Buffer this exporter writes.
