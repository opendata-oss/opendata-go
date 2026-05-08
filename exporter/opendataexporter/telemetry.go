package opendataexporter

import (
	"context"
	"time"

	"github.com/opendata-oss/opendata-go/buffer"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
)

const telemetryScopeName = "github.com/opendata-oss/opendata-go/exporter/opendataexporter"

type exporterTelemetry struct {
	logger *zap.Logger

	requestsTotal             metric.Int64Counter
	requestFailuresTotal      metric.Int64Counter
	bytesMarshaledTotal       metric.Int64Counter
	metricsReceivedTotal      metric.Int64Counter
	dataPointsReceivedTotal   metric.Int64Counter
	logRecordsReceivedTotal   metric.Int64Counter
	marshalDuration           metric.Float64Histogram
	enqueueWaitDuration       metric.Float64Histogram
	durableWaitDuration       metric.Float64Histogram
	pendingInputs             metric.Int64UpDownCounter
	flushesTotal              metric.Int64Counter
	flushDuration             metric.Float64Histogram
	batchInputs               metric.Int64Histogram
	batchEntries              metric.Int64Histogram
	batchUncompressedBytes    metric.Int64Histogram
	batchAge                  metric.Float64Histogram
	storePutDuration          metric.Float64Histogram
	storePayloadBytes         metric.Int64Histogram
	manifestEnqueueDuration   metric.Float64Histogram
	manifestConflictsTotal    metric.Int64Counter
	manifestConflictsPerWrite metric.Int64Histogram

	// Phase 3.7 buffer.producer.* metric instruments — F5 of Phase 3
	// rev-2 review. Names mirror design §Metrics with the
	// `opendataexporter.` namespace prefix to keep them adjacent
	// to the rest of the exporter's metrics in dashboards.
	appendChBlockDuration   metric.Float64Histogram
	workersBusyGauge        metric.Int64Gauge
	encodeDuration          metric.Float64Histogram
	uploadDuration          metric.Float64Histogram
	manifestAppendBatchSize metric.Int64Histogram
	manifestAppendDuration  metric.Float64Histogram
	headOfLineBlockDuration metric.Float64Histogram
	batchOutcomeTotal       metric.Int64Counter
	haltedGauge             metric.Int64Gauge
	inflightBytesGauge      metric.Int64Gauge
	inflightBatchesGauge    metric.Int64Gauge

	slowRequestThreshold time.Duration
	slowFlushThreshold   time.Duration
}

func newExporterTelemetry(set componentTelemetrySettings, cfg Config) (*exporterTelemetry, error) {
	logger := set.logger
	if logger == nil {
		logger = zap.NewNop()
	}

	meterProvider := set.meterProvider
	if meterProvider == nil {
		meterProvider = noop.NewMeterProvider()
	}

	meter := meterProvider.Meter(telemetryScopeName)
	requestsTotal, err := meter.Int64Counter(
		"opendataexporter.requests_total",
		metric.WithDescription("Number of export requests received by the OpenData exporter."),
	)
	if err != nil {
		return nil, err
	}
	requestFailuresTotal, err := meter.Int64Counter(
		"opendataexporter.request_failures_total",
		metric.WithDescription("Number of export requests that failed before becoming durable."),
	)
	if err != nil {
		return nil, err
	}
	bytesMarshaledTotal, err := meter.Int64Counter(
		"opendataexporter.bytes_marshaled_total",
		metric.WithDescription("Total OTLP payload bytes marshaled by the OpenData exporter."),
	)
	if err != nil {
		return nil, err
	}
	metricsReceivedTotal, err := meter.Int64Counter(
		"opendataexporter.metrics_received_total",
		metric.WithDescription("Total metric descriptors received by the OpenData exporter."),
	)
	if err != nil {
		return nil, err
	}
	dataPointsReceivedTotal, err := meter.Int64Counter(
		"opendataexporter.data_points_received_total",
		metric.WithDescription("Total metric data points received by the OpenData exporter."),
	)
	if err != nil {
		return nil, err
	}
	logRecordsReceivedTotal, err := meter.Int64Counter(
		"opendataexporter.log_records_received_total",
		metric.WithDescription("Total log records received by the OpenData exporter."),
	)
	if err != nil {
		return nil, err
	}
	marshalDuration, err := meter.Float64Histogram(
		"opendataexporter.marshal_duration_seconds",
		metric.WithUnit("s"),
		metric.WithDescription("Time spent marshaling OTLP metrics payloads."),
	)
	if err != nil {
		return nil, err
	}
	enqueueWaitDuration, err := meter.Float64Histogram(
		"opendataexporter.enqueue_wait_duration_seconds",
		metric.WithUnit("s"),
		metric.WithDescription("Time spent waiting to hand a payload to the buffer."),
	)
	if err != nil {
		return nil, err
	}
	durableWaitDuration, err := meter.Float64Histogram(
		"opendataexporter.durable_wait_duration_seconds",
		metric.WithUnit("s"),
		metric.WithDescription("Time from request receipt until the payload is durably flushed."),
	)
	if err != nil {
		return nil, err
	}
	pendingInputs, err := meter.Int64UpDownCounter(
		"opendataexporter.pending_inputs",
		metric.WithDescription("Number of accepted inputs not yet durably flushed."),
	)
	if err != nil {
		return nil, err
	}
	flushesTotal, err := meter.Int64Counter(
		"opendataexporter.flushes_total",
		metric.WithDescription("Number of buffer flush attempts."),
	)
	if err != nil {
		return nil, err
	}
	flushDuration, err := meter.Float64Histogram(
		"opendataexporter.flush_duration_seconds",
		metric.WithUnit("s"),
		metric.WithDescription("Time spent writing a batch to storage and enqueueing it in the manifest."),
	)
	if err != nil {
		return nil, err
	}
	batchInputs, err := meter.Int64Histogram(
		"opendataexporter.batch_inputs",
		metric.WithDescription("Number of accepted inputs contained in a flushed batch."),
	)
	if err != nil {
		return nil, err
	}
	batchEntries, err := meter.Int64Histogram(
		"opendataexporter.batch_entries",
		metric.WithDescription("Number of encoded entries contained in a flushed batch."),
	)
	if err != nil {
		return nil, err
	}
	batchUncompressedBytes, err := meter.Int64Histogram(
		"opendataexporter.batch_uncompressed_bytes",
		metric.WithDescription("Uncompressed bytes contained in a flushed batch before batch encoding."),
	)
	if err != nil {
		return nil, err
	}
	batchAge, err := meter.Float64Histogram(
		"opendataexporter.batch_age_seconds",
		metric.WithUnit("s"),
		metric.WithDescription("Age of a batch when it is flushed."),
	)
	if err != nil {
		return nil, err
	}
	storePutDuration, err := meter.Float64Histogram(
		"opendataexporter.store_put_duration_seconds",
		metric.WithUnit("s"),
		metric.WithDescription("Time spent writing batch objects to object storage."),
	)
	if err != nil {
		return nil, err
	}
	storePayloadBytes, err := meter.Int64Histogram(
		"opendataexporter.store_payload_bytes",
		metric.WithDescription("Compressed batch payload size written to object storage."),
	)
	if err != nil {
		return nil, err
	}
	manifestEnqueueDuration, err := meter.Float64Histogram(
		"opendataexporter.manifest_enqueue_duration_seconds",
		metric.WithUnit("s"),
		metric.WithDescription("Time spent appending a flushed batch to the queue manifest."),
	)
	if err != nil {
		return nil, err
	}
	manifestConflictsTotal, err := meter.Int64Counter(
		"opendataexporter.manifest_conflicts_total",
		metric.WithDescription("Number of optimistic concurrency conflicts while updating the queue manifest."),
	)
	if err != nil {
		return nil, err
	}
	manifestConflictsPerWrite, err := meter.Int64Histogram(
		"opendataexporter.manifest_conflicts_per_write",
		metric.WithDescription("Number of optimistic concurrency conflicts encountered by each manifest write."),
	)
	if err != nil {
		return nil, err
	}

	appendChBlockDuration, err := meter.Float64Histogram(
		"opendataexporter.append_ch_block_duration_seconds",
		metric.WithUnit("s"),
		metric.WithDescription("Time AppendContext spent blocked on the appendCh send (zero in the common no-backpressure case)."),
	)
	if err != nil {
		return nil, err
	}
	workersBusyGauge, err := meter.Int64Gauge(
		"opendataexporter.workers_busy",
		metric.WithDescription("Current count of busy encoder/uploader workers (one series per stage attribute)."),
	)
	if err != nil {
		return nil, err
	}
	encodeDuration, err := meter.Float64Histogram(
		"opendataexporter.encode_duration_seconds",
		metric.WithUnit("s"),
		metric.WithDescription("Per-batch encode wall time."),
	)
	if err != nil {
		return nil, err
	}
	uploadDuration, err := meter.Float64Histogram(
		"opendataexporter.upload_duration_seconds",
		metric.WithUnit("s"),
		metric.WithDescription("Per-batch object-PUT wall time (one sample per attempt)."),
	)
	if err != nil {
		return nil, err
	}
	manifestAppendBatchSize, err := meter.Int64Histogram(
		"opendataexporter.manifest_append_batch_size",
		metric.WithDescription("Ordinals coalesced into a single CAS round trip."),
	)
	if err != nil {
		return nil, err
	}
	manifestAppendDuration, err := meter.Float64Histogram(
		"opendataexporter.manifest_append_duration_seconds",
		metric.WithUnit("s"),
		metric.WithDescription("Per-CAS manifest write wall time."),
	)
	if err != nil {
		return nil, err
	}
	headOfLineBlockDuration, err := meter.Float64Histogram(
		"opendataexporter.head_of_line_block_duration_seconds",
		metric.WithUnit("s"),
		metric.WithDescription("Time the manifest committer waited for the next-expected ordinal while later ordinals were already ready."),
	)
	if err != nil {
		return nil, err
	}
	batchOutcomeTotal, err := meter.Int64Counter(
		"opendataexporter.batch_outcome_total",
		metric.WithDescription("Per-batch terminal outcome count (one series per outcome attribute)."),
	)
	if err != nil {
		return nil, err
	}
	haltedGauge, err := meter.Int64Gauge(
		"opendataexporter.halted",
		metric.WithDescription("1 when the producer has entered the halted state, 0 otherwise."),
	)
	if err != nil {
		return nil, err
	}
	inflightBytesGauge, err := meter.Int64Gauge(
		"opendataexporter.inflight_bytes",
		metric.WithDescription("Bytes currently reserved against MaxInFlightBytes."),
	)
	if err != nil {
		return nil, err
	}
	inflightBatchesGauge, err := meter.Int64Gauge(
		"opendataexporter.inflight_batches",
		metric.WithDescription("Batches currently reserved against MaxInFlightBatches."),
	)
	if err != nil {
		return nil, err
	}

	threshold := 2 * cfg.FlushInterval
	if threshold < 5*time.Second {
		threshold = 5 * time.Second
	}

	return &exporterTelemetry{
		logger:                    logger,
		requestsTotal:             requestsTotal,
		requestFailuresTotal:      requestFailuresTotal,
		bytesMarshaledTotal:       bytesMarshaledTotal,
		metricsReceivedTotal:      metricsReceivedTotal,
		dataPointsReceivedTotal:   dataPointsReceivedTotal,
		logRecordsReceivedTotal:   logRecordsReceivedTotal,
		marshalDuration:           marshalDuration,
		enqueueWaitDuration:       enqueueWaitDuration,
		durableWaitDuration:       durableWaitDuration,
		pendingInputs:             pendingInputs,
		flushesTotal:              flushesTotal,
		flushDuration:             flushDuration,
		batchInputs:               batchInputs,
		batchEntries:              batchEntries,
		batchUncompressedBytes:    batchUncompressedBytes,
		batchAge:                  batchAge,
		storePutDuration:          storePutDuration,
		storePayloadBytes:         storePayloadBytes,
		manifestEnqueueDuration:   manifestEnqueueDuration,
		manifestConflictsTotal:    manifestConflictsTotal,
		manifestConflictsPerWrite: manifestConflictsPerWrite,
		appendChBlockDuration:     appendChBlockDuration,
		workersBusyGauge:          workersBusyGauge,
		encodeDuration:            encodeDuration,
		uploadDuration:            uploadDuration,
		manifestAppendBatchSize:   manifestAppendBatchSize,
		manifestAppendDuration:    manifestAppendDuration,
		headOfLineBlockDuration:   headOfLineBlockDuration,
		batchOutcomeTotal:         batchOutcomeTotal,
		haltedGauge:               haltedGauge,
		inflightBytesGauge:        inflightBytesGauge,
		inflightBatchesGauge:      inflightBatchesGauge,
		slowRequestThreshold:      threshold,
		slowFlushThreshold:        threshold,
	}, nil
}

type componentTelemetrySettings struct {
	logger        *zap.Logger
	meterProvider metric.MeterProvider
}

func (t *exporterTelemetry) recordRequestStartMetrics(ctx context.Context, metricCount, dataPointCount int) {
	t.requestsTotal.Add(ctx, 1)
	t.metricsReceivedTotal.Add(ctx, int64(metricCount))
	t.dataPointsReceivedTotal.Add(ctx, int64(dataPointCount))
}

func (t *exporterTelemetry) recordRequestStartLogs(ctx context.Context, logRecordCount int) {
	t.requestsTotal.Add(ctx, 1)
	t.logRecordsReceivedTotal.Add(ctx, int64(logRecordCount))
}

func (t *exporterTelemetry) recordMarshal(ctx context.Context, sizeBytes int, duration time.Duration) {
	t.bytesMarshaledTotal.Add(ctx, int64(sizeBytes))
	t.marshalDuration.Record(ctx, duration.Seconds())
}

func (t *exporterTelemetry) recordEnqueueWait(ctx context.Context, duration time.Duration) {
	t.enqueueWaitDuration.Record(ctx, duration.Seconds())
}

func (t *exporterTelemetry) recordDurableWaitMetrics(ctx context.Context, duration time.Duration, err error, metricCount, dataPointCount, sizeBytes int) {
	t.durableWaitDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(resultAttr(err)))
	if err != nil {
		return
	}
	if duration >= t.slowRequestThreshold {
		t.logger.Warn(
			"OpenData exporter request completed slowly",
			zap.String("signal", "metrics"),
			zap.Duration("duration", duration),
			zap.Int("metrics", metricCount),
			zap.Int("data_points", dataPointCount),
			zap.Int("payload_bytes", sizeBytes),
		)
	}
}

func (t *exporterTelemetry) recordDurableWaitLogs(ctx context.Context, duration time.Duration, err error, logRecordCount, sizeBytes int) {
	t.durableWaitDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(resultAttr(err)))
	if err != nil {
		return
	}
	if duration >= t.slowRequestThreshold {
		t.logger.Warn(
			"OpenData exporter request completed slowly",
			zap.String("signal", "logs"),
			zap.Duration("duration", duration),
			zap.Int("log_records", logRecordCount),
			zap.Int("payload_bytes", sizeBytes),
		)
	}
}

func (t *exporterTelemetry) recordFailure(ctx context.Context, stage string, err error) {
	t.requestFailuresTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("stage", stage)))
	t.logger.Error("OpenData exporter request failed", zap.String("stage", stage), zap.Error(err))
}

func (t *exporterTelemetry) OnAccepted() {
	t.pendingInputs.Add(context.Background(), 1)
}

func (t *exporterTelemetry) OnFlush(reason buffer.FlushReason, stats buffer.FlushStats, duration time.Duration, err error) {
	ctx := context.Background()
	attrs := metric.WithAttributes(
		attribute.String("reason", string(reason)),
		resultAttr(err),
	)
	t.pendingInputs.Add(ctx, -int64(stats.Inputs))
	t.flushesTotal.Add(ctx, 1, attrs)
	t.flushDuration.Record(ctx, duration.Seconds(), attrs)
	t.batchInputs.Record(ctx, int64(stats.Inputs), attrs)
	t.batchEntries.Record(ctx, int64(stats.Entries), attrs)
	t.batchUncompressedBytes.Record(ctx, int64(stats.UncompressedBytes), attrs)
	t.batchAge.Record(ctx, stats.Age.Seconds(), attrs)

	if err != nil {
		t.logger.Error(
			"OpenData exporter flush failed",
			zap.String("reason", string(reason)),
			zap.Int("inputs", stats.Inputs),
			zap.Int("entries", stats.Entries),
			zap.Int("uncompressed_bytes", stats.UncompressedBytes),
			zap.Duration("age", stats.Age),
			zap.Duration("duration", duration),
			zap.Error(err),
		)
		return
	}
	if duration >= t.slowFlushThreshold {
		t.logger.Warn(
			"OpenData exporter flush completed slowly",
			zap.String("reason", string(reason)),
			zap.Int("inputs", stats.Inputs),
			zap.Int("entries", stats.Entries),
			zap.Int("uncompressed_bytes", stats.UncompressedBytes),
			zap.Duration("age", stats.Age),
			zap.Duration("duration", duration),
		)
	}
}

func (t *exporterTelemetry) OnStorePut(sizeBytes int, duration time.Duration, err error) {
	ctx := context.Background()
	attrs := metric.WithAttributes(resultAttr(err))
	t.storePutDuration.Record(ctx, duration.Seconds(), attrs)
	t.storePayloadBytes.Record(ctx, int64(sizeBytes), attrs)
}

func (t *exporterTelemetry) OnManifestEnqueue(entries int, duration time.Duration, conflicts int, err error) {
	ctx := context.Background()
	attrs := metric.WithAttributes(resultAttr(err))
	t.manifestEnqueueDuration.Record(ctx, duration.Seconds(), attrs)
	t.manifestConflictsPerWrite.Record(ctx, int64(conflicts), attrs)
	if conflicts > 0 {
		t.manifestConflictsTotal.Add(ctx, int64(conflicts))
	}
	if conflicts >= 3 {
		t.logger.Warn(
			"OpenData exporter manifest update encountered repeated conflicts",
			zap.Int("entries", entries),
			zap.Int("conflicts", conflicts),
			zap.Duration("duration", duration),
		)
	}
	if err != nil {
		t.logger.Error(
			"OpenData exporter manifest update failed",
			zap.Int("entries", entries),
			zap.Int("conflicts", conflicts),
			zap.Duration("duration", duration),
			zap.Error(err),
		)
	}
}

func resultAttr(err error) attribute.KeyValue {
	if err != nil {
		return attribute.String("result", "error")
	}
	return attribute.String("result", "success")
}

// Phase 3.7 Observer hooks: wired to concrete OTel metric instruments
// per design §Metrics. F5 of Phase 3 rev-2 review (extends
// buffer.Observer in place — Option (a) of the rev-2 compatibility
// question).

func (t *exporterTelemetry) OnAppendChBlock(d time.Duration) {
	t.appendChBlockDuration.Record(context.Background(), d.Seconds())
}

func (t *exporterTelemetry) OnWorkersBusy(stage buffer.PipelineStage, busy int) {
	t.workersBusyGauge.Record(context.Background(), int64(busy),
		metric.WithAttributes(attribute.String("stage", string(stage))))
}

func (t *exporterTelemetry) OnEncodeDuration(d time.Duration, err error) {
	t.encodeDuration.Record(context.Background(), d.Seconds(),
		metric.WithAttributes(resultAttr(err)))
}

func (t *exporterTelemetry) OnUploadDuration(d time.Duration, _ int, err error) {
	t.uploadDuration.Record(context.Background(), d.Seconds(),
		metric.WithAttributes(resultAttr(err)))
}

func (t *exporterTelemetry) OnManifestAppendBatchSize(n int) {
	t.manifestAppendBatchSize.Record(context.Background(), int64(n))
}

func (t *exporterTelemetry) OnManifestAppendDuration(d time.Duration, _ int, err error) {
	t.manifestAppendDuration.Record(context.Background(), d.Seconds(),
		metric.WithAttributes(resultAttr(err)))
}

func (t *exporterTelemetry) OnHeadOfLineBlock(d time.Duration) {
	t.headOfLineBlockDuration.Record(context.Background(), d.Seconds())
}

func (t *exporterTelemetry) OnBatchOutcome(outcome buffer.BatchOutcome) {
	t.batchOutcomeTotal.Add(context.Background(), 1,
		metric.WithAttributes(attribute.String("outcome", string(outcome))))
}

func (t *exporterTelemetry) OnHalted(halted bool) {
	v := int64(0)
	if halted {
		v = 1
	}
	t.haltedGauge.Record(context.Background(), v)
}

func (t *exporterTelemetry) OnInflightBytes(bytes int64) {
	t.inflightBytesGauge.Record(context.Background(), bytes)
}

func (t *exporterTelemetry) OnInflightBatches(batches int) {
	t.inflightBatchesGauge.Record(context.Background(), int64(batches))
}
