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
		slowRequestThreshold:      threshold,
		slowFlushThreshold:        threshold,
	}, nil
}

type componentTelemetrySettings struct {
	logger        *zap.Logger
	meterProvider metric.MeterProvider
}

func (t *exporterTelemetry) recordRequestStart(ctx context.Context, metricCount, dataPointCount int) {
	t.requestsTotal.Add(ctx, 1)
	t.metricsReceivedTotal.Add(ctx, int64(metricCount))
	t.dataPointsReceivedTotal.Add(ctx, int64(dataPointCount))
}

func (t *exporterTelemetry) recordMarshal(ctx context.Context, sizeBytes int, duration time.Duration) {
	t.bytesMarshaledTotal.Add(ctx, int64(sizeBytes))
	t.marshalDuration.Record(ctx, duration.Seconds())
}

func (t *exporterTelemetry) recordEnqueueWait(ctx context.Context, duration time.Duration) {
	t.enqueueWaitDuration.Record(ctx, duration.Seconds())
}

func (t *exporterTelemetry) recordDurableWait(ctx context.Context, duration time.Duration, err error, metricCount, dataPointCount, sizeBytes int) {
	t.durableWaitDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(resultAttr(err)))
	if err != nil {
		return
	}
	if duration >= t.slowRequestThreshold {
		t.logger.Warn(
			"OpenData exporter request completed slowly",
			zap.Duration("duration", duration),
			zap.Int("metrics", metricCount),
			zap.Int("data_points", dataPointCount),
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
