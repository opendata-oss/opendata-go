package opendataexporter

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/opendata-oss/opendata-go/buffer"
	"github.com/opendata-oss/opendata-go/objstore"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestOpenDataExporterConsumeMetricsRoundTrip(t *testing.T) {
	store := objstore.NewInMemory()
	cfg := &Config{
		ObjectStore: ObjectStoreConfig{
			Type:   objectStoreTypeS3,
			Bucket: "metrics-bucket",
			Region: "us-west-2",
		},
		DataPathPrefix: "ingest/otel/metrics/data",
		ManifestPath:   "ingest/otel/metrics/manifest",
		FlushInterval:  24 * time.Hour,
		FlushSizeBytes: 1,
		Compression:    compressionNone,
	}

	exp := newOpenDataExporter(cfg)
	exp.storeFactory = func(context.Context, ObjectStoreConfig) (objstore.ObjectStore, error) {
		return store, nil
	}

	ctx := context.Background()
	if err := exp.Start(ctx, nil); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() {
		if err := exp.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown returned error: %v", err)
		}
	})

	original := testMetrics()
	if err := exp.ConsumeMetrics(ctx, original); err != nil {
		t.Fatalf("ConsumeMetrics returned error: %v", err)
	}

	manifestResult, err := store.Get(ctx, cfg.ManifestPath)
	if err != nil {
		t.Fatalf("Get manifest returned error: %v", err)
	}

	entries, err := buffer.DecodeManifestEntries(manifestResult.Data)
	if err != nil {
		t.Fatalf("DecodeManifestEntries returned error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 manifest entry, got %d", len(entries))
	}
	if len(entries[0].Metadata) != 1 {
		t.Fatalf("expected 1 metadata range, got %d", len(entries[0].Metadata))
	}

	if got, want := entries[0].Metadata[0].Payload, EncodeMetadata(SignalTypeMetrics, PayloadEncodingOTLP); !bytes.Equal(got, want) {
		t.Fatalf("unexpected metadata payload: got %v want %v", got, want)
	}

	batchResult, err := store.Get(ctx, entries[0].Location)
	if err != nil {
		t.Fatalf("Get batch returned error: %v", err)
	}

	payloads, err := buffer.DecodeBatch(batchResult.Data)
	if err != nil {
		t.Fatalf("DecodeBatch returned error: %v", err)
	}
	if len(payloads) != 1 {
		t.Fatalf("expected 1 payload, got %d", len(payloads))
	}

	unmarshaler := &pmetric.ProtoUnmarshaler{}
	roundTripped, err := unmarshaler.UnmarshalMetrics(payloads[0])
	if err != nil {
		t.Fatalf("UnmarshalMetrics returned error: %v", err)
	}

	marshaler := &pmetric.ProtoMarshaler{}
	originalBytes, err := marshaler.MarshalMetrics(original)
	if err != nil {
		t.Fatalf("MarshalMetrics original returned error: %v", err)
	}
	roundTripBytes, err := marshaler.MarshalMetrics(roundTripped)
	if err != nil {
		t.Fatalf("MarshalMetrics round trip returned error: %v", err)
	}
	if !bytes.Equal(originalBytes, roundTripBytes) {
		t.Fatalf("metrics round trip mismatch")
	}
}

func TestOpenDataExporterConsumeMetricsBeforeStart(t *testing.T) {
	exp := newOpenDataExporter(&Config{
		ObjectStore: ObjectStoreConfig{
			Type: objectStoreTypeS3,
		},
		Compression: compressionNone,
	})

	err := exp.ConsumeMetrics(context.Background(), testMetrics())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, errExporterNotStarted) {
		t.Fatalf("expected %v, got %v", errExporterNotStarted, err)
	}
}

func TestEncodeMetadataLogs(t *testing.T) {
	got := EncodeMetadata(SignalTypeLogs, PayloadEncodingOTLP)
	want := []byte{MetadataVersion, SignalTypeLogs, PayloadEncodingOTLP, 0}
	if !bytes.Equal(got, want) {
		t.Fatalf("EncodeMetadata(logs, otlp) = %v, want %v", got, want)
	}
	if got[1] != 2 {
		t.Fatalf("expected signal byte 2 (logs), got %d", got[1])
	}
}

func TestOpenDataExporterConsumeLogsRoundTrip(t *testing.T) {
	store := objstore.NewInMemory()
	cfg := &Config{
		ObjectStore: ObjectStoreConfig{
			Type:   objectStoreTypeS3,
			Bucket: "logs-bucket",
			Region: "us-west-2",
		},
		DataPathPrefix: "ingest/otel/logs/data",
		ManifestPath:   "ingest/otel/logs/manifest",
		FlushInterval:  24 * time.Hour,
		FlushSizeBytes: 1,
		Compression:    compressionNone,
	}

	exp := newOpenDataExporterForSignal(cfg, SignalTypeLogs)
	exp.storeFactory = func(context.Context, ObjectStoreConfig) (objstore.ObjectStore, error) {
		return store, nil
	}

	ctx := context.Background()
	if err := exp.Start(ctx, nil); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() {
		if err := exp.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown returned error: %v", err)
		}
	})

	original := testLogs()
	if err := exp.ConsumeLogs(ctx, original); err != nil {
		t.Fatalf("ConsumeLogs returned error: %v", err)
	}

	manifestResult, err := store.Get(ctx, cfg.ManifestPath)
	if err != nil {
		t.Fatalf("Get manifest returned error: %v", err)
	}

	entries, err := buffer.DecodeManifestEntries(manifestResult.Data)
	if err != nil {
		t.Fatalf("DecodeManifestEntries returned error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 manifest entry, got %d", len(entries))
	}
	if len(entries[0].Metadata) != 1 {
		t.Fatalf("expected 1 metadata range, got %d", len(entries[0].Metadata))
	}

	if got, want := entries[0].Metadata[0].Payload, EncodeMetadata(SignalTypeLogs, PayloadEncodingOTLP); !bytes.Equal(got, want) {
		t.Fatalf("unexpected metadata payload: got %v want %v", got, want)
	}

	batchResult, err := store.Get(ctx, entries[0].Location)
	if err != nil {
		t.Fatalf("Get batch returned error: %v", err)
	}

	payloads, err := buffer.DecodeBatch(batchResult.Data)
	if err != nil {
		t.Fatalf("DecodeBatch returned error: %v", err)
	}
	if len(payloads) != 1 {
		t.Fatalf("expected 1 payload, got %d", len(payloads))
	}

	unmarshaler := &plog.ProtoUnmarshaler{}
	roundTripped, err := unmarshaler.UnmarshalLogs(payloads[0])
	if err != nil {
		t.Fatalf("UnmarshalLogs returned error: %v", err)
	}

	marshaler := &plog.ProtoMarshaler{}
	originalBytes, err := marshaler.MarshalLogs(original)
	if err != nil {
		t.Fatalf("MarshalLogs original returned error: %v", err)
	}
	roundTripBytes, err := marshaler.MarshalLogs(roundTripped)
	if err != nil {
		t.Fatalf("MarshalLogs round trip returned error: %v", err)
	}
	if !bytes.Equal(originalBytes, roundTripBytes) {
		t.Fatalf("logs round trip mismatch")
	}
}

func TestOpenDataExporterConsumeLogsBeforeStart(t *testing.T) {
	exp := newOpenDataExporterForSignal(&Config{
		ObjectStore: ObjectStoreConfig{
			Type: objectStoreTypeS3,
		},
		Compression: compressionNone,
	}, SignalTypeLogs)

	err := exp.ConsumeLogs(context.Background(), testLogs())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, errExporterNotStarted) {
		t.Fatalf("expected %v, got %v", errExporterNotStarted, err)
	}
}

func TestConsumeMetricsRejectedOnLogsExporter(t *testing.T) {
	exp := newOpenDataExporterForSignal(&Config{
		ObjectStore: ObjectStoreConfig{Type: objectStoreTypeS3},
		Compression: compressionNone,
	}, SignalTypeLogs)
	if err := exp.ConsumeMetrics(context.Background(), testMetrics()); err == nil {
		t.Fatal("expected ConsumeMetrics on a logs exporter to error")
	}
}

func TestConsumeLogsRejectedOnMetricsExporter(t *testing.T) {
	exp := newOpenDataExporterForSignal(&Config{
		ObjectStore: ObjectStoreConfig{Type: objectStoreTypeS3},
		Compression: compressionNone,
	}, SignalTypeMetrics)
	if err := exp.ConsumeLogs(context.Background(), testLogs()); err == nil {
		t.Fatal("expected ConsumeLogs on a metrics exporter to error")
	}
}

func TestMetricsAndLogsExportersDoNotCrossContaminate(t *testing.T) {
	store := objstore.NewInMemory()

	metricsCfg := &Config{
		ObjectStore: ObjectStoreConfig{
			Type:   objectStoreTypeS3,
			Bucket: "shared-bucket",
			Region: "us-west-2",
		},
		DataPathPrefix: "ingest/otel/metrics/data",
		ManifestPath:   "ingest/otel/metrics/manifest",
		FlushInterval:  24 * time.Hour,
		FlushSizeBytes: 1,
		Compression:    compressionNone,
	}
	logsCfg := &Config{
		ObjectStore: ObjectStoreConfig{
			Type:   objectStoreTypeS3,
			Bucket: "shared-bucket",
			Region: "us-west-2",
		},
		DataPathPrefix: "ingest/otel/logs/data",
		ManifestPath:   "ingest/otel/logs/manifest",
		FlushInterval:  24 * time.Hour,
		FlushSizeBytes: 1,
		Compression:    compressionNone,
	}

	metricsExp := newOpenDataExporterForSignal(metricsCfg, SignalTypeMetrics)
	metricsExp.storeFactory = func(context.Context, ObjectStoreConfig) (objstore.ObjectStore, error) { return store, nil }
	logsExp := newOpenDataExporterForSignal(logsCfg, SignalTypeLogs)
	logsExp.storeFactory = func(context.Context, ObjectStoreConfig) (objstore.ObjectStore, error) { return store, nil }

	ctx := context.Background()
	if err := metricsExp.Start(ctx, nil); err != nil {
		t.Fatalf("metrics Start: %v", err)
	}
	t.Cleanup(func() { _ = metricsExp.Shutdown(context.Background()) })
	if err := logsExp.Start(ctx, nil); err != nil {
		t.Fatalf("logs Start: %v", err)
	}
	t.Cleanup(func() { _ = logsExp.Shutdown(context.Background()) })

	if err := metricsExp.ConsumeMetrics(ctx, testMetrics()); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}
	if err := logsExp.ConsumeLogs(ctx, testLogs()); err != nil {
		t.Fatalf("ConsumeLogs: %v", err)
	}

	// Metrics manifest holds exactly one entry whose envelope tags it as metrics.
	metricsManifest, err := store.Get(ctx, metricsCfg.ManifestPath)
	if err != nil {
		t.Fatalf("get metrics manifest: %v", err)
	}
	metricsEntries, err := buffer.DecodeManifestEntries(metricsManifest.Data)
	if err != nil {
		t.Fatalf("decode metrics manifest: %v", err)
	}
	if len(metricsEntries) != 1 {
		t.Fatalf("metrics manifest entry count: got %d want 1", len(metricsEntries))
	}
	if got, want := metricsEntries[0].Metadata[0].Payload, EncodeMetadata(SignalTypeMetrics, PayloadEncodingOTLP); !bytes.Equal(got, want) {
		t.Fatalf("metrics manifest envelope: got %v want %v", got, want)
	}

	// Logs manifest holds exactly one entry whose envelope tags it as logs.
	logsManifest, err := store.Get(ctx, logsCfg.ManifestPath)
	if err != nil {
		t.Fatalf("get logs manifest: %v", err)
	}
	logsEntries, err := buffer.DecodeManifestEntries(logsManifest.Data)
	if err != nil {
		t.Fatalf("decode logs manifest: %v", err)
	}
	if len(logsEntries) != 1 {
		t.Fatalf("logs manifest entry count: got %d want 1", len(logsEntries))
	}
	if got, want := logsEntries[0].Metadata[0].Payload, EncodeMetadata(SignalTypeLogs, PayloadEncodingOTLP); !bytes.Equal(got, want) {
		t.Fatalf("logs manifest envelope: got %v want %v", got, want)
	}

	// Data objects live under their respective prefixes; no cross-pollination.
	if got := metricsEntries[0].Location; len(got) == 0 || got[:len(metricsCfg.DataPathPrefix)] != metricsCfg.DataPathPrefix {
		t.Fatalf("metrics batch location %q does not start with %q", got, metricsCfg.DataPathPrefix)
	}
	if got := logsEntries[0].Location; len(got) == 0 || got[:len(logsCfg.DataPathPrefix)] != logsCfg.DataPathPrefix {
		t.Fatalf("logs batch location %q does not start with %q", got, logsCfg.DataPathPrefix)
	}
}

func testLogs() plog.Logs {
	ld := plog.NewLogs()

	resourceLogs := ld.ResourceLogs().AppendEmpty()
	resourceLogs.Resource().Attributes().PutStr("service.name", "opendata-exporter-test")
	resourceLogs.Resource().Attributes().PutStr("k8s.namespace.name", "responsive")

	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()
	scopeLogs.Scope().SetName("opendataexporter")

	rec := scopeLogs.LogRecords().AppendEmpty()
	rec.Body().SetStr("controller reconciled topic foo")
	rec.SetSeverityNumber(plog.SeverityNumberInfo)
	rec.SetSeverityText("INFO")
	rec.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(1_700_000_000, 0)))
	rec.Attributes().PutStr("topic", "foo")

	return ld
}

func testMetrics() pmetric.Metrics {
	md := pmetric.NewMetrics()

	resourceMetrics := md.ResourceMetrics().AppendEmpty()
	resourceMetrics.Resource().Attributes().PutStr("service.name", "opendata-exporter-test")

	scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()
	scopeMetrics.Scope().SetName("opendataexporter")

	metric := scopeMetrics.Metrics().AppendEmpty()
	metric.SetName("requests.total")
	metric.SetDescription("test gauge")
	metric.SetUnit("1")
	metric.SetEmptyGauge()

	dp := metric.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("route", "/metrics")
	dp.SetIntValue(42)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(1_700_000_000, 0)))

	return md
}
