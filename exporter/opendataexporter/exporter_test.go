package opendataexporter

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/opendata-oss/opendata-go/ingest"
	"github.com/opendata-oss/opendata-go/objstore"
	"go.opentelemetry.io/collector/pdata/pcommon"
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

	entries, err := ingest.DecodeManifestEntries(manifestResult.Data)
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

	payloads, err := ingest.DecodeBatch(batchResult.Data)
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
