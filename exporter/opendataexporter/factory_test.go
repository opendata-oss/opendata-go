package opendataexporter

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter"
)

func TestNewFactory(t *testing.T) {
	f := NewFactory()

	if got := f.Type(); got != componentType {
		t.Fatalf("expected type %q, got %q", componentType, got)
	}
	if got := f.MetricsStability(); got != component.StabilityLevelAlpha {
		t.Fatalf("expected alpha metrics stability, got %s", got)
	}
}

func TestCreateDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig().(*Config)

	if cfg.ObjectStore.Type != objectStoreTypeS3 {
		t.Fatalf("expected object store type %q, got %q", objectStoreTypeS3, cfg.ObjectStore.Type)
	}
	if cfg.DataPathPrefix != "ingest/otel/metrics/data" {
		t.Fatalf("unexpected data path prefix: %q", cfg.DataPathPrefix)
	}
	if cfg.ManifestPath != "ingest/otel/metrics/manifest" {
		t.Fatalf("unexpected manifest path: %q", cfg.ManifestPath)
	}
	if cfg.FlushInterval != 10*time.Second {
		t.Fatalf("unexpected flush interval: %v", cfg.FlushInterval)
	}
	if cfg.FlushSizeBytes != defaultFlushSizeMiB {
		t.Fatalf("unexpected flush size: %d", cfg.FlushSizeBytes)
	}
	if cfg.Compression != compressionZstd {
		t.Fatalf("unexpected compression: %q", cfg.Compression)
	}
	if cfg.UploadConcurrency < 1 {
		t.Fatalf("default upload_concurrency must be at least 1, got %d", cfg.UploadConcurrency)
	}
	if cfg.EncodeConcurrency < 1 {
		t.Fatalf("default encode_concurrency must be at least 1, got %d", cfg.EncodeConcurrency)
	}
	if cfg.MaxInFlightBatches < 1 {
		t.Fatalf("default max_inflight_batches must be at least 1, got %d", cfg.MaxInFlightBatches)
	}
	if cfg.MaxInFlightBytes < 1 {
		t.Fatalf("default max_inflight_bytes must be at least 1, got %d", cfg.MaxInFlightBytes)
	}
	if cfg.ManifestAppendBatchSize < 1 {
		t.Fatalf("default manifest_append_batch_size must be at least 1, got %d", cfg.ManifestAppendBatchSize)
	}
}

func TestCreateMetricsExporter(t *testing.T) {
	f := NewFactory()
	cfg := createDefaultConfig()

	exp, err := f.CreateMetrics(context.Background(), exporter.Settings{
		ID:                component.NewID(componentType),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
	}, cfg)
	if err != nil {
		t.Fatalf("CreateMetrics returned error: %v", err)
	}
	if exp == nil {
		t.Fatal("expected exporter instance, got nil")
	}
}
