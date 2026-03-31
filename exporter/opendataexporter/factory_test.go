package opendataexporter

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
)

func TestNewFactory(t *testing.T) {
	f := NewFactory()

	if got := f.Type(); got != componentType {
		t.Fatalf("expected type %q, got %q", componentType, got)
	}
	if got := f.MetricsStability(); got != component.StabilityLevelAlpha {
		t.Fatalf("expected alpha stability, got %s", got)
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
}

func TestCreateMetricsExporter(t *testing.T) {
	f := NewFactory()
	cfg := createDefaultConfig()

	exp, err := f.CreateMetrics(context.Background(), exporter.Settings{
		ID: component.NewID(componentType),
	}, cfg)
	if err != nil {
		t.Fatalf("CreateMetrics returned error: %v", err)
	}
	if exp == nil {
		t.Fatal("expected exporter instance, got nil")
	}
}
