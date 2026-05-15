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
	if got := f.LogsStability(); got != component.StabilityLevelAlpha {
		t.Fatalf("expected alpha logs stability, got %s", got)
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

func TestCreateLogsExporter(t *testing.T) {
	f := NewFactory()
	cfg := createDefaultConfig()

	exp, err := f.CreateLogs(context.Background(), exporter.Settings{
		ID:                component.NewID(componentType),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
	}, cfg)
	if err != nil {
		t.Fatalf("CreateLogs returned error: %v", err)
	}
	if exp == nil {
		t.Fatal("expected exporter instance, got nil")
	}
}

// TestCreateLogsExporterSwapsMetricsDefaults verifies that when the user
// leaves the path defaults at their metrics-flavored values, the logs
// factory swaps them to logs-flavored defaults so a misconfigured logs
// pipeline cannot share a manifest with metrics.
func TestCreateLogsExporterSwapsMetricsDefaults(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	if cfg.DataPathPrefix != defaultMetricsDataPathPrefix {
		t.Fatalf("setup: default data_path_prefix is not the metrics default: %q", cfg.DataPathPrefix)
	}

	applyLogsPathDefaults(cfg, nil)
	if cfg.DataPathPrefix != defaultLogsDataPathPrefix {
		t.Fatalf("DataPathPrefix not swapped to logs default: got %q want %q", cfg.DataPathPrefix, defaultLogsDataPathPrefix)
	}
	if cfg.ManifestPath != defaultLogsManifestPath {
		t.Fatalf("ManifestPath not swapped to logs default: got %q want %q", cfg.ManifestPath, defaultLogsManifestPath)
	}
}

// TestCreateLogsExporterPreservesUserPaths verifies that explicit user
// configuration is left alone — only the literal metrics defaults are
// swapped.
func TestCreateLogsExporterPreservesUserPaths(t *testing.T) {
	cfg := &Config{
		DataPathPrefix: "ingest/custom/data",
		ManifestPath:   "ingest/custom/manifest",
	}
	applyLogsPathDefaults(cfg, nil)
	if cfg.DataPathPrefix != "ingest/custom/data" {
		t.Fatalf("DataPathPrefix should be preserved: got %q", cfg.DataPathPrefix)
	}
	if cfg.ManifestPath != "ingest/custom/manifest" {
		t.Fatalf("ManifestPath should be preserved: got %q", cfg.ManifestPath)
	}
}

// TestCreateLogsExporterDoesNotMutateInputConfig pins down a real OTel
// lifecycle hazard: when the Collector wires a single named exporter into
// both metrics and logs pipelines, both create functions receive the same
// *Config instance. If createLogsExporter mutates that shared object, a
// metrics exporter created after a logs exporter inherits logs-flavored
// paths and silently writes to the wrong manifest.
func TestCreateLogsExporterDoesNotMutateInputConfig(t *testing.T) {
	f := NewFactory()
	cfg := createDefaultConfig()
	originalDataPath := cfg.(*Config).DataPathPrefix
	originalManifest := cfg.(*Config).ManifestPath

	if _, err := f.CreateLogs(context.Background(), exporter.Settings{
		ID:                component.NewID(componentType),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
	}, cfg); err != nil {
		t.Fatalf("CreateLogs returned error: %v", err)
	}

	// The framework's config is unchanged; per-signal defaults live on the
	// constructed exporter's internal copy, not on the shared cfg object.
	if got := cfg.(*Config).DataPathPrefix; got != originalDataPath {
		t.Fatalf("CreateLogs mutated DataPathPrefix: got %q want %q", got, originalDataPath)
	}
	if got := cfg.(*Config).ManifestPath; got != originalManifest {
		t.Fatalf("CreateLogs mutated ManifestPath: got %q want %q", got, originalManifest)
	}
}
