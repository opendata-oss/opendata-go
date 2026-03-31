package opendataexporter

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
)

// NewFactory creates the OpenData exporter factory.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		componentType,
		createDefaultConfig,
		exporter.WithMetrics(createMetricsExporter, component.StabilityLevelAlpha),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		ObjectStore: ObjectStoreConfig{
			Type: objectStoreTypeS3,
		},
		DataPathPrefix: "ingest/otel/metrics/data",
		ManifestPath:   "ingest/otel/metrics/manifest",
		FlushInterval:  10 * time.Second,
		FlushSizeBytes: defaultFlushSizeMiB,
		Compression:    compressionZstd,
	}
}

func createMetricsExporter(_ context.Context, _ exporter.Settings, cfg component.Config) (exporter.Metrics, error) {
	return newOpenDataExporter(cfg.(*Config)), nil
}
