package opendataexporter

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/opendata-oss/opendata-go/buffer"
)

// Default object-store path layout for the metrics signal, used as the
// out-of-the-box createDefaultConfig values.
const (
	defaultMetricsDataPathPrefix = "ingest/otel/metrics/data"
	defaultMetricsManifestPath   = "ingest/otel/metrics/manifest"
)

// NewFactory creates the OpenData exporter factory.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		componentType,
		createDefaultConfig,
		exporter.WithMetrics(createMetricsExporter, component.StabilityLevelAlpha),
	)
}

// createDefaultConfig returns the default configuration. The OTel Collector
// framework calls this once per exporter component instance before merging
// user-supplied overrides.
func createDefaultConfig() component.Config {
	return &Config{
		ObjectStore: ObjectStoreConfig{
			Type: objectStoreTypeS3,
		},
		DataPathPrefix:          defaultMetricsDataPathPrefix,
		ManifestPath:            defaultMetricsManifestPath,
		FlushInterval:           10 * time.Second,
		FlushSizeBytes:          defaultFlushSizeMiB,
		Compression:             compressionZstd,
		UploadConcurrency:       buffer.DefaultUploadConcurrency,
		EncodeConcurrency:       buffer.DefaultEncodeConcurrency,
		MaxInFlightBatches:      buffer.DefaultMaxInFlightBatches,
		MaxInFlightBytes:        buffer.DefaultMaxInFlightBytes,
		ManifestAppendBatchSize: buffer.DefaultManifestAppendBatchSize,
		// Default the sending_queue to the exporterhelper standard
		// (NumConsumers=10, QueueSize=1000, non-blocking). Without
		// the queue, every OTel pipeline call into ConsumeLogs
		// blocks on AwaitDurable, which serializes the receiver
		// against the producer's batch flush cadence.
		SendingQueue: configoptional.Some(exporterhelper.NewDefaultQueueConfig()),
	}
}

// timeoutOptions returns a slice of exporterhelper.Option that adds
// WithTimeout iff the user configured a non-zero Timeout. A zero value
// preserves exporterhelper's default 5s TimeoutConfig — i.e. no behavior
// change unless the cell YAML opts in.
func timeoutOptions(c *Config) []exporterhelper.Option {
	if c.Timeout > 0 {
		return []exporterhelper.Option{
			exporterhelper.WithTimeout(exporterhelper.TimeoutConfig{Timeout: c.Timeout}),
		}
	}
	return nil
}

func createMetricsExporter(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Metrics, error) {
	c := cfg.(*Config)
	inner, err := newOpenDataExporterForSignalWithTelemetry(c, SignalTypeMetrics, componentTelemetrySettings{
		logger:        set.Logger,
		meterProvider: set.MeterProvider,
	})
	if err != nil {
		return nil, err
	}
	opts := []exporterhelper.Option{
		exporterhelper.WithStart(inner.Start),
		exporterhelper.WithShutdown(inner.Shutdown),
		exporterhelper.WithCapabilities(inner.Capabilities()),
		exporterhelper.WithQueue(c.SendingQueue),
	}
	opts = append(opts, timeoutOptions(c)...)
	return exporterhelper.NewMetrics(ctx, set, c, inner.ConsumeMetrics, opts...)
}
