package opendataexporter

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.uber.org/zap"

	"github.com/opendata-oss/opendata-go/buffer"
)

// Default object-store path layout for the metrics signal. Used both as the
// out-of-the-box createDefaultConfig values and as a sentinel for the logs
// factory so it can swap in logs-flavored defaults when the user hasn't
// customized them.
const (
	defaultMetricsDataPathPrefix = "ingest/otel/metrics/data"
	defaultMetricsManifestPath   = "ingest/otel/metrics/manifest"
	defaultLogsDataPathPrefix    = "ingest/otel/logs/data"
	defaultLogsManifestPath      = "ingest/otel/logs/manifest"
)

// NewFactory creates the OpenData exporter factory.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		componentType,
		createDefaultConfig,
		exporter.WithMetrics(createMetricsExporter, component.StabilityLevelAlpha),
		exporter.WithLogs(createLogsExporter, component.StabilityLevelAlpha),
	)
}

// createDefaultConfig returns the default configuration. The OTel Collector
// framework calls this once per exporter component instance before merging
// user-supplied overrides; the same defaults are used for both metrics and
// logs pipelines because the framework does not know the target signal at
// default time. The metrics-flavored path defaults are kept here for
// backward compatibility; the logs factory swaps them to logs paths if the
// user has not customized them (see createLogsExporter).
func createDefaultConfig() component.Config {
	return &Config{
		ObjectStore: ObjectStoreConfig{
			Type: objectStoreTypeS3,
		},
		DataPathPrefix: defaultMetricsDataPathPrefix,
		ManifestPath:   defaultMetricsManifestPath,
		FlushInterval:      10 * time.Second,
		FlushSizeBytes:     defaultFlushSizeMiB,
		Compression:        compressionZstd,
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

func createLogsExporter(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Logs, error) {
	// Copy the config before mutating. The OTel Collector framework may pass
	// the same *Config instance into both createMetricsExporter and
	// createLogsExporter when a single named exporter is wired into multiple
	// pipelines. If we mutated the original here, a metrics exporter created
	// after a logs exporter would inherit the logs-flavored paths.
	local := *(cfg.(*Config))
	applyLogsPathDefaults(&local, set.Logger)
	inner, err := newOpenDataExporterForSignalWithTelemetry(&local, SignalTypeLogs, componentTelemetrySettings{
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
		exporterhelper.WithQueue(local.SendingQueue),
	}
	opts = append(opts, timeoutOptions(&local)...)
	return exporterhelper.NewLogs(ctx, set, &local, inner.ConsumeLogs, opts...)
}

// applyLogsPathDefaults swaps the metrics-flavored default DataPathPrefix and
// ManifestPath to logs-flavored equivalents when the user has not customized
// them. The signal-shared default config means a YAML that omits these fields
// falls through to the metrics defaults; for a logs pipeline that would mix
// signals on the same manifest, which is the alpha's hard rule against. We
// log a warning so users notice the implicit swap and configure explicitly.
func applyLogsPathDefaults(c *Config, logger *zap.Logger) {
	if logger == nil {
		logger = zap.NewNop()
	}
	if c.DataPathPrefix == defaultMetricsDataPathPrefix {
		logger.Warn(
			"OpenData logs exporter using metrics-default data_path_prefix; swapping to logs default. Configure data_path_prefix explicitly to silence this warning.",
			zap.String("from", c.DataPathPrefix),
			zap.String("to", defaultLogsDataPathPrefix),
		)
		c.DataPathPrefix = defaultLogsDataPathPrefix
	}
	if c.ManifestPath == defaultMetricsManifestPath {
		logger.Warn(
			"OpenData logs exporter using metrics-default manifest_path; swapping to logs default. Configure manifest_path explicitly to silence this warning.",
			zap.String("from", c.ManifestPath),
			zap.String("to", defaultLogsManifestPath),
		)
		c.ManifestPath = defaultLogsManifestPath
	}
}
