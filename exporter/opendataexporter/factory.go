package opendataexporter

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.uber.org/zap"
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
		FlushInterval:  10 * time.Second,
		FlushSizeBytes: defaultFlushSizeMiB,
		Compression:    compressionZstd,
	}
}

func createMetricsExporter(_ context.Context, set exporter.Settings, cfg component.Config) (exporter.Metrics, error) {
	return newOpenDataExporterForSignalWithTelemetry(cfg.(*Config), SignalTypeMetrics, componentTelemetrySettings{
		logger:        set.Logger,
		meterProvider: set.MeterProvider,
	})
}

func createLogsExporter(_ context.Context, set exporter.Settings, cfg component.Config) (exporter.Logs, error) {
	// Copy the config before mutating. The OTel Collector framework may pass
	// the same *Config instance into both createMetricsExporter and
	// createLogsExporter when a single named exporter is wired into multiple
	// pipelines. If we mutated the original here, a metrics exporter created
	// after a logs exporter would inherit the logs-flavored paths.
	local := *(cfg.(*Config))
	applyLogsPathDefaults(&local, set.Logger)
	return newOpenDataExporterForSignalWithTelemetry(&local, SignalTypeLogs, componentTelemetrySettings{
		logger:        set.Logger,
		meterProvider: set.MeterProvider,
	})
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
