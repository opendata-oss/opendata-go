package opendataexporter

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/opendata-oss/opendata-go/ingest"
	"github.com/opendata-oss/opendata-go/objstore"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

var errExporterNotStarted = errors.New("opendata exporter not started")

type storeFactory func(context.Context, ObjectStoreConfig) (objstore.ObjectStore, error)

type openDataExporter struct {
	config       Config
	marshaler    *pmetric.ProtoMarshaler
	metadata     []byte
	storeFactory storeFactory
	telemetry    *exporterTelemetry

	mu       sync.RWMutex
	ingestor *ingest.Ingestor
}

func newOpenDataExporter(cfg *Config) *openDataExporter {
	exp, err := newOpenDataExporterWithTelemetry(cfg, componentTelemetrySettings{})
	if err != nil {
		panic(err)
	}
	return exp
}

func newOpenDataExporterWithTelemetry(cfg *Config, telemetrySettings componentTelemetrySettings) (*openDataExporter, error) {
	telemetry, err := newExporterTelemetry(telemetrySettings, *cfg)
	if err != nil {
		return nil, err
	}
	return &openDataExporter{
		config:       *cfg,
		marshaler:    &pmetric.ProtoMarshaler{},
		metadata:     EncodeMetadata(SignalTypeMetrics, PayloadEncodingOTLP),
		storeFactory: newObjectStore,
		telemetry:    telemetry,
	}, nil
}

func (e *openDataExporter) Start(ctx context.Context, _ component.Host) error {
	if err := e.config.Validate(); err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.ingestor != nil {
		return nil
	}

	store, err := e.storeFactory(ctx, e.config.ObjectStore)
	if err != nil {
		return err
	}

	compression, err := compressionTypeFromString(e.config.Compression)
	if err != nil {
		return err
	}

	ingestConfig := ingest.DefaultIngestorConfig()
	ingestConfig.DataPathPrefix = e.config.DataPathPrefix
	ingestConfig.ManifestPath = e.config.ManifestPath
	ingestConfig.FlushInterval = e.config.FlushInterval
	ingestConfig.FlushSizeBytes = e.config.FlushSizeBytes
	ingestConfig.BatchCompression = compression
	ingestConfig.Observer = e.telemetry

	e.ingestor = ingest.NewIngestor(store, ingestConfig)
	e.telemetry.logger.Info(
		"Starting OpenData exporter",
		zap.String("bucket", e.config.ObjectStore.Bucket),
		zap.String("region", e.config.ObjectStore.Region),
		zap.String("data_path_prefix", e.config.DataPathPrefix),
		zap.String("manifest_path", e.config.ManifestPath),
		zap.Duration("flush_interval", e.config.FlushInterval),
		zap.Int("flush_size_bytes", e.config.FlushSizeBytes),
		zap.String("compression", e.config.Compression),
	)
	return nil
}

func (e *openDataExporter) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	ing := e.ingestor
	e.ingestor = nil
	e.mu.Unlock()

	if ing == nil {
		return nil
	}
	return ing.Close(ctx)
}

func (e *openDataExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *openDataExporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	start := time.Now()
	metricCount := md.MetricCount()
	dataPointCount := md.DataPointCount()
	e.telemetry.recordRequestStart(ctx, metricCount, dataPointCount)

	marshalStart := time.Now()
	buf, err := e.marshaler.MarshalMetrics(md)
	if err != nil {
		e.telemetry.recordFailure(ctx, "marshal", err)
		return err
	}
	e.telemetry.recordMarshal(ctx, len(buf), time.Since(marshalStart))

	e.mu.RLock()
	ing := e.ingestor
	e.mu.RUnlock()
	if ing == nil {
		e.telemetry.recordFailure(ctx, "start", errExporterNotStarted)
		return errExporterNotStarted
	}

	enqueueStart := time.Now()
	handle, err := ing.Ingest([][]byte{buf}, e.metadata)
	e.telemetry.recordEnqueueWait(ctx, time.Since(enqueueStart))
	if err != nil {
		e.telemetry.recordFailure(ctx, "enqueue", err)
		return err
	}
	err = handle.Watcher.AwaitDurable(ctx)
	if err != nil {
		e.telemetry.recordFailure(ctx, "durable_wait", err)
	}
	e.telemetry.recordDurableWait(ctx, time.Since(start), err, metricCount, dataPointCount, len(buf))
	return err
}

func newObjectStore(ctx context.Context, cfg ObjectStoreConfig) (objstore.ObjectStore, error) {
	switch cfg.Type {
	case objectStoreTypeS3:
	default:
		return nil, fmt.Errorf("unsupported object_store.type %q", cfg.Type)
	}

	awsCfg, err := awscfg.LoadDefaultConfig(ctx, awscfg.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = &cfg.Endpoint
		}
	})

	return objstore.NewS3(client, cfg.Bucket), nil
}

func compressionTypeFromString(value string) (ingest.CompressionType, error) {
	switch strings.ToLower(value) {
	case compressionNone:
		return ingest.CompressionNone, nil
	case compressionZstd:
		return ingest.CompressionZstd, nil
	default:
		return 0, fmt.Errorf("unsupported compression %q", value)
	}
}
