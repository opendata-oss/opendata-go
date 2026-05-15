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
	"github.com/opendata-oss/opendata-go/buffer"
	"github.com/opendata-oss/opendata-go/objstore"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

var errExporterNotStarted = errors.New("opendata exporter not started")

type storeFactory func(context.Context, ObjectStoreConfig) (objstore.ObjectStore, error)

type openDataExporter struct {
	config       Config
	signalType   uint8
	metadata     []byte
	storeFactory storeFactory
	telemetry    *exporterTelemetry

	metricsMarshaler *pmetric.ProtoMarshaler
	logsMarshaler    *plog.ProtoMarshaler

	mu       sync.RWMutex
	producer *buffer.Producer
}

func newOpenDataExporter(cfg *Config) *openDataExporter {
	return newOpenDataExporterForSignal(cfg, SignalTypeMetrics)
}

func newOpenDataExporterForSignal(cfg *Config, signalType uint8) *openDataExporter {
	exp, err := newOpenDataExporterForSignalWithTelemetry(cfg, signalType, componentTelemetrySettings{})
	if err != nil {
		panic(err)
	}
	return exp
}

func newOpenDataExporterForSignalWithTelemetry(cfg *Config, signalType uint8, telemetrySettings componentTelemetrySettings) (*openDataExporter, error) {
	if signalType != SignalTypeMetrics && signalType != SignalTypeLogs {
		return nil, fmt.Errorf("unsupported signal type %d", signalType)
	}
	telemetry, err := newExporterTelemetry(telemetrySettings, *cfg)
	if err != nil {
		return nil, err
	}
	exp := &openDataExporter{
		config:       *cfg,
		signalType:   signalType,
		metadata:     EncodeMetadata(signalType, PayloadEncodingOTLP),
		storeFactory: newObjectStore,
		telemetry:    telemetry,
	}
	switch signalType {
	case SignalTypeMetrics:
		exp.metricsMarshaler = &pmetric.ProtoMarshaler{}
	case SignalTypeLogs:
		exp.logsMarshaler = &plog.ProtoMarshaler{}
	}
	return exp, nil
}

func (e *openDataExporter) Start(ctx context.Context, _ component.Host) error {
	if err := e.config.Validate(); err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.producer != nil {
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

	producerConfig := buffer.DefaultProducerConfig()
	producerConfig.DataPathPrefix = e.config.DataPathPrefix
	producerConfig.ManifestPath = e.config.ManifestPath
	producerConfig.FlushInterval = e.config.FlushInterval
	producerConfig.FlushSizeBytes = e.config.FlushSizeBytes
	producerConfig.BatchCompression = compression
	producerConfig.Observer = e.telemetry

	e.producer = buffer.NewProducer(store, producerConfig)
	// Wire the producer into the telemetry's async-gauge callback so
	// `buffer.producer.oldest_unflushed_batch_age_seconds` starts
	// reporting non-zero values on the next OTel collection cycle.
	e.telemetry.setProducer(e.producer)
	e.telemetry.logger.Info(
		"Starting OpenData exporter",
		zap.String("signal", signalLabel(e.signalType)),
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
	p := e.producer
	e.producer = nil
	e.mu.Unlock()

	if p == nil {
		return nil
	}
	// Detach from the async-gauge callback before tearing down so
	// the callback can't observe a closed producer mid-Close.
	e.telemetry.setProducer(nil)
	return p.Close(ctx)
}

func (e *openDataExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *openDataExporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	if e.signalType != SignalTypeMetrics {
		return fmt.Errorf("ConsumeMetrics called on %s exporter", signalLabel(e.signalType))
	}

	start := time.Now()
	metricCount := md.MetricCount()
	dataPointCount := md.DataPointCount()
	e.telemetry.recordRequestStartMetrics(ctx, metricCount, dataPointCount)

	marshalStart := time.Now()
	buf, err := e.metricsMarshaler.MarshalMetrics(md)
	if err != nil {
		e.telemetry.recordFailure(ctx, "marshal", err)
		return err
	}
	e.telemetry.recordMarshal(ctx, len(buf), time.Since(marshalStart))

	err = e.appendAndAwait(ctx, buf)
	e.telemetry.recordDurableWaitMetrics(ctx, time.Since(start), err, metricCount, dataPointCount, len(buf))
	return err
}

func (e *openDataExporter) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	if e.signalType != SignalTypeLogs {
		return fmt.Errorf("ConsumeLogs called on %s exporter", signalLabel(e.signalType))
	}

	start := time.Now()
	logRecordCount := ld.LogRecordCount()
	e.telemetry.recordRequestStartLogs(ctx, logRecordCount)

	marshalStart := time.Now()
	buf, err := e.logsMarshaler.MarshalLogs(ld)
	if err != nil {
		e.telemetry.recordFailure(ctx, "marshal", err)
		return err
	}
	e.telemetry.recordMarshal(ctx, len(buf), time.Since(marshalStart))

	err = e.appendAndAwait(ctx, buf)
	e.telemetry.recordDurableWaitLogs(ctx, time.Since(start), err, logRecordCount, len(buf))
	return err
}

// appendAndAwait hands the marshaled payload to the producer and waits for it
// to become durable. Shared by ConsumeMetrics and ConsumeLogs.
func (e *openDataExporter) appendAndAwait(ctx context.Context, payload []byte) error {
	e.mu.RLock()
	producer := e.producer
	e.mu.RUnlock()
	if producer == nil {
		err := errExporterNotStarted
		e.telemetry.recordFailure(ctx, "start", err)
		return err
	}

	enqueueStart := time.Now()
	handle, err := producer.Append([][]byte{payload}, e.metadata)
	e.telemetry.recordEnqueueWait(ctx, time.Since(enqueueStart))
	if err != nil {
		e.telemetry.recordFailure(ctx, "enqueue", err)
		return err
	}
	if err := handle.Watcher.AwaitDurable(ctx); err != nil {
		e.telemetry.recordFailure(ctx, "durable_wait", err)
		return err
	}
	return nil
}

func signalLabel(signalType uint8) string {
	switch signalType {
	case SignalTypeMetrics:
		return "metrics"
	case SignalTypeLogs:
		return "logs"
	default:
		return fmt.Sprintf("unknown(%d)", signalType)
	}
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

func compressionTypeFromString(value string) (buffer.CompressionType, error) {
	switch strings.ToLower(value) {
	case compressionNone:
		return buffer.CompressionNone, nil
	case compressionZstd:
		return buffer.CompressionZstd, nil
	default:
		return 0, fmt.Errorf("unsupported compression %q", value)
	}
}
