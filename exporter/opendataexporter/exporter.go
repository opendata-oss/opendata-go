package opendataexporter

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/opendata-oss/opendata-go/ingest"
	"github.com/opendata-oss/opendata-go/objstore"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

var errExporterNotStarted = errors.New("opendata exporter not started")

type storeFactory func(context.Context, ObjectStoreConfig) (objstore.ObjectStore, error)

type openDataExporter struct {
	config       Config
	marshaler    *pmetric.ProtoMarshaler
	metadata     []byte
	storeFactory storeFactory

	mu       sync.RWMutex
	ingestor *ingest.Ingestor
}

func newOpenDataExporter(cfg *Config) *openDataExporter {
	return &openDataExporter{
		config:       *cfg,
		marshaler:    &pmetric.ProtoMarshaler{},
		metadata:     EncodeMetadata(SignalTypeMetrics, PayloadEncodingOTLP),
		storeFactory: newObjectStore,
	}
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

	e.ingestor = ingest.NewIngestor(store, ingestConfig)
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
	buf, err := e.marshaler.MarshalMetrics(md)
	if err != nil {
		return err
	}

	e.mu.RLock()
	ing := e.ingestor
	e.mu.RUnlock()
	if ing == nil {
		return errExporterNotStarted
	}

	handle, err := ing.Ingest([][]byte{buf}, e.metadata)
	if err != nil {
		return err
	}
	return handle.Watcher.AwaitDurable(ctx)
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
