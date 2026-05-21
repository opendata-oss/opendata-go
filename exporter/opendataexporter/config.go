package opendataexporter

import (
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const (
	objectStoreTypeS3   = "s3"
	compressionNone     = "none"
	compressionZstd     = "zstd"
	defaultFlushSizeMiB = 1024 * 1024
)

// ObjectStoreConfig configures the object storage backend used by the exporter.
type ObjectStoreConfig struct {
	Type     string `mapstructure:"type"`
	Bucket   string `mapstructure:"bucket"`
	Region   string `mapstructure:"region"`
	Endpoint string `mapstructure:"endpoint"`
}

// Config configures the OpenData exporter.
type Config struct {
	ObjectStore    ObjectStoreConfig `mapstructure:"object_store"`
	DataPathPrefix string            `mapstructure:"data_path_prefix"`
	ManifestPath   string            `mapstructure:"manifest_path"`
	FlushInterval  time.Duration     `mapstructure:"flush_interval"`
	FlushSizeBytes int               `mapstructure:"flush_size_bytes"`
	Compression    string            `mapstructure:"compression"`

	// UploadConcurrency caps the number of object_store upload workers
	// running in parallel. Forwarded to `buffer.ProducerConfig.UploadConcurrency`
	// at Start; the producer default is 1 (serial upload).
	UploadConcurrency int `mapstructure:"upload_concurrency"`

	// EncodeConcurrency caps the number of encoder workers (OTLP →
	// opendata batch format). Forwarded to
	// `buffer.ProducerConfig.EncodeConcurrency` at Start; the
	// producer default is 1 (serial encode — pre-Phase-3 behavior).
	// Calibrate to the host pod's CPU limit for CPU-bound workloads.
	EncodeConcurrency int `mapstructure:"encode_concurrency"`

	// MaxInFlightBatches caps the count of batches in the producer
	// pipeline (accumulator + encoded + uploading + committing).
	// Forwarded to `buffer.ProducerConfig.MaxInFlightBatches`. The
	// byte budget is the binding constraint; this is a secondary
	// safety cap. Producer default is 64.
	MaxInFlightBatches int `mapstructure:"max_inflight_batches"`

	// MaxInFlightBytes caps the total bytes held by the producer
	// pipeline. Forwarded to `buffer.ProducerConfig.MaxInFlightBytes`.
	// Producer default is 256 MiB. Size against the host pod's
	// memory budget and the network bandwidth-delay product.
	MaxInFlightBytes int `mapstructure:"max_inflight_bytes"`

	// ManifestAppendBatchSize is the maximum number of ready
	// ordinals the ManifestCommitter coalesces into a single
	// PutIfMatch call. Forwarded to
	// `buffer.ProducerConfig.ManifestAppendBatchSize`. Producer
	// default is 1 (one batch per CAS, pre-Phase-3 behavior); 16
	// is the post-sweep production guess. Coalescing trades a
	// small batch-level latency increase for ~N× lower per-batch
	// manifest CAS cost — the dominant chunk of durable_wait
	// when upload concurrency is healthy.
	ManifestAppendBatchSize int `mapstructure:"manifest_append_batch_size"`

	// SendingQueue wraps the exporter's ConsumeLogs/Metrics with the
	// standard OTel exporterhelper queue. Decouples the receiver's
	// HTTP response from the producer's AwaitDurable wait — the
	// receiver ack happens after the request is queued, not after
	// the batch is durable. NumConsumers determines how many
	// concurrent push calls run, which is the dominant lever for
	// throughput when the per-call producer flush is the bottleneck.
	// Default (NewDefaultQueueConfig): enabled, NumConsumers=10,
	// QueueSize=1000, BlockOnOverflow=false.
	SendingQueue configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`

	// Timeout is the per-call ceiling that exporterhelper applies to
	// ConsumeLogs/Metrics. Crossing it cancels the in-flight call and
	// drops the batch with "context deadline exceeded". Standard
	// exporterhelper default (NewDefaultTimeoutConfig) is 5s, which is
	// too tight under high-concurrency manifest CAS contention — the
	// p99.5 of `opendataexporter_durable_wait_duration_seconds` ran
	// 2-3s in row 8.4 validation, with bursts past 5s when CH merge
	// pressure spiked. Configure explicitly per cell. A value of 0
	// preserves exporterhelper's 5s default; any positive duration is
	// passed verbatim to exporterhelper.WithTimeout.
	Timeout time.Duration `mapstructure:"timeout"`
}

// Validate checks whether the exporter configuration is usable.
func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("config must not be nil")
	}

	if c.ObjectStore.Type == "" {
		return fmt.Errorf("object_store.type is required")
	}
	if c.ObjectStore.Type != objectStoreTypeS3 {
		return fmt.Errorf("unsupported object_store.type %q", c.ObjectStore.Type)
	}
	if c.ObjectStore.Bucket == "" {
		return fmt.Errorf("object_store.bucket is required")
	}
	if c.ObjectStore.Region == "" {
		return fmt.Errorf("object_store.region is required")
	}
	if c.DataPathPrefix == "" {
		return fmt.Errorf("data_path_prefix is required")
	}
	if c.ManifestPath == "" {
		return fmt.Errorf("manifest_path is required")
	}
	if c.FlushInterval <= 0 {
		return fmt.Errorf("flush_interval must be greater than zero")
	}
	if c.FlushSizeBytes <= 0 {
		return fmt.Errorf("flush_size_bytes must be greater than zero")
	}
	if c.UploadConcurrency < 1 {
		return fmt.Errorf("upload_concurrency must be at least 1 (got %d)", c.UploadConcurrency)
	}
	if c.EncodeConcurrency < 1 {
		return fmt.Errorf("encode_concurrency must be at least 1 (got %d)", c.EncodeConcurrency)
	}
	if c.MaxInFlightBatches < 1 {
		return fmt.Errorf("max_inflight_batches must be at least 1 (got %d)", c.MaxInFlightBatches)
	}
	if c.MaxInFlightBytes < 1 {
		return fmt.Errorf("max_inflight_bytes must be at least 1 (got %d)", c.MaxInFlightBytes)
	}
	if c.ManifestAppendBatchSize < 1 {
		return fmt.Errorf("manifest_append_batch_size must be at least 1 (got %d)", c.ManifestAppendBatchSize)
	}
	if c.SendingQueue.HasValue() {
		if err := c.SendingQueue.Get().Validate(); err != nil {
			return fmt.Errorf("sending_queue: %w", err)
		}
	}
	if c.Timeout < 0 {
		return fmt.Errorf("timeout must be >= 0 (got %s); 0 inherits exporterhelper default", c.Timeout)
	}

	switch strings.ToLower(c.Compression) {
	case compressionNone, compressionZstd:
		return nil
	default:
		return fmt.Errorf("unsupported compression %q", c.Compression)
	}
}
