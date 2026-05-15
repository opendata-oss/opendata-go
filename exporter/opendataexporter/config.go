package opendataexporter

import (
	"fmt"
	"strings"
	"time"
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

	switch strings.ToLower(c.Compression) {
	case compressionNone, compressionZstd:
		return nil
	default:
		return fmt.Errorf("unsupported compression %q", c.Compression)
	}
}
