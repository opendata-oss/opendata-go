package ingest

import "time"

// Default configuration values for IngestorConfig.
const (
	DefaultDataPathPrefix    = "ingest"
	DefaultManifestPath      = "ingest/manifest"
	DefaultFlushInterval     = 100 * time.Millisecond
	DefaultFlushSizeBytes    = 64 * 1024 * 1024 // 64 MiB
	DefaultMaxBufferedInputs = 1000
)

// IngestorConfig controls where data batches and the queue manifest are stored,
// how often batches are flushed, and when backpressure is applied.
type IngestorConfig struct {
	// DataPathPrefix is the path prefix for data batch objects in object storage.
	DataPathPrefix string

	// ManifestPath is the path to the queue manifest in object storage.
	ManifestPath string

	// FlushInterval triggers a flush of the current batch when elapsed.
	FlushInterval time.Duration

	// FlushSizeBytes triggers a flush when the batch reaches this size.
	FlushSizeBytes int

	// MaxBufferedInputs is the capacity of the internal channel before
	// backpressure is applied.
	MaxBufferedInputs int

	// BatchCompression is the compression algorithm for data batches.
	BatchCompression CompressionType

	// Observer receives lifecycle events from the ingestor.
	Observer Observer
}

// DefaultIngestorConfig returns an IngestorConfig with sensible defaults.
func DefaultIngestorConfig() IngestorConfig {
	return IngestorConfig{
		DataPathPrefix:    DefaultDataPathPrefix,
		ManifestPath:      DefaultManifestPath,
		FlushInterval:     DefaultFlushInterval,
		FlushSizeBytes:    DefaultFlushSizeBytes,
		MaxBufferedInputs: DefaultMaxBufferedInputs,
		BatchCompression:  CompressionNone,
	}
}
