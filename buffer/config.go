package buffer

import "time"

// Default configuration values for ProducerConfig.
const (
	DefaultDataPathPrefix    = "ingest"
	DefaultManifestPath      = "ingest/manifest"
	DefaultFlushInterval     = 100 * time.Millisecond
	DefaultFlushSizeBytes    = 64 * 1024 * 1024 // 64 MiB
	DefaultMaxBufferedInputs = 1000
)

// ProducerConfig controls where data batches and the queue manifest are stored,
// how often batches are flushed, and when backpressure is applied.
type ProducerConfig struct {
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

	// Observer receives lifecycle events from the buffer.
	Observer Observer
}

// DefaultProducerConfig returns a ProducerConfig with sensible defaults.
func DefaultProducerConfig() ProducerConfig {
	return ProducerConfig{
		DataPathPrefix:    DefaultDataPathPrefix,
		ManifestPath:      DefaultManifestPath,
		FlushInterval:     DefaultFlushInterval,
		FlushSizeBytes:    DefaultFlushSizeBytes,
		MaxBufferedInputs: DefaultMaxBufferedInputs,
		BatchCompression:  CompressionNone,
	}
}
