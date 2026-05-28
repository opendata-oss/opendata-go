package buffer

import "time"

// Default configuration values for ProducerConfig.
const (
	DefaultDataPathPrefix    = "ingest"
	DefaultManifestPath      = "ingest/manifest"
	DefaultFlushInterval     = 100 * time.Millisecond
	DefaultFlushSizeBytes    = 64 * 1024 * 1024 // 64 MiB
	DefaultMaxBufferedInputs = 1000

	// Pipelining defaults give single-flight encode + upload +
	// manifest CAS. Operators opt into pipelining by raising these.
	DefaultEncodeConcurrency       = 1
	DefaultUploadConcurrency       = 1
	DefaultMaxInFlightBatches      = 64
	DefaultMaxInFlightBytes        = 256 * 1024 * 1024 // 256 MiB
	DefaultManifestAppendBatchSize = 1
	DefaultUploadMaxAttempts       = 6
	DefaultUploadInitialBackoff    = 100 * time.Millisecond
	DefaultManifestMaxAttempts     = 6
	DefaultManifestInitialBackoff  = 100 * time.Millisecond
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

	// ---- Pipelining knobs. ----
	//
	// These control the rotator/encoder/uploader/committer/resolver
	// pipeline. The defaults give single-flight behavior, so a
	// ProducerConfig that leaves them unset behaves as a serial
	// producer.

	// EncodeConcurrency is the number of encoder workers. Default 1
	// (serial encode).
	EncodeConcurrency int

	// UploadConcurrency is the number of object_store upload workers.
	// Default 1 (serial upload).
	UploadConcurrency int

	// MaxInFlightBatches caps the count of batches in the pipeline
	// (queued accumulator entries plus encoded/uploading/committing
	// batches). Default 64. Secondary safety cap; the byte budget
	// is the binding constraint.
	MaxInFlightBatches int

	// MaxInFlightBytes caps the total bytes held by the pipeline,
	// reserved at appendCh enqueue and released at commit success.
	// Default 256 MiB.
	MaxInFlightBytes int

	// ManifestAppendBatchSize is the maximum number of ready ordinals
	// the ManifestCommitter coalesces into a single PutIfMatch call.
	// Default 1 (one batch per CAS). Raising it (e.g. to 16) trades
	// commit latency for fewer manifest writes under high throughput.
	ManifestAppendBatchSize int

	// UploadMaxAttempts is the per-batch retry budget for object_store
	// PUT failures. Default 6. Permanent failures (non-retryable
	// errors or budget-exhausted retries) cause the committer to
	// skip the ordinal and resolve the batch's watcher with the
	// error; subsequent ordinals are not blocked.
	UploadMaxAttempts int

	// UploadInitialBackoff is the first retry delay for upload
	// failures. Subsequent retries use exponential backoff with
	// jitter. Default 100 ms.
	UploadInitialBackoff time.Duration

	// ManifestMaxAttempts is the per-CAS retry budget for manifest
	// write failures (excluding ErrPreconditionFailed, which has
	// its own re-read+re-plan loop). Default 6. Exhaustion halts
	// the producer.
	ManifestMaxAttempts int

	// ManifestInitialBackoff is the first retry delay for manifest
	// write failures. Default 100 ms.
	ManifestInitialBackoff time.Duration
}

// DefaultProducerConfig returns a ProducerConfig with sensible defaults.
// Defaults give single-flight behavior; raise the pipelining knobs to
// opt into parallel encode/upload and manifest coalescing.
func DefaultProducerConfig() ProducerConfig {
	return ProducerConfig{
		DataPathPrefix:    DefaultDataPathPrefix,
		ManifestPath:      DefaultManifestPath,
		FlushInterval:     DefaultFlushInterval,
		FlushSizeBytes:    DefaultFlushSizeBytes,
		MaxBufferedInputs: DefaultMaxBufferedInputs,
		BatchCompression:  CompressionNone,

		EncodeConcurrency:       DefaultEncodeConcurrency,
		UploadConcurrency:       DefaultUploadConcurrency,
		MaxInFlightBatches:      DefaultMaxInFlightBatches,
		MaxInFlightBytes:        DefaultMaxInFlightBytes,
		ManifestAppendBatchSize: DefaultManifestAppendBatchSize,
		UploadMaxAttempts:       DefaultUploadMaxAttempts,
		UploadInitialBackoff:    DefaultUploadInitialBackoff,
		ManifestMaxAttempts:     DefaultManifestMaxAttempts,
		ManifestInitialBackoff:  DefaultManifestInitialBackoff,
	}
}
