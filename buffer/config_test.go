package buffer

import (
	"testing"
	"time"
)

// TestDefaultProducerConfig_DefaultsToSingleFlight asserts that
// `DefaultProducerConfig` returns the pipelining knobs at values that
// give single-flight behavior, so the producer behaves serially until
// operators explicitly opt into pipelining.
func TestDefaultProducerConfig_DefaultsToSingleFlight(t *testing.T) {
	cfg := DefaultProducerConfig()

	// Core (non-pipelining) fields.
	if cfg.DataPathPrefix != DefaultDataPathPrefix {
		t.Errorf("DataPathPrefix = %q, want %q", cfg.DataPathPrefix, DefaultDataPathPrefix)
	}
	if cfg.ManifestPath != DefaultManifestPath {
		t.Errorf("ManifestPath = %q, want %q", cfg.ManifestPath, DefaultManifestPath)
	}
	if cfg.FlushInterval != DefaultFlushInterval {
		t.Errorf("FlushInterval = %v, want %v", cfg.FlushInterval, DefaultFlushInterval)
	}
	if cfg.FlushSizeBytes != DefaultFlushSizeBytes {
		t.Errorf("FlushSizeBytes = %d, want %d", cfg.FlushSizeBytes, DefaultFlushSizeBytes)
	}
	if cfg.MaxBufferedInputs != DefaultMaxBufferedInputs {
		t.Errorf("MaxBufferedInputs = %d, want %d", cfg.MaxBufferedInputs, DefaultMaxBufferedInputs)
	}
	if cfg.BatchCompression != CompressionNone {
		t.Errorf("BatchCompression = %v, want %v", cfg.BatchCompression, CompressionNone)
	}

	// Pipelining defaults: must give single-flight behavior.
	if cfg.EncodeConcurrency != 1 {
		t.Errorf("EncodeConcurrency = %d, want 1 (single-flight encode)", cfg.EncodeConcurrency)
	}
	if cfg.UploadConcurrency != 1 {
		t.Errorf("UploadConcurrency = %d, want 1 (single-flight upload)", cfg.UploadConcurrency)
	}
	if cfg.ManifestAppendBatchSize != 1 {
		t.Errorf("ManifestAppendBatchSize = %d, want 1 (one CAS per batch)", cfg.ManifestAppendBatchSize)
	}

	// Bound checks for the safety caps.
	if cfg.MaxInFlightBatches <= 0 {
		t.Errorf("MaxInFlightBatches must be positive, got %d", cfg.MaxInFlightBatches)
	}
	if cfg.MaxInFlightBytes <= 0 {
		t.Errorf("MaxInFlightBytes must be positive, got %d", cfg.MaxInFlightBytes)
	}
	if cfg.UploadMaxAttempts <= 0 {
		t.Errorf("UploadMaxAttempts must be positive, got %d", cfg.UploadMaxAttempts)
	}
	if cfg.ManifestMaxAttempts <= 0 {
		t.Errorf("ManifestMaxAttempts must be positive, got %d", cfg.ManifestMaxAttempts)
	}
	if cfg.UploadInitialBackoff <= 0 {
		t.Errorf("UploadInitialBackoff must be positive, got %v", cfg.UploadInitialBackoff)
	}
	if cfg.ManifestInitialBackoff <= 0 {
		t.Errorf("ManifestInitialBackoff must be positive, got %v", cfg.ManifestInitialBackoff)
	}

	// Sanity: backoff defaults are sub-second so a clean retry doesn't
	// stall the producer for an unreasonable time.
	if cfg.UploadInitialBackoff > time.Second {
		t.Errorf("UploadInitialBackoff = %v, want < 1s", cfg.UploadInitialBackoff)
	}
	if cfg.ManifestInitialBackoff > time.Second {
		t.Errorf("ManifestInitialBackoff = %v, want < 1s", cfg.ManifestInitialBackoff)
	}
}
