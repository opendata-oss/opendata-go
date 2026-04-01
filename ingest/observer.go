package ingest

import "time"

// FlushReason identifies why the ingestor flushed the current batch.
type FlushReason string

const (
	FlushReasonSize     FlushReason = "size"
	FlushReasonTime     FlushReason = "time"
	FlushReasonManual   FlushReason = "manual"
	FlushReasonShutdown FlushReason = "shutdown"
)

// FlushStats describes a flushed batch.
type FlushStats struct {
	Inputs            int
	Entries           int
	UncompressedBytes int
	Age               time.Duration
}

// Observer receives ingestor lifecycle events for observability.
type Observer interface {
	OnAccepted()
	OnFlush(reason FlushReason, stats FlushStats, duration time.Duration, err error)
	OnStorePut(sizeBytes int, duration time.Duration, err error)
	OnManifestEnqueue(entries int, duration time.Duration, conflicts int, err error)
}
