package buffer

import "time"

// FlushReason identifies why the buffer flushed the current batch.
type FlushReason string

const (
	// FlushReasonSize indicates that the batch flushed after reaching the size threshold.
	FlushReasonSize FlushReason = "size"
	// FlushReasonTime indicates that the batch flushed after reaching the time threshold.
	FlushReasonTime FlushReason = "time"
	// FlushReasonManual indicates that the batch flushed due to an explicit flush request.
	FlushReasonManual FlushReason = "manual"
	// FlushReasonShutdown indicates that the batch flushed during buffer shutdown.
	FlushReasonShutdown FlushReason = "shutdown"
)

// FlushStats describes a flushed batch.
type FlushStats struct {
	Inputs            int
	Entries           int
	UncompressedBytes int
	Age               time.Duration
}

// PipelineStage identifies a stage in the producer pipeline for
// `buffer.producer.queue_depth` labeling.
type PipelineStage string

// Pipeline stages, in producer order.
const (
	StageAppend  PipelineStage = "append"
	StageRotate  PipelineStage = "rotate"
	StageEncode  PipelineStage = "encode"
	StageUpload  PipelineStage = "upload"
	StageCommit  PipelineStage = "commit"
	StageResolve PipelineStage = "resolve"
)

// BatchOutcome labels `buffer.producer.batch_outcome` for the
// per-batch terminal state.
type BatchOutcome string

// Per-batch terminal outcomes.
const (
	OutcomeCommitted      BatchOutcome = "committed"
	OutcomeEncodeFailed   BatchOutcome = "encode_failed"
	OutcomeUploadFailed   BatchOutcome = "upload_failed"
	OutcomeManifestFailed BatchOutcome = "manifest_failed"
	OutcomeAbandoned      BatchOutcome = "abandoned"
)

// Observer receives buffer lifecycle events for observability.
//
// The hooks have no-op-friendly signatures so an implementation can
// leave any it doesn't need as empty bodies.
type Observer interface {
	// Batch-lifecycle hooks.
	OnAccepted()
	OnFlush(reason FlushReason, stats FlushStats, duration time.Duration, err error)
	OnStorePut(sizeBytes int, duration time.Duration, err error)
	OnManifestEnqueue(entries int, duration time.Duration, conflicts int, err error)

	// Pipeline metric hooks.

	// OnAppendChBlock reports the wall time `Append` /
	// `AppendContext` spent blocked on the `appendCh` send (zero
	// when no backpressure was in play).
	OnAppendChBlock(duration time.Duration)

	// OnWorkersBusy reports the current count of busy encoder /
	// uploader workers. Stage is StageEncode or StageUpload.
	// Emitted on each entry into and exit from a worker's per-batch
	// work body.
	OnWorkersBusy(stage PipelineStage, busy int)

	// OnEncodeDuration reports per-batch encode wall time. Fired
	// once per batch from the encoder.
	OnEncodeDuration(duration time.Duration, err error)

	// OnUploadDuration reports per-batch object-PUT wall time.
	// Distinct from OnStorePut; both fire from the uploader's PUT
	// emit site.
	OnUploadDuration(duration time.Duration, sizeBytes int, err error)

	// OnManifestAppendBatchSize reports how many ordinals were
	// coalesced into a single CAS round trip.
	OnManifestAppendBatchSize(size int)

	// OnManifestAppendDuration reports per-CAS manifest write
	// wall time.
	OnManifestAppendDuration(duration time.Duration, conflicts int, err error)

	// OnHeadOfLineBlock reports the wall time the committer waited
	// for the next-expected ordinal while later ordinals were
	// already present in `ready`. Zero in the common case where
	// completions arrive in order.
	OnHeadOfLineBlock(duration time.Duration)

	// OnBatchOutcome reports the per-batch terminal state. Fires
	// once per batch from the WatcherResolver after watchers
	// resolve.
	OnBatchOutcome(outcome BatchOutcome)

	// OnHalted reports the producer entering or leaving the halted
	// state (true = halted, false = healthy). Wired to the
	// committer's "retry budget exhausted" path.
	OnHalted(halted bool)

	// OnInflightBytes reports the current sum of bytes reserved
	// against `MaxInFlightBytes` after a release event. Emitted
	// once per terminal batch outcome from the WatcherResolver.
	OnInflightBytes(bytes int64)

	// OnInflightBatches reports the current count of batches
	// reserved against `MaxInFlightBatches` after a release event.
	// Emitted alongside OnInflightBytes.
	OnInflightBatches(batches int)

	// OnQueueDepth reports the current queue / in-flight count for
	// a pipeline stage, surfaced as `buffer.producer.queue_depth`.
	// Emitted at stage entry/exit transitions; uses channel lengths
	// as an eventually-consistent proxy for the queue depth.
	OnQueueDepth(stage PipelineStage, depth int)
}
