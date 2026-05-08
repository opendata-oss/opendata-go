package buffer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/opendata-oss/opendata-go/objstore"
)

// DurabilityWatcher allows callers to check or wait for a batch to be durably flushed.
type DurabilityWatcher struct {
	mu     sync.Mutex
	result error
	done   bool
	ch     chan struct{}
}

func newDurabilityWatcher() *DurabilityWatcher {
	return &DurabilityWatcher{ch: make(chan struct{})}
}

// Result returns the flush outcome if the batch has been flushed, or (nil, false)
// if the flush has not completed yet.
func (w *DurabilityWatcher) Result() (bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.done {
		return false, nil
	}
	return true, w.result
}

// AwaitDurable blocks until the batch containing this write has been durably flushed.
func (w *DurabilityWatcher) AwaitDurable(ctx context.Context) error {
	select {
	case <-w.ch:
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.result
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *DurabilityWatcher) resolve(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return
	}
	w.result = err
	w.done = true
	close(w.ch)
}

// WriteHandle is returned by Append and provides access to a DurabilityWatcher.
type WriteHandle struct {
	Watcher *DurabilityWatcher
}

type appendMessage struct {
	entries         [][]byte
	metadata        []byte
	ingestionTimeMs int64
	watcher         *DurabilityWatcher
	// byteCost is the budget reservation made at AppendContext enqueue
	// (`sum(len(entries)) + len(metadata)`). The rotator sums these
	// into the emitted batch's `byteCost`; the WatcherResolver
	// releases that batch total back to the budget on terminal
	// outcome. F1 of Phase 3 rev-2 review.
	byteCost int64
}

type flushMessage struct {
	result chan error
}

type batchAccumulator struct {
	entries   [][]byte
	metadata  []QueueMetadata
	watchers  []*DurabilityWatcher
	sizeBytes int
	// byteCost is the sum of per-message reservations from
	// `AppendContext`. The rotator carries this onto the emitted
	// `pendingBatch` so the WatcherResolver can release it back to
	// the budget on terminal outcome. F1 of Phase 3 rev-2 review.
	byteCost  int64
	startedAt time.Time
	started   bool
}

func (b *batchAccumulator) add(msg *appendMessage) {
	startIndex := uint32(len(b.entries))
	b.entries = append(b.entries, msg.entries...)
	b.metadata = append(b.metadata, QueueMetadata{
		StartIndex:      startIndex,
		IngestionTimeMs: msg.ingestionTimeMs,
		Payload:         msg.metadata,
	})
	b.watchers = append(b.watchers, msg.watcher)

	for _, e := range msg.entries {
		b.sizeBytes += len(e)
	}
	b.sizeBytes += len(msg.metadata)
	b.byteCost += msg.byteCost

	if !b.started {
		b.startedAt = time.Now()
		b.started = true
	}
}

func (b *batchAccumulator) isEmpty() bool {
	return len(b.entries) == 0
}

func (b *batchAccumulator) reset() (entries [][]byte, metadata []QueueMetadata, watchers []*DurabilityWatcher, byteCost int64) {
	entries = b.entries
	metadata = b.metadata
	watchers = b.watchers
	byteCost = b.byteCost

	b.entries = nil
	b.metadata = nil
	b.watchers = nil
	b.sizeBytes = 0
	b.byteCost = 0
	b.started = false
	return entries, metadata, watchers, byteCost
}

func (b *batchAccumulator) stats() FlushStats {
	return FlushStats{
		Inputs:            len(b.metadata),
		Entries:           len(b.entries),
		UncompressedBytes: b.sizeBytes,
		Age:               time.Since(b.startedAt),
	}
}

// pendingBatch is the unit of work the Rotator emits to the Encoder
// pool (§3.S1 → §3.S2). Encoders consume `pendingBatch` and produce
// `encodedBatch` after running EncodeBatch.
//
// Phase 3.3 of the producer pipelining design (Migration Plan step 4)
// introduced the two-stage layout (Rotator → fused Uploader). Phase
// 3.4 (Migration Plan step 5; this commit) splits the Encoder out so
// the post-rotator pipeline is now Rotator → Encoder pool → Uploader.
// 3.5 splits the ManifestCommitter (§3.S4); 3.6 splits the
// WatcherResolver (§3.S5).
type pendingBatch struct {
	ordinal  uint64
	entries  [][]byte
	metadata []QueueMetadata
	watchers []*DurabilityWatcher
	stats    FlushStats
	reason   FlushReason
	// flushAck is non-nil only for batches emitted in response to a
	// Producer.Flush call. The Uploader sends the per-batch result
	// to this channel after the watchers are resolved, so Flush can
	// return the durable outcome to its caller.
	flushAck chan error
	// byteCost is the sum of `AppendContext`-time reservations for
	// the messages in this batch. Released by the WatcherResolver
	// on terminal outcome. F1 of Phase 3 rev-2 review.
	byteCost int64
}

// encodedBatch is the unit of work the Encoder pool emits to the
// Uploader. Carries the encoded payload, the deterministic location
// (computed from runID + ordinal per §3.S2), and a `pipelineStartedAt`
// timestamp the Uploader uses for OnFlush observer reporting.
type encodedBatch struct {
	ordinal           uint64
	payload           []byte
	metadata          []QueueMetadata
	watchers          []*DurabilityWatcher
	stats             FlushStats
	reason            FlushReason
	flushAck          chan error
	location          string // <data_path_prefix>/<runID>/<ordinal:016x>
	size              int    // len(payload)
	pipelineStartedAt time.Time
	byteCost          int64 // budget reservation carried for release on terminal outcome
}

// Producer accepts opaque byte entries, batches them, and flushes to object
// storage on size or time thresholds.
type Producer struct {
	enqueuer *manifestEnqueuer
	store    objstore.ObjectStore
	config   ProducerConfig

	// runID is generated once when the producer starts and is constant
	// for the producer's lifetime. Used as the per-process prefix in
	// the data-object location so two restarted producers cannot
	// collide on a path even if they pick the same ordinals. See
	// design §3.S2.
	runID string

	appendCh chan *appendMessage
	flushCh  chan *flushMessage
	// pendingCh carries pending batches from the Rotator to the
	// Encoder pool. Unbuffered: with EncodeConcurrency=1 the Rotator
	// blocks on send until the Encoder picks the batch up, preserving
	// the pre-Phase-3 single-flight throughput. With higher
	// EncodeConcurrency (opt-in), one of the N encoders picks up
	// each batch as workers free up.
	pendingCh chan *pendingBatch
	// encodedCh carries encoded batches from the Encoder pool to the
	// Uploader. Unbuffered: with UploadConcurrency=1 the Encoder
	// blocks on send until the Uploader picks up.
	encodedCh chan *encodedBatch
	// uploadCompletionCh carries upload outcomes (PUT result + the
	// encoded batch's metadata) from the Uploader to the
	// ManifestCommitter. Buffered to `MaxInFlightBatches` so a slow
	// CAS round trip lets newly-completed uploads accumulate; the
	// committer gathers all available completions before each drain,
	// which is what makes `ManifestAppendBatchSize > 1` actually
	// coalesce in the common case where uploads finish in roughly
	// arrival order. Without buffering + gather, in-order completions
	// would each trigger their own CAS regardless of batch size. See
	// design §Test Plan ("32 ready ordinals → exactly 2 CAS calls").
	uploadCompletionCh chan *uploadCompletion
	// resolverCh carries final per-batch outcomes from the Encoder
	// (encode failures) and ManifestCommitter (commit success / PUT
	// failure / CAS failure) to the WatcherResolver. The resolver
	// calls OnFlush, resolves watchers, and signals flushAck —
	// freeing the committer to move on to the next CAS without
	// waiting for AwaitDurable consumers. Phase 3.6 (Migration Plan
	// step 7).
	resolverCh chan *resolverItem

	rotatorDone      chan struct{}
	encoderPoolDone  chan struct{}
	uploaderPoolDone chan struct{}
	committerDone    chan struct{}
	resolverDone     chan struct{}
	closeOnce        sync.Once

	// shutdownCh is closed when Close starts, signaling internal
	// budget waits to give up early instead of blocking forever
	// on capacity that won't return. F1 of Phase 3 rev-2 review.
	shutdownCh chan struct{}

	// shutdownCtx is the parent context for all internal store calls
	// (uploader Put + manifest Get/PutIfMatch). cancelShutdown is
	// called by Close when the caller-supplied ctx fires; it aborts
	// every in-flight store call so the pipeline can drain quickly
	// instead of blocking on context.Background()-rooted I/O.
	// F4 of Phase 3 rev-2 review.
	shutdownCtx    context.Context
	cancelShutdown context.CancelFunc

	// budget enforces MaxInFlightBytes and MaxInFlightBatches per
	// design §Backpressure. Reservations are made in AppendContext
	// (bytes) and the rotator emit (batch slot); both released by
	// the WatcherResolver on terminal outcome.
	budget *producerBudget

	// Phase 3.7 metrics state (atomic counters for `workers_busy`).
	encodeWorkersBusy atomic.Int64
	uploadWorkersBusy atomic.Int64

	// halted marks the producer as terminally failed (manifest CAS
	// retry budget exhausted on a non-CAS-conflict error). Once set,
	// AppendContext returns ErrProducerHalted immediately and the
	// committer resolves every subsequent batch with the same error
	// without attempting a CAS. F2 of Phase 3 rev-2 review.
	halted       atomic.Bool
	haltedNotify sync.Once // guards a one-time OnHalted(true) emit
}

// uploadCompletion is what the Uploader signals to the
// ManifestCommitter once the data object PUT has completed (success
// or failure). The Committer holds these in an ordinal-indexed map
// and drains them in monotonic order, coalescing up to
// ManifestAppendBatchSize ready ordinals into one CAS round trip.
//
// Encode failures also flow through this channel as synthetic
// completions (eb constructed from the pendingBatch with a nil
// payload, putError = encode error, encodeFailed = true). Without
// this signal the committer's `next` cursor would block forever on
// the missing ordinal — F3 of the Phase 3 rev-3 review (design
// §Failure / "Skipping ordinals").
type uploadCompletion struct {
	eb           *encodedBatch
	putError     error // non-nil if the PUT or encode failed; the committer skips the ordinal but resolves watchers
	encodeFailed bool  // true if the failure happened in the encoder (not the uploader); chooses OutcomeEncodeFailed vs OutcomeUploadFailed
}

// resolverItem is what the ManifestCommitter sends to the
// WatcherResolver when a batch terminates: commit success, CAS
// failure, upload (PUT) failure, or encode failure routed through
// the committer as a synthetic completion (F3 of Phase 3 rev-3
// review). The resolver calls OnFlush, resolves all watchers with
// `err`, and signals flushAck.
//
// Phase 3.6 (Migration Plan step 7) splits this out of the
// ManifestCommitter so the committer can move on to the next CAS
// without waiting for AwaitDurable consumers to drain.
type resolverItem struct {
	reason            FlushReason
	stats             FlushStats
	pipelineStartedAt time.Time
	watchers          []*DurabilityWatcher
	flushAck          chan error
	err               error
	// outcome labels the batch's terminal state for the
	// `buffer.producer.batch_outcome` counter (design §Metrics).
	// Encoder sets OutcomeEncodeFailed; ManifestCommitter sets
	// OutcomeUploadFailed (PUT err), OutcomeManifestFailed (CAS
	// err), or OutcomeCommitted (success). OutcomeAbandoned is
	// used when the pipeline tears down before the batch terminates
	// (not currently emitted; reserved for future use).
	outcome BatchOutcome
	// byteCost is what the WatcherResolver releases back to the
	// budget on terminal outcome (commit or any failure). Always
	// matches the corresponding pendingBatch / encodedBatch byteCost.
	// F1 of Phase 3 rev-2 review.
	byteCost int64
}

// NewProducer creates a new Producer backed by the given ObjectStore.
//
// Spawns one Rotator goroutine + EncodeConcurrency Encoder goroutines
// + UploadConcurrency Uploader goroutines + one ManifestCommitter
// goroutine. See
// `plans/odb-high-throughput/phase03-producer-pipelining-design.md`
// for the pipeline contract.
func NewProducer(store objstore.ObjectStore, config ProducerConfig) *Producer {
	// Apply defaults for the Phase-3 fields that may be unset when
	// callers build a ProducerConfig literally (e.g. test code that
	// pre-dates these knobs). This keeps the budget + retry budgets
	// meaningful even for partial configs while still letting
	// callers opt into tighter caps explicitly. Production code
	// should call DefaultProducerConfig() and override.
	//
	// F6 of Phase 3 rev-3 review: previously DefaultUploadMaxAttempts
	// and DefaultManifestMaxAttempts (both 6) were declared but only
	// applied for callers using DefaultProducerConfig(). Callers
	// constructing ProducerConfig literals silently got the local
	// "unset → 1" fallback in putWithRetry / manifestCommitter,
	// which meant no retry budget at all.
	if config.MaxInFlightBytes < 1 {
		config.MaxInFlightBytes = DefaultMaxInFlightBytes
	}
	if config.MaxInFlightBatches < 1 {
		config.MaxInFlightBatches = DefaultMaxInFlightBatches
	}
	if config.UploadMaxAttempts < 1 {
		config.UploadMaxAttempts = DefaultUploadMaxAttempts
	}
	if config.UploadInitialBackoff <= 0 {
		config.UploadInitialBackoff = DefaultUploadInitialBackoff
	}
	if config.ManifestMaxAttempts < 1 {
		config.ManifestMaxAttempts = DefaultManifestMaxAttempts
	}
	if config.ManifestInitialBackoff <= 0 {
		config.ManifestInitialBackoff = DefaultManifestInitialBackoff
	}
	enqueuer := newManifestEnqueuer(store, config.ManifestPath)
	completionBuf := config.MaxInFlightBatches
	if completionBuf < 1 {
		completionBuf = 1
	}
	p := &Producer{
		enqueuer:           enqueuer,
		store:              store,
		config:             config,
		runID:              ulid.Make().String(),
		appendCh:           make(chan *appendMessage, config.MaxBufferedInputs),
		flushCh:            make(chan *flushMessage),
		pendingCh:          make(chan *pendingBatch),
		encodedCh:          make(chan *encodedBatch),
		uploadCompletionCh: make(chan *uploadCompletion, completionBuf),
		resolverCh:         make(chan *resolverItem),
		rotatorDone:        make(chan struct{}),
		encoderPoolDone:    make(chan struct{}),
		uploaderPoolDone:   make(chan struct{}),
		committerDone:      make(chan struct{}),
		resolverDone:       make(chan struct{}),
		shutdownCh:         make(chan struct{}),
		budget:             newProducerBudget(config.MaxInFlightBytes, config.MaxInFlightBatches),
	}
	p.shutdownCtx, p.cancelShutdown = context.WithCancel(context.Background())
	go p.rotator()
	go p.encoderPool()
	go p.uploaderPool()
	go p.manifestCommitter()
	go p.watcherResolver()
	// Supervisor goroutine: closes resolverCh after the committer
	// (the only sender) exits. The committer is downstream of the
	// encoder pool and uploader pool, so its exit implies upstream
	// stages have already drained. F3 of Phase 3 rev-3 review
	// removed the encoder→resolver direct path, leaving the
	// committer as the single resolver sender.
	go func() {
		<-p.committerDone
		close(p.resolverCh)
	}()
	return p
}

// rotator drains appendCh, manages the open accumulator, assigns
// monotonic ordinals at rotation time, and emits pending batches to
// pendingCh on size / time / manual-flush / shutdown triggers.
//
// Single goroutine: ordinals are strictly monotonic by construction
// (no other goroutine increments the counter). See design §3.S1.
//
// Lifecycle: when appendCh closes, the rotator emits any open
// accumulator as a final FlushReasonShutdown batch (or skips if
// empty), closes pendingCh (signaling the encoder pool to drain
// and exit), and signals rotatorDone.
func (p *Producer) rotator() {
	defer close(p.rotatorDone)
	defer close(p.pendingCh)

	batch := &batchAccumulator{}
	var ordinal uint64

	emit := func(reason FlushReason, flushAck chan error) {
		if batch.isEmpty() {
			// For a flush request against an empty accumulator,
			// signal success immediately without sending downstream.
			// Preserves the pre-Phase-3 behavior of `Flush`
			// returning nil for an empty buffer.
			if flushAck != nil {
				flushAck <- nil
			}
			return
		}
		// Acquire one MaxInFlightBatches slot before the batch
		// becomes pendingBatch. The slot is released by the
		// WatcherResolver alongside the byte release. Returns
		// ErrShutdown only if Close starts mid-wait, in which
		// case the rotator drops the batch (its watchers will be
		// resolved with ErrShutdown by the resolver via the close
		// cascade) and exits. F1 of Phase 3 rev-2 review.
		if err := p.budget.acquireBatchSlot(p.shutdownCh); err != nil {
			// On shutdown, release the bytes the batch was holding
			// since no resolver-side release will run for it.
			p.budget.release(batch.byteCost, 0)
			_, _, watchers, _ := batch.reset()
			for _, dw := range watchers {
				dw.resolve(ErrShutdown)
			}
			if flushAck != nil {
				flushAck <- ErrShutdown
			}
			return
		}
		stats := batch.stats()
		entries, metadata, watchers, byteCost := batch.reset()
		// Reconcile the per-message byte reservation to the
		// accumulator's actual framed byte cost: per-entry length
		// prefix (batchEntryLenSize bytes per entry) + per-batch
		// footer (batchFooterSize bytes). Per design §Backpressure
		// step (b): "the reservation is reconciled to the
		// accumulator's actual byte count (entries + metadata +
		// buffer framing)". Without this, many tiny records
		// undercount the budget and may blow MaxInFlightBytes
		// during encoding (F2 of Phase 3 rev-3 review).
		framingExtra := int64(batchEntryLenSize)*int64(len(entries)) + int64(batchFooterSize)
		framedCost := byteCost + framingExtra
		p.budget.addReservation(framingExtra)
		pb := &pendingBatch{
			ordinal:  ordinal,
			entries:  entries,
			metadata: metadata,
			watchers: watchers,
			stats:    stats,
			reason:   reason,
			flushAck: flushAck,
			byteCost: framedCost,
		}
		ordinal++
		// Blocks until an encoder picks it up. While blocked, this
		// goroutine isn't draining appendCh either, so backpressure
		// propagates through appendCh to Append callers — matching
		// the pre-Phase-3 single-goroutine behavior at default
		// concurrency.
		p.pendingCh <- pb
		if p.config.Observer != nil {
			p.config.Observer.OnQueueDepth(StageEncode, len(p.pendingCh))
		}
	}

	var timer *time.Timer
	resetTimer := func() {
		if timer != nil {
			timer.Stop()
		}
		if batch.started {
			remaining := time.Until(batch.startedAt.Add(p.config.FlushInterval))
			if remaining <= 0 {
				remaining = 0
			}
			timer = time.NewTimer(remaining)
		} else {
			timer = nil
		}
	}

	for {
		resetTimer()

		var timerCh <-chan time.Time
		if timer != nil {
			timerCh = timer.C
		}

		select {
		case msg, ok := <-p.appendCh:
			if !ok {
				// Channel closed — emit any remaining accumulator
				// as a final shutdown batch, then exit. The deferred
				// close(p.readyCh) signals the uploader to drain
				// the channel (now empty after the emit) and exit.
				emit(FlushReasonShutdown, nil)
				return
			}
			batch.add(msg)
			if batch.sizeBytes >= p.config.FlushSizeBytes {
				emit(FlushReasonSize, nil)
			}
		case fm := <-p.flushCh:
			// Drain any pending append messages into the accumulator
			// before emitting, so the flush captures everything
			// submitted before this Flush call.
			drainAppendChInto(p.appendCh, batch)
			emit(FlushReasonManual, fm.result)
		case <-timerCh:
			emit(FlushReasonTime, nil)
		}
	}
}

// drainAppendChInto greedily moves any messages currently sitting in
// the channel into the accumulator without blocking.
func drainAppendChInto(appendCh chan *appendMessage, batch *batchAccumulator) {
	for {
		select {
		case msg, ok := <-appendCh:
			if !ok {
				return
			}
			batch.add(msg)
		default:
			return
		}
	}
}

// encoderPool spawns EncodeConcurrency encoder goroutines and closes
// encodedCh when all of them have exited. See design §3.S2.
//
// The supervisor pattern is used because multiple encoders read from
// pendingCh and a single channel close is needed once they're all
// done. A WaitGroup gates close(encodedCh) on all encoders exiting.
func (p *Producer) encoderPool() {
	defer close(p.encoderPoolDone)
	defer close(p.encodedCh)

	n := p.config.EncodeConcurrency
	if n < 1 {
		n = 1
	}

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			p.encoder()
		}()
	}
	wg.Wait()
}

// encoder is one worker in the encoder pool. Reads pendingBatch
// values from pendingCh, runs EncodeBatch, computes the deterministic
// location (`<data_path_prefix>/<runID>/<ordinal:016x>`), and emits
// encodedBatch values to encodedCh. See design §3.S2.
//
// Failure: encode errors fail the batch immediately — watchers and
// flushAck are resolved with the encode error here, OnFlush fires
// with the (very short) elapsed time, and the batch is dropped (no
// encodedCh emit, no manifest entry). The pipeline continues with
// later batches, matching the design's "pipeline continues with
// later batches" rule.
func (p *Producer) encoder() {
	for pb := range p.pendingCh {
		pipelineStartedAt := time.Now()
		busy := p.encodeWorkersBusy.Add(1)
		if p.config.Observer != nil {
			p.config.Observer.OnWorkersBusy(StageEncode, int(busy))
		}
		encodeStart := time.Now()
		payload, err := EncodeBatch(pb.entries, p.config.BatchCompression)
		busy = p.encodeWorkersBusy.Add(-1)
		if p.config.Observer != nil {
			p.config.Observer.OnEncodeDuration(time.Since(encodeStart), err)
			p.config.Observer.OnWorkersBusy(StageEncode, int(busy))
		}
		if err != nil {
			// Encode failure: route through the ManifestCommitter
			// as a synthetic completion so the committer can
			// advance its `next` cursor past this ordinal. Without
			// this, the committer would block forever on the
			// missing ordinal and any later batches would stack
			// up in `ready[]` — F3 of Phase 3 rev-3 review.
			//
			// The committer's `failed` path resolves the batch's
			// watchers via the WatcherResolver (with
			// OutcomeEncodeFailed when uc.encodeFailed is set),
			// releases the byte budget, and skips the ordinal in
			// the CAS group.
			eb := &encodedBatch{
				ordinal:           pb.ordinal,
				payload:           nil,
				metadata:          pb.metadata,
				watchers:          pb.watchers,
				stats:             pb.stats,
				reason:            pb.reason,
				flushAck:          pb.flushAck,
				location:          "",
				size:              0,
				pipelineStartedAt: pipelineStartedAt,
				byteCost:          pb.byteCost,
			}
			p.uploadCompletionCh <- &uploadCompletion{eb: eb, putError: err, encodeFailed: true}
			if p.config.Observer != nil {
				p.config.Observer.OnQueueDepth(StageCommit, len(p.uploadCompletionCh))
			}
			continue
		}
		eb := &encodedBatch{
			ordinal:           pb.ordinal,
			payload:           payload,
			metadata:          pb.metadata,
			watchers:          pb.watchers,
			stats:             pb.stats,
			reason:            pb.reason,
			flushAck:          pb.flushAck,
			location:          fmt.Sprintf("%s/%s/%016x", p.config.DataPathPrefix, p.runID, pb.ordinal),
			size:              len(payload),
			pipelineStartedAt: pipelineStartedAt,
			byteCost:          pb.byteCost,
		}
		// Blocks until uploader takes it. With UploadConcurrency=1
		// default this preserves single-flight throughput.
		p.encodedCh <- eb
		if p.config.Observer != nil {
			p.config.Observer.OnQueueDepth(StageUpload, len(p.encodedCh))
		}
	}
}

// uploaderPool spawns UploadConcurrency uploader goroutines and
// closes uploadCompletionCh when all of them have exited. See design
// §3.S3.
//
// With UploadConcurrency>1, uploads finish in arbitrary order — that
// is the whole point of the parallelism. The ManifestCommitter
// reorders completions back into ordinal order via its
// uploadCompletions map (§3.S4); the OOO-upload-completion test in
// `producer_test.go` (Phase 3.8) is the dedicated check for this
// reordering invariant.
func (p *Producer) uploaderPool() {
	defer close(p.uploaderPoolDone)
	defer close(p.uploadCompletionCh)

	n := p.config.UploadConcurrency
	if n < 1 {
		n = 1
	}

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			p.uploaderWorker()
		}()
	}
	wg.Wait()
}

// uploaderWorker is one worker in the uploader pool. Reads
// encodedBatch values from encodedCh, runs the data-object PUT
// (retried up to UploadMaxAttempts with exponential backoff per
// design §Failure → Upload failure), and signals the result to the
// ManifestCommitter via uploadCompletionCh. PUT success and PUT
// failure both flow through uploadCompletionCh — failures carry a
// non-nil putError so the committer can skip the ordinal in the
// manifest sequence and resolve the batch's watchers with the
// error.
//
// Retry strategy (F2 of Phase 3 rev-2 review): retry every error
// uniformly until UploadMaxAttempts is hit. The design distinguishes
// retryable (network/5xx/throttling) from non-retryable (4xx other
// than 429), but the objstore package does not yet categorize
// errors, so we retry uniformly. Non-retryable failures pay a
// modest extra-attempts cost rather than fail fast — acceptable
// because uploads are off the critical metadata path.
func (p *Producer) uploaderWorker() {
	for eb := range p.encodedCh {
		busy := p.uploadWorkersBusy.Add(1)
		if p.config.Observer != nil {
			p.config.Observer.OnWorkersBusy(StageUpload, int(busy))
		}
		err := p.putWithRetry(p.shutdownCtx, eb)
		busy = p.uploadWorkersBusy.Add(-1)
		if p.config.Observer != nil {
			p.config.Observer.OnWorkersBusy(StageUpload, int(busy))
		}
		// Always signal the committer — even on PUT failure — so
		// the committer can resolve the batch's watchers and skip
		// the ordinal in the manifest sequence.
		p.uploadCompletionCh <- &uploadCompletion{eb: eb, putError: err}
		if p.config.Observer != nil {
			p.config.Observer.OnQueueDepth(StageCommit, len(p.uploadCompletionCh))
		}
	}
}

// putWithRetry attempts up to `UploadMaxAttempts` PUTs of the
// encoded batch's payload, with exponential backoff starting at
// `UploadInitialBackoff`. Per-attempt OnStorePut + OnUploadDuration
// observer hooks fire on every attempt. Returns the last error wrapped
// as a storage error, or nil on success. Aborts early on shutdownCh
// fire, returning ErrShutdown.
func (p *Producer) putWithRetry(ctx context.Context, eb *encodedBatch) error {
	maxAttempts := p.config.UploadMaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	backoff := p.config.UploadInitialBackoff
	if backoff <= 0 {
		backoff = DefaultUploadInitialBackoff
	}

	var err error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		putStart := time.Now()
		err = p.store.Put(ctx, eb.location, eb.payload)
		putDuration := time.Since(putStart)
		if p.config.Observer != nil {
			p.config.Observer.OnStorePut(eb.size, putDuration, err)
			p.config.Observer.OnUploadDuration(putDuration, eb.size, err)
		}
		if err == nil {
			return nil
		}
		// Backoff before the next attempt, unless this was the last
		// attempt — in which case fall through to return the error.
		if attempt+1 < maxAttempts {
			select {
			case <-time.After(backoffFor(attempt, backoff)):
			case <-p.shutdownCh:
				return ErrShutdown
			}
		}
	}
	return storageErr(err.Error())
}

// backoffFor returns the exponential backoff duration for the given
// attempt index (0 = first retry). Doubles per attempt, capped at
// 10s. Pure function — same input gives same output, so tests can
// assert timing bounds.
func backoffFor(attempt int, initial time.Duration) time.Duration {
	const maxBackoff = 10 * time.Second
	d := initial
	for i := 0; i < attempt && d < maxBackoff; i++ {
		d *= 2
	}
	if d > maxBackoff {
		d = maxBackoff
	}
	return d
}

// manifestCommitter drains upload completions in monotonic ordinal
// order, coalesces up to ManifestAppendBatchSize ready ordinals into
// one CAS round trip, resolves watchers, and signals flush waiters.
// Single goroutine per design §3.S4 — manifest mutation is
// serialized through one owner.
//
// Ordinal ordering: the Uploader may send completions in any order
// (with UploadConcurrency>1, faster uploads finish first). The
// committer holds them in an ordinal-indexed map and drains
// contiguous runs starting from the next-expected ordinal. With
// UploadConcurrency=1 (Phase 3.5 default) completions arrive in
// order, so the map holds at most one item at a time.
//
// PUT-failure handling: an ordinal whose PUT failed is skipped in
// the manifest sequence (no entry written, no committed sequence
// assigned). Its watchers are resolved with the putError before the
// committer moves on. Ordinal monotonicity is preserved because
// the committer always advances its `next-expected` cursor by 1
// regardless of whether the ordinal contributed an entry to the
// CAS group.
//
// Coalescing: at ManifestAppendBatchSize=1 (Phase 3.5 default),
// every contiguous run of length 1 results in one CAS — same shape
// as pre-Phase-3 behavior. The structure (multi-item enqueueBatch +
// per-group watcher resolution + flushAck signaling) is exercised
// even at size=1.
func (p *Producer) manifestCommitter() {
	defer close(p.committerDone)

	type pendingResolution struct {
		uc      *uploadCompletion
		err     error // result to resolve watchers with (CAS err, putError, or nil on success)
		outcome BatchOutcome
	}

	next := uint64(0)
	ready := make(map[uint64]*uploadCompletion)
	batchSize := p.config.ManifestAppendBatchSize
	if batchSize < 1 {
		batchSize = 1
	}
	manifestMaxAttempts := p.config.ManifestMaxAttempts
	if manifestMaxAttempts < 1 {
		manifestMaxAttempts = 1
	}
	manifestBackoff := p.config.ManifestInitialBackoff
	if manifestBackoff <= 0 {
		manifestBackoff = DefaultManifestInitialBackoff
	}

	// halted is the local mirror of p.halted — once flipped, the
	// committer abandons the manifest, resolves the current group +
	// every batch in `ready` with ErrProducerHalted, and drains the
	// remaining uploadCompletionCh stream with the same error so
	// upstream goroutines don't get stuck. F2 of Phase 3 rev-2 review.
	halted := false

	enterHalted := func(reason error) {
		halted = true
		p.halted.Store(true)
		p.haltedNotify.Do(func() {
			if p.config.Observer != nil {
				p.config.Observer.OnHalted(true)
			}
		})
		_ = reason // kept as a parameter for future structured-error logging
	}

	// head-of-line block measurement: when the committer is waiting
	// for the next-expected ordinal but later ordinals are already
	// present in `ready`, record the start of the wait; on the next
	// drain that successfully advances `next`, emit the elapsed.
	// Zero in the common case (UploadConcurrency=1; completions
	// arrive in order).
	var holStart time.Time

	// resolve hands off the batch's outcome to the WatcherResolver
	// (Phase 3.6 — design §3.S5) so the committer can return to its
	// loop and start the next CAS without waiting for AwaitDurable
	// consumers.
	resolve := func(uc *uploadCompletion, err error, outcome BatchOutcome) {
		p.resolverCh <- &resolverItem{
			reason:            uc.eb.reason,
			stats:             uc.eb.stats,
			pipelineStartedAt: uc.eb.pipelineStartedAt,
			watchers:          uc.eb.watchers,
			flushAck:          uc.eb.flushAck,
			err:               err,
			outcome:           outcome,
			byteCost:          uc.eb.byteCost,
		}
	}

	// casWithRetry wraps enqueueBatch (which already retries
	// internally on ErrPreconditionFailed) with an outer retry on
	// other storage errors, bounded by ManifestMaxAttempts. Returns
	// the cumulative conflict count + the final error. F2 of
	// Phase 3 rev-2 review.
	casWithRetry := func(items []enqueueItem) (int, error) {
		var (
			totalConflicts int
			err            error
		)
		for attempt := 0; attempt < manifestMaxAttempts; attempt++ {
			var c int
			c, err = p.enqueuer.enqueueBatch(p.shutdownCtx, items)
			totalConflicts += c
			if err == nil {
				return totalConflicts, nil
			}
			if attempt+1 < manifestMaxAttempts {
				select {
				case <-time.After(backoffFor(attempt, manifestBackoff)):
				case <-p.shutdownCh:
					return totalConflicts, ErrShutdown
				}
			}
		}
		return totalConflicts, err
	}

	drain := func() {
		// Drain contiguous ordinals starting at `next`, building one
		// commit group of up to `batchSize` items. PUT-failed
		// ordinals are skipped from the CAS itself but still consume
		// a slot in the next-expected sequence (their watchers are
		// resolved with the putError outside the CAS group).
		for {
			var group []*uploadCompletion
			var failed []pendingResolution

			for len(group)+len(failed) < batchSize {
				uc, ok := ready[next]
				if !ok {
					break
				}
				delete(ready, next)
				next++
				if uc.putError != nil {
					outcome := OutcomeUploadFailed
					if uc.encodeFailed {
						outcome = OutcomeEncodeFailed
					}
					failed = append(failed, pendingResolution{uc: uc, err: uc.putError, outcome: outcome})
				} else {
					group = append(group, uc)
				}
			}

			if len(group) == 0 && len(failed) == 0 {
				return
			}

			var casErr error
			if len(group) > 0 {
				items := make([]enqueueItem, 0, len(group))
				for _, uc := range group {
					items = append(items, enqueueItem{
						Location: uc.eb.location,
						Metadata: uc.eb.metadata,
					})
				}
				manifestStart := time.Now()
				conflicts, err := casWithRetry(items)
				casDuration := time.Since(manifestStart)
				casErr = err
				if p.config.Observer != nil {
					totalMeta := 0
					for _, it := range items {
						totalMeta += len(it.Metadata)
					}
					p.config.Observer.OnManifestEnqueue(totalMeta, casDuration, conflicts, casErr)
					p.config.Observer.OnManifestAppendBatchSize(len(items))
					p.config.Observer.OnManifestAppendDuration(casDuration, conflicts, casErr)
				}
			}

			// On retry-exhausted CAS error (anything that isn't
			// shutdown), enter halted state: resolve current group +
			// failed-upload group + every remaining batch in `ready`
			// with ErrProducerHalted. The committer keeps reading
			// uploadCompletionCh after this so upstream uploaders
			// can drain, but treats every subsequent receive as a
			// halted resolution.
			if casErr != nil && !errors.Is(casErr, ErrShutdown) {
				enterHalted(casErr)
				for _, uc := range group {
					resolve(uc, ErrProducerHalted, OutcomeManifestFailed)
				}
				for _, pr := range failed {
					resolve(pr.uc, pr.err, pr.outcome)
				}
				for _, uc := range ready {
					resolve(uc, ErrProducerHalted, OutcomeAbandoned)
				}
				ready = make(map[uint64]*uploadCompletion)
				return
			}

			groupOutcome := OutcomeCommitted
			if casErr != nil {
				groupOutcome = OutcomeManifestFailed
			}
			for _, uc := range group {
				resolve(uc, casErr, groupOutcome)
			}
			for _, pr := range failed {
				resolve(pr.uc, pr.err, pr.outcome)
			}
		}
	}

	// gather is the coalescing trigger. After a blocking receive of
	// the first completion, gather drains every additional completion
	// already present in the buffered channel without blocking. The
	// effect: while the committer was busy on the previous CAS, all
	// completions that arrived behind it accumulate in the channel
	// buffer, and one `gather + drain` pair turns them into one CAS
	// group of up to `ManifestAppendBatchSize` ordinals. Without
	// `gather`, every completion would trigger its own CAS regardless
	// of batch size — the regression F3 of the rev-2 review.
	//
	// Returns true if the channel was closed during gather; the
	// caller does the post-close final drain.
	gather := func() bool {
		for {
			select {
			case more, ok := <-p.uploadCompletionCh:
				if !ok {
					return true
				}
				ready[more.eb.ordinal] = more
			default:
				return false
			}
		}
	}

	for {
		uc, ok := <-p.uploadCompletionCh
		if !ok {
			if !halted {
				drain()
			}
			return
		}
		// Halted-mode short-circuit: resolve every incoming batch
		// with ErrProducerHalted, no CAS attempted. Keeps the
		// uploaderPool unblocked so the pipeline can drain to close
		// even after a halt.
		if halted {
			resolve(uc, ErrProducerHalted, OutcomeAbandoned)
			continue
		}
		ready[uc.eb.ordinal] = uc

		closed := gather()

		// If we have items but `next` hasn't arrived, this is the
		// start of a head-of-line block.
		if _, hasNext := ready[next]; !hasNext && holStart.IsZero() {
			holStart = time.Now()
		}
		// Snapshot whether the next-expected ordinal is present
		// before we drain (drain advances `next` on success).
		_, willAdvance := ready[next]
		drain()
		if willAdvance && !holStart.IsZero() {
			if p.config.Observer != nil {
				p.config.Observer.OnHeadOfLineBlock(time.Since(holStart))
			}
			holStart = time.Time{}
		}

		if closed {
			// Channel closed mid-gather: any straggling ordinals are
			// already in `ready` (drained above). With holes from an
			// aborted pipeline, drain returns without committing them
			// and their watchers were never resolved — matching the
			// pre-Phase-3 partial-shutdown semantics.
			return
		}
	}
}

// watcherResolver consumes resolverItem values from resolverCh and
// performs the per-batch terminal work — OnFlush observer call,
// resolving every watcher with the batch's outcome, and (for
// Flush-triggered batches) signaling flushAck. See design §3.S5.
//
// Single goroutine. Splitting this out of the ManifestCommitter
// (Phase 3.5) frees the committer to start its next CAS as soon as
// the previous CAS returns, without waiting for AwaitDurable
// consumers to drain.
func (p *Producer) watcherResolver() {
	defer close(p.resolverDone)
	for ri := range p.resolverCh {
		if p.config.Observer != nil {
			p.config.Observer.OnFlush(ri.reason, ri.stats, time.Since(ri.pipelineStartedAt), ri.err)
			if ri.outcome != "" {
				p.config.Observer.OnBatchOutcome(ri.outcome)
			}
		}
		for _, dw := range ri.watchers {
			dw.resolve(ri.err)
		}
		if ri.flushAck != nil {
			ri.flushAck <- ri.err
		}
		// Release the budget reservation for this batch (one batch
		// slot + byteCost bytes). All terminal outcomes — commit
		// success, encode failure, upload failure, manifest failure —
		// flow through the resolver, so this is the single release
		// point. F1 of Phase 3 rev-2 review.
		p.budget.release(ri.byteCost, 1)
		if p.config.Observer != nil {
			usedBytes, usedBatches := p.budget.snapshot()
			p.config.Observer.OnInflightBytes(usedBytes)
			p.config.Observer.OnInflightBatches(int(usedBatches))
		}
	}
}

// Append submits entries and associated metadata for buffering.
// It returns a WriteHandle whose Watcher can be used to await durability.
// Applies backpressure when the internal buffer is full.
//
// Append blocks indefinitely on `appendCh` send when the byte budget
// is full. Callers that want a cancellable variant should use
// AppendContext (Phase 3.7, design rev 2 §Cancellation).
func (p *Producer) Append(entries [][]byte, metadata []byte) (*WriteHandle, error) {
	return p.AppendContext(context.Background(), entries, metadata)
}

// AppendContext is the cancellation-aware variant of Append. See
// design rev 2 §Cancellation: existing pre-Phase-3 callers stay on
// the context-free Append; new callers that want to bound the
// backpressure-block can pass a context here. After the message is
// enqueued, ctx cancellation does not affect the in-flight batch —
// the watcher's AwaitDurable(ctx) is the cancellation point for
// the durable wait.
//
// Backpressure: AppendContext reserves `sum(len(entries)) + len(metadata)`
// bytes against `MaxInFlightBytes` *before* the appendCh send (F1 of
// Phase 3 rev-2 review). The reservation is released by the
// WatcherResolver on the eventual terminal outcome of whichever batch
// includes this message. If `ctx` cancels during the byte wait, no
// reservation is held and no message is enqueued.
func (p *Producer) AppendContext(ctx context.Context, entries [][]byte, metadata []byte) (*WriteHandle, error) {
	if len(entries) == 0 {
		return nil, invalidInputErr("entries must not be empty")
	}
	if metadata == nil {
		metadata = []byte{}
	}
	// Halted-state short-circuit: once the manifest CAS retry budget
	// has been exhausted, all subsequent Appends fail immediately
	// per design §Failure → Manifest CAS failure. F2 of Phase 3 rev-2
	// review.
	if p.halted.Load() {
		return nil, ErrProducerHalted
	}

	byteCost := appendByteCost(entries, metadata)
	blockStart := time.Now()
	if err := p.budget.reserveBytes(ctx, byteCost, p.shutdownCh); err != nil {
		return nil, err
	}

	watcher := newDurabilityWatcher()
	msg := &appendMessage{
		entries:         entries,
		metadata:        metadata,
		ingestionTimeMs: time.Now().UnixMilli(),
		watcher:         watcher,
		byteCost:        byteCost,
	}

	select {
	case p.appendCh <- msg:
		if p.config.Observer != nil {
			p.config.Observer.OnAppendChBlock(time.Since(blockStart))
			p.config.Observer.OnAccepted()
			p.config.Observer.OnQueueDepth(StageAppend, len(p.appendCh))
		}
		return &WriteHandle{Watcher: watcher}, nil
	case <-ctx.Done():
		// The reservation was held but the message never entered the
		// pipeline; release it so a future Append can use the budget.
		p.budget.release(byteCost, 0)
		return nil, ctx.Err()
	case <-p.rotatorDone:
		p.budget.release(byteCost, 0)
		return nil, ErrShutdown
	}
}

// Flush forces a flush of the current batch and blocks until every
// in-flight batch has either committed or failed. Per design
// phase03-producer-pipelining-design.md §"Producer.Flush(ctx) blocks
// until every in-flight batch has either committed or failed.
// Forces a final manifest CAS even if the current commit-pending
// queue is below ManifestAppendBatchSize."
//
// Two-phase wait (F1 of Phase 3 rev-3 review):
//  1. Send the flushMessage and wait for the rotator's flushAck.
//     This ensures the open accumulator (if any) has been emitted as
//     a final batch under FlushReasonManual.
//  2. Wait for the budget's in-flight batch counter to drain to 0.
//     This catches batches that were already in flight (size/time
//     triggers) when Flush was called but had not yet terminated.
//
// Without phase 2, Flush would return as soon as the rotator's
// flush batch committed — which can happen before earlier-emitted
// size/time batches finish, because the manifest committer's strict
// ordinal ordering only fires after the entire commit chain
// completes. The empty-accumulator case (where the rotator signals
// flushAck immediately without emitting a batch) was the loudest
// failure mode: Flush would return nil with batches still uploading.
func (p *Producer) Flush(ctx context.Context) error {
	fm := &flushMessage{result: make(chan error, 1)}
	select {
	case p.flushCh <- fm:
	case <-p.rotatorDone:
		return ErrShutdown
	case <-ctx.Done():
		return ctx.Err()
	}
	var flushErr error
	select {
	case flushErr = <-fm.result:
	case <-p.resolverDone:
		// Pipeline torn down before the resolver delivered the
		// outcome — the flushed batch's watchers were never resolved
		// with a real result.
		return ErrShutdown
	case <-ctx.Done():
		return ctx.Err()
	}
	// Phase 2: wait for any other in-flight batches (size/time
	// triggers from before Flush was called) to terminate. Returns
	// ctx.Err() if the caller cancels mid-wait.
	if err := p.budget.waitDrained(ctx, p.shutdownCh); err != nil {
		return err
	}
	return flushErr
}

// ConflictRate returns the percentage of manifest writes that encountered a conflict.
func (p *Producer) ConflictRate() float64 {
	return p.enqueuer.conflictRate()
}

// Close flushes any remaining buffered entries and shuts down the producer.
//
// Honors `ctx` end-to-end: if it fires while waiting for any pipeline
// goroutine to drain, Close cancels the shared shutdown context (which
// aborts all in-flight store calls), closes the shutdown signal (which
// unblocks any internal budget/retry waits), and returns `ctx.Err()`.
// In-flight watchers resolve with the cancellation error as the
// pipeline cascade propagates it. Without this, a stuck store call
// would hang Close forever — F4 of Phase 3 rev-2 review.
func (p *Producer) Close(ctx context.Context) error {
	var err error
	p.closeOnce.Do(func() {
		// Best-effort graceful flush. If ctx is already canceled or
		// fires here, Flush returns the ctx error; we still proceed
		// to the shutdown path (which will use the same ctx).
		flushErr := p.Flush(ctx)

		close(p.appendCh)

		// Wait for every pipeline goroutine to exit, with ctx as the
		// ceiling. The waits are sequenced in dependency order — by
		// the time a downstream channel's owner exits, the upstream
		// stages have already drained.
		dones := []<-chan struct{}{
			p.rotatorDone,
			p.encoderPoolDone,
			p.uploaderPoolDone,
			p.committerDone,
			p.resolverDone,
		}
		for _, dc := range dones {
			select {
			case <-dc:
			case <-ctx.Done():
				// Force-abort: cancel all store calls and close the
				// shutdown signal so internal budget/retry waits
				// drop their channel selects. Goroutines exit in the
				// background; we return ctx.Err() so the caller
				// isn't blocked.
				p.cancelShutdown()
				select {
				case <-p.shutdownCh:
				default:
					close(p.shutdownCh)
				}
				err = ctx.Err()
				return
			}
		}
		err = flushErr
	})
	return err
}
