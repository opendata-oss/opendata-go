package buffer

import (
	"context"
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
}

type flushMessage struct {
	result chan error
}

type batchAccumulator struct {
	entries   [][]byte
	metadata  []QueueMetadata
	watchers  []*DurabilityWatcher
	sizeBytes int
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

	if !b.started {
		b.startedAt = time.Now()
		b.started = true
	}
}

func (b *batchAccumulator) isEmpty() bool {
	return len(b.entries) == 0
}

func (b *batchAccumulator) reset() ([][]byte, []QueueMetadata, []*DurabilityWatcher) {
	entries := b.entries
	metadata := b.metadata
	watchers := b.watchers

	b.entries = nil
	b.metadata = nil
	b.watchers = nil
	b.sizeBytes = 0
	b.started = false
	return entries, metadata, watchers
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
	// ManifestCommitter. Unbuffered: the Uploader blocks on send
	// until the Committer picks up. Phase 3.5 (Migration Plan step
	// 6).
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

	// Phase 3.7 metrics state (atomic counters for `workers_busy`).
	encodeWorkersBusy atomic.Int64
	uploadWorkersBusy atomic.Int64
}

// uploadCompletion is what the Uploader signals to the
// ManifestCommitter once the data object PUT has completed (success
// or failure). The Committer holds these in an ordinal-indexed map
// and drains them in monotonic order, coalescing up to
// ManifestAppendBatchSize ready ordinals into one CAS round trip.
type uploadCompletion struct {
	eb       *encodedBatch
	putError error // non-nil if the PUT failed; the committer skips the ordinal but resolves watchers
}

// resolverItem is what an upstream stage (Encoder on encode failure,
// or ManifestCommitter on commit success / commit failure / PUT
// failure) sends to the WatcherResolver. The resolver calls
// OnFlush, resolves all watchers with `err`, and signals flushAck.
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
}

// NewProducer creates a new Producer backed by the given ObjectStore.
//
// Spawns one Rotator goroutine + EncodeConcurrency Encoder goroutines
// + UploadConcurrency Uploader goroutines + one ManifestCommitter
// goroutine. See
// `plans/odb-high-throughput/phase03-producer-pipelining-design.md`
// for the pipeline contract.
func NewProducer(store objstore.ObjectStore, config ProducerConfig) *Producer {
	enqueuer := newManifestEnqueuer(store, config.ManifestPath)
	p := &Producer{
		enqueuer:           enqueuer,
		store:              store,
		config:             config,
		runID:              ulid.Make().String(),
		appendCh:           make(chan *appendMessage, config.MaxBufferedInputs),
		flushCh:            make(chan *flushMessage),
		pendingCh:          make(chan *pendingBatch),
		encodedCh:          make(chan *encodedBatch),
		uploadCompletionCh: make(chan *uploadCompletion),
		resolverCh:         make(chan *resolverItem),
		rotatorDone:        make(chan struct{}),
		encoderPoolDone:    make(chan struct{}),
		uploaderPoolDone:   make(chan struct{}),
		committerDone:      make(chan struct{}),
		resolverDone:       make(chan struct{}),
	}
	go p.rotator()
	go p.encoderPool()
	go p.uploaderPool()
	go p.manifestCommitter()
	go p.watcherResolver()
	// Supervisor goroutine: closes resolverCh once all upstream
	// senders (encoder pool + manifest committer) have exited. This
	// is the standard pattern for closing a fan-in channel with
	// multiple producers.
	go func() {
		<-p.encoderPoolDone
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
		stats := batch.stats()
		entries, metadata, watchers := batch.reset()
		pb := &pendingBatch{
			ordinal:  ordinal,
			entries:  entries,
			metadata: metadata,
			watchers: watchers,
			stats:    stats,
			reason:   reason,
			flushAck: flushAck,
		}
		ordinal++
		// Blocks until an encoder picks it up. While blocked, this
		// goroutine isn't draining appendCh either, so backpressure
		// propagates through appendCh to Append callers — matching
		// the pre-Phase-3 single-goroutine behavior at default
		// concurrency.
		p.pendingCh <- pb
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
			// Encode failure: hand off to the WatcherResolver
			// (Phase 3.6) instead of resolving inline. The
			// resolver calls OnFlush, resolves watchers, and
			// signals flushAck.
			p.resolverCh <- &resolverItem{
				reason:            pb.reason,
				stats:             pb.stats,
				pipelineStartedAt: pipelineStartedAt,
				watchers:          pb.watchers,
				flushAck:          pb.flushAck,
				err:               err,
				outcome:           OutcomeEncodeFailed,
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
		}
		// Blocks until uploader takes it. With UploadConcurrency=1
		// default this preserves single-flight throughput.
		p.encodedCh <- eb
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
// encodedBatch values from encodedCh, runs the data-object PUT,
// and signals the result to the ManifestCommitter via
// uploadCompletionCh. PUT success and PUT failure both flow
// through uploadCompletionCh — failures carry a non-nil putError
// so the committer can skip the ordinal in the manifest sequence
// and resolve the batch's watchers with the error.
func (p *Producer) uploaderWorker() {
	for eb := range p.encodedCh {
		busy := p.uploadWorkersBusy.Add(1)
		if p.config.Observer != nil {
			p.config.Observer.OnWorkersBusy(StageUpload, int(busy))
		}
		ctx := context.Background()
		putStart := time.Now()
		err := p.store.Put(ctx, eb.location, eb.payload)
		putDuration := time.Since(putStart)
		busy = p.uploadWorkersBusy.Add(-1)
		if p.config.Observer != nil {
			p.config.Observer.OnStorePut(eb.size, putDuration, err)
			p.config.Observer.OnUploadDuration(putDuration, eb.size, err)
			p.config.Observer.OnWorkersBusy(StageUpload, int(busy))
		}
		if err != nil {
			err = storageErr(err.Error())
		}
		// Always signal the committer — even on PUT failure — so
		// the committer can resolve the batch's watchers and skip
		// the ordinal in the manifest sequence.
		p.uploadCompletionCh <- &uploadCompletion{eb: eb, putError: err}
	}
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
		uc  *uploadCompletion
		err error // result to resolve watchers with (CAS err, putError, or nil on success)
	}

	next := uint64(0)
	ready := make(map[uint64]*uploadCompletion)
	batchSize := p.config.ManifestAppendBatchSize
	if batchSize < 1 {
		batchSize = 1
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
		}
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
					failed = append(failed, pendingResolution{uc: uc, err: uc.putError})
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
				ctx := context.Background()
				manifestStart := time.Now()
				var conflicts int
				conflicts, casErr = p.enqueuer.enqueueBatch(ctx, items)
				casDuration := time.Since(manifestStart)
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

			groupOutcome := OutcomeCommitted
			if casErr != nil {
				groupOutcome = OutcomeManifestFailed
			}
			for _, uc := range group {
				resolve(uc, casErr, groupOutcome)
			}
			for _, pr := range failed {
				resolve(pr.uc, pr.err, OutcomeUploadFailed)
			}
		}
	}

	for uc := range p.uploadCompletionCh {
		ready[uc.eb.ordinal] = uc
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
	}
	// uploadCompletionCh closed: drain any straggling ordinals (with
	// UploadConcurrency=1, ready is empty here since drain() runs
	// after each completion; with >1, there may be holes if the
	// upstream stages aborted mid-flight, in which case the
	// committer simply returns without committing the orphaned
	// ordinals — their watchers were never resolved, which signals
	// shutdown to AwaitDurable callers via the goroutine eventually
	// dropping. This matches the pre-Phase-3 partial-shutdown
	// semantics).
	drain()
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
func (p *Producer) AppendContext(ctx context.Context, entries [][]byte, metadata []byte) (*WriteHandle, error) {
	if len(entries) == 0 {
		return nil, invalidInputErr("entries must not be empty")
	}
	if metadata == nil {
		metadata = []byte{}
	}

	watcher := newDurabilityWatcher()
	msg := &appendMessage{
		entries:         entries,
		metadata:        metadata,
		ingestionTimeMs: time.Now().UnixMilli(),
		watcher:         watcher,
	}

	blockStart := time.Now()
	select {
	case p.appendCh <- msg:
		if p.config.Observer != nil {
			p.config.Observer.OnAppendChBlock(time.Since(blockStart))
			p.config.Observer.OnAccepted()
		}
		return &WriteHandle{Watcher: watcher}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.rotatorDone:
		return nil, ErrShutdown
	}
}

// Flush forces a flush of the current batch, blocking until it is durably written.
func (p *Producer) Flush(ctx context.Context) error {
	fm := &flushMessage{result: make(chan error, 1)}
	select {
	case p.flushCh <- fm:
	case <-p.rotatorDone:
		return ErrShutdown
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-fm.result:
		return err
	case <-p.resolverDone:
		// Pipeline torn down before the resolver delivered the
		// outcome — the flushed batch's watchers were never resolved
		// with a real result.
		return ErrShutdown
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ConflictRate returns the percentage of manifest writes that encountered a conflict.
func (p *Producer) ConflictRate() float64 {
	return p.enqueuer.conflictRate()
}

// Close flushes any remaining buffered entries and shuts down the producer.
func (p *Producer) Close(ctx context.Context) error {
	var err error
	p.closeOnce.Do(func() {
		err = p.Flush(ctx)
		close(p.appendCh)
		<-p.rotatorDone
		<-p.encoderPoolDone
		<-p.uploaderPoolDone
		<-p.committerDone
		<-p.resolverDone
	})
	return err
}
