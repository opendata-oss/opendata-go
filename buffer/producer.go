package buffer

import (
	"context"
	"fmt"
	"sync"
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

// readyBatch is the unit of work the Rotator emits to the Uploader.
//
// Phase 3.3 of the producer pipelining design (Migration Plan step 4)
// introduces this two-stage layout: the Rotator owns the accumulator
// and assigns monotonic ordinals; the Uploader consumes ready batches
// and runs the existing encode + PUT + manifest CAS + watcher-resolve
// sequence in one body. Subsequent units split that body further:
// 3.4 splits the Encoder out (§3.S2), 3.5 splits the ManifestCommitter
// out (§3.S4), 3.6 splits the WatcherResolver out (§3.S5).
//
// The location format change (`<run_id>/<ordinal:016x>` per §3.S2) is
// deferred to 3.4 alongside the Encoder pool. The `ordinal` field is
// already populated here so 3.4's diff is local to the Encoder.
type readyBatch struct {
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

// Producer accepts opaque byte entries, batches them, and flushes to object
// storage on size or time thresholds.
type Producer struct {
	enqueuer *manifestEnqueuer
	store    objstore.ObjectStore
	config   ProducerConfig

	appendCh chan *appendMessage
	flushCh  chan *flushMessage
	// readyCh carries ready batches from the Rotator to the Uploader.
	// Unbuffered: the Rotator blocks on send until the Uploader picks
	// the batch up. With UploadConcurrency=1 (Phase 3.3 default) this
	// preserves the pre-Phase-3 single-flight throughput exactly —
	// while the Uploader is encoding/uploading, the Rotator stops
	// emitting and (transitively) stops draining appendCh, so
	// appendCh fills and Append callers block, identical to the
	// pre-Phase-3 behavior.
	readyCh chan *readyBatch

	rotatorDone  chan struct{}
	uploaderDone chan struct{}
	closeOnce    sync.Once
}

// NewProducer creates a new Producer backed by the given ObjectStore.
//
// Spawns one Rotator goroutine + one Uploader goroutine. See
// `plans/odb-high-throughput/phase03-producer-pipelining-design.md`
// for the pipeline contract.
func NewProducer(store objstore.ObjectStore, config ProducerConfig) *Producer {
	enqueuer := newManifestEnqueuer(store, config.ManifestPath)
	p := &Producer{
		enqueuer:     enqueuer,
		store:        store,
		config:       config,
		appendCh:     make(chan *appendMessage, config.MaxBufferedInputs),
		flushCh:      make(chan *flushMessage),
		readyCh:      make(chan *readyBatch),
		rotatorDone:  make(chan struct{}),
		uploaderDone: make(chan struct{}),
	}
	go p.rotator()
	go p.uploader()
	return p
}

// rotator drains appendCh, manages the open accumulator, assigns
// monotonic ordinals at rotation time, and emits ready batches to
// readyCh on size / time / manual-flush / shutdown triggers.
//
// Single goroutine: ordinals are strictly monotonic by construction
// (no other goroutine increments the counter). See design §3.S1.
//
// Lifecycle: when appendCh closes, the rotator emits any open
// accumulator as a final FlushReasonShutdown batch (or skips if
// empty), closes readyCh (signaling the uploader to drain and
// exit), and signals rotatorDone.
func (p *Producer) rotator() {
	defer close(p.rotatorDone)
	defer close(p.readyCh)

	batch := &batchAccumulator{}
	var ordinal uint64

	emit := func(reason FlushReason, flushAck chan error) {
		if batch.isEmpty() {
			// For a flush request against an empty accumulator,
			// signal success immediately without sending to the
			// uploader. Preserves the pre-Phase-3 behavior of
			// `Flush` returning nil for an empty buffer.
			if flushAck != nil {
				flushAck <- nil
			}
			return
		}
		stats := batch.stats()
		entries, metadata, watchers := batch.reset()
		rb := &readyBatch{
			ordinal:  ordinal,
			entries:  entries,
			metadata: metadata,
			watchers: watchers,
			stats:    stats,
			reason:   reason,
			flushAck: flushAck,
		}
		ordinal++
		// Blocks until uploader takes it. While blocked, this
		// goroutine isn't draining appendCh either, so backpressure
		// propagates through appendCh to Append callers — exactly
		// matching the pre-Phase-3 single-goroutine behavior.
		p.readyCh <- rb
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

// uploader consumes ready batches from readyCh and runs the existing
// encode + PUT + manifest CAS + watcher-resolve sequence in one body.
// Phase 3.4 (Encoder pool), 3.5 (ManifestCommitter), and 3.6
// (WatcherResolver) progressively split this fused stage out.
//
// Single goroutine in Phase 3.3 (UploadConcurrency=1 default).
// Watchers are resolved with the batch's outcome; if the batch
// carried a flushAck (Producer.Flush emitted it), the same outcome
// goes to flushAck after watchers are resolved.
func (p *Producer) uploader() {
	defer close(p.uploaderDone)

	for rb := range p.readyCh {
		start := time.Now()
		err := p.writeAndEnqueue(rb.entries, rb.metadata)
		if p.config.Observer != nil {
			p.config.Observer.OnFlush(rb.reason, rb.stats, time.Since(start), err)
		}
		for _, dw := range rb.watchers {
			dw.resolve(err)
		}
		if rb.flushAck != nil {
			rb.flushAck <- err
		}
	}
}

func (p *Producer) writeAndEnqueue(entries [][]byte, metadata []QueueMetadata) error {
	payload, err := EncodeBatch(entries, p.config.BatchCompression)
	if err != nil {
		return err
	}

	id := ulid.Make()
	path := fmt.Sprintf("%s/%s.batch", p.config.DataPathPrefix, id.String())

	ctx := context.Background()
	putStart := time.Now()
	if err := p.store.Put(ctx, path, payload); err != nil {
		if p.config.Observer != nil {
			p.config.Observer.OnStorePut(len(payload), time.Since(putStart), err)
		}
		return storageErr(err.Error())
	}
	if p.config.Observer != nil {
		p.config.Observer.OnStorePut(len(payload), time.Since(putStart), nil)
	}

	manifestStart := time.Now()
	conflicts, err := p.enqueuer.enqueue(ctx, path, metadata)
	if p.config.Observer != nil {
		p.config.Observer.OnManifestEnqueue(len(metadata), time.Since(manifestStart), conflicts, err)
	}
	return err
}

// Append submits entries and associated metadata for buffering.
// It returns a WriteHandle whose Watcher can be used to await durability.
// Applies backpressure when the internal buffer is full.
func (p *Producer) Append(entries [][]byte, metadata []byte) (*WriteHandle, error) {
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

	select {
	case p.appendCh <- msg:
		if p.config.Observer != nil {
			p.config.Observer.OnAccepted()
		}
		return &WriteHandle{Watcher: watcher}, nil
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
	case <-p.uploaderDone:
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
		<-p.uploaderDone
	})
	return err
}
