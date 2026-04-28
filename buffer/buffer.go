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

// Buffer accepts opaque byte entries, batches them, and flushes to object
// storage on size or time thresholds.
type Buffer struct {
	producer *queueProducer
	store    objstore.ObjectStore
	config   BufferConfig

	appendCh  chan *appendMessage
	flushCh   chan *flushMessage
	done      chan struct{}
	closeOnce sync.Once
}

// NewBuffer creates a new Buffer backed by the given ObjectStore.
func NewBuffer(store objstore.ObjectStore, config BufferConfig) *Buffer {
	producer := newQueueProducer(store, config.ManifestPath)
	b := &Buffer{
		producer: producer,
		store:    store,
		config:   config,
		appendCh: make(chan *appendMessage, config.MaxBufferedInputs),
		flushCh:  make(chan *flushMessage),
		done:     make(chan struct{}),
	}
	go b.run()
	return b
}

func (b *Buffer) run() {
	defer close(b.done)

	batch := &batchAccumulator{}

	var timer *time.Timer
	resetTimer := func() {
		if timer != nil {
			timer.Stop()
		}
		if batch.started {
			remaining := time.Until(batch.startedAt.Add(b.config.FlushInterval))
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
		case msg, ok := <-b.appendCh:
			if !ok {
				// Channel closed — flush remaining and exit.
				_ = b.writeBatch(batch, FlushReasonShutdown)
				return
			}
			batch.add(msg)
			if batch.sizeBytes >= b.config.FlushSizeBytes {
				_ = b.writeBatch(batch, FlushReasonSize)
			}
		case fm := <-b.flushCh:
			// Drain any pending append messages before flushing.
			b.drainAppendCh(batch)
			fm.result <- b.writeBatch(batch, FlushReasonManual)
		case <-timerCh:
			_ = b.writeBatch(batch, FlushReasonTime)
		}
	}
}

func (b *Buffer) drainAppendCh(batch *batchAccumulator) {
	for {
		select {
		case msg, ok := <-b.appendCh:
			if !ok {
				return
			}
			batch.add(msg)
		default:
			return
		}
	}
}

func (b *Buffer) writeBatch(batch *batchAccumulator, reason FlushReason) error {
	if batch.isEmpty() {
		return nil
	}

	stats := batch.stats()
	start := time.Now()
	entries, metadata, watchers := batch.reset()
	err := b.writeAndEnqueue(entries, metadata)
	if b.config.Observer != nil {
		b.config.Observer.OnFlush(reason, stats, time.Since(start), err)
	}
	for _, w := range watchers {
		w.resolve(err)
	}
	return err
}

func (b *Buffer) writeAndEnqueue(entries [][]byte, metadata []QueueMetadata) error {
	payload, err := EncodeBatch(entries, b.config.BatchCompression)
	if err != nil {
		return err
	}

	id := ulid.Make()
	path := fmt.Sprintf("%s/%s.batch", b.config.DataPathPrefix, id.String())

	ctx := context.Background()
	putStart := time.Now()
	if err := b.store.Put(ctx, path, payload); err != nil {
		if b.config.Observer != nil {
			b.config.Observer.OnStorePut(len(payload), time.Since(putStart), err)
		}
		return storageErr(err.Error())
	}
	if b.config.Observer != nil {
		b.config.Observer.OnStorePut(len(payload), time.Since(putStart), nil)
	}

	manifestStart := time.Now()
	conflicts, err := b.producer.enqueue(ctx, path, metadata)
	if b.config.Observer != nil {
		b.config.Observer.OnManifestEnqueue(len(metadata), time.Since(manifestStart), conflicts, err)
	}
	return err
}

// Append submits entries and associated metadata for buffering.
// It returns a WriteHandle whose Watcher can be used to await durability.
// Applies backpressure when the internal buffer is full.
func (b *Buffer) Append(entries [][]byte, metadata []byte) (*WriteHandle, error) {
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
	case b.appendCh <- msg:
		if b.config.Observer != nil {
			b.config.Observer.OnAccepted()
		}
		return &WriteHandle{Watcher: watcher}, nil
	case <-b.done:
		return nil, ErrShutdown
	}
}

// Flush forces a flush of the current batch, blocking until it is durably written.
func (b *Buffer) Flush(ctx context.Context) error {
	fm := &flushMessage{result: make(chan error, 1)}
	select {
	case b.flushCh <- fm:
	case <-b.done:
		return ErrShutdown
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-fm.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ConflictRate returns the percentage of manifest writes that encountered a conflict.
func (b *Buffer) ConflictRate() float64 {
	return b.producer.conflictRate()
}

// Close flushes any remaining buffered entries and shuts down the buffer.
func (b *Buffer) Close(ctx context.Context) error {
	var err error
	b.closeOnce.Do(func() {
		err = b.Flush(ctx)
		close(b.appendCh)
		<-b.done
	})
	return err
}
