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

// Writer accepts opaque byte entries, batches them, and flushes to object
// storage on size or time thresholds.
type Writer struct {
	producer *queueProducer
	store    objstore.ObjectStore
	config   WriterConfig

	appendCh  chan *appendMessage
	flushCh   chan *flushMessage
	done      chan struct{}
	closeOnce sync.Once
}

// NewWriter creates a new Writer backed by the given ObjectStore.
func NewWriter(store objstore.ObjectStore, config WriterConfig) *Writer {
	producer := newQueueProducer(store, config.ManifestPath)
	w := &Writer{
		producer: producer,
		store:    store,
		config:   config,
		appendCh: make(chan *appendMessage, config.MaxBufferedInputs),
		flushCh:  make(chan *flushMessage),
		done:     make(chan struct{}),
	}
	go w.run()
	return w
}

func (w *Writer) run() {
	defer close(w.done)

	batch := &batchAccumulator{}

	var timer *time.Timer
	resetTimer := func() {
		if timer != nil {
			timer.Stop()
		}
		if batch.started {
			remaining := time.Until(batch.startedAt.Add(w.config.FlushInterval))
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
		case msg, ok := <-w.appendCh:
			if !ok {
				// Channel closed — flush remaining and exit.
				_ = w.writeBatch(batch, FlushReasonShutdown)
				return
			}
			batch.add(msg)
			if batch.sizeBytes >= w.config.FlushSizeBytes {
				_ = w.writeBatch(batch, FlushReasonSize)
			}
		case fm := <-w.flushCh:
			// Drain any pending append messages before flushing.
			w.drainAppendCh(batch)
			fm.result <- w.writeBatch(batch, FlushReasonManual)
		case <-timerCh:
			_ = w.writeBatch(batch, FlushReasonTime)
		}
	}
}

func (w *Writer) drainAppendCh(batch *batchAccumulator) {
	for {
		select {
		case msg, ok := <-w.appendCh:
			if !ok {
				return
			}
			batch.add(msg)
		default:
			return
		}
	}
}

func (w *Writer) writeBatch(batch *batchAccumulator, reason FlushReason) error {
	if batch.isEmpty() {
		return nil
	}

	stats := batch.stats()
	start := time.Now()
	entries, metadata, watchers := batch.reset()
	err := w.writeAndEnqueue(entries, metadata)
	if w.config.Observer != nil {
		w.config.Observer.OnFlush(reason, stats, time.Since(start), err)
	}
	for _, dw := range watchers {
		dw.resolve(err)
	}
	return err
}

func (w *Writer) writeAndEnqueue(entries [][]byte, metadata []QueueMetadata) error {
	payload, err := EncodeBatch(entries, w.config.BatchCompression)
	if err != nil {
		return err
	}

	id := ulid.Make()
	path := fmt.Sprintf("%s/%s.batch", w.config.DataPathPrefix, id.String())

	ctx := context.Background()
	putStart := time.Now()
	if err := w.store.Put(ctx, path, payload); err != nil {
		if w.config.Observer != nil {
			w.config.Observer.OnStorePut(len(payload), time.Since(putStart), err)
		}
		return storageErr(err.Error())
	}
	if w.config.Observer != nil {
		w.config.Observer.OnStorePut(len(payload), time.Since(putStart), nil)
	}

	manifestStart := time.Now()
	conflicts, err := w.producer.enqueue(ctx, path, metadata)
	if w.config.Observer != nil {
		w.config.Observer.OnManifestEnqueue(len(metadata), time.Since(manifestStart), conflicts, err)
	}
	return err
}

// Append submits entries and associated metadata for buffering.
// It returns a WriteHandle whose Watcher can be used to await durability.
// Applies backpressure when the internal buffer is full.
func (w *Writer) Append(entries [][]byte, metadata []byte) (*WriteHandle, error) {
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
	case w.appendCh <- msg:
		if w.config.Observer != nil {
			w.config.Observer.OnAccepted()
		}
		return &WriteHandle{Watcher: watcher}, nil
	case <-w.done:
		return nil, ErrShutdown
	}
}

// Flush forces a flush of the current batch, blocking until it is durably written.
func (w *Writer) Flush(ctx context.Context) error {
	fm := &flushMessage{result: make(chan error, 1)}
	select {
	case w.flushCh <- fm:
	case <-w.done:
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
func (w *Writer) ConflictRate() float64 {
	return w.producer.conflictRate()
}

// Close flushes any remaining buffered entries and shuts down the writer.
func (w *Writer) Close(ctx context.Context) error {
	var err error
	w.closeOnce.Do(func() {
		err = w.Flush(ctx)
		close(w.appendCh)
		<-w.done
	})
	return err
}
