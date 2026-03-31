package ingest

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

// WriteHandle is returned by Ingest and provides access to a DurabilityWatcher.
type WriteHandle struct {
	Watcher *DurabilityWatcher
}

type ingestMessage struct {
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

func (b *batchAccumulator) add(msg *ingestMessage) {
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

// Ingestor accepts opaque byte entries, batches them, and flushes to object
// storage on size or time thresholds.
type Ingestor struct {
	producer *queueProducer
	store    objstore.ObjectStore
	config   IngestorConfig

	ingestCh  chan *ingestMessage
	flushCh   chan *flushMessage
	done      chan struct{}
	closeOnce sync.Once
}

// NewIngestor creates a new Ingestor backed by the given ObjectStore.
func NewIngestor(store objstore.ObjectStore, config IngestorConfig) *Ingestor {
	producer := newQueueProducer(store, config.ManifestPath)
	ing := &Ingestor{
		producer: producer,
		store:    store,
		config:   config,
		ingestCh: make(chan *ingestMessage, config.MaxBufferedInputs),
		flushCh:  make(chan *flushMessage),
		done:     make(chan struct{}),
	}
	go ing.run()
	return ing
}

func (ing *Ingestor) run() {
	defer close(ing.done)

	batch := &batchAccumulator{}

	var timer *time.Timer
	resetTimer := func() {
		if timer != nil {
			timer.Stop()
		}
		if batch.started {
			remaining := time.Until(batch.startedAt.Add(ing.config.FlushInterval))
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
		case msg, ok := <-ing.ingestCh:
			if !ok {
				// Channel closed — flush remaining and exit.
				_ = ing.writeBatch(batch)
				return
			}
			batch.add(msg)
			if batch.sizeBytes >= ing.config.FlushSizeBytes {
				_ = ing.writeBatch(batch)
			}
		case fm := <-ing.flushCh:
			// Drain any pending ingest messages before flushing.
			ing.drainIngestCh(batch)
			fm.result <- ing.writeBatch(batch)
		case <-timerCh:
			_ = ing.writeBatch(batch)
		}
	}
}

func (ing *Ingestor) drainIngestCh(batch *batchAccumulator) {
	for {
		select {
		case msg, ok := <-ing.ingestCh:
			if !ok {
				return
			}
			batch.add(msg)
		default:
			return
		}
	}
}

func (ing *Ingestor) writeBatch(batch *batchAccumulator) error {
	if batch.isEmpty() {
		return nil
	}

	entries, metadata, watchers := batch.reset()
	err := ing.writeAndEnqueue(entries, metadata)
	for _, w := range watchers {
		w.resolve(err)
	}
	return err
}

func (ing *Ingestor) writeAndEnqueue(entries [][]byte, metadata []QueueMetadata) error {
	payload, err := EncodeBatch(entries, ing.config.BatchCompression)
	if err != nil {
		return err
	}

	id := ulid.Make()
	path := fmt.Sprintf("%s/%s.batch", ing.config.DataPathPrefix, id.String())

	ctx := context.Background()
	if err := ing.store.Put(ctx, path, payload); err != nil {
		return storageErr(err.Error())
	}

	return ing.producer.enqueue(ctx, path, metadata)
}

// Ingest submits entries and associated metadata for ingestion.
// It returns a WriteHandle whose Watcher can be used to await durability.
// Applies backpressure when the internal buffer is full.
func (ing *Ingestor) Ingest(entries [][]byte, metadata []byte) (*WriteHandle, error) {
	if len(entries) == 0 {
		return nil, invalidInputErr("entries must not be empty")
	}
	if metadata == nil {
		metadata = []byte{}
	}

	watcher := newDurabilityWatcher()
	msg := &ingestMessage{
		entries:         entries,
		metadata:        metadata,
		ingestionTimeMs: time.Now().UnixMilli(),
		watcher:         watcher,
	}

	select {
	case ing.ingestCh <- msg:
		return &WriteHandle{Watcher: watcher}, nil
	case <-ing.done:
		return nil, ErrShutdown
	}
}

// Flush forces a flush of the current batch, blocking until it is durably written.
func (ing *Ingestor) Flush(ctx context.Context) error {
	fm := &flushMessage{result: make(chan error, 1)}
	select {
	case ing.flushCh <- fm:
	case <-ing.done:
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
func (ing *Ingestor) ConflictRate() float64 {
	return ing.producer.conflictRate()
}

// Close flushes any remaining buffered entries and shuts down the ingestor.
func (ing *Ingestor) Close(ctx context.Context) error {
	var err error
	ing.closeOnce.Do(func() {
		err = ing.Flush(ctx)
		close(ing.ingestCh)
		<-ing.done
	})
	return err
}
