package buffer

import (
	"context"
	"sync"
)

// producerBudget enforces the Phase-3 in-flight backpressure caps:
// `MaxInFlightBytes` (the binding memory bound) and
// `MaxInFlightBatches` (a secondary count safety). See design §Backpressure.
//
// Bytes are reserved at `AppendContext` enqueue (one reservation per
// caller-submitted message, sized as `sum(len(entries)) + len(metadata)`)
// and released by the WatcherResolver on terminal outcome (commit
// success or any failure). The reservation propagates through the
// pipeline on every batch struct so the resolver knows how much to
// release per batch.
//
// Batch slots are acquired by the Rotator at `emit` time (one slot
// per emitted `pendingBatch`) and released by the WatcherResolver
// alongside the byte release. Acquiring at the rotator (rather than
// at `AppendContext`) is what makes the cap meaningful: many
// callers' Appends can coalesce into a single batch, so per-Append
// slot-counting would over-charge.
//
// Cancellation: callers wait via `reserveBytes(ctx)`; the rotator
// waits via `acquireBatchSlot(shutdown)`. Both wake on capacity
// release or on their respective cancel signal. sync.Cond doesn't
// support context cancellation natively, so this implementation uses
// a one-shot waiter channel that's reissued on every release —
// inexpensive in the common no-block path because we only allocate
// when callers actually wait.
type producerBudget struct {
	mu sync.Mutex

	bytesUsed   int64
	batchesUsed int64

	maxBytes   int64
	maxBatches int64

	// waiter is closed when capacity might have freed; recreated on
	// the next contended reserve/acquire. nil when no waiter has
	// asked for a wakeup.
	waiter chan struct{}

	// drainCh is closed when batchesUsed transitions to 0. Allocated
	// lazily by waitDrained when batchesUsed > 0; closed and reset
	// to nil by release when batchesUsed reaches 0. Used by
	// Producer.Flush to block until every in-flight batch has
	// terminated, per design §"Producer.Flush(ctx) blocks until
	// every in-flight batch has either committed or failed". F1 of
	// Phase 3 rev-3 review.
	drainCh chan struct{}
}

func newProducerBudget(maxBytes, maxBatches int) *producerBudget {
	if maxBytes < 1 {
		maxBytes = 1
	}
	if maxBatches < 1 {
		maxBatches = 1
	}
	return &producerBudget{
		maxBytes:   int64(maxBytes),
		maxBatches: int64(maxBatches),
	}
}

// reserveBytes blocks until `n` bytes can be reserved against
// `MaxInFlightBytes`, or `ctx` is canceled (returns ctx.Err()), or
// `shutdown` is closed (returns ErrShutdown). Oversize requests
// (n > maxBytes) succeed once everything else is released — matching
// the design's "block until capacity frees" rule.
func (b *producerBudget) reserveBytes(ctx context.Context, n int64, shutdown <-chan struct{}) error {
	for {
		b.mu.Lock()
		// Allow a single oversize request to claim the budget once it
		// is empty; otherwise the producer would deadlock on a
		// payload larger than the cap. After this reservation lands,
		// the next caller waits per the normal rule.
		if b.bytesUsed == 0 || b.bytesUsed+n <= b.maxBytes {
			b.bytesUsed += n
			b.mu.Unlock()
			return nil
		}
		ch := b.ensureWaiterLocked()
		b.mu.Unlock()

		select {
		case <-ch:
			// Capacity may have freed; re-check.
		case <-ctx.Done():
			return ctx.Err()
		case <-shutdown:
			return ErrShutdown
		}
	}
}

// acquireBatchSlot blocks until one batch slot is available against
// `MaxInFlightBatches`, or `shutdown` fires (returns ErrShutdown).
// Used by the Rotator at emit time.
func (b *producerBudget) acquireBatchSlot(shutdown <-chan struct{}) error {
	for {
		b.mu.Lock()
		if b.batchesUsed < b.maxBatches {
			b.batchesUsed++
			b.mu.Unlock()
			return nil
		}
		ch := b.ensureWaiterLocked()
		b.mu.Unlock()

		select {
		case <-ch:
		case <-shutdown:
			return ErrShutdown
		}
	}
}

// release returns `bytes` and `batches` worth of capacity to the
// budget. Always wakes any pending waiters because either path could
// have been the limiting one. Called by the WatcherResolver per
// terminal batch outcome with `(batch.byteCost, 1)`. If batchesUsed
// drops to 0, also closes the drainCh waiter (if any) so a pending
// Flush can return.
func (b *producerBudget) release(bytes int64, batches int64) {
	b.mu.Lock()
	b.bytesUsed -= bytes
	if b.bytesUsed < 0 {
		b.bytesUsed = 0
	}
	b.batchesUsed -= batches
	if b.batchesUsed < 0 {
		b.batchesUsed = 0
	}
	if ch := b.waiter; ch != nil {
		close(ch)
		b.waiter = nil
	}
	if b.batchesUsed == 0 && b.drainCh != nil {
		close(b.drainCh)
		b.drainCh = nil
	}
	b.mu.Unlock()
}

// waitDrained blocks until batchesUsed reaches 0, returning nil. If
// batchesUsed is already 0, returns immediately. Returns ctx.Err()
// or ErrShutdown if those fire first. Per design: "Flush(ctx)
// blocks until every in-flight batch has either committed or
// failed." F1 of Phase 3 rev-3 review.
func (b *producerBudget) waitDrained(ctx context.Context, shutdown <-chan struct{}) error {
	for {
		b.mu.Lock()
		if b.batchesUsed == 0 {
			b.mu.Unlock()
			return nil
		}
		if b.drainCh == nil {
			b.drainCh = make(chan struct{})
		}
		ch := b.drainCh
		b.mu.Unlock()

		select {
		case <-ch:
			// batchesUsed transitioned to 0 — re-check under the
			// lock because a new acquire may have raced ahead. If
			// we're still at 0, return; otherwise wait again on a
			// fresh drainCh.
		case <-ctx.Done():
			return ctx.Err()
		case <-shutdown:
			return ErrShutdown
		}
	}
}

// snapshot returns the current (bytesUsed, batchesUsed) for metric
// emission and tests. Holds the budget lock so values are coherent.
func (b *producerBudget) snapshot() (int64, int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.bytesUsed, b.batchesUsed
}

// addReservation increments bytesUsed by `extra` (which may be
// negative) without blocking. Used by the Rotator at emit-time
// reconciliation to charge the per-batch framing overhead that
// `appendByteCost` does not capture (per-entry length prefix +
// per-batch footer). Per design §Backpressure step (b): "the
// reservation is reconciled to the accumulator's actual byte count
// (entries + metadata + buffer framing)". May transiently exceed
// `MaxInFlightBytes` by up to ~`MaxInFlightBatches * (4 * entries +
// 7)` bytes — bounded and small relative to the cap. F2 of Phase 3
// rev-3 review.
func (b *producerBudget) addReservation(extra int64) {
	b.mu.Lock()
	b.bytesUsed += extra
	if b.bytesUsed < 0 {
		b.bytesUsed = 0
	}
	if ch := b.waiter; ch != nil {
		close(ch)
		b.waiter = nil
	}
	b.mu.Unlock()
}

// ensureWaiterLocked returns the current waiter channel, allocating
// one if absent. Caller must hold b.mu.
func (b *producerBudget) ensureWaiterLocked() chan struct{} {
	if b.waiter == nil {
		b.waiter = make(chan struct{})
	}
	return b.waiter
}

// appendByteCost is the per-message byte cost reserved at
// AppendContext enqueue. Matches the design's accounting:
// `sum(len(entries)) + len(metadata)`.
func appendByteCost(entries [][]byte, metadata []byte) int64 {
	var total int64
	for _, e := range entries {
		total += int64(len(e))
	}
	total += int64(len(metadata))
	return total
}
