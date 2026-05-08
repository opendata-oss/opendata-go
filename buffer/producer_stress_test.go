package buffer

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opendata-oss/opendata-go/objstore"
)

// TestProducer_concurrent_appends_no_race exercises the Phase-3
// pipeline under 1000 concurrent Append calls to surface any data
// races and prove all watchers resolve.
//
// Run with `go test -race`. See design §Test Plan.
func TestProducer_concurrent_appends_no_race(t *testing.T) {
	const n = 1000

	store := objstore.NewInMemory()
	cfg := testConfig()
	// Force a flush per-Append so the test exercises N rotations
	// rather than coalescing everything into one batch. With size=1
	// the rotator emits as soon as a single entry arrives.
	cfg.FlushSizeBytes = 1
	p := NewProducer(store, cfg)
	defer func() { _ = p.Close(context.Background()) }()

	var wg sync.WaitGroup
	wg.Add(n)
	handles := make([]*WriteHandle, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			h, err := p.Append([][]byte{[]byte("entry")}, []byte("metadata"))
			handles[i] = h
			errs[i] = err
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
		if handles[i] == nil {
			t.Fatalf("Append[%d]: nil handle", i)
		}
	}

	// All watchers must resolve with nil within a reasonable bound.
	deadline := time.Now().Add(15 * time.Second)
	for i, h := range handles {
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		if err := h.Watcher.AwaitDurable(ctx); err != nil {
			cancel()
			t.Fatalf("watcher[%d]: %v", i, err)
		}
		cancel()
	}

	// The manifest must contain at least one sequence and the
	// sum of metadata payloads across entries must equal n
	// (each Append contributed exactly one metadata range).
	entries := readManifestEntries(t, store)
	if len(entries) == 0 {
		t.Fatal("manifest empty after 1000 Append calls")
	}
	totalMeta := 0
	for _, e := range entries {
		totalMeta += len(e.Metadata)
	}
	if totalMeta != n {
		t.Fatalf("expected %d metadata items across the manifest, got %d", n, totalMeta)
	}

	// Sequences must be strictly monotonic and contiguous.
	seqs := make([]uint64, len(entries))
	for i, e := range entries {
		seqs[i] = e.Sequence
	}
	if !sort.SliceIsSorted(seqs, func(i, j int) bool { return seqs[i] < seqs[j] }) {
		t.Fatalf("sequences not monotonic: %v", seqs)
	}
	for i := 1; i < len(seqs); i++ {
		if seqs[i] != seqs[i-1]+1 {
			t.Fatalf("sequences not contiguous at index %d: %v", i, seqs)
		}
	}
}

// choreographableStore wraps an InMemory ObjectStore and lets a test
// hold Put() calls until release. PutIfMatch is also instrumented:
// a counter on every call (always exposed) plus an optional
// manifest-CAS gate that holds the first PutIfMatch until released
// (used by the F3 in-order coalescing test to keep ordinals piling
// up while the first CAS is in flight). Get/Delete are passthrough.
//
// Test usage:
//
//	store := newChoreographableStore()
//	// ... start producer + appends ...
//	store.releasePut(loc4)
//	store.releasePut(loc3)
//	// ...
//
// Each Put call blocks until releasePut is called for its path.
type choreographableStore struct {
	inner *objstore.InMemory

	mu       sync.Mutex
	pending  map[string]chan struct{} // path -> release channel
	observed map[string]bool          // paths observed entering Put
	cond     *sync.Cond

	// PutIfMatch instrumentation. casCount is unconditional; the
	// optional CAS gate is set up by holdFirstCAS and released by
	// releaseFirstCAS so the F3 in-order coalescing test can keep
	// the committer parked on its first CAS while subsequent
	// upload completions pile up in the buffered uploadCompletionCh.
	casCount       atomic.Int64
	firstCASGate   chan struct{} // nil = no gate; closed = gate open; pending = gate closed
	firstCASSeen   chan struct{} // closed once the first PutIfMatch is observed
	firstCASActive atomic.Bool   // true while the first CAS is parked at the gate
}

func newChoreographableStore() *choreographableStore {
	s := &choreographableStore{
		inner:    objstore.NewInMemory(),
		pending:  make(map[string]chan struct{}),
		observed: make(map[string]bool),
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

func (s *choreographableStore) Get(ctx context.Context, path string) (objstore.GetResult, error) {
	return s.inner.Get(ctx, path)
}

func (s *choreographableStore) Put(ctx context.Context, path string, data []byte) error {
	// Make a release channel for this path on first observation.
	s.mu.Lock()
	ch, ok := s.pending[path]
	if !ok {
		ch = make(chan struct{})
		s.pending[path] = ch
	}
	s.observed[path] = true
	s.cond.Broadcast()
	s.mu.Unlock()

	select {
	case <-ch:
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.inner.Put(ctx, path, data)
}

func (s *choreographableStore) PutIfMatch(ctx context.Context, path string, data []byte, version *objstore.Version) error {
	s.casCount.Add(1)
	// First-CAS gate: when armed, the very first PutIfMatch parks
	// here until releaseFirstCAS is called. Subsequent CAS calls
	// pass through unconditionally.
	if s.firstCASActive.CompareAndSwap(false, true) {
		// We are the first CAS. Signal observation, then wait for
		// release if a gate was set up.
		if s.firstCASSeen != nil {
			close(s.firstCASSeen)
		}
		if s.firstCASGate != nil {
			select {
			case <-s.firstCASGate:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return s.inner.PutIfMatch(ctx, path, data, version)
}

func (s *choreographableStore) Delete(ctx context.Context, path string) error {
	return s.inner.Delete(ctx, path)
}

// holdFirstCAS arms a one-shot gate that blocks the first PutIfMatch
// call until releaseFirstCAS is called. firstCASSeen() returns a
// channel closed when the first CAS arrives at the gate.
func (s *choreographableStore) holdFirstCAS() {
	s.firstCASGate = make(chan struct{})
	s.firstCASSeen = make(chan struct{})
}

// firstCASObserved returns a channel closed when the first PutIfMatch
// has been observed (after holdFirstCAS was called).
func (s *choreographableStore) firstCASObserved() <-chan struct{} {
	return s.firstCASSeen
}

// releaseFirstCAS opens the gate so the parked first CAS can proceed.
func (s *choreographableStore) releaseFirstCAS() {
	close(s.firstCASGate)
}

// casCalls reports the total PutIfMatch calls observed by the store,
// including retries.
func (s *choreographableStore) casCalls() int64 {
	return s.casCount.Load()
}

// waitForObserved blocks until all paths starting with `prefix` are
// observed in Put. Returns false on timeout.
func (s *choreographableStore) waitForObserved(prefix string, count int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		matching := 0
		for p := range s.observed {
			if strings.HasPrefix(p, prefix) {
				matching++
			}
		}
		if matching >= count {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		// Timed wait with a 50 ms cap.
		ch := make(chan struct{})
		go func() {
			time.Sleep(50 * time.Millisecond)
			close(ch)
		}()
		s.mu.Unlock()
		<-ch
		s.mu.Lock()
	}
}

// observedPaths returns a snapshot of the set of paths observed in
// Put, sorted. Used to discover the locations the producer chose.
func (s *choreographableStore) observedPaths() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.observed))
	for p := range s.observed {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

func (s *choreographableStore) releasePut(path string) {
	s.mu.Lock()
	ch, ok := s.pending[path]
	if !ok {
		ch = make(chan struct{})
		s.pending[path] = ch
	}
	// Idempotent: a second releasePut for the same path is a no-op.
	// Without this, tests that release a known path explicitly and
	// then bulk-release the rest would double-close.
	select {
	case <-ch:
		s.mu.Unlock()
		return
	default:
	}
	s.mu.Unlock()
	close(ch)
}

// TestProducer_uploads_complete_out_of_order is the dedicated OOO
// upload-completion choreographed test from design §Test Plan: with
// UploadConcurrency >= N, release per-batch PUTs in reverse ordinal
// order and assert the ManifestCommitter still appends entries in
// monotonic sequence order.
//
// This is the headline scenario for the rotator/uploader/committer
// split: out-of-order PUT completion is what the parallel-upload
// path produces, and the committer must reorder back into ordinal
// sequence before the manifest CAS.
func TestProducer_uploads_complete_out_of_order(t *testing.T) {
	const n = 4

	store := newChoreographableStore()
	cfg := testConfig()
	cfg.FlushSizeBytes = 1    // each Append → its own batch
	cfg.EncodeConcurrency = n // all encodes can run in parallel
	cfg.UploadConcurrency = n // all PUTs can be in flight at once
	p := NewProducer(store, cfg)
	defer func() { _ = p.Close(context.Background()) }()

	prefix := cfg.DataPathPrefix + "/" + p.runID + "/"

	// Submit n Appends concurrently. Each becomes its own batch.
	handles := make([]*WriteHandle, n)
	for i := 0; i < n; i++ {
		h, err := p.Append([][]byte{[]byte{byte(i)}}, []byte("md"))
		if err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
		handles[i] = h
	}

	// Wait for all n PUTs to be queued (each blocks inside Put
	// awaiting release).
	if !store.waitForObserved(prefix, n, 5*time.Second) {
		t.Fatalf("only %d of %d PUTs observed within 5s", len(store.observedPaths()), n)
	}

	paths := store.observedPaths()
	if len(paths) < n {
		t.Fatalf("expected >= %d observed paths, got %d", n, len(paths))
	}
	// observedPaths is sorted, and the producer's location format
	// is `<prefix>/<runID>/<ordinal:016x>` — so sorted lexically
	// equals sorted by ordinal.

	// Release in reverse order. The committer must NOT make
	// progress until ordinal 0's PUT completes (head-of-line).
	for i := n - 1; i > 0; i-- {
		store.releasePut(paths[i])
	}

	// At this point ordinals 1..n-1 have completed PUTs but the
	// committer is waiting for ordinal 0. Give it a moment to
	// confirm no manifest append happened yet.
	time.Sleep(50 * time.Millisecond)
	if entries, err := store.Get(context.Background(), cfg.ManifestPath); err == nil {
		decoded, _ := DecodeManifestEntries(entries.Data)
		if len(decoded) != 0 {
			t.Fatalf("manifest already has %d entries before ordinal 0 released; head-of-line block broken", len(decoded))
		}
	} else if !errors.Is(err, objstore.ErrNotFound) {
		t.Fatalf("manifest Get: %v", err)
	}

	// Release ordinal 0. Now the committer drains all n in monotonic
	// order. With ManifestAppendBatchSize=1 (default) we expect n
	// CAS calls; with higher coalescing they collapse but the
	// sequence order in the manifest must still be monotonic.
	store.releasePut(paths[0])

	// All watchers resolve with nil.
	for i, h := range handles {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := h.Watcher.AwaitDurable(ctx); err != nil {
			cancel()
			t.Fatalf("watcher[%d]: %v", i, err)
		}
		cancel()
	}

	// Manifest sequences are [0, 1, 2, ..., n-1] in order.
	entries := readManifestEntries(t, store.inner)
	if len(entries) != n {
		t.Fatalf("expected %d manifest entries, got %d", n, len(entries))
	}
	for i, e := range entries {
		if e.Sequence != uint64(i) {
			t.Fatalf("entries[%d].Sequence = %d, want %d", i, e.Sequence, i)
		}
		// The entry's location must match one of the PUT paths in
		// monotonic order (i.e. the i-th entry's location is
		// paths[i] — the deterministic ordinal-derived path).
		if e.Location != paths[i] {
			t.Fatalf("entries[%d].Location = %q, want %q", i, e.Location, paths[i])
		}
	}
}

// faultyStore wraps an InMemory ObjectStore and injects per-method
// failures by counter. Tests configure failPutCount / failCASCount
// (atomic) — the next N calls to Put / PutIfMatch return the
// configured failure error before the call falls through to the inner
// store. Used by the F2 retry / halt tests.
type faultyStore struct {
	inner *objstore.InMemory

	// Atomic counters: each call decrements; while > 0, the call
	// returns failPutErr / failCASErr.
	failPutCount atomic.Int64
	failCASErr   error
	failCASCount atomic.Int64
	failPutErr   error

	putCalls atomic.Int64
	casCalls atomic.Int64
}

func newFaultyStore() *faultyStore {
	return &faultyStore{inner: objstore.NewInMemory()}
}

func (s *faultyStore) Get(ctx context.Context, path string) (objstore.GetResult, error) {
	return s.inner.Get(ctx, path)
}

func (s *faultyStore) Put(ctx context.Context, path string, data []byte) error {
	s.putCalls.Add(1)
	if s.failPutCount.Load() > 0 {
		s.failPutCount.Add(-1)
		return s.failPutErr
	}
	return s.inner.Put(ctx, path, data)
}

func (s *faultyStore) PutIfMatch(ctx context.Context, path string, data []byte, version *objstore.Version) error {
	s.casCalls.Add(1)
	if s.failCASCount.Load() > 0 {
		s.failCASCount.Add(-1)
		return s.failCASErr
	}
	return s.inner.PutIfMatch(ctx, path, data, version)
}

func (s *faultyStore) Delete(ctx context.Context, path string) error {
	return s.inner.Delete(ctx, path)
}

// TestProducer_upload_retries_succeed_within_budget: F2 of Phase 3
// rev-2 review. UploadMaxAttempts=3; the store fails the first 2
// PUT calls; the 3rd succeeds. Watcher resolves with nil and the
// store observed exactly 3 PUTs.
func TestProducer_upload_retries_succeed_within_budget(t *testing.T) {
	store := newFaultyStore()
	store.failPutErr = errors.New("transient PUT error")
	store.failPutCount.Store(2)

	cfg := testConfig()
	cfg.FlushSizeBytes = 1
	cfg.UploadMaxAttempts = 3
	cfg.UploadInitialBackoff = time.Millisecond
	p := NewProducer(store, cfg)
	defer func() { _ = p.Close(context.Background()) }()

	h, err := p.Append([][]byte{[]byte("entry")}, nil)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.Watcher.AwaitDurable(ctx); err != nil {
		t.Fatalf("watcher: %v", err)
	}
	if got := store.putCalls.Load(); got != 3 {
		t.Fatalf("expected exactly 3 PUT attempts, got %d", got)
	}
}

// TestProducer_upload_retry_exhaustion_resolves_with_error: store
// always fails PUT; UploadMaxAttempts=2; watcher resolves with
// the underlying storage error and the committer skips the
// ordinal. Producer is NOT halted — design §Failure → Upload
// failure: "permanent. Resolve watchers with error; committer
// skips the ordinal."
func TestProducer_upload_retry_exhaustion_resolves_with_error(t *testing.T) {
	store := newFaultyStore()
	store.failPutErr = errors.New("permanent PUT error")
	store.failPutCount.Store(100) // way more than UploadMaxAttempts

	cfg := testConfig()
	cfg.FlushSizeBytes = 1
	cfg.UploadMaxAttempts = 2
	cfg.UploadInitialBackoff = time.Millisecond
	p := NewProducer(store, cfg)
	defer func() { _ = p.Close(context.Background()) }()

	h, err := p.Append([][]byte{[]byte("entry")}, nil)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	werr := h.Watcher.AwaitDurable(ctx)
	if werr == nil {
		t.Fatalf("watcher resolved with nil; expected the underlying PUT error")
	}
	if !errors.Is(werr, ErrStorage) {
		t.Fatalf("expected ErrStorage in chain; got %v", werr)
	}
	// Producer must NOT be halted — upload exhaustion is a per-batch
	// permanent failure, not a producer-wide one.
	if p.halted.Load() {
		t.Fatalf("producer halted after upload retry exhaustion; design says only manifest exhaustion halts")
	}
	// Subsequent Append must succeed (and would itself fail upload
	// because the store is still failing — but that's the next
	// batch's problem; the producer accepts new work).
	h2, err := p.Append([][]byte{[]byte("e2")}, nil)
	if err != nil {
		t.Fatalf("post-failure Append: %v", err)
	}
	werr = h2.Watcher.AwaitDurable(ctx)
	if werr == nil || !errors.Is(werr, ErrStorage) {
		t.Fatalf("second watcher: expected ErrStorage, got %v", werr)
	}
	if got := store.putCalls.Load(); got != 4 {
		t.Fatalf("expected 4 PUT attempts (2 per batch × 2 batches), got %d", got)
	}
}

// haltObserver counts OnHalted invocations and records the last
// halted state. Used to assert that a halted producer notifies its
// observer exactly once.
type haltObserver struct {
	mu          sync.Mutex
	haltedCalls int
	lastHalted  bool
}

func (o *haltObserver) OnAccepted()                                           {}
func (o *haltObserver) OnFlush(FlushReason, FlushStats, time.Duration, error) {}
func (o *haltObserver) OnStorePut(int, time.Duration, error)                  {}
func (o *haltObserver) OnManifestEnqueue(int, time.Duration, int, error)      {}
func (o *haltObserver) OnAppendChBlock(time.Duration)                         {}
func (o *haltObserver) OnWorkersBusy(PipelineStage, int)                      {}
func (o *haltObserver) OnEncodeDuration(time.Duration, error)                 {}
func (o *haltObserver) OnUploadDuration(time.Duration, int, error)            {}
func (o *haltObserver) OnManifestAppendBatchSize(int)                         {}
func (o *haltObserver) OnManifestAppendDuration(time.Duration, int, error)    {}
func (o *haltObserver) OnHeadOfLineBlock(time.Duration)                       {}
func (o *haltObserver) OnBatchOutcome(BatchOutcome)                           {}
func (o *haltObserver) OnInflightBytes(int64)                                 {}
func (o *haltObserver) OnInflightBatches(int)                                 {}
func (o *haltObserver) OnQueueDepth(PipelineStage, int)                       {}
func (o *haltObserver) OnHalted(halted bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.haltedCalls++
	o.lastHalted = halted
}

// TestProducer_manifest_retry_exhaustion_halts_producer: F2 of
// Phase 3 rev-2 review. Store always fails PutIfMatch (with a
// non-CAS-conflict error); ManifestMaxAttempts=2; the producer must:
//
//  1. resolve the in-flight batch's watcher with ErrProducerHalted
//  2. transition to halted state (Producer.halted == true)
//  3. emit OnHalted(true) exactly once
//  4. reject subsequent Append calls with ErrProducerHalted
func TestProducer_manifest_retry_exhaustion_halts_producer(t *testing.T) {
	store := newFaultyStore()
	store.failCASErr = errors.New("permanent CAS error")
	store.failCASCount.Store(100)

	obs := &haltObserver{}

	cfg := testConfig()
	cfg.FlushSizeBytes = 1
	cfg.ManifestMaxAttempts = 2
	cfg.ManifestInitialBackoff = time.Millisecond
	cfg.Observer = obs
	p := NewProducer(store, cfg)
	defer func() { _ = p.Close(context.Background()) }()

	h, err := p.Append([][]byte{[]byte("entry")}, nil)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	werr := h.Watcher.AwaitDurable(ctx)
	if !errors.Is(werr, ErrProducerHalted) {
		t.Fatalf("expected ErrProducerHalted, got %v", werr)
	}
	if !p.halted.Load() {
		t.Fatalf("producer.halted not set after manifest retry exhaustion")
	}
	obs.mu.Lock()
	if obs.haltedCalls != 1 || !obs.lastHalted {
		t.Fatalf("expected exactly one OnHalted(true); got %d calls (lastHalted=%v)", obs.haltedCalls, obs.lastHalted)
	}
	obs.mu.Unlock()

	// Subsequent Append must return ErrProducerHalted immediately.
	if _, err := p.Append([][]byte{[]byte("e2")}, nil); !errors.Is(err, ErrProducerHalted) {
		t.Fatalf("expected ErrProducerHalted, got %v", err)
	}
	// And the manifest must record exactly ManifestMaxAttempts CAS
	// calls (we don't count internal ErrPreconditionFailed retries
	// because the fault store's failure isn't precondition-failed).
	if got := store.casCalls.Load(); got != 2 {
		t.Fatalf("expected exactly 2 CAS attempts (= ManifestMaxAttempts), got %d", got)
	}
}

// TestProducer_Close_canceled_ctx_returns_quickly: F4 of Phase 3
// rev-2 review. Without the fix, a stuck store.Put call would
// indefinitely block Close because the pipeline used
// context.Background() everywhere; the unconditional `<-doneCh`
// waits in Close had no escape hatch.
//
// Setup: choreographable store with a never-released PUT. Append one
// batch (its uploader parks at the gate). Call Close with a ~150 ms
// deadline. Assert: (a) Close returns ctx.Err() within ~250 ms;
// (b) the in-flight watcher resolves with an error within the same
// bound (the cancellation cascades through the pipeline because
// shutdownCtx is wired into store.Put).
func TestProducer_Close_canceled_ctx_returns_quickly(t *testing.T) {
	store := newChoreographableStore()

	cfg := testConfig()
	cfg.FlushSizeBytes = 1
	p := NewProducer(store, cfg)

	h, err := p.Append([][]byte{[]byte("entry")}, nil)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	prefix := cfg.DataPathPrefix + "/" + p.runID + "/"
	if !store.waitForObserved(prefix, 1, 5*time.Second) {
		t.Fatalf("PUT never observed at gate")
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	closeStart := time.Now()
	closeErr := p.Close(closeCtx)
	closeElapsed := time.Since(closeStart)

	if closeElapsed > 500*time.Millisecond {
		t.Fatalf("Close did not honor ctx deadline; took %v (expected ≤ 500 ms)", closeElapsed)
	}
	if closeErr == nil {
		t.Fatalf("expected Close to return ctx.Err(); got nil")
	}
	if !errors.Is(closeErr, context.DeadlineExceeded) && !errors.Is(closeErr, context.Canceled) {
		t.Fatalf("expected ctx error, got %v", closeErr)
	}

	// The in-flight watcher must resolve within a bounded window —
	// the shutdown cascade aborts the store.Put via shutdownCtx and
	// the resolver propagates the error. AwaitDurable's own ctx is
	// generous so we observe the resolver-driven resolution rather
	// than ctx-cancel.
	awaitCtx, awaitCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer awaitCancel()
	werr := h.Watcher.AwaitDurable(awaitCtx)
	if werr == nil {
		t.Fatalf("watcher resolved with nil after Close-ctx-cancel; expected an error")
	}
}

// TestProducer_byte_budget_blocks_AppendContext_until_release exercises
// F1 of the Phase 3 rev-2 review: `MaxInFlightBytes` must actually
// gate `AppendContext`, not just sit in `ProducerConfig`. The first
// Append fills the budget to the cap; the second Append's reservation
// would exceed it and must block until a watcher resolves and the
// resolver releases bytes back to the budget.
//
// Setup: tiny `MaxInFlightBytes` (smaller than two payloads). Hold
// the upload PUT so the first batch never resolves on its own; the
// test asserts that AppendContext is blocked, then releases the PUT,
// confirms the watcher resolves, and finally confirms the second
// AppendContext unblocks once the budget frees.
func TestProducer_byte_budget_blocks_AppendContext_until_release(t *testing.T) {
	store := newChoreographableStore()

	// Each payload is 16 bytes (the byte slice). Cap budget at 16 so
	// only one in flight at a time.
	cfg := testConfig()
	cfg.FlushSizeBytes = 1
	cfg.MaxInFlightBytes = 16
	cfg.MaxInFlightBatches = 64
	p := NewProducer(store, cfg)
	defer func() { _ = p.Close(context.Background()) }()

	// First Append: the reservation fits (budget empty → oversize
	// rule does not apply, but in this case 16-byte payload exactly
	// equals the cap). Returns immediately.
	h1, err := p.Append([][]byte{make([]byte, 16)}, nil)
	if err != nil {
		t.Fatalf("first Append: %v", err)
	}

	// Wait until the first batch's PUT is parked at the gate; that
	// confirms the reservation has propagated through the pipeline
	// and the budget is held until release.
	prefix := cfg.DataPathPrefix + "/" + p.runID + "/"
	if !store.waitForObserved(prefix, 1, 5*time.Second) {
		t.Fatalf("first PUT never observed within 5s")
	}

	// Second Append in a goroutine: must block on the budget.
	type result struct {
		h   *WriteHandle
		err error
	}
	res := make(chan result, 1)
	go func() {
		h, err := p.Append([][]byte{make([]byte, 16)}, nil)
		res <- result{h, err}
	}()

	// Confirm the second Append is blocked (no result within a
	// generous-but-bounded window).
	select {
	case r := <-res:
		t.Fatalf("second Append unblocked too early; got h=%v err=%v", r.h, r.err)
	case <-time.After(150 * time.Millisecond):
		// Expected — the budget is full and the second Append is
		// parked in reserveBytes.
	}

	// Release the first PUT. The first batch flows through the
	// pipeline to the WatcherResolver, which releases the budget.
	// The second AppendContext should then unblock.
	paths := store.observedPaths()
	store.releasePut(paths[0])

	if err := h1.Watcher.AwaitDurable(context.Background()); err != nil {
		t.Fatalf("first watcher: %v", err)
	}

	select {
	case r := <-res:
		if r.err != nil {
			t.Fatalf("second Append: %v", r.err)
		}
		// Release its PUT too so the test's deferred Close() can
		// drain cleanly.
		if !store.waitForObserved(prefix, 2, 5*time.Second) {
			t.Fatalf("second PUT never observed")
		}
		paths = store.observedPaths()
		for _, p := range paths {
			store.releasePut(p)
		}
		if err := r.h.Watcher.AwaitDurable(context.Background()); err != nil {
			t.Fatalf("second watcher: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("second Append never unblocked after first batch resolved")
	}
}

// TestProducer_byte_budget_AppendContext_returns_ctx_err_on_cancel
// exercises the design's "AppendContext returns ctx.Err() when the
// supplied context cancels" guarantee under budget pressure. Ensures
// no leaked reservation: a subsequent AppendContext must succeed.
func TestProducer_byte_budget_AppendContext_returns_ctx_err_on_cancel(t *testing.T) {
	store := newChoreographableStore()

	cfg := testConfig()
	cfg.FlushSizeBytes = 1
	cfg.MaxInFlightBytes = 16
	cfg.MaxInFlightBatches = 64
	p := NewProducer(store, cfg)
	defer func() { _ = p.Close(context.Background()) }()

	// Fill the budget with one held in-flight batch.
	h1, err := p.Append([][]byte{make([]byte, 16)}, nil)
	if err != nil {
		t.Fatalf("first Append: %v", err)
	}
	prefix := cfg.DataPathPrefix + "/" + p.runID + "/"
	if !store.waitForObserved(prefix, 1, 5*time.Second) {
		t.Fatalf("first PUT never observed")
	}

	// AppendContext with a soon-to-cancel context: must return
	// ctx.Err() once the context fires, leaving the budget unchanged.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := p.AppendContext(ctx, [][]byte{make([]byte, 16)}, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}

	// Release the first PUT and confirm a fresh AppendContext now
	// succeeds — proving the canceled call did not leak its
	// reservation.
	paths := store.observedPaths()
	store.releasePut(paths[0])
	if err := h1.Watcher.AwaitDurable(context.Background()); err != nil {
		t.Fatalf("first watcher: %v", err)
	}

	h2, err := p.AppendContext(context.Background(), [][]byte{make([]byte, 16)}, nil)
	if err != nil {
		t.Fatalf("post-release AppendContext: %v", err)
	}
	if !store.waitForObserved(prefix, 2, 5*time.Second) {
		t.Fatalf("second PUT never observed")
	}
	paths = store.observedPaths()
	for _, p := range paths {
		store.releasePut(p)
	}
	if err := h2.Watcher.AwaitDurable(context.Background()); err != nil {
		t.Fatalf("second watcher: %v", err)
	}
}

// TestProducer_manifest_commit_coalesces_in_order_under_slow_first_CAS
// is the F3 regression gate: with `ManifestAppendBatchSize > 1`,
// in-order completions arriving while the committer is busy on a CAS
// must coalesce into the next CAS instead of each issuing their own.
//
// Setup: 32 batches, batchSize=16, UploadConcurrency=32. Hold all
// data PUTs. Release ordinal 0's PUT first; wait for the committer's
// first CAS to be observed (parked at the choreographed gate).
// Release the remaining 31 PUTs; their completions accumulate in the
// buffered uploadCompletionCh while the first CAS is parked.
// Release the first CAS. The committer drains the 31 queued
// completions in two coalesced CAS groups (16 + 15).
//
// With the F3 fix (buffered channel + gather-before-drain), the test
// observes exactly 3 CAS calls (1 for ordinal 0 + 2 coalesced groups).
// Without the fix (unbuffered channel + drain after each receive),
// each of the 31 in-order completions triggers its own CAS = 32 total.
// The asserted bound (≤5) leaves slack for scheduling jitter while
// still failing dramatically on the unfixed code path.
func TestProducer_manifest_commit_coalesces_in_order_under_slow_first_CAS(t *testing.T) {
	const n = 32

	store := newChoreographableStore()
	store.holdFirstCAS()

	cfg := testConfig()
	cfg.FlushSizeBytes = 1
	cfg.EncodeConcurrency = n
	cfg.UploadConcurrency = n
	cfg.MaxInFlightBatches = 64 // sized to hold n in flight without backpressure
	cfg.ManifestAppendBatchSize = 16
	p := NewProducer(store, cfg)
	defer func() { _ = p.Close(context.Background()) }()

	prefix := cfg.DataPathPrefix + "/" + p.runID + "/"

	// Submit n Appends. Each becomes its own batch (FlushSizeBytes=1).
	handles := make([]*WriteHandle, n)
	for i := 0; i < n; i++ {
		h, err := p.Append([][]byte{{byte(i % 256)}}, []byte("md"))
		if err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
		handles[i] = h
	}

	// Wait until all n PUTs are observed at the choreography gate.
	if !store.waitForObserved(prefix, n, 5*time.Second) {
		t.Fatalf("only %d of %d PUTs observed within 5s", len(store.observedPaths()), n)
	}
	paths := store.observedPaths()

	// Release ordinal 0's PUT (paths is sorted lexically; the
	// `<runID>/<ordinal:016x>` format makes lexical order = ordinal
	// order). The committer receives ordinal 0, drains, and parks
	// at the held first CAS.
	store.releasePut(paths[0])
	select {
	case <-store.firstCASObserved():
	case <-time.After(5 * time.Second):
		t.Fatalf("first CAS never observed; ordinal 0 PUT may not have completed")
	}

	// Release the remaining 31 PUTs. Their completions queue in the
	// buffered uploadCompletionCh while the first CAS is parked.
	for i := 1; i < n; i++ {
		store.releasePut(paths[i])
	}

	// Give the upload pool a brief window to push the 31 completions
	// onto uploadCompletionCh before we release the first CAS. Without
	// this, the committer may receive ordinal 1 before the rest queue
	// up, breaking the deterministic 3-CAS expectation. The window is
	// small relative to test timeout but generous relative to the
	// in-memory PUT cost (sub-millisecond).
	time.Sleep(50 * time.Millisecond)

	store.releaseFirstCAS()

	// All watchers resolve.
	for i, h := range handles {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := h.Watcher.AwaitDurable(ctx); err != nil {
			cancel()
			t.Fatalf("watcher[%d]: %v", i, err)
		}
		cancel()
	}

	// Coalescing assertion. With F3 fix: 1 (ordinal 0 alone) + 2
	// (coalesced 1..31) = 3 CAS calls. Without F3 fix: 32. The bound
	// of 5 leaves slack for any scheduling-induced split of the
	// post-release group while still failing on the unfixed path.
	casCalls := store.casCalls()
	if casCalls > 5 {
		t.Fatalf(
			"expected coalesced CAS count ≤ 5 for n=%d ManifestAppendBatchSize=16; got %d (without F3 fix this is ~%d)",
			n, casCalls, n,
		)
	}

	// Manifest must contain all 32 entries in monotonic order.
	entries := readManifestEntries(t, store.inner)
	if len(entries) != n {
		t.Fatalf("expected %d manifest entries, got %d", n, len(entries))
	}
	for i, e := range entries {
		if e.Sequence != uint64(i) {
			t.Fatalf("entries[%d].Sequence = %d, want %d", i, e.Sequence, i)
		}
	}
}

// TestProducer_Flush_waits_for_in_flight_size_triggered_batch is the
// F1 regression test from Phase 3 rev-3 review.
//
// Scenario:
//  1. FlushSizeBytes=1: the first Append immediately triggers a
//     size-rotation. The accumulator drains.
//  2. The choreographable store holds the PUT, so the batch is
//     parked in the uploader and the watcher is unresolved.
//  3. The caller invokes Flush. The accumulator is now empty, so
//     the rotator's empty-accumulator path signals flushAck<-nil
//     immediately.
//  4. Without the fix: Flush returns nil while the size-triggered
//     batch is still uploading — violating the design contract
//     "Flush(ctx) blocks until every in-flight batch has either
//     committed or failed".
//  5. With the fix: Flush returns flushAck, then waits on the
//     budget's drain signal. It only returns once the held PUT
//     releases and the watcher resolves.
func TestProducer_Flush_waits_for_in_flight_size_triggered_batch(t *testing.T) {
	store := newChoreographableStore()
	cfg := testConfig()
	cfg.FlushSizeBytes = 1 // every Append triggers a size rotation
	p := NewProducer(store, cfg)
	defer func() { _ = p.Close(context.Background()) }()

	prefix := cfg.DataPathPrefix + "/" + p.runID + "/"

	h, err := p.Append([][]byte{[]byte("entry")}, []byte("md"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Wait for the size-triggered batch to reach the uploader, where
	// the choreographable store parks it on Put.
	if !store.waitForObserved(prefix, 1, 5*time.Second) {
		t.Fatalf("PUT never observed; the size-triggered batch did not reach the uploader")
	}

	// At this point: accumulator is empty (the size-triggered batch
	// left), one batch is in flight (parked on Put).

	// Run Flush in a goroutine with a tight timeout. Without the F1
	// fix, Flush returns nil within microseconds.
	flushDone := make(chan error, 1)
	go func() {
		flushDone <- p.Flush(context.Background())
	}()

	select {
	case err := <-flushDone:
		t.Fatalf("Flush returned %v while a size-triggered batch was still in flight; F1 regression", err)
	case <-time.After(200 * time.Millisecond):
		// Expected: Flush is blocked waiting for the in-flight batch.
	}

	// Release the held PUT so the in-flight batch can commit.
	paths := store.observedPaths()
	if len(paths) == 0 {
		t.Fatalf("no observed PUT paths")
	}
	store.releasePut(paths[0])

	select {
	case err := <-flushDone:
		if err != nil {
			t.Fatalf("Flush returned error after release: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Flush did not return within 5s after PUT release")
	}

	// And the original watcher resolved.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.Watcher.AwaitDurable(ctx); err != nil {
		t.Fatalf("watcher AwaitDurable: %v", err)
	}
}

// recordingObserver records the per-hook calls a test cares about.
// Used by the rev-4 regression tests to assert specific hooks fire
// with the right values. All other hooks no-op.
type recordingObserver struct {
	mu             sync.Mutex
	inflightBytes  []int64
	inflightBatch  []int
	queueDepth     map[PipelineStage][]int
	batchOutcomes  []BatchOutcome
	flushDurations []time.Duration
}

func newRecordingObserver() *recordingObserver {
	return &recordingObserver{queueDepth: map[PipelineStage][]int{}}
}

func (o *recordingObserver) OnAccepted()                                        {}
func (o *recordingObserver) OnStorePut(int, time.Duration, error)               {}
func (o *recordingObserver) OnManifestEnqueue(int, time.Duration, int, error)   {}
func (o *recordingObserver) OnAppendChBlock(time.Duration)                      {}
func (o *recordingObserver) OnWorkersBusy(PipelineStage, int)                   {}
func (o *recordingObserver) OnEncodeDuration(time.Duration, error)              {}
func (o *recordingObserver) OnUploadDuration(time.Duration, int, error)         {}
func (o *recordingObserver) OnManifestAppendBatchSize(int)                      {}
func (o *recordingObserver) OnManifestAppendDuration(time.Duration, int, error) {}
func (o *recordingObserver) OnHeadOfLineBlock(time.Duration)                    {}
func (o *recordingObserver) OnHalted(bool)                                      {}
func (o *recordingObserver) OnFlush(_ FlushReason, _ FlushStats, d time.Duration, _ error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.flushDurations = append(o.flushDurations, d)
}
func (o *recordingObserver) OnBatchOutcome(outcome BatchOutcome) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.batchOutcomes = append(o.batchOutcomes, outcome)
}
func (o *recordingObserver) OnInflightBytes(b int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.inflightBytes = append(o.inflightBytes, b)
}
func (o *recordingObserver) OnInflightBatches(b int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.inflightBatch = append(o.inflightBatch, b)
}
func (o *recordingObserver) OnQueueDepth(stage PipelineStage, depth int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.queueDepth[stage] = append(o.queueDepth[stage], depth)
}

func (o *recordingObserver) maxInflightBytes() int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	var m int64
	for _, b := range o.inflightBytes {
		if b > m {
			m = b
		}
	}
	return m
}

func (o *recordingObserver) maxInflightBatches() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	var m int
	for _, b := range o.inflightBatch {
		if b > m {
			m = b
		}
	}
	return m
}

func (o *recordingObserver) outcomeCounts() map[BatchOutcome]int {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := map[BatchOutcome]int{}
	for _, o2 := range o.batchOutcomes {
		out[o2]++
	}
	return out
}

func (o *recordingObserver) queueDepthStages() []PipelineStage {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]PipelineStage, 0, len(o.queueDepth))
	for s := range o.queueDepth {
		out = append(out, s)
	}
	return out
}

// TestProducer_inflight_gauges_emit_on_acquire is the F1 (rev-4)
// regression test. The rev-3 implementation only emitted
// `buffer.producer.inflight_bytes` / `inflight_batches` on the
// resolver-side release path, so a stuck upload would never produce
// a positive gauge sample (gauge stays at 0, then snaps to 0 on
// release). The fix emits a snapshot at every budget mutation:
// AppendContext byte reservation, rotator batch-slot acquire,
// rotator framed-byte addReservation, and resolver release.
//
// This test holds the upload via choreographableStore so the batch
// is parked in flight, then asserts that the recordingObserver has
// seen positive inflight gauge samples.
func TestProducer_inflight_gauges_emit_on_acquire(t *testing.T) {
	store := newChoreographableStore()
	obs := newRecordingObserver()

	cfg := testConfig()
	cfg.FlushSizeBytes = 1
	cfg.Observer = obs
	p := NewProducer(store, cfg)
	defer func() { _ = p.Close(context.Background()) }()

	prefix := cfg.DataPathPrefix + "/" + p.runID + "/"

	if _, err := p.Append([][]byte{[]byte("entry")}, []byte("md")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if !store.waitForObserved(prefix, 1, 5*time.Second) {
		t.Fatalf("PUT never observed; the size-triggered batch did not reach the uploader")
	}

	// At this point: one batch is parked in the uploader. The budget
	// snapshot should reflect the in-flight state.
	if maxBytes := obs.maxInflightBytes(); maxBytes <= 0 {
		t.Fatalf("expected positive max OnInflightBytes sample while a batch is in flight, got %d", maxBytes)
	}
	if maxBatches := obs.maxInflightBatches(); maxBatches < 1 {
		t.Fatalf("expected max OnInflightBatches >= 1 while a batch is in flight, got %d", maxBatches)
	}

	// Release so Close can drain.
	for _, p := range store.observedPaths() {
		store.releasePut(p)
	}
}

// TestProducer_byte_budget_charges_framing_overhead is the F2 (rev-4)
// regression test for framed byte-budget reconciliation. A workload
// of N tiny entries should charge the budget for the encoder's framed
// allocation: sum(len(entries)) + len(metadata) + batchEntryLenSize*N
// + batchFooterSize, not just sum(len(entries)) + len(metadata).
//
// The choreographable store holds the PUT so the batch stays in
// flight while we sample the inflight-bytes gauge. The peak sample
// must reach the framed cost, proving rotation reconciled correctly.
func TestProducer_byte_budget_charges_framing_overhead(t *testing.T) {
	const numEntries = 16
	store := newChoreographableStore()
	obs := newRecordingObserver()

	cfg := testConfig()
	cfg.Observer = obs
	cfg.FlushSizeBytes = 1024 * 1024 // size won't trigger; we'll Flush
	p := NewProducer(store, cfg)
	defer func() { _ = p.Close(context.Background()) }()

	entries := make([][]byte, numEntries)
	for i := range entries {
		entries[i] = []byte{byte(i)}
	}
	metadata := []byte("md")
	if _, err := p.Append(entries, metadata); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Drive a manual Flush in a goroutine — the rotator's emit reconciles
	// to framed cost before sending downstream, then the upload parks.
	flushDone := make(chan error, 1)
	go func() {
		flushDone <- p.Flush(context.Background())
	}()

	prefix := cfg.DataPathPrefix + "/" + p.runID + "/"
	if !store.waitForObserved(prefix, 1, 5*time.Second) {
		t.Fatalf("PUT never observed; rotation did not reach the uploader")
	}

	// At this point the batch is parked in the uploader and the
	// framed reservation is active. Compute the expected framed cost.
	rawBytes := int64(numEntries) + int64(len(metadata))
	framingExtra := int64(batchEntryLenSize)*int64(numEntries) + int64(batchFooterSize)
	wantFramed := rawBytes + framingExtra

	gotMax := obs.maxInflightBytes()
	if gotMax < wantFramed {
		t.Fatalf("max OnInflightBytes = %d, want >= %d (raw %d + framing %d); rotation reconciliation regressed",
			gotMax, wantFramed, rawBytes, framingExtra)
	}

	// Release so Flush + Close can drain.
	for _, p := range store.observedPaths() {
		store.releasePut(p)
	}
	if err := <-flushDone; err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

// TestProducer_encode_failure_advances_committer_past_ordinal is the
// F3 (rev-4) regression test. The rev-3 fix routes encode failures
// through uploadCompletionCh as synthetic completions so the
// committer's `next` cursor advances past the failed ordinal. Without
// this fix, a single encode-failed ordinal would block all later
// successful batches in the committer's ready[] map.
//
// We choreograph mixed encode pass/fail via the test-only encodeFn
// hook: ordinal 0 fails, ordinals 1-2 succeed. With the fix: all
// three watchers resolve, manifest contains 2 entries (sequences 0
// and 1, since ordinal 0 was skipped). Without the fix: the two
// successful ordinals are stuck in the committer's ready[] map and
// their watchers never resolve.
func TestProducer_encode_failure_advances_committer_past_ordinal(t *testing.T) {
	store := objstore.NewInMemory()
	cfg := testConfig()
	cfg.FlushSizeBytes = 1 // each Append → its own batch

	p := NewProducer(store, cfg)
	// Inject: fail the first call to encode, then pass through.
	var calls atomic.Int64
	p.encodeFn = func(entries [][]byte, comp CompressionType) ([]byte, error) {
		if calls.Add(1) == 1 {
			return nil, errors.New("synthetic encode failure")
		}
		return EncodeBatch(entries, comp)
	}
	defer func() { _ = p.Close(context.Background()) }()

	const n = 3
	handles := make([]*WriteHandle, n)
	for i := 0; i < n; i++ {
		h, err := p.Append([][]byte{{byte(i)}}, []byte("md"))
		if err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
		handles[i] = h
	}

	// All three watchers must resolve within a tight bound. Without
	// the F3 fix, watchers 1 and 2 stay blocked because their
	// successful uploads sit behind the missing ordinal in the
	// committer's ready[] map.
	deadline := time.Now().Add(10 * time.Second)
	for i, h := range handles {
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		err := h.Watcher.AwaitDurable(ctx)
		cancel()
		if i == 0 {
			if err == nil || err.Error() != "synthetic encode failure" {
				t.Fatalf("watcher[0] = %v, want \"synthetic encode failure\"", err)
			}
		} else {
			if err != nil {
				t.Fatalf("watcher[%d] = %v, want nil (committer should advance past ordinal 0)", i, err)
			}
		}
	}

	// Manifest should contain exactly two entries (the two successful
	// ordinals). Sequences are assigned by the manifest in append
	// order, so they are 0 and 1.
	entries := readManifestEntries(t, store)
	if len(entries) != 2 {
		t.Fatalf("expected 2 manifest entries (ordinals 1+2 succeeded; ordinal 0 skipped), got %d", len(entries))
	}
	for i, e := range entries {
		if e.Sequence != uint64(i) {
			t.Fatalf("entries[%d].Sequence = %d, want %d", i, e.Sequence, i)
		}
	}
}

// TestProducer_OnQueueDepth_fires_per_stage is the F4 (rev-4)
// regression test for the queue_depth observer hook. A successful
// batch through the pipeline must fire OnQueueDepth at the four
// stage handoffs: append (AppendContext), encode (rotator → encoder),
// upload (encoder → uploader), commit (uploader → committer).
func TestProducer_OnQueueDepth_fires_per_stage(t *testing.T) {
	store := objstore.NewInMemory()
	obs := newRecordingObserver()
	cfg := testConfig()
	cfg.Observer = obs
	cfg.FlushSizeBytes = 1
	p := NewProducer(store, cfg)
	defer func() { _ = p.Close(context.Background()) }()

	h, err := p.Append([][]byte{[]byte("entry")}, []byte("md"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := h.Watcher.AwaitDurable(context.Background()); err != nil {
		t.Fatalf("AwaitDurable: %v", err)
	}

	got := map[PipelineStage]bool{}
	for _, s := range obs.queueDepthStages() {
		got[s] = true
	}
	want := []PipelineStage{StageAppend, StageEncode, StageUpload, StageCommit}
	for _, s := range want {
		if !got[s] {
			t.Errorf("OnQueueDepth never fired for stage %q", s)
		}
	}
}

// TestNewProducer_applies_retry_defaults_to_literal_config is the
// F6 (rev-4) regression test. A ProducerConfig literal that omits
// the retry-budget fields must inherit DefaultUploadMaxAttempts /
// DefaultManifestMaxAttempts (both 6) so retries actually happen.
// Without the fix, NewProducer left them at 0 and the inner
// putWithRetry / casWithRetry treated 0 as "1 attempt" — i.e. no
// retry budget at all.
//
// We assert by counting PUT calls under a faulty store that fails
// the first two attempts. With the default budget (6), the third
// attempt succeeds and the watcher resolves nil. Without the fix
// (1 attempt), the first failure becomes terminal.
func TestNewProducer_applies_retry_defaults_to_literal_config(t *testing.T) {
	store := newFaultyStore()
	store.failPutErr = errors.New("transient PUT error")
	store.failPutCount.Store(2)

	// Construct a ProducerConfig literal — note: no UploadMaxAttempts
	// / UploadInitialBackoff / Manifest* values. testConfig() has the
	// same shape, but to be unambiguous we build a literal here.
	cfg := ProducerConfig{
		DataPathPrefix:    "test-ingest",
		ManifestPath:      "test/manifest",
		FlushInterval:     24 * time.Hour,
		FlushSizeBytes:    1,
		MaxBufferedInputs: 16,
		BatchCompression:  CompressionNone,
	}
	// Use a small backoff override only by injecting it via the
	// applied-default path below — we still rely on NewProducer to
	// apply the default attempts count. The default backoff is
	// 100ms; the test tolerates that.
	p := NewProducer(store, cfg)
	defer func() { _ = p.Close(context.Background()) }()

	h, err := p.Append([][]byte{[]byte("entry")}, nil)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := h.Watcher.AwaitDurable(context.Background()); err != nil {
		t.Fatalf("AwaitDurable: %v (retry defaults likely not applied — produced no retry budget)", err)
	}
	// Three calls: 2 fails + 1 success. Without the fix, only 1
	// attempt is made and AwaitDurable returns the first error.
	if got := store.putCalls.Load(); got < 3 {
		t.Fatalf("expected >= 3 PUT calls (2 fail + 1 success); got %d. Retry defaults likely not applied to literal config.", got)
	}
}
