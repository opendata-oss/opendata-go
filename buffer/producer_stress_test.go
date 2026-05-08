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
