package logdb

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"
)

// scriptedServer replies with the given bodies in order, recording the raw query
// of each request. It fails the test if called more times than it has bodies.
type scriptedServer struct {
	mu      sync.Mutex
	bodies  []string
	queries []string
}

func (s *scriptedServer) handler(t *testing.T) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()

		i := len(s.queries)
		s.queries = append(s.queries, r.URL.RawQuery)
		if i >= len(s.bodies) {
			t.Errorf("server called %d times; script has %d responses", i+1, len(s.bodies))
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		writeJSON(w, s.bodies[i])
	}
}

func (s *scriptedServer) query(i int) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i >= len(s.queries) {
		return ""
	}
	return s.queries[i]
}

// TestFollow_AdvancesCursorOnEmptyPoll is the whole reason this type exists. A
// poll that finds nothing still advances the cursor, because the server lifts
// nextSequence to the frontier it observed -- which moves on writes to *any*
// key. A follower that resumed from its own last entry instead would rescan the
// same widening empty range on every poll, which is the cost RFC 0007 removes.
func TestFollow_AdvancesCursorOnEmptyPoll(t *testing.T) {
	script := &scriptedServer{bodies: []string{
		`{"status":"success","key":"b3JkZXJz","values":[],"nextSequence":500}`,
		`{"status":"success","key":"b3JkZXJz","values":[],"nextSequence":900}`,
	}}
	c := newTestClient(t, script.handler(t))

	f := c.Follow("orders", FollowOptions{})

	entries, err := f.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() = _, %v; want no error -- an idle log is not a failure", err)
	}
	if len(entries) != 0 {
		t.Errorf("Next() returned %d entries; want 0", len(entries))
	}
	if f.Cursor() != 500 {
		t.Errorf("Cursor() = %d; want 500", f.Cursor())
	}

	if _, err := f.Next(context.Background()); err != nil {
		t.Fatalf("Next() = _, %v; want no error", err)
	}
	if f.Cursor() != 900 {
		t.Errorf("Cursor() = %d; want 900", f.Cursor())
	}

	// The second poll must start where the first one left off.
	if got, want := script.query(1), "follow=true&key=orders&start_seq=500"; got != want {
		t.Errorf("second poll query = %q; want %q", got, want)
	}
}

// TestFollow_DoesNotRepeatEntries asserts entries are delivered once, in order,
// across polls -- the invariant a caller relies on to avoid double-processing.
func TestFollow_DoesNotRepeatEntries(t *testing.T) {
	script := &scriptedServer{bodies: []string{
		`{"status":"success","key":"b3JkZXJz","values":[{"sequence":10,"value":"YQ=="},{"sequence":13,"value":"Yg=="}],"nextSequence":14}`,
		`{"status":"success","key":"b3JkZXJz","values":[],"nextSequence":20}`,
		`{"status":"success","key":"b3JkZXJz","values":[{"sequence":21,"value":"Yw=="}],"nextSequence":22}`,
	}}
	c := newTestClient(t, script.handler(t))

	f := c.Follow("orders", FollowOptions{})

	var seen []uint64
	for range 3 {
		entries, err := f.Next(context.Background())
		if err != nil {
			t.Fatalf("Next() = _, %v; want no error", err)
		}
		for _, e := range entries {
			seen = append(seen, e.Sequence)
		}
	}

	want := []uint64{10, 13, 21}
	if len(seen) != len(want) {
		t.Fatalf("saw sequences %v; want %v", seen, want)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("saw sequences %v; want %v", seen, want)
		}
	}

	// Each poll resumes from the cursor the previous one reported.
	for i, want := range []string{
		"follow=true&key=orders&start_seq=0",
		"follow=true&key=orders&start_seq=14",
		"follow=true&key=orders&start_seq=20",
	} {
		if got := script.query(i); got != want {
			t.Errorf("poll %d query = %q; want %q", i, got, want)
		}
	}
}

// TestFollow_AdvancesPastEntriesWithoutCursorField asserts a follower still makes
// progress when nextSequence is absent -- which is what a server predating RFC
// 0007 returns, and what the published OpenAPI spec still describes. Trusting
// the field alone would leave the cursor at 0 and redeliver the same entries
// forever.
func TestFollow_AdvancesPastEntriesWithoutCursorField(t *testing.T) {
	script := &scriptedServer{bodies: []string{
		`{"status":"success","key":"b3JkZXJz","values":[{"sequence":5,"value":"YQ=="},{"sequence":6,"value":"Yg=="}]}`,
		`{"status":"success","key":"b3JkZXJz","values":[]}`,
	}}
	c := newTestClient(t, script.handler(t))

	f := c.Follow("orders", FollowOptions{})
	if _, err := f.Next(context.Background()); err != nil {
		t.Fatalf("Next() = _, %v; want no error", err)
	}

	if f.Cursor() != 7 {
		t.Errorf("Cursor() = %d; want 7 (one past the last entry)", f.Cursor())
	}
	if _, err := f.Next(context.Background()); err != nil {
		t.Fatalf("Next() = _, %v; want no error", err)
	}
	if got, want := script.query(1), "follow=true&key=orders&start_seq=7"; got != want {
		t.Errorf("second poll query = %q; want %q", got, want)
	}
}

// TestFollow_CursorNeverMovesBackwards asserts a cursor that would regress is
// ignored. Resuming from a lower sequence would redeliver entries the caller has
// already processed.
func TestFollow_CursorNeverMovesBackwards(t *testing.T) {
	script := &scriptedServer{bodies: []string{
		`{"status":"success","key":"b3JkZXJz","values":[],"nextSequence":900}`,
		`{"status":"success","key":"b3JkZXJz","values":[],"nextSequence":100}`,
	}}
	c := newTestClient(t, script.handler(t))

	f := c.Follow("orders", FollowOptions{})
	if _, err := f.Next(context.Background()); err != nil {
		t.Fatalf("Next() = _, %v; want no error", err)
	}
	if _, err := f.Next(context.Background()); err != nil {
		t.Fatalf("Next() = _, %v; want no error", err)
	}

	if f.Cursor() != 900 {
		t.Errorf("Cursor() = %d; want it held at 900", f.Cursor())
	}
}

// TestFollow_StartsFromConfiguredCursor asserts a checkpoint is honoured, so a
// consumer can resume where a previous process stopped.
func TestFollow_StartsFromConfiguredCursor(t *testing.T) {
	script := &scriptedServer{bodies: []string{
		`{"status":"success","key":"b3JkZXJz","values":[],"nextSequence":1000}`,
	}}
	c := newTestClient(t, script.handler(t))

	f := c.Follow("orders", FollowOptions{From: 999})
	if f.Cursor() != 999 {
		t.Errorf("Cursor() before polling = %d; want 999", f.Cursor())
	}
	if _, err := f.Next(context.Background()); err != nil {
		t.Fatalf("Next() = _, %v; want no error", err)
	}
	if got, want := script.query(0), "follow=true&key=orders&start_seq=999"; got != want {
		t.Errorf("query = %q; want %q", got, want)
	}
}

// TestFollow_SendsConfiguredLimitAndTimeout asserts both knobs reach the wire
// when set, and are omitted when not, so the server's own defaults apply.
func TestFollow_SendsConfiguredLimitAndTimeout(t *testing.T) {
	tests := []struct {
		name string
		opts FollowOptions
		want string
	}{
		{
			name: "unset",
			opts: FollowOptions{},
			want: "follow=true&key=orders&start_seq=0",
		},
		{
			name: "configured",
			opts: FollowOptions{Limit: 64, PollTimeout: 5 * time.Second},
			want: "follow=true&key=orders&limit=64&start_seq=0&timeout_ms=5000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			script := &scriptedServer{bodies: []string{
				`{"status":"success","key":"b3JkZXJz","values":[],"nextSequence":1}`,
			}}
			c := newTestClient(t, script.handler(t))

			if _, err := c.Follow("orders", tt.opts).Next(context.Background()); err != nil {
				t.Fatalf("Next() = _, %v; want no error", err)
			}
			if got := script.query(0); got != tt.want {
				t.Errorf("query = %q; want %q", got, tt.want)
			}
		})
	}
}

// TestFollow_ReturnsContextErrorOnCancel asserts cancelling mid-poll yields the
// context's own error rather than a transport error wrapping it. A follower
// spends most of its life blocked in a long poll, so shutdown is the common
// path, not an edge case -- callers should be able to compare against
// context.Canceled directly.
func TestFollow_ReturnsContextErrorOnCancel(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release)

	c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		_, _ = io.WriteString(w, "{}")
	})

	ctx, cancel := context.WithCancel(context.Background())
	f := c.Follow("orders", FollowOptions{})

	errCh := make(chan error, 1)
	go func() {
		_, err := f.Next(ctx)
		errCh <- err
	}()

	<-started
	cancel()

	err := <-errCh
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Next() error = %v; want context.Canceled", err)
	}
	// errors.Is alone would also pass for a transport error wrapping the cause,
	// which is exactly what this test exists to rule out.
	if inner := errors.Unwrap(err); inner != nil {
		t.Errorf("Next() error = %v (%T); want the bare context error, not one wrapping %v", err, err, inner)
	}
}

// TestFollow_CursorIsUnchangedByAFailedPoll asserts a failed poll does not move
// the cursor, so a retry re-reads the range that was never delivered.
func TestFollow_CursorIsUnchangedByAFailedPoll(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"status":"error","message":"storage error"}`)
	})

	f := c.Follow("orders", FollowOptions{From: 42})
	if _, err := f.Next(context.Background()); err == nil {
		t.Fatal("Next() = _, nil; want an error")
	}
	if f.Cursor() != 42 {
		t.Errorf("Cursor() = %d; want it held at 42", f.Cursor())
	}
}
