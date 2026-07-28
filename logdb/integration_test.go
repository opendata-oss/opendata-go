//go:build integration

// Integration tests for the logdb client against a real OpenData Log server.
//
// The rest of the suite proves the client agrees with our reading of the
// server's JSON shapes. These tests are what catch a disagreement -- a renamed
// field, a changed envelope, long-poll semantics that differ from the RFC. The
// server's schema lives in Rust prost macros with no shared schema file, and
// both RFC 0004 and the published OpenAPI spec are stale relative to it, so the
// running server is the only authority.
//
// Run with:
//
//	make logdb-integration
//
// Requires Docker, or an already-running server addressed by LOGDB_BASE_URL.
package logdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const (
	// logImageEnv overrides the server image under test.
	logImageEnv = "LOGDB_LOG_IMAGE"

	// defaultLogImage is the image published by the opendata build-binaries
	// workflow.
	defaultLogImage = "ghcr.io/opendata-oss/log:latest"

	// baseURLEnv points the tests at an already-running server instead of
	// starting a container.
	baseURLEnv = "LOGDB_BASE_URL"
)

// testClient returns a client for a live server, starting one in Docker unless
// LOGDB_BASE_URL names an existing one.
func testClient(t *testing.T) *Client {
	t.Helper()

	baseURL := os.Getenv(baseURLEnv)
	if baseURL == "" {
		baseURL = startLogServer(t)
	}

	c, err := New(baseURL)
	if err != nil {
		t.Fatalf("New(%q) = _, %v; want no error", baseURL, err)
	}
	return c
}

// startLogServer runs the Log server in Docker with in-memory storage and
// returns its base URL.
func startLogServer(t *testing.T) string {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker is required for integration tests (or set %s): %v", baseURLEnv, err)
	}
	if out, err := exec.Command("docker", "info").CombinedOutput(); err != nil {
		t.Skipf("docker daemon is required for integration tests (or set %s): %v: %s",
			baseURLEnv, err, strings.TrimSpace(string(out)))
	}

	image := os.Getenv(logImageEnv)
	if image == "" {
		image = defaultLogImage
	}

	port := freePort(t)
	name := fmt.Sprintf("logdb-integration-%d", time.Now().UnixNano())

	// --in-memory keeps each run isolated with no volume to clean up.
	out, err := exec.Command("docker", "run", "-d", "--rm",
		"--name", name,
		"-p", fmt.Sprintf("%d:8080", port),
		image,
		"--in-memory", "--port", "8080",
	).CombinedOutput()
	if err != nil {
		t.Fatalf("start log server from %s: %v\n%s", image, err, out)
	}

	t.Cleanup(func() {
		if t.Failed() {
			if logs, err := exec.Command("docker", "logs", name).CombinedOutput(); err == nil {
				t.Logf("log server output:\n%s", logs)
			}
		}
		_, _ = exec.Command("docker", "rm", "-f", name).CombinedOutput()
	})

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForReady(t, baseURL)
	return baseURL
}

// waitForReady blocks until the server's readiness probe passes, which also
// confirms its storage backend is reachable.
func waitForReady(t *testing.T, baseURL string) {
	t.Helper()

	probe, err := New(baseURL, WithHTTPClient(&http.Client{Timeout: 2 * time.Second}))
	if err != nil {
		t.Fatalf("New(%q) = _, %v; want no error", baseURL, err)
	}

	deadline := time.Now().Add(60 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := probe.Ready(ctx)
		cancel()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s to become ready: %v", baseURL, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func freePort(t *testing.T) int {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate TCP port: %v", err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().(*net.TCPAddr).Port
}

// uniqueKey keeps tests independent when they share one server: every test owns
// its own stream, though they still share the global sequence counter.
func uniqueKey(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())
}

// TestIntegration_HealthProbes asserts both probe routes exist and answer.
func TestIntegration_HealthProbes(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()

	if err := c.Healthy(ctx); err != nil {
		t.Errorf("Healthy() = %v; want nil", err)
	}
	if err := c.Ready(ctx); err != nil {
		t.Errorf("Ready() = %v; want nil", err)
	}
}

// TestIntegration_AppendScanRoundTrip is the core contract check: a multi-record
// append, read back with its sequences, including a value that is not valid
// UTF-8. If base64 handling or a JSON field name disagrees with the server, this
// is where it shows.
func TestIntegration_AppendScanRoundTrip(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	key := uniqueKey(t)

	binary := []byte{0x00, 0xff, 0xfe, 0x80, 0x01, 0x7f}
	records := []Record{
		{Key: []byte(key), Value: []byte("order-1")},
		{Key: []byte(key), Value: binary},
		{Key: []byte(key), Value: []byte("")},
	}

	// AwaitDurable so the records are readable without polling for visibility.
	res, err := c.Append(ctx, records, AppendOptions{AwaitDurable: true})
	if err != nil {
		t.Fatalf("Append() = _, %v; want no error", err)
	}
	if res.RecordsAppended != 3 {
		t.Errorf("RecordsAppended = %d; want 3", res.RecordsAppended)
	}

	scan, err := c.Scan(ctx, key, ScanOptions{})
	if err != nil {
		t.Fatalf("Scan() = _, %v; want no error", err)
	}

	if !bytes.Equal(scan.Key, []byte(key)) {
		t.Errorf("Scan Key = %q; want %q", scan.Key, key)
	}
	if len(scan.Values) != 3 {
		t.Fatalf("Scan returned %d entries; want 3", len(scan.Values))
	}
	for i, want := range [][]byte{[]byte("order-1"), binary, {}} {
		if !bytes.Equal(scan.Values[i].Value, want) {
			t.Errorf("Values[%d].Value = %#v; want %#v", i, scan.Values[i].Value, want)
		}
	}

	// The batch occupies a contiguous run starting at the reported sequence, and
	// sequences must ascend.
	if scan.Values[0].Sequence != res.StartSequence {
		t.Errorf("first entry sequence = %d; want the reported StartSequence %d",
			scan.Values[0].Sequence, res.StartSequence)
	}
	for i := 1; i < len(scan.Values); i++ {
		if scan.Values[i].Sequence <= scan.Values[i-1].Sequence {
			t.Errorf("sequences are not ascending: %d then %d",
				scan.Values[i-1].Sequence, scan.Values[i].Sequence)
		}
	}

	// nextSequence is absent from the published OpenAPI spec, so confirm the
	// server really sends it -- the follower depends on it entirely.
	if scan.NextSequence <= scan.Values[len(scan.Values)-1].Sequence {
		t.Errorf("NextSequence = %d; want it past the last entry at %d",
			scan.NextSequence, scan.Values[len(scan.Values)-1].Sequence)
	}
}

// TestIntegration_ScanRespectsLimitAndRange asserts the narrowing parameters are
// spelled the way the server parses them. A misspelled query parameter is
// silently ignored, so a wrong name looks like an unbounded scan rather than an
// error.
func TestIntegration_ScanRespectsLimitAndRange(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	key := uniqueKey(t)

	records := make([]Record, 5)
	for i := range records {
		records[i] = Record{Key: []byte(key), Value: []byte{byte('a' + i)}}
	}
	if _, err := c.Append(ctx, records, AppendOptions{AwaitDurable: true}); err != nil {
		t.Fatalf("Append() = _, %v; want no error", err)
	}

	limited, err := c.Scan(ctx, key, ScanOptions{Limit: 2})
	if err != nil {
		t.Fatalf("Scan() = _, %v; want no error", err)
	}
	if len(limited.Values) != 2 {
		t.Fatalf("Scan with Limit 2 returned %d entries; want 2", len(limited.Values))
	}

	// Resume from the reported cursor and the remaining entries follow, with no
	// repeat of the first page.
	rest, err := c.Scan(ctx, key, ScanOptions{StartSeq: &limited.NextSequence})
	if err != nil {
		t.Fatalf("Scan() = _, %v; want no error", err)
	}
	if len(rest.Values) != 3 {
		t.Fatalf("resumed scan returned %d entries; want 3", len(rest.Values))
	}
	if rest.Values[0].Sequence <= limited.Values[1].Sequence {
		t.Errorf("resumed scan repeated sequence %d; want to start past %d",
			rest.Values[0].Sequence, limited.Values[1].Sequence)
	}

	// An end bound is exclusive.
	bounded, err := c.Scan(ctx, key, ScanOptions{
		StartSeq: &limited.Values[0].Sequence,
		EndSeq:   &limited.Values[1].Sequence,
	})
	if err != nil {
		t.Fatalf("Scan() = _, %v; want no error", err)
	}
	if len(bounded.Values) != 1 {
		t.Errorf("bounded scan returned %d entries; want 1 (end is exclusive)", len(bounded.Values))
	}
}

// TestIntegration_Count asserts an exact count, which is how a consumer measures
// its own lag.
func TestIntegration_Count(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	key := uniqueKey(t)

	records := make([]Record, 4)
	for i := range records {
		records[i] = Record{Key: []byte(key), Value: []byte{byte('a' + i)}}
	}
	res, err := c.Append(ctx, records, AppendOptions{AwaitDurable: true})
	if err != nil {
		t.Fatalf("Append() = _, %v; want no error", err)
	}

	total, err := c.Count(ctx, key, CountOptions{})
	if err != nil {
		t.Fatalf("Count() = _, %v; want no error", err)
	}
	if total != 4 {
		t.Errorf("Count() = %d; want 4", total)
	}

	from := res.StartSequence + 2
	pending, err := c.Count(ctx, key, CountOptions{StartSeq: &from})
	if err != nil {
		t.Fatalf("Count() = _, %v; want no error", err)
	}
	if pending != 2 {
		t.Errorf("Count() from sequence %d = %d; want 2", from, pending)
	}
}

// TestIntegration_ListKeys asserts the nested keys envelope. RFC 0004 documents
// the inner field as "value" while the server sends "key", so the RFC's shape
// would decode to a slice of empty keys and this test is the only thing that
// would notice.
func TestIntegration_ListKeys(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	key := uniqueKey(t)

	if _, err := c.Append(ctx, []Record{{Key: []byte(key), Value: []byte("v")}}, AppendOptions{AwaitDurable: true}); err != nil {
		t.Fatalf("Append() = _, %v; want no error", err)
	}

	keys, err := c.ListKeys(ctx, ListKeysOptions{})
	if err != nil {
		t.Fatalf("ListKeys() = _, %v; want no error", err)
	}
	if len(keys) == 0 {
		t.Fatal("ListKeys() returned nothing; want at least the key just written")
	}

	var found bool
	for _, k := range keys {
		if bytes.Equal(k, []byte(key)) {
			found = true
		}
		if len(k) == 0 {
			t.Error("ListKeys() returned an empty key; the envelope is likely being unwrapped with the wrong field name")
		}
	}
	if !found {
		t.Errorf("ListKeys() = %q; want it to include %q", keys, key)
	}
}

// TestIntegration_ListSegments asserts the segment listing decodes, including
// the millisecond timestamp.
func TestIntegration_ListSegments(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()

	if _, err := c.Append(ctx, []Record{{Key: []byte(uniqueKey(t)), Value: []byte("v")}}, AppendOptions{AwaitDurable: true}); err != nil {
		t.Fatalf("Append() = _, %v; want no error", err)
	}

	segments, err := c.ListSegments(ctx, ListSegmentsOptions{})
	if err != nil {
		t.Fatalf("ListSegments() = _, %v; want no error", err)
	}
	if len(segments) == 0 {
		t.Fatal("ListSegments() returned nothing; want at least segment 0")
	}

	// A zero StartTime means startTimeMs never arrived, which is what a renamed
	// field would look like.
	if segments[0].StartTime.IsZero() {
		t.Errorf("segment[0].StartTime is zero; want the server's startTimeMs, got %+v", segments[0])
	}
	if segments[0].StartTime.Year() < 2020 {
		t.Errorf("segment[0].StartTime = %v; want a plausible wall-clock time", segments[0].StartTime)
	}
}

// TestIntegration_FollowPicksUpConcurrentWrite is the long-poll contract: a
// follower blocked on an empty stream must wake when a record lands, and must
// not redeliver it.
func TestIntegration_FollowPicksUpConcurrentWrite(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	key := uniqueKey(t)

	f := c.Follow(key, FollowOptions{PollTimeout: 10 * time.Second})

	// Drain first so the follower is caught up and the next poll genuinely
	// blocks rather than returning a backlog.
	if _, err := f.Next(ctx); err != nil {
		t.Fatalf("initial Next() = _, %v; want no error", err)
	}
	caughtUp := f.Cursor()

	writeErr := make(chan error, 1)
	go func() {
		// Long enough that the follower is parked in its poll.
		time.Sleep(500 * time.Millisecond)
		_, err := c.Append(ctx, []Record{{Key: []byte(key), Value: []byte("late-arrival")}}, AppendOptions{AwaitDurable: true})
		writeErr <- err
	}()

	start := time.Now()
	entries, err := f.Next(ctx)
	elapsed := time.Since(start)

	if err := <-writeErr; err != nil {
		t.Fatalf("concurrent Append() = _, %v; want no error", err)
	}
	if err != nil {
		t.Fatalf("Next() = _, %v; want no error", err)
	}

	if len(entries) != 1 || !bytes.Equal(entries[0].Value, []byte("late-arrival")) {
		t.Fatalf("Next() = %+v; want the single late arrival", entries)
	}
	// It should have blocked for the write rather than spinning: a follower that
	// returned immediately would be polling hot.
	if elapsed < 250*time.Millisecond {
		t.Errorf("Next() returned after %v; want it to have blocked until the write landed", elapsed)
	}
	if f.Cursor() <= caughtUp {
		t.Errorf("Cursor() = %d; want it past the caught-up %d", f.Cursor(), caughtUp)
	}

	// And the entry is not served twice.
	next, err := c.Scan(ctx, key, ScanOptions{StartSeq: new(f.Cursor())})
	if err != nil {
		t.Fatalf("Scan() = _, %v; want no error", err)
	}
	if len(next.Values) != 0 {
		t.Errorf("scan from the follower's cursor returned %d entries; want 0", len(next.Values))
	}
}

// TestIntegration_FollowAdvancesOverIdlePoll asserts the RFC 0007 frontier lift
// is real: an idle poll returns nothing but still moves the cursor forward,
// because activity on *other* keys advances the frontier. This is what keeps
// tail polling proportional to new data instead of to the backlog, and it cannot
// be observed against a single-key fake.
func TestIntegration_FollowAdvancesOverIdlePoll(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	idleKey, busyKey := uniqueKey(t)+"-idle", uniqueKey(t)+"-busy"

	f := c.Follow(idleKey, FollowOptions{PollTimeout: time.Second})
	if _, err := f.Next(ctx); err != nil {
		t.Fatalf("initial Next() = _, %v; want no error", err)
	}
	before := f.Cursor()

	// Write to a different key entirely.
	if _, err := c.Append(ctx, []Record{{Key: []byte(busyKey), Value: []byte("unrelated")}}, AppendOptions{AwaitDurable: true}); err != nil {
		t.Fatalf("Append() = _, %v; want no error", err)
	}

	entries, err := f.Next(ctx)
	if err != nil {
		t.Fatalf("Next() = _, %v; want no error", err)
	}
	if len(entries) != 0 {
		t.Fatalf("Next() on an idle key returned %d entries; want 0", len(entries))
	}
	if f.Cursor() <= before {
		t.Errorf("Cursor() = %d; want it advanced past %d by activity on another key", f.Cursor(), before)
	}
}

// TestIntegration_RejectsMissingKey asserts a server-side 400 arrives as an
// APIError carrying the server's own message.
func TestIntegration_RejectsMissingKey(t *testing.T) {
	c := testClient(t)

	// An empty record batch never reaches the server, so exercise the error path
	// with a request the server itself rejects: a record with no key.
	_, err := c.Append(context.Background(), []Record{{Value: []byte("no key")}}, AppendOptions{})
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("Append() error = %v; want it to wrap ErrInvalidInput", err)
	}

	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("Append() error = %v; want an *APIError", err)
	}
	if apiErr.StatusCode != http.StatusBadRequest {
		t.Errorf("StatusCode = %d; want 400", apiErr.StatusCode)
	}
	if apiErr.Message == "" {
		t.Error("Message is empty; want the server's explanation")
	}
	t.Logf("server rejected the request with: %s", apiErr.Message)
}
