package logdb

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
)

// fakeLog is a minimal in-memory stand-in for the Log server: enough of append,
// scan, count and keys to exercise a produce-then-consume cycle without Docker.
// It mirrors the two behaviours a client is most likely to get wrong -- one
// global sequence counter shared by every key, so a single key's sequences are
// monotonic but not contiguous, and a nextSequence that reaches the observed
// frontier when a scan drains.
//
// Task 6 repeats these assertions against the real server; this is the fast
// version that runs in the normal suite.
type fakeLog struct {
	mu      sync.Mutex
	entries []fakeEntry
}

type fakeEntry struct {
	key   string
	value []byte
	seq   uint64
}

func (l *fakeLog) server(t *testing.T) *Client {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/log/append", l.handleAppend)
	mux.HandleFunc("GET /api/v1/log/scan", l.handleScan)
	mux.HandleFunc("GET /api/v1/log/count", l.handleCount)
	mux.HandleFunc("GET /api/v1/log/keys", l.handleKeys)

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	c, err := New(srv.URL)
	if err != nil {
		t.Fatalf("New() = _, %v; want no error", err)
	}
	return c
}

func (l *fakeLog) handleAppend(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Records []Record `json:"records"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"status":"error","message":"Invalid JSON"}`))
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	start := uint64(len(l.entries))
	for _, rec := range req.Records {
		l.entries = append(l.entries, fakeEntry{
			key:   string(rec.Key),
			value: rec.Value,
			seq:   uint64(len(l.entries)),
		})
	}

	writeJSON(w, `{"status":"success","recordsAppended":`+strconv.Itoa(len(req.Records))+
		`,"startSequence":`+strconv.FormatUint(start, 10)+`}`)
}

func (l *fakeLog) handleScan(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	start, _ := strconv.ParseUint(r.URL.Query().Get("start_seq"), 10, 64)
	limit := 32
	if v := r.URL.Query().Get("limit"); v != "" {
		limit, _ = strconv.Atoi(v)
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	var values []Entry
	truncated := false
	for _, e := range l.entries {
		if e.key != key || e.seq < start {
			continue
		}
		if len(values) == limit {
			truncated = true
			break
		}
		values = append(values, Entry{Sequence: e.seq, Value: e.value})
	}

	// Drained: lift the cursor to the frontier, which is what lets an idle key
	// skip a range it has provably not missed anything in. Truncated: one past
	// the last entry handed out.
	next := uint64(len(l.entries))
	if truncated {
		next = values[len(values)-1].Sequence + 1
	}

	if values == nil {
		values = []Entry{}
	}
	body, _ := json.Marshal(map[string]any{
		"status":       "success",
		"key":          []byte(key),
		"values":       values,
		"nextSequence": next,
	})
	w.Header().Set("Content-Type", contentTypeJSON)
	_, _ = w.Write(body)
}

func (l *fakeLog) handleCount(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	start, _ := strconv.ParseUint(r.URL.Query().Get("start_seq"), 10, 64)

	l.mu.Lock()
	defer l.mu.Unlock()

	var n uint64
	for _, e := range l.entries {
		if e.key == key && e.seq >= start {
			n++
		}
	}
	writeJSON(w, `{"status":"success","count":`+strconv.FormatUint(n, 10)+`}`)
}

func (l *fakeLog) handleKeys(w http.ResponseWriter, _ *http.Request) {
	l.mu.Lock()
	defer l.mu.Unlock()

	seen := map[string]bool{}
	var keys []map[string][]byte
	for _, e := range l.entries {
		if seen[e.key] {
			continue
		}
		seen[e.key] = true
		keys = append(keys, map[string][]byte{"key": []byte(e.key)})
	}

	body, _ := json.Marshal(map[string]any{"status": "success", "keys": keys})
	w.Header().Set("Content-Type", contentTypeJSON)
	_, _ = w.Write(body)
}

// TestEndToEnd_AppendScanFollow exercises the whole client against the fake log:
// interleave two streams, read one back, count the lag from a checkpoint, then
// tail the stream across an idle poll and a later write.
func TestEndToEnd_AppendScanFollow(t *testing.T) {
	ctx := context.Background()
	log := &fakeLog{}
	c := log.server(t)

	// Two keys interleaved, so "orders" gets sequences 0 and 2 -- monotonic but
	// not contiguous, because the counter is shared.
	res, err := c.Append(ctx, []Record{
		{Key: []byte("orders"), Value: []byte("order-1")},
		{Key: []byte("events"), Value: []byte("login")},
		{Key: []byte("orders"), Value: []byte("order-2")},
	}, AppendOptions{})
	if err != nil {
		t.Fatalf("Append() = _, %v; want no error", err)
	}
	if res.RecordsAppended != 3 || res.StartSequence != 0 {
		t.Fatalf("Append() = %+v; want 3 records from sequence 0", res)
	}

	scan, err := c.Scan(ctx, "orders", ScanOptions{})
	if err != nil {
		t.Fatalf("Scan() = _, %v; want no error", err)
	}
	if len(scan.Values) != 2 {
		t.Fatalf("Scan() returned %d entries; want 2", len(scan.Values))
	}
	if scan.Values[0].Sequence != 0 || scan.Values[1].Sequence != 2 {
		t.Errorf("sequences = %d, %d; want 0, 2 (the shared counter leaves gaps)",
			scan.Values[0].Sequence, scan.Values[1].Sequence)
	}
	if !bytes.Equal(scan.Values[1].Value, []byte("order-2")) {
		t.Errorf("Values[1].Value = %q; want %q", scan.Values[1].Value, "order-2")
	}

	keys, err := c.ListKeys(ctx, ListKeysOptions{})
	if err != nil {
		t.Fatalf("ListKeys() = _, %v; want no error", err)
	}
	if len(keys) != 2 {
		t.Errorf("ListKeys() = %q; want 2 keys", keys)
	}

	// Lag from a checkpoint: one "orders" entry remains after sequence 1.
	pending, err := c.Count(ctx, "orders", CountOptions{StartSeq: new(uint64(1))})
	if err != nil {
		t.Fatalf("Count() = _, %v; want no error", err)
	}
	if pending != 1 {
		t.Errorf("Count() = %d; want 1", pending)
	}

	// Tail from the start: first poll drains the backlog.
	f := c.Follow("orders", FollowOptions{})
	entries, err := f.Next(ctx)
	if err != nil {
		t.Fatalf("Next() = _, %v; want no error", err)
	}
	if len(entries) != 2 {
		t.Fatalf("first Next() returned %d entries; want the 2-entry backlog", len(entries))
	}

	// Drained, so the cursor sits at the frontier -- past "orders"' own last
	// entry at 2, because writes to "events" moved the frontier too.
	if f.Cursor() != 3 {
		t.Errorf("Cursor() = %d; want 3 (the observed frontier, not last entry + 1)", f.Cursor())
	}

	// An idle poll yields nothing and is not an error.
	entries, err = f.Next(ctx)
	if err != nil {
		t.Fatalf("idle Next() = _, %v; want no error", err)
	}
	if len(entries) != 0 {
		t.Errorf("idle Next() returned %d entries; want 0", len(entries))
	}

	// A later write is picked up, exactly once.
	if _, err := c.Append(ctx, []Record{{Key: []byte("orders"), Value: []byte("order-3")}}, AppendOptions{}); err != nil {
		t.Fatalf("Append() = _, %v; want no error", err)
	}

	entries, err = f.Next(ctx)
	if err != nil {
		t.Fatalf("Next() = _, %v; want no error", err)
	}
	if len(entries) != 1 || !bytes.Equal(entries[0].Value, []byte("order-3")) {
		t.Fatalf("Next() = %+v; want just order-3", entries)
	}

	entries, err = f.Next(ctx)
	if err != nil {
		t.Fatalf("Next() = _, %v; want no error", err)
	}
	if len(entries) != 0 {
		t.Errorf("Next() redelivered %d entries; want 0", len(entries))
	}
}

// TestEndToEnd_FollowerRespectsLimit asserts a configured limit paginates the
// backlog rather than truncating it: successive polls walk the whole stream.
func TestEndToEnd_FollowerRespectsLimit(t *testing.T) {
	ctx := context.Background()
	log := &fakeLog{}
	c := log.server(t)

	records := make([]Record, 5)
	for i := range records {
		records[i] = Record{Key: []byte("orders"), Value: []byte{byte('a' + i)}}
	}
	if _, err := c.Append(ctx, records, AppendOptions{}); err != nil {
		t.Fatalf("Append() = _, %v; want no error", err)
	}

	f := c.Follow("orders", FollowOptions{Limit: 2})

	var got []byte
	for range 3 {
		entries, err := f.Next(ctx)
		if err != nil {
			t.Fatalf("Next() = _, %v; want no error", err)
		}
		for _, e := range entries {
			got = append(got, e.Value...)
		}
	}

	if string(got) != "abcde" {
		t.Errorf("paged values = %q; want %q", got, "abcde")
	}
}
