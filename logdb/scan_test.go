package logdb

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

// TestScan_OmitsUnsetOptions asserts an unset option is absent from the query
// rather than sent as a zero value. The server's defaults are start 0, end
// max-uint64 and limit 32; the client must not restate or contradict them.
func TestScan_OmitsUnsetOptions(t *testing.T) {
	var gotQuery string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		writeJSON(w, `{"status":"success","key":"b3JkZXJz","values":[],"nextSequence":0}`)
	})

	if _, err := c.Scan(context.Background(), "orders", ScanOptions{}); err != nil {
		t.Fatalf("Scan() = _, %v; want no error", err)
	}

	if gotQuery != "key=orders" {
		t.Errorf("query = %q; want %q", gotQuery, "key=orders")
	}
}

// TestScan_SendsAllOptions pins the full query string, including the snake_case
// parameter names the server expects (JSON fields are camelCase, query
// parameters are not) and the millisecond encoding of PollTimeout.
func TestScan_SendsAllOptions(t *testing.T) {
	var gotQuery string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		writeJSON(w, `{"status":"success","key":"b3JkZXJz","values":[],"nextSequence":200}`)
	})

	start, end := uint64(100), uint64(200)
	_, err := c.Scan(context.Background(), "orders", ScanOptions{
		StartSeq:    &start,
		EndSeq:      &end,
		Limit:       64,
		Follow:      true,
		PollTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Scan() = _, %v; want no error", err)
	}

	// url.Values.Encode sorts by key, so this order is stable.
	const want = "end_seq=200&follow=true&key=orders&limit=64&start_seq=100&timeout_ms=5000"
	if gotQuery != want {
		t.Errorf("query =\n\t%s\nwant\n\t%s", gotQuery, want)
	}
}

// TestScan_SendsExplicitZeroStartSeq asserts *uint64 earns its pointer: a
// caller-supplied 0 is sent, where an unset field is omitted. Both mean "from
// the beginning" today, but conflating them would silently break if the server
// ever changed its default.
func TestScan_SendsExplicitZeroStartSeq(t *testing.T) {
	var gotQuery string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		writeJSON(w, `{"status":"success","key":"aw==","values":[],"nextSequence":0}`)
	})

	zero := uint64(0)
	if _, err := c.Scan(context.Background(), "k", ScanOptions{StartSeq: &zero}); err != nil {
		t.Fatalf("Scan() = _, %v; want no error", err)
	}

	if want := "key=k&start_seq=0"; gotQuery != want {
		t.Errorf("query = %q; want %q", gotQuery, want)
	}
}

// TestScan_EscapesKey asserts a key carrying query-syntax characters survives.
// Keys are passed as a plain query parameter rather than base64, so escaping is
// the only thing standing between an odd key and a mangled request.
func TestScan_EscapesKey(t *testing.T) {
	const key = "tenant a/orders?x=1&y=2"

	var gotKey string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.URL.Query().Get("key")
		writeJSON(w, `{"status":"success","values":[],"nextSequence":0}`)
	})

	if _, err := c.Scan(context.Background(), key, ScanOptions{}); err != nil {
		t.Fatalf("Scan() = _, %v; want no error", err)
	}

	if gotKey != key {
		t.Errorf("server saw key %q; want %q", gotKey, key)
	}
}

// TestScan_DecodesEntriesAndCursor asserts the response mapping, including
// nextSequence. That field is the RFC 0007 resume cursor and is missing from
// the published OpenAPI spec, so nothing but the server's own behaviour
// documents it.
func TestScan_DecodesEntriesAndCursor(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, `{"status":"success","key":"b3JkZXJz","values":[`+
			`{"sequence":42,"value":"b3JkZXItMQ=="},`+
			`{"sequence":47,"value":"//4AAYA="}],"nextSequence":48}`)
	})

	got, err := c.Scan(context.Background(), "orders", ScanOptions{})
	if err != nil {
		t.Fatalf("Scan() = _, %v; want no error", err)
	}

	if !bytes.Equal(got.Key, []byte("orders")) {
		t.Errorf("Key = %q; want %q", got.Key, "orders")
	}
	if got.NextSequence != 48 {
		t.Errorf("NextSequence = %d; want 48", got.NextSequence)
	}
	if len(got.Values) != 2 {
		t.Fatalf("len(Values) = %d; want 2", len(got.Values))
	}
	if got.Values[0].Sequence != 42 || !bytes.Equal(got.Values[0].Value, []byte("order-1")) {
		t.Errorf("Values[0] = %+v; want sequence 42, value %q", got.Values[0], "order-1")
	}
	// Sequences are drawn from one counter shared by every key, so a single
	// key's sequences are monotonic but not contiguous.
	if got.Values[1].Sequence != 47 {
		t.Errorf("Values[1].Sequence = %d; want 47", got.Values[1].Sequence)
	}
	if want := []byte{0xff, 0xfe, 0x00, 0x01, 0x80}; !bytes.Equal(got.Values[1].Value, want) {
		t.Errorf("Values[1].Value = %#v; want %#v", got.Values[1].Value, want)
	}
}

// TestScan_EmptyResultIsNotAnError asserts a drained scan reports no entries and
// an advanced cursor, rather than failing. An idle key is normal.
func TestScan_EmptyResultIsNotAnError(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, `{"status":"success","key":"b3JkZXJz","values":[],"nextSequence":900}`)
	})

	got, err := c.Scan(context.Background(), "orders", ScanOptions{})
	if err != nil {
		t.Fatalf("Scan() = _, %v; want no error", err)
	}
	if len(got.Values) != 0 {
		t.Errorf("len(Values) = %d; want 0", len(got.Values))
	}
	if got.NextSequence != 900 {
		t.Errorf("NextSequence = %d; want 900", got.NextSequence)
	}
}

// TestScan_MapsServerError asserts scan failures travel the shared error path.
func TestScan_MapsServerError(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"status":"error","message":"Missing required parameter: key"}`))
	})

	_, err := c.Scan(context.Background(), "orders", ScanOptions{})
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("Scan() error = %v; want it to wrap ErrInvalidInput", err)
	}

	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("Scan() error = %v; want an *APIError", err)
	}
	if apiErr.Message != "Missing required parameter: key" {
		t.Errorf("Message = %q; want the server's message", apiErr.Message)
	}
}
