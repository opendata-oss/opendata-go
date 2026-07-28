package logdb

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestWithHTTPClient asserts the supplied *http.Client is the one used, so
// callers can control timeouts and layer retries in their transport.
func TestWithHTTPClient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(50 * time.Millisecond)
		writeJSON(w, `{"status":"success","recordsAppended":1,"startSequence":0}`)
	}))
	t.Cleanup(srv.Close)

	c, err := New(srv.URL, WithHTTPClient(&http.Client{Timeout: time.Millisecond}))
	if err != nil {
		t.Fatalf("New() = _, %v; want no error", err)
	}

	if _, err := c.Append(context.Background(), []Record{{Key: []byte("k"), Value: []byte("v")}}, AppendOptions{}); err == nil {
		t.Error("Append() = _, nil; want the supplied client's 1ms timeout to fire")
	}
}

// newTestClient starts an httptest server running handler and returns a Client
// pointed at it.
func newTestClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	c, err := New(srv.URL)
	if err != nil {
		t.Fatalf("New(%q) = _, %v; want no error", srv.URL, err)
	}
	return c
}

// TestNew_RejectsUnusableBaseURL asserts that a base URL without a scheme and
// host is rejected at construction. The Log server has no canonical port (the
// docs say 3001, the Log README quickstart says 8080), so there is no default
// to fall back on and a bad URL must fail loudly rather than at first use.
func TestNew_RejectsUnusableBaseURL(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
	}{
		{"empty", ""},
		{"no scheme", "localhost:3001"},
		{"host only", "//localhost:3001"},
		{"unparseable", "http://[::1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := New(tt.baseURL)
			if err == nil {
				t.Fatalf("New(%q) = %v, nil; want an error", tt.baseURL, c)
			}
			if !errors.Is(err, ErrInvalidInput) {
				t.Errorf("New(%q) error = %v; want it to wrap ErrInvalidInput", tt.baseURL, err)
			}
		})
	}
}

// TestNew_AcceptsBaseURLWithPathPrefix asserts a base URL carrying a path
// prefix keeps that prefix when endpoint paths are joined onto it, so the
// client works behind a reverse proxy that mounts the Log API under a subpath.
func TestNew_AcceptsBaseURLWithPathPrefix(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		writeJSON(w, `{"status":"success","recordsAppended":1,"startSequence":0}`)
	}))
	t.Cleanup(srv.Close)

	c, err := New(srv.URL + "/log")
	if err != nil {
		t.Fatalf("New() = _, %v; want no error", err)
	}
	if _, err := c.Append(context.Background(), []Record{{Key: []byte("k"), Value: []byte("v")}}, AppendOptions{}); err != nil {
		t.Fatalf("Append() = _, %v; want no error", err)
	}

	const want = "/log/api/v1/log/append"
	if gotPath != want {
		t.Errorf("request path = %q; want %q", gotPath, want)
	}
}

// TestAppend_SendsProtoJSONRequest pins the exact wire request: the route, the
// headers, and the body. The Accept header matters more than it looks: the
// server's format check is substring-based, so a header containing
// "application/protobuf" without the "+json" suffix flips it to binary
// protobuf, which this client cannot decode.
func TestAppend_SendsProtoJSONRequest(t *testing.T) {
	var (
		gotMethod      string
		gotPath        string
		gotContentType string
		gotAccept      string
		gotBody        []byte
	)

	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotContentType = r.Header.Get("Content-Type")
		gotAccept = r.Header.Get("Accept")
		gotBody, _ = io.ReadAll(r.Body)
		writeJSON(w, `{"status":"success","recordsAppended":2,"startSequence":42}`)
	})

	got, err := c.Append(context.Background(), []Record{
		{Key: []byte("orders"), Value: []byte("order-1")},
		{Key: []byte("events"), Value: []byte("login")},
	}, AppendOptions{AwaitDurable: true})
	if err != nil {
		t.Fatalf("Append() = _, %v; want no error", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q; want %q", gotMethod, http.MethodPost)
	}
	if gotPath != "/api/v1/log/append" {
		t.Errorf("path = %q; want %q", gotPath, "/api/v1/log/append")
	}
	if gotContentType != "application/protobuf+json" {
		t.Errorf("Content-Type = %q; want %q", gotContentType, "application/protobuf+json")
	}
	if gotAccept != "application/protobuf+json" {
		t.Errorf("Accept = %q; want %q", gotAccept, "application/protobuf+json")
	}

	// Base64 vectors taken from the Log README quickstart, so this asserts
	// fidelity to the documented wire format and not merely to our own encoder.
	const wantBody = `{"records":[{"key":"b3JkZXJz","value":"b3JkZXItMQ=="},` +
		`{"key":"ZXZlbnRz","value":"bG9naW4="}],"awaitDurable":true}`
	if string(gotBody) != wantBody {
		t.Errorf("body =\n\t%s\nwant\n\t%s", gotBody, wantBody)
	}

	want := AppendResult{RecordsAppended: 2, StartSequence: 42}
	if got != want {
		t.Errorf("Append() = %+v; want %+v", got, want)
	}
}

// TestAppend_OmitsAwaitDurableWhenUnset asserts awaitDurable is still present
// and false by default, so the request body stays explicit rather than relying
// on the server's serde default.
func TestAppend_SendsAwaitDurableFalseByDefault(t *testing.T) {
	var gotBody []byte
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		writeJSON(w, `{"status":"success","recordsAppended":1,"startSequence":0}`)
	})

	if _, err := c.Append(context.Background(), []Record{{Key: []byte("k"), Value: []byte("v")}}, AppendOptions{}); err != nil {
		t.Fatalf("Append() = _, %v; want no error", err)
	}

	const wantBody = `{"records":[{"key":"aw==","value":"dg=="}],"awaitDurable":false}`
	if string(gotBody) != wantBody {
		t.Errorf("body = %s; want %s", gotBody, wantBody)
	}
}

// TestAppend_RoundTripsArbitraryBytes asserts keys and values are opaque byte
// strings, not text. encoding/json base64-encodes []byte with padded standard
// base64, which is what the server's serde_with Base64 expects.
func TestAppend_RoundTripsArbitraryBytes(t *testing.T) {
	binary := []byte{0xff, 0xfe, 0x00, 0x01, 0x80}

	var gotRecords []Record
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Records []Record `json:"records"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("server could not decode request: %v", err)
		}
		gotRecords = req.Records
		writeJSON(w, `{"status":"success","recordsAppended":1,"startSequence":0}`)
	})

	if _, err := c.Append(context.Background(), []Record{{Key: []byte("k"), Value: binary}}, AppendOptions{}); err != nil {
		t.Fatalf("Append() = _, %v; want no error", err)
	}

	if len(gotRecords) != 1 {
		t.Fatalf("server saw %d records; want 1", len(gotRecords))
	}
	if !bytes.Equal(gotRecords[0].Value, binary) {
		t.Errorf("value round-trip = %#v; want %#v", gotRecords[0].Value, binary)
	}

	// Pin the encoding itself, so a future switch away from encoding/json's
	// []byte handling cannot silently change the wire format.
	if got, want := base64.StdEncoding.EncodeToString(binary), "//4AAYA="; got != want {
		t.Errorf("base64 vector = %q; want %q", got, want)
	}
}

// TestAppend_DecodesMaxUint64Sequence asserts a 64-bit sequence survives the
// JSON round-trip as a number. This is the assumption that made dropping
// protobuf safe: the server (serde) emits u64 as a JSON number and
// encoding/json reads it as one, where protojson would have used a string.
func TestAppend_DecodesMaxUint64Sequence(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, `{"status":"success","recordsAppended":1,"startSequence":18446744073709551615}`)
	})

	got, err := c.Append(context.Background(), []Record{{Key: []byte("k"), Value: []byte("v")}}, AppendOptions{})
	if err != nil {
		t.Fatalf("Append() = _, %v; want no error", err)
	}
	if want := uint64(1<<64 - 1); got.StartSequence != want {
		t.Errorf("StartSequence = %d; want %d", got.StartSequence, want)
	}
}

// TestAppend_IgnoresUnknownResponseFields asserts a field added server-side
// does not break existing calls. encoding/json discards unknown fields by
// default; this test exists so that stays true if decoding options change.
func TestAppend_IgnoresUnknownResponseFields(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, `{"status":"success","recordsAppended":1,"startSequence":7,"futureField":{"nested":true}}`)
	})

	got, err := c.Append(context.Background(), []Record{{Key: []byte("k"), Value: []byte("v")}}, AppendOptions{})
	if err != nil {
		t.Fatalf("Append() = _, %v; want no error", err)
	}
	if got.StartSequence != 7 {
		t.Errorf("StartSequence = %d; want 7", got.StartSequence)
	}
}

// TestAppend_RejectsEmptyRecords asserts an empty batch fails locally rather
// than spending a round trip on a request the server will reject.
func TestAppend_RejectsEmptyRecords(t *testing.T) {
	c := newTestClient(t, func(http.ResponseWriter, *http.Request) {
		t.Error("server was called; want the empty batch rejected client-side")
	})

	if _, err := c.Append(context.Background(), nil, AppendOptions{}); !errors.Is(err, ErrInvalidInput) {
		t.Errorf("Append(nil) error = %v; want it to wrap ErrInvalidInput", err)
	}
}

// TestAppend_MapsErrorEnvelope covers the server's own error responses, which
// are always the JSON envelope regardless of the Accept header.
func TestAppend_MapsErrorEnvelope(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		body       string
		wantErr    error
		wantMsg    string
		wantStatus string
	}{
		{
			name:       "bad request",
			status:     http.StatusBadRequest,
			body:       `{"status":"error","message":"record[0]: key is required"}`,
			wantErr:    ErrInvalidInput,
			wantMsg:    "record[0]: key is required",
			wantStatus: "error",
		},
		{
			name:       "internal error",
			status:     http.StatusInternalServerError,
			body:       `{"status":"error","message":"storage error: unavailable"}`,
			wantErr:    ErrServerError,
			wantMsg:    "storage error: unavailable",
			wantStatus: "error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.status)
				_, _ = io.WriteString(w, tt.body)
			})

			_, err := c.Append(context.Background(), []Record{{Key: []byte("k"), Value: []byte("v")}}, AppendOptions{})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Append() error = %v; want it to wrap %v", err, tt.wantErr)
			}

			var apiErr *APIError
			if !errors.As(err, &apiErr) {
				t.Fatalf("Append() error = %v; want an *APIError", err)
			}
			if apiErr.StatusCode != tt.status {
				t.Errorf("StatusCode = %d; want %d", apiErr.StatusCode, tt.status)
			}
			if apiErr.Status != tt.wantStatus {
				t.Errorf("Status = %q; want %q", apiErr.Status, tt.wantStatus)
			}
			if apiErr.Message != tt.wantMsg {
				t.Errorf("Message = %q; want %q", apiErr.Message, tt.wantMsg)
			}
		})
	}
}

// TestAppend_MapsPlainTextNotFound covers a Log server running as a read-only
// gateway: it does not register the append route at all, so the rejection never
// reaches a handler and comes back as axum's plain-text 404 rather than the
// JSON envelope. Decoding must degrade to the body text, not fail.
func TestAppend_MapsPlainTextNotFound(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, "")
	})

	_, err := c.Append(context.Background(), []Record{{Key: []byte("k"), Value: []byte("v")}}, AppendOptions{})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Append() error = %v; want it to wrap ErrNotFound", err)
	}

	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("Append() error = %v; want an *APIError", err)
	}
	if apiErr.Status != "" {
		t.Errorf("Status = %q; want empty for a non-envelope body", apiErr.Status)
	}
	if apiErr.Message != "Not Found" {
		t.Errorf("Message = %q; want the status-derived %q", apiErr.Message, "Not Found")
	}
}

// TestAppend_MapsPlainTextBodyToMessage asserts a non-empty plain-text error
// body is surfaced as the message instead of being discarded.
func TestAppend_MapsPlainTextBodyToMessage(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, "Method Not Allowed")
	})

	_, err := c.Append(context.Background(), []Record{{Key: []byte("k"), Value: []byte("v")}}, AppendOptions{})

	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("Append() error = %v; want an *APIError", err)
	}
	if apiErr.Message != "Method Not Allowed" {
		t.Errorf("Message = %q; want %q", apiErr.Message, "Method Not Allowed")
	}
	if !errors.Is(err, ErrInvalidInput) {
		t.Errorf("Append() error = %v; want a 4xx to wrap ErrInvalidInput", err)
	}
}

// TestAppend_HonoursContextCancellation asserts the caller's context governs
// the request.
func TestAppend_HonoursContextCancellation(t *testing.T) {
	c := newTestClient(t, func(http.ResponseWriter, *http.Request) {
		t.Error("server was called; want the cancelled context to short-circuit")
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.Append(ctx, []Record{{Key: []byte("k"), Value: []byte("v")}}, AppendOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Append() error = %v; want it to wrap context.Canceled", err)
	}
}

// writeJSON writes a 200 response carrying the server's ProtoJSON content type.
func writeJSON(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "application/protobuf+json")
	_, _ = io.WriteString(w, body)
}
