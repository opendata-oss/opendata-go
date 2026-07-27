package logdb

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestCount_OmitsUnsetOptions asserts an unbounded count sends only the key.
func TestCount_OmitsUnsetOptions(t *testing.T) {
	var gotPath, gotQuery string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotQuery = r.URL.Path, r.URL.RawQuery
		writeJSON(w, `{"status":"success","count":1024}`)
	})

	got, err := c.Count(context.Background(), "orders", CountOptions{})
	if err != nil {
		t.Fatalf("Count() = _, %v; want no error", err)
	}

	if gotPath != "/api/v1/log/count" {
		t.Errorf("path = %q; want %q", gotPath, "/api/v1/log/count")
	}
	if gotQuery != "key=orders" {
		t.Errorf("query = %q; want %q", gotQuery, "key=orders")
	}
	if got != 1024 {
		t.Errorf("Count() = %d; want 1024", got)
	}
}

// TestCount_SendsSequenceRange asserts the snake_case range parameters. Counting
// from a checkpoint is how a consumer measures its own lag.
func TestCount_SendsSequenceRange(t *testing.T) {
	var gotQuery string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		writeJSON(w, `{"status":"success","count":7}`)
	})

	start, end := uint64(100), uint64(500)
	if _, err := c.Count(context.Background(), "orders", CountOptions{StartSeq: &start, EndSeq: &end}); err != nil {
		t.Fatalf("Count() = _, %v; want no error", err)
	}

	if want := "end_seq=500&key=orders&start_seq=100"; gotQuery != want {
		t.Errorf("query = %q; want %q", gotQuery, want)
	}
}

// TestCount_DecodesLargeCount asserts a 64-bit count survives as a JSON number.
func TestCount_DecodesLargeCount(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, `{"status":"success","count":18446744073709551615}`)
	})

	got, err := c.Count(context.Background(), "orders", CountOptions{})
	if err != nil {
		t.Fatalf("Count() = _, %v; want no error", err)
	}
	if want := uint64(1<<64 - 1); got != want {
		t.Errorf("Count() = %d; want %d", got, want)
	}
}

// TestListKeys_UnwrapsNestedKeys asserts the keys envelope is flattened. This
// nesting is the one place the server wraps a key in an object, and it names the
// inner field "key" -- RFC 0004 documents it as "value", so the RFC's shape
// would silently decode to nothing.
func TestListKeys_UnwrapsNestedKeys(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/log/keys" {
			t.Errorf("path = %q; want %q", r.URL.Path, "/api/v1/log/keys")
		}
		writeJSON(w, `{"status":"success","keys":[{"key":"ZXZlbnRz"},{"key":"b3JkZXJz"}]}`)
	})

	got, err := c.ListKeys(context.Background(), ListKeysOptions{})
	if err != nil {
		t.Fatalf("ListKeys() = _, %v; want no error", err)
	}

	want := [][]byte{[]byte("events"), []byte("orders")}
	if len(got) != len(want) {
		t.Fatalf("ListKeys() returned %d keys; want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Errorf("key[%d] = %q; want %q", i, got[i], want[i])
		}
	}
}

// TestListKeys_ReturnsRawBytes asserts keys come back as bytes, not strings.
// Append accepts arbitrary bytes, so a listing can legitimately contain a key
// that is not valid UTF-8 -- and therefore one that Scan cannot read back.
func TestListKeys_ReturnsRawBytes(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, `{"status":"success","keys":[{"key":"//4AAYA="}]}`)
	})

	got, err := c.ListKeys(context.Background(), ListKeysOptions{})
	if err != nil {
		t.Fatalf("ListKeys() = _, %v; want no error", err)
	}

	want := []byte{0xff, 0xfe, 0x00, 0x01, 0x80}
	if len(got) != 1 || !bytes.Equal(got[0], want) {
		t.Fatalf("ListKeys() = %#v; want [%#v]", got, want)
	}
}

// TestListKeys_OmitsUnsetOptions asserts no query is sent when nothing is set.
func TestListKeys_OmitsUnsetOptions(t *testing.T) {
	var gotQuery string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		writeJSON(w, `{"status":"success","keys":[]}`)
	})

	if _, err := c.ListKeys(context.Background(), ListKeysOptions{}); err != nil {
		t.Fatalf("ListKeys() = _, %v; want no error", err)
	}
	if gotQuery != "" {
		t.Errorf("query = %q; want it empty", gotQuery)
	}
}

// TestListKeys_SendsSegmentRangeAndLimit asserts keys are scoped by segment ID,
// not by sequence -- discover the boundaries with ListSegments first.
func TestListKeys_SendsSegmentRangeAndLimit(t *testing.T) {
	var gotQuery string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		writeJSON(w, `{"status":"success","keys":[]}`)
	})

	start, end := uint32(1), uint32(5)
	_, err := c.ListKeys(context.Background(), ListKeysOptions{
		StartSegment: &start,
		EndSegment:   &end,
		Limit:        10,
	})
	if err != nil {
		t.Fatalf("ListKeys() = _, %v; want no error", err)
	}

	if want := "end_segment=5&limit=10&start_segment=1"; gotQuery != want {
		t.Errorf("query = %q; want %q", gotQuery, want)
	}
}

// TestListSegments_DecodesSegments asserts the segment mapping, including the
// conversion of startTimeMs into a time.Time.
func TestListSegments_DecodesSegments(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/log/segments" {
			t.Errorf("path = %q; want %q", r.URL.Path, "/api/v1/log/segments")
		}
		writeJSON(w, `{"status":"success","segments":[`+
			`{"id":0,"startSeq":0,"startTimeMs":1705766400000},`+
			`{"id":1,"startSeq":100,"startTimeMs":1705766460000}]}`)
	})

	got, err := c.ListSegments(context.Background(), ListSegmentsOptions{})
	if err != nil {
		t.Fatalf("ListSegments() = _, %v; want no error", err)
	}

	if len(got) != 2 {
		t.Fatalf("ListSegments() returned %d segments; want 2", len(got))
	}
	if got[0].ID != 0 || got[0].StartSeq != 0 {
		t.Errorf("segment[0] = %+v; want ID 0, StartSeq 0", got[0])
	}
	if want := time.UnixMilli(1705766400000); !got[0].StartTime.Equal(want) {
		t.Errorf("segment[0].StartTime = %v; want %v", got[0].StartTime, want)
	}
	if got[1].ID != 1 || got[1].StartSeq != 100 {
		t.Errorf("segment[1] = %+v; want ID 1, StartSeq 100", got[1])
	}
	if want := time.UnixMilli(1705766460000); !got[1].StartTime.Equal(want) {
		t.Errorf("segment[1].StartTime = %v; want %v", got[1].StartTime, want)
	}
}

// TestListSegments_SendsNoLimit asserts no limit parameter is sent. RFC 0004
// documents one, but the handler does not parse it, so sending it would be dead
// weight that implies a bound the server does not honour.
func TestListSegments_SendsNoLimit(t *testing.T) {
	var gotQuery string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		writeJSON(w, `{"status":"success","segments":[]}`)
	})

	start, end := uint64(0), uint64(1000)
	_, err := c.ListSegments(context.Background(), ListSegmentsOptions{StartSeq: &start, EndSeq: &end})
	if err != nil {
		t.Fatalf("ListSegments() = _, %v; want no error", err)
	}

	if strings.Contains(gotQuery, "limit") {
		t.Errorf("query = %q; want no limit parameter", gotQuery)
	}
	if want := "end_seq=1000&start_seq=0"; gotQuery != want {
		t.Errorf("query = %q; want %q", gotQuery, want)
	}
}

// TestListSegments_OmitsUnsetOptions asserts no query is sent when nothing is
// set.
func TestListSegments_OmitsUnsetOptions(t *testing.T) {
	var gotQuery string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		writeJSON(w, `{"status":"success","segments":[]}`)
	})

	if _, err := c.ListSegments(context.Background(), ListSegmentsOptions{}); err != nil {
		t.Fatalf("ListSegments() = _, %v; want no error", err)
	}
	if gotQuery != "" {
		t.Errorf("query = %q; want it empty", gotQuery)
	}
}
