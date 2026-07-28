package logdb

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// pathScan is the scan route.
const pathScan = "/api/v1/log/scan"

// ScanOptions narrows a Scan. Every field is optional, and an unset field is
// left out of the request so the server applies its own default: sequence 0 to
// max-uint64, at most 32 entries, no long-polling.
type ScanOptions struct {
	// StartSeq is the inclusive global sequence to start from. A pointer so a
	// deliberate 0 is distinguishable from "unset".
	StartSeq *uint64

	// EndSeq is the exclusive global sequence to stop at.
	EndSeq *uint64

	// Limit caps how many entries come back. Unset means the server's 32.
	Limit int

	// Follow holds the request open until an entry arrives or PollTimeout
	// elapses, instead of returning an empty result immediately. Prefer
	// Client.Follow, which also manages the resume cursor.
	Follow bool

	// PollTimeout bounds a Follow request. Unset means the server's 30s.
	// Truncated to milliseconds on the wire.
	PollTimeout time.Duration
}

// values renders the options as query parameters. Parameter names are
// snake_case, unlike the camelCase of JSON bodies.
func (o ScanOptions) values(key string) url.Values {
	q := url.Values{}
	q.Set("key", key)
	if o.StartSeq != nil {
		q.Set("start_seq", strconv.FormatUint(*o.StartSeq, 10))
	}
	if o.EndSeq != nil {
		q.Set("end_seq", strconv.FormatUint(*o.EndSeq, 10))
	}
	if o.Limit > 0 {
		q.Set("limit", strconv.Itoa(o.Limit))
	}
	if o.Follow {
		q.Set("follow", "true")
	}
	if o.PollTimeout > 0 {
		q.Set("timeout_ms", strconv.FormatInt(o.PollTimeout.Milliseconds(), 10))
	}
	return q
}

// ScanResult is one page of a key's log stream.
type ScanResult struct {
	// Key echoes the key that was scanned.
	Key []byte `json:"key"`

	// Values are the entries found, ordered by sequence. Empty is normal: the
	// key may be idle, or a Follow poll may have timed out.
	Values []Entry `json:"values"`

	// NextSequence is the exclusive global sequence to resume from, and is the
	// only correct way to continue a scan. It is not simply one past the last
	// entry: when a scan drains without hitting its limit, the server lifts it
	// to the frontier it observed, which advances on writes to *any* key. That
	// is what keeps tail polling proportional to new data rather than to the
	// size of the backlog. See RFC 0007.
	NextSequence uint64 `json:"nextSequence"`
}

// Scan reads entries for one key within a sequence range.
//
// The key is sent as a plain query parameter, so it must be valid UTF-8 — even
// though Append accepts arbitrary bytes. A key written as non-UTF-8 bytes cannot
// be read back through this API.
func (c *Client) Scan(ctx context.Context, key string, opts ScanOptions) (ScanResult, error) {
	var out ScanResult
	if err := c.doJSON(ctx, http.MethodGet, pathScan, opts.values(key), nil, &out); err != nil {
		return ScanResult{}, err
	}
	return out, nil
}
