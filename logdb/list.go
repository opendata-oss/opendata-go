package logdb

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const (
	pathCount    = "/api/v1/log/count"
	pathKeys     = "/api/v1/log/keys"
	pathSegments = "/api/v1/log/segments"
)

// seqRange renders an optional inclusive-start, exclusive-end sequence range.
// Unset bounds are omitted so the server applies 0 and max-uint64.
func seqRange(startSeq, endSeq *uint64) url.Values {
	q := url.Values{}
	if startSeq != nil {
		q.Set("start_seq", strconv.FormatUint(*startSeq, 10))
	}
	if endSeq != nil {
		q.Set("end_seq", strconv.FormatUint(*endSeq, 10))
	}
	return q
}

// CountOptions bounds a Count to a sequence range. Unset bounds count the whole
// stream.
type CountOptions struct {
	// StartSeq is the inclusive global sequence to count from.
	StartSeq *uint64

	// EndSeq is the exclusive global sequence to count to.
	EndSeq *uint64
}

// Count returns an exact count of the entries for one key, which is how a
// consumer measures its own lag: count from its checkpoint to the end.
//
// As with Scan, the key travels as a plain query parameter and so must be valid
// UTF-8.
func (c *Client) Count(ctx context.Context, key string, opts CountOptions) (uint64, error) {
	q := seqRange(opts.StartSeq, opts.EndSeq)
	q.Set("key", key)

	var out struct {
		Count uint64 `json:"count"`
	}
	if err := c.doJSON(ctx, http.MethodGet, pathCount, q, nil, &out); err != nil {
		return 0, err
	}
	return out.Count, nil
}

// ListKeysOptions bounds a ListKeys call to a range of segments.
type ListKeysOptions struct {
	// StartSegment is the inclusive segment ID to list from.
	StartSegment *uint32

	// EndSegment is the exclusive segment ID to list to.
	EndSegment *uint32

	// Limit caps how many keys come back. Unset means the server's 32.
	Limit int
}

// ListKeys returns the distinct keys present in a range of segments.
//
// Keys are scoped by segment ID rather than by sequence, so call ListSegments
// first to discover the boundaries. The keys are raw bytes and need not be valid
// UTF-8; one that is not cannot be passed to Scan or Count.
func (c *Client) ListKeys(ctx context.Context, opts ListKeysOptions) ([][]byte, error) {
	q := url.Values{}
	if opts.StartSegment != nil {
		q.Set("start_segment", strconv.FormatUint(uint64(*opts.StartSegment), 10))
	}
	if opts.EndSegment != nil {
		q.Set("end_segment", strconv.FormatUint(uint64(*opts.EndSegment), 10))
	}
	if opts.Limit > 0 {
		q.Set("limit", strconv.Itoa(opts.Limit))
	}

	// Keys are the one response the server wraps in an object per element, and
	// it names the inner field "key" (RFC 0004 documents it as "value").
	var out struct {
		Keys []struct {
			Key []byte `json:"key"`
		} `json:"keys"`
	}
	if err := c.doJSON(ctx, http.MethodGet, pathKeys, q, nil, &out); err != nil {
		return nil, err
	}

	keys := make([][]byte, len(out.Keys))
	for i, k := range out.Keys {
		keys[i] = k.Key
	}
	return keys, nil
}

// ListSegmentsOptions bounds a ListSegments call to a sequence range.
//
// There is deliberately no Limit: RFC 0004 documents one, but the handler does
// not parse it, so sending it would imply a bound the server will not honour.
type ListSegmentsOptions struct {
	// StartSeq is the inclusive global sequence to list from.
	StartSeq *uint64

	// EndSeq is the exclusive global sequence to list to.
	EndSeq *uint64
}

// ListSegments returns the segments overlapping a sequence range, in order.
func (c *Client) ListSegments(ctx context.Context, opts ListSegmentsOptions) ([]Segment, error) {
	var out struct {
		Segments []struct {
			ID          uint32 `json:"id"`
			StartSeq    uint64 `json:"startSeq"`
			StartTimeMs int64  `json:"startTimeMs"`
		} `json:"segments"`
	}
	if err := c.doJSON(ctx, http.MethodGet, pathSegments, seqRange(opts.StartSeq, opts.EndSeq), nil, &out); err != nil {
		return nil, err
	}

	segments := make([]Segment, len(out.Segments))
	for i, s := range out.Segments {
		segments[i] = Segment{
			ID:        s.ID,
			StartSeq:  s.StartSeq,
			StartTime: time.UnixMilli(s.StartTimeMs).UTC(),
		}
	}
	return segments, nil
}
