package logdb

import (
	"context"
	"time"
)

// FollowOptions configures a Follower.
type FollowOptions struct {
	// From is the inclusive global sequence to start from, normally a cursor
	// checkpointed by a previous run. Zero starts at the beginning of the log.
	From uint64

	// Limit caps how many entries one Next call returns. Unset means the
	// server's 32, which is small for a consumer catching up on a backlog.
	Limit int

	// PollTimeout bounds how long the server holds a poll open waiting for an
	// entry. Unset means the server's 30s.
	PollTimeout time.Duration
}

// Follower tails one key's log stream, tracking the resume cursor between calls.
//
// It is not safe for concurrent use: one goroutine per Follower.
type Follower struct {
	client      *Client
	key         string
	limit       int
	pollTimeout time.Duration
	cursor      uint64
}

// Follow returns a Follower tailing key.
//
// Next blocks server-side for up to FollowOptions.PollTimeout, so any timeout on
// the Client's *http.Client must exceed it or every poll fails. Prefer bounding
// individual calls with a context.
func (c *Client) Follow(key string, opts FollowOptions) *Follower {
	return &Follower{
		client:      c,
		key:         key,
		limit:       opts.Limit,
		pollTimeout: opts.PollTimeout,
		cursor:      opts.From,
	}
}

// Cursor is the exclusive sequence the next poll will resume from. Checkpoint it
// to resume a later Follower where this one stopped, via FollowOptions.From.
func (f *Follower) Cursor() uint64 {
	return f.cursor
}

// Next returns the entries that arrived since the last call, blocking until at
// least one exists or the server's poll timeout elapses.
//
// A timeout is reported as zero entries and a nil error, not a failure: an idle
// stream is normal, and the cursor still advances so the following poll only
// scans data that has since arrived. Callers should loop until their context is
// cancelled and treat an empty result as "nothing yet".
//
// The cursor moves only on success, so a failed poll can be retried without
// losing entries.
func (f *Follower) Next(ctx context.Context) ([]Entry, error) {
	start := f.cursor

	res, err := f.client.Scan(ctx, f.key, ScanOptions{
		StartSeq:    &start,
		Limit:       f.limit,
		Follow:      true,
		PollTimeout: f.pollTimeout,
	})
	if err != nil {
		// A cancelled or expired context surfaces as a transport error wrapping
		// the cause. Report the cause: shutting down mid-poll is the normal way
		// a follower ends, and callers should not have to unwrap to see it.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, err
	}

	f.cursor = f.advance(res)
	return res.Values, nil
}

// advance computes the next cursor. It takes the highest of three sound lower
// bounds so the follower cannot stall or regress:
//
//   - the current cursor, so the cursor never moves backwards;
//   - one past the last entry returned, which holds even against a server that
//     does not send nextSequence at all (it predates RFC 0007, and the published
//     OpenAPI spec still omits the field). Without this a missing field would
//     decode as zero and the same entries would be redelivered forever;
//   - the server's nextSequence, which is the only bound that can skip an
//     observed-empty range and is therefore what makes idle polling cheap.
func (f *Follower) advance(res ScanResult) uint64 {
	next := f.cursor

	if n := len(res.Values); n > 0 {
		// A wrap at the top of the sequence space leaves next alone rather than
		// resetting it to zero.
		if last := res.Values[n-1].Sequence; last+1 > next {
			next = last + 1
		}
	}
	if res.NextSequence > next {
		next = res.NextSequence
	}
	return next
}
