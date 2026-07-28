// Package logdb is a client for the OpenData Log HTTP API.
//
// Log is a key-oriented log database. Every key is its own ordered stream, and
// creating a stream is just writing a new key: there is no partition count to
// provision and no topic to declare. Appends assign each record a sequence
// number from one counter shared by every key, so sequences reflect global
// append order. A single key's sequences are therefore monotonic but not
// contiguous, with gaps wherever other keys were written in between.
//
// # Usage
//
//	c, err := logdb.New("http://localhost:8080")
//	if err != nil {
//		return err
//	}
//
//	_, err = c.Append(ctx, []logdb.Record{
//		{Key: []byte("orders"), Value: []byte("order-1")},
//	}, logdb.AppendOptions{})
//
// Client is safe for concurrent use. Follower is not: use one per goroutine.
//
// # Tailing a stream
//
// Follow is the reason to prefer this package over hand-rolled HTTP calls. A
// scan reports a resume cursor in [ScanResult.NextSequence], and continuing from
// that cursor rather than from your own last entry is what keeps tail polling
// proportional to newly arrived data instead of to the size of the backlog. The
// cursor advances even when a poll finds nothing, because the server lifts it to
// the frontier it observed and writes to any key move that frontier. Follower
// tracks this for you; see RFC 0007 in the opendata repository for why it
// matters.
//
// A poll that times out yields zero entries and a nil error. An idle stream is
// normal, not a failure, so loop until your context is cancelled and treat an
// empty result as "nothing yet".
//
// # Keys are bytes on write but text on read
//
// Append takes arbitrary bytes for both key and value. Scan and Count, however,
// pass the key as a plain query-string parameter that the server decodes into a
// UTF-8 string, so they take a string rather than a []byte. A key written as
// non-UTF-8 bytes cannot be read back through this API at all. [Client.ListKeys]
// returns raw bytes for that reason and may hand back a key that Scan cannot
// accept.
//
// # Server defaults
//
// Options left unset are omitted from the request so the server applies its own
// defaults rather than a value guessed here. Worth knowing:
//
//   - Scan and ListKeys return at most 32 entries unless Limit says otherwise,
//     which is small for a consumer working through a backlog.
//   - A Follow poll is held open for 30s unless PollTimeout says otherwise. Any
//     timeout on the Client's *http.Client must exceed it, or every poll fails.
//   - Sequence and segment ranges are inclusive at the start and exclusive at
//     the end.
//
// # Errors
//
// Every non-200 response becomes an [*APIError] carrying the status code and the
// server's message. It unwraps to [ErrInvalidInput] (4xx), [ErrNotFound] (404),
// [ErrServerError] (5xx) or [ErrNotReady] (503), so callers can branch with
// errors.Is instead of comparing status codes. ErrNotReady also satisfies
// ErrServerError.
//
// A 404 on Append usually means the server is running as a read-only gateway: it
// does not register the append route at all, so writes are not rejected so much
// as unrouted.
//
// No retries are performed. Supply a transport that retries via
// [WithHTTPClient] if you want them.
//
// # Documentation accuracy
//
// This client is written against the server implementation, which disagrees with
// both RFC 0004 and the published OpenAPI spec at
// https://www.opendata.dev/docs/openapi/log.yaml. If you are cross-checking
// against those documents, the differences are:
//
//   - Records carry a flat base64 key. RFC 0004 shows it nested as
//     {"key":{"value":"..."}}.
//   - The keys listing nests each key under "key". RFC 0004 calls that field
//     "value", a shape that decodes to empty keys without erroring.
//   - Scan returns nextSequence. The OpenAPI spec omits it entirely, though the
//     whole tail-following contract depends on it.
//   - The default limit is 32. RFC 0004 says 1000.
//   - The segments endpoint accepts no limit, despite RFC 0004 documenting one.
//   - Scan accepts follow and timeout_ms, and the server serves /-/healthy and
//     /-/ready. None of these appear in RFC 0004.
//
// The integration tests in this package (build tag "integration") are what keep
// this client honest against the running server; the unit tests only prove it is
// self-consistent.
package logdb
