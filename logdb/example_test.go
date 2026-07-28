package logdb_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/opendata-oss/opendata-go/logdb"
)

// Append writes records to a stream. Each record's key names the stream it joins.
func Example() {
	c, err := logdb.New("http://localhost:8080")
	if err != nil {
		log.Fatal(err)
	}

	res, err := c.Append(context.Background(), []logdb.Record{
		{Key: []byte("orders"), Value: []byte("order-1")},
		{Key: []byte("orders"), Value: []byte("order-2")},
	}, logdb.AppendOptions{})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("wrote %d records from sequence %d\n", res.RecordsAppended, res.StartSequence)
}

// AwaitDurable blocks until the batch is persisted, at the cost of latency and
// throughput. Without it the server acknowledges as soon as records are buffered
// in memory, which does not survive a crash.
func ExampleClient_Append_durable() {
	c, _ := logdb.New("http://localhost:8080")

	_, err := c.Append(context.Background(), []logdb.Record{
		{Key: []byte("payments"), Value: []byte("charge-1")},
	}, logdb.AppendOptions{AwaitDurable: true})
	if err != nil {
		log.Fatal(err)
	}
}

// Scan reads one page of a stream. Note that sequences ascend but are not
// contiguous: the counter is shared with every other key.
func ExampleClient_Scan() {
	c, _ := logdb.New("http://localhost:8080")

	res, err := c.Scan(context.Background(), "orders", logdb.ScanOptions{Limit: 100})
	if err != nil {
		log.Fatal(err)
	}

	for _, e := range res.Values {
		fmt.Printf("seq=%d value=%s\n", e.Sequence, e.Value)
	}

	// Continue from the reported cursor, not from the last entry's sequence.
	next, err := c.Scan(context.Background(), "orders", logdb.ScanOptions{
		StartSeq: &res.NextSequence,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%d more entries\n", len(next.Values))
}

// Follow tails a stream. An empty result means the poll timed out with nothing
// new, which is normal, so the loop keeps going until the context is cancelled.
// Checkpoint Cursor to resume in a later process.
func ExampleClient_Follow() {
	// The server holds each poll open, so the HTTP client's own timeout must
	// exceed PollTimeout.
	c, _ := logdb.New("http://localhost:8080",
		logdb.WithHTTPClient(&http.Client{Timeout: time.Minute}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f := c.Follow("orders", logdb.FollowOptions{
		From:        0, // or a checkpoint from a previous run
		Limit:       500,
		PollTimeout: 30 * time.Second,
	})

	for {
		entries, err := f.Next(ctx)
		if err != nil {
			// Cancellation is the normal way a follower ends. Return rather
			// than exiting, so the deferred cancel still runs.
			if !errors.Is(err, context.Canceled) {
				log.Printf("follow orders: %v", err)
			}
			return
		}

		for _, e := range entries {
			fmt.Printf("seq=%d value=%s\n", e.Sequence, e.Value)
		}

		// Persist this to resume where you left off.
		_ = f.Cursor()
	}
}

// Count measures consumer lag exactly: how many entries remain past a
// checkpoint.
func ExampleClient_Count() {
	c, _ := logdb.New("http://localhost:8080")

	checkpoint := uint64(1000)
	pending, err := c.Count(context.Background(), "orders", logdb.CountOptions{
		StartSeq: &checkpoint,
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%d entries behind\n", pending)
}

// Keys are scoped by segment, so discover the segment boundaries first. The keys
// come back as raw bytes and need not be valid UTF-8.
func ExampleClient_ListKeys() {
	c, _ := logdb.New("http://localhost:8080")
	ctx := context.Background()

	segments, err := c.ListSegments(ctx, logdb.ListSegmentsOptions{})
	if err != nil {
		log.Fatal(err)
	}
	for _, s := range segments {
		fmt.Printf("segment %d starts at sequence %d (%s)\n", s.ID, s.StartSeq, s.StartTime)
	}

	keys, err := c.ListKeys(ctx, logdb.ListKeysOptions{Limit: 1000})
	if err != nil {
		log.Fatal(err)
	}
	for _, k := range keys {
		fmt.Printf("key=%q\n", k)
	}
}

// Errors unwrap to sentinels, so callers branch with errors.Is rather than on
// status codes.
func ExampleAPIError() {
	c, _ := logdb.New("http://localhost:8080")

	_, err := c.Append(context.Background(), []logdb.Record{
		{Key: []byte("orders"), Value: []byte("order-1")},
	}, logdb.AppendOptions{})

	switch {
	case err == nil:
	case errors.Is(err, logdb.ErrNotFound):
		// The append route is unregistered: this server is a read-only gateway.
		log.Print("server is read-only")
	case errors.Is(err, logdb.ErrInvalidInput):
		log.Print("rejected as malformed")
	case errors.Is(err, logdb.ErrServerError):
		if apiErr, ok := errors.AsType[*logdb.APIError](err); ok {
			log.Printf("server failed with %d: %s", apiErr.StatusCode, apiErr.Message)
		}
	}
}
