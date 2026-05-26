package buffer

import (
	"context"
	"testing"
	"time"

	"github.com/opendata-oss/opendata-go/objstore"
)

// pollUntil returns true when `cond` returns true before `timeout`,
// otherwise false. Used to bridge the rotator-goroutine async boundary
// without sleeping for a fixed (and inevitably-wrong) interval.
func pollUntil(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

func TestProducer_OldestUnflushedBatchAge_zero_when_idle(t *testing.T) {
	store := objstore.NewInMemory()
	p := NewProducer(store, testConfig())
	t.Cleanup(func() { _ = p.Close(context.Background()) })

	if got := p.OldestUnflushedBatchAge(); got != 0 {
		t.Fatalf("idle producer should report 0 age, got %s", got)
	}
}

func TestProducer_OldestUnflushedBatchAge_starts_on_first_append(t *testing.T) {
	store := objstore.NewInMemory()
	cfg := testConfig()
	// Force a long flush interval so the accumulator stays populated;
	// the test verifies the gauge tracks the wait, not the flush path.
	cfg.FlushInterval = 24 * time.Hour
	cfg.FlushSizeBytes = 1 << 30 // large enough that one append can't trip a size flush
	p := NewProducer(store, cfg)
	t.Cleanup(func() { _ = p.Close(context.Background()) })

	before := time.Now()
	if _, err := p.Append([][]byte{[]byte("first")}, nil); err != nil {
		t.Fatalf("append: %v", err)
	}

	// Rotator handles appendCh asynchronously; wait until the gauge
	// reflects the new record.
	if !pollUntil(2*time.Second, func() bool {
		return p.OldestUnflushedBatchAge() > 0
	}) {
		t.Fatalf("expected OldestUnflushedBatchAge > 0 after Append; still 0")
	}

	// The age should be roughly the time since `before` (within a wide
	// tolerance to absorb scheduler jitter on the rotator goroutine).
	age := p.OldestUnflushedBatchAge()
	if age <= 0 {
		t.Fatalf("age must be positive, got %s", age)
	}
	if age > time.Since(before)+50*time.Millisecond {
		t.Fatalf("age %s exceeds wall-clock elapsed %s by > 50ms (clock skew)",
			age, time.Since(before))
	}

	// A second Append must NOT advance the gauge — the first record's
	// arrival time is the floor.
	firstAge := p.OldestUnflushedBatchAge()
	time.Sleep(20 * time.Millisecond)
	if _, err := p.Append([][]byte{[]byte("second")}, nil); err != nil {
		t.Fatalf("second append: %v", err)
	}
	// Wait one rotator turn.
	if !pollUntil(1*time.Second, func() bool {
		// Just make sure the rotator picked up the message.
		return p.OldestUnflushedBatchAge() >= firstAge
	}) {
		t.Fatalf("second append never observed by rotator")
	}
	// Now the age should still be growing from the original timestamp,
	// not reset.
	secondAge := p.OldestUnflushedBatchAge()
	if secondAge < firstAge {
		t.Fatalf("second Append reset the gauge: firstAge=%s, secondAge=%s; gauge must anchor on the first record's arrival",
			firstAge, secondAge)
	}
}

func TestProducer_OldestUnflushedBatchAge_resets_on_flush(t *testing.T) {
	store := objstore.NewInMemory()
	cfg := testConfig()
	cfg.FlushInterval = 24 * time.Hour
	p := NewProducer(store, cfg)
	t.Cleanup(func() { _ = p.Close(context.Background()) })

	if _, err := p.Append([][]byte{[]byte("payload")}, nil); err != nil {
		t.Fatalf("append: %v", err)
	}
	if !pollUntil(2*time.Second, func() bool {
		return p.OldestUnflushedBatchAge() > 0
	}) {
		t.Fatalf("gauge never tripped above 0 post-Append")
	}

	if err := p.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// Flush is synchronous through durability; the accumulator should be
	// empty when Flush returns, so the gauge should observe 0 promptly.
	if !pollUntil(500*time.Millisecond, func() bool {
		return p.OldestUnflushedBatchAge() == 0
	}) {
		t.Fatalf("OldestUnflushedBatchAge stayed > 0 after Flush returned: got %s",
			p.OldestUnflushedBatchAge())
	}
}
