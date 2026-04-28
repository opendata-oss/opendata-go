package buffer

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/opendata-oss/opendata-go/objstore"
)

func testConfig() ProducerConfig {
	return ProducerConfig{
		DataPathPrefix:    "test-ingest",
		ManifestPath:      "test/manifest",
		FlushInterval:     24 * time.Hour,
		FlushSizeBytes:    64 * 1024 * 1024,
		MaxBufferedInputs: 1000,
		BatchCompression:  CompressionNone,
	}
}

func readManifestEntries(t *testing.T, store objstore.ObjectStore) []QueueEntry {
	t.Helper()
	result, err := store.Get(context.Background(), "test/manifest")
	if err != nil {
		t.Fatal(err)
	}
	entries, err := DecodeManifestEntries(result.Data)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

func TestProducer_should_append_entries_and_enqueue_location(t *testing.T) {
	store := objstore.NewInMemory()
	p := NewProducer(store, testConfig())

	ctx := context.Background()
	if _, err := p.Append([][]byte{[]byte("data1")}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Append([][]byte{[]byte("data2")}, nil); err != nil {
		t.Fatal(err)
	}
	if err := p.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	entries := readManifestEntries(t, store)
	if len(entries) != 1 {
		t.Fatalf("expected 1 manifest entry, got %d", len(entries))
	}
	if len(entries[0].Location) == 0 {
		t.Fatal("expected non-empty location")
	}
}

func TestProducer_should_write_valid_batch_to_object_store(t *testing.T) {
	store := objstore.NewInMemory()
	p := NewProducer(store, testConfig())

	ctx := context.Background()
	if _, err := p.Append([][]byte{[]byte("mydata")}, nil); err != nil {
		t.Fatal(err)
	}
	if err := p.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	entries := readManifestEntries(t, store)
	result, err := store.Get(ctx, entries[0].Location)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := DecodeBatch(result.Data)
	if err != nil {
		t.Fatal(err)
	}
	if len(parsed) != 1 || !bytes.Equal(parsed[0], []byte("mydata")) {
		t.Fatalf("expected [mydata], got %v", parsed)
	}
}

func TestProducer_should_flush_when_batch_size_exceeded(t *testing.T) {
	store := objstore.NewInMemory()
	config := testConfig()
	config.FlushSizeBytes = 10
	p := NewProducer(store, config)

	ctx := context.Background()
	wh, err := p.Append([][]byte{[]byte("some-long-data")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := wh.Watcher.AwaitDurable(ctx); err != nil {
		t.Fatal(err)
	}

	entries := readManifestEntries(t, store)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
}

func TestProducer_should_flush_when_interval_elapsed(t *testing.T) {
	store := objstore.NewInMemory()
	config := testConfig()
	config.FlushInterval = 50 * time.Millisecond
	config.FlushSizeBytes = 64 * 1024 * 1024
	p := NewProducer(store, config)

	ctx := context.Background()
	wh, err := p.Append([][]byte{[]byte("v1")}, nil)
	if err != nil {
		t.Fatal(err)
	}

	done, _ := wh.Watcher.Result()
	if done {
		t.Fatal("expected not yet flushed")
	}

	if err := wh.Watcher.AwaitDurable(ctx); err != nil {
		t.Fatal(err)
	}

	entries := readManifestEntries(t, store)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
}

func TestProducer_should_batch_multiple_appends_into_single_file(t *testing.T) {
	store := objstore.NewInMemory()
	p := NewProducer(store, testConfig())

	ctx := context.Background()
	wh1, err := p.Append([][]byte{[]byte("data1")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	wh2, err := p.Append([][]byte{[]byte("data2")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	done1, err := wh1.Watcher.Result()
	if !done1 || err != nil {
		t.Fatalf("watcher1: done=%v err=%v", done1, err)
	}
	done2, err := wh2.Watcher.Result()
	if !done2 || err != nil {
		t.Fatalf("watcher2: done=%v err=%v", done2, err)
	}

	entries := readManifestEntries(t, store)
	if len(entries) != 1 {
		t.Fatalf("expected 1 manifest entry, got %d", len(entries))
	}

	result, err := store.Get(ctx, entries[0].Location)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := DecodeBatch(result.Data)
	if err != nil {
		t.Fatal(err)
	}
	if len(parsed) != 2 {
		t.Fatalf("expected 2 records, got %d", len(parsed))
	}
	if !bytes.Equal(parsed[0], []byte("data1")) || !bytes.Equal(parsed[1], []byte("data2")) {
		t.Fatalf("unexpected records: %v", parsed)
	}
}

func TestProducer_should_not_flush_empty_batch(t *testing.T) {
	store := objstore.NewInMemory()
	p := NewProducer(store, testConfig())

	if err := p.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	_, err := store.Get(context.Background(), "test/manifest")
	if err == nil {
		t.Fatal("expected no manifest to exist")
	}
}

func TestProducer_should_reject_empty_entries(t *testing.T) {
	store := objstore.NewInMemory()
	p := NewProducer(store, testConfig())

	_, err := p.Append(nil, nil)
	if err == nil {
		t.Fatal("expected error for nil entries")
	}
	_, err = p.Append([][]byte{}, []byte("meta"))
	if err == nil {
		t.Fatal("expected error for empty entries")
	}
}

func TestProducer_should_flush_remaining_entries_on_close(t *testing.T) {
	store := objstore.NewInMemory()
	p := NewProducer(store, testConfig())

	if _, err := p.Append([][]byte{[]byte("unflushed")}, nil); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := p.Close(ctx); err != nil {
		t.Fatal(err)
	}

	entries := readManifestEntries(t, store)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	result, err := store.Get(ctx, entries[0].Location)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := DecodeBatch(result.Data)
	if err != nil {
		t.Fatal(err)
	}
	if len(parsed) != 1 || !bytes.Equal(parsed[0], []byte("unflushed")) {
		t.Fatalf("expected [unflushed], got %v", parsed)
	}
}

func TestProducer_should_produce_separate_batches_per_flush(t *testing.T) {
	store := objstore.NewInMemory()
	p := NewProducer(store, testConfig())

	ctx := context.Background()
	if _, err := p.Append([][]byte{[]byte("batch1")}, nil); err != nil {
		t.Fatal(err)
	}
	if err := p.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err := p.Append([][]byte{[]byte("batch2")}, nil); err != nil {
		t.Fatal(err)
	}
	if err := p.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	entries := readManifestEntries(t, store)
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if entries[0].Location == entries[1].Location {
		t.Fatal("expected different locations for each batch")
	}
}

func TestProducer_should_record_metadata_in_queue_entry(t *testing.T) {
	store := objstore.NewInMemory()
	p := NewProducer(store, testConfig())

	ctx := context.Background()
	meta := []byte(`{"topic":"events"}`)
	if _, err := p.Append([][]byte{[]byte("payload")}, meta); err != nil {
		t.Fatal(err)
	}
	if err := p.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	entries := readManifestEntries(t, store)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if len(entries[0].Metadata) != 1 {
		t.Fatalf("expected 1 metadata, got %d", len(entries[0].Metadata))
	}
	if !bytes.Equal(entries[0].Metadata[0].Payload, meta) {
		t.Fatalf("expected metadata %q, got %q", meta, entries[0].Metadata[0].Payload)
	}
}
