package ingest

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
)

func TestBatch_should_roundtrip_entries(t *testing.T) {
	entries := [][]byte{[]byte("hello"), []byte("world"), []byte("foo")}
	encoded, err := EncodeBatch(entries, CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeBatch(encoded)
	if err != nil {
		t.Fatal(err)
	}
	assertEntriesEqual(t, entries, decoded)
}

func TestBatch_should_roundtrip_empty_batch(t *testing.T) {
	encoded, err := EncodeBatch(nil, CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) != batchFooterSize {
		t.Fatalf("expected %d bytes, got %d", batchFooterSize, len(encoded))
	}
	decoded, err := DecodeBatch(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 0 {
		t.Fatalf("expected empty, got %d entries", len(decoded))
	}
}

func TestBatch_should_roundtrip_empty_record(t *testing.T) {
	entries := [][]byte{{}}
	encoded, err := EncodeBatch(entries, CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeBatch(encoded)
	if err != nil {
		t.Fatal(err)
	}
	assertEntriesEqual(t, entries, decoded)
}

func TestBatch_should_reject_truncated_data(t *testing.T) {
	entries := [][]byte{[]byte("hello")}
	encoded, err := EncodeBatch(entries, CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	// Truncate the record data, then rewrite footer.
	truncated := encoded[:len(encoded)-batchFooterSize-1]
	truncated = append(truncated, byte(CompressionNone))
	truncated = binary.LittleEndian.AppendUint32(truncated, 1)
	truncated = binary.LittleEndian.AppendUint16(truncated, batchVersion)

	_, err = DecodeBatch(truncated)
	if err == nil {
		t.Fatal("expected error for truncated data")
	}
}

func TestBatch_should_reject_unsupported_version(t *testing.T) {
	buf := []byte{byte(CompressionNone)}
	buf = binary.LittleEndian.AppendUint32(buf, 0)
	buf = binary.LittleEndian.AppendUint16(buf, 99)
	_, err := DecodeBatch(buf)
	if err == nil || !errors.Is(err, ErrSerialization) {
		t.Fatalf("expected serialization error, got %v", err)
	}
}

func TestBatch_should_roundtrip_with_zstd(t *testing.T) {
	entries := [][]byte{[]byte("hello"), []byte("world"), []byte("foo")}
	encoded, err := EncodeBatch(entries, CompressionZstd)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeBatch(encoded)
	if err != nil {
		t.Fatal(err)
	}
	assertEntriesEqual(t, entries, decoded)
}

func TestBatch_should_roundtrip_empty_batch_with_zstd(t *testing.T) {
	encoded, err := EncodeBatch(nil, CompressionZstd)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeBatch(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 0 {
		t.Fatalf("expected empty, got %d entries", len(decoded))
	}
}

func TestBatch_should_roundtrip_large_batch_with_zstd(t *testing.T) {
	entries := make([][]byte, 1000)
	for i := range entries {
		entries[i] = []byte(fmt.Sprintf("entry-%04d", i))
	}
	encoded, err := EncodeBatch(entries, CompressionZstd)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeBatch(encoded)
	if err != nil {
		t.Fatal(err)
	}
	assertEntriesEqual(t, entries, decoded)
}

func TestBatch_should_compress_smaller_for_repetitive_data(t *testing.T) {
	entries := make([][]byte, 100)
	for i := range entries {
		entries[i] = []byte("repeated-data-that-compresses-well")
	}
	uncompressed, err := EncodeBatch(entries, CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	compressed, err := EncodeBatch(entries, CompressionZstd)
	if err != nil {
		t.Fatal(err)
	}
	if len(compressed) >= len(uncompressed) {
		t.Fatalf("compressed (%d) should be smaller than uncompressed (%d)",
			len(compressed), len(uncompressed))
	}
}

func TestBatch_should_reject_unsupported_compression_type(t *testing.T) {
	buf := []byte{0xFF}
	buf = binary.LittleEndian.AppendUint32(buf, 0)
	buf = binary.LittleEndian.AppendUint16(buf, batchVersion)
	_, err := DecodeBatch(buf)
	if err == nil || !errors.Is(err, ErrSerialization) {
		t.Fatalf("expected serialization error, got %v", err)
	}
}

func assertEntriesEqual(t *testing.T, expected, actual [][]byte) {
	t.Helper()
	if len(expected) != len(actual) {
		t.Fatalf("entry count: expected %d, got %d", len(expected), len(actual))
	}
	for i := range expected {
		if !bytes.Equal(expected[i], actual[i]) {
			t.Fatalf("entry %d: expected %q, got %q", i, expected[i], actual[i])
		}
	}
}
