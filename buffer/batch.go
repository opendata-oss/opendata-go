// Package buffer provides Go bindings for the opendata stateless buffer component.
package buffer

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/klauspost/compress/zstd"
)

// CompressionType identifies the compression algorithm applied to a data batch.
type CompressionType uint8

// Supported compression types.
const (
	CompressionNone CompressionType = 0
	CompressionZstd CompressionType = 1
)

const (
	batchEntryLenSize = 4                                                 // u32 LE per entry
	batchVersionSize  = 2                                                 // u16 LE
	batchCountSize    = 4                                                 // u32 LE
	batchCompSize     = 1                                                 // u8
	batchFooterSize   = batchCompSize + batchCountSize + batchVersionSize // 7 bytes
	batchVersion      = 1
	zstdLevel         = 3
)

// EncodeBatch serializes entries into the binary batch format with the given
// compression. The format is:
//
//	[compressed record block] [compression_type: u8] [record_count: u32 LE] [version: u16 LE]
//
// Memory shape (Phase 2 of producer memory tuning, May 2026):
//   - CompressionNone: writes length-prefix + entry bytes + footer into one
//     buffer of capacity `dataSize + batchFooterSize`. No intermediate
//     uncompressed-block allocation, no full-buffer copy at footer time.
//   - CompressionZstd: builds the uncompressed length-prefixed block once,
//     then asks zstd.EncodeAll to append-into a pre-sized output buffer
//     (capacity hint = dataSize/2 + footer). Eliminates the
//     compressed→final-buffer copy that the previous implementation paid.
func EncodeBatch(entries [][]byte, compression CompressionType) ([]byte, error) {
	// Compute uncompressed entry block size.
	dataSize := 0
	for _, e := range entries {
		if len(e) > math.MaxUint32 {
			return nil, invalidInputErr(fmt.Sprintf("entry size %d exceeds u32 max", len(e)))
		}
		dataSize += batchEntryLenSize + len(e)
	}

	switch compression {
	case CompressionNone:
		buf := make([]byte, 0, dataSize+batchFooterSize)
		for _, e := range entries {
			buf = binary.LittleEndian.AppendUint32(buf, uint32(len(e)))
			buf = append(buf, e...)
		}
		buf = append(buf, byte(compression))
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(entries)))
		buf = binary.LittleEndian.AppendUint16(buf, batchVersion)
		return buf, nil
	case CompressionZstd:
		entryBuf := make([]byte, 0, dataSize)
		for _, e := range entries {
			entryBuf = binary.LittleEndian.AppendUint32(entryBuf, uint32(len(e)))
			entryBuf = append(entryBuf, e...)
		}
		enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
		if err != nil {
			return nil, serializationErr(fmt.Sprintf("zstd encoder init failed: %v", err))
		}
		// 2x compression is a conservative hint for OTLP-shaped payloads;
		// EncodeAll grows the destination if needed.
		buf := make([]byte, 0, dataSize/2+batchFooterSize)
		buf = enc.EncodeAll(entryBuf, buf)
		if err := enc.Close(); err != nil {
			return nil, serializationErr(fmt.Sprintf("zstd encoder close failed: %v", err))
		}
		buf = append(buf, byte(compression))
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(entries)))
		buf = binary.LittleEndian.AppendUint16(buf, batchVersion)
		return buf, nil
	default:
		return nil, serializationErr(fmt.Sprintf("unsupported compression type: %d", compression))
	}
}

// DecodeBatch deserializes a binary batch into its constituent entries.
func DecodeBatch(data []byte) ([][]byte, error) {
	if len(data) < batchFooterSize {
		return nil, serializationErr("batch too small for footer")
	}

	footerStart := len(data) - batchFooterSize
	footer := data[footerStart:]

	compressionByte := footer[0]
	recordCount := binary.LittleEndian.Uint32(footer[1:5])
	version := binary.LittleEndian.Uint16(footer[5:7])

	if version != batchVersion {
		return nil, serializationErr(fmt.Sprintf("unsupported batch version: %d", version))
	}

	compType := CompressionType(compressionByte)
	recordBlock := data[:footerStart]

	var entryData []byte
	switch compType {
	case CompressionNone:
		entryData = recordBlock
	case CompressionZstd:
		dec, err := zstd.NewReader(nil)
		if err != nil {
			return nil, serializationErr(fmt.Sprintf("zstd decoder init failed: %v", err))
		}
		defer dec.Close()
		decompressed, err := dec.DecodeAll(recordBlock, nil)
		if err != nil {
			return nil, serializationErr(fmt.Sprintf("zstd decompression failed: %v", err))
		}
		entryData = decompressed
	default:
		return nil, serializationErr(fmt.Sprintf("unsupported compression type: %d", compressionByte))
	}

	entries := make([][]byte, 0, recordCount)
	offset := 0
	for i := uint32(0); i < recordCount; i++ {
		if offset+batchEntryLenSize > len(entryData) {
			return nil, serializationErr("truncated record length")
		}
		entryLen := int(binary.LittleEndian.Uint32(entryData[offset : offset+batchEntryLenSize]))
		offset += batchEntryLenSize
		if offset+entryLen > len(entryData) {
			return nil, serializationErr("truncated record data")
		}
		entry := make([]byte, entryLen)
		copy(entry, entryData[offset:offset+entryLen])
		entries = append(entries, entry)
		offset += entryLen
	}

	return entries, nil
}
