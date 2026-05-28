package buffer

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/opendata-oss/opendata-go/objstore"
)

// Manifest binary format constants.
const (
	manifestVersion      uint16 = 1
	mEntryLenSize               = 4                                                             // u32 LE
	mSequenceSize               = 8                                                             // u64 LE
	mLocationLenSize            = 2                                                             // u16 LE
	mMetadataCountSize          = 4                                                             // u32 LE
	mStartIndexSize             = 4                                                             // u32 LE
	mIngestionTimeMsSize        = 8                                                             // i64 LE
	mMetadataLenSize            = 4                                                             // u32 LE
	mEntriesCountSize           = 4                                                             // u32 LE
	mEpochSize                  = 8                                                             // u64 LE
	mVersionSize                = 2                                                             // u16 LE
	mFooterSize                 = mEntriesCountSize + mSequenceSize + mEpochSize + mVersionSize // 22 bytes
)

// QueueMetadata holds per-range metadata for a queue entry.
type QueueMetadata struct {
	StartIndex      uint32
	IngestionTimeMs int64
	Payload         []byte
}

// QueueEntry represents a single entry in the queue manifest.
type QueueEntry struct {
	Sequence uint64
	Location string
	Metadata []QueueMetadata
}

// manifest is the in-memory representation of a queue manifest.
type manifest struct {
	data          []byte // existing serialized entries + footer
	appended      []byte // newly appended entries (not yet in data)
	appendedCount int
	nextSequence  uint64
	epoch         uint64
}

func emptyManifest() *manifest {
	buf := make([]byte, 0, mFooterSize)
	buf = binary.LittleEndian.AppendUint32(buf, 0) // entries count
	buf = binary.LittleEndian.AppendUint64(buf, 0) // next sequence
	buf = binary.LittleEndian.AppendUint64(buf, 0) // epoch
	buf = binary.LittleEndian.AppendUint16(buf, manifestVersion)
	return &manifest{
		data: buf,
	}
}

func manifestFromBytes(data []byte) (*manifest, error) {
	if len(data) == 0 {
		return nil, serializationErr("queue manifest data must not be empty")
	}
	if len(data) < mFooterSize {
		return nil, serializationErr("queue manifest too short for footer")
	}

	versionStart := len(data) - mVersionSize
	version := binary.LittleEndian.Uint16(data[versionStart:])
	if version != manifestVersion {
		return nil, serializationErr(fmt.Sprintf("unsupported queue manifest version: %d", version))
	}

	epochStart := len(data) - mVersionSize - mEpochSize
	epoch := binary.LittleEndian.Uint64(data[epochStart : epochStart+mEpochSize])

	seqStart := len(data) - mVersionSize - mEpochSize - mSequenceSize
	nextSeq := binary.LittleEndian.Uint64(data[seqStart : seqStart+mSequenceSize])

	return &manifest{
		data:         data,
		nextSequence: nextSeq,
		epoch:        epoch,
	}, nil
}

func (m *manifest) existingEntriesCount() uint32 {
	if len(m.data) == 0 {
		return 0
	}
	footerStart := len(m.data) - mFooterSize
	return binary.LittleEndian.Uint32(m.data[footerStart : footerStart+mEntriesCountSize])
}

func (m *manifest) append(entry *QueueEntry) error {
	sequenced := *entry
	sequenced.Sequence = m.nextSequence
	encoded, err := encodeQueueEntry(&sequenced)
	if err != nil {
		return err
	}
	m.appended = append(m.appended, encoded...)
	m.nextSequence++
	m.appendedCount++
	return nil
}

func (m *manifest) toBytes() ([]byte, error) {
	if len(m.appended) == 0 {
		return m.data, nil
	}

	baseCount := m.existingEntriesCount()
	var prefix []byte
	if len(m.data) > 0 {
		footerStart := len(m.data) - mFooterSize
		prefix = m.data[:footerStart]
	}

	totalCount := uint64(baseCount) + uint64(m.appendedCount)
	if totalCount > uint64(^uint32(0)) {
		return nil, serializationErr(fmt.Sprintf(
			"total entry count %d exceeds u32 max", totalCount))
	}

	buf := make([]byte, 0, len(prefix)+len(m.appended)+mFooterSize)
	buf = append(buf, prefix...)
	buf = append(buf, m.appended...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(totalCount))
	buf = binary.LittleEndian.AppendUint64(buf, m.nextSequence)
	buf = binary.LittleEndian.AppendUint64(buf, m.epoch)
	buf = binary.LittleEndian.AppendUint16(buf, manifestVersion)
	return buf, nil
}

func encodeQueueEntry(entry *QueueEntry) ([]byte, error) {
	if len(entry.Location) > int(^uint16(0)) {
		return nil, invalidInputErr(fmt.Sprintf("location length %d exceeds u16 max", len(entry.Location)))
	}

	metadataSize := mMetadataCountSize
	for _, m := range entry.Metadata {
		if len(m.Payload) > int(^uint32(0)) {
			return nil, invalidInputErr(fmt.Sprintf("metadata payload size %d exceeds u32 max", len(m.Payload)))
		}
		metadataSize += mStartIndexSize + mIngestionTimeMsSize + mMetadataLenSize + len(m.Payload)
	}

	entryBodyLen := mSequenceSize + mLocationLenSize + len(entry.Location) + metadataSize

	buf := make([]byte, 0, mEntryLenSize+entryBodyLen)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(entryBodyLen))
	buf = binary.LittleEndian.AppendUint64(buf, entry.Sequence)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(entry.Location)))
	buf = append(buf, entry.Location...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(entry.Metadata)))
	for _, m := range entry.Metadata {
		buf = binary.LittleEndian.AppendUint32(buf, m.StartIndex)
		buf = binary.LittleEndian.AppendUint64(buf, uint64(m.IngestionTimeMs))
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(m.Payload)))
		buf = append(buf, m.Payload...)
	}
	return buf, nil
}

// DecodeManifestEntries parses all queue entries from manifest bytes.
func DecodeManifestEntries(data []byte) ([]QueueEntry, error) {
	if len(data) < mFooterSize {
		return nil, serializationErr("manifest too short for footer")
	}
	footerStart := len(data) - mFooterSize
	count := binary.LittleEndian.Uint32(data[footerStart : footerStart+mEntriesCountSize])

	offset := 0
	entries := make([]QueueEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		entry, newOffset, err := decodeQueueEntry(data, offset, footerStart)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
		offset = newOffset
	}
	return entries, nil
}

func decodeQueueEntry(data []byte, offset, end int) (QueueEntry, int, error) {
	if offset+mEntryLenSize > end {
		return QueueEntry{}, 0, serializationErr("queue entry corrupt: entry length field does not fit")
	}

	entryLen := int(binary.LittleEndian.Uint32(data[offset : offset+mEntryLenSize]))
	offset += mEntryLenSize

	if offset+entryLen > end {
		return QueueEntry{}, 0, serializationErr("queue entry corrupt: entry shorter than declared length")
	}
	entryEnd := offset + entryLen

	seq := binary.LittleEndian.Uint64(data[offset : offset+mSequenceSize])
	offset += mSequenceSize

	locLen := int(binary.LittleEndian.Uint16(data[offset : offset+mLocationLenSize]))
	offset += mLocationLenSize

	if offset+locLen+mMetadataCountSize > entryEnd {
		return QueueEntry{}, 0, serializationErr("queue entry corrupt: location + metadata count overflow")
	}

	location := string(data[offset : offset+locLen])
	offset += locLen

	metadataCount := int(binary.LittleEndian.Uint32(data[offset : offset+mMetadataCountSize]))
	offset += mMetadataCountSize

	metadata := make([]QueueMetadata, 0, metadataCount)
	for j := 0; j < metadataCount; j++ {
		if offset+mStartIndexSize > entryEnd {
			return QueueEntry{}, 0, serializationErr("queue entry corrupt: start index overflow")
		}
		startIndex := binary.LittleEndian.Uint32(data[offset : offset+mStartIndexSize])
		offset += mStartIndexSize

		if offset+mIngestionTimeMsSize > entryEnd {
			return QueueEntry{}, 0, serializationErr("queue entry corrupt: ingestion time overflow")
		}
		ingestionTimeMs := int64(binary.LittleEndian.Uint64(data[offset : offset+mIngestionTimeMsSize]))
		offset += mIngestionTimeMsSize

		if offset+mMetadataLenSize > entryEnd {
			return QueueEntry{}, 0, serializationErr("queue entry corrupt: metadata length overflow")
		}
		mLen := int(binary.LittleEndian.Uint32(data[offset : offset+mMetadataLenSize]))
		offset += mMetadataLenSize

		if offset+mLen > entryEnd {
			return QueueEntry{}, 0, serializationErr("queue entry corrupt: metadata payload overflow")
		}
		payload := make([]byte, mLen)
		copy(payload, data[offset:offset+mLen])
		offset += mLen

		metadata = append(metadata, QueueMetadata{
			StartIndex:      startIndex,
			IngestionTimeMs: ingestionTimeMs,
			Payload:         payload,
		})
	}

	return QueueEntry{
		Sequence: seq,
		Location: location,
		Metadata: metadata,
	}, entryEnd, nil
}

// conflictCounter tracks writes and conflicts for computing the conflict rate.
type conflictCounter struct {
	writeCount    atomic.Uint64
	conflictCount atomic.Uint64
}

func (c *conflictCounter) recordWrite() {
	c.writeCount.Add(1)
}

func (c *conflictCounter) recordConflict() {
	c.conflictCount.Add(1)
}

func (c *conflictCounter) conflictRate() float64 {
	writes := c.writeCount.Load()
	if writes == 0 {
		return 0
	}
	conflicts := c.conflictCount.Load()
	rate := (float64(conflicts) / float64(writes)) * 100.0
	if rate > 100 {
		return 100
	}
	return rate
}

// manifestEnqueuer appends entries to a shared manifest using optimistic concurrency.
type manifestEnqueuer struct {
	store        objstore.ObjectStore
	manifestPath string
	counter      conflictCounter
}

func newManifestEnqueuer(store objstore.ObjectStore, manifestPath string) *manifestEnqueuer {
	return &manifestEnqueuer{
		store:        store,
		manifestPath: manifestPath,
	}
}

// enqueueItem is one entry to append in a coalesced CAS round trip.
// Lets the ManifestCommitter append up to ManifestAppendBatchSize
// ordinals atomically.
type enqueueItem struct {
	Location string
	Metadata []QueueMetadata
}

// enqueueBatch appends N queue entries to the manifest in **one** CAS
// round trip, retrying automatically on optimistic concurrency
// conflicts. All items succeed together or none do — there is no
// partial-CAS state visible to readers.
//
// Returns the number of CAS conflicts that were retried before
// succeeding (or the final error). Zero items is a no-op (returns 0,
// nil).
func (p *manifestEnqueuer) enqueueBatch(ctx context.Context, items []enqueueItem) (int, error) {
	if len(items) == 0 {
		return 0, nil
	}
	conflicts := 0

	for {
		m, version, err := p.readManifest(ctx)
		if err != nil {
			return conflicts, err
		}

		for i := range items {
			entry := &QueueEntry{
				Location: items[i].Location,
				Metadata: items[i].Metadata,
			}
			if err := m.append(entry); err != nil {
				return conflicts, err
			}
		}

		data, err := m.toBytes()
		if err != nil {
			return conflicts, err
		}

		p.counter.recordWrite()
		err = p.store.PutIfMatch(ctx, p.manifestPath, data, version)
		if err == nil {
			return conflicts, nil
		}
		if errors.Is(err, objstore.ErrPreconditionFailed) {
			p.counter.recordConflict()
			conflicts++
			continue
		}
		return conflicts, storageErr(err.Error())
	}
}

func (p *manifestEnqueuer) readManifest(ctx context.Context) (*manifest, *objstore.Version, error) {
	result, err := p.store.Get(ctx, p.manifestPath)
	if errors.Is(err, objstore.ErrNotFound) {
		return emptyManifest(), nil, nil
	}
	if err != nil {
		return nil, nil, storageErr(err.Error())
	}
	m, err := manifestFromBytes(result.Data)
	if err != nil {
		return nil, nil, err
	}
	return m, &result.Version, nil
}

func (p *manifestEnqueuer) conflictRate() float64 {
	return p.counter.conflictRate()
}
