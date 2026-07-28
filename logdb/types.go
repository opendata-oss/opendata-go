package logdb

import "time"

// Record is a single record to append. Both fields are opaque byte strings that
// the log stores but never interprets.
//
// The Key names a log stream: every record sharing a key forms one ordered,
// append-only log. Creating a stream is just writing a new key.
type Record struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

// Entry is one record read back from a stream, paired with the global sequence
// number assigned to it at append time.
type Entry struct {
	Sequence uint64 `json:"sequence"`
	Value    []byte `json:"value"`
}

// Segment describes one segment of the log: a contiguous window of the global
// sequence space spanning every key. Segments are the unit of compaction and
// retention.
type Segment struct {
	ID       uint32
	StartSeq uint64

	// StartTime is when the segment was created.
	StartTime time.Time
}

// AppendResult reports what the server did with an Append.
type AppendResult struct {
	// RecordsAppended is how many records the server accepted.
	RecordsAppended int32

	// StartSequence is the sequence assigned to the first record of the batch.
	// Sequences come from one counter shared by every key, so a batch's
	// records occupy StartSequence through StartSequence+RecordsAppended-1.
	StartSequence uint64
}

// appendRequest is the POST /api/v1/log/append body. Field order is the JSON
// field order, which the tests pin.
type appendRequest struct {
	Records []Record `json:"records"`

	// AwaitDurable is always emitted rather than omitted when false: the
	// server defaults it to false anyway, and sending it keeps the request
	// body explicit about a durability choice.
	AwaitDurable bool `json:"awaitDurable"`
}

// appendResponse is the POST /api/v1/log/append success envelope.
type appendResponse struct {
	Status          string `json:"status"`
	RecordsAppended int32  `json:"recordsAppended"`
	StartSequence   uint64 `json:"startSequence"`
}

// errorResponse is the error envelope the server returns for every error it
// handles itself. Rejections that never reach a handler are not JSON at all.
type errorResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}
