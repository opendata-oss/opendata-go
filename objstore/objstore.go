// Package objstore defines an object storage interface compatible with the
// opendata buffer manifest's optimistic-concurrency protocol.
package objstore

import (
	"context"
	"errors"
)

// ErrNotFound is returned when the requested object does not exist.
var ErrNotFound = errors.New("object not found")

// ErrPreconditionFailed is returned when a conditional put fails because the
// object was modified since it was last read.
var ErrPreconditionFailed = errors.New("precondition failed")

// Version is an opaque token representing the version of an object. It is used
// for conditional puts (optimistic concurrency).
type Version struct {
	ETag string
}

// GetResult holds the data and version returned by a Get operation.
type GetResult struct {
	Data    []byte
	Version Version
}

// ObjectStore is the interface that object storage backends must implement.
type ObjectStore interface {
	// Get retrieves the object at the given path.
	// Returns ErrNotFound if the object does not exist.
	Get(ctx context.Context, path string) (GetResult, error)

	// Put writes data to the given path, unconditionally overwriting any
	// existing object.
	Put(ctx context.Context, path string, data []byte) error

	// PutIfMatch writes data to the given path only if the current version
	// matches the provided version. If version is nil, the object must not
	// already exist (create mode).
	// Returns ErrPreconditionFailed on version mismatch.
	PutIfMatch(ctx context.Context, path string, data []byte, version *Version) error

	// Delete removes the object at the given path. It is not an error to
	// delete a non-existent object.
	Delete(ctx context.Context, path string) error
}
