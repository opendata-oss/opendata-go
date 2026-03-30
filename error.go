package ingest

import (
	"errors"
	"fmt"
)

var (
	// ErrStorage indicates an object storage failure.
	ErrStorage = errors.New("storage error")

	// ErrSerialization indicates a batch or manifest encoding/decoding failure.
	ErrSerialization = errors.New("serialization error")

	// ErrInvalidInput indicates the caller supplied invalid arguments.
	ErrInvalidInput = errors.New("invalid input")

	// ErrFenced indicates that another consumer has taken over (epoch mismatch).
	ErrFenced = errors.New("consumer fenced: epoch mismatch")

	// ErrShutdown indicates the ingestor has been shut down.
	ErrShutdown = errors.New("ingestor shut down")
)

func storageErr(msg string) error {
	return fmt.Errorf("%w: %s", ErrStorage, msg)
}

func serializationErr(msg string) error {
	return fmt.Errorf("%w: %s", ErrSerialization, msg)
}

func invalidInputErr(msg string) error {
	return fmt.Errorf("%w: %s", ErrInvalidInput, msg)
}
