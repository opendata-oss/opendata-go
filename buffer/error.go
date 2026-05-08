package buffer

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

	// ErrShutdown indicates the buffer has been shut down.
	ErrShutdown = errors.New("buffer shut down")

	// ErrProducerHalted indicates the producer has entered a halted
	// state because manifest CAS retries (or another fatal pipeline
	// failure) exceeded their budget. After a halt, new Append /
	// AppendContext calls return this error immediately and all
	// in-flight watchers resolve with it. Recovery requires
	// constructing a fresh Producer. F2 of Phase 3 rev-2 review.
	ErrProducerHalted = errors.New("producer halted")
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
