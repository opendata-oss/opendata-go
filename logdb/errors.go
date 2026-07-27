package logdb

import (
	"errors"
	"fmt"
	"net/http"
)

var (
	// ErrInvalidInput indicates invalid input: either arguments supplied by the
	// caller, or a request the server rejected as malformed (HTTP 4xx).
	ErrInvalidInput = errors.New("invalid input")

	// ErrNotFound indicates the route does not exist (HTTP 404). A Log server
	// running as a read-only gateway does not register the append route, so
	// Append against one fails this way rather than with a permission error.
	ErrNotFound = errors.New("not found")

	// ErrServerError indicates a server-side failure (HTTP 5xx), typically a
	// storage or encoding error.
	ErrServerError = errors.New("server error")

	// ErrNotReady indicates the readiness probe failed (HTTP 503): the server
	// is running but cannot reach its storage backend.
	//
	// It wraps ErrServerError so both readings work off one error: code polling
	// for readiness tests for ErrNotReady, while code that only cares that the
	// remote side failed tests for ErrServerError.
	ErrNotReady = fmt.Errorf("%w: not ready", ErrServerError)
)

// APIError is returned for any response the server does not answer with 200.
type APIError struct {
	// StatusCode is the HTTP status code.
	StatusCode int

	// Status is the "status" field of the error envelope. It is empty when the
	// response body was not the envelope.
	Status string

	// Message is the server's message, or a status-derived fallback when the
	// body carried none.
	Message string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("logdb: http %d: %s", e.StatusCode, e.Message)
}

// Unwrap maps the status code onto a sentinel so callers can branch with
// errors.Is instead of comparing status codes.
func (e *APIError) Unwrap() error {
	switch {
	case e.StatusCode == http.StatusNotFound:
		return ErrNotFound
	case e.StatusCode == http.StatusServiceUnavailable:
		return ErrNotReady
	case e.StatusCode >= 500:
		return ErrServerError
	case e.StatusCode >= 400:
		return ErrInvalidInput
	}
	return nil
}
