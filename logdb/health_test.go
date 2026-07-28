package logdb

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
)

// TestHealthy_ReportsRunning asserts a 200 from the liveness probe is success.
// The probe body is plain text, not the JSON envelope, so this also covers the
// no-body path through the shared request helper.
func TestHealthy_ReportsRunning(t *testing.T) {
	var gotPath string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_, _ = io.WriteString(w, "OK")
	})

	if err := c.Healthy(context.Background()); err != nil {
		t.Fatalf("Healthy() = %v; want nil", err)
	}
	if gotPath != "/-/healthy" {
		t.Errorf("path = %q; want %q", gotPath, "/-/healthy")
	}
}

// TestReady_ReportsServing asserts a 200 from the readiness probe is success.
func TestReady_ReportsServing(t *testing.T) {
	var gotPath string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_, _ = io.WriteString(w, "OK")
	})

	if err := c.Ready(context.Background()); err != nil {
		t.Fatalf("Ready() = %v; want nil", err)
	}
	if gotPath != "/-/ready" {
		t.Errorf("path = %q; want %q", gotPath, "/-/ready")
	}
}

// TestReady_MapsUnavailableToNotReady asserts a 503 is distinguishable from a
// generic failure, and that it still counts as a server-side error: a caller
// polling for readiness wants the specific signal, one wrapping every remote
// failure wants the general one, and both must work off the same error.
func TestReady_MapsUnavailableToNotReady(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, "Not Ready")
	})

	err := c.Ready(context.Background())
	if !errors.Is(err, ErrNotReady) {
		t.Fatalf("Ready() error = %v; want it to wrap ErrNotReady", err)
	}
	if !errors.Is(err, ErrServerError) {
		t.Errorf("Ready() error = %v; want ErrNotReady to also satisfy ErrServerError", err)
	}

	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("Ready() error = %v; want an *APIError", err)
	}
	if apiErr.Message != "Not Ready" {
		t.Errorf("Message = %q; want %q", apiErr.Message, "Not Ready")
	}
}

// TestHealthy_DoesNotReportNotReadyForOtherFailures asserts ErrNotReady means
// exactly 503 and is not a catch-all for server-side failures.
func TestHealthy_DoesNotReportNotReadyForOtherFailures(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	err := c.Healthy(context.Background())
	if !errors.Is(err, ErrServerError) {
		t.Fatalf("Healthy() error = %v; want it to wrap ErrServerError", err)
	}
	if errors.Is(err, ErrNotReady) {
		t.Errorf("Healthy() error = %v; want a 500 not to be reported as ErrNotReady", err)
	}
}
