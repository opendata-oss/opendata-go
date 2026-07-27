package logdb

import (
	"context"
	"net/http"
)

const (
	pathHealthy = "/-/healthy"
	pathReady   = "/-/ready"
)

// Healthy reports whether the server process is up. It is a liveness check
// only: it does not touch storage, so it stays healthy while the backend is
// unreachable. Use Ready to learn whether the server can actually serve.
//
// The probes answer in plain text rather than the JSON envelope, so nothing is
// decoded from a success response.
func (c *Client) Healthy(ctx context.Context) error {
	return c.doJSON(ctx, http.MethodGet, pathHealthy, nil, nil, nil)
}

// Ready reports whether the server can serve requests, which it answers by
// reading from its storage backend. A negative answer is ErrNotReady.
func (c *Client) Ready(ctx context.Context) error {
	return c.doJSON(ctx, http.MethodGet, pathReady, nil, nil, nil)
}
