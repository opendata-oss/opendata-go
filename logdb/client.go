package logdb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// contentTypeJSON is the ProtoJSON media type the Log server uses for JSON
// bodies.
//
// Sending it as Accept is load-bearing. The server decides the response format
// by substring match: a header containing "application/protobuf" but not
// "application/protobuf+json" selects binary protobuf, which this client cannot
// decode. Never send a bare "application/protobuf", and never a multi-value
// Accept listing both.
const contentTypeJSON = "application/protobuf+json"

// pathAppend is the append route, registered only when the server fronts a
// writable log.
const pathAppend = "/api/v1/log/append"

// maxErrorBody bounds how much of an error body is read before giving up on
// finding a message in it.
const maxErrorBody = 64 << 10

// Client is a Log HTTP API client. It is safe for concurrent use.
type Client struct {
	baseURL *url.URL
	http    *http.Client
}

// Option configures a Client.
type Option func(*Client)

// WithHTTPClient sets the HTTP client used for every request. Use it to control
// timeouts, tune the transport, or layer retries: the Log client performs no
// retries of its own.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) { c.http = hc }
}

// New returns a Client for the Log server at baseURL, which must carry a scheme
// and host, e.g. "http://localhost:3001". Any path is kept as a prefix, so a
// server mounted under a subpath works.
//
// baseURL is required and has no default: the Log server has no canonical port.
func New(baseURL string, opts ...Option) (*Client, error) {
	if baseURL == "" {
		return nil, fmt.Errorf("%w: base URL is required", ErrInvalidInput)
	}
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("%w: parse base URL %q: %w", ErrInvalidInput, baseURL, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("%w: base URL %q needs a scheme and host", ErrInvalidInput, baseURL)
	}

	c := &Client{baseURL: u, http: http.DefaultClient}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// Append writes records to the log, assigning each one a sequence number from
// the counter shared by every key.
//
// By default the server acknowledges as soon as the records are buffered in
// memory, which does not survive a crash. Set AppendOptions.AwaitDurable to
// block until they are persisted.
func (c *Client) Append(ctx context.Context, records []Record, opts AppendOptions) (AppendResult, error) {
	if len(records) == 0 {
		return AppendResult{}, fmt.Errorf("%w: append needs at least one record", ErrInvalidInput)
	}

	in := appendRequest{Records: records, AwaitDurable: opts.AwaitDurable}

	var out appendResponse
	if err := c.doJSON(ctx, http.MethodPost, pathAppend, nil, in, &out); err != nil {
		return AppendResult{}, err
	}
	return AppendResult{
		RecordsAppended: out.RecordsAppended,
		StartSequence:   out.StartSequence,
	}, nil
}

// AppendOptions controls a single Append call.
type AppendOptions struct {
	// AwaitDurable holds the server's response until every record in the batch
	// is durable in object storage. It costs latency and, because it flushes
	// the write pipeline, throughput.
	AwaitDurable bool
}

// doJSON sends a request to path and decodes a JSON success body into out.
// A nil in sends no body; a nil out discards the response body.
func (c *Client) doJSON(ctx context.Context, method, path string, query url.Values, in, out any) error {
	var body io.Reader
	if in != nil {
		encoded, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("logdb: encode %s request: %w", path, err)
		}
		body = bytes.NewReader(encoded)
	}

	u := c.baseURL.JoinPath(path)
	u.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return fmt.Errorf("logdb: build %s request: %w", path, err)
	}
	req.Header.Set("Accept", contentTypeJSON)
	if in != nil {
		req.Header.Set("Content-Type", contentTypeJSON)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("logdb: %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return apiError(resp)
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("logdb: decode %s response: %w", path, err)
	}
	return nil
}

// apiError converts a non-200 response into an *APIError.
//
// Errors the server handles itself always carry the JSON envelope. Rejections
// that never reach a handler do not: a read-only gateway does not register the
// append route, so it answers with a plain-text 404, and method or extractor
// rejections behave the same way. Fall back to the body text, then to the
// status text.
func apiError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))

	var env errorResponse
	if err := json.Unmarshal(body, &env); err == nil && env.Message != "" {
		return &APIError{
			StatusCode: resp.StatusCode,
			Status:     env.Status,
			Message:    env.Message,
		}
	}

	message := strings.TrimSpace(string(body))
	if message == "" {
		message = http.StatusText(resp.StatusCode)
	}
	return &APIError{StatusCode: resp.StatusCode, Message: message}
}
