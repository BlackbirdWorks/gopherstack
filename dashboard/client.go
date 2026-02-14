package dashboard

import (
	"net/http"
	"net/http/httptest"
)

// InMemClient adapts an [http.Handler] to be used as an HTTP client.
// It satisfies the aws.HTTPClient interface (implicitly by method signature).
type InMemClient struct {
	Handler http.Handler
}

// Do executes an HTTP request against the in-memory handler.
func (c *InMemClient) Do(req *http.Request) (*http.Response, error) {
	// AWS SDK might set RawPath inconsistently with Path when using custom endpoints with paths (e.g. /s3).
	// This confuses http.ServeMux. We clear RawPath to force ServeMux to use Path.
	req.URL.RawPath = ""

	rec := httptest.NewRecorder()
	c.Handler.ServeHTTP(rec, req)

	resp := rec.Result()

	// Ensure the body is a ReadCloser that we can fully read
	// httptest.ResponseRecorder bodies are already in memory
	return resp, nil
}

// RoundTrip executes a single HTTP transaction, returning
// a Response for the provided Request.
func (c *InMemClient) RoundTrip(req *http.Request) (*http.Response, error) {
	// Clone the request primarily to ensure the context is preserved
	// and to follow http.RoundTripper contract
	clone := req.Clone(req.Context())
	if clone.Body == nil {
		clone.Body = http.NoBody
	}

	rec := httptest.NewRecorder()
	c.Handler.ServeHTTP(rec, clone)

	return rec.Result(), nil
}

// Stream capable client for larger payloads?
// For in-memory, buffering is usually fine unless we're dealing with huge files.
// Since Gopherstack is inherently in-memory, we can accept this limitation.
// For true streaming we'd need a pipe, but http.ResponseWriter interface
// doesn't support reading from the written side simultaneously easily without
// launching a goroutine. Given the constraints, httptest.ResponseRecorder is safe.

// Ensure compilation fails if we don't satisfy the interface expected by AWS SDK.
var _ interface {
	Do(req *http.Request) (*http.Response, error)
} = (*InMemClient)(nil)
