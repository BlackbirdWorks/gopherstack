// Package testclient builds per-test HTTP clients that own their transport, so
// httptest.Server.Close (which closes idle conns on http.DefaultTransport) cannot break them.
package testclient

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

// New returns a client with a private transport and default redirect policy and no timeout,
// matching the stdlib default client. Idle connections are closed when tb finishes.
func New(tb testing.TB) *http.Client {
	tb.Helper()

	c := &http.Client{Transport: httpclient.NewTransport()}
	tb.Cleanup(c.CloseIdleConnections)

	return c
}
