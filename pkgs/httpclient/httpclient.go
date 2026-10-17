// Package httpclient builds outbound HTTP clients that own their transport.
package httpclient

import (
	"net/http"
	"time"
)

// NewTransport returns a clone of http.DefaultTransport that no other client shares, so
// CloseIdleConnections on the default transport (httptest.Server.Close) cannot reach it.
func NewTransport() *http.Transport {
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return &http.Transport{Proxy: http.ProxyFromEnvironment}
	}

	return base.Clone()
}

// New returns a client with its own transport and the given timeout. Create it once per
// backend and reuse it so connections are pooled.
func New(timeout time.Duration) *http.Client {
	return &http.Client{Timeout: timeout, Transport: NewTransport()}
}
