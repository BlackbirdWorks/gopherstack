package httpclient_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

func get(t *testing.T, c *http.Client, url string, trace *httptrace.ClientTrace) {
	t.Helper()

	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(t.Context(), trace), http.MethodGet, url, nil)
	require.NoError(t, err)

	resp, err := c.Do(req)
	require.NoError(t, err)

	_, err = io.Copy(io.Discard, resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
}

// A client sharing http.DefaultTransport loses its idle connections whenever any other
// httptest.Server closes; a client from this package must not.
func TestClientIsolatedFromDefaultTransportClose(t *testing.T) {
	t.Parallel()

	tests := []struct {
		newClient  func() *http.Client
		name       string
		wantReused bool
	}{
		{name: "default_transport", newClient: func() *http.Client { return &http.Client{} }, wantReused: false},
		{
			name:       "own_transport",
			newClient:  func() *http.Client { return httpclient.New(time.Minute) },
			wantReused: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte("ok"))
			}))
			t.Cleanup(srv.Close)

			c := tt.newClient()
			idle := make(chan struct{}, 1)

			get(t, c, srv.URL, &httptrace.ClientTrace{PutIdleConn: func(error) { idle <- struct{}{} }})
			<-idle

			other := httptest.NewServer(http.NotFoundHandler())
			other.Close()

			var reused bool

			get(t, c, srv.URL, &httptrace.ClientTrace{GotConn: func(i httptrace.GotConnInfo) { reused = i.Reused }})

			assert.Equal(t, tt.wantReused, reused)
		})
	}
}

func TestNewTransportDistinct(t *testing.T) {
	t.Parallel()

	a, b := httpclient.NewTransport(), httpclient.NewTransport()

	assert.NotSame(t, http.DefaultTransport, a)
	assert.NotSame(t, a, b)
	assert.NotNil(t, a.Proxy)
}

func TestIsLoopbackHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		host string
		want bool
	}{
		{"127.0.0.1", true}, {"::1", true}, {"localhost", false}, {"10.0.0.1", false}, {"", false},
	}

	for _, tt := range tests {
		t.Run(tt.host, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, httpclient.IsLoopbackHost(tt.host))
		})
	}
}
