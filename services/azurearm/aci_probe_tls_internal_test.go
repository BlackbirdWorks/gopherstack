package azurearm

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestACIProbeTLSConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		url      string
		wantSkip bool
	}{
		{name: "loopback v4", url: "https://127.0.0.1:8443/h", wantSkip: true},
		{name: "loopback v6", url: "https://[::1]:8443/h", wantSkip: true},
		{name: "public ip", url: "https://203.0.113.9/h"},
		{name: "hostname", url: "https://example.com/h"},
		{name: "localhost name", url: "https://localhost/h"},
		{name: "invalid", url: "://bad"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := aciProbeTLSConfig(tt.url)
			assert.Equal(t, tt.wantSkip, cfg.InsecureSkipVerify)
		})
	}
}

func TestDefaultACIHTTPProbeLoopbackSelfSigned(t *testing.T) {
	t.Parallel()

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	code, err := defaultACIHTTPProbe(t.Context(), ACIHTTPProbe{URL: srv.URL, Timeout: 5 * time.Second})
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, code)
}
