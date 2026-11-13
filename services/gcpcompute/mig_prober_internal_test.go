package gcpcompute

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNetProberLoopbackOnly(t *testing.T) {
	t.Parallel()

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	loopback := strings.TrimPrefix(srv.URL, "https://")

	tests := []struct {
		name    string
		address string
		want    string
	}{
		{"loopback self signed", loopback, healthHealthy},
		{"public address", "192.0.2.1:443", healthTimeout},
		{"malformed address", "not-an-address", healthTimeout},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := newNetProber().Probe(t.Context(), HealthProbe{
				Type: protoHTTPS, Address: tc.address, Path: "/", Timeout: 5 * time.Second,
			})
			assert.Equal(t, tc.want, got)
		})
	}
}
