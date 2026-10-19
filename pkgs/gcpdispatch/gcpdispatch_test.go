package gcpdispatch_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/code"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpdispatch"
)

func TestHTTPToRPC(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sc   int
		want code.Code
	}{
		{"bad request", http.StatusBadRequest, code.Code_INVALID_ARGUMENT},
		{"unauthorized", http.StatusUnauthorized, code.Code_UNAUTHENTICATED},
		{"not found", http.StatusNotFound, code.Code_NOT_FOUND},
		{"throttled", http.StatusTooManyRequests, code.Code_RESOURCE_EXHAUSTED},
		{"unavailable", http.StatusServiceUnavailable, code.Code_UNAVAILABLE},
		{"other 5xx", http.StatusBadGateway, code.Code_UNAVAILABLE},
		{"other 4xx", http.StatusTeapot, code.Code_UNKNOWN},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, gcpdispatch.HTTPToRPC(tt.sc))
		})
	}
}

func TestBackoff(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		failures  int32
		doublings int32
		want      time.Duration
	}{
		{"first failure", 1, 3, 10 * time.Second},
		{"doubles", 3, 3, 40 * time.Second},
		{"last doubling", 4, 3, 80 * time.Second},
		{"linear after", 5, 3, 160 * time.Second},
		{"linear step", 6, 3, 240 * time.Second},
		{"capped", 7, 3, 300 * time.Second},
		{"zero failures", 0, 3, 10 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := gcpdispatch.Backoff(10*time.Second, 300*time.Second, tt.doublings, tt.failures)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestTokens(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_900_000_000, 0)
	parts := strings.Split(gcpdispatch.OIDCToken("sa@p.iam", "", "https://x.test/y", now), ".")
	require.Len(t, parts, 3)
	assert.Empty(t, parts[2])

	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	require.NoError(t, err)

	var claims map[string]any
	require.NoError(t, json.Unmarshal(raw, &claims))
	assert.Equal(t, "https://x.test/y", claims["aud"])
	assert.Equal(t, "sa@p.iam", claims["email"])

	assert.True(t, strings.HasPrefix(gcpdispatch.OAuthToken("sa@p.iam", ""), "gopherstack-unsigned-oauth."))
}
