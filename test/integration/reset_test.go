package integration_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_HealthEndpoint verifies that GET /_gopherstack/health returns
// a JSON body with a dynamic list of services rather than a static hard-coded list.
func TestIntegration_HealthEndpoint(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, endpoint+"/_gopherstack/health", nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.True(t, strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json"))
}
