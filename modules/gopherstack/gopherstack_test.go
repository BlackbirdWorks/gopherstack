package gopherstack_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	gopherstackmodule "github.com/blackbirdworks/gopherstack/modules/gopherstack"
)

// TestRun verifies that a Gopherstack container starts, exposes a valid
// endpoint, and responds to the health check.
// Requires Docker and network access to pull the pre-built image; skipped in
// short mode and when the Docker provider is unavailable.
func TestRun(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx := context.Background()

	container, err := gopherstackmodule.Run(ctx, gopherstackmodule.DefaultImage)
	require.NoError(t, err)

	defer testcontainers.TerminateContainer(container) //nolint:errcheck

	url, err := container.BaseURL(ctx)
	require.NoError(t, err)
	assert.Truef(t, len(url) > 0, "expected non-empty base URL, got %q", url)

	// Health endpoint should return 200 OK.
	resp, err := http.Get(url + "/_gopherstack/health") //nolint:noctx
	require.NoError(t, err)

	defer resp.Body.Close() //nolint:errcheck

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

// TestWithEnv verifies that the WithEnv option is applied to the container
// request without requiring Docker.
func TestWithEnv(t *testing.T) {
	t.Parallel()

	req := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "dummy",
		},
	}

	opt := gopherstackmodule.WithEnv(map[string]string{"LOG_LEVEL": "debug", "DEMO": "true"})
	err := opt.Customize(&req)
	require.NoError(t, err)

	assert.Equal(t, "debug", req.Env["LOG_LEVEL"])
	assert.Equal(t, "true", req.Env["DEMO"])
}

// TestWithEnvNilMap verifies that WithEnv initializes the Env map when it is nil.
func TestWithEnvNilMap(t *testing.T) {
	t.Parallel()

	req := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "dummy",
			Env:   nil,
		},
	}

	opt := gopherstackmodule.WithEnv(map[string]string{"KEY": "value"})
	err := opt.Customize(&req)
	require.NoError(t, err)

	require.NotNil(t, req.Env)
	assert.Equal(t, "value", req.Env["KEY"])
}
