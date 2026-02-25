package gopherstack_test

import (
	"context"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	gopherstackmodule "github.com/blackbirdworks/gopherstack/modules/gopherstack"
)

// TestRun verifies that a Gopherstack container starts, exposes a valid
// endpoint, and responds to the health check.
// Requires Docker and a published Gopherstack image; only runs when
// GOPHERSTACK_MODULE_TESTS=1 is set.
func TestRun(t *testing.T) {
	t.Parallel()

	if os.Getenv("GOPHERSTACK_MODULE_TESTS") != "1" {
		t.Skip("skipping: set GOPHERSTACK_MODULE_TESTS=1 to run Docker-based module tests")
	}

	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx := context.Background()

	container, err := gopherstackmodule.Run(ctx, gopherstackmodule.DefaultImage)
	require.NoError(t, err)

	defer testcontainers.TerminateContainer(container)

	url, err := container.BaseURL(ctx)
	require.NoError(t, err)
	assert.NotEmpty(t, url, "expected non-empty base URL")

	// Health endpoint should return 200 OK.
	resp, err := http.Get(url + "/_gopherstack/health")
	require.NoError(t, err)

	defer resp.Body.Close()

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
