package gcpfunctions_test

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/services/gcpfunctions"
)

const dockerOptIn = "GOPHERSTACK_GCF_DOCKER_TEST"

// TestDocker_NodeFunction runs a real Node.js function through the Functions Framework. It needs a container
// runtime, network access for npx and the node image, so it is opt-in via GOPHERSTACK_GCF_DOCKER_TEST=1.
func TestDocker_NodeFunction(t *testing.T) {
	t.Parallel()

	if os.Getenv(dockerOptIn) == "" {
		t.Skipf("set %s=1 to run against a real container runtime", dockerOptIn)
	}

	lr := gcpserve.NewLazyRuntime(nil, "gcpfunctions.test.runtime")
	t.Cleanup(func() { _ = lr.Close() })

	if _, err := lr.Get(); err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	e := newEnv(t, false)
	e.handler.Backend.SetRuntime(func() (gcpfunctions.Runner, error) { return lr.Get() })
	e.handler.Backend.SetStartupTimeout(5 * time.Minute)

	fn := e.createFunction(t, "real", fnSpec(e.upload(t, zipOf(t, nodeFilesFn()))))

	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Minute)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+e.handler.IngressAddr()+"/", nil)
	require.NoError(t, err)

	req.Host = hostOf(fn)

	resp, err := e.http.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
