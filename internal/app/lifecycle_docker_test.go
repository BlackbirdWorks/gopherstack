package app

import (
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const lifecycleDockerImageEnv = "GOPHERSTACK_LIFECYCLE_DOCKER_IMAGE"

// Opt-in: set GOPHERSTACK_LIFECYCLE_DOCKER_IMAGE=repo:tag to an image present locally.
func TestLifecycleDockerStartStop(t *testing.T) {
	t.Parallel()

	ref := os.Getenv(lifecycleDockerImageEnv)
	if ref == "" {
		t.Skipf("set %s to run against a real container runtime", lifecycleDockerImageEnv)
	}

	repo, tag, _ := strings.Cut(ref, ":")
	if tag == "" {
		tag = "latest"
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	port := strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
	require.NoError(t, l.Close())

	d := newLifecycleDeps()
	target := LifecycleTarget{Name: "gs-lifecycle-" + port, StateDir: t.TempDir()}
	start := StartCmd{
		LifecycleTarget: target, Image: repo, Tag: tag, Bind: "127.0.0.1", Port: port, Timeout: 2 * time.Minute,
	}
	t.Cleanup(func() { _ = (&StopCmd{LifecycleTarget: target}).run(t.Context(), d) })

	require.NoError(t, start.run(t.Context(), d))

	h, err := d.health(t.Context(), endpointFor("127.0.0.1", port))
	require.NoError(t, err)
	assert.Equal(t, "ok", h.Status)

	require.NoError(t, (&StopCmd{LifecycleTarget: target}).run(t.Context(), d))
	require.NoError(t, (&StopCmd{LifecycleTarget: target}).run(t.Context(), d))

	_, err = d.health(t.Context(), endpointFor("127.0.0.1", port))
	require.Error(t, err)
}
