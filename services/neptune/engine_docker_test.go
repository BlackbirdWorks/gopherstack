package neptune_test

import (
	"context"
	"io"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/neptune"
)

const dockerEngineWait = 5 * time.Minute

type recordingEngineRuntime struct {
	neptune.EngineRuntime
	ids []string
	mu  sync.Mutex
}

func (r *recordingEngineRuntime) CreateAndStart(ctx context.Context, spec container.Spec) (string, error) {
	id, err := r.EngineRuntime.CreateAndStart(ctx, spec)
	if err == nil {
		r.mu.Lock()
		r.ids = append(r.ids, id)
		r.mu.Unlock()
	}

	return id, err
}

func (r *recordingEngineRuntime) Close() error {
	if c, ok := r.EngineRuntime.(io.Closer); ok {
		return c.Close()
	}

	return nil
}

func (r *recordingEngineRuntime) containerIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.ids...)
}

func dockerContainerState(t *testing.T, id string) string {
	t.Helper()

	out, err := exec.CommandContext(t.Context(), "docker", "ps", "-a", "--no-trunc",
		"--filter", "id="+id, "--format", "{{.State}}").Output()
	require.NoError(t, err)

	return strings.TrimSpace(string(out))
}

func TestEngineDockerRealGremlinServer(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	er, ok := rt.(neptune.EngineRuntime)
	if !ok {
		t.Skip("container runtime cannot stop/start containers")
	}

	if err = rt.Ping(t.Context()); err != nil {
		t.Skipf("container daemon unreachable: %v", err)
	}

	rec := &recordingEngineRuntime{EngineRuntime: er}
	b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	b.EnableEngine(neptune.EngineConfig{Runtime: rec})
	t.Cleanup(b.Close)

	createEngineCluster(t, b, "1.4.0.0")

	c := waitClusterFor(t, b, "c1", "available", dockerEngineWait)
	require.Len(t, rec.containerIDs(), 1)
	id := rec.containerIDs()[0]

	assert.Equal(t, "127.0.0.1", c.Endpoint)
	assert.Equal(t, "running", dockerContainerState(t, id))

	addr := net.JoinHostPort(c.Endpoint, strconv.Itoa(c.Port))
	require.NoError(t, neptune.ProbeGremlin(t.Context(), addr))

	_, err = b.StopDBCluster(t.Context(), "c1")
	require.NoError(t, err)
	waitClusterFor(t, b, "c1", "stopped", dockerEngineWait)
	assert.Equal(t, "exited", dockerContainerState(t, id))

	_, err = b.StartDBCluster(t.Context(), "c1")
	require.NoError(t, err)

	started := waitClusterFor(t, b, "c1", "available", dockerEngineWait)
	assert.Equal(t, c.Port, started.Port)
	assert.Equal(t, "running", dockerContainerState(t, id))
	require.NoError(t, neptune.ProbeGremlin(t.Context(), addr))

	_, err = b.DeleteDBCluster(t.Context(), "c1", neptune.DBClusterDeleteOptions{SkipFinalSnapshot: true})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return dockerContainerState(t, id) == "" }, time.Minute, 250*time.Millisecond)
}

func waitClusterFor(
	t *testing.T, b *neptune.InMemoryBackend, id, want string, wait time.Duration,
) neptune.DBCluster {
	t.Helper()

	var c neptune.DBCluster

	require.Eventually(t, func() bool {
		cs, err := b.DescribeDBClusters(t.Context(), id, neptune.DBClusterFilters{})
		if err != nil || len(cs) != 1 {
			return false
		}

		c = cs[0]

		return c.Status == want
	}, wait, 250*time.Millisecond)

	return c
}
