package docdb_test

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
	"github.com/blackbirdworks/gopherstack/services/docdb"
)

const dockerEngineWait = 5 * time.Minute

type recordingEngineRuntime struct {
	docdb.EngineRuntime
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

func TestEngineDockerRealMongo(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	er, ok := rt.(docdb.EngineRuntime)
	if !ok {
		t.Skip("container runtime cannot stop/start containers")
	}

	if err = rt.Ping(t.Context()); err != nil {
		t.Skipf("container daemon unreachable: %v", err)
	}

	rec := &recordingEngineRuntime{EngineRuntime: er}
	b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
	b.EnableEngine(docdb.EngineConfig{Runtime: rec})
	t.Cleanup(b.Close)

	createEngineCluster(t, b, "5.0.0")

	c := waitClusterStatusFor(t, b, "available", dockerEngineWait)
	require.Len(t, rec.containerIDs(), 1)
	id := rec.containerIDs()[0]

	assert.Equal(t, "127.0.0.1", c.Endpoint)
	assert.Equal(t, "running", dockerContainerState(t, id))

	conn, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(t.Context(), "tcp",
		net.JoinHostPort(c.Endpoint, strconv.Itoa(c.Port)))
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	_, err = b.ModifyDBCluster(t.Context(), "c1", "", nil, 0, "", "",
		&docdb.ModifyDBClusterOptions{MasterUserPassword: "n3wpassword!"})
	require.NoError(t, err)

	_, err = b.StopDBCluster(t.Context(), "c1")
	require.NoError(t, err)
	waitClusterStatusFor(t, b, "stopped", dockerEngineWait)
	require.Eventually(t, func() bool { return dockerContainerState(t, id) == "exited" },
		time.Minute, 250*time.Millisecond)

	_, err = b.StartDBCluster(t.Context(), "c1")
	require.NoError(t, err)

	started := waitClusterStatusFor(t, b, "available", dockerEngineWait)
	assert.Equal(t, c.Port, started.Port)
	assert.Equal(t, "running", dockerContainerState(t, id))

	_, err = b.DeleteDBCluster(t.Context(), "c1", &docdb.DeleteDBClusterOptions{SkipFinalSnapshot: true})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return dockerContainerState(t, id) == "" }, time.Minute, 250*time.Millisecond)
}
