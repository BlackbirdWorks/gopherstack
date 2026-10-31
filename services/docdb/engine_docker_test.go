package docdb_test

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
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

const (
	dockerEngineWait = 5 * time.Minute
	listDBs          = "if(db.adminCommand({listDatabases:1}).ok!==1){quit(1)}"
)

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

func mongoShellExit(t *testing.T, args ...string) int {
	t.Helper()

	cmd := exec.CommandContext(t.Context(), "docker", append([]string{"run", "--rm", "--network", "host"}, args...)...)
	if err := cmd.Run(); err != nil {
		if ee, ok := errors.AsType[*exec.ExitError](err); ok {
			return ee.ExitCode()
		}

		require.NoError(t, err)
	}

	return 0
}

func TestEngineDockerTLSAndSCRAM(t *testing.T) {
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

	b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
	b.EnableEngine(docdb.EngineConfig{Runtime: &recordingEngineRuntime{EngineRuntime: er}, TLS: true})
	t.Cleanup(b.Close)

	ctx := t.Context()
	_, err = b.CreateDBClusterParameterGroup(ctx, "pg", "docdb5.0", "d", nil)
	require.NoError(t, err)

	_, err = b.CreateDBCluster(ctx, "c1", "docdb", "5.0.0", "admin1", "s3cretpassword",
		"", "pg", "", 0, false, false, 0, "", "", nil, nil, nil)
	require.NoError(t, err)

	c := waitClusterStatusFor(t, b, "available", dockerEngineWait)

	_, err = b.CreateDBInstance(ctx, "i1", "c1", "", "docdb", 0, nil, nil)
	require.NoError(t, err)

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ca.pem"), b.TLSCAPEM(), 0o644))

	host := net.JoinHostPort(c.Endpoint, strconv.Itoa(c.Port))
	shell := func(extra ...string) int {
		args := append([]string{"-v", dir + ":/c:ro", "mongo:5.0", "mongo", "--quiet", "--host", c.Endpoint,
			"--port", strconv.Itoa(c.Port)}, extra...)

		return mongoShellExit(t, args...)
	}
	login := func(pass string) []string {
		return []string{"-u", "admin1", "-p", pass, "--authenticationDatabase", "admin", "--eval", listDBs}
	}
	secure := []string{"--tls", "--tlsCAFile", "/c/ca.pem"}

	assert.Zero(t, shell(append(append([]string{}, secure...), login("s3cretpassword")...)...), "tls + scram")
	assert.NotZero(t, shell(append(append([]string{}, secure...), login("wrongpassword")...)...), "wrong password")
	assert.NotZero(t, shell(append(append([]string{}, secure...), "--eval", listDBs)...), "unauthenticated")
	assert.NotZero(t, shell(login("s3cretpassword")...), "plaintext refused while tls is enabled: "+host)

	_, err = b.ModifyDBClusterParameterGroup(ctx, "pg", map[string]string{"tls": "disabled"})
	require.NoError(t, err)
	_, err = b.RebootDBInstance(ctx, "i1")
	require.NoError(t, err)

	assert.Zero(t, shell(login("s3cretpassword")...), "plaintext + scram once tls is disabled")
	assert.NotZero(t, shell(login("wrongpassword")...), "wrong password still fails")
	assert.NotZero(t, shell(append(append([]string{}, secure...), login("s3cretpassword")...)...),
		"tls refused when disabled")
}

func TestEngineDockerPasswordChange(t *testing.T) {
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
		t.Skip("container runtime cannot exec with env")
	}

	if err = rt.Ping(t.Context()); err != nil {
		t.Skipf("container daemon unreachable: %v", err)
	}

	b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
	b.EnableEngine(docdb.EngineConfig{Runtime: &recordingEngineRuntime{EngineRuntime: er}})
	t.Cleanup(b.Close)

	ctx := t.Context()
	_, err = b.CreateDBCluster(ctx, "c1", "docdb", "5.0.0", "admin1", "s3cretpassword",
		"", "", "", 0, false, false, 0, "", "", nil, nil, nil)
	require.NoError(t, err)

	c := waitClusterStatusFor(t, b, "available", dockerEngineWait)

	_, err = b.ModifyDBCluster(ctx, "c1", "", nil, 0, "", "",
		&docdb.ModifyDBClusterOptions{MasterUserPassword: "n3wpassword!"})
	require.NoError(t, err)

	login := func(pass string) int {
		return mongoShellExit(t, "mongo:5.0", "mongo", "--quiet", "--host", c.Endpoint, "--port", strconv.Itoa(c.Port),
			"-u", "admin1", "-p", pass, "--authenticationDatabase", "admin", "--eval", listDBs)
	}

	assert.Zero(t, login("n3wpassword!"), "new password accepted")
	assert.NotZero(t, login("s3cretpassword"), "old password rejected")
}
