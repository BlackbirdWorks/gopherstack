package docdb_test

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
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

var errRuntimeDown = errors.New("runtime down")

type fakeEngineRuntime struct {
	createErr error
	onCreate  func(spec container.Spec)
	execs     [][]string
	envs      [][]string
	specs     []container.Spec
	removed   []string
	stopped   []string
	started   []string
	mu        sync.Mutex
	execExit  int
}

func (f *fakeEngineRuntime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.createErr != nil {
		return "", f.createErr
	}

	f.specs = append(f.specs, spec)

	if f.onCreate != nil {
		f.onCreate(spec)
	}

	return "ctr-" + spec.Name, nil
}

func (f *fakeEngineRuntime) StopAndRemove(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.removed = append(f.removed, id)

	return nil
}

func (f *fakeEngineRuntime) StopContainer(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.stopped = append(f.stopped, id)

	return nil
}

func (f *fakeEngineRuntime) StartContainer(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.started = append(f.started, id)

	return nil
}

func (f *fakeEngineRuntime) Exec(_ context.Context, _ string, cmd []string) (container.ExecResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.execs = append(f.execs, cmd)

	return container.ExecResult{ExitCode: f.execExit, Stderr: "boom"}, nil
}

func (f *fakeEngineRuntime) ExecEnv(
	_ context.Context, _ string, cmd, env []string,
) (container.ExecResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.execs = append(f.execs, cmd)
	f.envs = append(f.envs, env)

	return container.ExecResult{ExitCode: f.execExit, Stderr: "boom s3cretpassword n3wpassword!"}, nil
}

func (f *fakeEngineRuntime) execEnvs() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([][]string(nil), f.envs...)
}

func (f *fakeEngineRuntime) snapshot() ([]container.Spec, []string, []string, []string, [][]string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]container.Spec(nil), f.specs...), append([]string(nil), f.removed...),
		append([]string(nil), f.stopped...), append([]string(nil), f.started...), append([][]string(nil), f.execs...)
}

func (f *fakeEngineRuntime) createdSpecs() []container.Spec {
	specs, _, _, _, _ := f.snapshot()

	return specs
}

func (f *fakeEngineRuntime) removedIDs() []string {
	_, removed, _, _, _ := f.snapshot()

	return removed
}

func (f *fakeEngineRuntime) execCalls() [][]string {
	_, _, _, _, execs := f.snapshot()

	return execs
}

type gatedMongoProbe struct {
	gate chan struct{}
}

func newGatedMongoProbe() *gatedMongoProbe { return &gatedMongoProbe{gate: make(chan struct{})} }

func (g *gatedMongoProbe) probe(ctx context.Context, _ string) error {
	select {
	case <-g.gate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func engineBackend(
	t *testing.T, rt *fakeEngineRuntime, probe func(context.Context, string) error,
) *docdb.InMemoryBackend {
	t.Helper()

	b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
	b.EnableEngine(docdb.EngineConfig{Runtime: rt, Probe: probe, StartTimeout: time.Minute})
	t.Cleanup(b.Close)

	return b
}

func readyProbe(context.Context, string) error { return nil }

func createEngineCluster(t *testing.T, b *docdb.InMemoryBackend, version string) *docdb.DBCluster {
	t.Helper()

	c, err := b.CreateDBCluster(t.Context(), "c1", "docdb", version, "admin1", "s3cretpassword",
		"", "", "", 0, false, false, 0, "", "", nil, nil, nil)
	require.NoError(t, err)

	return c
}

func clusterStatus(b *docdb.InMemoryBackend, id string) docdb.DBCluster {
	cs, err := b.DescribeDBClusters(context.Background(), id)
	if err != nil || len(cs) == 0 {
		return docdb.DBCluster{}
	}

	return cs[0]
}

func waitClusterStatus(t *testing.T, b *docdb.InMemoryBackend, want string) docdb.DBCluster {
	t.Helper()

	return waitClusterStatusFor(t, b, want, 10*time.Second)
}

func waitClusterStatusFor(t *testing.T, b *docdb.InMemoryBackend, want string, wait time.Duration) docdb.DBCluster {
	t.Helper()

	var c docdb.DBCluster

	require.Eventually(t, func() bool {
		c = clusterStatus(b, "c1")

		return c.Status == want
	}, wait, 5*time.Millisecond)

	return c
}

func publishedPort(t *testing.T, spec container.Spec) int {
	t.Helper()

	require.Len(t, spec.Ports, 1)
	parts := strings.Split(spec.Ports[0], ":")
	require.Len(t, parts, 3)
	assert.Equal(t, "27017", parts[2])

	p, err := strconv.Atoi(parts[1])
	require.NoError(t, err)

	return p
}

func TestEngineCreateClusterStartsMongo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		version   string
		wantImage string
	}{
		{name: "docdb36", version: "3.6.0", wantImage: "mongo:4.4"},
		{name: "docdb40", version: "4.0.0", wantImage: "mongo:4.4"},
		{name: "docdb50", version: "5.0.0", wantImage: "mongo:5.0"},
		{name: "default", version: "", wantImage: "mongo:4.4"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := &fakeEngineRuntime{}
			gate := newGatedMongoProbe()
			b := engineBackend(t, rt, gate.probe)

			created := createEngineCluster(t, b, tt.version)
			assert.Equal(t, "creating", created.Status)

			_, err := b.CreateDBInstance(t.Context(), "i1", "c1", "", "docdb", 0, nil, nil)
			require.NoError(t, err)

			insts, err := b.DescribeDBInstances(t.Context(), "i1", "")
			require.NoError(t, err)
			assert.Equal(t, "creating", insts[0].DBInstanceStatus)

			close(gate.gate)
			c := waitClusterStatus(t, b, "available")

			specs := rt.createdSpecs()
			require.Len(t, specs, 1)
			assert.Equal(t, tt.wantImage, specs[0].Image)
			assert.Contains(t, specs[0].Env, "MONGO_INITDB_ROOT_USERNAME=admin1")
			assert.Contains(t, specs[0].Env, "MONGO_INITDB_ROOT_PASSWORD=s3cretpassword")
			assert.True(t, strings.HasPrefix(specs[0].Name, "gopherstack-docdb-"))

			port := publishedPort(t, specs[0])
			assert.Equal(t, "127.0.0.1", c.Endpoint)
			assert.Equal(t, "127.0.0.1", c.ReaderEndpoint)
			assert.Equal(t, port, c.Port)

			require.Eventually(t, func() bool {
				got, derr := b.DescribeDBInstances(t.Context(), "i1", "")
				require.NoError(t, derr)
				insts = got

				return got[0].DBInstanceStatus == "available"
			}, 10*time.Second, 5*time.Millisecond)

			assert.Equal(t, "127.0.0.1", insts[0].Endpoint)
			assert.Equal(t, port, insts[0].Port)
		})
	}
}

func TestEngineInstanceJoinsReadyCluster(t *testing.T) {
	t.Parallel()

	rt := &fakeEngineRuntime{}
	b := engineBackend(t, rt, readyProbe)
	createEngineCluster(t, b, "5.0.0")
	c := waitClusterStatus(t, b, "available")

	inst, err := b.CreateDBInstance(t.Context(), "i1", "c1", "", "docdb", 0, nil, nil)
	require.NoError(t, err)

	assert.Equal(t, "available", inst.DBInstanceStatus)
	assert.Equal(t, "127.0.0.1", inst.Endpoint)
	assert.Equal(t, c.Port, inst.Port)
	assert.Len(t, rt.createdSpecs(), 1)
}

func TestEngineStartFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		rt    *fakeEngineRuntime
		probe func(context.Context, string) error
		name  string
	}{
		{name: "create_error", rt: &fakeEngineRuntime{createErr: errRuntimeDown}, probe: readyProbe},
		{
			name:  "never_ready",
			rt:    &fakeEngineRuntime{},
			probe: func(context.Context, string) error { return errRuntimeDown },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			b.EnableEngine(docdb.EngineConfig{Runtime: tt.rt, Probe: tt.probe, StartTimeout: 50 * time.Millisecond})
			t.Cleanup(b.Close)

			createEngineCluster(t, b, "5.0.0")
			waitClusterStatus(t, b, "failed")

			require.Eventually(t, func() bool {
				return len(tt.rt.createdSpecs()) == len(tt.rt.removedIDs())
			}, 10*time.Second, 5*time.Millisecond)
		})
	}
}

func TestEngineStopStartCluster(t *testing.T) {
	t.Parallel()

	rt := &fakeEngineRuntime{}
	b := engineBackend(t, rt, readyProbe)
	createEngineCluster(t, b, "5.0.0")
	before := waitClusterStatus(t, b, "available")

	_, err := b.StopDBCluster(t.Context(), "c1")
	require.NoError(t, err)

	stopped := waitClusterStatus(t, b, "stopped")
	_, _, stops, _, _ := rt.snapshot()
	require.Len(t, stops, 1)
	assert.Equal(t, before.Port, stopped.Port)

	_, err = b.StartDBCluster(t.Context(), "c1")
	require.NoError(t, err)

	started := waitClusterStatus(t, b, "available")
	_, _, _, starts, _ := rt.snapshot()
	assert.Equal(t, stops, starts)
	assert.Equal(t, before.Port, started.Port)
	assert.Equal(t, "127.0.0.1", started.Endpoint)
	assert.Empty(t, rt.removedIDs())
}

func TestEngineRemovesContainers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		teardown func(t *testing.T, b *docdb.InMemoryBackend)
		name     string
	}{
		{
			name: "delete_cluster",
			teardown: func(t *testing.T, b *docdb.InMemoryBackend) {
				t.Helper()

				_, err := b.DeleteDBCluster(t.Context(), "c1", &docdb.DeleteDBClusterOptions{SkipFinalSnapshot: true})
				require.NoError(t, err)
			},
		},
		{name: "reset", teardown: func(_ *testing.T, b *docdb.InMemoryBackend) { b.Reset() }},
		{name: "close", teardown: func(_ *testing.T, b *docdb.InMemoryBackend) { b.Close() }},
		{
			name: "shutdown",
			teardown: func(t *testing.T, b *docdb.InMemoryBackend) {
				t.Helper()
				docdb.NewHandler(b).Shutdown(t.Context())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := &fakeEngineRuntime{}
			b := engineBackend(t, rt, readyProbe)
			createEngineCluster(t, b, "5.0.0")
			waitClusterStatus(t, b, "available")

			tt.teardown(t, b)

			require.Eventually(t, func() bool { return len(rt.removedIDs()) == 1 }, 10*time.Second, 5*time.Millisecond)
			assert.Equal(t, "ctr-"+rt.createdSpecs()[0].Name, rt.removedIDs()[0])
		})
	}
}

func TestEngineDeleteDuringStartupRemovesContainer(t *testing.T) {
	t.Parallel()

	rt := &fakeEngineRuntime{}
	gate := newGatedMongoProbe()
	b := engineBackend(t, rt, gate.probe)
	createEngineCluster(t, b, "5.0.0")

	require.Eventually(t, func() bool { return len(rt.createdSpecs()) == 1 }, 10*time.Second, 5*time.Millisecond)

	_, err := b.DeleteDBCluster(t.Context(), "c1", &docdb.DeleteDBClusterOptions{SkipFinalSnapshot: true})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return len(rt.removedIDs()) == 1 }, 10*time.Second, 5*time.Millisecond)
}

func TestEngineModifyMasterPassword(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		version  string
		execExit int
		wantErr  bool
	}{
		{name: "applied", version: "5.0.0"},
		{name: "old_version", version: "3.6.0"},
		{name: "exec_failure", version: "5.0.0", execExit: 1, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := &fakeEngineRuntime{execExit: tt.execExit}
			b := engineBackend(t, rt, readyProbe)
			createEngineCluster(t, b, tt.version)
			waitClusterStatus(t, b, "available")

			_, err := b.ModifyDBCluster(t.Context(), "c1", "", nil, 0, "", "",
				&docdb.ModifyDBClusterOptions{MasterUserPassword: "n3wpassword!"})
			if tt.wantErr {
				require.ErrorIs(t, err, docdb.ErrInvalidClusterState)
			} else {
				require.NoError(t, err)
			}

			execs := rt.execCalls()
			require.Len(t, execs, 1)
			assert.Equal(t, "mongo", execs[0][0])

			argv := strings.Join(execs[0], " ")
			assert.NotContains(t, argv, "s3cretpassword")
			assert.NotContains(t, argv, "n3wpassword!")

			envs := rt.execEnvs()
			require.Len(t, envs, 1)
			assert.Contains(t, envs[0], "GS_OLD_PW=s3cretpassword")
			assert.Contains(t, envs[0], "GS_NEW_PW=n3wpassword!")

			if err != nil {
				assert.NotContains(t, err.Error(), "s3cretpassword")
				assert.NotContains(t, err.Error(), "n3wpassword!")
			}
		})
	}
}

func TestEngineModifyKeepsPublishedEndpoint(t *testing.T) {
	t.Parallel()

	rt := &fakeEngineRuntime{}
	b := engineBackend(t, rt, readyProbe)
	createEngineCluster(t, b, "5.0.0")
	before := waitClusterStatus(t, b, "available")

	got, err := b.ModifyDBCluster(t.Context(), "c1", "", nil, 0, "", "",
		&docdb.ModifyDBClusterOptions{Port: 1234, NewDBClusterIdentifier: "c2"})
	require.NoError(t, err)

	assert.Equal(t, "c2", got.DBClusterIdentifier)
	assert.Equal(t, before.Port, got.Port)
	assert.Equal(t, "127.0.0.1", got.Endpoint)

	_, err = b.DeleteDBCluster(t.Context(), "c2", &docdb.DeleteDBClusterOptions{SkipFinalSnapshot: true})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return len(rt.removedIDs()) == 1 }, 10*time.Second, 5*time.Millisecond)
}

func TestEngineRestoreRelaunchesClusters(t *testing.T) {
	t.Parallel()

	src := docdb.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := src.CreateDBCluster(t.Context(), "c1", "docdb", "5.0.0", "admin1", "s3cretpassword",
		"", "", "", 0, false, false, 0, "", "", nil, nil, nil)
	require.NoError(t, err)

	snap := src.Snapshot(t.Context())

	rt := &fakeEngineRuntime{}
	b := engineBackend(t, rt, readyProbe)
	require.NoError(t, b.Restore(t.Context(), snap))

	c := waitClusterStatus(t, b, "available")
	specs := rt.createdSpecs()
	require.Len(t, specs, 1)
	assert.Equal(t, "mongo:5.0", specs[0].Image)
	assert.Equal(t, publishedPort(t, specs[0]), c.Port)
}

func TestEngineDisabledKeepsMetadataOnly(t *testing.T) {
	t.Parallel()

	b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
	t.Cleanup(b.Close)

	c := createEngineCluster(t, b, "5.0.0")

	assert.Equal(t, "available", c.Status)
	assert.Equal(t, "c1.cluster.docdb.us-east-1.amazonaws.com", c.Endpoint)
	assert.Equal(t, 27017, c.Port)
}

func serveFakeMongod(t *testing.T, ln net.Listener) {
	t.Helper()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			go answerIsMaster(conn)
		}
	}()
}

func answerIsMaster(conn net.Conn) {
	defer func() { _ = conn.Close() }()

	hdr := make([]byte, 16)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		return
	}

	rest := make([]byte, binary.LittleEndian.Uint32(hdr[0:4])-16)
	if _, err := io.ReadFull(conn, rest); err != nil {
		return
	}

	doc := []byte{0, 0, 0, 0, 0x01}
	doc = append(doc, "ok\x00"...)
	doc = binary.LittleEndian.AppendUint64(doc, 0x3FF0000000000000)
	doc = append(doc, 0)
	binary.LittleEndian.PutUint32(doc, uint32(len(doc)))

	msg := binary.LittleEndian.AppendUint32(nil, uint32(16+5+len(doc)))
	msg = binary.LittleEndian.AppendUint32(msg, 2)
	msg = binary.LittleEndian.AppendUint32(msg, 1)
	msg = binary.LittleEndian.AppendUint32(msg, 2013)
	msg = binary.LittleEndian.AppendUint32(msg, 0)
	msg = append(msg, 0)
	msg = append(msg, doc...)
	_, _ = conn.Write(msg)
}

func TestEngineDefaultProbeSpeaksMongoWire(t *testing.T) {
	t.Parallel()

	rt := &fakeEngineRuntime{}
	rt.onCreate = func(spec container.Spec) {
		port := strings.Split(spec.Ports[0], ":")[1]

		ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:"+port)
		if err != nil {
			return
		}

		serveFakeMongod(t, ln)
		t.Cleanup(func() { _ = ln.Close() })
	}

	b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
	b.EnableEngine(docdb.EngineConfig{Runtime: rt, StartTimeout: time.Minute})
	t.Cleanup(b.Close)

	createEngineCluster(t, b, "5.0.0")
	waitClusterStatus(t, b, "available")
}
