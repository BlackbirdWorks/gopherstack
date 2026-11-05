package redisdocker_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/redisdocker"
)

type fakeRuntime struct {
	specs   []container.Spec
	removed []string
	tlsSeen map[string]bool
	mu      sync.Mutex
}

func (f *fakeRuntime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.specs = append(f.specs, spec)

	if f.tlsSeen == nil {
		f.tlsSeen = map[string]bool{}
	}

	for _, m := range spec.Mounts {
		dir, _, _ := strings.Cut(m, ":")
		if _, err := os.Stat(filepath.Join(dir, "tls", "tls.key")); err == nil {
			f.tlsSeen[spec.Name] = true
		}
	}

	return "c" + strconv.Itoa(len(f.specs)), nil
}

func (f *fakeRuntime) StopAndRemove(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.removed = append(f.removed, id)

	return nil
}

func (f *fakeRuntime) Exec(_ context.Context, id string, _ []string) (container.ExecResult, error) {
	return container.ExecResult{Stdout: "10.0.0." + strings.TrimPrefix(id, "c") + " \n"}, nil
}

type sentCmd struct {
	addr string
	args []any
}

type fakeAdmin struct {
	target redisdocker.Target
	log    *cmdLog
}

type cmdLog struct {
	cmds []sentCmd
	mu   sync.Mutex
}

func (l *cmdLog) add(c sentCmd) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.cmds = append(l.cmds, c)
}

func (l *cmdLog) with(prefix ...string) []sentCmd {
	l.mu.Lock()
	defer l.mu.Unlock()

	var out []sentCmd

	for _, c := range l.cmds {
		if len(c.args) < len(prefix) {
			continue
		}

		match := true

		for i, p := range prefix {
			if fmt.Sprint(c.args[i]) != p {
				match = false
			}
		}

		if match {
			out = append(out, c)
		}
	}

	return out
}

func (a *fakeAdmin) Do(_ context.Context, args ...any) (any, error) {
	a.log.add(sentCmd{addr: a.target.Addr, args: args})

	switch strings.ToUpper(fmt.Sprint(args[0])) {
	case "PING":
		return "PONG", nil
	case "INFO":
		return "role:slave\r\nmaster_link_status:up\r\n", nil
	case "CLUSTER":
		switch fmt.Sprint(args[1]) {
		case "INFO":
			return fmt.Sprintf("cluster_state:ok\r\ncluster_known_nodes:%d\r\n", len(a.log.with("CLUSTER", "MEET"))+1), nil
		case "NODES":
			return strings.Repeat("n 1.2.3.4:1 myself,slave p 0 0 1 connected\n", len(a.log.with("CLUSTER", "REPLICATE"))), nil
		case "MYID":
			return "id-" + a.target.Addr, nil
		}
	}

	return "OK", nil
}

func (a *fakeAdmin) Close() error { return nil }

func newTestEngine(t *testing.T) (*redisdocker.Engine, *fakeRuntime, *cmdLog) {
	t.Helper()

	alloc, err := portalloc.New(41000, 41100)
	require.NoError(t, err)

	ca, err := devtls.NewCA()
	require.NoError(t, err)

	rt := &fakeRuntime{}
	log := &cmdLog{}
	eng := redisdocker.New(redisdocker.Config{
		Runtime: rt, Ports: alloc, CA: ca, Host: "127.0.0.1", Label: "test", TempDir: t.TempDir(),
		Dial:     func(tg redisdocker.Target) redisdocker.Admin { return &fakeAdmin{target: tg, log: log} },
		FetchRDB: func(_ context.Context, tg redisdocker.Target) ([]byte, error) { return []byte("REDIS0011" + tg.Addr), nil },
	})

	return eng, rt, log
}

func joined(s container.Spec) string { return strings.Join(s.Cmd, " ") }

func TestLaunchClusterTopology(t *testing.T) {
	t.Parallel()

	eng, rt, log := newTestEngine(t)

	g, err := eng.Launch(t.Context(), redisdocker.Spec{
		ClusterMode: true,
		Shards: []redisdocker.ShardSpec{
			{Slots: []redisdocker.SlotRange{{Start: 0, End: 99}, {Start: 5000, End: 16383}}, Replicas: 1},
			{Slots: []redisdocker.SlotRange{{Start: 100, End: 4999}}, Replicas: 1},
		},
	})
	require.NoError(t, err)

	t.Cleanup(g.Close)

	require.Len(t, rt.specs, 4)

	eps := g.Endpoints()
	require.Len(t, eps, 2)
	seen := map[string]bool{}

	for _, shard := range eps {
		require.Len(t, shard, 2)

		for _, ep := range shard {
			assert.False(t, seen[ep.Addr()], "endpoint %s repeated", ep.Addr())
			seen[ep.Addr()] = true
		}
	}

	for i, spec := range rt.specs {
		port := strconv.Itoa(eps[i/2][i%2].Port)
		assert.Contains(t, joined(spec), "--cluster-enabled yes")
		assert.Contains(t, joined(spec), "--cluster-announce-hostname 127.0.0.1")
		assert.Contains(t, joined(spec), "--cluster-preferred-endpoint-type hostname")
		assert.Contains(t, joined(spec), "--port "+port)
		assert.Equal(t, []string{"127.0.0.1:" + port + ":" + port}, spec.Ports)
		assert.NotContains(t, strings.Join(spec.Env, " "), "GS_SEED")
	}

	meets := log.with("CLUSTER", "MEET")
	assert.Len(t, meets, 3)

	slots := log.with("CLUSTER", "ADDSLOTSRANGE")
	require.Len(t, slots, 2)
	assert.Equal(t, []any{"CLUSTER", "ADDSLOTSRANGE", "0", "99", "5000", "16383"}, slots[0].args)
	assert.Equal(t, []any{"CLUSTER", "ADDSLOTSRANGE", "100", "4999"}, slots[1].args)
	assert.Equal(t, eps[0][0].Addr(), slots[0].addr)
	assert.Equal(t, eps[1][0].Addr(), slots[1].addr)

	repl := log.with("CLUSTER", "REPLICATE")
	require.Len(t, repl, 2)
	assert.Equal(t, []any{"CLUSTER", "REPLICATE", "id-" + eps[0][0].Addr()}, repl[0].args)
	assert.Equal(t, eps[0][1].Addr(), repl[0].addr)
	assert.Equal(t, []any{"CLUSTER", "REPLICATE", "id-" + eps[1][0].Addr()}, repl[1].args)
	assert.Equal(t, eps[1][1].Addr(), repl[1].addr)
}

func TestLaunchReplicationWithoutClusterMode(t *testing.T) {
	t.Parallel()

	eng, rt, log := newTestEngine(t)

	g, err := eng.Launch(t.Context(), redisdocker.Spec{Shards: []redisdocker.ShardSpec{{Replicas: 2}}})
	require.NoError(t, err)

	t.Cleanup(g.Close)

	require.Len(t, rt.specs, 3)
	assert.NotContains(t, joined(rt.specs[0]), "cluster-enabled")
	assert.False(t, g.ClusterMode())

	eps := g.Endpoints()[0]
	of := log.with("REPLICAOF")
	require.Len(t, of, 2)
	assert.Equal(t, []any{"REPLICAOF", "10.0.0.1", strconv.Itoa(eps[0].Port)}, of[0].args)
	assert.Equal(t, eps[1].Addr(), of[0].addr)
	assert.Equal(t, eps[2].Addr(), of[1].addr)
	assert.Empty(t, log.with("CLUSTER", "MEET"))
}

func TestLaunchTLSIssuesCertsAndDisablesPlaintext(t *testing.T) {
	t.Parallel()

	eng, rt, _ := newTestEngine(t)

	g, err := eng.Launch(t.Context(), redisdocker.Spec{
		TLS: true, ClusterMode: true,
		Shards: []redisdocker.ShardSpec{{Replicas: 0}},
	})
	require.NoError(t, err)

	t.Cleanup(g.Close)

	require.Len(t, rt.specs, 1)
	cmd := joined(rt.specs[0])
	port := strconv.Itoa(g.Endpoints()[0][0].Port)

	assert.Contains(t, cmd, "--port 0 --tls-port "+port)
	assert.Contains(t, cmd, "--tls-cluster yes")
	assert.Contains(t, cmd, "--tls-replication yes")
	assert.True(t, rt.tlsSeen[rt.specs[0].Name], "certificate files must exist when the container starts")
	assert.NotEmpty(t, eng.CAPEM())
}

func TestLaunchTLSWithoutCAFails(t *testing.T) {
	t.Parallel()

	rt := &fakeRuntime{}
	eng := redisdocker.New(redisdocker.Config{Runtime: rt, TempDir: t.TempDir()})

	_, err := eng.Launch(t.Context(), redisdocker.Spec{TLS: true, Shards: []redisdocker.ShardSpec{{}}})
	require.ErrorIs(t, err, redisdocker.ErrNoCA)
	assert.Empty(t, rt.specs)
}

func TestLaunchValidatesSpec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		spec redisdocker.Spec
		err  error
	}{
		{name: "no shards", spec: redisdocker.Spec{}, err: redisdocker.ErrSpec},
		{
			name: "two shards without cluster mode",
			spec: redisdocker.Spec{Shards: []redisdocker.ShardSpec{{}, {}}},
			err:  redisdocker.ErrSpec,
		},
		{
			name: "too many replicas",
			spec: redisdocker.Spec{Shards: []redisdocker.ShardSpec{{Replicas: 6}}},
			err:  redisdocker.ErrSpec,
		},
		{
			name: "slot gap",
			spec: redisdocker.Spec{ClusterMode: true, Shards: []redisdocker.ShardSpec{
				{Slots: []redisdocker.SlotRange{{Start: 0, End: 10}}},
				{Slots: []redisdocker.SlotRange{{Start: 12, End: 16383}}},
			}},
			err: redisdocker.ErrSlots,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			eng, rt, _ := newTestEngine(t)
			_, err := eng.Launch(t.Context(), tc.spec)
			require.ErrorIs(t, err, tc.err)
			assert.Empty(t, rt.specs)
		})
	}
}

func TestLaunchDefaultsToEvenSlots(t *testing.T) {
	t.Parallel()

	eng, _, log := newTestEngine(t)

	g, err := eng.Launch(t.Context(), redisdocker.Spec{
		ClusterMode: true, Shards: []redisdocker.ShardSpec{{}, {}},
	})
	require.NoError(t, err)

	t.Cleanup(g.Close)

	slots := log.with("CLUSTER", "ADDSLOTSRANGE")
	require.Len(t, slots, 2)
	assert.Equal(t, []any{"CLUSTER", "ADDSLOTSRANGE", "0", "8191"}, slots[0].args)
	assert.Equal(t, []any{"CLUSTER", "ADDSLOTSRANGE", "8192", "16383"}, slots[1].args)
}

func TestLaunchSeedsPrimariesOnly(t *testing.T) {
	t.Parallel()

	eng, rt, _ := newTestEngine(t)

	g, err := eng.Launch(t.Context(), redisdocker.Spec{
		Shards: []redisdocker.ShardSpec{{Replicas: 1, Seed: []byte("REDIS0011")}},
	})
	require.NoError(t, err)

	t.Cleanup(g.Close)

	assert.Contains(t, strings.Join(rt.specs[0].Env, " "), "GS_SEED=/gs/seed-0.rdb")
	assert.NotContains(t, strings.Join(rt.specs[1].Env, " "), "GS_SEED")
}

func TestCloseRemovesContainersAndReleasesPorts(t *testing.T) {
	t.Parallel()

	eng, rt, _ := newTestEngine(t)

	g, err := eng.Launch(t.Context(), redisdocker.Spec{Shards: []redisdocker.ShardSpec{{Replicas: 1}}})
	require.NoError(t, err)

	g.Close()
	g.Close()

	assert.ElementsMatch(t, []string{"c1", "c2"}, rt.removed)
}

func TestSnapshotReadsEachShardPrimary(t *testing.T) {
	t.Parallel()

	eng, _, _ := newTestEngine(t)

	g, err := eng.Launch(t.Context(), redisdocker.Spec{
		ClusterMode: true, Shards: []redisdocker.ShardSpec{{Replicas: 1}, {}},
	})
	require.NoError(t, err)

	t.Cleanup(g.Close)

	got, err := g.Snapshot(t.Context())
	require.NoError(t, err)

	eps := g.Endpoints()
	require.Len(t, got, 2)
	assert.Equal(t, "REDIS0011"+eps[0][0].Addr(), string(got[0]))
	assert.Equal(t, "REDIS0011"+eps[1][0].Addr(), string(got[1]))
}

func TestApplyACLSyncsEveryNodeAndDropsStaleUsers(t *testing.T) {
	t.Parallel()

	eng, _, log := newTestEngine(t)

	g, err := eng.Launch(t.Context(), redisdocker.Spec{Shards: []redisdocker.ShardSpec{{Replicas: 1}}})
	require.NoError(t, err)

	t.Cleanup(g.Close)

	first := redisdocker.ACLPolicy{Users: []redisdocker.ACLUser{
		{Name: "app", AccessString: "on ~app:* +@all", Passwords: []string{"secret"}},
		{Name: "old", AccessString: "on ~* +@all", NoPassword: true},
	}}
	require.NoError(t, g.ApplyACL(t.Context(), first))

	setuser := log.with("ACL", "SETUSER")
	require.Len(t, setuser, 6, "two users plus a locked-down default on each of two nodes")
	assert.Equal(t, []any{"ACL", "SETUSER", "default", "reset"}, setuser[2].args)

	second := redisdocker.ACLPolicy{OpenDefault: true, Users: []redisdocker.ACLUser{first.Users[0]}}
	require.NoError(t, g.ApplyACL(t.Context(), second))

	del := log.with("ACL", "DELUSER")
	require.Len(t, del, 2)
	assert.Equal(t, []any{"ACL", "DELUSER", "old"}, del[0].args)

	opened := false

	for _, c := range log.with("ACL", "SETUSER", "default") {
		opened = opened || slicesContain(c.args, "nopass")
	}

	assert.True(t, opened, "OpenDefault leaves default passwordless")
}

func TestApplyACLRejectsBadUsersAndNeverEchoesPasswords(t *testing.T) {
	t.Parallel()

	eng, _, _ := newTestEngine(t)

	g, err := eng.Launch(t.Context(), redisdocker.Spec{Shards: []redisdocker.ShardSpec{{}}})
	require.NoError(t, err)

	t.Cleanup(g.Close)

	err = g.ApplyACL(t.Context(), redisdocker.ACLPolicy{Users: []redisdocker.ACLUser{{Name: "a b"}}})
	require.ErrorIs(t, err, redisdocker.ErrACL)
}

func slicesContain(args []any, want string) bool {
	for _, a := range args {
		if fmt.Sprint(a) == want {
			return true
		}
	}

	return false
}
