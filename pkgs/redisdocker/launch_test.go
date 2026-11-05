package redisdocker_test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/redisdocker"
	"github.com/blackbirdworks/gopherstack/pkgs/redisdocker/redisdockertest"
)

func newTestEngine(t *testing.T) (*redisdocker.Engine, *redisdockertest.Fake) {
	t.Helper()

	f, eng := redisdockertest.New(t, redisdockertest.Ports(41000, 41100))

	return eng, f
}

func joined(s container.Spec) string { return strings.Join(s.Cmd, " ") }

func TestLaunchClusterTopology(t *testing.T) {
	t.Parallel()

	eng, fk := newTestEngine(t)

	g, err := eng.Launch(t.Context(), redisdocker.Spec{
		ClusterMode: true,
		Shards: []redisdocker.ShardSpec{
			{Slots: []redisdocker.SlotRange{{Start: 0, End: 99}, {Start: 5000, End: 16383}}, Replicas: 1},
			{Slots: []redisdocker.SlotRange{{Start: 100, End: 4999}}, Replicas: 1},
		},
	})
	require.NoError(t, err)

	t.Cleanup(g.Close)

	require.Len(t, fk.Specs(), 4)

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

	for i, spec := range fk.Specs() {
		port := strconv.Itoa(eps[i/2][i%2].Port)
		assert.Contains(t, joined(spec), "--cluster-enabled yes")
		assert.Contains(t, joined(spec), "--cluster-announce-hostname 127.0.0.1")
		assert.Contains(t, joined(spec), "--cluster-preferred-endpoint-type hostname")
		assert.Contains(t, joined(spec), "--port "+port)
		assert.Equal(t, []string{"127.0.0.1:" + port + ":" + port}, spec.Ports)
		assert.NotContains(t, strings.Join(spec.Env, " "), "GS_SEED")
	}

	meets := fk.Cmds("CLUSTER", "MEET")
	assert.Len(t, meets, 3)

	slots := fk.Cmds("CLUSTER", "ADDSLOTSRANGE")
	require.Len(t, slots, 2)
	assert.Equal(t, []any{"CLUSTER", "ADDSLOTSRANGE", "0", "99", "5000", "16383"}, slots[0].Args)
	assert.Equal(t, []any{"CLUSTER", "ADDSLOTSRANGE", "100", "4999"}, slots[1].Args)
	assert.Equal(t, eps[0][0].Addr(), slots[0].Addr)
	assert.Equal(t, eps[1][0].Addr(), slots[1].Addr)

	repl := fk.Cmds("CLUSTER", "REPLICATE")
	require.Len(t, repl, 2)
	assert.Equal(t, []any{"CLUSTER", "REPLICATE", "id-" + eps[0][0].Addr()}, repl[0].Args)
	assert.Equal(t, eps[0][1].Addr(), repl[0].Addr)
	assert.Equal(t, []any{"CLUSTER", "REPLICATE", "id-" + eps[1][0].Addr()}, repl[1].Args)
	assert.Equal(t, eps[1][1].Addr(), repl[1].Addr)
}

func TestLaunchReplicationWithoutClusterMode(t *testing.T) {
	t.Parallel()

	eng, fk := newTestEngine(t)

	g, err := eng.Launch(t.Context(), redisdocker.Spec{Shards: []redisdocker.ShardSpec{{Replicas: 2}}})
	require.NoError(t, err)

	t.Cleanup(g.Close)

	require.Len(t, fk.Specs(), 3)
	assert.NotContains(t, joined(fk.Specs()[0]), "cluster-enabled")
	assert.False(t, g.ClusterMode())

	eps := g.Endpoints()[0]
	of := fk.Cmds("REPLICAOF")
	require.Len(t, of, 2)
	assert.Equal(t, []any{"REPLICAOF", "10.0.0.1", strconv.Itoa(eps[0].Port)}, of[0].Args)
	assert.Equal(t, eps[1].Addr(), of[0].Addr)
	assert.Equal(t, eps[2].Addr(), of[1].Addr)
	assert.Empty(t, fk.Cmds("CLUSTER", "MEET"))
}

func TestLaunchTLSIssuesCertsAndDisablesPlaintext(t *testing.T) {
	t.Parallel()

	eng, fk := newTestEngine(t)

	g, err := eng.Launch(t.Context(), redisdocker.Spec{
		TLS: true, ClusterMode: true,
		Shards: []redisdocker.ShardSpec{{Replicas: 0}},
	})
	require.NoError(t, err)

	t.Cleanup(g.Close)

	require.Len(t, fk.Specs(), 1)
	cmd := joined(fk.Specs()[0])
	port := strconv.Itoa(g.Endpoints()[0][0].Port)

	assert.Contains(t, cmd, "--port 0 --tls-port "+port)
	assert.Contains(t, cmd, "--tls-cluster yes")
	assert.Contains(t, cmd, "--tls-replication yes")
	assert.True(t, fk.CertsPresentAtStart(fk.Specs()[0].Name), "certificate files must exist when the container starts")
	assert.NotEmpty(t, eng.CAPEM())
}

func TestLaunchTLSWithoutCAFails(t *testing.T) {
	t.Parallel()

	fk := &redisdockertest.Fake{}
	eng := redisdocker.New(redisdocker.Config{Runtime: fk, TempDir: t.TempDir()})

	_, err := eng.Launch(t.Context(), redisdocker.Spec{TLS: true, Shards: []redisdocker.ShardSpec{{}}})
	require.ErrorIs(t, err, redisdocker.ErrNoCA)
	assert.Empty(t, fk.Specs())
}

func TestLaunchValidatesSpec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err  error
		name string
		spec redisdocker.Spec
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

			eng, fk := newTestEngine(t)
			_, err := eng.Launch(t.Context(), tc.spec)
			require.ErrorIs(t, err, tc.err)
			assert.Empty(t, fk.Specs())
		})
	}
}

func TestLaunchDefaultsToEvenSlots(t *testing.T) {
	t.Parallel()

	eng, fk := newTestEngine(t)

	g, err := eng.Launch(t.Context(), redisdocker.Spec{
		ClusterMode: true, Shards: []redisdocker.ShardSpec{{}, {}},
	})
	require.NoError(t, err)

	t.Cleanup(g.Close)

	slots := fk.Cmds("CLUSTER", "ADDSLOTSRANGE")
	require.Len(t, slots, 2)
	assert.Equal(t, []any{"CLUSTER", "ADDSLOTSRANGE", "0", "8191"}, slots[0].Args)
	assert.Equal(t, []any{"CLUSTER", "ADDSLOTSRANGE", "8192", "16383"}, slots[1].Args)
}

func TestLaunchSeedsPrimariesOnly(t *testing.T) {
	t.Parallel()

	eng, fk := newTestEngine(t)

	g, err := eng.Launch(t.Context(), redisdocker.Spec{
		Shards: []redisdocker.ShardSpec{{Replicas: 1, Seed: []byte("REDIS0011")}},
	})
	require.NoError(t, err)

	t.Cleanup(g.Close)

	assert.Contains(t, strings.Join(fk.Specs()[0].Env, " "), "GS_SEED=/gs/seed-0.rdb")
	assert.NotContains(t, strings.Join(fk.Specs()[1].Env, " "), "GS_SEED")
}

func TestCloseRemovesContainersAndReleasesPorts(t *testing.T) {
	t.Parallel()

	eng, fk := newTestEngine(t)

	g, err := eng.Launch(t.Context(), redisdocker.Spec{Shards: []redisdocker.ShardSpec{{Replicas: 1}}})
	require.NoError(t, err)

	g.Close()
	g.Close()

	assert.ElementsMatch(t, []string{"c1", "c2"}, fk.Removed())
}

func TestSnapshotReadsEachShardPrimary(t *testing.T) {
	t.Parallel()

	eng, _ := newTestEngine(t)

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

	eng, fk := newTestEngine(t)

	g, err := eng.Launch(t.Context(), redisdocker.Spec{Shards: []redisdocker.ShardSpec{{Replicas: 1}}})
	require.NoError(t, err)

	t.Cleanup(g.Close)

	first := redisdocker.ACLPolicy{Users: []redisdocker.ACLUser{
		{Name: "app", AccessString: "on ~app:* +@all", Passwords: []string{"secret"}},
		{Name: "old", AccessString: "on ~* +@all", NoPassword: true},
	}}
	require.NoError(t, g.ApplyACL(t.Context(), first))

	setuser := fk.Cmds("ACL", "SETUSER")
	require.Len(t, setuser, 6, "two users plus a locked-down default on each of two nodes")
	assert.Equal(t, []any{"ACL", "SETUSER", "default", "reset"}, setuser[2].Args)

	second := redisdocker.ACLPolicy{OpenDefault: true, Users: []redisdocker.ACLUser{first.Users[0]}}
	require.NoError(t, g.ApplyACL(t.Context(), second))

	del := fk.Cmds("ACL", "DELUSER")
	require.Len(t, del, 2)
	assert.Equal(t, []any{"ACL", "DELUSER", "old"}, del[0].Args)

	opened := false

	for _, c := range fk.Cmds("ACL", "SETUSER", "default") {
		opened = opened || slicesContain(c.Args, "nopass")
	}

	assert.True(t, opened, "OpenDefault leaves default passwordless")
}

func TestApplyACLRejectsBadUsersAndNeverEchoesPasswords(t *testing.T) {
	t.Parallel()

	eng, _ := newTestEngine(t)

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
