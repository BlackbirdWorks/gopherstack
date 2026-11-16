package redisdocker_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/redisdocker"
)

func TestFailoverPromotesReplica(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cluster bool
	}{
		{name: "cluster", cluster: true},
		{name: "standalone"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			eng, fk := newTestEngine(t)

			g, err := eng.Launch(t.Context(), redisdocker.Spec{
				ClusterMode: tt.cluster, Shards: []redisdocker.ShardSpec{{Replicas: 1}},
			})
			require.NoError(t, err)

			t.Cleanup(g.Close)

			eps := g.Endpoints()
			first, err := g.Primary(0)
			require.NoError(t, err)
			assert.Equal(t, eps[0][0], first)

			require.NoError(t, g.Failover(t.Context(), 0))

			now, err := g.Primary(0)
			require.NoError(t, err)
			assert.Equal(t, eps[0][1], now, "the replica is the primary")
			assert.Equal(t, eps, g.Endpoints(), "node identity is stable")

			if tt.cluster {
				cmds := fk.Cmds("CLUSTER", "FAILOVER")
				require.Len(t, cmds, 1)
				assert.Contains(t, cmds[0].Addr, strconv.Itoa(eps[0][1].Port))

				return
			}

			assert.Len(t, fk.Cmds("REPLICAOF", "NO", "ONE"), 1)
		})
	}
}

func TestFailoverErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr error
		spec    redisdocker.ShardSpec
		shard   int
	}{
		{name: "no_replica", spec: redisdocker.ShardSpec{}, wantErr: redisdocker.ErrNoReplica},
		{name: "bad_shard", spec: redisdocker.ShardSpec{Replicas: 1}, shard: 3, wantErr: redisdocker.ErrShardRange},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			eng, _ := newTestEngine(t)

			g, err := eng.Launch(t.Context(), redisdocker.Spec{Shards: []redisdocker.ShardSpec{tt.spec}})
			require.NoError(t, err)

			t.Cleanup(g.Close)

			require.ErrorIs(t, g.Failover(t.Context(), tt.shard), tt.wantErr)
		})
	}
}

func TestSetReplicasGrowsAndShrinks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		start      int
		target     int
		wantNodes  int
		wantRemove int
		cluster    bool
	}{
		{name: "grow_cluster", cluster: true, start: 0, target: 2, wantNodes: 3},
		{name: "grow_standalone", start: 1, target: 3, wantNodes: 4},
		{name: "shrink_cluster", cluster: true, start: 2, target: 0, wantNodes: 1, wantRemove: 2},
		{name: "shrink_standalone", start: 2, target: 1, wantNodes: 2, wantRemove: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			eng, fk := newTestEngine(t)

			g, err := eng.Launch(t.Context(), redisdocker.Spec{
				ClusterMode: tt.cluster, Shards: []redisdocker.ShardSpec{{Replicas: tt.start}},
			})
			require.NoError(t, err)

			t.Cleanup(g.Close)

			require.NoError(t, g.SetReplicas(t.Context(), 0, tt.target))

			assert.Len(t, g.Endpoints()[0], tt.wantNodes)
			assert.Len(t, fk.Removed(), tt.wantRemove)

			if tt.cluster && tt.wantRemove > 0 {
				assert.NotEmpty(t, fk.Cmds("CLUSTER", "FORGET"))
			}
		})
	}
}

func TestSetReplicasRejectsBadInput(t *testing.T) {
	t.Parallel()

	eng, _ := newTestEngine(t)

	g, err := eng.Launch(t.Context(), redisdocker.Spec{Shards: []redisdocker.ShardSpec{{}}})
	require.NoError(t, err)

	t.Cleanup(g.Close)

	require.ErrorIs(t, g.SetReplicas(t.Context(), 0, 9), redisdocker.ErrSpec)
	require.ErrorIs(t, g.SetReplicas(t.Context(), 5, 1), redisdocker.ErrShardRange)
}
