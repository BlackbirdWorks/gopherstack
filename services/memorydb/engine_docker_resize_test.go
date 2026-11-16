package memorydb_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	memorydbsdk "github.com/aws/aws-sdk-go-v2/service/memorydb"
	memorydbtypes "github.com/aws/aws-sdk-go-v2/service/memorydb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

func createDockerCluster(
	t *testing.T,
	client *memorydbsdk.Client,
	b *memorydb.InMemoryBackend,
	name string,
	replicas int32,
) {
	t.Helper()

	_, err := client.CreateCluster(t.Context(), &memorydbsdk.CreateClusterInput{
		ClusterName: aws.String(name), NodeType: aws.String("db.r6g.large"), ACLName: aws.String("open-access"),
		NumReplicasPerShard: aws.Int32(replicas), TLSEnabled: aws.Bool(false),
	})
	require.NoError(t, err)

	waitAvailable(t, client, b, name)
}

func TestDockerFailoverShard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr  string
		name     string
		shard    string
		replicas int32
		wantCmds int
	}{
		{name: "promotes_replica", replicas: 1, shard: "fo-0001-0000", wantCmds: 1},
		{name: "unknown_shard", replicas: 1, shard: "fo-0001-0009", wantErr: "ShardNotFoundFault"},
		{name: "no_replica", replicas: 0, shard: "fo-0001-0000", wantErr: "InvalidParameterValue"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, fk, b := newDockerFakeClient(t)
			createDockerCluster(t, client, b, "fo", tt.replicas)

			_, err := client.FailoverShard(t.Context(), &memorydbsdk.FailoverShardInput{
				ClusterName: aws.String("fo"), ShardName: aws.String(tt.shard),
			})

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Len(t, fk.Cmds("CLUSTER", "FAILOVER"), tt.wantCmds)
		})
	}
}

func TestDockerUpdateClusterReplicaCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		start     int32
		target    int32
		wantNodes int
		wantStops int
		wantRuns  int
	}{
		{name: "grow", start: 0, target: 2, wantNodes: 3, wantRuns: 3},
		{name: "shrink", start: 2, target: 1, wantNodes: 2, wantStops: 1, wantRuns: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, fk, b := newDockerFakeClient(t)
			createDockerCluster(t, client, b, "rs", tt.start)

			_, err := client.UpdateCluster(t.Context(), &memorydbsdk.UpdateClusterInput{
				ClusterName:          aws.String("rs"),
				ReplicaConfiguration: &memorydbtypes.ReplicaConfigurationRequest{ReplicaCount: tt.target},
			})
			require.NoError(t, err)

			cl := waitAvailable(t, client, b, "rs")
			require.Len(t, cl.Shards, 1)
			assert.Len(t, cl.Shards[0].Nodes, tt.wantNodes)
			assert.Len(t, fk.Specs(), tt.wantRuns, "containers actually started")
			assert.Len(t, fk.Removed(), tt.wantStops, fmt.Sprint(fk.Removed()))
			assert.Equal(t, tt.target, aws.ToInt32(cl.Shards[0].NumberOfNodes)-1)
		})
	}
}
