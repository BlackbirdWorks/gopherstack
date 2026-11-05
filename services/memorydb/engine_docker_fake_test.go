package memorydb_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	memorydbsdk "github.com/aws/aws-sdk-go-v2/service/memorydb"
	memorydbtypes "github.com/aws/aws-sdk-go-v2/service/memorydb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/redisdocker/redisdockertest"
	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

func newDockerFakeClient(t *testing.T) (*memorydbsdk.Client, *redisdockertest.Fake, *memorydb.InMemoryBackend) {
	t.Helper()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	fk, eng := redisdockertest.New(t, redisdockertest.Ports(43000, 43100))
	b.UseDockerEngine(eng)
	t.Cleanup(b.Close)

	h := memorydb.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	return newMemorydbSDKClient(t, h), fk, b
}

func waitAvailable(t *testing.T, c *memorydbsdk.Client, name string) memorydbtypes.Cluster {
	t.Helper()

	return waitAvailableFor(t, c, name, time.Minute)
}

func waitAvailableFor(t *testing.T, c *memorydbsdk.Client, name string, limit time.Duration) memorydbtypes.Cluster {
	t.Helper()

	var got memorydbtypes.Cluster

	require.Eventually(t, func() bool {
		out, err := c.DescribeClusters(t.Context(), &memorydbsdk.DescribeClustersInput{
			ClusterName: aws.String(name), ShowShardDetails: aws.Bool(true),
		})
		if err != nil || len(out.Clusters) != 1 {
			return false
		}

		got = out.Clusters[0]

		return aws.ToString(got.Status) == "available"
	}, limit, 20*time.Millisecond)

	return got
}

func TestDockerClusterTopologyAndEndpoints(t *testing.T) {
	t.Parallel()

	client, fk, _ := newDockerFakeClient(t)

	_, err := client.CreateCluster(t.Context(), &memorydbsdk.CreateClusterInput{
		ClusterName: aws.String("topo"), NodeType: aws.String("db.r6g.large"), ACLName: aws.String("open-access"),
		NumShards: aws.Int32(2), NumReplicasPerShard: aws.Int32(1), TLSEnabled: aws.Bool(false),
	})
	require.NoError(t, err)

	cl := waitAvailable(t, client, "topo")

	assert.Len(t, fk.Specs(), 4, "2 shards x (1 primary + 1 replica)")
	require.Len(t, cl.Shards, 2)

	seen := map[string]bool{}

	for _, sh := range cl.Shards {
		require.Len(t, sh.Nodes, 2)

		for _, n := range sh.Nodes {
			key := aws.ToString(n.Endpoint.Address) + ":" + string(rune(n.Endpoint.Port))
			assert.False(t, seen[key], "node endpoint repeated")
			seen[key] = true
		}
	}

	assert.Equal(t, "0-8191", aws.ToString(cl.Shards[0].Slots))
	assert.Equal(t, "8192-16383", aws.ToString(cl.Shards[1].Slots))
	assert.Equal(t, aws.ToString(cl.Shards[0].Nodes[0].Endpoint.Address), aws.ToString(cl.ClusterEndpoint.Address))
	assert.Equal(t, cl.Shards[0].Nodes[0].Endpoint.Port, cl.ClusterEndpoint.Port)

	assert.Len(t, fk.Cmds("CLUSTER", "ADDSLOTSRANGE"), 2)
	assert.Len(t, fk.Cmds("CLUSTER", "REPLICATE"), 2)
}

func TestDockerClusterReportsCreatingUntilLaunched(t *testing.T) {
	t.Parallel()

	client, _, b := newDockerFakeClient(t)

	_, err := client.CreateCluster(t.Context(), &memorydbsdk.CreateClusterInput{
		ClusterName: aws.String("early"), NodeType: aws.String("db.r6g.large"), ACLName: aws.String("open-access"),
		TLSEnabled: aws.Bool(true),
	})
	require.NoError(t, err)

	waitAvailable(t, client, "early")
	assert.NotEmpty(t, b.DockerCAPEM(), "TLS clusters hand clients a CA")
}

func TestDockerACLSyncOnUserAndACLChanges(t *testing.T) {
	t.Parallel()

	client, fk, _ := newDockerFakeClient(t)
	ctx := t.Context()

	_, err := client.CreateUser(ctx, &memorydbsdk.CreateUserInput{
		UserName: aws.String("app"), AccessString: aws.String("on ~app:* +@all"),
		AuthenticationMode: &memorydbtypes.AuthenticationMode{
			Type: memorydbtypes.InputAuthenticationTypePassword, Passwords: []string{"passwordpassword1"},
		},
	})
	require.NoError(t, err)

	_, err = client.CreateACL(ctx, &memorydbsdk.CreateACLInput{ACLName: aws.String("app-acl"), UserNames: []string{"app"}})
	require.NoError(t, err)

	_, err = client.CreateCluster(ctx, &memorydbsdk.CreateClusterInput{
		ClusterName: aws.String("acl"), NodeType: aws.String("db.r6g.large"), ACLName: aws.String("app-acl"),
		NumReplicasPerShard: aws.Int32(1), TLSEnabled: aws.Bool(false),
	})
	require.NoError(t, err)

	waitAvailable(t, client, "acl")

	set := fk.Cmds("ACL", "SETUSER", "app")
	require.Len(t, set, 2, "synced to the primary and the replica before the cluster is available")
	assert.Equal(t, []any{"ACL", "SETUSER", "app", "reset", ">passwordpassword1", "on", "~app:*", "+@all"}, set[0].Args)
	assert.Len(t, fk.Cmds("ACL", "SETUSER", "default"), 2)

	_, err = client.UpdateUser(ctx, &memorydbsdk.UpdateUserInput{
		UserName: aws.String("app"), AccessString: aws.String("on ~other:* +get"),
	})
	require.NoError(t, err)

	set = fk.Cmds("ACL", "SETUSER", "app")
	require.Len(t, set, 4)
	assert.Equal(t, []any{"ACL", "SETUSER", "app", "reset", ">passwordpassword1", "on", "~other:*", "+get"}, set[3].Args)

	_, err = client.UpdateACL(ctx, &memorydbsdk.UpdateACLInput{
		ACLName: aws.String("app-acl"), UserNamesToRemove: []string{"app"},
	})
	require.NoError(t, err)
	assert.Len(t, fk.Cmds("ACL", "DELUSER", "app"), 2)
}

func TestDockerSnapshotRoundTripKeepsShards(t *testing.T) {
	t.Parallel()

	client, fk, _ := newDockerFakeClient(t)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &memorydbsdk.CreateClusterInput{
		ClusterName: aws.String("src"), NodeType: aws.String("db.r6g.large"), ACLName: aws.String("open-access"),
		NumShards: aws.Int32(2), TLSEnabled: aws.Bool(false),
	})
	require.NoError(t, err)
	waitAvailable(t, client, "src")

	_, err = client.CreateSnapshot(ctx, &memorydbsdk.CreateSnapshotInput{
		ClusterName: aws.String("src"), SnapshotName: aws.String("snap"),
	})
	require.NoError(t, err)

	_, err = client.CreateCluster(ctx, &memorydbsdk.CreateClusterInput{
		ClusterName: aws.String("copy"), NodeType: aws.String("db.r6g.large"), ACLName: aws.String("open-access"),
		SnapshotName: aws.String("snap"), TLSEnabled: aws.Bool(false),
	})
	require.NoError(t, err)
	waitAvailable(t, client, "copy")

	seeded := 0

	for _, sp := range fk.Specs() {
		for _, e := range sp.Env {
			if len(e) > 8 && e[:8] == "GS_SEED=" {
				seeded++
			}
		}
	}

	assert.Equal(t, 2, seeded, "each restored shard primary starts from its own RDB")
}

func TestDockerSnapshotShapeMismatchRejected(t *testing.T) {
	t.Parallel()

	client, _, _ := newDockerFakeClient(t)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &memorydbsdk.CreateClusterInput{
		ClusterName: aws.String("src"), NodeType: aws.String("db.r6g.large"), ACLName: aws.String("open-access"),
		NumShards: aws.Int32(2), TLSEnabled: aws.Bool(false),
	})
	require.NoError(t, err)
	waitAvailable(t, client, "src")

	_, err = client.CreateSnapshot(ctx, &memorydbsdk.CreateSnapshotInput{
		ClusterName: aws.String("src"), SnapshotName: aws.String("snap"),
	})
	require.NoError(t, err)

	_, err = client.CreateCluster(ctx, &memorydbsdk.CreateClusterInput{
		ClusterName: aws.String("bad"), NodeType: aws.String("db.r6g.large"), ACLName: aws.String("open-access"),
		SnapshotName: aws.String("snap"), NumShards: aws.Int32(3),
	})
	require.Error(t, err)
}

func TestDockerDeleteRemovesContainers(t *testing.T) {
	t.Parallel()

	client, fk, b := newDockerFakeClient(t)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &memorydbsdk.CreateClusterInput{
		ClusterName: aws.String("gone"), NodeType: aws.String("db.r6g.large"), ACLName: aws.String("open-access"),
		NumReplicasPerShard: aws.Int32(1), TLSEnabled: aws.Bool(false),
	})
	require.NoError(t, err)
	waitAvailable(t, client, "gone")

	_, err = client.DeleteCluster(ctx, &memorydbsdk.DeleteClusterInput{ClusterName: aws.String("gone")})
	require.NoError(t, err)

	b.Close()
	assert.ElementsMatch(t, []string{"c1", "c2"}, fk.Removed())
}
