package elasticache_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/redisdocker/redisdockertest"
	"github.com/blackbirdworks/gopherstack/services/elasticache"
)

func newDockerFakeStack(t *testing.T) (*elasticachesdk.Client, *redisdockertest.Fake, *elasticache.InMemoryBackend) {
	t.Helper()

	b := elasticache.NewInMemoryBackend(elasticache.EngineDocker, "000000000000", "us-east-1", nil)
	fk, eng := redisdockertest.New(t, redisdockertest.Ports(45000, 45100))
	b.UseDockerEngine(eng)
	t.Cleanup(b.Close)

	return newTestStackSeeded(t, b), fk, b
}

type endpointKey struct {
	addr string
	port int32
}

func nodeEndpoints(t *testing.T, rg *types.ReplicationGroup) map[endpointKey]bool {
	t.Helper()

	seen := map[endpointKey]bool{}

	for _, ng := range rg.NodeGroups {
		for _, m := range ng.NodeGroupMembers {
			require.NotNil(t, m.ReadEndpoint)

			key := endpointKey{aws.ToString(m.ReadEndpoint.Address), aws.ToInt32(m.ReadEndpoint.Port)}
			assert.False(t, seen[key], "node endpoint %v repeated", key)
			seen[key] = true
		}
	}

	return seen
}

func TestDockerClusterModeGroupTopology(t *testing.T) {
	t.Parallel()

	client, fk, _ := newDockerFakeStack(t)

	out, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId: aws.String("cm"), ReplicationGroupDescription: aws.String("d"),
		Engine: aws.String("redis"), CacheNodeType: aws.String("cache.t3.micro"),
		NumNodeGroups: aws.Int32(2), ReplicasPerNodeGroup: aws.Int32(1),
		NodeGroupConfiguration: []types.NodeGroupConfiguration{
			{Slots: aws.String("0-4999")}, {Slots: aws.String("5000-16383")},
		},
	})
	require.NoError(t, err)

	rg := out.ReplicationGroup
	assert.Len(t, fk.Specs(), 4, "2 shards x (1 primary + 1 replica)")
	require.NotNil(t, rg.ConfigurationEndpoint)
	assert.Len(t, nodeEndpoints(t, rg), 4)
	require.Len(t, rg.NodeGroups, 2)
	assert.Equal(t, "0-4999", aws.ToString(rg.NodeGroups[0].Slots))
	assert.Equal(t, "5000-16383", aws.ToString(rg.NodeGroups[1].Slots))

	slots := fk.Cmds("CLUSTER", "ADDSLOTSRANGE")
	require.Len(t, slots, 2)
	assert.Equal(t, []any{"CLUSTER", "ADDSLOTSRANGE", "0", "4999"}, slots[0].Args)
	assert.Equal(t, []any{"CLUSTER", "ADDSLOTSRANGE", "5000", "16383"}, slots[1].Args)
	assert.Len(t, fk.Cmds("CLUSTER", "REPLICATE"), 2)

	cfgPort := aws.ToInt32(rg.ConfigurationEndpoint.Port)
	assert.Equal(t, aws.ToInt32(rg.NodeGroups[0].NodeGroupMembers[0].ReadEndpoint.Port), cfgPort)
}

func TestDockerReplicationGroupWithoutClusterMode(t *testing.T) {
	t.Parallel()

	client, fk, _ := newDockerFakeStack(t)

	out, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId: aws.String("rep"), ReplicationGroupDescription: aws.String("d"),
		Engine: aws.String("redis"), CacheNodeType: aws.String("cache.t3.micro"), NumCacheClusters: aws.Int32(3),
	})
	require.NoError(t, err)

	rg := out.ReplicationGroup
	assert.Len(t, fk.Specs(), 3)
	assert.Len(t, fk.Cmds("REPLICAOF"), 2)
	assert.Empty(t, fk.Cmds("CLUSTER", "MEET"))
	assert.Len(t, nodeEndpoints(t, rg), 3)

	ng := rg.NodeGroups[0]
	require.NotNil(t, ng.PrimaryEndpoint)
	require.NotNil(t, ng.ReaderEndpoint)

	var primaryPort, replicaPort int32

	for _, m := range ng.NodeGroupMembers {
		if aws.ToString(m.CurrentRole) == "primary" {
			primaryPort = aws.ToInt32(m.ReadEndpoint.Port)
		} else if replicaPort == 0 {
			replicaPort = aws.ToInt32(m.ReadEndpoint.Port)
		}
	}

	assert.Equal(t, primaryPort, aws.ToInt32(ng.PrimaryEndpoint.Port))
	assert.Equal(t, replicaPort, aws.ToInt32(ng.ReaderEndpoint.Port))
	assert.NotEqual(t, aws.ToInt32(ng.PrimaryEndpoint.Port), aws.ToInt32(ng.ReaderEndpoint.Port))
}

func TestDockerTransitEncryptionServesTLSOnly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		mode types.TransitEncryptionMode
		tls  bool
	}{
		{name: "required", mode: types.TransitEncryptionModeRequired, tls: true},
		{name: "default", tls: true},
		{name: "preferred keeps plaintext", mode: types.TransitEncryptionModePreferred},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, fk, b := newDockerFakeStack(t)

			_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
				ReplicationGroupId: aws.String("tls"), ReplicationGroupDescription: aws.String("d"),
				Engine: aws.String("redis"), CacheNodeType: aws.String("cache.t3.micro"),
				TransitEncryptionEnabled: aws.Bool(true), TransitEncryptionMode: tc.mode,
				AuthToken: aws.String("a-long-enough-token-1234"),
			})
			require.NoError(t, err)

			cmd := strings.Join(fk.Specs()[0].Cmd, " ")
			assert.Equal(t, tc.tls, strings.Contains(cmd, "--tls-port"))
			assert.Equal(t, tc.tls, strings.Contains(cmd, "--port 0"))
			assert.NotEmpty(t, b.DockerCAPEM())
		})
	}
}

func TestDockerACLFromUserGroupsAndAuthToken(t *testing.T) {
	t.Parallel()

	client, fk, _ := newDockerFakeStack(t)
	ctx := t.Context()

	for _, u := range []struct{ id, name, access string }{
		{"u-default", "default", "off ~* -@all"},
		{"u-app", "app", "on ~app:* +@all"},
	} {
		_, err := client.CreateUser(ctx, &elasticachesdk.CreateUserInput{
			UserId: aws.String(u.id), UserName: aws.String(u.name), Engine: aws.String("redis"),
			AccessString: aws.String(u.access), AuthenticationMode: &types.AuthenticationMode{
				Type: types.InputAuthenticationTypePassword, Passwords: []string{"passwordpassword1"},
			},
		})
		require.NoError(t, err)
	}

	_, err := client.CreateUserGroup(ctx, &elasticachesdk.CreateUserGroupInput{
		UserGroupId: aws.String("ug"), Engine: aws.String("redis"), UserIds: []string{"u-default", "u-app"},
	})
	require.NoError(t, err)

	_, err = client.CreateReplicationGroup(ctx, &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId: aws.String("acl"), ReplicationGroupDescription: aws.String("d"),
		Engine: aws.String("redis"), CacheNodeType: aws.String("cache.t3.micro"), NumCacheClusters: aws.Int32(2),
		UserGroupIds: []string{"ug"},
	})
	require.NoError(t, err)

	set := fk.Cmds("ACL", "SETUSER", "app")
	require.Len(t, set, 2, "primary and replica")
	assert.Equal(t, []any{"ACL", "SETUSER", "app", "reset", ">passwordpassword1", "on", "~app:*", "+@all"}, set[0].Args)

	def := fk.Cmds("ACL", "SETUSER", "default")
	require.NotEmpty(t, def)
	assert.Contains(t, def[0].Args, ">passwordpassword1", "the listed default user replaces the open one")

	_, err = client.ModifyUser(ctx, &elasticachesdk.ModifyUserInput{
		UserId: aws.String("u-app"), AccessString: aws.String("on ~other:* +get"),
	})
	require.NoError(t, err)

	set = fk.Cmds("ACL", "SETUSER", "app")
	require.Len(t, set, 4)
	assert.Equal(t, []any{"ACL", "SETUSER", "app", "reset", ">passwordpassword1", "on", "~other:*", "+get"}, set[3].Args)

	_, err = client.ModifyUserGroup(ctx, &elasticachesdk.ModifyUserGroupInput{
		UserGroupId: aws.String("ug"), UserIdsToRemove: []string{"u-app"},
	})
	require.NoError(t, err)
	assert.Len(t, fk.Cmds("ACL", "DELUSER", "app"), 2)
}

func TestDockerAuthTokenProtectsDefaultUser(t *testing.T) {
	t.Parallel()

	client, fk, _ := newDockerFakeStack(t)

	_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId: aws.String("tok"), ReplicationGroupDescription: aws.String("d"),
		Engine: aws.String("redis"), CacheNodeType: aws.String("cache.t3.micro"),
		TransitEncryptionEnabled: aws.Bool(true), AuthToken: aws.String("a-long-enough-token-1234"),
	})
	require.NoError(t, err)

	def := fk.Cmds("ACL", "SETUSER", "default")
	require.Len(t, def, 1)
	assert.Equal(t, []any{
		"ACL", "SETUSER", "default", "reset", "on", ">a-long-enough-token-1234", "~*", "&*", "+@all",
	}, def[0].Args)

	_, err = client.ModifyReplicationGroup(t.Context(), &elasticachesdk.ModifyReplicationGroupInput{
		ReplicationGroupId: aws.String("tok"), AuthToken: aws.String("another-long-token-5678"),
		AuthTokenUpdateStrategy: types.AuthTokenUpdateStrategyTypeSet, ApplyImmediately: aws.Bool(true),
	})
	require.NoError(t, err)

	def = fk.Cmds("ACL", "SETUSER", "default")
	require.Len(t, def, 2)
	assert.Contains(t, def[1].Args, ">another-long-token-5678")
}

func TestDockerSnapshotRestoreSeedsEachShard(t *testing.T) {
	t.Parallel()

	client, fk, _ := newDockerFakeStack(t)
	ctx := t.Context()

	_, err := client.CreateReplicationGroup(ctx, &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId: aws.String("src"), ReplicationGroupDescription: aws.String("d"),
		Engine: aws.String("redis"), CacheNodeType: aws.String("cache.t3.micro"), NumNodeGroups: aws.Int32(2),
	})
	require.NoError(t, err)

	_, err = client.CreateSnapshot(ctx, &elasticachesdk.CreateSnapshotInput{
		ReplicationGroupId: aws.String("src"), SnapshotName: aws.String("snap"),
	})
	require.NoError(t, err)

	before := len(fk.Specs())

	_, err = client.CreateReplicationGroup(ctx, &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId: aws.String("dst"), ReplicationGroupDescription: aws.String("d"),
		SnapshotName: aws.String("snap"),
	})
	require.NoError(t, err)

	seeded := 0

	for _, sp := range fk.Specs()[before:] {
		if strings.Contains(strings.Join(sp.Env, " "), "GS_SEED=") {
			seeded++
		}
	}

	assert.Equal(t, 2, seeded, "each restored node group primary starts from its own RDB")
}

func TestDockerDeleteReplicationGroupRemovesContainers(t *testing.T) {
	t.Parallel()

	client, fk, b := newDockerFakeStack(t)

	_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId: aws.String("del"), ReplicationGroupDescription: aws.String("d"),
		Engine: aws.String("redis"), CacheNodeType: aws.String("cache.t3.micro"), NumCacheClusters: aws.Int32(2),
	})
	require.NoError(t, err)

	_, err = client.DeleteReplicationGroup(t.Context(), &elasticachesdk.DeleteReplicationGroupInput{
		ReplicationGroupId: aws.String("del"),
	})
	require.NoError(t, err)

	b.Close()
	assert.ElementsMatch(t, []string{"c1", "c2"}, fk.Removed())
}

func TestDockerStandaloneClusterGetsOwnContainer(t *testing.T) {
	t.Parallel()

	client, fk, _ := newDockerFakeStack(t)

	out, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId: aws.String("solo"), Engine: aws.String("redis"), CacheNodeType: aws.String("cache.t3.micro"),
		NumCacheNodes: aws.Int32(1),
	})
	require.NoError(t, err)

	require.Len(t, fk.Specs(), 1)
	assert.Contains(t, strings.Join(fk.Specs()[0].Cmd, " "), "--port "+strconv.Itoa(int(portOf(out.CacheCluster))))
}

func portOf(c *types.CacheCluster) int32 {
	if c.ConfigurationEndpoint != nil {
		return aws.ToInt32(c.ConfigurationEndpoint.Port)
	}

	return aws.ToInt32(c.CacheNodes[0].Endpoint.Port)
}
