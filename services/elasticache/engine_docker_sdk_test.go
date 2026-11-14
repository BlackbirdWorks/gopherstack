package elasticache_test

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc/portalloctest"
	"github.com/blackbirdworks/gopherstack/pkgs/redisdocker"
	"github.com/blackbirdworks/gopherstack/services/elasticache"
)

func newRealDockerStack(t *testing.T) (*elasticachesdk.Client, *elasticache.InMemoryBackend) {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	if err = rt.Ping(t.Context()); err != nil {
		t.Skipf("container runtime unreachable: %v", err)
	}

	alloc := portalloctest.New(t, 100)

	b := elasticache.NewInMemoryBackend(elasticache.EngineDocker, "000000000000", "us-east-1", alloc)
	require.NoError(t, b.EnableDockerEngine(rt, redisdocker.Config{StartTimeout: 5 * time.Minute}))
	t.Cleanup(func() {
		b.Close()
		_ = rt.Close()
	})

	return newTestStackSeeded(t, b), b
}

func memberAddr(m types.NodeGroupMember) string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(int(aws.ToInt32(m.ReadEndpoint.Port))))
}

func TestDockerRealClusterModeGroupAcrossSlotsAndReplicas(t *testing.T) {
	t.Parallel()

	client, _ := newRealDockerStack(t)
	ctx := t.Context()

	out, err := client.CreateReplicationGroup(ctx, &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId: aws.String("cm"), ReplicationGroupDescription: aws.String("d"),
		Engine: aws.String("redis"), CacheNodeType: aws.String("cache.t3.micro"),
		NumNodeGroups: aws.Int32(2), ReplicasPerNodeGroup: aws.Int32(1),
	})
	require.NoError(t, err)

	rg := out.ReplicationGroup
	require.Len(t, nodeEndpoints(t, rg), 4)

	cc := redis.NewClusterClient(
		&redis.ClusterOptions{Addrs: []string{memberAddr(rg.NodeGroups[0].NodeGroupMembers[0])}},
	)
	t.Cleanup(func() { _ = cc.Close() })

	keys := make([]string, 0, 40)
	for i := range 40 {
		k := "key-" + strconv.Itoa(i)
		keys = append(keys, k)
		require.NoError(t, cc.Set(ctx, k, "v"+k, 0).Err(), k)
	}

	for _, k := range keys {
		got, gerr := cc.Get(ctx, k).Result()
		require.NoError(t, gerr, k)
		assert.Equal(t, "v"+k, got)
	}

	shards, err := cc.ClusterShards(ctx).Result()
	require.NoError(t, err)
	require.Len(t, shards, 2, "both shards own slots")

	covered := 0

	for _, sh := range shards {
		for _, r := range sh.Slots {
			covered += int(r.End - r.Start + 1)
		}
	}

	assert.Equal(t, redisdocker.TotalSlots, covered)

	for _, ng := range rg.NodeGroups {
		var replica types.NodeGroupMember

		for _, m := range ng.NodeGroupMembers {
			if aws.ToString(m.CurrentRole) == "replica" {
				replica = m
			}
		}

		rc := redis.NewClient(&redis.Options{Addr: memberAddr(replica)})
		t.Cleanup(func() { _ = rc.Close() })

		require.Eventually(t, func() bool {
			n, derr := rc.DBSize(ctx).Result()

			return derr == nil && n > 0
		}, time.Minute, 200*time.Millisecond, "writes to the primary reach the replica")
	}
}

func TestDockerRealTLSAuthTokenACLAndSnapshotRestore(t *testing.T) {
	t.Parallel()

	client, b := newRealDockerStack(t)
	ctx := t.Context()

	mkUser := func(id, name, access, pw string) {
		_, err := client.CreateUser(ctx, &elasticachesdk.CreateUserInput{
			UserId: aws.String(id), UserName: aws.String(name), Engine: aws.String("redis"),
			AccessString: aws.String(access), AuthenticationMode: &types.AuthenticationMode{
				Type: types.InputAuthenticationTypePassword, Passwords: []string{pw},
			},
		})
		require.NoError(t, err)
	}

	mkUser("u-default", "default", "off ~* -@all", "unused-password-123")
	mkUser("u-app", "app", "on ~app:* +@all", "app-password-12345")

	_, err := client.CreateUserGroup(ctx, &elasticachesdk.CreateUserGroupInput{
		UserGroupId: aws.String("ug"), Engine: aws.String("redis"), UserIds: []string{"u-default", "u-app"},
	})
	require.NoError(t, err)

	out, err := client.CreateReplicationGroup(ctx, &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId: aws.String("sec"), ReplicationGroupDescription: aws.String("d"),
		Engine: aws.String("redis"), CacheNodeType: aws.String("cache.t3.micro"), NumCacheClusters: aws.Int32(2),
		TransitEncryptionEnabled: aws.Bool(true), UserGroupIds: []string{"ug"},
	})
	require.NoError(t, err)

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(b.DockerCAPEM()))

	tlsCfg := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	primary := out.ReplicationGroup.NodeGroups[0].PrimaryEndpoint
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(int(aws.ToInt32(primary.Port))))

	app := redis.NewClient(
		&redis.Options{Addr: addr, Username: "app", Password: "app-password-12345", TLSConfig: tlsCfg},
	)
	t.Cleanup(func() { _ = app.Close() })

	require.NoError(t, app.Set(ctx, "app:k", "v", 0).Err())

	denied := app.Set(ctx, "other", "v", 0).Err()
	require.Error(t, denied)
	assert.Contains(t, denied.Error(), "NOPERM")

	plain := redis.NewClient(&redis.Options{Addr: addr, Username: "app", Password: "app-password-12345"})
	t.Cleanup(func() { _ = plain.Close() })
	require.Error(t, plain.Ping(ctx).Err(), "plaintext is refused when transit encryption is on")

	_, err = client.CreateSnapshot(ctx, &elasticachesdk.CreateSnapshotInput{
		ReplicationGroupId: aws.String("sec"), SnapshotName: aws.String("sec-snap"),
	})
	require.NoError(t, err)

	restored, err := client.CreateReplicationGroup(ctx, &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId: aws.String("sec2"), ReplicationGroupDescription: aws.String("d"),
		SnapshotName: aws.String("sec-snap"), TransitEncryptionEnabled: aws.Bool(true), UserGroupIds: []string{"ug"},
	})
	require.NoError(t, err)

	raddr := net.JoinHostPort("127.0.0.1",
		strconv.Itoa(int(aws.ToInt32(restored.ReplicationGroup.NodeGroups[0].PrimaryEndpoint.Port))))
	rc := redis.NewClient(
		&redis.Options{Addr: raddr, Username: "app", Password: "app-password-12345", TLSConfig: tlsCfg},
	)
	t.Cleanup(func() { _ = rc.Close() })

	got, err := rc.Get(ctx, "app:k").Result()
	require.NoError(t, err)
	assert.Equal(t, "v", got)
}
