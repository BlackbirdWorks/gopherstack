package memorydb_test

import (
	"crypto/tls"
	"crypto/x509"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	memorydbsdk "github.com/aws/aws-sdk-go-v2/service/memorydb"
	memorydbtypes "github.com/aws/aws-sdk-go-v2/service/memorydb/types"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/redisdocker"
	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

func newRealDockerClient(t *testing.T) (*memorydbsdk.Client, *memorydb.InMemoryBackend) {
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

	alloc, err := portalloc.New(44000, 44100)
	require.NoError(t, err)

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	b.SetEngine(memorydb.EngineDocker, alloc)
	require.NoError(t, b.EnableDockerEngine(rt, redisdocker.Config{StartTimeout: 5 * time.Minute}))
	t.Cleanup(func() {
		b.Close()
		_ = rt.Close()
	})

	h := memorydb.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	return newMemorydbSDKClient(t, h), b
}

func clusterClientFor(
	t *testing.T, cl memorydbtypes.Cluster, b *memorydb.InMemoryBackend, user, pass string,
) *redis.ClusterClient {
	t.Helper()

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(b.DockerCAPEM()))

	cc := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    []string{aws.ToString(cl.ClusterEndpoint.Address) + ":" + itoa(cl.ClusterEndpoint.Port)},
		Username: user, Password: pass,
		TLSConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
	})
	t.Cleanup(func() { _ = cc.Close() })

	return cc
}

func itoa(n int32) string { return strconv.Itoa(int(n)) }

func TestDockerRealClusterTLSACLAndSnapshotRestore(t *testing.T) {
	t.Parallel()

	client, b := newRealDockerClient(t)
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
		ClusterName: aws.String("real"), NodeType: aws.String("db.r6g.large"), ACLName: aws.String("app-acl"),
		NumShards: aws.Int32(2), NumReplicasPerShard: aws.Int32(1),
	})
	require.NoError(t, err)

	cl := waitAvailableFor(t, client, "real", 5*time.Minute)
	assert.True(t, cl.TLSEnabled != nil && *cl.TLSEnabled)

	cc := clusterClientFor(t, cl, b, "app", "passwordpassword1")

	keys := []string{"app:a", "app:b", "app:c", "app:d", "app:e", "app:f", "app:g", "app:h"}
	for _, k := range keys {
		require.NoError(t, cc.Set(ctx, k, "v-"+k, 0).Err(), k)
	}

	got, err := cc.Get(ctx, "app:a").Result()
	require.NoError(t, err)
	assert.Equal(t, "v-app:a", got)

	denied := cc.Set(ctx, "other:key", "x", 0).Err()
	require.Error(t, denied)
	assert.Contains(t, denied.Error(), "NOPERM")

	assertSlotsAndReplication(t, cc, cl, keys)

	_, err = client.CreateSnapshot(ctx, &memorydbsdk.CreateSnapshotInput{
		ClusterName: aws.String("real"), SnapshotName: aws.String("real-snap"),
	})
	require.NoError(t, err)

	_, err = client.CreateCluster(ctx, &memorydbsdk.CreateClusterInput{
		ClusterName: aws.String("restored"), NodeType: aws.String("db.r6g.large"), ACLName: aws.String("app-acl"),
		SnapshotName: aws.String("real-snap"), NumReplicasPerShard: aws.Int32(0),
	})
	require.NoError(t, err)

	restored := waitAvailableFor(t, client, "restored", 5*time.Minute)
	rc := clusterClientFor(t, restored, b, "app", "passwordpassword1")

	for _, k := range keys {
		v, gerr := rc.Get(ctx, k).Result()
		require.NoError(t, gerr, k)
		assert.Equal(t, "v-"+k, v)
	}
}

func assertSlotsAndReplication(t *testing.T, cc *redis.ClusterClient, cl memorydbtypes.Cluster, keys []string) {
	t.Helper()

	var slots []redis.ClusterSlot

	require.Eventually(t, func() bool {
		var err error

		slots, err = cc.ClusterSlots(t.Context()).Result()

		return err == nil && len(slots) == 2 && len(slots[0].Nodes) == 2 && len(slots[1].Nodes) == 2
	}, time.Minute, 500*time.Millisecond)

	covered := 0
	for _, s := range slots {
		covered += s.End - s.Start + 1
	}

	assert.Equal(t, redisdocker.TotalSlots, covered)

	require.Eventually(t, func() bool {
		replicas := 0

		for _, k := range keys {
			if v, err := cc.Get(t.Context(), k).Result(); err == nil && v != "" {
				replicas++
			}
		}

		return replicas == len(keys)
	}, time.Minute, 200*time.Millisecond)

	for _, sh := range cl.Shards {
		require.Len(t, sh.Nodes, 2)
	}
}
