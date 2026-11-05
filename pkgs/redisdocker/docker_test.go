package redisdocker_test

import (
	"crypto/tls"
	"crypto/x509"
	"os/exec"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/redisdocker"
)

func dockerEngine(t *testing.T) *redisdocker.Engine {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	if err = rt.Ping(t.Context()); err != nil {
		t.Skipf("container runtime unreachable: %v", err)
	}

	ca, err := devtls.NewCA()
	require.NoError(t, err)

	alloc, err := portalloc.New(42000, 42100)
	require.NoError(t, err)

	return redisdocker.New(redisdocker.Config{
		Runtime: rt, Ports: alloc, CA: ca, Label: "redisdocker-test", TempDir: t.TempDir(),
		StartTimeout: 5 * time.Minute,
	})
}

func TestDockerClusterTLSACLSnapshot(t *testing.T) {
	t.Parallel()

	eng := dockerEngine(t)

	g, err := eng.Launch(t.Context(), redisdocker.Spec{
		Version: "7.1", ClusterMode: true, TLS: true,
		Shards: []redisdocker.ShardSpec{{Replicas: 1}, {Replicas: 1}},
	})
	require.NoError(t, err)

	t.Cleanup(g.Close)

	require.NoError(t, g.ApplyACL(t.Context(), redisdocker.ACLPolicy{Users: []redisdocker.ACLUser{
		{Name: "app", AccessString: "on ~app:* +@all", Passwords: []string{"app-password-1"}},
	}}))

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(eng.CAPEM()))
	eps := g.Endpoints()
	cc := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{eps[0][0].Addr()}, Username: "app", Password: "app-password-1", ReadOnly: true,
		TLSConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
	})

	t.Cleanup(func() { _ = cc.Close() })

	for _, k := range []string{"app:a", "app:b", "app:c", "app:d", "app:e", "app:f"} {
		require.NoError(t, cc.Set(t.Context(), k, "v-"+k, 0).Err(), k)
	}

	got, err := cc.Get(t.Context(), "app:a").Result()
	require.NoError(t, err)
	assert.Equal(t, "v-app:a", got)

	err = cc.Set(t.Context(), "other:x", "1", 0).Err()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NOPERM")

	var shards []redis.ClusterShard

	require.Eventually(t, func() bool {
		var serr error

		shards, serr = cc.ClusterShards(t.Context()).Result()

		return serr == nil && len(shards) == 2 && len(shards[0].Nodes) == 2 && len(shards[1].Nodes) == 2
	}, time.Minute, 500*time.Millisecond, "replicas listed in CLUSTER SHARDS")

	covered := 0

	for _, sh := range shards {
		for _, r := range sh.Slots {
			covered += int(r.End - r.Start + 1)
		}
	}

	assert.Equal(t, redisdocker.TotalSlots, covered)

	snap, err := g.Snapshot(t.Context())
	require.NoError(t, err)
	require.Len(t, snap, 2)
	assert.True(t, redisdocker.IsRDB(snap[0]))
}
