package elasticache_test

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticache"
)

type snapRESP struct {
	conn net.Conn
	r    *bufio.Reader
}

func dialSnapRESP(t *testing.T, addr string) *snapRESP {
	t.Helper()

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return &snapRESP{conn: conn, r: bufio.NewReader(conn)}
}

func (c *snapRESP) do(t *testing.T, args ...string) string {
	t.Helper()

	var sb strings.Builder
	fmt.Fprintf(&sb, "*%d\r\n", len(args))

	for _, a := range args {
		fmt.Fprintf(&sb, "$%d\r\n%s\r\n", len(a), a)
	}

	require.NoError(t, c.conn.SetDeadline(time.Now().Add(2*time.Second)))
	_, err := c.conn.Write([]byte(sb.String()))
	require.NoError(t, err)

	line, err := c.r.ReadString('\n')
	require.NoError(t, err)

	line = strings.TrimRight(line, "\r\n")
	if strings.HasPrefix(line, "$") && line != "$-1" {
		n, convErr := strconv.Atoi(line[1:])
		require.NoError(t, convErr)

		buf := make([]byte, n+2)
		_, err = c.r.Read(buf)
		require.NoError(t, err)

		return string(buf[:n])
	}

	return line
}

func seedKeyspace(t *testing.T, c *snapRESP) {
	t.Helper()

	cmds := [][]string{
		{"SET", "str", "hello"},
		{"SET", "ttl-str", "expiring"},
		{"EXPIRE", "ttl-str", "600"},
		{"HSET", "hash", "f1", "v1", "f2", "v2"},
		{"RPUSH", "list", "a", "b", "c"},
		{"SADD", "set", "m1", "m2"},
		{"ZADD", "zset", "1.5", "one", "2.5", "two"},
		{"XADD", "stream", "1-1", "field", "val"},
	}
	for _, cmd := range cmds {
		c.do(t, cmd...)
	}
}

func assertKeyspace(t *testing.T, c *snapRESP) {
	t.Helper()

	assert.Equal(t, "hello", c.do(t, "GET", "str"))
	assert.Equal(t, "v2", c.do(t, "HGET", "hash", "f2"))
	assert.Equal(t, "c", c.do(t, "LINDEX", "list", "2"))
	assert.Equal(t, ":1", c.do(t, "SISMEMBER", "set", "m2"))
	assert.Equal(t, "2.5", c.do(t, "ZSCORE", "zset", "two"))
	assert.Equal(t, ":1", c.do(t, "XLEN", "stream"))
	assert.Equal(t, ":-1", c.do(t, "TTL", "str"))

	assert.Equal(t, "expiring", c.do(t, "GET", "ttl-str"))

	ttl, err := strconv.Atoi(strings.TrimPrefix(c.do(t, "TTL", "ttl-str"), ":"))
	require.NoError(t, err)
	assert.Positive(t, ttl)
	assert.LessOrEqual(t, ttl, 600)
}

func clusterAddr(t *testing.T, b *elasticache.InMemoryBackend, id string) string {
	t.Helper()

	p, err := b.DescribeClusters(t.Context(), id, "", 0, false)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	require.NotEmpty(t, p.Data[0].ConnectAddress)

	return p.Data[0].ConnectAddress
}

func groupAddr(t *testing.T, b *elasticache.InMemoryBackend, rgID string) string {
	t.Helper()

	p, err := b.DescribeClusters(t.Context(), "", "", 0, false)
	require.NoError(t, err)

	for _, c := range p.Data {
		if c.ReplicationGroupID == rgID && c.ConnectAddress != "" {
			return c.ConnectAddress
		}
	}

	require.Failf(t, "no member", "replication group %s has no connectable member", rgID)

	return ""
}

func newSnapStack(t *testing.T) (*elasticache.InMemoryBackend, *elasticachesdk.Client) {
	t.Helper()

	b := elasticache.NewInMemoryBackend(elasticache.EngineEmbedded, "000000000000", "us-east-1", nil)
	t.Cleanup(b.Reset)

	return b, newTestStackSeeded(t, b)
}

func TestSnapshotData_ClusterRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		take func(t *testing.T, c *elasticachesdk.Client, src string) string
		name string
	}{
		{
			name: "create_snapshot",
			take: func(t *testing.T, c *elasticachesdk.Client, src string) string {
				t.Helper()
				_, err := c.CreateSnapshot(t.Context(), &elasticachesdk.CreateSnapshotInput{
					CacheClusterId: aws.String(src), SnapshotName: aws.String("snap-a"),
				})
				require.NoError(t, err)

				return "snap-a"
			},
		},
		{
			name: "copy_snapshot",
			take: func(t *testing.T, c *elasticachesdk.Client, src string) string {
				t.Helper()
				_, err := c.CreateSnapshot(t.Context(), &elasticachesdk.CreateSnapshotInput{
					CacheClusterId: aws.String(src), SnapshotName: aws.String("snap-orig"),
				})
				require.NoError(t, err)
				_, err = c.CopySnapshot(t.Context(), &elasticachesdk.CopySnapshotInput{
					SourceSnapshotName: aws.String("snap-orig"), TargetSnapshotName: aws.String("snap-copy"),
				})
				require.NoError(t, err)
				_, err = c.DeleteSnapshot(t.Context(), &elasticachesdk.DeleteSnapshotInput{
					SnapshotName: aws.String("snap-orig"),
				})
				require.NoError(t, err)

				return "snap-copy"
			},
		},
		{
			name: "final_snapshot_on_delete",
			take: func(t *testing.T, c *elasticachesdk.Client, src string) string {
				t.Helper()
				_, err := c.DeleteCacheCluster(t.Context(), &elasticachesdk.DeleteCacheClusterInput{
					CacheClusterId: aws.String(src), FinalSnapshotIdentifier: aws.String("snap-final"),
				})
				require.NoError(t, err)

				return "snap-final"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, client := newSnapStack(t)

			_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
				CacheClusterId: aws.String("src"), Engine: aws.String("redis"),
			})
			require.NoError(t, err)

			seedKeyspace(t, dialSnapRESP(t, clusterAddr(t, b, "src")))

			snap := tt.take(t, client, "src")

			_, err = client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
				CacheClusterId: aws.String("restored"), SnapshotName: aws.String(snap),
			})
			require.NoError(t, err)

			assertKeyspace(t, dialSnapRESP(t, clusterAddr(t, b, "restored")))
		})
	}
}

func TestSnapshotData_ReplicationGroupRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		restore bool
	}{
		{name: "restored_from_snapshot", restore: true},
		{name: "fresh_group_is_empty", restore: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, client := newSnapStack(t)

			_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
				ReplicationGroupId:          aws.String("rg-src"),
				ReplicationGroupDescription: aws.String("src"),
			})
			require.NoError(t, err)

			seedKeyspace(t, dialSnapRESP(t, groupAddr(t, b, "rg-src")))

			_, err = client.CreateSnapshot(t.Context(), &elasticachesdk.CreateSnapshotInput{
				ReplicationGroupId: aws.String("rg-src"), SnapshotName: aws.String("rg-snap"),
			})
			require.NoError(t, err)

			in := &elasticachesdk.CreateReplicationGroupInput{
				ReplicationGroupId:          aws.String("rg-dst"),
				ReplicationGroupDescription: aws.String("dst"),
			}
			if tt.restore {
				in.SnapshotName = aws.String("rg-snap")
			}

			_, err = client.CreateReplicationGroup(t.Context(), in)
			require.NoError(t, err)

			c := dialSnapRESP(t, groupAddr(t, b, "rg-dst"))
			if tt.restore {
				assertKeyspace(t, c)

				return
			}

			assert.Equal(t, "$-1", c.do(t, "GET", "str"))
		})
	}
}

func TestSnapshotData_EmptySourceRestores(t *testing.T) {
	t.Parallel()

	b, client := newSnapStack(t)

	_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId: aws.String("empty"), Engine: aws.String("redis"),
	})
	require.NoError(t, err)

	_, err = client.CreateSnapshot(t.Context(), &elasticachesdk.CreateSnapshotInput{
		CacheClusterId: aws.String("empty"), SnapshotName: aws.String("empty-snap"),
	})
	require.NoError(t, err)

	_, err = client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId: aws.String("from-empty"), SnapshotName: aws.String("empty-snap"),
	})
	require.NoError(t, err)

	assert.Equal(t, "$-1", dialSnapRESP(t, clusterAddr(t, b, "from-empty")).do(t, "GET", "str"))
}
