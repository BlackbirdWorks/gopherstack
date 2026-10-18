package memorydb_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

func seedKeyspace(t *testing.T, c *respConn) {
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

func assertKeyspace(t *testing.T, c *respConn) {
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

func TestSnapshotData_ClusterRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		take func(t *testing.T, h *memorydb.Handler) string
		name string
	}{
		{
			name: "create_snapshot",
			take: func(t *testing.T, h *memorydb.Handler) string {
				t.Helper()
				rec := doRequest(
					t,
					h,
					"CreateSnapshot",
					map[string]any{"ClusterName": "src", "SnapshotName": "snap-a"},
				)
				require.Equal(t, 200, rec.Code, rec.Body.String())

				return "snap-a"
			},
		},
		{
			name: "copy_snapshot",
			take: func(t *testing.T, h *memorydb.Handler) string {
				t.Helper()
				rec := doRequest(
					t,
					h,
					"CreateSnapshot",
					map[string]any{"ClusterName": "src", "SnapshotName": "snap-orig"},
				)
				require.Equal(t, 200, rec.Code, rec.Body.String())
				rec = doRequest(t, h, "CopySnapshot", map[string]any{
					"SourceSnapshotName": "snap-orig", "TargetSnapshotName": "snap-copy",
				})
				require.Equal(t, 200, rec.Code, rec.Body.String())
				rec = doRequest(t, h, "DeleteSnapshot", map[string]any{"SnapshotName": "snap-orig"})
				require.Equal(t, 200, rec.Code, rec.Body.String())

				return "snap-copy"
			},
		},
		{
			name: "final_snapshot_on_delete",
			take: func(t *testing.T, h *memorydb.Handler) string {
				t.Helper()
				rec := doRequest(t, h, "DeleteCluster", map[string]any{
					"ClusterName": "src", "FinalSnapshotName": "snap-final",
				})
				require.Equal(t, 200, rec.Code, rec.Body.String())

				return "snap-final"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newDataPlaneHandler(t, memorydb.EngineEmbedded)
			src := createClusterObj(t, h, minimalClusterBody("src"))
			seedKeyspace(t, dialResp(t, connectAddr(t, src)))

			snap := tt.take(t, h)

			body := minimalClusterBody("restored")
			body["SnapshotName"] = snap
			restored := createClusterObj(t, h, body)

			assertKeyspace(t, dialResp(t, connectAddr(t, restored)))
		})
	}
}

func TestSnapshotData_EmptySourceRestores(t *testing.T) {
	t.Parallel()

	h, _ := newDataPlaneHandler(t, memorydb.EngineEmbedded)
	createClusterObj(t, h, minimalClusterBody("empty"))

	rec := doRequest(
		t,
		h,
		"CreateSnapshot",
		map[string]any{"ClusterName": "empty", "SnapshotName": "empty-snap"},
	)
	require.Equal(t, 200, rec.Code, rec.Body.String())

	body := minimalClusterBody("from-empty")
	body["SnapshotName"] = "empty-snap"
	restored := createClusterObj(t, h, body)

	assert.Equal(t, "$-1", dialResp(t, connectAddr(t, restored)).do(t, "GET", "str"))
}
