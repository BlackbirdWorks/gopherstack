package neptune_test

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/bolt"
	"github.com/blackbirdworks/gopherstack/services/neptune"
)

func newBoltBackend(t *testing.T) (*neptune.InMemoryBackend, neptune.DBCluster) {
	t.Helper()

	rt := &upstreamRuntime{fakeEngineRuntime: &fakeEngineRuntime{}}
	b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	b.EnableEngine(neptune.EngineConfig{
		Runtime: rt, Probe: func(context.Context, string) error { return nil },
		StartTimeout: time.Minute, OpenCypher: true,
	})
	t.Cleanup(b.Close)

	createEngineCluster(t, b, "1.3.0.0")

	return b, waitCluster(t, b, "c1", "available")
}

func boltDial(t *testing.T, c neptune.DBCluster, v bolt.Version) *bolt.Client {
	t.Helper()

	nc, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", net.JoinHostPort(c.Endpoint, strconv.Itoa(c.Port)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Close() })

	cl, err := bolt.Dial(nc, v)
	require.NoError(t, err)

	r, err := cl.Call(bolt.TagHello, map[string]any{"user_agent": "t", "scheme": "none"})
	require.NoError(t, err)
	require.Equal(t, bolt.TagSuccess, r.Tag)

	if v.Major == 5 && v.Minor >= 1 {
		r, err = cl.Call(bolt.TagLogon, map[string]any{"scheme": "none"})
		require.NoError(t, err)
		require.Equal(t, bolt.TagSuccess, r.Tag)
	}

	return cl
}

func httpCount(t *testing.T, c neptune.DBCluster, label string) float64 {
	t.Helper()

	base := "http://" + net.JoinHostPort(c.Endpoint, strconv.Itoa(c.Port))
	code, out := cypherDo(t, http.MethodGet, base+"/openCypher?"+url.Values{
		"query": {"MATCH (n:" + label + ") RETURN count(n) AS c"},
	}.Encode(), "", "")
	require.Equal(t, http.StatusOK, code)

	rows, ok := out["results"].([]any)
	require.True(t, ok)
	require.Len(t, rows, 1)

	return rows[0].(map[string]any)["c"].(float64)
}

func TestBoltOnClusterPort(t *testing.T) {
	t.Parallel()

	for _, v := range []bolt.Version{{Major: 4, Minor: 4}, {Major: 5, Minor: 4}} {
		t.Run(strconv.Itoa(int(v.Major))+"."+strconv.Itoa(int(v.Minor)), func(t *testing.T) {
			t.Parallel()

			_, c := newBoltBackend(t)
			cl := boltDial(t, c, v)

			rows, sum, err := cl.Query(
				"CREATE (n:Person {name:$n, age:30, h:1.5}) RETURN n",
				map[string]any{"n": "Alice"},
			)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			assert.Equal(t, "w", sum.Meta()["type"])

			node, ok := rows[0][0].(*bolt.Struct)
			require.True(t, ok)
			assert.Equal(t, byte(0x4E), node.Tag)
			assert.Equal(t, []any{"Person"}, node.Fields[1])
			assert.Equal(t, map[string]any{"name": "Alice", "age": int64(30), "h": 1.5}, node.Fields[2])
			assert.Len(t, node.Fields, map[byte]int{4: 3, 5: 4}[v.Major])

			// Bolt and HTTP share one graph.
			assert.InDelta(t, 1, httpCount(t, c, "Person"), 0)

			rows, _, err = cl.Query("MATCH (n:Person)-[r:KNOWS]->(m) RETURN r", nil)
			require.NoError(t, err)
			assert.Empty(t, rows)
		})
	}
}

func TestBoltRelationshipAndPath(t *testing.T) {
	t.Parallel()

	_, c := newBoltBackend(t)
	cl := boltDial(t, c, bolt.Version{Major: 5, Minor: 0})

	_, _, err := cl.Query("CREATE (:P {n:1})-[:R {w:2}]->(:P {n:2})", nil)
	require.NoError(t, err)

	rows, _, err := cl.Query("MATCH (a)-[r:R]->(b) RETURN r", nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)

	rel := rows[0][0].(*bolt.Struct)
	assert.Equal(t, byte(0x52), rel.Tag)
	assert.Equal(t, "R", rel.Fields[3])
	assert.Len(t, rel.Fields, 8)

	rows, _, err = cl.Query("MATCH p=(a)-[r:R]->(b) RETURN p", nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)

	path := rows[0][0].(*bolt.Struct)
	assert.Equal(t, byte(0x50), path.Tag)
	assert.Len(t, path.Fields[0], 2)
	assert.Equal(t, []any{int64(1), int64(1)}, path.Fields[2])
}

func TestBoltTransactionsAndErrors(t *testing.T) {
	t.Parallel()

	_, c := newBoltBackend(t)
	cl := boltDial(t, c, bolt.Version{Major: 5, Minor: 4})

	for _, tc := range []struct {
		name string
		end  byte
		want float64
	}{
		{"rollback", bolt.TagRollback, 0},
		{"commit", bolt.TagCommit, 1},
	} {
		r, err := cl.Call(bolt.TagBegin, map[string]any{})
		require.NoError(t, err, tc.name)
		require.Equal(t, bolt.TagSuccess, r.Tag)

		_, err = cl.Call(bolt.TagRun, "CREATE (:Tx)", map[string]any{}, map[string]any{})
		require.NoError(t, err)
		_, _, err = cl.PullAll(-1)
		require.NoError(t, err)

		r, err = cl.Call(tc.end)
		require.NoError(t, err)
		require.Equal(t, bolt.TagSuccess, r.Tag, tc.name)
		assert.InDelta(t, tc.want, httpCount(t, c, "Tx"), 0, tc.name)
	}

	r, err := cl.Call(bolt.TagRun, "MATCH (n RETURN n", map[string]any{}, map[string]any{})
	require.NoError(t, err)
	require.Equal(t, bolt.TagFailure, r.Tag)
	assert.Equal(t, bolt.CodeSyntax, r.Meta()["code"])
	assert.Contains(t, r.Meta()["message"], "MalformedQueryException")

	r, err = cl.Call(bolt.TagReset)
	require.NoError(t, err)
	assert.Equal(t, bolt.TagSuccess, r.Tag)

	r, err = cl.Call(bolt.TagRoute, map[string]any{}, []any{}, map[string]any{})
	require.NoError(t, err)
	require.Equal(t, bolt.TagSuccess, r.Tag)
	assert.Contains(t, r.Meta()["rt"], "servers")
}

func TestBoltGraphPersistsAcrossSnapshot(t *testing.T) {
	t.Parallel()

	b, c := newBoltBackend(t)
	cl := boltDial(t, c, bolt.Version{Major: 5, Minor: 0})

	_, _, err := cl.Query("CREATE (:Kept {f:2.0, i:2})", nil)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	rt := &upstreamRuntime{fakeEngineRuntime: &fakeEngineRuntime{}}
	b2 := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	b2.EnableEngine(neptune.EngineConfig{
		Runtime: rt, Probe: func(context.Context, string) error { return nil },
		StartTimeout: time.Minute, OpenCypher: true,
	})
	t.Cleanup(b2.Close)
	require.NoError(t, b2.Restore(t.Context(), snap))

	c2 := waitCluster(t, b2, "c1", "available")
	cl2 := boltDial(t, c2, bolt.Version{Major: 5, Minor: 0})

	rows, _, err := cl2.Query("MATCH (n:Kept) RETURN n.f AS f, n.i AS i", nil)
	require.NoError(t, err)
	assert.Equal(t, [][]any{{2.0, int64(2)}}, rows)
}
