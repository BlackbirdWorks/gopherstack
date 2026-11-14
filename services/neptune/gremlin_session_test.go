package neptune_test

import (
	"testing"

	gremlingo "github.com/apache/tinkerpop/gremlin-go/v3/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func vertexCount(t *testing.T, g *gremlingo.GraphTraversalSource) int64 {
	t.Helper()

	rs, err := g.V().Count().ToList()
	require.NoError(t, err)
	require.Len(t, rs, 1)

	n, err := rs[0].GetInt64()
	require.NoError(t, err)

	return n
}

func TestGremlinGoTransactions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		finish func(t *testing.T, tx *gremlingo.Transaction)
		name   string
		want   int64
	}{
		{
			name: "commit", want: 2,
			finish: func(t *testing.T, tx *gremlingo.Transaction) { t.Helper(); require.NoError(t, tx.Commit()) },
		},
		{
			name:   "rollback",
			finish: func(t *testing.T, tx *gremlingo.Transaction) { t.Helper(); require.NoError(t, tx.Rollback()) },
		},
		{
			name:   "close_without_commit",
			finish: func(t *testing.T, tx *gremlingo.Transaction) { t.Helper(); require.NoError(t, tx.Close()) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, c := newEmbeddedBackend(t)
			g := gremlinG(t, c)

			tx := g.Tx()
			gtx, err := tx.Begin()
			require.NoError(t, err)
			require.NoError(t, <-gtx.AddV("person").Iterate())
			require.NoError(t, <-gtx.AddV("software").Iterate())
			tt.finish(t, tx)

			assert.Equal(t, tt.want, vertexCount(t, g))
		})
	}
}

func TestGremlinGoScriptSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		script []string
		want   int64
	}{
		{"close_commits_all_requests", []string{"g.addV('a').next()", "g.addV('b').next()"}, 2},
		{"failure_discards_open_tx", []string{"g.addV('a').next()", "g.V().fail('rollback')", "g.addV('b').next()"}, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, c := newEmbeddedBackend(t)
			g := gremlinG(t, c)

			drc, err := gremlingo.NewDriverRemoteConnection(gremlinWS(c))
			require.NoError(t, err)
			t.Cleanup(drc.Close)

			sess, err := drc.CreateSession()
			require.NoError(t, err)

			for _, q := range tt.script {
				rs, serr := sess.Submit(q)
				if serr == nil {
					_, serr = rs.All()
				}

				_ = serr
			}

			sess.Close()

			assert.Equal(t, tt.want, vertexCount(t, g))
		})
	}
}

func TestGremlinGoMerge(t *testing.T) {
	t.Parallel()

	_, c := newEmbeddedBackend(t)
	g := gremlinG(t, c)

	for range 2 {
		require.NoError(t, <-g.MergeV(map[any]any{gremlingo.T.Label: "person", "name": "marko"}).
			Option(gremlingo.Merge.OnCreate, map[any]any{"age": int32(29)}).
			Option(gremlingo.Merge.OnMatch, map[any]any{"seen": true}).Iterate())
	}

	assert.Equal(t, int64(1), vertexCount(t, g))

	rs, err := g.V().Has("name", "marko").Values("age").ToList()
	require.NoError(t, err)
	assert.Equal(t, []string{"29"}, renderAll(rs))

	rs, err = g.V().Has("name", "marko").Values("seen").ToList()
	require.NoError(t, err)
	assert.Equal(t, []string{"true"}, renderAll(rs))

	require.NoError(t, <-g.AddV("software").Property(gremlingo.T.Id, "lop").Iterate())
	require.NoError(t, <-g.MergeE(map[any]any{
		gremlingo.T.Label: "created", gremlingo.Direction.Out: "lop", gremlingo.Direction.In: "lop",
	}).Iterate())

	rs, err = g.E().HasLabel("created").Count().ToList()
	require.NoError(t, err)
	assert.Equal(t, []string{"1"}, renderAll(rs))
}
