package opencypher_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
	"github.com/blackbirdworks/gopherstack/services/neptune/streamlog"
)

var errBoom = errors.New("boom")

func streamGraph() *opencypher.Graph {
	g := opencypher.NewGraph()
	g.Stream().SetPolicy(streamlog.Policy{Enabled: true})

	return g
}

func streamOps(t *testing.T, g *opencypher.Graph) []string {
	t.Helper()

	page, err := g.Stream().Read(streamlog.Query{Iterator: streamlog.TrimHorizon, Limit: 100})
	require.NoError(t, err)

	out := make([]string, 0, len(page.Records))
	for _, r := range page.Records {
		out = append(out, r.Op+" "+r.Data.Type+" "+r.Data.Key)
	}

	return out
}

func TestStreamRecordsCommitsOnly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		do   func(t *testing.T, g *opencypher.Graph)
		name string
		want []string
	}{
		{
			name: "execute",
			do: func(t *testing.T, g *opencypher.Graph) {
				t.Helper()
				_, err := g.Execute(t.Context(), "CREATE (:A {n:1})", nil)
				require.NoError(t, err)
			},
			want: []string{"ADD vl label", "ADD vp n"},
		},
		{
			name: "failed write",
			do: func(t *testing.T, g *opencypher.Graph) {
				t.Helper()
				_, err := g.Execute(t.Context(), "CREATE (:A {n:1}) WITH 1 AS x RETURN 1/0", nil)
				require.Error(t, err)
			},
		},
		{
			name: "read only",
			do: func(t *testing.T, g *opencypher.Graph) {
				t.Helper()
				_, err := g.Execute(t.Context(), "MATCH (n) RETURN n", nil)
				require.NoError(t, err)
			},
		},
		{
			name: "rolled back transaction",
			do: func(t *testing.T, g *opencypher.Graph) {
				t.Helper()
				tx, err := g.Begin(t.Context())
				require.NoError(t, err)
				_, err = tx.Execute(t.Context(), "CREATE (:A)", nil)
				require.NoError(t, err)
				tx.Rollback()
			},
		},
		{
			name: "committed transaction",
			do: func(t *testing.T, g *opencypher.Graph) {
				t.Helper()
				tx, err := g.Begin(t.Context())
				require.NoError(t, err)
				_, err = tx.Execute(t.Context(), "CREATE (:A)", nil)
				require.NoError(t, err)
				_, err = tx.Execute(t.Context(), "CREATE (:B)", nil)
				require.NoError(t, err)
				tx.Commit()
			},
			want: []string{"ADD vl label", "ADD vl label"},
		},
		{
			name: "failed update",
			do: func(t *testing.T, g *opencypher.Graph) {
				t.Helper()

				err := g.Update(t.Context(), func(st *opencypher.Store) error {
					_, aerr := st.AddNode("x", []string{"A"}, nil)
					require.NoError(t, aerr)

					return errBoom
				})
				require.Error(t, err)
			},
		},
		{
			name: "update",
			do: func(t *testing.T, g *opencypher.Graph) {
				t.Helper()

				require.NoError(t, g.Update(t.Context(), func(st *opencypher.Store) error {
					_, aerr := st.AddNode("x", []string{"A"}, map[string]any{"k": "v"})

					return aerr
				}))
			},
			want: []string{"ADD vl label", "ADD vp k"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			g := streamGraph()
			tt.do(t, g)
			assert.Equal(t, append([]string{}, tt.want...), append([]string{}, streamOps(t, g)...))
		})
	}
}

func TestStreamRecordOrder(t *testing.T) {
	t.Parallel()

	g := streamGraph()
	_, err := g.Execute(t.Context(), "CREATE (a:A {n:1})-[:R {w:1}]->(b:B)", nil)
	require.NoError(t, err)
	_, err = g.Execute(t.Context(), "MATCH (a:A) SET a.n = 2", nil)
	require.NoError(t, err)
	_, err = g.Execute(t.Context(), "MATCH (a:A) DETACH DELETE a", nil)
	require.NoError(t, err)

	assert.Equal(t, []string{
		"ADD vl label", "ADD vp n", "ADD vl label", "ADD e label", "ADD ep w",
		"REMOVE vp n", "ADD vp n",
		"REMOVE ep w", "REMOVE e label", "REMOVE vp n", "REMOVE vl label",
	}, streamOps(t, g))
}
