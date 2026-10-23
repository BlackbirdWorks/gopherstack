package opencypher_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

func TestExportImportRoundTrip(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		setup string
		check string
		want  string
	}{
		{"graph", seed, "MATCH (a)-[r:KNOWS]->(b) RETURN a.name AS a, r.since AS s, b.name AS b ORDER BY s",
			`[{"a":"Alice","s":2010,"b":"Bob"},{"a":"Bob","s":2015,"b":"Cara"}]`},
		{"types", "CREATE (:T {i:1, f:1.0, g:2.5, l:[1,2.0], s:'a', b:true})",
			"MATCH (n:T) RETURN n.f / 2 AS f, n.i / 2 AS i, n.l AS l, n.g AS g",
			`[{"f":0.5,"i":0,"l":[1,2.0],"g":2.5}]`},
		{"labels", "CREATE (:A:B {k:1})", "MATCH (n:A:B) RETURN count(n) AS c", `[{"c":1}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			g := opencypher.NewGraph()
			_, err := run(t, g, tc.setup, nil)
			require.NoError(t, err)

			dump, err := g.Export(t.Context())
			require.NoError(t, err)

			g2 := opencypher.NewGraph()
			require.NoError(t, g2.Import(t.Context(), dump))

			got, err := run(t, g2, tc.check, nil)
			require.NoError(t, err)
			assert.JSONEq(t, tc.want, got)

			// Sequence numbers survive, so new entities do not collide or reorder.
			_, err = run(t, g2, "CREATE (:Later)", nil)
			require.NoError(t, err)
			n, _ := g2.Counts()
			n1, _ := g.Counts()
			assert.Equal(t, n1+1, n)
		})
	}
}

func TestImportRejectsGarbage(t *testing.T) {
	t.Parallel()

	require.Error(t, opencypher.NewGraph().Import(t.Context(), []byte("not json")))
}

func TestTransactions(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		want   string
		commit bool
	}{
		{"commit", `[{"c":1}]`, true},
		{"rollback", `[{"c":0}]`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			g := opencypher.NewGraph()

			tx, err := g.Begin(t.Context())
			require.NoError(t, err)

			_, err = tx.Execute(t.Context(), "CREATE (:X)", nil)
			require.NoError(t, err)

			// A failing statement leaves earlier writes of the transaction intact.
			_, err = tx.Execute(t.Context(), "MATCH (n RETURN n", nil)
			require.Error(t, err)

			if tc.commit {
				tx.Commit()
			} else {
				tx.Rollback()
			}

			_, err = tx.Execute(t.Context(), "RETURN 1", nil)
			require.Error(t, err)

			got, err := run(t, g, "MATCH (n:X) RETURN count(n) AS c", nil)
			require.NoError(t, err)
			assert.JSONEq(t, tc.want, got)
		})
	}
}
