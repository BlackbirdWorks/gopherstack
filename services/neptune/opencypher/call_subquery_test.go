package opencypher_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

func TestCallSubquery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		query   string
		want    string
		wantErr string
	}{
		{
			name: "per row limit",
			query: `MATCH (p:Person) CALL { WITH p MATCH (p)-[:KNOWS]->(f) RETURN f.name AS friend }
				RETURN p.name AS p, friend ORDER BY p`,
			want: `[{"p":"Alice","friend":"Bob"},{"p":"Bob","friend":"Cara"}]`,
		},
		{
			name: "aggregate per row",
			query: `MATCH (p:Person) CALL { WITH p MATCH (p)-[r]->() RETURN count(r) AS out }
				RETURN p.name AS p, out ORDER BY p`,
			want: `[{"p":"Alice","out":2},{"p":"Bob","out":1},{"p":"Cara","out":0}]`,
		},
		{
			name: "uncorrelated",
			query: `MATCH (p:Person {name:'Bob'}) CALL { MATCH (a:Admin) RETURN a.name AS admin }
				RETURN p.name AS p, admin`,
			want: `[{"p":"Bob","admin":"Cara"}]`,
		},
		{
			name: "order limit inside",
			query: `MATCH (p:Person {name:'Alice'}) CALL { WITH p MATCH (p)-[]->(f) RETURN f.name AS f ORDER BY f DESC LIMIT 1 }
				RETURN f`,
			want: `[{"f":"Cara"}]`,
		},
		{
			name:    "update inside",
			query:   `MATCH (p:Person) CALL { WITH p CREATE (:X) RETURN 1 AS one } RETURN one`,
			wantErr: "Updates inside a CALL subquery",
		},
		{
			name:    "overlapping variable",
			query:   `MATCH (p:Person) CALL { WITH p RETURN 1 AS p } RETURN p`,
			wantErr: "already declared",
		},
		{
			name:    "aliased import",
			query:   `MATCH (p:Person) CALL { WITH p AS q RETURN q.name AS n } RETURN n`,
			wantErr: "without aliases",
		},
		{
			name:    "no return",
			query:   `MATCH (p:Person) CALL { WITH p MATCH (p)-[]->(f) WITH f } RETURN p`,
			wantErr: "must end with RETURN",
		},
		{name: "procedure call", query: `CALL db.labels()`, wantErr: "procedure"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			g := opencypher.NewGraph()
			_, err := run(t, g, seed, nil)
			require.NoError(t, err)

			got, err := run(t, g, tt.query, nil)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.JSONEq(t, tt.want, got)
		})
	}
}
