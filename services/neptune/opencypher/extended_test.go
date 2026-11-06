package opencypher_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

func TestUnionListComprehensionAndPatternPredicates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			name: "union distinct",
			query: "MATCH (n:Person) WHERE n.age > 28 RETURN n.name AS n UNION " +
				"MATCH (n:Admin) RETURN n.name AS n",
			want: `[{"n":"Alice"},{"n":"Cara"}]`,
		},
		{
			name: "union all keeps duplicates",
			query: "MATCH (n:Person) WHERE n.age > 28 RETURN n.name AS n UNION ALL " +
				"MATCH (n:Admin) RETURN n.name AS n",
			want: `[{"n":"Alice"},{"n":"Cara"},{"n":"Cara"}]`,
		},
		{name: "union three parts", query: "RETURN 1 AS x UNION RETURN 2 AS x UNION RETURN 1 AS x", want: `[{"x":1},{"x":2}]`},
		{name: "list comprehension filter project", query: "RETURN [x IN range(1,6) WHERE x % 2 = 0 | x * 10] AS l",
			want: `[{"l":[20,40,60]}]`},
		{name: "list comprehension identity", query: "RETURN [x IN [1,2,3]] AS l", want: `[{"l":[1,2,3]}]`},
		{name: "list comprehension where only", query: "RETURN [x IN [1,2,3] WHERE x > 1] AS l", want: `[{"l":[2,3]}]`},
		{
			name: "list comprehension over property",
			query: "MATCH (n:Person) WITH collect(n.name) AS names " +
				"RETURN [s IN names | toUpper(s)] AS up",
			want: `[{"up":["ALICE","BOB","CARA"]}]`,
		},
		{
			name:  "pattern predicate",
			query: "MATCH (a:Person) WHERE (a)-[:KNOWS]->() RETURN a.name AS n ORDER BY n",
			want:  `[{"n":"Alice"},{"n":"Bob"}]`,
		},
		{
			name:  "negated pattern predicate",
			query: "MATCH (a:Person) WHERE NOT (a)-[:KNOWS]->() RETURN a.name AS n",
			want:  `[{"n":"Cara"}]`,
		},
		{
			name:  "pattern predicate with bound ends",
			query: "MATCH (a:Person), (b:Person) WHERE (a)-[:LIKES]->(b) RETURN a.name AS a, b.name AS b",
			want:  `[{"a":"Alice","b":"Cara"}]`,
		},
		{
			name:  "size of pattern",
			query: "MATCH (a:Person {name:'Alice'}) RETURN size((a)-->()) AS c",
			want:  `[{"c":2}]`,
		},
		{name: "parenthesised expression unaffected", query: "RETURN (1 + 2) * 3 AS v", want: `[{"v":9}]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			g := opencypher.NewGraph()
			_, err := run(t, g, seed, nil)
			require.NoError(t, err)

			got, err := run(t, g, tt.query, nil)
			require.NoError(t, err)
			assert.JSONEq(t, tt.want, got)
		})
	}
}

func TestUnionErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		code  string
	}{
		{name: "column mismatch", query: "RETURN 1 AS a UNION RETURN 2 AS b", code: opencypher.CodeMalformedQuery},
		{name: "mixed union kinds", query: "RETURN 1 AS a UNION RETURN 2 AS a UNION ALL RETURN 3 AS a",
			code: opencypher.CodeMalformedQuery},
		{name: "update in union", query: "CREATE (:X) RETURN 1 AS a UNION RETURN 2 AS a", code: opencypher.CodeUnsupported},
		{name: "part without return", query: "MATCH (n) UNION RETURN 1 AS a", code: opencypher.CodeMalformedQuery},
		{name: "dangling union", query: "RETURN 1 AS a UNION", code: opencypher.CodeMalformedQuery},
		{name: "foreach unsupported", query: "FOREACH (x IN [1] | CREATE (:X))", code: opencypher.CodeUnsupported},
		{name: "shortest path unsupported", query: "MATCH p = shortestPath((a)-[*]-(b)) RETURN p",
			code: opencypher.CodeUnsupported},
		{name: "all shortest paths unsupported", query: "MATCH p = allShortestPaths((a)-[*]-(b)) RETURN p",
			code: opencypher.CodeUnsupported},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			g := opencypher.NewGraph()
			_, err := run(t, g, seed, nil)
			require.NoError(t, err)

			_, err = run(t, g, tt.query, nil)
			require.Error(t, err)

			e, ok := opencypher.AsError(err)
			require.True(t, ok, err.Error())
			assert.Equal(t, tt.code, e.Code, err.Error())
		})
	}
}
