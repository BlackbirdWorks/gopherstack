package opencypher_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

const seed = `CREATE (a:Person {name:'Alice', age:30}), (b:Person {name:'Bob', age:25}),
 (c:Person:Admin {name:'Cara', age:35}),
 (a)-[:KNOWS {since:2010}]->(b), (b)-[:KNOWS {since:2015}]->(c), (a)-[:LIKES]->(c)`

func run(t *testing.T, g *opencypher.Graph, q string, params map[string]any) (string, error) {
	t.Helper()

	res, err := g.Execute(t.Context(), q, params)
	if err != nil {
		return "", err
	}

	b, err := json.Marshal(res)
	require.NoError(t, err)

	return string(b), nil
}

func TestQueries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params map[string]any
		name   string
		query  string
		want   string
	}{
		{name: "count all", query: "MATCH (n) RETURN count(*) AS c", want: `[{"c":3}]`},
		{name: "label filter", query: "MATCH (n:Admin) RETURN n.name", want: `[{"n.name":"Cara"}]`},
		{
			name:  "where order limit",
			query: "MATCH (n:Person) WHERE n.age >= 30 RETURN n.name AS name ORDER BY n.age DESC LIMIT 1",
			want:  `[{"name":"Cara"}]`,
		},
		{
			name: "param", params: map[string]any{"who": "Bob"},
			query: "MATCH (n:Person {name:$who}) RETURN n.age AS age", want: `[{"age":25}]`,
		},
		{
			name:  "traverse out",
			query: "MATCH (a {name:'Alice'})-[:KNOWS]->(b) RETURN b.name AS n",
			want:  `[{"n":"Bob"}]`,
		},
		{
			name:  "traverse in",
			query: "MATCH (c {name:'Cara'})<-[r]-(x) RETURN x.name AS n, type(r) AS t ORDER BY n",
			want:  `[{"n":"Alice","t":"LIKES"},{"n":"Bob","t":"KNOWS"}]`,
		},
		{
			name:  "undirected",
			query: "MATCH (b {name:'Bob'})-[:KNOWS]-(x) RETURN x.name AS n ORDER BY n",
			want:  `[{"n":"Alice"},{"n":"Cara"}]`,
		},
		{
			name:  "variable length",
			query: "MATCH (a {name:'Alice'})-[:KNOWS*1..2]->(x) RETURN x.name AS n ORDER BY n",
			want:  `[{"n":"Bob"},{"n":"Cara"}]`,
		},
		{
			name:  "collect group",
			query: "MATCH (a)-[:KNOWS]->(b) RETURN a.name AS a, collect(b.name) AS bs ORDER BY a",
			want:  `[{"a":"Alice","bs":["Bob"]},{"a":"Bob","bs":["Cara"]}]`,
		},
		{
			name:  "aggregates",
			query: "MATCH (n:Person) RETURN sum(n.age) AS s, avg(n.age) AS a, min(n.age) AS lo, max(n.age) AS hi",
			want:  `[{"s":90,"a":30,"lo":25,"hi":35}]`,
		},
		{
			name:  "skip",
			query: "MATCH (n:Person) RETURN n.name AS n ORDER BY n SKIP 1",
			want:  `[{"n":"Bob"},{"n":"Cara"}]`,
		},
		{
			name:  "distinct",
			query: "MATCH (a)-[r]->() RETURN DISTINCT type(r) AS t ORDER BY t",
			want:  `[{"t":"KNOWS"},{"t":"LIKES"}]`,
		},
		{
			name:  "with filter",
			query: "MATCH (a)-[:KNOWS]->(b) WITH a, count(b) AS c WHERE c > 0 RETURN a.name AS n",
			want:  `[{"n":"Alice"},{"n":"Bob"}]`,
		},
		{name: "unwind", query: "UNWIND [1,2,3] AS x RETURN x * 2 AS d", want: `[{"d":2},{"d":4},{"d":6}]`},
		{
			name:  "optional match",
			query: "MATCH (n:Admin) OPTIONAL MATCH (n)-[:KNOWS]->(m) RETURN n.name AS n, m",
			want:  `[{"n":"Cara","m":null}]`,
		},
		{
			name:  "string ops",
			query: "MATCH (n:Person) WHERE n.name STARTS WITH 'A' OR n.name CONTAINS 'ob' RETURN n.name AS n ORDER BY n",
			want:  `[{"n":"Alice"},{"n":"Bob"}]`,
		},
		{name: "case", query: "RETURN CASE WHEN 1 > 2 THEN 'a' ELSE 'b' END AS c", want: `[{"c":"b"}]`},
		{name: "count empty", query: "MATCH (n:Nope) RETURN count(n) AS c", want: `[{"c":0}]`},
		{name: "is null", query: "MATCH (n:Person) WHERE n.missing IS NULL RETURN count(*) AS c", want: `[{"c":3}]`},
		{
			name:  "in",
			query: "MATCH (n:Person) WHERE n.age IN [25, 35] RETURN n.name AS n ORDER BY n",
			want:  `[{"n":"Bob"},{"n":"Cara"}]`,
		},
		{name: "labels fn", query: "MATCH (n:Admin) RETURN labels(n) AS l", want: `[{"l":["Person","Admin"]}]`},
		{name: "list slice", query: "RETURN [1,2,3,4][1..3] AS s", want: `[{"s":[2,3]}]`},
		{name: "integer division", query: "RETURN 7 / 2 AS i, 7 / 2.0 AS f", want: `[{"i":3,"f":3.5}]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			g := opencypher.NewGraph()
			_, err := run(t, g, seed, nil)
			require.NoError(t, err)

			got, err := run(t, g, tt.query, tt.params)
			require.NoError(t, err)
			assert.JSONEq(t, tt.want, got)
		})
	}
}

func TestNodeAndRelationshipShapes(t *testing.T) {
	t.Parallel()

	g := opencypher.NewGraph()
	_, err := run(
		t,
		g,
		`CREATE (:Person {`+"`~id`"+`:'n1', name:'A'})-[:KNOWS {w:1}]->(:Person {`+"`~id`"+`:'n2'})`,
		nil,
	)
	require.NoError(t, err)

	got, err := run(t, g, "MATCH (a)-[r]->(b) RETURN a, r", nil)
	require.NoError(t, err)

	var rows []map[string]map[string]any

	require.NoError(t, json.Unmarshal([]byte(got), &rows))
	require.Len(t, rows, 1)

	a, r := rows[0]["a"], rows[0]["r"]
	assert.Equal(t, "n1", a["~id"])
	assert.Equal(t, "node", a["~entityType"])
	assert.Equal(t, []any{"Person"}, a["~labels"])
	assert.Equal(t, map[string]any{"name": "A"}, a["~properties"])
	assert.Equal(t, "relationship", r["~entityType"])
	assert.Equal(t, "n1", r["~start"])
	assert.Equal(t, "n2", r["~end"])
	assert.Equal(t, "KNOWS", r["~type"])
	assert.Equal(t, map[string]any{"w": float64(1)}, r["~properties"])
}

func TestWrites(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		write     string
		check     string
		want      string
		wantNodes int
		wantRels  int
	}{
		{
			name: "set property", write: "MATCH (n {name:'Bob'}) SET n.age = 26, n.city = 'X'",
			check: "MATCH (n {name:'Bob'}) RETURN n.age AS a, n.city AS c", want: `[{"a":26,"c":"X"}]`,
			wantNodes: 3, wantRels: 3,
		},
		{
			name: "set null removes", write: "MATCH (n {name:'Bob'}) SET n.age = null",
			check: "MATCH (n {name:'Bob'}) RETURN n.age AS a", want: `[{"a":null}]`, wantNodes: 3, wantRels: 3,
		},
		{
			name:      "set map merge",
			write:     "MATCH (n {name:'Bob'}) SET n += {x: 1}",
			check:     "MATCH (n {name:'Bob'}) RETURN n.x AS x, n.age AS a",
			want:      `[{"x":1,"a":25}]`,
			wantNodes: 3,
			wantRels:  3,
		},
		{
			name: "set label", write: "MATCH (n {name:'Bob'}) SET n:Admin",
			check: "MATCH (n:Admin) RETURN count(*) AS c", want: `[{"c":2}]`, wantNodes: 3, wantRels: 3,
		},
		{
			name: "remove label", write: "MATCH (n:Admin) REMOVE n:Admin",
			check: "MATCH (n:Admin) RETURN count(*) AS c", want: `[{"c":0}]`, wantNodes: 3, wantRels: 3,
		},
		{
			name: "detach delete", write: "MATCH (n {name:'Cara'}) DETACH DELETE n",
			check: "MATCH (n) RETURN count(*) AS c", want: `[{"c":2}]`, wantNodes: 2, wantRels: 1,
		},
		{
			name: "delete rel", write: "MATCH ()-[r:LIKES]->() DELETE r",
			check: "MATCH ()-[r]->() RETURN count(*) AS c", want: `[{"c":2}]`, wantNodes: 3, wantRels: 2,
		},
		{
			name: "merge existing", write: "MERGE (n:Person {name:'Alice'}) ON MATCH SET n.seen = true",
			check: "MATCH (n:Person) RETURN count(*) AS c", want: `[{"c":3}]`, wantNodes: 3, wantRels: 3,
		},
		{
			name: "merge creates", write: "MERGE (n:Person {name:'Dan'}) ON CREATE SET n.age = 1",
			check: "MATCH (n {name:'Dan'}) RETURN n.age AS a", want: `[{"a":1}]`, wantNodes: 4, wantRels: 3,
		},
		{
			name:      "create rel between matched",
			write:     "MATCH (a {name:'Bob'}), (b {name:'Alice'}) CREATE (a)-[:KNOWS]->(b)",
			check:     "MATCH ()-[r:KNOWS]->() RETURN count(*) AS c",
			want:      `[{"c":3}]`,
			wantNodes: 3,
			wantRels:  4,
		},
		{
			name: "unwind create", write: "UNWIND [1,2] AS i CREATE (:Num {v: i})",
			check: "MATCH (n:Num) RETURN sum(n.v) AS s", want: `[{"s":3}]`, wantNodes: 5, wantRels: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			g := opencypher.NewGraph()
			_, err := run(t, g, seed, nil)
			require.NoError(t, err)

			out, err := run(t, g, tt.write, nil)
			require.NoError(t, err)
			assert.JSONEq(t, `[]`, out)

			got, err := run(t, g, tt.check, nil)
			require.NoError(t, err)
			assert.JSONEq(t, tt.want, got)

			nodes, rels := g.Counts()
			assert.Equal(t, tt.wantNodes, nodes)
			assert.Equal(t, tt.wantRels, rels)
		})
	}
}

func TestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		code  string
	}{
		{name: "syntax", query: "MATC (n) RETURN n", code: opencypher.CodeMalformedQuery},
		{name: "empty", query: "  ", code: opencypher.CodeMalformedQuery},
		{name: "undefined var", query: "MATCH (n) RETURN m", code: opencypher.CodeMalformedQuery},
		{
			name:  "delete connected",
			query: "MATCH (n {name:'Alice'}) DELETE n",
			code:  opencypher.CodeConstraintViolation,
		},
		{name: "missing param", query: "RETURN $nope", code: opencypher.CodeInvalidParameter},
		{name: "map property", query: "CREATE (:X {p: {a: 1}})", code: opencypher.CodeConstraintViolation},
		{name: "call unsupported", query: "CALL db.labels()", code: opencypher.CodeUnsupported},
		{name: "unknown function", query: "RETURN nope(1)", code: opencypher.CodeMalformedQuery},
		{name: "ends with match", query: "MATCH (n)", code: opencypher.CodeMalformedQuery},
		{name: "unterminated string", query: "RETURN 'abc", code: opencypher.CodeMalformedQuery},
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
			require.True(t, ok)
			assert.Equal(t, tt.code, e.Code)
		})
	}
}

func TestFailedWriteRollsBack(t *testing.T) {
	t.Parallel()

	g := opencypher.NewGraph()
	_, err := run(t, g, seed, nil)
	require.NoError(t, err)

	_, err = run(t, g, "CREATE (:Tmp) WITH 1 AS x MATCH (n {name:'Alice'}) DELETE n", nil)
	require.Error(t, err)

	nodes, rels := g.Counts()
	assert.Equal(t, 3, nodes)
	assert.Equal(t, 3, rels)
}

func TestParseParameters(t *testing.T) {
	t.Parallel()

	got, err := opencypher.ParseParameters(`{"a":1,"b":1.5,"c":"s","d":[1,2],"e":{"k":2}}`)
	require.NoError(t, err)
	assert.Equal(t, int64(1), got["a"])
	assert.InDelta(t, 1.5, got["b"], 0)
	assert.Equal(t, []any{int64(1), int64(2)}, got["d"])
	assert.Equal(t, map[string]any{"k": int64(2)}, got["e"])

	_, err = opencypher.ParseParameters(`[1]`)
	require.Error(t, err)

	empty, err := opencypher.ParseParameters("")
	require.NoError(t, err)
	assert.Empty(t, empty)
}
