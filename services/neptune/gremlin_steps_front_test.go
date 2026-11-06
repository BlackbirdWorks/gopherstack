package neptune_test

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	gremlingo "github.com/apache/tinkerpop/gremlin-go/v3/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

func TestGremlinGoStepFamilies(t *testing.T) {
	t.Parallel()

	_, c := newEmbeddedBackend(t)
	g := gremlinG(t, c)
	seedGremlin(t, g)

	tests := []struct {
		name  string
		build func() *gremlingo.GraphTraversal
		want  []string
	}{
		{"math_current", func() *gremlingo.GraphTraversal {
			return g.V().HasLabel("person").Values("age").Math("_ / 2")
		}, []string{"14.5", "13.5", "16"}},
		{"math_by", func() *gremlingo.GraphTraversal {
			return g.V("marko").As("a").Out("knows").As("b").Math("a - b").By("age")
		}, []string{"2", "-3"}},
		{"sample_count", func() *gremlingo.GraphTraversal { return g.V().Sample(2).Count() }, []string{"2"}},
		{"sample_local", func() *gremlingo.GraphTraversal {
			return g.V().Values("name").Fold().Sample(gremlingo.Scope.Local, 2).Count(gremlingo.Scope.Local)
		}, []string{"2"}},
		{"coin_none", func() *gremlingo.GraphTraversal { return g.V().Coin(0).Count() }, []string{"0"}},
		{"coin_all", func() *gremlingo.GraphTraversal { return g.V().Coin(1).Count() }, []string{"4"}},
		{"sack_sum", func() *gremlingo.GraphTraversal {
			return g.WithSack(1).V("marko").OutE("knows").Sack(gremlingo.Operator.Sum).By("weight").Sack()
		}, []string{"1.5", "2"}},
		{"sack_order", func() *gremlingo.GraphTraversal {
			return g.WithSack(0).V("marko").OutE().Sack(gremlingo.Operator.Sum).By("weight").InV().
				Order().By(gremlingo.T__.Sack()).Values("name")
		}, []string{"lop", "vadas", "josh"}},
		{"index_list", func() *gremlingo.GraphTraversal {
			return g.V().HasLabel("software").Values("name").Fold().Index()
		}, []string{"[[lop 0]]"}},
		{"index_map", func() *gremlingo.GraphTraversal {
			return g.V().HasLabel("software").Values("name").Fold().Index().
				With(gremlingo.WithOptions.Indexer, gremlingo.WithOptions.Map)
		}, []string{"map[0:lop]"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rs, err := tt.build().ToList()
			require.NoError(t, err)

			got := renderAll(rs)
			if tt.name == "sample_local" || strings.HasPrefix(tt.name, "math") ||
				strings.HasPrefix(tt.name, "sack_sum") {
				assert.ElementsMatch(t, tt.want, got)

				return
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGremlinGoTree(t *testing.T) {
	t.Parallel()

	_, c := newEmbeddedBackend(t)
	g := gremlinG(t, c)
	seedGremlin(t, g)

	rs, err := g.V("marko").Out("knows").Tree().By("name").ToList()
	require.NoError(t, err)
	require.Len(t, rs, 1)

	tree, ok := rs[0].GetInterface().(map[any]any)
	require.True(t, ok, "%T", rs[0].GetInterface())
	require.Len(t, tree, 1)

	kids, ok := tree["marko"].(map[any]any)
	require.True(t, ok)
	assert.Len(t, kids, 2)
	assert.Contains(t, kids, "vadas")
	assert.Contains(t, kids, "josh")

	rs, err = g.V("marko").Out("knows").Tree("t").By("name").Cap("t").ToList()
	require.NoError(t, err)
	require.Len(t, rs, 1)
	assert.IsType(t, map[any]any{}, rs[0].GetInterface())
}

func TestGremlinGoSubgraphNeedsGraphSON(t *testing.T) {
	t.Parallel()

	_, c := newEmbeddedBackend(t)
	g := gremlinG(t, c)
	seedGremlin(t, g)

	_, err := g.E().HasLabel("knows").Subgraph("sg").Cap("sg").ToList()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "GraphBinary")

	n, err := g.E().HasLabel("knows").Subgraph("sg").Count().ToList()
	require.NoError(t, err)
	assert.Equal(t, []string{"2"}, renderAll(n))
}

func TestGremlinHTTPGraphSONShapes(t *testing.T) {
	t.Parallel()

	_, c := newEmbeddedBackend(t)
	seedGremlin(t, gremlinG(t, c))

	post := func(script string) map[string]any {
		body, err := json.Marshal(map[string]string{"gremlin": script})
		require.NoError(t, err)

		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
			"http://"+clusterAddr(c)+"/gremlin", strings.NewReader(string(body)))
		require.NoError(t, err)

		resp, err := testclient.New(t).Do(req)
		require.NoError(t, err)

		defer resp.Body.Close()

		raw, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode, string(raw))

		var out map[string]any
		require.NoError(t, json.Unmarshal(raw, &out))

		return out
	}

	data := func(out map[string]any) []any {
		res, _ := out["result"].(map[string]any)
		list, _ := res["data"].(map[string]any)
		items, _ := list["@value"].([]any)

		return items
	}

	t.Run("subgraph", func(t *testing.T) {
		t.Parallel()

		items := data(post(`g.E().hasLabel('knows').subgraph('sg').cap('sg')`))
		require.Len(t, items, 1)

		graph := items[0].(map[string]any)
		assert.Equal(t, "tinker:graph", graph["@type"])

		val := graph["@value"].(map[string]any)
		vertices := val["vertices"].([]any)
		edges := val["edges"].([]any)
		require.Len(t, vertices, 3)
		require.Len(t, edges, 2)

		v0 := vertices[0].(map[string]any)
		assert.Equal(t, "g:Vertex", v0["@type"])

		v0val := v0["@value"].(map[string]any)
		assert.Equal(t, "marko", v0val["id"])
		assert.Equal(t, "person", v0val["label"])
		assert.Contains(t, v0val["properties"], "name")

		e0 := edges[0].(map[string]any)
		assert.Equal(t, "g:Edge", e0["@type"])

		e0val := e0["@value"].(map[string]any)
		assert.Equal(t, "marko", e0val["outV"])
		assert.Equal(t, "knows", e0val["label"])
		assert.Contains(t, e0val["properties"], "weight")
	})

	t.Run("tree", func(t *testing.T) {
		t.Parallel()

		items := data(post(`g.V('marko').out('knows').tree().by('name')`))
		require.Len(t, items, 1)

		tree := items[0].(map[string]any)
		assert.Equal(t, "g:Tree", tree["@type"])

		entries := tree["@value"].([]any)
		require.Len(t, entries, 1)

		entry := entries[0].(map[string]any)
		assert.Equal(t, "marko", entry["key"])

		child := entry["value"].(map[string]any)
		assert.Equal(t, "g:Tree", child["@type"])
		assert.Len(t, child["@value"], 2)
	})
}
