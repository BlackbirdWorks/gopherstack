package neptune_test

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"

	gremlingo "github.com/apache/tinkerpop/gremlin-go/v3/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/bolt"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/neptune"
)

func newEmbeddedBackend(t *testing.T) (*neptune.InMemoryBackend, neptune.DBCluster) {
	t.Helper()

	b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	b.EnableEngine(neptune.EngineConfig{InProcessGremlin: true, OpenCypher: true})
	t.Cleanup(b.Close)

	createEngineCluster(t, b, "1.3.0.0")

	return b, waitCluster(t, b, "c1", "available")
}

func clusterAddr(c neptune.DBCluster) string {
	return net.JoinHostPort(c.Endpoint, strconv.Itoa(c.Port))
}

func gremlinWS(c neptune.DBCluster) string { return "ws://" + clusterAddr(c) + "/gremlin" }

func gremlinG(t *testing.T, c neptune.DBCluster) *gremlingo.GraphTraversalSource {
	t.Helper()

	drc, err := gremlingo.NewDriverRemoteConnection(gremlinWS(c))
	require.NoError(t, err)
	t.Cleanup(drc.Close)

	return gremlingo.Traversal_().With(drc)
}

func gremlinClient(t *testing.T, c neptune.DBCluster) *gremlingo.Client {
	t.Helper()

	cl, err := gremlingo.NewClient(gremlinWS(c))
	require.NoError(t, err)
	t.Cleanup(cl.Close)

	return cl
}

func renderResult(r *gremlingo.Result) string {
	switch v := r.GetInterface().(type) {
	case *gremlingo.Vertex:
		return fmt.Sprintf("v[%v:%s]", v.Id, v.Label)
	case *gremlingo.Edge:
		return fmt.Sprintf("e[%s:%v->%v]", v.Label, v.OutV.Id, v.InV.Id)
	case *gremlingo.Path:
		parts := make([]string, len(v.Objects))
		for i, o := range v.Objects {
			parts[i] = fmt.Sprint(o)
		}

		return "path[" + strings.Join(parts, ",") + "]"
	case *gremlingo.Property:
		return fmt.Sprintf("p[%s=%v]", v.Key, v.Value)
	case *gremlingo.VertexProperty:
		return fmt.Sprintf("vp[%s=%v]", v.Label, v.Value)
	default:
		return fmt.Sprint(v)
	}
}

func renderAll(rs []*gremlingo.Result) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = renderResult(r)
	}

	return out
}

func seedGremlin(t *testing.T, g *gremlingo.GraphTraversalSource) {
	t.Helper()

	people := []struct {
		id   string
		name string
		age  int32
	}{{"marko", "marko", 29}, {"vadas", "vadas", 27}, {"josh", "josh", 32}}
	for _, p := range people {
		require.NoError(t, <-g.AddV("person").Property(gremlingo.T.Id, p.id).
			Property("name", p.name).Property("age", p.age).Iterate())
	}

	require.NoError(t, <-g.AddV("software").Property(gremlingo.T.Id, "lop").
		Property("name", "lop").Property("lang", "java").Iterate())

	for _, e := range []struct {
		from, label, to string
		weight          float64
	}{
		{"marko", "knows", "vadas", 0.5}, {"marko", "knows", "josh", 1.0},
		{"marko", "created", "lop", 0.4}, {"josh", "created", "lop", 0.4},
	} {
		require.NoError(t, <-g.V(e.from).AddE(e.label).To(gremlingo.T__.V(e.to)).Property("weight", e.weight).Iterate())
	}
}

func TestGremlinGoBytecode(t *testing.T) {
	t.Parallel()

	_, c := newEmbeddedBackend(t)
	g := gremlinG(t, c)
	seedGremlin(t, g)

	tests := []struct {
		name  string
		build func() *gremlingo.GraphTraversal
		want  []string
	}{
		{"count", func() *gremlingo.GraphTraversal { return g.V().Count() }, []string{"4"}},
		{"labels", func() *gremlingo.GraphTraversal { return g.V().HasLabel("person").Values("name") },
			[]string{"marko", "vadas", "josh"}},
		{"predicate", func() *gremlingo.GraphTraversal { return g.V().Has("age", gremlingo.P.Gt(28)).Values("name") },
			[]string{"marko", "josh"}},
		{"within", func() *gremlingo.GraphTraversal {
			return g.V().Has("name", gremlingo.P.Within("vadas", "lop")).Values("name")
		}, []string{"vadas", "lop"}},
		{"out", func() *gremlingo.GraphTraversal { return g.V("marko").Out("knows").Values("name") },
			[]string{"vadas", "josh"}},
		{"vertex", func() *gremlingo.GraphTraversal { return g.V("lop") }, []string{"v[lop:software]"}},
		{"edges", func() *gremlingo.GraphTraversal { return g.V("marko").OutE("created") },
			[]string{"e[created:marko->lop]"}},
		{"order", func() *gremlingo.GraphTraversal {
			return g.V().HasLabel("person").Order().By("age", gremlingo.Order.Desc).Values("name")
		}, []string{"josh", "marko", "vadas"}},
		{"group_count", func() *gremlingo.GraphTraversal { return g.V().GroupCount().By(gremlingo.T.Label) },
			[]string{"map[person:3 software:1]"}},
		{"value_map", func() *gremlingo.GraphTraversal { return g.V("lop").ValueMap("name") },
			[]string{"map[name:[lop]]"}},
		{"project", func() *gremlingo.GraphTraversal {
			return g.V("marko").Project("n", "c").By("name").By(gremlingo.T__.Out().Count())
		}, []string{"map[c:3 n:marko]"}},
		{"repeat", func() *gremlingo.GraphTraversal {
			return g.V("marko").Repeat(gremlingo.T__.Out()).Times(2).Values("name")
		}, []string{"lop"}},
		{"path", func() *gremlingo.GraphTraversal { return g.V("marko").Out("knows").Path().By("name") },
			[]string{"path[marko,vadas]", "path[marko,josh]"}},
		{"properties", func() *gremlingo.GraphTraversal { return g.V("lop").Properties("lang") },
			[]string{"vp[lang=java]"}},
		{"sum", func() *gremlingo.GraphTraversal { return g.E().Values("weight").Sum() }, []string{"2.3"}},
		{"fold", func() *gremlingo.GraphTraversal { return g.V("marko").Out("knows").Values("name").Fold() },
			[]string{"[vadas josh]"}},
		{"textp", func() *gremlingo.GraphTraversal {
			return g.V().Has("name", gremlingo.TextP.StartingWith("jo")).Values("name")
		}, []string{"josh"}},
		{"where", func() *gremlingo.GraphTraversal {
			return g.V().Where(gremlingo.T__.Out("created")).Values("name")
		}, []string{"marko", "josh"}},
		{"element_map", func() *gremlingo.GraphTraversal { return g.V("lop").ElementMap("lang") },
			[]string{"map[id:lop label:software lang:java]"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rs, err := tt.build().ToList()
			require.NoError(t, err)
			assert.Equal(t, tt.want, renderAll(rs))
		})
	}
}

func TestGremlinGoStringEval(t *testing.T) {
	t.Parallel()

	_, c := newEmbeddedBackend(t)
	cl := gremlinClient(t, c)

	tests := []struct {
		name     string
		script   string
		bindings map[string]any
		want     []string
	}{
		{"create", `g.addV('person').property(T.id,'a').property('name','alice').property('age',30)`,
			nil, []string{"v[a:person]"}},
		{"create_second", `g.addV('person').property(T.id,'b').property('name','bob')`, nil, []string{"v[b:person]"}},
		{"edge", `g.V('a').addE('knows').to(g.V('b')).property('since', 2020)`, nil, []string{"e[knows:a->b]"}},
		{"read", `g.V('a').out('knows').values('name')`, nil, []string{"bob"}},
		{"binding", `g.V(x).values('name')`, map[string]any{"x": "a"}, []string{"alice"}},
		{"count", `g.V().count()`, nil, []string{"2"}},
		{"empty", `g.V().has('name','nobody')`, nil, []string{}},
		{"group", `g.V().group().by('name').by(count())`, nil, []string{"map[alice:1 bob:1]"}},
	}

	for _, tt := range tests {
		rs, err := cl.Submit(tt.script, tt.bindings)
		require.NoError(t, err, tt.name)

		all, err := rs.All()
		require.NoError(t, err, tt.name)
		assert.Equal(t, tt.want, renderAll(all), tt.name)
	}
}

func TestGremlinGoErrors(t *testing.T) {
	t.Parallel()

	_, c := newEmbeddedBackend(t)
	cl := gremlinClient(t, c)
	g := gremlinG(t, c)

	tests := []struct {
		name   string
		script string
		want   string
	}{
		{"syntax", `g.V(`, "MalformedQueryException"},
		{"unknown_step", `g.V().nosuchstep()`, "MalformedQueryException"},
		{"bad_binding", `g.V(missing)`, "MalformedQueryException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rs, err := cl.Submit(tt.script)
			if err == nil {
				_, err = rs.All()
			}

			require.Error(t, err)
		})
	}

	require.NoError(t, <-g.AddV("x").Property(gremlingo.T.Id, "dup").Iterate())
	require.Error(t, <-g.AddV("x").Property(gremlingo.T.Id, "dup").Iterate())

	_, err := g.V().Out().Out().Values().Property("k", "v").ToList()
	require.NoError(t, err)
}

func TestGremlinCrossLanguage(t *testing.T) {
	t.Parallel()

	_, c := newEmbeddedBackend(t)
	g := gremlinG(t, c)
	base := "http://" + clusterAddr(c)

	require.NoError(t, <-g.AddV("Person").Property("name", "Ada").Property("age", int64(36)).Iterate())

	// Gremlin -> openCypher HTTP and Bolt.
	code, out := cypherDo(t, http.MethodGet, base+"/openCypher?"+url.Values{
		"query": {"MATCH (n:Person) RETURN n.name AS name, n.age AS age"},
	}.Encode(), "", "")
	require.Equal(t, http.StatusOK, code)

	rows, ok := out["results"].([]any)
	require.True(t, ok)
	require.Len(t, rows, 1)
	assert.Equal(t, map[string]any{"name": "Ada", "age": float64(36)}, rows[0])

	cl := boltDial(t, c, bolt.Version{Major: 5, Minor: 0})
	boltRows, _, err := cl.Query("MATCH (n:Person) RETURN n.name AS name", nil)
	require.NoError(t, err)
	assert.Equal(t, [][]any{{"Ada"}}, boltRows)

	// openCypher HTTP and Bolt -> Gremlin.
	code, _ = cypherDo(t, http.MethodPost, base+"/openCypher", "application/x-www-form-urlencoded",
		url.Values{"query": {"CREATE (:Person {name:'Grace', age: 40})"}}.Encode())
	require.Equal(t, http.StatusOK, code)

	_, _, err = cl.Query("CREATE (:Person {name:'Linus', age: 50})-[:KNOWS {since: 1991}]->(:Person {name:'Ken'})", nil)
	require.NoError(t, err)

	rs, err := g.V().HasLabel("Person").Order().By("name").Values("name").ToList()
	require.NoError(t, err)
	assert.Equal(t, []string{"Ada", "Grace", "Ken", "Linus"}, renderAll(rs))

	rs, err = g.V().Has("name", "Linus").OutE("KNOWS").Values("since").ToList()
	require.NoError(t, err)
	assert.Equal(t, []string{"1991"}, renderAll(rs))

	// Gremlin edge visible to openCypher.
	require.NoError(t, <-g.V().Has("name", "Ada").AddE("KNOWS").To(gremlingo.T__.V().Has("name", "Grace")).Iterate())

	rows2, _, err := cl.Query("MATCH (a {name:'Ada'})-[:KNOWS]->(b) RETURN b.name AS b", nil)
	require.NoError(t, err)
	assert.Equal(t, [][]any{{"Grace"}}, rows2)

	// Deleting through openCypher is seen by Gremlin.
	_, _, err = cl.Query("MATCH (n:Person {name:'Ken'}) DETACH DELETE n", nil)
	require.NoError(t, err)

	rs, err = g.V().HasLabel("Person").Count().ToList()
	require.NoError(t, err)
	assert.Equal(t, []string{"3"}, renderAll(rs))
}

func TestGremlinPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b, c := newEmbeddedBackend(t)
	g := gremlinG(t, c)
	seedGremlin(t, g)

	snap := b.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	b2.EnableEngine(neptune.EngineConfig{InProcessGremlin: true, OpenCypher: true})
	t.Cleanup(b2.Close)
	require.NoError(t, b2.Restore(t.Context(), snap))

	c2 := waitCluster(t, b2, "c1", "available")
	g2 := gremlinG(t, c2)

	rs, err := g2.V("marko").Out("knows").Values("name").ToList()
	require.NoError(t, err)
	assert.Equal(t, []string{"vadas", "josh"}, renderAll(rs))

	cl := boltDial(t, c2, bolt.Version{Major: 5, Minor: 0})
	rows, _, err := cl.Query("MATCH ()-[r:created]->() RETURN count(r) AS c", nil)
	require.NoError(t, err)
	assert.Equal(t, [][]any{{int64(2)}}, rows)
}

func TestGremlinClusterStopStart(t *testing.T) {
	t.Parallel()

	b, c := newEmbeddedBackend(t)
	g := gremlinG(t, c)
	require.NoError(t, <-g.AddV("Kept").Iterate())

	_, err := b.StopDBCluster(t.Context(), "c1")
	require.NoError(t, err)
	waitCluster(t, b, "c1", "stopped")

	_, err = b.StartDBCluster(t.Context(), "c1")
	require.NoError(t, err)
	c2 := waitCluster(t, b, "c1", "available")

	rs, err := gremlinG(t, c2).V().HasLabel("Kept").Count().ToList()
	require.NoError(t, err)
	assert.Equal(t, []string{"1"}, renderAll(rs))
}

type engineMode string

func (m engineMode) GetNeptuneEngine() string { return string(m) }

func TestProviderServesGremlinInProcess(t *testing.T) {
	t.Parallel()

	for _, mode := range []string{neptune.EngineEmbedded, neptune.EngineDocker} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()

			svc, err := (&neptune.Provider{}).Init(&service.AppContext{Config: engineMode(mode)})
			require.NoError(t, err)

			h, ok := svc.(*neptune.Handler)
			require.True(t, ok)
			t.Cleanup(func() { h.Shutdown(t.Context()) })

			b, ok := h.Backend.(*neptune.InMemoryBackend)
			require.True(t, ok)

			createEngineCluster(t, b, "1.3.0.0")
			c := waitCluster(t, b, "c1", "available")

			rs, err := gremlinClient(t, c).Submit("g.addV('x').count()")
			require.NoError(t, err)

			all, err := rs.All()
			require.NoError(t, err)
			assert.Equal(t, []string{"1"}, renderAll(all))
		})
	}
}
