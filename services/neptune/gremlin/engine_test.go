package gremlin_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune/gremlin"
	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

func render(v any) string {
	switch t := v.(type) {
	case *gremlin.Vertex:
		return "v[" + t.ID + "]"
	case *gremlin.Edge:
		return "e[" + t.Label + "]"
	case *gremlin.Property:
		return fmt.Sprintf("p[%s=%s]", t.Key, render(t.Val))
	case []any:
		parts := make([]string, len(t))
		for i, e := range t {
			parts[i] = render(e)
		}

		return "[" + strings.Join(parts, ",") + "]"
	case gremlin.Set:
		return "set" + render([]any(t))
	case *gremlin.Map:
		parts := make([]string, t.Len())

		for i := range parts {
			k, val := t.Entry(i)
			ks := render(k)

			if tok, ok := k.(gremlin.Token); ok {
				ks = tok.Name
			}

			parts[i] = ks + ":" + render(val)
		}

		return "{" + strings.Join(parts, ",") + "}"
	case *gremlin.Path:
		return "path" + render(t.Objects)
	default:
		return fmt.Sprint(v)
	}
}

func evalAll(t *testing.T, g *opencypher.Graph, script string) ([]string, error) {
	t.Helper()

	res, err := gremlin.NewEngine().Eval(context.Background(), g, script, nil)
	if err != nil {
		return nil, err
	}

	out := make([]string, len(res))
	for i, r := range res {
		out[i] = render(r)
	}

	return out, nil
}

const seed = `g.addV('person').property(T.id,'marko').property('name','marko').property('age',29).next();
g.addV('person').property(T.id,'vadas').property('name','vadas').property('age',27).next();
g.addV('person').property(T.id,'josh').property('name','josh').property('age',32).next();
g.addV('software').property(T.id,'lop').property('name','lop').property('lang','java').next();
g.V('marko').addE('knows').to(g.V('vadas')).property('weight',0.5).next();
g.V('marko').addE('knows').to(g.V('josh')).property('weight',1.0).next();
g.V('marko').addE('created').to(g.V('lop')).property('weight',0.4).next();
g.V('josh').addE('created').to(g.V('lop')).property('weight',0.4).next()`

func seeded(t *testing.T) *opencypher.Graph {
	t.Helper()

	g := opencypher.NewGraph()
	_, err := evalAll(t, g, seed)
	require.NoError(t, err)

	return g
}

func TestTraversals(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		script string
		want   []string
	}{
		{"count", `g.V().count()`, []string{"4"}},
		{"edge_count", `g.E().count()`, []string{"4"}},
		{"has_label_values", `g.V().hasLabel('person').values('name')`, []string{"marko", "vadas", "josh"}},
		{"has_pred", `g.V().has('age', gt(28)).values('name')`, []string{"marko", "josh"}},
		{"has_within", `g.V().has('name', within('vadas','lop')).values('name')`, []string{"vadas", "lop"}},
		{"has_label_key_value", `g.V().has('person','name','josh').id()`, []string{"josh"}},
		{"out", `g.V('marko').out('knows').values('name')`, []string{"vadas", "josh"}},
		{"out_all", `g.V('marko').out().count()`, []string{"3"}},
		{"in", `g.V('lop').in('created').values('name')`, []string{"marko", "josh"}},
		{"both", `g.V('josh').both().values('name')`, []string{"lop", "marko"}},
		{"oute_inv", `g.V('marko').outE('knows').inV().values('name')`, []string{"vadas", "josh"}},
		{"other_v", `g.V('josh').bothE().otherV().values('name')`, []string{"lop", "marko"}},
		{"order_by_desc", `g.V().hasLabel('person').order().by('age', desc).values('name')`,
			[]string{"josh", "marko", "vadas"}},
		{"limit_range", `g.V().hasLabel('person').order().by('age').range(1,3).values('name')`,
			[]string{"marko", "josh"}},
		{"dedup", `g.V().out().dedup().count()`, []string{"3"}},
		{"group_count", `g.V().groupCount().by(label)`, []string{"{person:3,software:1}"}},
		{"group", `g.V().hasLabel('person').group().by('age').by('name')`,
			[]string{"{29:[marko],27:[vadas],32:[josh]}"}},
		{"value_map", `g.V('marko').valueMap('name','age')`, []string{"{age:[29],name:[marko]}"}},
		{"element_map", `g.V('lop').elementMap()`, []string{"{id:lop,label:software,lang:java,name:lop}"}},
		{"project", `g.V('marko').project('n','c').by('name').by(out().count())`, []string{"{n:marko,c:3}"}},
		{"fold_unfold", `g.V().hasLabel('person').values('age').fold()`, []string{"[29,27,32]"}},
		{"sum_mean", `g.V().hasLabel('person').values('age').sum()`, []string{"88"}},
		{"max_min", `g.V().hasLabel('person').values('age').max()`, []string{"32"}},
		{"where_traversal", `g.V().where(out('created')).values('name')`, []string{"marko", "josh"}},
		{"not", `g.V().hasLabel('person').not(out('created')).values('name')`, []string{"vadas"}},
		{"and_or", `g.V().or(has('name','lop'), and(has('age', lt(30)), hasLabel('person'))).values('name')`,
			[]string{"marko", "vadas", "lop"}},
		{"as_select", `g.V('marko').as('a').out('knows').as('b').select('a','b').by('name')`,
			[]string{"{a:marko,b:vadas}", "{a:marko,b:josh}"}},
		{
			"where_label",
			`g.V().as('a').out('created').in('created').as('b').where('a', neq('b')).select('a','b').by('name')`,
			[]string{"{a:marko,b:josh}", "{a:josh,b:marko}"},
		},
		{"path", `g.V('marko').out('knows').path().by('name')`, []string{"path[marko,vadas]", "path[marko,josh]"}},
		{"repeat_times", `g.V('marko').repeat(out()).times(2).values('name')`, []string{"lop"}},
		{
			"repeat_until",
			`g.V('marko').repeat(out()).until(hasLabel('software')).values('name')`,
			[]string{"lop", "lop"},
		},
		{"repeat_emit", `g.V('marko').repeat(out()).times(2).emit().values('name')`,
			[]string{"vadas", "josh", "lop", "lop"}},
		{"coalesce", `g.V().coalesce(values('lang'), values('name')).dedup()`,
			[]string{"marko", "vadas", "josh", "java"}},
		{"constant", `g.V('lop').constant('x')`, []string{"x"}},
		{"choose", `g.V().hasLabel('person').choose(has('age', gt(28)), constant('old'), constant('young'))`,
			[]string{"old", "young", "old"}},
		{"union", `g.V('marko').union(out('knows'), out('created')).count()`, []string{"3"}},
		{"textp", `g.V().has('name', TextP.startingWith('jo')).values('name')`, []string{"josh"}},
		{"between", `g.V().has('age', between(27, 30)).values('name')`, []string{"marko", "vadas"}},
		{"inject", `g.inject(1,2,3).sum()`, []string{"6"}},
		{"properties", `g.V('lop').properties('lang')`, []string{"p[lang=java]"}},
		{"edge_values", `g.E().hasLabel('created').values('weight').sum()`, []string{"0.8"}},
		{"label_step", `g.V('marko').outE().label().dedup()`, []string{"knows", "created"}},
		{"count_local", `g.V('marko').out().fold().count(local)`, []string{"3"}},
		{"has_not", `g.V().hasNot('age').values('name')`, []string{"lop"}},
		{"has_id", `g.V().hasId('josh', 'lop').values('name')`, []string{"josh", "lop"}},
		{"tail_skip", `g.V().hasLabel('person').skip(1).values('name')`, []string{"vadas", "josh"}},
		{"side_effect_cap", `g.V().hasLabel('person').aggregate('x').cap('x').count(local)`, []string{"3"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := evalAll(t, seeded(t), tt.script)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMutations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		script string
		check  string
		want   []string
	}{
		{"add_property", `g.V('lop').property('lang','go')`, `g.V('lop').values('lang')`, []string{"go"}},
		{"drop_vertex", `g.V('vadas').drop()`, `g.V().count()`, []string{"3"}},
		{"drop_cascades", `g.V('lop').drop()`, `g.E().count()`, []string{"2"}},
		{"drop_property", `g.V('marko').properties('age').drop()`, `g.V('marko').values('age')`, []string{}},
		{"drop_edges", `g.E().hasLabel('knows').drop()`, `g.E().count()`, []string{"2"}},
		{"multi_label", `g.addV('a::b').property(T.id,'ab')`, `g.V().hasLabel('b').id()`, []string{"ab"}},
		{"list_cardinality", `g.V('lop').property(list,'tag','x').property(list,'tag','y')`,
			`g.V('lop').values('tag')`, []string{"x", "y"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			g := seeded(t)
			_, err := evalAll(t, g, tt.script)
			require.NoError(t, err)

			got, err := evalAll(t, g, tt.check)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		script string
		code   string
	}{
		{"unknown_step", `g.V().frobnicate()`, gremlin.CodeMalformedQuery},
		{"syntax", `g.V(`, gremlin.CodeMalformedQuery},
		{"bad_start", `x.V()`, gremlin.CodeMalformedQuery},
		{"duplicate_id", `g.addV('a').property(T.id,'marko')`, gremlin.CodeConstraintViolation},
		{"out_on_edge", `g.E().out()`, gremlin.CodeMalformedQuery},
		{"null_property", `g.V('lop').property('x', null)`, gremlin.CodeMalformedQuery},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := evalAll(t, seeded(t), tt.script)
			require.Error(t, err)

			ge, ok := gremlin.AsError(err)
			require.True(t, ok, err.Error())
			assert.Equal(t, tt.code, ge.Code)
		})
	}
}

func TestFailedMutationRollsBack(t *testing.T) {
	t.Parallel()

	g := seeded(t)
	_, err := evalAll(t, g, `g.addV('x').property(T.id,'new').next(); g.V().frobnicate()`)
	require.Error(t, err)

	got, err := evalAll(t, g, `g.V('new').count()`)
	require.NoError(t, err)
	assert.Equal(t, []string{"0"}, got)
}

func TestSharedWithOpenCypher(t *testing.T) {
	t.Parallel()

	g := seeded(t)

	res, err := g.Execute(context.Background(), `MATCH (n:person) RETURN n.name AS name ORDER BY name`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 3)

	_, err = g.Execute(context.Background(), `CREATE (:person {name:'peter', age: 35})`, nil)
	require.NoError(t, err)

	got, err := evalAll(t, g, `g.V().has('name','peter').values('age')`)
	require.NoError(t, err)
	assert.Equal(t, []string{"35"}, got)
}
