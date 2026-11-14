package gremlin_test

import (
	"context"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune/gremlin"
	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

func seededRand(a, b uint64) gremlin.Option {
	return gremlin.WithRand(func() *rand.Rand { return rand.New(rand.NewPCG(a, b)) })
}

func evalWith(t *testing.T, e *gremlin.Engine, g *opencypher.Graph, script string) ([]any, error) {
	t.Helper()

	return e.Eval(context.Background(), g, script, nil)
}

func evalRendered(t *testing.T, e *gremlin.Engine, g *opencypher.Graph, script string) ([]string, error) {
	t.Helper()

	res, err := evalWith(t, e, g, script)
	if err != nil {
		return nil, err
	}

	out := make([]string, len(res))
	for i, r := range res {
		out[i] = render(r)
	}

	return out, nil
}

func TestMathStep(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		script string
		want   []string
	}{
		{"add_current", `g.inject(4).math('_ + 1')`, []string{"5"}},
		{"result_is_double", `g.inject(4).math('_ * 2').map(constant(1)).count()`, []string{"1"}},
		{"by_values", `g.V().hasLabel('person').values('age').math('_ / 2')`, []string{"14.5", "13.5", "16"}},
		{"by_modulated_labels",
			`g.V('marko').as('a').out('knows').as('b').math('a - b').by('age')`, []string{"2", "-3"}},
		{"by_ring_cycles",
			`g.V('marko').as('a').out('knows').as('b').math('a + b').by('age').by('age')`, []string{"56", "61"}},
		{"map_keys",
			`g.V().hasLabel('person').project('x','y').by('age').by(constant(2)).math('x * y')`,
			[]string{"58", "54", "64"}},
		{"precedence", `g.inject(1).math('2 + 3 * 4')`, []string{"14"}},
		{"parens", `g.inject(1).math('(2 + 3) * 4')`, []string{"20"}},
		{"modulo", `g.inject(10).math('_ % 4')`, []string{"2"}},
		{"power_right_assoc", `g.inject(1).math('2 ^ 3 ^ 2')`, []string{"512"}},
		{"unary_minus_power", `g.inject(1).math('-2 ^ 2')`, []string{"-4"}},
		{"power_negative_exponent", `g.inject(2).math('_ ^ -1')`, []string{"0.5"}},
		{"sci_notation", `g.inject(1).math('1.5e2 + _')`, []string{"151"}},
		{"abs", `g.inject(-3).math('abs(_)')`, []string{"3"}},
		{"sqrt", `g.inject(9).math('sqrt(_)')`, []string{"3"}},
		{"cbrt", `g.inject(27).math('cbrt(_)')`, []string{"3"}},
		{"ceil_floor", `g.inject(2.5).math('ceil(_) * 10 + floor(_)')`, []string{"32"}},
		{"log2_log10", `g.inject(8).math('log2(_) + log10(100)')`, []string{"5"}},
		{"exp_log", `g.inject(0).math('exp(_) + log(1)')`, []string{"1"}},
		{"trig", `g.inject(0).math('sin(_) + cos(_) + tan(_)')`, []string{"1"}},
		{"hyperbolic", `g.inject(0).math('sinh(_) + cosh(_) + tanh(_)')`, []string{"1"}},
		{"inverse_trig", `g.inject(0).math('asin(_) + atan(_) + acos(1)')`, []string{"0"}},
		{"signum", `g.inject(-5, 0, 7).math('signum(_)')`, []string{"-1", "0", "1"}},
		{"divide_by_zero_is_inf", `g.inject(1).math('_ / 0')`, []string{"+Inf"}},
		{"whitespace", `g.inject(2).math('  _   *   3 ')`, []string{"6"}},
		{"path_label", `g.V('marko').values('age').as('n').math('n + 1')`, []string{"30"}},
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

func TestMathStepErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		script string
	}{
		{"dangling_operator", `g.inject(1).math('1 +')`},
		{"unknown_function", `g.inject(1).math('foo(1)')`},
		{"unbalanced", `g.inject(1).math('(1 + 2')`},
		{"trailing_garbage", `g.inject(1).math('1 2')`},
		{"empty", `g.inject(1).math('')`},
		{"missing_variable", `g.inject(1).math('x + 1')`},
		{"non_numeric_current", `g.V('marko').math('_ + 1')`},
		{"non_numeric_by", `g.V('marko').as('a').math('a').by('name')`},
		{"no_argument", `g.inject(1).math()`},
		{"bad_character", `g.inject(1).math('1 # 2')`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := evalAll(t, seeded(t), tt.script)
			require.Error(t, err)
		})
	}
}

func TestMathNaN(t *testing.T) {
	t.Parallel()

	res, err := gremlin.NewEngine().Eval(context.Background(), seeded(t), `g.inject(-1).math('sqrt(_)')`, nil)
	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.True(t, math.IsNaN(res[0].(float64)))
}

func TestSampleAndCoin(t *testing.T) {
	t.Parallel()

	e := gremlin.NewEngine(seededRand(7, 11))

	tests := []struct {
		name   string
		script string
		want   []string
	}{
		{"sample_count", `g.V().sample(2).count()`, []string{"2"}},
		{"sample_more_than_input", `g.V().sample(10).count()`, []string{"4"}},
		{"sample_zero", `g.V().sample(0).count()`, []string{"0"}},
		{"sample_weighted_picks_only_positive",
			`g.inject(0,0,5).sample(1).by(identity())`, []string{"5"}},
		{"sample_local_list", `g.inject([1,2,3,4,5]).sample(local, 2).count(local)`, []string{"2"}},
		{"sample_local_small", `g.inject([1,2]).sample(local, 5)`, []string{"[1,2]"}},
		{"sample_local_map", `g.V('marko').valueMap().sample(local, 1).count(local)`, []string{"1"}},
		{"sample_local_scalar", `g.inject(3).sample(local, 1)`, []string{"3"}},
		{"coin_zero", `g.V().coin(0.0).count()`, []string{"0"}},
		{"coin_one", `g.V().coin(1.0).count()`, []string{"4"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := evalRendered(t, e, seeded(t), tt.script)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSampleDeterministic(t *testing.T) {
	t.Parallel()

	scripts := []string{
		`g.V().sample(2).id()`,
		`g.V().coin(0.5).id()`,
		`g.inject([1,2,3,4,5,6,7,8]).sample(local, 3)`,
	}

	for _, script := range scripts {
		t.Run(script, func(t *testing.T) {
			t.Parallel()

			g := seeded(t)
			a, err := evalRendered(t, gremlin.NewEngine(seededRand(1, 2)), g, script)
			require.NoError(t, err)

			b, err := evalRendered(t, gremlin.NewEngine(seededRand(1, 2)), g, script)
			require.NoError(t, err)
			assert.Equal(t, a, b)
		})
	}
}

func TestSampleSubset(t *testing.T) {
	t.Parallel()

	for seed := range uint64(20) {
		g := seeded(t)
		got, err := evalRendered(t, gremlin.NewEngine(seededRand(seed, 3)), g, `g.V().sample(2).id()`)
		require.NoError(t, err)
		require.Len(t, got, 2)
		assert.NotEqual(t, got[0], got[1])
		assert.Subset(t, []string{"marko", "vadas", "josh", "lop"}, got)
	}
}

func TestSampleErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		script string
	}{
		{"no_size", `g.V().sample()`},
		{"negative", `g.V().sample(-1)`},
		{"non_numeric_weight", `g.V().sample(1).by('name')`},
		{"two_by", `g.V().sample(1).by('age').by('age')`},
		{"coin_no_arg", `g.V().coin()`},
		{"coin_string", `g.V().coin('x')`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := evalAll(t, seeded(t), tt.script)
			require.Error(t, err)
		})
	}
}

func TestTreeStep(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		script string
		want   []string
	}{
		{"one_level", `g.V('marko').out().tree().by('name')`, []string{"{marko:{vadas:{},josh:{},lop:{}}}"}},
		{"two_levels", `g.V().out().out().tree().by('name')`, []string{"{marko:{josh:{lop:{}}}}"}},
		{"by_cycles_per_depth", `g.V('marko').out().out().tree().by('name').by(label)`,
			[]string{"{marko:{person:{lop:{}}}}"}},
		{"side_effect_cap", `g.V('marko').out().tree('t').by('name').cap('t')`,
			[]string{"{marko:{vadas:{},josh:{},lop:{}}}"}},
		{"side_effect_passes_through", `g.V('marko').out().tree('t').by('name').values('name')`,
			[]string{"vadas", "josh", "lop"}},
		{"empty", `g.V('nobody').tree()`, []string{"{}"}},
		{"elements", `g.V('marko').out('knows').tree()`, []string{"{v[marko]:{v[vadas]:{},v[josh]:{}}}"}},
		{"edges_in_path", `g.V('marko').outE('created').inV().tree().by('name').by('weight').by('name')`,
			[]string{"{marko:{0.4:{lop:{}}}}"}},
		{"merged_roots", `g.V('marko','josh').out('created').tree().by('name')`,
			[]string{"{marko:{lop:{}},josh:{lop:{}}}"}},
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

func TestTreeErrors(t *testing.T) {
	t.Parallel()

	_, err := evalAll(t, seeded(t), `g.V().tree('a','b')`)
	require.Error(t, err)

	_, err = evalAll(t, seeded(t), `g.V().tree().by('nosuchprop')`)
	require.Error(t, err)
}

func TestSackStep(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		script string
		want   []string
	}{
		{"initial", `g.withSack(1).V('marko').sack()`, []string{"1"}},
		{"sum_by_property", `g.withSack(1).V('marko').outE('knows').sack(sum).by('weight').sack()`,
			[]string{"1.5", "2"}},
		{"propagates_through_steps", `g.withSack(1).V('marko').sack(sum).by(constant(1)).out('knows').sack()`,
			[]string{"2", "2"}},
		{"branches_independent",
			`g.withSack(0).V('marko').outE().sack(sum).by('weight').inV().path().by('name').by(label).by('name')`,
			[]string{"path[marko,knows,vadas]", "path[marko,knows,josh]", "path[marko,created,lop]"}},
		{"minus", `g.withSack(10).inject(4).sack(minus).sack()`, []string{"6"}},
		{"mult", `g.withSack(10).inject(4).sack(mult).sack()`, []string{"40"}},
		{"int_div", `g.withSack(10).inject(4).sack(div).sack()`, []string{"2"}},
		{"float_div", `g.withSack(10.0).inject(4).sack(div).sack()`, []string{"2.5"}},
		{"min", `g.withSack(10).inject(4).sack(min).sack()`, []string{"4"}},
		{"max", `g.withSack(10).inject(4).sack(max).sack()`, []string{"10"}},
		{"assign", `g.withSack(10).inject(4).sack(assign).by(constant(7)).sack()`, []string{"7"}},
		{"sum_long", `g.withSack(10).inject(4).sack(sumLong).sack()`, []string{"14"}},
		{"mixed_numeric", `g.withSack(1).inject(0.5).sack(sum).sack()`, []string{"1.5"}},
		{"qualified_operator", `g.withSack(2).inject(5).sack(Operator.mult).sack()`, []string{"10"}},
		{"and", `g.withSack(true).inject(false).sack(and).sack()`, []string{"false"}},
		{"or", `g.withSack(true).inject(false).sack(or).sack()`, []string{"true"}},
		{"add_all_lists", `g.withSack([1]).inject([2,3]).sack(addAll).sack()`, []string{"[1,2,3]"}},
		{"string_assign", `g.withSack('a').V('marko').sack(assign).by('name').sack()`, []string{"marko"}},
		{"order_by_sack",
			`g.withSack(0).V('marko').outE().sack(sum).by('weight').inV().order().by(sack()).values('name')`,
			[]string{"lop", "vadas", "josh"}},
		{"with_merge_operator", `g.withSack(1, sum).V('marko').sack()`, []string{"1"}},
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

func TestSackErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		script string
	}{
		{"no_with_sack", `g.V().sack()`},
		{"no_with_sack_operator", `g.V().sack(sum).by(constant(1))`},
		{"div_by_zero", `g.withSack(1).inject(1).sack(div).by(constant(0))`},
		{"non_numeric_sum", `g.withSack('a').inject(1).sack(sum)`},
		{"and_non_boolean", `g.withSack(1).inject(1).sack(and)`},
		{"add_all_mismatch", `g.withSack(1).inject([1]).sack(addAll)`},
		{"missing_by_value", `g.withSack(1).V('marko').sack(sum).by('nosuchprop')`},
		{"bad_argument", `g.withSack(1).V().sack('x')`},
		{"split_lambda_form", `g.withSack(1, 2)`},
		{"too_many_args", `g.withSack(1, sum, sum, sum).V()`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := evalAll(t, seeded(t), tt.script)
			require.Error(t, err)
		})
	}
}

func TestSackSourceParsed(t *testing.T) {
	t.Parallel()

	stmts, err := gremlin.ParseScript(`g.withSack(1.5).V()`, nil)
	require.NoError(t, err)
	require.Len(t, stmts[0].Tr.Sources, 1)
	assert.Equal(t, "withSack", stmts[0].Tr.Sources[0].Name)
	assert.Equal(t, []any{1.5}, stmts[0].Tr.Sources[0].Args)
}

func TestSubgraphStep(t *testing.T) {
	t.Parallel()

	g := seeded(t)

	res, err := evalWith(t, gremlin.NewEngine(), g, `g.E().hasLabel('knows').subgraph('sg').cap('sg')`)
	require.NoError(t, err)
	require.Len(t, res, 1)

	sg, ok := res[0].(*gremlin.SubGraph)
	require.True(t, ok)
	require.Len(t, sg.Vertices, 3)
	require.Len(t, sg.Edges, 2)

	assert.Equal(t, "marko", sg.Vertices[0].ID)
	assert.Equal(t, "person", sg.Vertices[0].Label)
	assert.Equal(t, "age", sg.Vertices[0].Props[0].Key)
	assert.Equal(t, []any{int64(29)}, sg.Vertices[0].Props[0].Vals)
	assert.Equal(t, "knows", sg.Edges[0].Edge.Label)
	assert.Equal(t, "marko", sg.Edges[0].Edge.OutID)
	assert.Equal(t, "vadas", sg.Edges[0].Edge.InID)
	assert.Equal(t, "weight", sg.Edges[0].Props[0].Key)
	assert.InDelta(t, 0.5, sg.Edges[0].Props[0].Vals[0], 1e-9)
}

func TestSubgraphTraversals(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		script string
		want   []string
	}{
		{"cap", `g.V('marko').outE('created').subgraph('sg').cap('sg')`, []string{"graph[v=2,e=1]"}},
		{"dedups_shared_vertices", `g.E().subgraph('sg').cap('sg')`, []string{"graph[v=4,e=4]"}},
		{"passes_edges_through", `g.E().hasLabel('knows').subgraph('sg').count()`, []string{"2"}},
		{"after_traversal", `g.V('marko').outE().subgraph('sg').inV().outE().subgraph('sg').cap('sg')`,
			[]string{"graph[v=4,e=4]"}},
		{"multi_key_cap", `g.E().hasLabel('knows').subgraph('a').inV().tree('t').cap('a','t')`,
			[]string{"{a:graph[v=3,e=2],t:{e[knows]:{v[vadas]:{}},e[knows]:{v[josh]:{}}}}"}},
		{"unfilled_key", `g.V().cap('sg')`, []string{"[]"}},
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

func TestSubgraphErrors(t *testing.T) {
	t.Parallel()

	for _, script := range []string{`g.V().subgraph('sg')`, `g.E().subgraph()`, `g.E().subgraph('a','b')`, `g.V().cap()`} {
		_, err := evalAll(t, seeded(t), script)
		require.Error(t, err, script)
	}
}

func TestIndexStep(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		script string
		want   []string
	}{
		{"list_default", `g.V().hasLabel('software').values('name').fold().index()`, []string{"[[lop,0]]"}},
		{"unfold_pairs", `g.V().hasLabel('person').values('name').fold().index().unfold()`,
			[]string{"[marko,0]", "[vadas,1]", "[josh,2]"}},
		{
			"map_mode",
			`g.V().hasLabel('person').values('name').fold().index().with(WithOptions.indexer, WithOptions.map)`,
			[]string{"{0:marko,1:vadas,2:josh}"},
		},
		{"list_mode_explicit",
			`g.inject([5,6]).index().with(WithOptions.indexer, WithOptions.list)`, []string{"[[5,0],[6,1]]"}},
		{"wire_form", `g.inject([5,6]).index().with('~tinkerpop.index.indexer', 1)`, []string{"{0:5,1:6}"}},
		{"scalar", `g.inject(9).index()`, []string{"[[9,0]]"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := evalAll(t, seeded(t), tt.script)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	_, err := evalAll(t, seeded(t), `g.inject([1]).index().with(WithOptions.indexer, 5)`)
	require.Error(t, err)
}

func TestQueriesShareEngineRace(t *testing.T) {
	t.Parallel()

	e := gremlin.NewEngine()
	g := seeded(t)

	for i := range 8 {
		t.Run("q", func(t *testing.T) {
			t.Parallel()

			script := `g.V().sample(2).tree().by('name')`
			if i%2 == 0 {
				script = `g.withSack(1).V().coin(0.5).sack(sum).by(constant(1)).math('_ + 1').by(sack())`
			}

			_, _ = evalWith(t, e, g, script)
		})
	}
}
