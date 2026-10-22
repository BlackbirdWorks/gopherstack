package gremlin_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune/gremlin"
)

func TestMoreTraversals(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		script string
		want   []string
	}{
		{"unfold_map", `g.V('lop').valueMap().unfold()`, []string{"{lang:[java]}", "{name:[lop]}"}},
		{"value_map_tokens", `g.V('lop').valueMap('name').with(WithOptions.tokens)`,
			[]string{"{id:lop,label:software,name:[lop]}"}},
		{"select_all_pop", `g.V('marko').as('a').out('knows').as('a').select(all,'a')`,
			[]string{"[v[marko],v[vadas]]", "[v[marko],v[josh]]"}},
		{"select_first", `g.V('marko').as('a').out('knows').as('a').select(first,'a').values('name')`,
			[]string{"marko", "marko"}},
		{"order_local", `g.V().hasLabel('person').values('age').fold().order(local).by(desc)`, []string{"[32,29,27]"}},
		{"where_by", `g.V().as('a').out('knows').as('b').where('a', gt('b')).by('age').count()`, []string{"0"}},
		{"mean", `g.V().hasLabel('person').values('age').mean()`, []string{"29.333333333333332"}},
		{"min_string", `g.V().values('name').min()`, []string{"josh"}},
		{"negative_number", `g.inject(-3, 2.5, 7L, 1.5f).fold()`, []string{"[-3,2.5,7,1.5]"}},
		{"list_literal", `g.inject([1,2,3]).unfold().sum()`, []string{"6"}},
		{"map_literal", `g.inject([a:1,b:'x'])`, []string{"{a:1,b:x}"}},
		{"string_escapes", `g.inject('it\'s', "tab\t")`, []string{"it's", "tab\t"}},
		{"comments_and_semicolons", "g.V().count(); // trailing\ng.E().count()", []string{"4"}},
		{"regex", `g.V().has('name', TextP.regex('^j.*h$')).values('name')`, []string{"josh"}},
		{"pred_and", `g.V().has('age', gt(26).and(lt(30))).values('name')`, []string{"marko", "vadas"}},
		{"pred_or", `g.V().has('age', P.lt(28).or(P.gt(31))).values('name')`, []string{"vadas", "josh"}},
		{"pred_negate", `g.V().has('age', P.eq(29).negate()).values('name')`, []string{"vadas", "josh"}},
		{"without", `g.V().hasLabel('person').has('name', without('marko','josh')).values('name')`, []string{"vadas"}},
		{"outside_inside", `g.V().has('age', outside(28, 31)).values('name')`, []string{"vadas", "josh"}},
		{"has_t_label", `g.V().has(T.label, 'software').id()`, []string{"lop"}},
		{"has_id_pred", `g.V().hasId(within('marko','lop')).id()`, []string{"marko", "lop"}},
		{"iterate", `g.V().iterate()`, []string{}},
		{"has_next", `g.V().hasNext()`, []string{"true"}},
		{"next_n", `g.V().id().next(2)`, []string{"marko", "vadas"}},
		{"to_set", `g.V().out().id().toSet()`, []string{"vadas", "josh", "lop"}},
		{"bare_anonymous", `g.V('marko').out('knows').where(out('created')).values('name')`, []string{"josh"}},
		{"optional", `g.V('vadas').optional(out('knows')).values('name')`, []string{"vadas"}},
		{"local_count", `g.V().hasLabel('person').local(out().count())`, []string{"3", "0", "1"}},
		{"simple_path", `g.V('marko').repeat(both().simplePath()).times(2).dedup().values('name')`,
			[]string{"lop", "josh"}},
		{
			"choose_option",
			`g.V().hasLabel('person').choose(values('name')).option('marko', constant('M')).option(none, constant('?'))`,
			[]string{"M", "?", "?"},
		},
		{"loops_until", `g.V('marko').repeat(out()).until(loops().is(2)).values('name')`, []string{"lop"}},
		{
			"until_first",
			`g.V('marko').until(hasLabel('software')).repeat(out()).values('name')`,
			[]string{"lop", "lop"},
		},
		{"emit_first", `g.V('marko').emit().repeat(out()).times(1).values('name')`,
			[]string{"marko", "vadas", "josh", "lop"}},
		{
			"group_default",
			`g.V().hasLabel('person').group().by(label)`,
			[]string{"{person:[v[marko],v[vadas],v[josh]]}"},
		},
		{"group_count_default", `g.V().out().groupCount().by('name')`, []string{"{vadas:1,josh:1,lop:2}"}},
		{"path_no_by", `g.V('marko').out('created').path()`, []string{"path[v[marko],v[lop]]"}},
		{"id_of_edge_vertices", `g.E().hasLabel('knows').inV().id()`, []string{"vadas", "josh"}},
		{"both_v", `g.E().hasLabel('created').limit(1).bothV().id()`, []string{"marko", "lop"}},
		{"properties_key", `g.V('lop').properties().key()`, []string{"lang", "name"}},
		{"properties_value", `g.V('lop').properties('name').value()`, []string{"lop"}},
		{"has_key_value", `g.V('lop').properties().hasKey('lang').value()`, []string{"java"}},
		{"element_map_edge", `g.E().hasLabel('created').limit(1).elementMap()`,
			[]string{"{id:%s,label:created,IN:{id:lop,label:software},OUT:{id:marko,label:person},weight:0.4}"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := evalAll(t, seeded(t), tt.script)
			require.NoError(t, err)

			if tt.name == "element_map_edge" {
				require.Len(t, got, 1)
				assert.Contains(
					t,
					got[0],
					"label:created,IN:{id:lop,label:software},OUT:{id:marko,label:person},weight:0.4}",
				)

				return
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		script string
	}{
		{"empty", ``},
		{"unterminated_string", `g.V().has('a`},
		{"bad_char", `g.V()#`},
		{"missing_paren", `g.V().has('a', 1`},
		{"dangling_dot", `g.V().`},
		{"unknown_variable", `g.V(nothere)`},
		{"deep_nesting", "g.V()" + deepNest(200)},
		{"bad_enum", `g.V().order().by(T.nothing)`},
		{"no_steps", `g`},
		{"bad_map", `g.inject([a:])`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := gremlin.ParseScript(tt.script, nil)
			require.Error(t, err)

			ge, ok := gremlin.AsError(err)
			require.True(t, ok)
			assert.Equal(t, gremlin.CodeMalformedQuery, ge.Code)
		})
	}
}

func deepNest(n int) string {
	return strings.Repeat(".not(__", n) + strings.Repeat(")", n)
}

func TestRepeatIsBounded(t *testing.T) {
	t.Parallel()

	g := seeded(t)

	_, err := evalAll(t, g, `g.V('marko').addE('self').to(g.V('marko'))`)
	require.NoError(t, err)

	_, err = evalAll(t, g, `g.V('marko').repeat(out('self')).until(hasLabel('software')).count()`)
	require.Error(t, err)

	ge, ok := gremlin.AsError(err)
	require.True(t, ok)
	assert.Equal(t, gremlin.CodeConstraintViolation, ge.Code)
}

func TestMutates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		script string
		want   bool
	}{
		{`g.V().count()`, false},
		{`g.V().drop()`, true},
		{`g.V().where(__.addV())`, true},
		{`g.V().union(out(), __.property('a',1))`, true},
		{`g.addE('x').from(g.V(1)).to(g.V(2))`, true},
	}

	for _, tt := range tests {
		stmts, err := gremlin.ParseScript(tt.script, nil)
		require.NoError(t, err, tt.script)
		assert.Equal(t, tt.want, stmts[0].Tr.Mutates(), tt.script)
	}
}
