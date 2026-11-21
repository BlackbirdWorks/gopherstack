package gremlin_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune/gremlin"
)

func TestMultiValuedPropertyOps(t *testing.T) {
	t.Parallel()

	const setup = `g.addV('n').property(T.id,'z').property('a',1).property('a',2).iterate(); `

	tests := []struct {
		name   string
		script string
		want   []string
	}{
		{"drop one value", setup + `g.V('z').properties('a').hasValue(1).drop().iterate(); g.V('z').values('a')`,
			[]string{"2"}},
		{"drop other value", setup + `g.V('z').properties('a').hasValue(2).drop().iterate(); g.V('z').values('a')`,
			[]string{"1"}},
		{"drop last value removes key", setup +
			`g.V('z').properties('a').drop().iterate(); g.V('z').properties('a').count()`, []string{"0"}},
		{"dedup keeps distinct values", setup + `g.V('z').properties('a').dedup().count()`, []string{"2"}},
		{"barrier keeps distinct values", setup + `g.V('z').properties('a').barrier().value()`,
			[]string{"1", "2"}},
		{"int and double are distinct", `g.addV('n').property('a',1).property('a',1.0).properties('a').count()`,
			[]string{"2"}},
		{"same int is a no-op", `g.addV('n').property('a',1).property('a',1).properties('a').count()`,
			[]string{"1"}},
		{"has matches any value", setup + `g.V().has('a',2).count()`, []string{"1"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := evalAll(t, seeded(t), tt.script)
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestBarrierMergeKeyIsInjective(t *testing.T) {
	t.Parallel()

	const script = `g.withSack('?').inject('a|0|s:x','a')` +
		`.sack(assign).by(choose(is('a'),constant('x|0|s:y'),constant('y'))).barrier()`

	got, err := evalAll(t, seeded(t), script)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a|0|s:x", "a"}, got)

	got, err = evalAll(t, seeded(t), `g.inject(['a,s:b'],['a','b']).barrier()`)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"[a,s:b]", "[a,b]"}, got)
}

func TestBarrierIsTransparent(t *testing.T) {
	t.Parallel()

	sources := []string{
		`g.V()`, `g.V().out()`, `g.V().both()`, `g.V().both().both()`, `g.V().out().in()`,
		`g.inject(1,1,2,3,3,3)`, `g.inject('a','a','b')`, `g.V().hasLabel('person').outE().inV()`,
		`g.withSack(1).V().both()`, `g.withSack(1).inject(2,2,5)`,
	}
	tails := []string{
		``, `.count()`, `.fold()`, `.groupCount()`, `.dedup()`, `.dedup().count()`, `.id()`,
		`.order().limit(3)`, `.order().range(1,4)`, `.order().tail(2)`, `.limit(2).count()`,
		`.path().count(local)`, `.as('a').select('a')`, `.repeat(identity()).times(2)`,
		`.union(identity(),identity())`, `.where(identity())`, `.store('x').cap('x')`, `.aggregate('x').cap('x')`,
		`.group().by(identity()).by(count())`, `.map(constant(7))`, `.coin(1.0)`, `.not(has('zz'))`,
		`.groupCount().by(label)`, `.sack()`, `.simplePath().count()`, `.fold().count(local)`,
		`.sum()`, `.mean()`, `.max()`, `.min()`, `.order().by(id)`, `.values('name')`, `.valueMap('name')`,
		`.project('k').by(constant(1))`, `.tree()`, `.flatMap(identity())`,
	}

	type pair struct{ plain, barrier string }

	cases := make([]pair, 0, 3*len(tails)*len(sources))

	for _, s := range sources {
		for _, tl := range tails {
			cases = append(cases,
				pair{s + tl, s + `.barrier()` + tl},
				pair{s + tl, s + `.barrier(2)` + tl},
				pair{s + tl, s + `.barrier().barrier()` + tl},
			)
		}
	}

	for _, c := range cases {
		t.Run(c.barrier, func(t *testing.T) {
			t.Parallel()

			want, werr := evalAll(t, seeded(t), c.plain)
			got, gerr := evalAll(t, seeded(t), c.barrier)

			if werr != nil {
				require.Error(t, gerr)

				return
			}

			require.NoError(t, gerr)
			assert.ElementsMatch(t, canonical(want), canonical(got))
		})
	}
}

// canonical sorts the bytes of each result so a fold's element order does not matter.
func canonical(in []string) []string {
	out := make([]string, len(in))

	for i, s := range in {
		b := []byte(s)
		slices.Sort(b)
		out[i] = string(b)
	}

	return out
}

func TestBarrierHostileInput(t *testing.T) {
	t.Parallel()

	nested := func(n int) string {
		return `g.V()` + strings.Repeat(`.union(identity()`, n) + strings.Repeat(`)`, n)
	}

	tests := []struct {
		name    string
		script  string
		wantErr bool
	}{
		{"huge size", `g.inject(1,1).barrier(4611686018427387904).count()`, false},
		{"negative size", `g.inject(1,1).barrier(-5)`, true},
		{"zero size", `g.inject(1,1).barrier(0)`, true},
		{"zero total sack", `g.withSack(0).inject(1,1).barrier(normSack).sack()`, false},
		{"string sack", `g.withSack('x').inject(1).barrier(normSack)`, true},
		{"null sack", `g.withSack(1).inject(1).sack(assign).by(constant('z')).barrier(normSack)`, true},
		{"deep nesting", nested(500), true},
		{"barrier in nested", `g.V().local(barrier().count())`, false},
		{"barrier repeat", `g.V().repeat(both().barrier()).times(6).count()`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.NotPanics(t, func() {
				_, err := evalAll(t, seeded(t), tt.script)
				if tt.wantErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			})
		})
	}
}

func TestWebSocketDateProperty(t *testing.T) {
	t.Parallel()

	c := dial(t, newServer(t, seeded(t)))
	body := `{"requestId":"33333333-3333-3333-3333-333333333333","op":"bytecode","processor":"traversal","args":{` +
		`"gremlin":{"@type":"g:Bytecode","@value":{"step":[["addV","d"],["property","t",` +
		`{"@type":"g:Date","@value":1700000000123}],["values","t"]]}},"aliases":{"g":"g"}}}`

	resp := call(t, c, websocket.BinaryMessage, frame("application/vnd.gremlin-v3.0+json", body))
	require.Len(t, resp, 1)
	assert.Equal(t, 200, resp[0].Status.Code)
	assert.JSONEq(t, `{"@type":"g:List","@value":[{"@type":"g:Date","@value":1700000000123}]}`,
		string(resp[0].Result.Data))
}

func TestWebSocketRejectedPropertyValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
		code  int
	}{
		{"uuid", `{"@type":"g:UUID","@value":"48af8178-50ce-971a-fc41-8c9a954cea62"}`, gremlin.StatusScriptEvalError},
		{"map", `{"@type":"g:Map","@value":["a",1]}`, gremlin.StatusScriptEvalError},
		{"null", `null`, gremlin.StatusScriptEvalError},
		{"date", `{"@type":"g:Date","@value":1}`, gremlin.StatusSuccess},
		{"short", `{"@type":"gx:Int16","@value":1}`, gremlin.StatusSuccess},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := dial(t, newServer(t, seeded(t)))
			body := `{"requestId":"44444444-4444-4444-4444-444444444444","op":"bytecode","processor":"traversal",` +
				`"args":{"gremlin":{"@type":"g:Bytecode","@value":{"step":[["addV","d"],["property","k",` +
				tt.value + `]]}},"aliases":{"g":"g"}}}`

			resp := call(t, c, websocket.BinaryMessage, frame("application/vnd.gremlin-v3.0+json", body))
			require.Len(t, resp, 1)
			assert.Equal(t, tt.code, resp[0].Status.Code, resp[0].Status.Message)
		})
	}
}
