package gremlin_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune/gremlin"
)

func TestTraverserBulk(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		script string
		want   []string
	}{
		{"barrier keeps count", `g.V().both().barrier().count()`, []string{"8"}},
		{"barrier emits every result", `g.V('lop').in().barrier().values('name')`,
			[]string{"marko", "josh"}},
		{"barrier merges duplicates", `g.V('marko').out().out().barrier().count()`, []string{"1"}},
		{"bulked emission", `g.inject(1,1,2).barrier()`, []string{"1", "1", "2"}},
		{"bulked fold", `g.inject(1,1,2).barrier().fold()`, []string{"[1,1,2]"}},
		{"bulked groupCount", `g.inject('a','a','b').barrier().groupCount()`, []string{"{a:2,b:1}"}},
		{"bulked dedup", `g.inject('a','a','b').barrier().dedup()`, []string{"a", "b"}},
		{"bulked dedup count", `g.inject('a','a','b').barrier().dedup().count()`, []string{"2"}},
		{"bulked sum", `g.inject(1,1,2).barrier().sum()`, []string{"4"}},
		{"bulked count local", `g.inject([1,2],[1,2]).barrier().count(local)`, []string{"2", "2"}},
		{"bulked map", `g.inject(1,1).barrier().map(constant(7))`, []string{"7", "7"}},
		{"barrier size", `g.inject(1,1,1,1).barrier(2).count()`, []string{"4"}},
		{"merge sums sacks", `g.withSack(1,sum).inject(1,1,2).barrier().sack()`, []string{"2", "2", "1"}},
		{"merge sums sacks count", `g.withSack(1,sum).inject(1,1,2).barrier().count()`, []string{"3"}},
		{"no merge op separates sacks",
			`g.withSack(1).inject(1,1).sack(sum).by(constant(1)).barrier().sack()`, []string{"2", "2"}},
		{"max merge", `g.withSack(1,max).inject(5,5).sack(sum).by(identity()).barrier().sack()`,
			[]string{"6", "6"}},
		{"path keeps traversers apart", `g.inject(1,1).as('a').barrier().path().count(local)`,
			[]string{"1", "1"}},
		{"normSack", `g.withSack(1.0).inject('a','b').sack(mult).by(constant(2.0)).barrier(normSack).sack()`,
			[]string{"0.5", "0.5"}},
		{"normSack weighted", `g.withSack(1.0,sum).inject('a','a','b').barrier(normSack).sack()`,
			[]string{"0.4", "0.4", "0.2"}},
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

func TestBarrierErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		script string
		code   string
	}{
		{"normSack without sack", `g.V().barrier(normSack)`, gremlin.CodeMalformedQuery},
		{"non numeric sack", `g.withSack('x').V().barrier(normSack)`, gremlin.CodeMalformedQuery},
		{"bad argument", `g.V().barrier('x')`, gremlin.CodeInvalidParameter},
		{"too many arguments", `g.V().barrier(1,2)`, gremlin.CodeInvalidParameter},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := evalAll(t, seeded(t), tt.script)
			require.Error(t, err)

			ge, ok := gremlin.AsError(err)
			require.True(t, ok)
			assert.Equal(t, tt.code, ge.Code)
		})
	}
}
