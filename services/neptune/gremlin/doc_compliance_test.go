package gremlin_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune/gremlin"
)

func TestVertexPropertyCardinality(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		script string
		want   []string
	}{
		{"default adds", `g.V('lop').property('lang','go').values('lang')`, []string{"java", "go"}},
		{"default dedupes", `g.V('lop').property('lang','java').values('lang')`, []string{"java"}},
		{"set adds", `g.V('lop').property(set,'lang','go').values('lang')`, []string{"java", "go"}},
		{"single replaces", `g.V('lop').property(single,'lang','go').values('lang')`, []string{"go"}},
		{"single drops all", `g.V('lop').property('lang','go').property(single,'lang','rust').values('lang')`,
			[]string{"rust"}},
		{"qualified single", `g.V('lop').property(VertexProperty.Cardinality.single,'lang','go').values('lang')`,
			[]string{"go"}},
		{"edge replaces", `g.E().hasLabel('knows').limit(1).property('weight',9.0).values('weight')`, []string{"9"}},
		{"addV set default", `g.addV('n').property('a',1).property('a',2).values('a')`, []string{"1", "2"}},
		{"addV single", `g.addV('n').property('a',1).property(single,'a',2).values('a')`, []string{"2"}},
		{"addE property replaces", `g.V('marko').addE('x').to(V('lop')).property('w',1).property('w',2).values('w')`,
			[]string{"2"}},
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

func TestDocumentedFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		script string
		code   string
	}{
		{"list cardinality", `g.V('lop').property(list,'tag','x')`, gremlin.CodeMalformedQuery},
		{"qualified list cardinality", `g.V('lop').property(VertexProperty.Cardinality.list,'t','x')`,
			gremlin.CodeMalformedQuery},
		{"integer vertex id", `g.addV('a').property(T.id, 5)`, gremlin.CodeInvalidParameter},
		{"integer edge id", `g.V('marko').addE('x').to(V('lop')).property(T.id, 5)`, gremlin.CodeInvalidParameter},
		{"list property value", `g.V('lop').property('k',[1,2])`, gremlin.CodeMalformedQuery},
		{"map property value", `g.V('lop').property('k',[a:1])`, gremlin.CodeMalformedQuery},
		{"io write", `g.io('https://x.test/g.graphml').write()`, gremlin.CodeUnsupported},
		{"program", `g.V().program(1)`, gremlin.CodeUnsupported},
		{"tx", `g.tx().commit()`, gremlin.CodeUnsupported},
		{"lambda", `g.V().sideEffect{it.get()}`, gremlin.CodeUnsupported},
		{"from vertex", `g.V().addE('x').from(__.V().next()).to(__.V().next())`, gremlin.CodeUnsupported},
		{"variable assignment", `x = 1; g.V(x)`, gremlin.CodeMalformedQuery},
		{"arithmetic", `1 + 1`, gremlin.CodeMalformedQuery},
		{"duplicate label", `g.addV('person').property(T.id,'marko')`, gremlin.CodeConstraintViolation},
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

func TestAddVExistingIDAddsLabel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		script string
		want   []string
	}{
		{"label appended", `g.addV('label2').property(T.id,'c1').label()`, []string{"label1::label2"}},
		{"properties added", `g.addV('label2').property(T.id,'c1').property('p',1).values('p')`, []string{"1"}},
		{"existing property kept", `g.addV('label2').property(T.id,'c1').property('k','b').values('k')`,
			[]string{"a", "b"}},
		{"no new vertex", `g.addV('label2').property(T.id,'c1').V().hasId('c1').count()`, []string{"1"}},
		{"has second label", `g.addV('label2').property(T.id,'c1').V('c1').hasLabel('label2').count()`,
			[]string{"1"}},
		{
			"joined label matches nothing",
			`g.addV('label2').property(T.id,'c1').V('c1').hasLabel('label1::label2').count()`,
			[]string{"0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			g := seeded(t)
			_, err := evalAll(t, g, `g.addV('label1').property(T.id,'c1').property('k','a')`)
			require.NoError(t, err)

			got, err := evalAll(t, g, tt.script)
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestScriptStatementExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		script string
		count  string
		want   []string
	}{
		{"iterate runs", `g.addV('p').iterate(); g.V().count()`, "5", nil},
		{"next runs", "g.addV('p').next()\ng.V().count()", "5", nil},
		{"bare earlier statement skipped", `g.addV('p'); g.V().count()`, "4", nil},
		{"last result only", `g.V().iterate(); g.V().count()`, "4", nil},
		{"bare last returns", `g.V().count()`, "4", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := evalAll(t, seeded(t), tt.script)
			require.NoError(t, err)
			assert.Equal(t, []string{tt.count}, got)
		})
	}
}
