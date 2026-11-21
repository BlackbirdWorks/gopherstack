package gremlin_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune/gremlin"
)

func TestMetaPropertiesAreNotPersisted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		script string
		check  string
		want   []string
	}{
		{"property meta pair dropped", `g.V('lop').property('lang','go','src','x')`,
			`g.V('lop').properties('lang').values('src')`, nil},
		{"property meta keeps value", `g.V('lop').property(single,'lang','go','src','x')`,
			`g.V('lop').values('lang')`, []string{"go"}},
		{"property on vertex property", `g.V('lop').properties('lang').property('src','x')`,
			`g.V('lop').properties('lang').properties()`, nil},
		{"properties of vertex property", `g.V('lop').properties('lang').count()`,
			`g.V('lop').properties('lang').properties().count()`, []string{"0"}},
		{"valueMap of vertex property", `g.V('lop').properties('lang').count()`,
			`g.V('lop').properties('lang').valueMap()`, []string{"{}"}},
		{"valueMap tokens of vertex property", `g.V('lop').properties('lang').count()`,
			`g.V('lop').properties('lang').valueMap(true).unfold().count()`, []string{"3"}},
		{"addV meta pair dropped", `g.addV('x').property(T.id,'m1').property('a',1,'b',2)`,
			`g.V('m1').properties('a').properties().count()`, []string{"0"}},
		{"has on vertex property meta", `g.V('lop').property('lang','go','src','x')`,
			`g.V('lop').properties('lang').has('src','x').count()`, []string{"0"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			g := seeded(t)
			_, err := evalAll(t, g, tt.script)
			require.NoError(t, err)

			got, err := evalAll(t, g, tt.check)
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestMetaPropertyArgumentErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		script string
		code   string
	}{
		{"odd key value array", `g.V('lop').property('lang','go','src')`, gremlin.CodeMalformedQuery},
		{"odd on addV", `g.addV('x').property('a',1,'b')`, gremlin.CodeMalformedQuery},
		{"edge property has no properties", `g.E().properties('weight').properties()`, gremlin.CodeMalformedQuery},
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
