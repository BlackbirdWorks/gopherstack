package gremlin_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/neptune/gremlin"
)

func TestScriptAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		script string
		want   gremlin.Access
	}{
		{"read", "g.V().has('a',1).count()", gremlin.Access{}},
		{"add_vertex", "g.addV('p')", gremlin.Access{Write: true}},
		{"drop", "g.V().drop()", gremlin.Access{Delete: true}},
		{"property", "g.V('1').property('k','v')", gremlin.Access{Write: true, Delete: true}},
		{"nested", "g.V().where(__.addE('x').to(__.V()))", gremlin.Access{Write: true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, gremlin.ScriptAccess(tt.script))
		})
	}
}
