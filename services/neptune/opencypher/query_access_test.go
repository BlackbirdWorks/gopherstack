package opencypher_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

func TestQueryAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  opencypher.Access
	}{
		{"read", "MATCH (n) RETURN n", opencypher.Access{}},
		{"create", "CREATE (n:T) RETURN n", opencypher.Access{Write: true}},
		{"merge", "MERGE (n:T {a:1}) RETURN n", opencypher.Access{Write: true}},
		{"set", "MATCH (n) SET n.a = 1 RETURN n", opencypher.Access{Write: true, Delete: true}},
		{"detach_delete", "MATCH (n) DETACH DELETE n", opencypher.Access{Delete: true}},
		{"unparsable", "NOT CYPHER", opencypher.Access{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, opencypher.QueryAccess(tt.query))
		})
	}
}
