package opencypher_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

func TestVariableLengthPropertyPredicate(t *testing.T) {
	t.Parallel()

	const msg = "Property predicate over variable-length relationships with non-constant expression " +
		"is not supported in this release."

	tests := []struct {
		params  map[string]any
		name    string
		query   string
		wantErr bool
	}{
		{name: "constant", query: "MATCH (x)-[:route*1..2 {dist:33}]->(y) RETURN x"},
		{name: "negative constant", query: "MATCH (x)-[:route*1..2 {dist:-1}]->(y) RETURN x"},
		{name: "parameter", query: "MATCH (x)-[:route*1..2 {dist:$d}]->(y) RETURN x", params: map[string]any{"d": 1}},
		{name: "list constant", query: "MATCH (x)-[:route*1..2 {dist:[1,2]}]->(y) RETURN x"},
		{
			name:    "property of node",
			query:   "MATCH (x)-[:route*1..2 {dist:33, code:x.name}]->(y) RETURN x",
			wantErr: true,
		},
		{
			name:    "function call",
			query:   "MATCH (x)-[:route*1..2 {dist:toInteger(3)}]->(y) RETURN x",
			wantErr: true,
		},
		{name: "fixed length allows it", query: "MATCH (x)-[:route {code:x.name}]->(y) RETURN x"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := opencypher.NewGraph().Execute(t.Context(), tt.query, tt.params)
			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			var ce *opencypher.Error

			require.ErrorAs(t, err, &ce)
			assert.Equal(t, opencypher.CodeUnsupported, ce.Code)
			assert.Contains(t, ce.Detail, msg)
		})
	}
}
