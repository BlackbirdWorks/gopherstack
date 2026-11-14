package opencypher_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

func TestSyntaxErrorQuotesToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{"plain token", "RETURN 1 'ab'", "Invalid input 'ab'"},
		{"embedded quote", `RETURN 1 "it's"`, "Invalid input 'it''s'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := opencypher.NewGraph().Execute(t.Context(), tt.query, nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}
