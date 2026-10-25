package resourceexplorer2_test

import (
	_ "embed"
	"encoding/json"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//go:embed supported_types.json
var documentedTypes []byte

func TestDocumentedTypesCatalog(t *testing.T) {
	t.Parallel()

	var c struct {
		Source string   `json:"source"`
		Types  []string `json:"types"`
	}

	require.NoError(t, json.Unmarshal(documentedTypes, &c))

	tests := []struct {
		check func(t *testing.T)
		name  string
	}{
		{name: "cites_docs", check: func(t *testing.T) {
			t.Helper()
			assert.Equal(t,
				"https://docs.aws.amazon.com/resource-explorer/latest/userguide/supported-resource-types.html", c.Source)
		}},
		{name: "sorted_unique", check: func(t *testing.T) {
			t.Helper()
			assert.True(t, slices.IsSorted(c.Types))
			assert.Len(t, slices.Compact(slices.Clone(c.Types)), len(c.Types))
		}},
		{name: "service_prefixed", check: func(t *testing.T) {
			t.Helper()
			assert.GreaterOrEqual(t, len(c.Types), 600)

			for _, ty := range c.Types {
				assert.Regexp(t, `^[a-z0-9-]+:[A-Za-z0-9_./-]+$`, ty)
			}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.check(t)
		})
	}
}
