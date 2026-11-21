package azurearm_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogicListSwagger(t *testing.T) {
	t.Parallel()

	schema := la{"type": "object", "properties": la{"name": la{"type": "string"}}}

	tests := []struct {
		triggers la
		name     string
		wantPath string
		wantVerb string
		wantPrm  []string
		wantBody bool
		none     bool
	}{
		{
			name: "post with schema",
			triggers: la{"manual": la{
				"type": "Request", "kind": "Http", "inputs": la{"method": "POST", "schema": schema},
			}},
			wantPath: "/triggers/manual/paths/invoke", wantVerb: "post", wantBody: true,
		},
		{
			name: "relative path parameters",
			triggers: la{"manual": la{
				"type": "Request", "kind": "Http",
				"inputs": la{"method": "GET", "relativePath": "/items/{id}/parts/{part}", "schema": schema},
			}},
			wantPath: "/triggers/manual/paths/invoke/items/{id}/parts/{part}", wantVerb: "get",
			wantPrm: []string{"id", "part"},
		},
		{
			name: "default method is post",
			triggers: la{"manual": la{
				"type": "Request", "kind": "Http", "inputs": la{},
			}},
			wantPath: "/triggers/manual/paths/invoke", wantVerb: "post",
		},
		{
			name: "no request trigger",
			triggers: la{"tick": la{
				"type": "Recurrence", "recurrence": la{"frequency": "Day", "interval": 1},
			}},
			none: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newLogicStack(t)
			s.create(t, "api", logicDef(tc.triggers, la{}))

			res, err := s.factory.NewWorkflowsClient().ListSwagger(t.Context(), logicRG, "api", nil)
			require.NoError(t, err)

			doc, ok := res.Interface.(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "2.0", doc["swagger"])

			paths, _ := doc["paths"].(map[string]any)
			if tc.none {
				assert.Empty(t, paths)

				return
			}

			item, _ := paths[tc.wantPath].(map[string]any)
			require.NotNil(t, item, "paths: %v", paths)

			op, _ := item[tc.wantVerb].(map[string]any)
			require.NotNil(t, op)

			var path, query []string

			hasBody := false

			for _, p := range op["parameters"].([]any) {
				pm, _ := p.(map[string]any)

				switch pm["in"] {
				case "path":
					path = append(path, pm["name"].(string))
				case "query":
					query = append(query, pm["name"].(string))
				case "body":
					hasBody = true

					assert.Equal(t, schema, pm["schema"])
				}
			}

			assert.Equal(t, tc.wantPrm, path)
			assert.Equal(t, []string{"api-version", "sp", "sv", "sig"}, query)
			assert.Equal(t, tc.wantBody, hasBody)
		})
	}

	s := newLogicStack(t)
	_, err := s.factory.NewWorkflowsClient().ListSwagger(t.Context(), logicRG, "missing", nil)
	require.Error(t, err)
}
