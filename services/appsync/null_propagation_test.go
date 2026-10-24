package appsync_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

const nullSchema = `
type Item { id: ID! name: String! note: String }
type Query {
  item: Item
  strictItem: Item!
  items: [Item]
  strictItems: [Item!]
  wrapped: Wrapper
  unmapped: String!
}
type Wrapper { inner: Item! label: String }
`

// Spec "Handling Field Errors": a null in a non-null position nulls the nearest nullable ancestor.
func TestNullPropagation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantData map[string]any
		name     string
		query    string
		wantMsg  string
		wantPath []any
	}{
		{
			name: "nullable parent absorbs", query: `{ item { id name } }`,
			wantData: map[string]any{"item": nil}, wantPath: []any{"item", "name"},
			wantMsg: "Cannot return null for non-nullable type: 'String' within parent 'Item' (/item/name)",
		},
		{
			name: "non-null parent bubbles to data", query: `{ strictItem { id name } }`,
			wantData: nil, wantPath: []any{"strictItem", "name"},
			wantMsg: "Cannot return null for non-nullable type: 'String' within parent 'Item' (/strictItem/name)",
		},
		{
			name: "nullable list element absorbs", query: `{ items { id name } }`,
			wantData: map[string]any{"items": []any{map[string]any{"id": "1", "name": "a"}, nil}},
			wantPath: []any{"items", float64(1), "name"},
			wantMsg:  "Cannot return null for non-nullable type: 'String' within parent 'Item' (/items/1/name)",
		},
		{
			name: "non-null list element nulls the list", query: `{ strictItems { id name } }`,
			wantData: map[string]any{"strictItems": nil},
			wantPath: []any{"strictItems", float64(1), "name"},
			wantMsg:  "Cannot return null for non-nullable type: 'String' within parent 'Item' (/strictItems/1/name)",
		},
		{
			name: "nested non-null chain stops at nullable", query: `{ wrapped { label inner { id name } } }`,
			wantData: map[string]any{"wrapped": nil}, wantPath: []any{"wrapped", "inner", "name"},
			wantMsg: "Cannot return null for non-nullable type: 'String' within parent 'Item' (/wrapped/inner/name)",
		},
		{
			name: "unmapped non-null root field", query: `{ unmapped }`,
			wantData: nil, wantPath: []any{"unmapped"},
			wantMsg: "Cannot return null for non-nullable type: 'String' within parent 'Query' (/unmapped)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			api, err := b.CreateGraphqlAPI("np", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)

			key, err := b.CreateAPIKey(api.APIID, "", 0)
			require.NoError(t, err)

			_, err = b.StartSchemaCreation(api.APIID, nullSchema)
			require.NoError(t, err)
			_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{Name: "None", Type: appsync.DataSourceTypeNone})
			require.NoError(t, err)

			for field, payload := range map[string]string{
				"item":        `{"id":"1","name":null}`,
				"strictItem":  `{"id":"1","name":null}`,
				"items":       `[{"id":"1","name":"a"},{"id":"2","name":null}]`,
				"strictItems": `[{"id":"1","name":"a"},{"id":"2","name":null}]`,
				"wrapped":     `{"label":"l","inner":{"id":"1","name":null}}`,
			} {
				_, err = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
					FieldName: field, DataSourceName: "None",
					RequestMappingTemplate:  `{"payload":` + payload + `}`,
					ResponseMappingTemplate: `$util.toJson($ctx.result)`,
				})
				require.NoError(t, err)
			}

			rec := doRequestWithHeaders(t, h, "/v1/apis/"+api.APIID+"/graphql",
				map[string]any{"query": tt.query}, map[string]string{"X-Api-Key": key.ID})
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var got gqlReply
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
			assert.Equal(t, tt.wantData, got.Data)
			require.Len(t, got.Errors, 1, rec.Body.String())
			assert.Equal(t, tt.wantMsg, got.Errors[0].Message)
			assert.Equal(t, tt.wantPath, got.Errors[0].Path)
		})
	}
}
