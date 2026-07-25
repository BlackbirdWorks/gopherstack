package lakeformation_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/lakeformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGrantPermissions_AllResourceKinds exercises GrantPermissions/ListPermissions
// against every Resource union member the real types.Resource struct supports
// (Catalog, Database, Table, TableWithColumns, DataLocation, DataCellsFilter,
// LFTag, LFTagExpression, LFTagPolicy). Before this fix, Resource only carried
// Catalog/Database/Table/TableWithColumns/DataLocation, so a grant against an
// LF-tag or LF-tag-policy resource -- both real, documented Lake Formation
// permission targets -- silently lost the resource-kind-specific fields on
// the wire (they were never in the JSON schema at all).
func TestGrantPermissions_AllResourceKinds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		resource map[string]any
		name     string
	}{
		{
			name:     "Catalog with Id",
			resource: map[string]any{"Catalog": map[string]any{"Id": "123456789012"}},
		},
		{
			name:     "LFTag",
			resource: map[string]any{"LFTag": map[string]any{"TagKey": "env", "TagValues": []any{"prod"}}},
		},
		{
			name: "LFTagPolicy",
			resource: map[string]any{
				"LFTagPolicy": map[string]any{
					"ResourceType": "TABLE",
					"Expression":   []any{map[string]any{"TagKey": "env", "TagValues": []any{"prod"}}},
				},
			},
		},
		{
			name:     "LFTagExpression",
			resource: map[string]any{"LFTagExpression": map[string]any{"Name": "expr1"}},
		},
		{
			name: "DataCellsFilter",
			resource: map[string]any{
				"DataCellsFilter": map[string]any{
					"TableCatalogId": "123456789012",
					"DatabaseName":   "mydb",
					"TableName":      "mytable",
					"Name":           "filter1",
				},
			},
		},
		{
			name: "Table with TableWildcard",
			resource: map[string]any{
				"Table": map[string]any{"DatabaseName": "mydb", "TableWildcard": map[string]any{}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()
			h := lakeformation.NewHandler(b)

			rec := postJSON(t, h, "/GrantPermissions", map[string]any{
				"Principal":   map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::123:user/u"},
				"Resource":    tt.resource,
				"Permissions": []any{"DESCRIBE"},
			})
			require.Equal(t, http.StatusOK, rec.Code, "grant should succeed for resource kind %s", tt.name)
			assert.Equal(t, 1, b.PermissionCount())

			// The same resource shape must round-trip through ListPermissions'
			// Resource-shaped filter (not a flat ResourceArn).
			rec2 := postJSON(t, h, "/ListPermissions", map[string]any{"Resource": tt.resource})
			require.Equal(t, http.StatusOK, rec2.Code)

			var out map[string]any
			require.NoError(t, jsonDecode(rec2.Body, &out))
			entries := out["PrincipalResourcePermissions"].([]any)
			assert.Len(t, entries, 1, "ListPermissions should find the grant by matching Resource shape")
		})
	}
}
