package workspaces_test

import (
	"net/http"
	"testing"
)

func TestConnectionAliasCRUD(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		name             string
		connectionString string
	}{
		{name: "simple alias", connectionString: "myalias.corp.example"},
		{name: "ip alias", connectionString: "10.0.0.1"},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newTestHandlerWithBackend(t)

			// Create
			rec := doTargetRequest(t, h, "CreateConnectionAlias", map[string]any{
				"ConnectionString": tc.connectionString,
			})
			if rec.Code != http.StatusOK {
				t.Fatalf("create: expected 200, got %d: %s", rec.Code, rec.Body)
			}

			var createOut map[string]string
			decodeJSON(t, rec.Body.Bytes(), &createOut)

			aliasID := createOut["AliasId"]
			if aliasID == "" {
				t.Fatal("expected non-empty AliasId")
			}

			// Describe
			rec2 := doTargetRequest(t, h, "DescribeConnectionAliases", map[string]any{
				"AliasIds": []string{aliasID},
			})
			if rec2.Code != http.StatusOK {
				t.Fatalf("describe: expected 200, got %d", rec2.Code)
			}

			var descOut struct {
				ConnectionAliases []map[string]any `json:"ConnectionAliases"`
			}
			decodeJSON(t, rec2.Body.Bytes(), &descOut)

			if len(descOut.ConnectionAliases) != 1 {
				t.Fatalf("expected 1 alias, got %d", len(descOut.ConnectionAliases))
			}

			// Associate
			rec3 := doTargetRequest(t, h, "AssociateConnectionAlias", map[string]any{
				"AliasId":    aliasID,
				"ResourceId": "res-123",
			})
			if rec3.Code != http.StatusOK {
				t.Fatalf("associate: expected 200, got %d", rec3.Code)
			}

			// Describe permissions
			rec4 := doTargetRequest(t, h, "DescribeConnectionAliasPermissions", map[string]any{
				"AliasId": aliasID,
			})
			if rec4.Code != http.StatusOK {
				t.Fatalf("describe perms: expected 200, got %d", rec4.Code)
			}

			// Update permission
			rec5 := doTargetRequest(t, h, "UpdateConnectionAliasPermission", map[string]any{
				"AliasId": aliasID,
				"ConnectionAliasPermission": map[string]any{
					"SharedAccountId":  "999988887777",
					"AllowAssociation": true,
				},
			})
			if rec5.Code != http.StatusOK {
				t.Fatalf("update perm: expected 200, got %d", rec5.Code)
			}

			// Disassociate
			rec6 := doTargetRequest(t, h, "DisassociateConnectionAlias", map[string]any{
				"AliasId": aliasID,
			})
			if rec6.Code != http.StatusOK {
				t.Fatalf("disassociate: expected 200, got %d", rec6.Code)
			}

			// Delete
			rec7 := doTargetRequest(t, h, "DeleteConnectionAlias", map[string]any{
				"AliasId": aliasID,
			})
			if rec7.Code != http.StatusOK {
				t.Fatalf("delete: expected 200, got %d", rec7.Code)
			}
		})
	}
}
