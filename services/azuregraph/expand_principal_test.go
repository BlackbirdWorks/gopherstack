package azuregraph_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoleAssignmentExpandPrincipal(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	u := newUser(t, e, "expand"+upnDomain)

	created := e.req(t, http.MethodPost, rolesBase+"roleAssignments", map[string]any{
		"principalId": str(u, "id"), "roleDefinitionId": readersRole, "directoryScopeId": "/",
	})
	require.Equal(t, http.StatusCreated, created.status, created.raw)

	id := str(created.body, "id")

	tests := []struct {
		name   string
		path   string
		status int
	}{
		{name: "list", path: "roleAssignments?$expand=principal", status: http.StatusOK},
		{name: "get", path: "roleAssignments/" + id + "?$expand=principal", status: http.StatusOK},
		{name: "select", path: "roleAssignments/" + id + "?$expand=principal($select=id)", status: http.StatusOK},
		{name: "unknown", path: "roleAssignments?$expand=nope", status: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := e.req(t, http.MethodGet, rolesBase+tt.path, nil)
			require.Equal(t, tt.status, got.status, got.raw)

			if tt.status != http.StatusOK {
				return
			}

			item := got.body
			if tt.name == "list" {
				vals, _ := got.body["value"].([]any)
				require.Len(t, vals, 1)

				item, _ = vals[0].(map[string]any)
			}

			principal, _ := item["principal"].(map[string]any)
			require.NotNil(t, principal, got.raw)
			assert.Equal(t, str(u, "id"), principal["id"])
			assert.Equal(t, "#microsoft.graph.user", principal["@odata.type"])
		})
	}
}
