package azuregraph_test

import (
	"encoding/json/jsontext"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const roleDefsPath = rolesBase + "roleDefinitions"

func customRoleBody(name string) map[string]any {
	return map[string]any{
		"displayName": name, "description": "d", "isEnabled": true,
		"rolePermissions": []any{map[string]any{
			"allowedResourceActions": []any{"microsoft.directory/applications/basic/read"},
		}},
	}
}

func TestCustomRoleDefinitionValidation(t *testing.T) {
	t.Parallel()

	with := func(k string, v any) map[string]any {
		b := customRoleBody("Valid")
		if v == nil {
			delete(b, k)
		} else {
			b[k] = v
		}

		return b
	}

	tests := []struct {
		body map[string]any
		name string
		code string
	}{
		{with("displayName", nil), "missing name", "Request_BadRequest"},
		{with("displayName", "  "), "blank name", "Request_BadRequest"},
		{with("isEnabled", nil), "missing isEnabled", "Request_BadRequest"},
		{with("rolePermissions", nil), "missing permissions", "Request_BadRequest"},
		{with("rolePermissions", []any{}), "empty permissions", "Request_BadRequest"},
		{
			with("rolePermissions", []any{map[string]any{"allowedResourceActions": []any{}}}),
			"empty actions",
			"Request_BadRequest",
		},
		{
			with("rolePermissions", []any{map[string]any{"allowedResourceActions": []any{"nope"}}}),
			"bad action",
			"Request_BadRequest",
		},
		{
			with("rolePermissions", []any{map[string]any{"allowedResourceActions": []any{"microsoft.directory/"}}}),
			"action no tail",
			"Request_BadRequest",
		},
		{with("templateId", "not-a-guid"), "bad template", "Request_BadRequest"},
		{with("templateId", readersRole), "builtin template", "Request_BadRequest"},
		{with("resourceScopes", []any{"/administrativeUnits/x"}), "scope", "Request_BadRequest"},
		{with("isBuiltIn", true), "isBuiltIn read-only", "Request_BadRequest"},
		{with("id", newID0()), "id read-only", "Request_BadRequest"},
		{with("bogus", 1), "unknown property", "Request_BadRequest"},
		{with("displayName", "Global Administrator"), "builtin name", "Request_BadRequest"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			assertErr(t, e.req(t, http.MethodPost, roleDefsPath, tt.body), http.StatusBadRequest, tt.code)
			custom := e.req(t, http.MethodGet, roleDefsPath+"?$filter=isBuiltIn%20eq%20false", nil)
			assert.Empty(t, values2(custom, "id"))
		})
	}

	t.Run("malformed json", func(t *testing.T) {
		t.Parallel()

		assertErr(t, newEnv(t).req(t, http.MethodPost, roleDefsPath, "{"), http.StatusBadRequest, "Request_BadRequest")
	})
}

func TestCustomRoleDefinitionLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	user := newUser(t, e, "roleuser"+upnDomain)

	created := e.req(t, http.MethodPost, roleDefsPath, customRoleBody("Support"))
	require.Equal(t, http.StatusCreated, created.status, created.raw)

	id := str(created.body, "id")
	assert.Equal(t, id, str(created.body, "templateId"))
	assert.Equal(t, false, created.body["isBuiltIn"])
	assert.Nil(t, created.body["version"])
	assert.Nil(t, created.body["deletedDateTime"])

	assertErr(
		t,
		e.req(t, http.MethodPost, roleDefsPath, customRoleBody("support")),
		http.StatusBadRequest,
		"Request_BadRequest",
	)

	got := e.req(t, http.MethodGet, roleDefsPath+"/"+id, nil)
	require.Equal(t, http.StatusOK, got.status, got.raw)
	assert.Equal(t, "Support", str(got.body, "displayName"))
	assert.Contains(t, values2(e.req(t, http.MethodGet, roleDefsPath, nil), "id"), id)
	assert.Equal(
		t,
		[]string{id},
		values2(e.req(t, http.MethodGet, roleDefsPath+"?$filter=isBuiltIn%20eq%20false", nil), "id"),
	)

	assign := func(role string) resp {
		return e.req(t, http.MethodPost, rolesBase+"roleAssignments", map[string]any{
			"principalId": str(user, "id"), "roleDefinitionId": role, "directoryScopeId": "/",
		})
	}

	assertErr(t, assign(newID0()), http.StatusBadRequest, "Request_BadRequest")

	require.Equal(
		t,
		http.StatusNoContent,
		e.req(
			t,
			http.MethodPatch,
			roleDefsPath+"/"+id,
			map[string]any{"isEnabled": false, "description": "off"},
		).status,
	)
	assertErr(t, assign(id), http.StatusBadRequest, "Request_BadRequest")
	require.Equal(
		t,
		http.StatusNoContent,
		e.req(t, http.MethodPatch, roleDefsPath+"/"+id, map[string]any{"isEnabled": true}).status,
	)

	asg := assign(id)
	require.Equal(t, http.StatusCreated, asg.status, asg.raw)
	assert.Equal(t, id, str(asg.body, "roleDefinitionId"))

	assertErr(t, e.req(t, http.MethodDelete, roleDefsPath+"/"+id, nil), http.StatusBadRequest, "Request_BadRequest")
	require.Equal(t, http.StatusNoContent,
		e.req(t, http.MethodDelete, rolesBase+"roleAssignments/"+str(asg.body, "id"), nil).status)
	require.Equal(t, http.StatusNoContent, e.req(t, http.MethodDelete, roleDefsPath+"/"+id, nil).status)

	assertErr(t, e.req(t, http.MethodGet, roleDefsPath+"/"+id, nil), http.StatusNotFound, "Request_ResourceNotFound")
	assertErr(t, assign(id), http.StatusBadRequest, "Request_BadRequest")

	deleted := e.req(t, http.MethodGet, rolesBase+"deletedItems/roleDefinitions/"+id, nil)
	require.Equal(t, http.StatusOK, deleted.status, deleted.raw)
	assert.NotNil(t, deleted.body["deletedDateTime"])

	restored := e.req(t, http.MethodPost, rolesBase+"deletedItems/roleDefinitions/"+id+"/restore", nil)
	require.Equal(t, http.StatusOK, restored.status, restored.raw)
	assert.Nil(t, restored.body["deletedDateTime"])
	require.Equal(t, http.StatusNoContent, e.req(t, http.MethodDelete, roleDefsPath+"/"+id, nil).status)
	require.Equal(
		t,
		http.StatusNoContent,
		e.req(t, http.MethodDelete, rolesBase+"deletedItems/roleDefinitions/"+id, nil).status,
	)
	assertErr(
		t,
		e.req(t, http.MethodGet, rolesBase+"deletedItems/roleDefinitions/"+id, nil),
		http.StatusNotFound,
		"Request_ResourceNotFound",
	)
}

func TestBuiltInRoleDefinitionsAreImmutable(t *testing.T) {
	t.Parallel()

	for _, method := range []string{http.MethodPatch, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			assertErr(t, e.req(t, method, roleDefsPath+"/"+globalAdminRole, map[string]any{"description": "x"}),
				http.StatusBadRequest, "Request_BadRequest")
			assert.NotEqual(
				t,
				"x",
				str(e.req(t, http.MethodGet, roleDefsPath+"/"+globalAdminRole, nil).body, "description"),
			)
		})
	}
}

func TestDeletedRoleDefinitionRetention(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	id := str(e.mustCreate(t, roleDefsPath, customRoleBody("Short")), "id")
	require.Equal(t, http.StatusNoContent, e.req(t, http.MethodDelete, roleDefsPath+"/"+id, nil).status)

	e.clock.advance(29 * 24 * time.Hour)
	require.Equal(t, http.StatusOK, e.req(t, http.MethodGet, rolesBase+"deletedItems/roleDefinitions/"+id, nil).status)

	e.clock.advance(2 * 24 * time.Hour)
	assertErr(t, e.req(t, http.MethodGet, rolesBase+"deletedItems/roleDefinitions/"+id, nil),
		http.StatusNotFound, "Request_ResourceNotFound")
	assertErr(t, e.req(t, http.MethodPost, rolesBase+"deletedItems/roleDefinitions/"+id+"/restore", nil),
		http.StatusNotFound, "Request_ResourceNotFound")
}

func TestCustomRoleDefinitionPersists(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	live := str(e.mustCreate(t, roleDefsPath, customRoleBody("Live")), "id")
	gone := str(e.mustCreate(t, roleDefsPath, customRoleBody("Gone")), "id")
	require.Equal(t, http.StatusNoContent, e.req(t, http.MethodDelete, roleDefsPath+"/"+gone, nil).status)

	fresh := newEnv(t)
	require.NoError(t, fresh.h.Restore(t.Context(), e.h.Snapshot(t.Context())))

	got := fresh.req(t, http.MethodGet, roleDefsPath+"/"+live, nil)
	require.Equal(t, http.StatusOK, got.status, got.raw)
	assert.Equal(t, "Live", str(got.body, "displayName"))
	assert.Equal(
		t,
		http.StatusOK,
		fresh.req(t, http.MethodGet, rolesBase+"deletedItems/roleDefinitions/"+gone, nil).status,
	)
	assertErr(
		t,
		fresh.req(t, http.MethodGet, roleDefsPath+"/"+gone, nil),
		http.StatusNotFound,
		"Request_ResourceNotFound",
	)
}

func TestCustomRoleDefinitionLimitUnderConcurrency(t *testing.T) {
	t.Parallel()

	b := newEnv(t).h.Backend
	const workers, perWorker, limit = 16, 70, 1000

	var created atomic.Int64

	var wg sync.WaitGroup

	for w := range workers {
		wg.Go(func() {
			for i := range perWorker {
				body := map[string]jsontext.Value{
					"displayName": jsontext.Value(fmt.Sprintf(`"role-%d-%d"`, w, i)),
					"isEnabled":   jsontext.Value("true"),
					"rolePermissions": jsontext.Value(
						`[{"allowedResourceActions":["microsoft.directory/applications/basic/read"]}]`),
				}

				if _, err := b.CreateRoleDefinition(body); err == nil {
					created.Add(1)
				}
			}
		})
	}

	wg.Wait()
	assert.EqualValues(t, limit, created.Load())
}
