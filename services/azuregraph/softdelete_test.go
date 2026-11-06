package azuregraph_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newUnifiedGroup(t *testing.T, e *env, name string) map[string]any {
	t.Helper()

	return e.mustCreate(t, "/v1.0/groups", map[string]any{
		"displayName": name, "mailEnabled": true, "mailNickname": name, "securityEnabled": false,
		"groupTypes": []string{"Unified"},
	})
}

func createApp(t *testing.T, e *env) map[string]any {
	t.Helper()

	return newApp(t, e, "Soft App")
}

func createUser(t *testing.T, e *env) map[string]any {
	t.Helper()

	return newUser(t, e, "soft"+upnDomain)
}

func createUnified(t *testing.T, e *env) map[string]any {
	t.Helper()

	return newUnifiedGroup(t, e, "softgroup")
}

func TestSoftDeleteLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(t *testing.T, e *env) map[string]any
		name   string
		base   string
		cast   string
	}{
		{createApp, "application", "/v1.0/applications/", "microsoft.graph.application"},
		{createUser, "user", "/v1.0/users/", "microsoft.graph.user"},
		{createUnified, "group", "/v1.0/groups/", "microsoft.graph.group"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			obj := tt.create(t, e)
			id := str(obj, "id")

			require.Equal(t, http.StatusNoContent, e.req(t, http.MethodDelete, tt.base+id, nil).status)
			assertErr(t, e.req(t, http.MethodGet, tt.base+id, nil), http.StatusNotFound, "Request_ResourceNotFound")

			list := e.req(t, http.MethodGet, "/v1.0/directory/deletedItems/"+tt.cast, nil)
			require.Equal(t, http.StatusOK, list.status, list.raw)
			assert.Equal(t, []string{id}, values2(list, "id"))

			got := e.req(t, http.MethodGet, "/v1.0/directory/deletedItems/"+id, nil)
			require.Equal(t, http.StatusOK, got.status, got.raw)
			assert.Equal(t, "2026-01-02T03:04:05Z", str(got.body, "deletedDateTime"))
			assert.Equal(t, "#"+tt.cast, str(got.body, "@odata.type"))

			restored := e.req(t, http.MethodPost, "/v1.0/directory/deletedItems/"+id+"/restore", nil)
			require.Equal(t, http.StatusOK, restored.status, restored.raw)
			assert.Equal(t, id, str(restored.body, "id"))
			require.Equal(t, http.StatusOK, e.req(t, http.MethodGet, tt.base+id, nil).status)
			assertErr(t, e.req(t, http.MethodGet, "/v1.0/directory/deletedItems/"+id, nil),
				http.StatusNotFound, "Request_ResourceNotFound")
		})
	}
}

func values2(r resp, key string) []string {
	items, _ := r.body["value"].([]any)
	out := make([]string, 0, len(items))

	for _, it := range items {
		m, _ := it.(map[string]any)
		out = append(out, str(m, key))
	}

	return out
}

func TestSoftDeleteRules(t *testing.T) {
	t.Parallel()

	t.Run("permanent delete and retention", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		a, b := newUser(t, e, "pa"+upnDomain), newUser(t, e, "pb"+upnDomain)

		for _, u := range []map[string]any{a, b} {
			require.Equal(t, http.StatusNoContent, e.req(t, http.MethodDelete, "/v1.0/users/"+str(u, "id"), nil).status)
		}

		require.Equal(t, http.StatusNoContent,
			e.req(t, http.MethodDelete, "/v1.0/directory/deletedItems/"+str(a, "id"), nil).status)
		assertErr(t, e.req(t, http.MethodPost, "/v1.0/directory/deletedItems/"+str(a, "id")+"/restore", nil),
			http.StatusNotFound, "Request_ResourceNotFound")

		e.clock.advance(30*24*time.Hour - time.Second)
		require.Equal(
			t,
			http.StatusOK,
			e.req(t, http.MethodGet, "/v1.0/directory/deletedItems/"+str(b, "id"), nil).status,
		)

		e.clock.advance(time.Second)
		assertErr(t, e.req(t, http.MethodGet, "/v1.0/directory/deletedItems/"+str(b, "id"), nil),
			http.StatusNotFound, "Request_ResourceNotFound")
	})

	t.Run("security group is hard deleted", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		g := newGroup(t, e, "Hard Group")
		require.Equal(t, http.StatusNoContent, e.req(t, http.MethodDelete, "/v1.0/groups/"+str(g, "id"), nil).status)

		list := e.req(t, http.MethodGet, "/v1.0/directory/deletedItems/microsoft.graph.group", nil)
		assert.Empty(t, values2(list, "id"))
	})

	t.Run("application delete parks its service principal", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		app := newApp(t, e, "WithSP")
		sp := e.mustCreate(t, "/v1.0/servicePrincipals", map[string]any{"appId": str(app, "appId")})

		require.Equal(
			t,
			http.StatusNoContent,
			e.req(t, http.MethodDelete, "/v1.0/applications/"+str(app, "id"), nil).status,
		)

		list := e.req(t, http.MethodGet, "/v1.0/directory/deletedItems/microsoft.graph.servicePrincipal", nil)
		assert.Equal(t, []string{str(sp, "id")}, values2(list, "id"))

		require.Equal(t, http.StatusOK,
			e.req(t, http.MethodPost, "/v1.0/directory/deletedItems/"+str(app, "id")+"/restore", nil).status)
		assertErr(t, e.req(t, http.MethodGet, "/v1.0/servicePrincipals/"+str(sp, "id"), nil),
			http.StatusNotFound, "Request_ResourceNotFound")

		require.Equal(t, http.StatusOK,
			e.req(t, http.MethodPost, "/v1.0/directory/deletedItems/"+str(sp, "id")+"/restore", nil).status)
		require.Equal(t, http.StatusOK, e.req(t, http.MethodGet, "/v1.0/servicePrincipals/"+str(sp, "id"), nil).status)
	})

	t.Run("restore conflicts with a reused userPrincipalName", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		u := newUser(t, e, "reuse"+upnDomain)
		require.Equal(t, http.StatusNoContent, e.req(t, http.MethodDelete, "/v1.0/users/"+str(u, "id"), nil).status)
		newUser(t, e, "reuse"+upnDomain)

		assertErr(t, e.req(t, http.MethodPost, "/v1.0/directory/deletedItems/"+str(u, "id")+"/restore", nil),
			http.StatusBadRequest, "Request_BadRequest")
	})

	t.Run("restore keeps credentials and group references to other objects are gone", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		app := newApp(t, e, "Creds")
		pw := e.req(t, http.MethodPost, "/v1.0/applications/"+str(app, "id")+"/addPassword", map[string]any{})
		sp := e.mustCreate(t, "/v1.0/servicePrincipals", map[string]any{"appId": str(app, "appId")})
		require.Equal(
			t,
			http.StatusNoContent,
			e.req(t, http.MethodDelete, "/v1.0/applications/"+str(app, "id"), nil).status,
		)
		require.Equal(t, http.StatusOK,
			e.req(t, http.MethodPost, "/v1.0/directory/deletedItems/"+str(app, "id")+"/restore", nil).status)
		require.Equal(t, http.StatusOK,
			e.req(t, http.MethodPost, "/v1.0/directory/deletedItems/"+str(sp, "id")+"/restore", nil).status)

		_, known, err := e.h.Backend.AuthenticateClient(str(app, "appId"), str(pw.body, "secretText"))
		require.NoError(t, err)
		assert.True(t, known)
	})

	t.Run("invalid ids and casts", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		assertErr(
			t,
			e.req(t, http.MethodGet, "/v1.0/directory/deletedItems/nope", nil),
			http.StatusBadRequest,
			"Request_BadRequest",
		)
		assertErr(t, e.req(t, http.MethodGet, "/v1.0/directory/deletedItems/microsoft.graph.widget", nil),
			http.StatusBadRequest, "BadRequest")
		assertErr(t, e.req(t, http.MethodGet, "/v1.0/directory/widgets", nil), http.StatusBadRequest, "BadRequest")
	})
}

func TestSoftDeletePersists(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	u := newUser(t, e, "persist2"+upnDomain)
	require.Equal(t, http.StatusNoContent, e.req(t, http.MethodDelete, "/v1.0/users/"+str(u, "id"), nil).status)

	snap := e.h.Snapshot(t.Context())
	fresh := newEnv(t)
	require.NoError(t, fresh.h.Restore(t.Context(), snap))

	got := fresh.req(t, http.MethodGet, "/v1.0/directory/deletedItems/"+str(u, "id"), nil)
	require.Equal(t, http.StatusOK, got.status, got.raw)
	assert.Equal(t, "persist2"+upnDomain, str(got.body, "userPrincipalName"))
}
