package azuregraph_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUserManager(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	boss := newUser(t, e, "boss"+upnDomain)
	mid := newUser(t, e, "mid"+upnDomain)
	dev := newUser(t, e, "dev"+upnDomain)
	grp := newGroup(t, e, "Team")
	ref := func(o map[string]any) map[string]any {
		return map[string]any{"@odata.id": "https://graph.microsoft.com/v1.0/users/" + str(o, "id")}
	}
	base := func(o map[string]any) string { return "/v1.0/users/" + str(o, "id") + "/manager" }

	assertErr(t, e.req(t, http.MethodGet, base(dev), nil), http.StatusNotFound, "Request_ResourceNotFound")

	require.Equal(t, http.StatusNoContent, e.req(t, http.MethodPut, base(mid)+"/$ref", ref(boss)).status)
	require.Equal(t, http.StatusNoContent, e.req(t, http.MethodPut, base(dev)+"/$ref", ref(mid)).status)

	t.Run("get manager", func(t *testing.T) {
		t.Parallel()

		r := e.req(t, http.MethodGet, base(dev), nil)
		require.Equal(t, http.StatusOK, r.status, r.raw)
		assert.Equal(t, str(mid, "id"), str(r.body, "id"))
		assert.Equal(t, "#microsoft.graph.user", str(r.body, "@odata.type"))

		r = e.req(t, http.MethodGet, base(dev)+"/$ref", nil)
		require.Equal(t, http.StatusOK, r.status, r.raw)
		assert.Contains(t, str(r.body, "@odata.id"), str(mid, "id"))
	})

	t.Run("direct reports", func(t *testing.T) {
		t.Parallel()

		r := e.req(t, http.MethodGet, "/v1.0/users/"+str(mid, "id")+"/directReports", nil)
		require.Equal(t, http.StatusOK, r.status, r.raw)

		items, _ := r.body["value"].([]any)
		require.Len(t, items, 1)
		assert.Equal(t, str(dev, "id"), str(items[0].(map[string]any), "id"))
	})

	t.Run("expand chain", func(t *testing.T) {
		t.Parallel()

		path := "/v1.0/users/" + str(dev, "id") + "?$expand=manager($levels=max;$select=id)"
		assertErr(t, e.req(t, http.MethodGet, path, nil), http.StatusBadRequest, "Request_UnsupportedQuery")

		r := e.req(t, http.MethodGet, path, nil, "ConsistencyLevel", "eventual")
		require.Equal(t, http.StatusOK, r.status, r.raw)

		m1, _ := r.body["manager"].(map[string]any)
		assert.Equal(t, str(mid, "id"), str(m1, "id"))

		m2, _ := m1["manager"].(map[string]any)
		assert.Equal(t, str(boss, "id"), str(m2, "id"))
		assert.NotContains(t, m2, "manager")

		one := e.req(t, http.MethodGet, "/v1.0/users/"+str(dev, "id")+"?$expand=manager", nil)
		require.Equal(t, http.StatusOK, one.status, one.raw)

		m1, _ = one.body["manager"].(map[string]any)
		assert.Equal(t, str(mid, "id"), str(m1, "id"))
		assert.NotContains(t, m1, "manager")
	})

	t.Run("rejections", func(t *testing.T) {
		t.Parallel()

		loop := e.req(t, http.MethodPut, base(boss)+"/$ref", ref(dev))
		assertErr(t, loop, http.StatusBadRequest, "Request_BadRequest")

		self := e.req(t, http.MethodPut, base(boss)+"/$ref", ref(boss))
		assertErr(t, self, http.StatusBadRequest, "Request_BadRequest")

		notUser := e.req(t, http.MethodPut, base(boss)+"/$ref",
			map[string]any{"@odata.id": "https://graph.microsoft.com/v1.0/groups/" + str(grp, "id")})
		assertErr(t, notUser, http.StatusBadRequest, "Request_BadRequest")

		gone := e.req(t, http.MethodPut, base(boss)+"/$ref",
			map[string]any{"@odata.id": "https://graph.microsoft.com/v1.0/users/00000000-0000-4000-8000-000000000009"})
		assertErr(t, gone, http.StatusNotFound, "Request_ResourceNotFound")

		assertErr(t, e.req(t, http.MethodPut, base(boss)+"/$ref", map[string]any{}),
			http.StatusBadRequest, "Request_BadRequest")
	})

	t.Run("remove and delete cleanup", func(t *testing.T) {
		t.Parallel()

		a := newUser(t, e, "ra"+upnDomain)
		b := newUser(t, e, "rb"+upnDomain)
		require.Equal(t, http.StatusNoContent, e.req(t, http.MethodPut, base(a)+"/$ref", ref(b)).status)
		require.Equal(t, http.StatusNoContent, e.req(t, http.MethodDelete, base(a)+"/$ref", nil).status)
		assertErr(t, e.req(t, http.MethodGet, base(a), nil), http.StatusNotFound, "Request_ResourceNotFound")

		require.Equal(t, http.StatusNoContent, e.req(t, http.MethodPut, base(a)+"/$ref", ref(b)).status)
		require.Equal(t, http.StatusNoContent, e.req(t, http.MethodDelete, "/v1.0/users/"+str(b, "id"), nil).status)
		assertErr(t, e.req(t, http.MethodGet, base(a), nil), http.StatusNotFound, "Request_ResourceNotFound")
	})
}
