package azuregraph_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azuregraph"
)

func TestPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	app := newApp(t, e, "Persisted")
	appID := str(app, "appId")
	sp := e.mustCreate(t, "/v1.0/servicePrincipals", map[string]any{"appId": appID})
	secret := str(
		e.req(t, http.MethodPost, "/v1.0/applications/"+str(app, "id")+"/addPassword", map[string]any{}).body,
		"secretText",
	)
	user := newUser(t, e, "persist"+upnDomain)
	group := newGroup(t, e, "Persisted Group")

	ref := func(o map[string]any) map[string]any {
		return map[string]any{"@odata.id": "https://graph.microsoft.com/v1.0/directoryObjects/" + str(o, "id")}
	}

	require.Equal(
		t,
		http.StatusNoContent,
		e.req(t, http.MethodPost, "/v1.0/groups/"+str(group, "id")+"/members/$ref", ref(user)).status,
	)
	require.Equal(
		t,
		http.StatusNoContent,
		e.req(t, http.MethodPost, "/v1.0/applications/"+str(app, "id")+"/owners/$ref", ref(user)).status,
	)
	e.mustCreate(t, "/v1.0/servicePrincipals/"+str(sp, "id")+"/appRoleAssignments", map[string]any{
		"principalId": str(sp, "id"), "resourceId": graphSP, "appRoleId": appReadAll,
	})

	snap := e.h.Snapshot(t.Context())
	assert.NotContains(t, string(snap), secret, "plaintext secrets are never persisted")

	fresh := newEnv(t)
	require.NoError(t, fresh.h.Restore(t.Context(), snap))

	for _, path := range []string{
		"/v1.0/applications/" + str(app, "id"), "/v1.0/servicePrincipals/" + str(sp, "id"),
		"/v1.0/users/" + str(user, "id"), "/v1.0/groups/" + str(group, "id"),
	} {
		before, after := e.req(t, http.MethodGet, path, nil), fresh.req(t, http.MethodGet, path, nil)
		require.Equal(t, http.StatusOK, after.status, path)

		delete(before.body, "@odata.context")
		delete(after.body, "@odata.context")
		assert.Equal(t, before.body, after.body, path)
	}

	assert.Len(t, fresh.req(t, http.MethodGet, "/v1.0/groups/"+str(group, "id")+"/members", nil).body["value"], 1)
	assert.Len(t, fresh.req(t, http.MethodGet, "/v1.0/applications/"+str(app, "id")+"/owners", nil).body["value"], 1)
	assert.Len(
		t,
		fresh.req(t, http.MethodGet, "/v1.0/servicePrincipals/"+str(sp, "id")+"/appRoleAssignments", nil).body["value"],
		1,
	)

	oid, known, err := fresh.h.Backend.AuthenticateClient(appID, secret)
	require.NoError(t, err)
	assert.True(t, known)
	assert.Equal(t, str(sp, "id"), oid)

	t.Run("reset re-seeds the built-in principal", func(t *testing.T) {
		t.Parallel()

		other := newEnv(t)
		require.NoError(t, other.h.Restore(t.Context(), snap))
		other.h.Reset()
		assert.Equal(t, http.StatusOK, other.req(t, http.MethodGet, "/v1.0/servicePrincipals/"+graphSP, nil).status)
		assert.Equal(
			t,
			http.StatusNotFound,
			other.req(t, http.MethodGet, "/v1.0/applications/"+str(app, "id"), nil).status,
		)
	})

	t.Run("incompatible snapshot starts empty", func(t *testing.T) {
		t.Parallel()

		other := newEnv(t)
		require.NoError(t, other.h.Restore(t.Context(), []byte(`{"version":99}`)))
		assert.Equal(t, http.StatusOK, other.req(t, http.MethodGet, "/v1.0/servicePrincipals/"+graphSP, nil).status)
	})

	t.Run("corrupt snapshot", func(t *testing.T) {
		t.Parallel()

		require.Error(t, newEnv(t).h.Restore(t.Context(), []byte("not json")))
	})
}

func TestOrganizationAndDomains(t *testing.T) {
	t.Parallel()

	e := newEnv(t, func(s *azuregraph.Settings) { s.ExtraDomains = "corp.example, Other.Example" })

	org := e.req(t, http.MethodGet, "/v1.0/organization", nil)
	require.Equal(t, http.StatusOK, org.status)

	first, _ := org.body["value"].([]any)[0].(map[string]any)
	assert.Equal(t, azuregraph.DefaultTenantID, str(first, "id"))

	domains, _ := first["verifiedDomains"].([]any)
	require.Len(t, domains, 3)

	d0, _ := domains[0].(map[string]any)
	assert.Equal(t, true, d0["isDefault"])
	assert.Equal(t, true, d0["isInitial"])
	assert.Equal(t, "gopherstack.onmicrosoft.com", str(d0, "name"))

	assert.Equal(
		t,
		http.StatusOK,
		e.req(t, http.MethodGet, "/v1.0/organization/"+azuregraph.DefaultTenantID, nil).status,
	)
	assertErr(
		t,
		e.req(t, http.MethodGet, "/v1.0/organization/"+newID0(), nil),
		http.StatusNotFound,
		"Request_ResourceNotFound",
	)

	list := e.req(t, http.MethodGet, "/v1.0/domains", nil)
	assert.Len(t, list.body["value"], 3)
	assert.Equal(t, http.StatusOK, e.req(t, http.MethodGet, "/v1.0/domains/other.example", nil).status)
	assertErr(
		t,
		e.req(t, http.MethodGet, "/v1.0/domains/nope.example", nil),
		http.StatusNotFound,
		"Request_ResourceNotFound",
	)
}

func TestRoutingErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	tests := []struct { //nolint:govet // test-only struct; layout not performance-critical
		name   string
		method string
		path   string
		status int
		code   string
	}{
		{"no version", http.MethodGet, "/applications", http.StatusBadRequest, "BadRequest"},
		{"unknown collection", http.MethodGet, "/v1.0/widgets", http.StatusBadRequest, "BadRequest"},
		{
			"unknown subresource",
			http.MethodGet,
			"/v1.0/users/" + newID0() + "/widgets",
			http.StatusNotFound,
			"Request_ResourceNotFound",
		},
		{"method not allowed", http.MethodPut, "/v1.0/applications", http.StatusMethodNotAllowed, "Request_BadRequest"},
		{"beta works", http.MethodGet, "/beta/applications", http.StatusOK, ""},
		{"root", http.MethodGet, "/", http.StatusBadRequest, "BadRequest"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := e.req(t, tt.method, tt.path, nil)
			if tt.code == "" {
				assert.Equal(t, tt.status, r.status)
				assert.Contains(t, str(r.body, "@odata.context"), "/beta/$metadata#applications")

				return
			}

			assertErr(t, r, tt.status, tt.code)
		})
	}

	r := e.req(t, http.MethodGet, "/v1.0/applications", nil)
	assert.NotEmpty(t, r.header.Get("Request-Id"))
	assert.NotEmpty(t, r.header.Get("Client-Request-Id"))

	echoed := e.req(t, http.MethodGet, "/v1.0/applications", nil, "Client-Request-Id", "abc-123")
	assert.Equal(t, "abc-123", echoed.header.Get("Client-Request-Id"))
	assert.Contains(t, echoed.header.Get("Content-Type"), "odata.metadata=minimal")
}

func TestProviderAndSettings(t *testing.T) {
	t.Parallel()

	p := &azuregraph.Provider{}
	assert.Equal(t, "AzureGraph", p.Name())

	_, err := p.Init(nil)
	require.ErrorIs(t, err, azuregraph.ErrNilAppContext)

	reg, err := p.Init(&service.AppContext{})
	require.NoError(t, err)

	h, ok := reg.(*azuregraph.Handler)
	require.True(t, ok)
	assert.Equal(t, azuregraph.DefaultPort, h.Port)
	assert.Equal(t, "AzureGraph", h.Name())
	assert.NotEmpty(t, h.GetSupportedOperations())
	assert.Equal(t, 0, h.MatchPriority())

	cfg := azuregraph.DefaultSettings()
	cfg.TLSCertFile = "cert.pem"
	assert.Error(
		t,
		azuregraph.NewHandler(azuregraph.NewBackend(cfg), cfg).StartWorker(t.Context()),
		"cert without key must fail before serving",
	)
}

func TestOperationNames(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	supported := map[string]bool{}

	for _, op := range e.h.GetSupportedOperations() {
		supported[op] = true
	}

	for _, tt := range []struct{ method, path, want string }{
		{http.MethodGet, "/v1.0/applications", "ListApplications"},
		{http.MethodPost, "/v1.0/users", "CreateUsers"},
		{http.MethodGet, "/v1.0/groups/x", "GetGroups"},
		{http.MethodPatch, "/v1.0/servicePrincipals(appId='a')", "UpdateServicePrincipals"},
		{http.MethodPost, "/v1.0/applications/x/addPassword", "AddPassword"},
		{http.MethodPost, "/v1.0/groups/x/members/$ref", "AddReference"},
		{http.MethodDelete, "/v1.0/groups/x/members/y/$ref", "RemoveReference"},
		{http.MethodGet, "/v1.0/me", "GetMe"},
		{http.MethodPost, "/v1.0/directoryObjects/getByIds", "GetByIds"},
		{http.MethodGet, "/v1.0/users/x/memberOf", "ListRelation"},
		{http.MethodPost, "/v1.0/servicePrincipals/x/appRoleAssignments", "CreateAppRoleAssignment"},
		{http.MethodGet, "/v1.0/applications/$count", "CountApplications"},
	} {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequestWithContext(t.Context(), tt.method, tt.path, nil)
			c := echo.New().NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, e.h.ExtractOperation(c))
			assert.True(t, supported[tt.want], tt.want)
		})
	}
}
