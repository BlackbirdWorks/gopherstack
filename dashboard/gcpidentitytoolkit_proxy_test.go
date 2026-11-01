package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/gcpidentitytoolkit"
)

const (
	idpBase     = "/dashboard/api/v1/gcp/identitytoolkit"
	idpProject  = idpBase + "/projects/demo-proj"
	idpPassword = "Sup3rSecret!pw"
	idpUserUID  = "uid-ada"
)

func idpSeed(t *testing.T, e *echo.Echo, method, path, body string) {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer owner")
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Less(t, rec.Code, http.StatusBadRequest, "%s: %s", path, rec.Body.String())
}

func newIDPDashboard(t *testing.T) *dashboard.DashboardHandler {
	t.Helper()

	ops := gcpidentitytoolkit.NewHandler(gcpidentitytoolkit.NewInMemoryBackend())
	e := echo.New()
	e.Any("/*", ops.Handler())

	idpSeed(t, e, http.MethodPost, "/v1/projects/demo-proj/accounts",
		`{"localId":"`+idpUserUID+`","email":"ada@example.com","password":"`+idpPassword+`","displayName":"Ada"}`)
	idpSeed(t, e, http.MethodPost, "/v1/projects/demo-proj/accounts:sendOobCode",
		`{"requestType":"PASSWORD_RESET","email":"ada@example.com"}`)
	idpSeed(t, e, http.MethodPost, "/v2/projects/demo-proj/tenants", `{"displayName":"acme-tenant"}`)

	return dashboard.NewHandler(dashboard.Config{GCPIdentityToolkitOps: ops})
}

func TestGCPIdentityToolkitProxy(t *testing.T) {
	t.Parallel()

	user := idpProject + "/users/" + idpUserUID

	tests := []struct {
		name  string
		steps []gcpStep
	}{
		{"read", []gcpStep{
			{http.MethodGet, idpBase + "/projects", "", "demo-proj", 200},
			{http.MethodGet, idpProject + "/users", "", `"email":"ada@example.com"`, 200},
			{http.MethodGet, idpProject + "/users?limit=1&offset=0", "", `"hasPassword":true`, 200},
			{http.MethodGet, idpProject + "/oobcodes", "", `"requestType":"PASSWORD_RESET"`, 200},
			{http.MethodGet, idpProject + "/tenants", "", `"displayName":"acme-tenant"`, 200},
			{http.MethodGet, idpProject + "/config", "", `"emailEnabled":true`, 200},
		}},
		{"user actions", []gcpStep{
			{http.MethodPost, user + "/disable", "", idpUserUID, 200},
			{http.MethodGet, idpProject + "/users", "", `"disabled":true`, 200},
			{http.MethodPost, user + "/enable", "", idpUserUID, 200},
			{http.MethodPost, user + "/claims", `{"claims":{"role":"admin"}}`, idpUserUID, 200},
			{http.MethodGet, idpProject + "/users", "", `role`, 200},
			{http.MethodPost, user + "/revoke", "", idpUserUID, 200},
			{http.MethodGet, idpProject + "/users", "", `"validSince"`, 200},
			{http.MethodDelete, user, "", "{}", 200},
			{http.MethodDelete, user, "", "USER_NOT_FOUND", 400},
		}},
		{"create", []gcpStep{
			{
				http.MethodPost,
				idpProject + "/users",
				`{"email":"bob@example.com","password":"` + idpPassword + `"}`,
				`"localId"`,
				200,
			},
			{http.MethodGet, idpProject + "/users", "", `"email":"bob@example.com"`, 200},
			{
				http.MethodPost,
				idpProject + "/users",
				`{"email":"bob@example.com","password":"` + idpPassword + `"}`,
				"EMAIL_EXISTS",
				400,
			},
			{http.MethodPost, idpProject + "/users", `{"email":"x@example.com"}`, "required", 400},
		}},
		{"tenant scope", []gcpStep{
			{http.MethodGet, idpProject + "/users?tenantId=nope-tenant", "", "TENANT_NOT_FOUND", 404},
		}},
		{"config toggles", []gcpStep{
			{http.MethodPatch, idpProject + "/config", `{"anonymousEnabled":false}`, `"anonymousEnabled":false`, 200},
			{http.MethodGet, idpProject + "/config", "", `"emailEnabled":true`, 200},
			{
				http.MethodPatch,
				idpProject + "/config",
				`{"emailEnabled":false,"anonymousEnabled":true}`,
				`"emailEnabled":false`,
				200,
			},
			{http.MethodPatch, idpProject + "/config", `{}`, "required", 400},
		}},
		{"claims validation", []gcpStep{
			{http.MethodPost, user + "/claims", `{"claims":"not-an-object"}`, "claims", 400},
			{http.MethodPost, user + "/claims", `{}`, "claims", 400},
			{http.MethodPost, user + "/claims", `{"claims":{"sub":"x"}}`, "FORBIDDEN_CLAIM", 400},
			{http.MethodPost, user + "/claims", `{"claims":{"big":"` + strings.Repeat("a", 1100) + `"}}`, "1000", 400},
		}},
		{"rejects", []gcpStep{
			{http.MethodPost, idpBase + "/projects", `{}`, "unsupported", 400},
			{http.MethodPost, idpProject + "/accounts:batchGet", `{}`, "unsupported", 400},
			{http.MethodPost, idpProject + "/users/" + idpUserUID + "/signIn", `{}`, "unsupported", 400},
			{http.MethodPost, idpProject + "/users/" + idpUserUID + "/update", `{}`, "unsupported", 400},
			{http.MethodDelete, idpProject + "/users", "", "unsupported", 400},
			{http.MethodDelete, idpProject + "/oobcodes", "", "unsupported", 400},
			{http.MethodDelete, idpProject + "/tenants", "", "unsupported", 400},
			{http.MethodPost, idpProject + "/tenants", `{}`, "unsupported", 400},
			{http.MethodGet, idpBase + "/projects/..%2fx/users", "", "unsupported", 400},
			{http.MethodGet, idpBase + "/projects/bad%20id/users", "", "unsupported", 400},
			{http.MethodGet, idpProject + "/users?tenantId=a%2Fb", "", "unsupported", 400},
			{http.MethodGet, idpProject + "/users?limit=0", "", "invalid", 400},
			{http.MethodGet, idpProject + "/users?limit=100000", "", "invalid", 400},
			{http.MethodGet, idpBase + "/token", "", "unsupported", 400},
			{http.MethodGet, idpBase + "/", "", "unsupported", 400},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runGCPSteps(t, newIDPDashboard(t), tt.steps)
		})
	}

	t.Run("hashes and keys stay server side", func(t *testing.T) {
		t.Parallel()

		h := newIDPDashboard(t)

		for _, p := range []string{
			idpProject + "/users", idpProject + "/config", idpProject + "/tenants",
		} {
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(echo.NewContext(httptest.NewRequest(http.MethodGet, p, nil), rec)))

			low := strings.ToLower(rec.Body.String())
			for _, banned := range []string{
				"passwordhash", "salt", "signerkey", "apikey", "fake-api-key", "hashconfig", strings.ToLower(idpPassword),
			} {
				assert.NotContains(t, low, banned, p)
			}
		}
	})

	t.Run("create does not echo the password", func(t *testing.T) {
		t.Parallel()

		h := newIDPDashboard(t)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, idpProject+"/users",
			strings.NewReader(`{"email":"c@example.com","password":"`+idpPassword+`"}`))
		require.NoError(t, h.Handler()(echo.NewContext(req, rec)))
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.NotContains(t, rec.Body.String(), idpPassword)
	})

	t.Run("unavailable without the service", func(t *testing.T) {
		t.Parallel()

		runGCPSteps(t, dashboard.NewHandler(dashboard.Config{}), []gcpStep{
			{http.MethodGet, idpProject + "/users", "", "", http.StatusServiceUnavailable},
			{http.MethodGet, idpBase + "/projects", "", "", http.StatusServiceUnavailable},
		})
	})
}
