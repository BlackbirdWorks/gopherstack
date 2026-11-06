package dashboard_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/azuresql"
)

const (
	sqlAPI      = "?api-version=2023-08-01-preview"
	sqlPxBase   = "/dashboard/api/v1/azure/sql"
	sqlQueryURL = "/dashboard/api/v1/azure/sqlquery"
	sqlPw       = "Adm1n-Secret-pw!"
	sqlServer   = armBase + "Microsoft.Sql/servers/srv1"
)

func newSQLARMDashboard(t *testing.T) *dashboard.DashboardHandler {
	t.Helper()

	svc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	armH, ok := svc.(*azurearm.Handler)
	require.True(t, ok)
	t.Cleanup(func() { armH.Shutdown(context.Background()) })

	h := dashboard.NewHandler(dashboard.Config{AzureARMOps: armH})
	seeds := []struct{ path, body string }{
		{"/subscriptions/" + armSub + "/resourceGroups/demo?api-version=2021-04-01", `{"location":"eastus"}`},
		{
			sqlServer + sqlAPI,
			`{"location":"eastus","properties":{"administratorLogin":"sqladmin","administratorLoginPassword":"` + sqlPw + `"}}`,
		},
		{
			sqlServer + "/databases/orders" + sqlAPI,
			`{"location":"eastus","sku":{"name":"S0","tier":"Standard"},"properties":{"maxSizeBytes":2147483648}}`,
		},
	}

	for _, s := range seeds {
		rec := doProxy(t, h, http.MethodPut, "/dashboard/api/v1/azure/arm"+s.path, s.body)
		require.Less(t, rec.Code, http.StatusBadRequest, "%s: %s", s.path, rec.Body.String())
	}

	return h
}

func TestAzureSQLProxy(t *testing.T) {
	t.Parallel()

	px := sqlPxBase + sqlServer
	list := sqlPxBase + "/subscriptions/" + armSub + "/providers/Microsoft.Sql/servers"

	tests := []struct {
		name  string
		steps []gcpStep
	}{
		{"read", []gcpStep{
			{http.MethodGet, list + sqlAPI, "", `"name":"srv1"`, 200},
			{http.MethodGet, sqlPxBase + armBase + "Microsoft.Sql/servers" + sqlAPI, "", `"name":"srv1"`, 200},
			{http.MethodGet, px + sqlAPI, "", `"administratorLogin":"sqladmin"`, 200},
			{http.MethodGet, px + "/databases" + sqlAPI, "", `"name":"orders"`, 200},
			{http.MethodGet, px + "/databases" + sqlAPI, "", `"maxSizeBytes":2147483648`, 200},
			{http.MethodGet, px + "/databases" + sqlAPI, "", `"status":"Online"`, 200},
			{http.MethodGet, px + "/databases/orders" + sqlAPI, "", `"name":"S0"`, 200},
			{http.MethodGet, px + "/firewallRules" + sqlAPI, "", `value`, 200},
		}},
		{"firewall writes", []gcpStep{
			{
				http.MethodPut, px + "/firewallRules/office" + sqlAPI,
				`{"properties":{"startIpAddress":"10.0.0.1","endIpAddress":"10.0.0.9"},"extra":"dropped"}`,
				`"startIpAddress":"10.0.0.1"`, 200,
			},
			{http.MethodGet, px + "/firewallRules" + sqlAPI, "", `"name":"office"`, 200},
			{http.MethodDelete, px + "/firewallRules/office" + sqlAPI, "", "", 200},
		}},
		{"rejects", []gcpStep{
			{http.MethodPut, px + sqlAPI, `{}`, "unsupported", 400},
			{http.MethodDelete, px + sqlAPI, "", "unsupported", 400},
			{http.MethodPut, px + "/databases/x" + sqlAPI, `{}`, "unsupported", 400},
			{http.MethodDelete, px + "/databases/orders" + sqlAPI, "", "unsupported", 400},
			{http.MethodGet, px + "/administrators" + sqlAPI, "", "unsupported", 400},
			{http.MethodGet, px + "/auditingSettings" + sqlAPI, "", "unsupported", 400},
			{http.MethodPost, px + "/databases" + sqlAPI, `{}`, "Method Not Allowed", 405},
			{http.MethodGet, px + "/databases/../x" + sqlAPI, "", "unsupported", 400},
			{http.MethodPut, list + sqlAPI, `{}`, "unsupported", 400},
			{
				http.MethodPut, px + "/firewallRules/bad" + sqlAPI,
				`{"properties":{"startIpAddress":"nope","endIpAddress":"1.1.1.1"}}`, "invalid", 400,
			},
			{
				http.MethodPut, px + "/firewallRules/bad" + sqlAPI,
				`{"properties":{"startIpAddress":"::1","endIpAddress":"::2"}}`, "invalid", 400,
			},
			{http.MethodPut, px + "/firewallRules/bad" + sqlAPI, `not json`, "invalid", 400},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runGCPSteps(t, newSQLARMDashboard(t), tt.steps)
		})
	}
}

func TestAzureSQLProxyNeverReturnsPassword(t *testing.T) {
	t.Parallel()

	h := newSQLARMDashboard(t)
	px := sqlPxBase + sqlServer

	for _, p := range []string{
		px + sqlAPI, sqlPxBase + "/subscriptions/" + armSub + "/providers/Microsoft.Sql/servers" + sqlAPI,
		px + "/databases" + sqlAPI,
	} {
		rec := doProxy(t, h, http.MethodGet, p, "")
		assert.NotContains(t, rec.Body.String(), sqlPw, p)
		assert.NotContains(t, strings.ToLower(rec.Body.String()), "administratorloginpassword", p)
	}
}

func TestAzureSQLProxyAbsent(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{})

	rec := doProxy(t, h, http.MethodGet, sqlPxBase+"/subscriptions/s/providers/Microsoft.Sql/servers"+sqlAPI, "")
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

	rec = doProxy(t, h, http.MethodPost, sqlQueryURL, `{"server":"x","database":"master","query":"SELECT 1"}`)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Contains(t, rec.Body.String(), "UNAVAILABLE")
}

type sqlTestEngine struct {
	gotDB string
	gotQ  string
}

func (*sqlTestEngine) Endpoint() (string, int)                              { return "127.0.0.1", 1 }
func (*sqlTestEngine) SetLogin(context.Context, string, string) error       { return nil }
func (*sqlTestEngine) CreateDatabase(context.Context, string, string) error { return nil }
func (*sqlTestEngine) DropDatabase(context.Context, string) error           { return nil }
func (*sqlTestEngine) ListDatabases(context.Context) ([]string, error)      { return nil, nil }
func (*sqlTestEngine) Close(context.Context)                                {}

func (e *sqlTestEngine) Query(_ context.Context, db, text string, _ int) (azuresql.QueryResult, error) {
	e.gotDB, e.gotQ = db, text

	return azuresql.QueryResult{Columns: []string{"one", "two"}, Rows: [][]string{{"1", "x"}}, Truncated: true}, nil
}

type sqlTestLauncher struct{ engine *sqlTestEngine }

func (l *sqlTestLauncher) Launch(context.Context, azuresql.LaunchSpec) (azuresql.Engine, error) {
	return l.engine, nil
}

func newSQLQueryDashboard(t *testing.T, withEngine bool) (*dashboard.DashboardHandler, *sqlTestEngine) {
	t.Helper()

	b := azuresql.NewInMemoryBackend()
	t.Cleanup(b.Close)

	eng := &sqlTestEngine{}
	if withEngine {
		b.SetLauncher(&sqlTestLauncher{engine: eng}, azuresql.DefaultStartupTimeout)
	}

	_, err := b.Ensure(strings.ToLower(sqlServer), azuresql.Spec{AdminLogin: "a", AdminPassword: sqlPw})
	require.NoError(t, err)

	return dashboard.NewHandler(dashboard.Config{AzureSQLOps: azuresql.NewHandler(b)}), eng
}

func TestAzureSQLQuery(t *testing.T) {
	t.Parallel()

	body := func(server, db, q string) string {
		return `{"server":"` + server + `","database":"` + db + `","query":"` + q + `"}`
	}

	tests := []struct {
		name       string
		body       string
		wantBody   string
		wantCode   int
		withEngine bool
	}{
		{
			name: "select", withEngine: true, body: body(sqlServer, "master", "SELECT 1"),
			wantCode: 200, wantBody: `"columns":["one","two"]`,
		},
		{
			name: "truncated flag", withEngine: true, body: body(sqlServer, "master", "SELECT 1"),
			wantCode: 200, wantBody: `"truncated":true`,
		},
		{
			name: "row cap", withEngine: true, body: body(sqlServer, "master", "SELECT 1"),
			wantCode: 200, wantBody: `"maxRows":500`,
		},
		{
			name: "delete rejected", withEngine: true, body: body(sqlServer, "master", "DELETE FROM t"),
			wantCode: 400, wantBody: "only SELECT",
		},
		{
			name: "batch rejected", withEngine: true, body: body(sqlServer, "master", "SELECT 1; DROP TABLE t"),
			wantCode: 400, wantBody: "single statement",
		},
		{
			name: "exec rejected", withEngine: true, body: body(sqlServer, "master", "SELECT 1 EXEC x"),
			wantCode: 400, wantBody: "EXEC",
		},
		{
			name: "bad database", withEngine: true, body: body(sqlServer, "a]b", "SELECT 1"),
			wantCode: 400, wantBody: "database name",
		},
		{
			name: "unknown server", withEngine: true, body: body("/subscriptions/x/servers/none", "master", "SELECT 1"),
			wantCode: 404, wantBody: "NOT_FOUND",
		},
		{
			name: "missing server", withEngine: true, body: `{"database":"master","query":"SELECT 1"}`,
			wantCode: 400, wantBody: "server is required",
		},
		{
			name: "state only", withEngine: false, body: body(sqlServer, "master", "SELECT 1"),
			wantCode: 409, wantBody: "state-only",
		},
		{name: "bad json", withEngine: true, body: `{`, wantCode: 400, wantBody: "invalid request body"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newSQLQueryDashboard(t, tt.withEngine)
			rec := doProxy(t, h, http.MethodPost, sqlQueryURL, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

func TestAzureSQLQueryReachesEngineOnlyWhenValid(t *testing.T) {
	t.Parallel()

	h, eng := newSQLQueryDashboard(t, true)

	rec := doProxy(
		t,
		h,
		http.MethodPost,
		sqlQueryURL,
		`{"server":"`+sqlServer+`","database":"app","query":"DROP TABLE t"}`,
	)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Empty(t, eng.gotQ)

	rec = doProxy(t, h, http.MethodPost, sqlQueryURL, `{"server":"`+sqlServer+`","database":"app","query":"SELECT 2"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "app", eng.gotDB)
	assert.Equal(t, "SELECT 2", eng.gotQ)
}

func TestAzureSQLQueryMethodAllowList(t *testing.T) {
	t.Parallel()

	h, _ := newSQLQueryDashboard(t, true)

	for _, m := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		req := httptest.NewRequest(m, sqlQueryURL, strings.NewReader(`{}`))
		rec := httptest.NewRecorder()
		require.NoError(t, h.Handler()(echo.NewContext(req, rec)))
		assert.NotEqual(t, http.StatusOK, rec.Code, m)
	}
}
