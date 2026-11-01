package dashboard_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/ocimysql"
)

const (
	mysqlComp = "ocid1.compartment.oc1..aaaa"
	mysqlBase = "/dashboard/api/v1/oci/mysql"
	mysqlPass = "Sup3rSecret!pw"
)

func mysqlSeed(t *testing.T, e *echo.Echo, method, path, body string) []byte {
	t.Helper()

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	require.Less(t, rec.Code, http.StatusBadRequest, "%s: %s", path, rec.Body.String())

	return rec.Body.Bytes()
}

func newMySQLDashboard(t *testing.T) (*dashboard.DashboardHandler, string) {
	t.Helper()

	b := ocimysql.NewInMemoryBackend()
	b.SetTransition(0)

	ops := ocimysql.NewHandler(b)
	e := echo.New()
	e.Any("/*", ops.Trusted())

	mysqlSeed(t, e, http.MethodPost, "/20190415/dbSystems",
		`{"compartmentId":"`+mysqlComp+`","displayName":"orders","shapeName":"MySQL.2",`+
			`"subnetId":"ocid1.subnet.oc1.iad.suba","adminUsername":"dbadmin","adminPassword":"`+mysqlPass+`"}`)

	var systems []struct {
		ID string `json:"id"`
	}

	require.NoError(t, json.Unmarshal(mysqlSeed(t, e, http.MethodGet,
		"/20190415/dbSystems?compartmentId="+mysqlComp, ""), &systems))
	require.Len(t, systems, 1)

	return dashboard.NewHandler(dashboard.Config{OCIMySQLOps: ops}), systems[0].ID
}

func TestOCIMySQLProxy(t *testing.T) {
	t.Parallel()

	api := mysqlBase + "/api"

	tests := []struct {
		steps func(id string) []gcpStep
		name  string
	}{
		{name: "read", steps: func(id string) []gcpStep {
			return []gcpStep{
				{http.MethodGet, mysqlBase + "/compartments", "", mysqlComp, 200},
				{http.MethodGet, api + "/dbSystems?compartmentId=" + mysqlComp, "", `"displayName":"orders"`, 200},
				{http.MethodGet, api + "/dbSystems/" + id, "", `"dataStorage"`, 200},
				{http.MethodGet, api + "/configurations?compartmentId=" + mysqlComp, "", `"type":"DEFAULT"`, 200},
				{http.MethodGet, api + "/backups?compartmentId=" + mysqlComp, "", "[", 200},
				{http.MethodGet, api + "/dbSystems/ocid1.mysqldbsystem.oc1..nope", "", "NotAuthorizedOrNotFound", 404},
			}
		}},
		{name: "lifecycle", steps: func(id string) []gcpStep {
			return []gcpStep{
				{http.MethodPost, api + "/dbSystems/" + id + "/actions/stop", "", "", http.StatusAccepted},
				{http.MethodGet, api + "/dbSystems/" + id, "", `"lifecycleState":"INACTIVE"`, 200},
				{http.MethodPost, api + "/dbSystems/" + id + "/actions/start", "", "", http.StatusAccepted},
				{http.MethodPost, api + "/dbSystems/" + id + "/actions/restart", "", "", http.StatusAccepted},
				{http.MethodDelete, api + "/dbSystems/" + id, "", "", http.StatusAccepted},
			}
		}},
		{name: "backup", steps: func(id string) []gcpStep {
			return []gcpStep{
				{
					http.MethodPost,
					api + "/backups",
					`{"dbSystemId":"` + id + `","displayName":"manual","retentionInDays":9999}`,
					`"displayName":"manual"`,
					200,
				},
				{
					http.MethodGet,
					api + "/backups?compartmentId=" + mysqlComp + "&dbSystemId=" + id,
					"",
					`"manual"`,
					200,
				},
				{http.MethodPost, api + "/backups", `{"displayName":"orphan"}`, "", 400},
			}
		}},
		{name: "rejects", steps: func(id string) []gcpStep {
			return []gcpStep{
				{http.MethodPost, api + "/dbSystems", `{}`, "unsupported", 400},
				{http.MethodPost, api + "/dbSystems/" + id, `{}`, "unsupported", 400},
				{http.MethodPost, api + "/dbSystems/" + id + "/actions/changeCompartment", `{}`, "unsupported", 400},
				{http.MethodDelete, api + "/backups/b1", "", "unsupported", 400},
				{http.MethodGet, api + "/channels", "", "unsupported", 400},
				{http.MethodGet, api + "/dbSystems/%2e%2e", "", "unsupported", 400},
				{http.MethodGet, api + "/", "", "unsupported", 400},
			}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, id := newMySQLDashboard(t)
			runGCPSteps(t, h, tt.steps(id))
		})
	}

	t.Run("passwords stay server side", func(t *testing.T) {
		t.Parallel()

		h, id := newMySQLDashboard(t)

		for _, p := range []string{api + "/dbSystems?compartmentId=" + mysqlComp, api + "/dbSystems/" + id} {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, p, nil)
			require.NoError(t, h.Handler()(echo.NewContext(req, rec)))
			assert.NotContains(t, strings.ToLower(rec.Body.String()), "password")
			assert.NotContains(t, rec.Body.String(), mysqlPass)
		}
	})

	t.Run("unavailable without the service", func(t *testing.T) {
		t.Parallel()

		runGCPSteps(t, dashboard.NewHandler(dashboard.Config{}), []gcpStep{
			{http.MethodGet, api + "/dbSystems", "", "", http.StatusServiceUnavailable},
		})
	})
}
