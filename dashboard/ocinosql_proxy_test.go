package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/ocinosql"
)

const (
	nosqlComp = "ocid1.compartment.oc1..nosql"
	nosqlBase = "/dashboard/api/v1/oci/nosql"
	nosqlAPI  = nosqlBase + "/api"
)

func newNoSQLDashboard(t *testing.T) *dashboard.DashboardHandler {
	t.Helper()

	b := ocinosql.NewInMemoryBackend()
	b.SetTransition(0)

	ops := ocinosql.NewHandler(b)
	e := echo.New()
	e.Any("/*", ops.Trusted())

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/20190828/tables", strings.NewReader(
		`{"compartmentId":"`+nosqlComp+`","name":"users","ddlStatement":"CREATE TABLE users (id INTEGER, name STRING, `+
			`PRIMARY KEY(id))","tableLimits":{"maxReadUnits":10,"maxWriteUnits":10,"maxStorageInGBs":1}}`)))
	require.Less(t, rec.Code, http.StatusBadRequest, rec.Body.String())

	return dashboard.NewHandler(dashboard.Config{OCINoSQLOps: ops})
}

func TestOCINoSQLProxy(t *testing.T) {
	t.Parallel()

	tbl := nosqlAPI + "/tables/users"
	cq := "?compartmentId=" + nosqlComp

	tests := []struct {
		name  string
		steps []gcpStep
	}{
		{name: "read", steps: []gcpStep{
			{http.MethodGet, nosqlBase + "/compartments", "", nosqlComp, 200},
			{http.MethodGet, nosqlAPI + "/tables" + cq, "", `"name":"users"`, 200},
			{http.MethodGet, tbl + cq, "", `"ddlStatement"`, 200},
			{http.MethodGet, tbl + "/indexes" + cq, "", `"items"`, 200},
			{http.MethodGet, tbl + "/usage" + cq, "", `"items"`, 200},
		}},
		{name: "rows and query", steps: []gcpStep{
			{
				http.MethodPut,
				tbl + "/rows",
				`{"compartmentId":"` + nosqlComp + `","value":{"id":1,"name":"ada"}}`,
				`"version"`,
				200,
			},
			{http.MethodGet, tbl + "/rows" + cq + "&key=id:1", "", `"ada"`, 200},
			{
				http.MethodPost,
				nosqlAPI + "/query",
				`{"compartmentId":"` + nosqlComp + `","statement":"SELECT * FROM users"}`,
				`"ada"`,
				200,
			},
			{http.MethodDelete, tbl + "/rows" + cq + "&key=id:1", "", `"isSuccess":true`, 200},
		}},
		{name: "create and drop", steps: []gcpStep{
			{
				http.MethodPost,
				nosqlAPI + "/tables",
				`{"compartmentId":"` + nosqlComp + `","name":"logs","ddlStatement":` +
					`"CREATE TABLE logs (id INTEGER, PRIMARY KEY(id))",` +
					`"tableLimits":{"capacityMode":"ON_DEMAND","maxStorageInGBs":5}}`,
				"",
				http.StatusAccepted,
			},
			{http.MethodGet, nosqlAPI + "/tables/logs" + cq, "", `"capacityMode":"ON_DEMAND"`, 200},
			{http.MethodDelete, nosqlAPI + "/tables/logs" + cq, "", "", http.StatusAccepted},
		}},
		{name: "body rebuild", steps: []gcpStep{
			{
				http.MethodPost,
				nosqlAPI + "/tables",
				`{"compartmentId":"` + nosqlComp + `","name":"evil",` +
					`"ddlStatement":"CREATE TABLE evil (id INTEGER, PRIMARY KEY(id))",` +
					`"tableLimits":{"capacityMode":"ON_DEMAND","maxStorageInGBs":1},` +
					`"freeformTags":{"x":"y"},"isAutoReclaimable":true}`,
				"",
				http.StatusAccepted,
			},
			{http.MethodGet, nosqlAPI + "/tables/evil" + cq, "", `"isAutoReclaimable":false`, 200},
		}},
		{name: "rejects", steps: []gcpStep{
			{http.MethodPut, tbl + cq, `{}`, "unsupported", 400},
			{http.MethodPost, tbl + "/actions/changeCompartment", `{}`, "unsupported", 400},
			{http.MethodPost, tbl + "/indexes", `{}`, "unsupported", 400},
			{http.MethodGet, nosqlAPI + "/workRequests", "", "unsupported", 400},
			{http.MethodGet, nosqlAPI + "/query/prepare", "", "unsupported", 400},
			{http.MethodGet, nosqlAPI + "/tables/%2e%2e", "", "unsupported", 400},
			{
				http.MethodPost,
				nosqlAPI + "/query",
				`{"compartmentId":"c","statement":"DROP TABLE users"}`,
				"invalid",
				400,
			},
			{
				http.MethodPost,
				nosqlAPI + "/query",
				`{"compartmentId":"c","statement":"  insert into users values(1)"}`,
				"invalid",
				400,
			},
			{
				http.MethodPost,
				nosqlAPI + "/tables",
				`{"compartmentId":"c","name":"t","ddlStatement":"x","tableLimits":{"capacityMode":"WEIRD"}}`,
				"invalid",
				400,
			},
			{
				http.MethodPost,
				nosqlAPI + "/tables",
				`{"compartmentId":"c","name":"t","ddlStatement":"x",` +
					`"tableLimits":{"capacityMode":"PROVISIONED","maxReadUnits":-1}}`,
				"invalid",
				400,
			},
			{http.MethodPut, tbl + "/rows", `{"compartmentId":"c","value":[1]}`, "invalid", 400},
			{http.MethodPut, tbl + "/rows", `{"compartmentId":"c","value":{},"option":"DROP"}`, "invalid", 400},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runGCPSteps(t, newNoSQLDashboard(t), tt.steps)
		})
	}

	t.Run("unavailable without the service", func(t *testing.T) {
		t.Parallel()

		runGCPSteps(t, dashboard.NewHandler(dashboard.Config{}), []gcpStep{
			{http.MethodGet, nosqlAPI + "/tables", "", "", http.StatusServiceUnavailable},
		})
	})
}
