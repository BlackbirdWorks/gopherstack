package dashboard_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/ocilogging"
)

const (
	logComp = "ocid1.compartment.oc1..aaaa"
	logBase = "/dashboard/api/v1/oci/logging"
)

func logSeed(t *testing.T, e *echo.Echo, method, path, body string) []byte {
	t.Helper()

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	require.Less(t, rec.Code, http.StatusBadRequest, "%s: %s", path, rec.Body.String())

	return rec.Body.Bytes()
}

func newLoggingDashboard(t *testing.T) (*dashboard.DashboardHandler, string, string) {
	t.Helper()

	b := ocilogging.NewInMemoryBackend()
	b.SetTransition(0)

	ops := ocilogging.NewHandler(b)
	e := echo.New()
	e.Any("/*", ops.Trusted())

	logSeed(t, e, http.MethodPost, "/20200531/logGroups",
		`{"compartmentId":"`+logComp+`","displayName":"apps"}`)

	var groups []struct {
		ID string `json:"id"`
	}

	require.NoError(t, json.Unmarshal(logSeed(t, e, http.MethodGet,
		"/20200531/logGroups?compartmentId="+logComp, ""), &groups))
	require.Len(t, groups, 1)

	logSeed(t, e, http.MethodPost, "/20200531/logGroups/"+groups[0].ID+"/logs",
		`{"displayName":"web","logType":"CUSTOM","isEnabled":true}`)

	var logs []struct {
		ID string `json:"id"`
	}

	require.NoError(t, json.Unmarshal(logSeed(t, e, http.MethodGet,
		"/20200531/logGroups/"+groups[0].ID+"/logs", ""), &logs))
	require.Len(t, logs, 1)

	return dashboard.NewHandler(dashboard.Config{OCILoggingOps: ops}), groups[0].ID, logs[0].ID
}

func TestOCILoggingProxy(t *testing.T) {
	t.Parallel()

	api := logBase + "/api"
	span := func() string {
		now := time.Now().UTC()

		return `"timeStart":"` + now.Add(-time.Hour).Format(time.RFC3339) + `","timeEnd":"` +
			now.Add(time.Hour).Format(time.RFC3339) + `"`
	}

	tests := []struct {
		steps func(group, log string) []gcpStep
		name  string
	}{
		{name: "read", steps: func(group, log string) []gcpStep {
			return []gcpStep{
				{http.MethodGet, logBase + "/compartments", "", logComp, 200},
				{http.MethodGet, api + "/logGroups?compartmentId=" + logComp, "", `"displayName":"apps"`, 200},
				{http.MethodGet, api + "/logGroups/" + group, "", `"displayName":"apps"`, 200},
				{http.MethodGet, api + "/logGroups/" + group + "/logs", "", `"displayName":"web"`, 200},
				{http.MethodGet, api + "/logGroups/" + group + "/logs/" + log, "", `"logType":"CUSTOM"`, 200},
				{http.MethodGet, api + "/logGroups/ocid1.loggroup.oc1..nope", "", "NotAuthorizedOrNotFound", 404},
			}
		}},
		{name: "enable and disable", steps: func(group, log string) []gcpStep {
			path := api + "/logGroups/" + group + "/logs/" + log

			return []gcpStep{
				{http.MethodPut, path, `{"isEnabled":false,"displayName":"renamed"}`, "", http.StatusAccepted},
				{http.MethodGet, path, "", `"isEnabled":false`, 200},
				{http.MethodGet, path, "", `"displayName":"web"`, 200},
				{http.MethodPut, path, `{"displayName":"renamed"}`, "unsupported", 400},
				{http.MethodPut, path, `{"isEnabled":true}`, "", http.StatusAccepted},
			}
		}},
		{name: "push and search", steps: func(_, log string) []gcpStep {
			return []gcpStep{
				{http.MethodPost, api + "/logs/" + log + "/actions/push", `{"data":"hello dashboard"}`, "", 200},
				{http.MethodPost, api + "/search?limit=10", `{` + span() + `,"searchQuery":"search \"` + logComp +
					`\" | where logContent = '*hello*'"}`, "hello dashboard", 200},
				{http.MethodPost, api + "/search", `{` + span() + `,"searchQuery":"search \"` + logComp +
					`\" | select x"}`, "", 400},
				{http.MethodPost, api + "/logs/" + log + "/actions/push", `{"data":""}`, "unsupported", 400},
			}
		}},
		{name: "rejects", steps: func(group, log string) []gcpStep {
			return []gcpStep{
				{http.MethodPost, api + "/logGroups", `{}`, "unsupported", 400},
				{http.MethodPut, api + "/logGroups/" + group, `{}`, "unsupported", 400},
				{http.MethodGet, api + "/unifiedAgentConfigurations", "", "unsupported", 400},
				{http.MethodGet, api + "/search", "", "unsupported", 400},
				{http.MethodPost, api + "/logs/" + log + "/actions/other", `{}`, "unsupported", 400},
				{http.MethodGet, api + "/logGroups/%2e%2e", "", "unsupported", 400},
				{http.MethodGet, api + "/", "", "unsupported", 400},
			}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, group, log := newLoggingDashboard(t)
			runGCPSteps(t, h, tt.steps(group, log))
		})
	}

	t.Run("unserved", func(t *testing.T) {
		t.Parallel()

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, api+"/logGroups?compartmentId="+logComp, nil)
		require.NoError(t, dashboard.NewHandler(dashboard.Config{}).Handler()(echo.NewContext(req, rec)))
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})
}
