package dashboard_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/ocimonitoring"
)

const (
	monComp = "ocid1.compartment.oc1..aaaa"
	monBase = "/dashboard/api/v1/oci/monitoring"
)

func monSeed(t *testing.T, e *echo.Echo, method, path, body string) []byte {
	t.Helper()

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	require.Less(t, rec.Code, http.StatusBadRequest, "%s: %s", path, rec.Body.String())

	return rec.Body.Bytes()
}

func newMonitoringDashboard(t *testing.T) (*dashboard.DashboardHandler, string) {
	t.Helper()

	b := ocimonitoring.NewInMemoryBackend()
	ops := ocimonitoring.NewHandler(b)
	e := echo.New()
	e.Any("/*", ops.Trusted())

	now := time.Now().UTC().Truncate(time.Second)
	points := make([]string, 0, 3)

	for i, v := range []int{10, 90, 95} {
		points = append(points, `{"timestamp":"`+now.Add(-time.Duration(3-i)*time.Minute).Format(time.RFC3339)+
			`","value":`+strconv.Itoa(v)+`}`)
	}

	monSeed(t, e, http.MethodPost, "/20180401/metrics", `{"metricData":[{"namespace":"app","compartmentId":"`+monComp+
		`","name":"cpu","dimensions":{"host":"a"},"datapoints":[`+strings.Join(points, ",")+`]}]}`)
	monSeed(t, e, http.MethodPost, "/20180401/alarms", `{"displayName":"cpu-high","compartmentId":"`+monComp+
		`","metricCompartmentId":"`+monComp+`","namespace":"app","query":"cpu[1m].max() > 80","severity":"CRITICAL",`+
		`"destinations":["ocid1.onstopic.oc1..topic"],"isEnabled":true}`)

	var alarms []struct {
		ID string `json:"id"`
	}

	require.NoError(t, json.Unmarshal(monSeed(t, e, http.MethodGet,
		"/20180401/alarms?compartmentId="+monComp, ""), &alarms))
	require.Len(t, alarms, 1)

	return dashboard.NewHandler(dashboard.Config{OCIMonitoringOps: ops}), alarms[0].ID
}

func TestOCIMonitoringProxy(t *testing.T) {
	t.Parallel()

	api := monBase + "/api"
	span := func() string {
		now := time.Now().UTC()

		return `"startTime":"` + now.Add(-time.Hour).Format(time.RFC3339) + `","endTime":"` +
			now.Add(time.Minute).Format(time.RFC3339) + `"`
	}

	tests := []struct {
		steps func(alarm string) []gcpStep
		name  string
	}{
		{name: "alarms", steps: func(alarm string) []gcpStep {
			return []gcpStep{
				{http.MethodGet, monBase + "/compartments", "", monComp, 200},
				{http.MethodGet, api + "/alarms?compartmentId=" + monComp, "", `"displayName":"cpu-high"`, 200},
				{http.MethodGet, api + "/alarms/status?compartmentId=" + monComp, "", `"status":`, 200},
				{http.MethodGet, api + "/alarms/" + alarm, "", `"namespace":"app"`, 200},
				{http.MethodGet, api + "/alarms/" + alarm + "/history", "", `"entries"`, 200},
				{http.MethodGet, api + "/alarms/ocid1.alarm.oc1..nope", "", "NotAuthorizedOrNotFound", 404},
			}
		}},
		{name: "metrics", steps: func(string) []gcpStep {
			return []gcpStep{
				{http.MethodPost, api + "/metrics/actions/listMetrics?compartmentId=" + monComp,
					`{"namespace":"app","sortBy":"NAME"}`, `"name":"cpu"`, 200},
				{http.MethodPost, api + "/metrics/actions/summarizeMetricsData?compartmentId=" + monComp,
					`{"namespace":"app","query":"cpu[1m].max()",` + span() + `}`, `"aggregatedDatapoints"`, 200},
				{http.MethodPost, api + "/metrics/actions/summarizeMetricsData?compartmentId=" + monComp,
					`{"namespace":"app","query":"nonsense"}`, "", 400},
			}
		}},
		{name: "rejects", steps: func(alarm string) []gcpStep {
			return []gcpStep{
				{http.MethodPost, api + "/metrics", `{"metricData":[]}`, "unsupported", 400},
				{http.MethodPost, api + "/alarms", `{}`, "unsupported", 400},
				{http.MethodGet, api + "/metrics/actions/listMetrics", "", "unsupported", 400},
				{http.MethodGet, api + "/alarms/" + alarm + "/other", "", "unsupported", 400},
				{http.MethodGet, api + "/alarms/%2e%2e", "", "unsupported", 400},
				{http.MethodGet, api + "/", "", "unsupported", 400},
			}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, alarm := newMonitoringDashboard(t)
			runGCPSteps(t, h, tt.steps(alarm))
		})
	}

	t.Run("unserved", func(t *testing.T) {
		t.Parallel()

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, api+"/alarms?compartmentId="+monComp, nil)
		require.NoError(t, dashboard.NewHandler(dashboard.Config{}).Handler()(echo.NewContext(req, rec)))
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})
}
