package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
)

const (
	acaAPI   = "?api-version=2025-07-01"
	acaProv  = armBase + "Microsoft.App"
	acaEnv   = acaProv + "/managedEnvironments/env"
	acaApp   = acaProv + "/containerApps/web"
	acaJob   = acaProv + "/jobs/nightly"
	acaProxy = "/dashboard/api/v1/azure/containerapps"
)

func newContainerAppsDashboard(t *testing.T) *dashboard.DashboardHandler {
	t.Helper()

	template := `{"containers":[{"name":"main","image":"nginx:1","resources":{"cpu":0.5,"memory":"1Gi"}}],` +
		`"scale":{"minReplicas":1,"maxReplicas":3}}`

	return newARMDashboard(t, []struct{ path, body string }{
		{"/subscriptions/" + armSub + "/resourceGroups/demo?api-version=2021-04-01", `{"location":"eastus"}`},
		{acaEnv + acaAPI, `{"location":"local","properties":{}}`},
		{acaApp + acaAPI, `{"location":"local","properties":{"environmentId":"` + acaEnv + `",` +
			`"configuration":{"activeRevisionsMode":"Multiple","ingress":{"external":true,"targetPort":80},` +
			`"secrets":[{"name":"token","value":"s3cr3t-value"}]},"template":` + template + `}}`},
		{acaJob + acaAPI, `{"location":"local","properties":{"environmentId":"` + acaEnv + `",` +
			`"configuration":{"triggerType":"Manual","replicaTimeout":60,"manualTriggerConfig":{}},"template":` +
			template + `}}`},
	})
}

func TestAzureContainerAppsProxy(t *testing.T) {
	t.Parallel()

	px := acaProxy + acaApp
	list := acaProxy + "/subscriptions/" + armSub + "/providers/Microsoft.App/"

	tests := []struct {
		name  string
		steps []gcpStep
	}{
		{"read", []gcpStep{
			{http.MethodGet, list + "containerApps" + acaAPI, "", `"name":"web"`, 200},
			{http.MethodGet, list + "managedEnvironments" + acaAPI, "", `"defaultDomain"`, 200},
			{http.MethodGet, list + "jobs" + acaAPI, "", `"name":"nightly"`, 200},
			{http.MethodGet, px + acaAPI, "", `"fqdn"`, 200},
			{http.MethodGet, px + "/revisions" + acaAPI, "", `"trafficWeight"`, 200},
			{http.MethodGet, acaProxy + acaJob + "/executions" + acaAPI, "", `"value"`, 200},
		}},
		{"app lifecycle", []gcpStep{
			{http.MethodPost, px + "/stop" + acaAPI, "", "", 200},
			{http.MethodGet, px + acaAPI, "", `"runningStatus":"Stopped"`, 200},
			{http.MethodPost, px + "/start" + acaAPI, "", "", 200},
			{http.MethodGet, px + acaAPI, "", `"runningStatus":"Running"`, 200},
		}},
		{"job start", []gcpStep{
			{http.MethodPost, acaProxy + acaJob + "/start" + acaAPI, `{}`, `"name"`, 200},
			{http.MethodGet, acaProxy + acaJob + "/executions" + acaAPI, "", `"status"`, 200},
		}},
		{"rejects", []gcpStep{
			{http.MethodDelete, px + acaAPI, "", "", http.StatusMethodNotAllowed},
			{http.MethodPut, px + acaAPI, `{}`, "", http.StatusMethodNotAllowed},
			{http.MethodPost, px + "/listSecrets" + acaAPI, "", "unsupported", 400},
			{http.MethodPost, acaProxy + acaJob + "/listSecrets" + acaAPI, "", "unsupported", 400},
			{http.MethodPost, px + "/revisions/x/delete" + acaAPI, "", "unsupported", 400},
			{http.MethodGet, px + "/authConfigs" + acaAPI, "", "unsupported", 400},
			{http.MethodGet, acaProxy + "/subscriptions/" + armSub + "/providers/Microsoft.Web/sites" + acaAPI, "",
				"unsupported", 400},
			{http.MethodGet, px + "/revisions/../../x" + acaAPI, "", "unsupported", 400},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runGCPSteps(t, newContainerAppsDashboard(t), tt.steps)
		})
	}

	t.Run("revision actions", func(t *testing.T) {
		t.Parallel()

		h := newContainerAppsDashboard(t)
		rec := httptest.NewRecorder()
		require.NoError(t, h.Handler()(echo.NewContext(
			httptest.NewRequest(http.MethodGet, px+"/revisions"+acaAPI, nil), rec)))
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		name := jsonField(t, rec.Body.Bytes(), "value", 0, "name")
		rev := px + "/revisions/" + name

		runGCPSteps(t, h, []gcpStep{
			{http.MethodPost, rev + "/restart" + acaAPI, "", "", 200},
			{http.MethodPost, rev + "/deactivate" + acaAPI, "", "", 200},
			{http.MethodGet, rev + acaAPI, "", `"active":false`, 200},
			{http.MethodPost, rev + "/activate" + acaAPI, "", "", 200},
			{http.MethodGet, rev + acaAPI, "", `"active":true`, 200},
		})
	})

	t.Run("secrets stay server side", func(t *testing.T) {
		t.Parallel()

		h := newContainerAppsDashboard(t)
		for _, p := range []string{px + acaAPI, list + "containerApps" + acaAPI, px + "/revisions" + acaAPI} {
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(echo.NewContext(httptest.NewRequest(http.MethodGet, p, nil), rec)))
			assert.NotContains(t, rec.Body.String(), "s3cr3t-value", p)
		}
	})

	t.Run("unserved", func(t *testing.T) {
		t.Parallel()

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, list+"containerApps"+acaAPI, nil)
		require.NoError(t, dashboard.NewHandler(dashboard.Config{}).Handler()(echo.NewContext(req, rec)))
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})
}
