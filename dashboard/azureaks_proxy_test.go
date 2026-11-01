package dashboard_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
)

const (
	aksAPI   = "?api-version=2025-05-01"
	aksProv  = armBase + "Microsoft.ContainerService"
	aksCl    = aksProv + "/managedClusters/prod"
	aksProxy = "/dashboard/api/v1/azure/aks"
)

func newAKSDashboard(t *testing.T) *dashboard.DashboardHandler {
	t.Helper()

	return newARMDashboard(t, []struct{ path, body string }{
		{"/subscriptions/" + armSub + "/resourceGroups/demo?api-version=2021-04-01", `{"location":"eastus"}`},
		{aksCl + aksAPI, `{"location":"eastus","identity":{"type":"SystemAssigned"},"properties":{` +
			`"dnsPrefix":"prod","kubernetesVersion":"1.35","servicePrincipalProfile":{"clientId":"cid","secret":"sp-s3cret"},` +
			`"windowsProfile":{"adminUsername":"azureuser","adminPassword":"win-p4ssw0rd"},` +
			`"agentPoolProfiles":[{"name":"system","mode":"System","vmSize":"Standard_D4s_v3","count":2,` +
			`"osType":"Linux","availabilityZones":["1","2"]}]}}`},
	})
}

func TestAzureAKSProxy(t *testing.T) {
	t.Parallel()

	list := aksProxy + "/subscriptions/" + armSub + "/providers/Microsoft.ContainerService/managedClusters"
	px := aksProxy + aksCl

	tests := []struct {
		name  string
		steps []gcpStep
	}{
		{"read", []gcpStep{
			{http.MethodGet, list + aksAPI, "", `"name":"prod"`, 200},
			{http.MethodGet, px + aksAPI, "", `"powerState":{"code":"Running"}`, 200},
			{http.MethodGet, px + "/agentPools" + aksAPI, "", `"name":"system"`, 200},
			{http.MethodGet, px + "/agentPools/system" + aksAPI, "", `"count":2`, 200},
			{http.MethodGet, px + "/upgradeProfiles/default" + aksAPI, "", `"controlPlaneProfile"`, 200},
		}},
		{"power", []gcpStep{
			{http.MethodPost, px + "/stop" + aksAPI, "", "", http.StatusAccepted},
			{http.MethodGet, px + aksAPI, "", `"powerState":{"code":"Stopped"}`, 200},
			{http.MethodPost, px + "/start" + aksAPI, "", "", http.StatusAccepted},
			{http.MethodGet, px + aksAPI, "", `"powerState":{"code":"Running"}`, 200},
		}},
		{"scale", []gcpStep{
			{http.MethodPut, px + "/agentPools/system" + aksAPI,
				`{"properties":{"mode":"System","vmSize":"Standard_D4s_v3","count":4,"osType":"Linux",` +
					`"availabilityZones":["1","2"]}}`, `"count":4`, 200},
			{http.MethodGet, px + "/agentPools/system" + aksAPI, "", `"count":4`, 200},
		}},
		{"delete", []gcpStep{
			{http.MethodDelete, px + aksAPI, "", "", http.StatusAccepted},
			{http.MethodGet, px + aksAPI, "", "ResourceNotFound", 404},
		}},
		{
			"rejects",
			[]gcpStep{
				{http.MethodPost, px + "/listClusterAdminCredential" + aksAPI, "", "unsupported", 400},
				{http.MethodPost, px + "/listClusterUserCredential" + aksAPI, "", "unsupported", 400},
				{http.MethodPost, px + "/runCommand" + aksAPI, `{}`, "unsupported", 400},
				{http.MethodPost, px + "/agentPools/system/upgradeNodeImageVersion" + aksAPI, "", "unsupported", 400},
				{http.MethodDelete, px + "/agentPools/system" + aksAPI, "", "unsupported", 400},
				{http.MethodPut, px + aksAPI, `{}`, "unsupported", 400},
				{http.MethodPut, list + aksAPI, `{}`, "unsupported", 400},
				{http.MethodDelete, list + aksAPI, "", "unsupported", 400},
				{http.MethodGet, px + "/accessProfiles/clusterAdmin" + aksAPI, "", "unsupported", 400},
				{http.MethodGet, px + "/agentPools/../x" + aksAPI, "", "unsupported", 400},
				{
					http.MethodGet,
					aksProxy + "/subscriptions/" + armSub + "/providers/Microsoft.Compute/virtualMachines" + aksAPI,
					"",
					"unsupported",
					400,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runGCPSteps(t, newAKSDashboard(t), tt.steps)
		})
	}

	t.Run("credentials stay server side", func(t *testing.T) {
		t.Parallel()

		h := newAKSDashboard(t)
		for _, p := range []string{px + aksAPI, list + aksAPI, px + "/agentPools" + aksAPI} {
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(echo.NewContext(httptest.NewRequest(http.MethodGet, p, nil), rec)))

			for _, secret := range []string{"sp-s3cret", "win-p4ssw0rd", "kubeconfig", "certificate-authority-data"} {
				assert.NotContains(t, rec.Body.String(), secret, p)
			}
		}
	})

	t.Run("scale round trips the pool as read", func(t *testing.T) {
		t.Parallel()

		h := newAKSDashboard(t)
		pool := px + "/agentPools/system" + aksAPI
		rec := httptest.NewRecorder()
		require.NoError(t, h.Handler()(echo.NewContext(httptest.NewRequest(http.MethodGet, pool, nil), rec)))

		var doc map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &doc))

		props, ok := doc["properties"].(map[string]any)
		require.True(t, ok)

		props["count"] = 5
		body, err := json.Marshal(map[string]any{"properties": props})
		require.NoError(t, err)

		runGCPSteps(t, h, []gcpStep{
			{http.MethodPut, pool, string(body), `"count":5`, 200},
		})
	})

	t.Run("unserved", func(t *testing.T) {
		t.Parallel()

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, list+aksAPI, nil)
		require.NoError(t, dashboard.NewHandler(dashboard.Config{}).Handler()(echo.NewContext(req, rec)))
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})
}
