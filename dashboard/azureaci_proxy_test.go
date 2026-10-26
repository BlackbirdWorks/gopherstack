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
	"github.com/blackbirdworks/gopherstack/pkgs/aadauth"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

const (
	aciSub    = "00000000-0000-0000-0000-000000000000"
	aciAPI    = "?api-version=2023-05-01"
	aciRGPath = "/subscriptions/" + aciSub + "/resourceGroups/demo"
	aciGroup  = aciRGPath + "/providers/Microsoft.ContainerInstance/containerGroups/web"
	aciSecret = "hunter2-very-secret"
	aciBody   = `{"location":"eastus","properties":{"osType":"Linux","restartPolicy":"Never",` +
		`"imageRegistryCredentials":[{"server":"registry.example.com","username":"u","password":"` + aciSecret + `"}],` +
		`"containers":[{"name":"app","properties":{"image":"alpine:3","command":["sleep","60"],` +
		`"environmentVariables":[{"name":"PLAIN","value":"visible"},{"name":"TOKEN","secureValue":"` + aciSecret + `"}],` +
		`"resources":{"requests":{"cpu":1,"memoryInGB":1}}}}]}}`
)

func newACIDashboard(t *testing.T) *dashboard.DashboardHandler {
	t.Helper()

	backend := azurearm.NewInMemoryBackend()
	registry := azurearm.NewRegistry(backend)
	engine := azurearm.NewACIEngine(nil, "no container runtime in this test")
	t.Cleanup(engine.Close)
	registry.Register(azurearm.NewContainerInstanceProvider(engine))

	issuer, err := aadauth.NewIssuer()
	require.NoError(t, err)

	h := dashboard.NewHandler(dashboard.Config{
		AzureARMOps: azurearm.NewHandler(backend, registry, issuer, azurearm.DefaultSettings()),
	})

	for _, s := range []struct{ path, body string }{
		{aciRGPath + "?api-version=2021-04-01", `{"location":"eastus"}`},
		{aciGroup + aciAPI, aciBody},
	} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPut, "/dashboard/api/v1/azure/arm"+s.path, strings.NewReader(s.body))
		require.NoError(t, h.Handler()(echo.NewContext(req, rec)))
		require.Less(t, rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	return h
}

func TestAzureACIProxy(t *testing.T) {
	t.Parallel()

	const (
		root = "/dashboard/api/v1/azure/aci/subscriptions/" + aciSub
		grp  = "/dashboard/api/v1/azure/aci" + aciGroup
	)

	tests := []struct {
		name  string
		steps []gcpStep
	}{
		{"browse", []gcpStep{
			{http.MethodGet, root + "/resourcegroups?api-version=2021-04-01", "", "InvalidRequestUri", 400},
			{http.MethodGet, root + "/providers/Microsoft.ContainerInstance/containerGroups" + aciAPI, "", `"web"`, 200},
			{
				http.MethodGet, root + "/resourceGroups/demo/providers/Microsoft.ContainerInstance/containerGroups" + aciAPI,
				"", `"restartPolicy":"Never"`, 200,
			},
			{http.MethodGet, grp + aciAPI, "", `"provisioningState":"Failed"`, 200},
			{http.MethodGet, grp + aciAPI, "", `"PLAIN"`, 200},
			{http.MethodGet, grp + "/containers/app/logs" + aciAPI + "&tail=10", "", "ContainerGroupNotRunning", 400},
		}},
		{"actions", []gcpStep{
			{http.MethodPost, grp + "/stop" + aciAPI, "", "", 204},
			{http.MethodPost, grp + "/start" + aciAPI, "", "", 204},
			{http.MethodPost, grp + "/restart" + aciAPI, "", "", 204},
			{http.MethodDelete, grp + aciAPI, "", "", 200},
			{http.MethodGet, grp + aciAPI, "", "ResourceNotFound", 404},
		}},
		{"rejects", []gcpStep{
			{http.MethodPut, grp + aciAPI, aciBody, "Method Not Allowed", 405},
			{http.MethodPatch, grp + aciAPI, "{}", "Method Not Allowed", 405},
			{http.MethodPost, grp + "/exec" + aciAPI, "{}", "InvalidRequestUri", 400},
			{http.MethodPost, grp + aciAPI, "{}", "InvalidRequestUri", 400},
			{http.MethodGet, root + "/providers/Microsoft.Storage/storageAccounts" + aciAPI, "", "InvalidRequestUri", 400},
			{
				http.MethodGet, root + "/resourceGroups/demo/providers/Microsoft.Storage/storageAccounts/x" + aciAPI,
				"", "InvalidRequestUri", 400,
			},
			{http.MethodGet, "/dashboard/api/v1/azure/aci/metadata/endpoints", "", "InvalidRequestUri", 400},
			{
				http.MethodGet, root + "/resourceGroups/%2e%2e/providers/Microsoft.ContainerInstance/containerGroups" + aciAPI,
				"", "InvalidRequestUri", 400,
			},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runGCPSteps(t, newACIDashboard(t), tt.steps)
		})
	}
}

func TestAzureACIProxyNeverReturnsSecrets(t *testing.T) {
	t.Parallel()

	h := newACIDashboard(t)

	for _, path := range []string{
		"/dashboard/api/v1/azure/aci" + aciGroup + aciAPI,
		"/dashboard/api/v1/azure/aci" + aciRGPath + "/providers/Microsoft.ContainerInstance/containerGroups" + aciAPI,
		"/dashboard/api/v1/azure/aci/subscriptions/" + aciSub + "/providers/Microsoft.ContainerInstance/containerGroups" + aciAPI,
		"/dashboard/api/v1/azure/aci" + aciGroup + "/containers/app/logs" + aciAPI,
	} {
		rec := httptest.NewRecorder()
		require.NoError(t, h.Handler()(echo.NewContext(httptest.NewRequest(http.MethodGet, path, nil), rec)))
		assert.NotEqual(t, http.StatusInternalServerError, rec.Code, path)
		assert.NotContains(t, rec.Body.String(), aciSecret, path)
		assert.NotContains(t, rec.Body.String(), "secureValue", path)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/dashboard/api/v1/azure/aci"+aciGroup+aciAPI, nil)
	require.NoError(t, h.Handler()(echo.NewContext(req, rec)))
	assert.Contains(t, rec.Body.String(), `"name":"TOKEN"`)
	assert.Contains(t, rec.Body.String(), `"username":"u"`)
}

func TestAzureACIProxyUnserved(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/dashboard/api/v1/azure/aci/subscriptions/"+aciSub+"/providers/Microsoft.ContainerInstance/containerGroups", nil)

	h := dashboard.NewHandler(dashboard.Config{})
	require.NoError(t, h.Handler()(echo.NewContext(req, rec)))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}
