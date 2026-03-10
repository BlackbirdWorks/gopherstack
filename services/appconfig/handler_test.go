package appconfig_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

func newTestHandler(t *testing.T) *appconfig.Handler {
	t.Helper()

	return appconfig.NewHandler(appconfig.NewInMemoryBackend())
}

func doRequest(
	t *testing.T,
	h *appconfig.Handler,
	method, path string,
	body []byte,
) *httptest.ResponseRecorder {
	t.Helper()

	var reqBody *bytes.Reader
	if body != nil {
		reqBody = bytes.NewReader(body)
	} else {
		reqBody = bytes.NewReader(nil)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, reqBody)
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(logger.Save(t.Context(), slog.Default()))

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "AppConfig", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateApplication")
	assert.Contains(t, ops, "GetApplication")
	assert.Contains(t, ops, "ListApplications")
	assert.Contains(t, ops, "DeleteApplication")
	assert.Contains(t, ops, "CreateDeploymentStrategy")
	assert.Contains(t, ops, "StartDeployment")
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 86, h.MatchPriority())
}

func TestHandler_ChaosInterface(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "appconfig", h.ChaosServiceName())
	assert.Equal(t, []string{"us-east-1"}, h.ChaosRegions())
	assert.NotEmpty(t, h.ChaosOperations())
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "applications prefix", path: "/applications", want: true},
		{name: "applications with id", path: "/applications/abc123", want: true},
		{name: "deploymentstrategies prefix", path: "/deploymentstrategies", want: true},
		{name: "deploymentstrategies with id", path: "/deploymentstrategies/strat-1", want: true},
		{name: "not matched", path: "/restapis/something", want: false},
		{name: "dashboard", path: "/dashboard/appconfig", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.RouteMatcher()(c))
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		want   string
	}{
		{name: "list applications", method: http.MethodGet, path: "/applications", want: "ListApplications"},
		{name: "create application", method: http.MethodPost, path: "/applications", want: "CreateApplication"},
		{name: "get application", method: http.MethodGet, path: "/applications/app-1", want: "GetApplication"},
		{name: "delete application", method: http.MethodDelete, path: "/applications/app-1", want: "DeleteApplication"},
		{
			name:   "list strategies",
			method: http.MethodGet,
			path:   "/deploymentstrategies",
			want:   "ListDeploymentStrategies",
		},
		{
			name:   "create strategy",
			method: http.MethodPost,
			path:   "/deploymentstrategies",
			want:   "CreateDeploymentStrategy",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

func TestHandler_Application_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		wantName   string
		body       []byte
		wantStatus int
	}{
		{
			name:       "create application",
			method:     http.MethodPost,
			path:       "/applications",
			body:       []byte(`{"name":"my-app","description":"test"}`),
			wantStatus: http.StatusCreated,
			wantName:   "my-app",
		},
		{
			name:       "list applications empty",
			method:     http.MethodGet,
			path:       "/applications",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantName != "" {
				var app appconfig.Application
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))
				assert.Equal(t, tt.wantName, app.Name)
			}
		})
	}
}

func TestHandler_GetApplication_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/applications/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_DeleteApplication(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create an application first.
	rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"name":"delete-me"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var app appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))

	// Delete it.
	rec = doRequest(t, h, http.MethodDelete, "/applications/"+app.ID, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Get should return 404 now.
	rec = doRequest(t, h, http.MethodGet, "/applications/"+app.ID, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_UpdateApplication(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"name":"original"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var app appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))

	rec = doRequest(
		t,
		h,
		http.MethodPatch,
		"/applications/"+app.ID,
		[]byte(`{"name":"updated","description":"new desc"}`),
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	var updated appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
	assert.Equal(t, "updated", updated.Name)
	assert.Equal(t, "new desc", updated.Description)
}

func TestHandler_Environment_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create app.
	rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"name":"env-app"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var app appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))

	// Create env.
	rec = doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/environments", []byte(`{"name":"production"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var env appconfig.Environment
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))
	assert.Equal(t, "production", env.Name)
	assert.Equal(t, app.ID, env.ApplicationID)

	// Get env.
	rec = doRequest(t, h, http.MethodGet, "/applications/"+app.ID+"/environments/"+env.ID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List envs.
	rec = doRequest(t, h, http.MethodGet, "/applications/"+app.ID+"/environments", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete env.
	rec = doRequest(t, h, http.MethodDelete, "/applications/"+app.ID+"/environments/"+env.ID, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestHandler_ConfigurationProfile_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"name":"prof-app"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var app appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))

	// Create profile.
	profileBody := []byte(`{"name":"my-config","locationUri":"hosted","type":"AWS.Freeform"}`)
	rec = doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/configurationprofiles", profileBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	var profile appconfig.ConfigurationProfile
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &profile))
	assert.Equal(t, "my-config", profile.Name)

	// Get profile.
	rec = doRequest(t, h, http.MethodGet, "/applications/"+app.ID+"/configurationprofiles/"+profile.ID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete profile.
	rec = doRequest(t, h, http.MethodDelete, "/applications/"+app.ID+"/configurationprofiles/"+profile.ID, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestHandler_DeploymentStrategy_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := []byte(`{"name":"my-strategy","deploymentDurationInMinutes":0,` +
		`"finalBakeTimeInMinutes":0,"growthFactor":100,"growthType":"LINEAR","replicateTo":"NONE"}`)
	rec := doRequest(t, h, http.MethodPost, "/deploymentstrategies", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var strategy appconfig.DeploymentStrategy
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &strategy))
	assert.Equal(t, "my-strategy", strategy.Name)

	// Get.
	rec = doRequest(t, h, http.MethodGet, "/deploymentstrategies/"+strategy.ID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List.
	rec = doRequest(t, h, http.MethodGet, "/deploymentstrategies", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Update.
	rec = doRequest(t, h, http.MethodPatch, "/deploymentstrategies/"+strategy.ID, []byte(`{"name":"updated-strategy"}`))
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete.
	rec = doRequest(t, h, http.MethodDelete, "/deploymentstrategies/"+strategy.ID, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestHandler_DeploymentStrategy_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/deploymentstrategies/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_HostedConfigurationVersion_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create app and profile first.
	rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"name":"hcv-app"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var app appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))

	profileBody := []byte(`{"name":"hcv-profile","locationUri":"hosted","type":"AWS.Freeform"}`)
	rec = doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/configurationprofiles", profileBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	var profile appconfig.ConfigurationProfile
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &profile))

	// Create hosted configuration version with raw body.
	e := echo.New()
	content := []byte(`{"feature":"enabled"}`)
	req := httptest.NewRequest(http.MethodPost,
		"/applications/"+app.ID+"/configurationprofiles/"+profile.ID+"/hostedconfigurationversions",
		bytes.NewReader(content),
	)
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(logger.Save(t.Context(), slog.Default()))
	recHcv := httptest.NewRecorder()
	c := e.NewContext(req, recHcv)
	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, recHcv.Code)

	// List versions.
	rec = doRequest(t, h, http.MethodGet,
		"/applications/"+app.ID+"/configurationprofiles/"+profile.ID+"/hostedconfigurationversions", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Get version 1.
	req2 := httptest.NewRequest(http.MethodGet,
		"/applications/"+app.ID+"/configurationprofiles/"+profile.ID+"/hostedconfigurationversions/1",
		nil,
	)
	req2 = req2.WithContext(logger.Save(t.Context(), slog.Default()))
	rec2 := httptest.NewRecorder()
	c2 := e.NewContext(req2, rec2)
	err = h.Handler()(c2)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Equal(t, content, rec2.Body.Bytes())

	// Delete version 1.
	rec = doRequest(t, h, http.MethodDelete,
		"/applications/"+app.ID+"/configurationprofiles/"+profile.ID+"/hostedconfigurationversions/1", nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestHandler_Deployment_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create app.
	rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"name":"deploy-app"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var app appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))

	// Create env.
	rec = doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/environments", []byte(`{"name":"staging"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var env appconfig.Environment
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))

	// Start deployment.
	depBody := []byte(`{"configurationProfileId":"prof-1","deploymentStrategyId":"strat-1","configurationVersion":"1"}`)
	rec = doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/environments/"+env.ID+"/deployments", depBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	var dep appconfig.Deployment
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &dep))
	assert.Equal(t, int32(1), dep.DeploymentNumber)
	assert.Equal(t, "COMPLETE", dep.State)

	// Get deployment.
	rec = doRequest(t, h, http.MethodGet, "/applications/"+app.ID+"/environments/"+env.ID+"/deployments/1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List deployments.
	rec = doRequest(t, h, http.MethodGet, "/applications/"+app.ID+"/environments/"+env.ID+"/deployments", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Stop deployment.
	rec = doRequest(t, h, http.MethodDelete, "/applications/"+app.ID+"/environments/"+env.ID+"/deployments/1", nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestHandler_UnknownRoute(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/unknown/path", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestProvider_NameAndInit(t *testing.T) {
	t.Parallel()

	p := appconfig.Provider{}
	assert.Equal(t, "AppConfig", p.Name())

	result, err := p.Init(nil)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestBackend_GetApplication_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend()
	_, err := b.GetApplication("nonexistent")
	require.Error(t, err)
}

func TestBackend_ListApplications_Empty(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend()
	apps := b.ListApplications()
	assert.Empty(t, apps)
}

func TestBackend_DeleteApplication_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend()
	err := b.DeleteApplication("nonexistent")
	require.Error(t, err)
}

func TestBackend_CreateEnvironment_AppNotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend()
	_, err := b.CreateEnvironment("nonexistent", "env", "")
	require.Error(t, err)
}

func TestBackend_CreateDeploymentStrategy(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend()
	s, err := b.CreateDeploymentStrategy("strat", "desc", 0, 0, 100, "LINEAR", "NONE")
	require.NoError(t, err)
	assert.Equal(t, "strat", s.Name)
	assert.NotEmpty(t, s.ID)

	strategies := b.ListDeploymentStrategies()
	assert.Len(t, strategies, 1)
}

func TestBackend_UpdateDeploymentStrategy_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend()
	_, err := b.UpdateDeploymentStrategy("nonexistent", "name", "", 0, 0, 0)
	require.Error(t, err)
}

func TestBackend_DeleteDeploymentStrategy_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend()
	err := b.DeleteDeploymentStrategy("nonexistent")
	require.Error(t, err)
}

func TestBackend_GetDeploymentStrategy_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend()
	_, err := b.GetDeploymentStrategy("nonexistent")
	require.Error(t, err)
}

func TestBackend_GetDeployment_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend()
	_, err := b.GetDeployment("app-1", "env-1", 1)
	require.Error(t, err)
}

func TestBackend_StopDeployment_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend()
	err := b.StopDeployment("app-1", "env-1", 1)
	require.Error(t, err)
}

func TestBackend_HostedConfigVersion_ProfileNotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend()
	app, err := b.CreateApplication("app", "")
	require.NoError(t, err)

	_, err = b.CreateHostedConfigurationVersion(app.ID, "nonexistent-profile", "application/json", []byte("{}"))
	require.Error(t, err)
}

func TestBackend_GetHostedConfigVersion_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend()
	_, err := b.GetHostedConfigurationVersion("app-1", "prof-1", 1)
	require.Error(t, err)
}

func TestBackend_DeleteHostedConfigVersion_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend()
	err := b.DeleteHostedConfigurationVersion("app-1", "prof-1", 1)
	require.Error(t, err)
}

func TestBackend_GetEnvironment_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend()
	_, err := b.GetEnvironment("app-1", "env-1")
	require.Error(t, err)
}

func TestBackend_ListEnvironments_AppNotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend()
	_, err := b.ListEnvironments("nonexistent")
	require.Error(t, err)
}

func TestBackend_GetConfigurationProfile_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend()
	_, err := b.GetConfigurationProfile("app-1", "prof-1")
	require.Error(t, err)
}

func TestBackend_ListConfigurationProfiles_AppNotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend()
	_, err := b.ListConfigurationProfiles("nonexistent")
	require.Error(t, err)
}

func TestBackend_UpdateConfigurationProfile_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend()
	_, err := b.UpdateConfigurationProfile("app-1", "prof-1", "name", "")
	require.Error(t, err)
}

func TestBackend_DeleteConfigurationProfile_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend()
	err := b.DeleteConfigurationProfile("app-1", "prof-1")
	require.Error(t, err)
}

func TestBackend_UpdateEnvironment_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend()
	_, err := b.UpdateEnvironment("app-1", "env-1", "name", "")
	require.Error(t, err)
}

func TestBackend_DeleteEnvironment_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend()
	err := b.DeleteEnvironment("app-1", "env-1")
	require.Error(t, err)
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/applications/app-abc", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	assert.Equal(t, "app-abc", h.ExtractResource(c))
}

func TestHandler_ExtractResource_Strategy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/deploymentstrategies/strat-xyz", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	assert.Equal(t, "strat-xyz", h.ExtractResource(c))
}

func TestHandler_UpdateEnvironment_HTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create app and env.
	rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"name":"upd-env-app"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var app appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))

	rec = doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/environments", []byte(`{"name":"staging"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var env appconfig.Environment
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))

	// Update env.
	rec = doRequest(
		t,
		h,
		http.MethodPatch,
		"/applications/"+app.ID+"/environments/"+env.ID,
		[]byte(`{"name":"production","description":"updated"}`),
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	var updated appconfig.Environment
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
	assert.Equal(t, "production", updated.Name)
}

func TestHandler_ListConfigurationProfiles_HTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"name":"list-prof-app"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var app appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))

	// Create two profiles.
	for _, name := range []string{"prof-1", "prof-2"} {
		body := []byte(`{"name":"` + name + `","locationUri":"hosted","type":"AWS.Freeform"}`)
		rec = doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/configurationprofiles", body)
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	// List.
	rec = doRequest(t, h, http.MethodGet, "/applications/"+app.ID+"/configurationprofiles", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_UpdateConfigurationProfile_HTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"name":"upd-prof-app"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var app appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))

	profileBody := []byte(`{"name":"old-name","locationUri":"hosted","type":"AWS.Freeform"}`)
	rec = doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/configurationprofiles", profileBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	var profile appconfig.ConfigurationProfile
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &profile))

	// Update.
	rec = doRequest(
		t,
		h,
		http.MethodPatch,
		"/applications/"+app.ID+"/configurationprofiles/"+profile.ID,
		[]byte(`{"name":"new-name","description":"updated"}`),
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	var updated appconfig.ConfigurationProfile
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
	assert.Equal(t, "new-name", updated.Name)
}

func TestHandler_ListDeploymentStrategies_HTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := []byte(`{"name":"list-strat","deploymentDurationInMinutes":0,` +
		`"finalBakeTimeInMinutes":0,"growthFactor":100,"growthType":"LINEAR","replicateTo":"NONE"}`)
	rec := doRequest(t, h, http.MethodPost, "/deploymentstrategies", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/deploymentstrategies", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_Environment_HTTP_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		pathSuffix string
		wantStatus int
	}{
		{
			name:       "get env not found",
			method:     http.MethodGet,
			pathSuffix: "/applications/nonexistent/environments/env-1",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "list envs app not found",
			method:     http.MethodGet,
			pathSuffix: "/applications/nonexistent/environments",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete env not found",
			method:     http.MethodDelete,
			pathSuffix: "/applications/nonexistent/environments/env-1",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.method, tt.pathSuffix, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ConfigProfile_HTTP_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		pathSuffix string
		wantStatus int
	}{
		{
			name:       "get profile not found",
			method:     http.MethodGet,
			pathSuffix: "/applications/nonexistent/configurationprofiles/prof-1",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "list profiles app not found",
			method:     http.MethodGet,
			pathSuffix: "/applications/nonexistent/configurationprofiles",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete profile not found",
			method:     http.MethodDelete,
			pathSuffix: "/applications/nonexistent/configurationprofiles/prof-1",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.method, tt.pathSuffix, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_Deployment_HTTP_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		pathSuffix string
		wantStatus int
	}{
		{
			name:       "get deployment not found",
			method:     http.MethodGet,
			pathSuffix: "/applications/app-1/environments/env-1/deployments/99",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "stop deployment not found",
			method:     http.MethodDelete,
			pathSuffix: "/applications/app-1/environments/env-1/deployments/99",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.method, tt.pathSuffix, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_HostedConfigVersion_HTTP_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		pathSuffix string
		wantStatus int
	}{
		{
			name:       "get version not found",
			method:     http.MethodGet,
			pathSuffix: "/applications/app-1/configurationprofiles/prof-1/hostedconfigurationversions/99",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete version not found",
			method:     http.MethodDelete,
			pathSuffix: "/applications/app-1/configurationprofiles/prof-1/hostedconfigurationversions/99",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "list versions unknown app",
			method:     http.MethodGet,
			pathSuffix: "/applications/app-1/configurationprofiles/prof-1/hostedconfigurationversions",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.method, tt.pathSuffix, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_HostedConfigVersion_HTTP_ListEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Pre-create app and profile so listing returns an empty list (200).
	appRec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"Name":"my-app"}`))
	require.Equal(t, http.StatusCreated, appRec.Code)

	var appOut struct {
		ID string `json:"Id"`
	}

	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &appOut))

	profRec := doRequest(t, h, http.MethodPost,
		"/applications/"+appOut.ID+"/configurationprofiles",
		[]byte(`{"Name":"my-profile","LocationUri":"hosted"}`),
	)
	require.Equal(t, http.StatusCreated, profRec.Code)

	var profOut struct {
		ID string `json:"Id"`
	}

	require.NoError(t, json.Unmarshal(profRec.Body.Bytes(), &profOut))

	listRec := doRequest(t, h, http.MethodGet,
		"/applications/"+appOut.ID+"/configurationprofiles/"+profOut.ID+"/hostedconfigurationversions",
		nil,
	)
	assert.Equal(t, http.StatusOK, listRec.Code)
}

func TestHandler_DeleteApplication_NotFound_HTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodDelete, "/applications/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_StartDeployment_NotFound_HTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := []byte(`{"configurationProfileId":"prof-1","deploymentStrategyId":"strat-1","configurationVersion":"1"}`)
	rec := doRequest(t, h, http.MethodPost, "/applications/nonexistent/environments/env-1/deployments", body)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_UpdateApplication_NotFound_HTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPatch, "/applications/nonexistent", []byte(`{"name":"new"}`))
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_ParseAppConfigPath_UnknownMethod(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "unknown method on application",
			method:     http.MethodPut,
			path:       "/applications/app-1",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "unknown method on strategy",
			method:     http.MethodPut,
			path:       "/deploymentstrategies/strat-1",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "unknown method on environment",
			method:     http.MethodPut,
			path:       "/applications/app-1/environments/env-1",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "unknown method on profile",
			method:     http.MethodPut,
			path:       "/applications/app-1/configurationprofiles/prof-1",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, tt.method, tt.path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListDeployments_HTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Pre-create app and environment so listing returns 200 with empty list.
	appRec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"Name":"list-deploy-app"}`))
	require.Equal(t, http.StatusCreated, appRec.Code)

	var appOut struct {
		ID string `json:"Id"`
	}

	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &appOut))

	envRec := doRequest(t, h, http.MethodPost,
		"/applications/"+appOut.ID+"/environments",
		[]byte(`{"Name":"production"}`),
	)
	require.Equal(t, http.StatusCreated, envRec.Code)

	var envOut struct {
		ID string `json:"Id"`
	}

	require.NoError(t, json.Unmarshal(envRec.Body.Bytes(), &envOut))

	listRec := doRequest(t, h, http.MethodGet,
		"/applications/"+appOut.ID+"/environments/"+envOut.ID+"/deployments",
		nil,
	)
	assert.Equal(t, http.StatusOK, listRec.Code)
}
