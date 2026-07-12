package appconfig_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

func newTestHandler(t *testing.T) *appconfig.Handler {
	t.Helper()

	return appconfig.NewHandler(appconfig.NewInMemoryBackend("123456789012", "us-east-1"))
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
		{
			name:   "list applications",
			method: http.MethodGet,
			path:   "/applications",
			want:   "ListApplications",
		},
		{
			name:   "create application",
			method: http.MethodPost,
			path:   "/applications",
			want:   "CreateApplication",
		},
		{
			name:   "get application",
			method: http.MethodGet,
			path:   "/applications/app-1",
			want:   "GetApplication",
		},
		{
			name:   "delete application",
			method: http.MethodDelete,
			path:   "/applications/app-1",
			want:   "DeleteApplication",
		},
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

// TestHandler_UpdateApplication_OmittedDescriptionPreserved verifies that
// omitting Description from an UpdateApplication request leaves the
// existing description untouched, matching real AWS AppConfig's
// UpdateApplicationInput.Description (an optional *string member: absent
// means unchanged, only a present value -- including "" -- overwrites).
func TestHandler_UpdateApplication_OmittedDescriptionPreserved(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/applications",
		[]byte(`{"Name":"orig","Description":"keep-me"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var app appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))

	// Update only Name; Description is omitted from the request body.
	rec = doRequest(t, h, http.MethodPatch, "/applications/"+app.ID,
		[]byte(`{"Name":"renamed"}`))
	assert.Equal(t, http.StatusOK, rec.Code)

	var updated appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
	assert.Equal(t, "renamed", updated.Name)
	assert.Equal(t, "keep-me", updated.Description,
		"omitted Description must not clobber the existing value")
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
	rec = doRequest(
		t,
		h,
		http.MethodPost,
		"/applications/"+app.ID+"/environments",
		[]byte(`{"name":"production"}`),
	)
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
	rec = doRequest(
		t,
		h,
		http.MethodPost,
		"/applications/"+app.ID+"/configurationprofiles",
		profileBody,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var profile appconfig.ConfigurationProfile
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &profile))
	assert.Equal(t, "my-config", profile.Name)

	// Get profile.
	rec = doRequest(
		t,
		h,
		http.MethodGet,
		"/applications/"+app.ID+"/configurationprofiles/"+profile.ID,
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete profile.
	rec = doRequest(
		t,
		h,
		http.MethodDelete,
		"/applications/"+app.ID+"/configurationprofiles/"+profile.ID,
		nil,
	)
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
	rec = doRequest(
		t,
		h,
		http.MethodPatch,
		"/deploymentstrategies/"+strategy.ID,
		[]byte(`{"name":"updated-strategy"}`),
	)
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
	rec = doRequest(
		t,
		h,
		http.MethodPost,
		"/applications/"+app.ID+"/configurationprofiles",
		profileBody,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var profile appconfig.ConfigurationProfile
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &profile))

	// Create hosted configuration version with raw body.
	e := echo.New()
	content := []byte(`{"feature":"enabled"}`)
	req := httptest.NewRequest(
		http.MethodPost,
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
	rec = doRequest(
		t,
		h,
		http.MethodGet,
		"/applications/"+app.ID+"/configurationprofiles/"+profile.ID+"/hostedconfigurationversions",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Get version 1.
	req2 := httptest.NewRequest(
		http.MethodGet,
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
	rec = doRequest(
		t,
		h,
		http.MethodDelete,
		"/applications/"+app.ID+"/configurationprofiles/"+profile.ID+"/hostedconfigurationversions/1",
		nil,
	)
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
	rec = doRequest(
		t,
		h,
		http.MethodPost,
		"/applications/"+app.ID+"/environments",
		[]byte(`{"name":"staging"}`),
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var env appconfig.Environment
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))

	// Create configuration profile (required by StartDeployment validation).
	profBody := []byte(`{"name":"my-profile","locationUri":"hosted"}`)
	rec = doRequest(
		t,
		h,
		http.MethodPost,
		"/applications/"+app.ID+"/configurationprofiles",
		profBody,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var prof appconfig.ConfigurationProfile
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &prof))

	// Create deployment strategy (required by StartDeployment validation).
	stratBody := []byte(`{"name":"my-strategy","deploymentDurationInMinutes":10,"growthFactor":20}`)
	rec = doRequest(t, h, http.MethodPost, "/deploymentstrategies", stratBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	var strat appconfig.DeploymentStrategy
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &strat))

	// Start deployment.
	depBodyStr := `{"configurationProfileId":"` + prof.ID +
		`","deploymentStrategyId":"` + strat.ID + `","configurationVersion":"1"}`
	rec = doRequest(
		t,
		h,
		http.MethodPost,
		"/applications/"+app.ID+"/environments/"+env.ID+"/deployments",
		[]byte(depBodyStr),
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var dep appconfig.Deployment
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &dep))
	assert.Equal(t, int32(1), dep.DeploymentNumber)
	assert.Equal(t, "COMPLETE", dep.State)

	// Get deployment.
	rec = doRequest(
		t,
		h,
		http.MethodGet,
		"/applications/"+app.ID+"/environments/"+env.ID+"/deployments/1",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List deployments.
	rec = doRequest(
		t,
		h,
		http.MethodGet,
		"/applications/"+app.ID+"/environments/"+env.ID+"/deployments",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Stop deployment (COMPLETE deployments can be rolled back in stub).
	rec = doRequest(
		t,
		h,
		http.MethodDelete,
		"/applications/"+app.ID+"/environments/"+env.ID+"/deployments/1",
		nil,
	)
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

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.GetApplication("nonexistent")
	require.Error(t, err)
}

func TestBackend_ListApplications_Empty(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	apps, _ := b.ListApplications("", 0)
	assert.Empty(t, apps)
}

func TestBackend_DeleteApplication_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	err := b.DeleteApplication("nonexistent")
	require.Error(t, err)
}

func TestBackend_CreateEnvironment_AppNotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.CreateEnvironment("nonexistent", "env", "", nil)
	require.Error(t, err)
}

func TestBackend_CreateDeploymentStrategy(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	s, err := b.CreateDeploymentStrategy("strat", "desc", 0, 0, 100, "LINEAR", "NONE")
	require.NoError(t, err)
	assert.Equal(t, "strat", s.Name)
	assert.NotEmpty(t, s.ID)

	strategies, _ := b.ListDeploymentStrategies("", 0)
	assert.Len(t, strategies, 1)
}

func TestBackend_UpdateDeploymentStrategy_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.UpdateDeploymentStrategy("nonexistent", "name", new(""), 0, 0, 0)
	require.Error(t, err)
}

func TestBackend_DeleteDeploymentStrategy_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	err := b.DeleteDeploymentStrategy("nonexistent")
	require.Error(t, err)
}

func TestBackend_GetDeploymentStrategy_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.GetDeploymentStrategy("nonexistent")
	require.Error(t, err)
}

func TestBackend_GetDeployment_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.GetDeployment("app-1", "env-1", 1)
	require.Error(t, err)
}

func TestBackend_StopDeployment_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	err := b.StopDeployment("app-1", "env-1", 1)
	require.Error(t, err)
}

func TestBackend_HostedConfigVersion_ProfileNotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	app, err := b.CreateApplication("app", "")
	require.NoError(t, err)

	_, err = b.CreateHostedConfigurationVersion(
		app.ID,
		"nonexistent-profile",
		"application/json",
		"",
		"",
		[]byte("{}"),
	)
	require.Error(t, err)
}

func TestBackend_GetHostedConfigVersion_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.GetHostedConfigurationVersion("app-1", "prof-1", 1)
	require.Error(t, err)
}

func TestBackend_DeleteHostedConfigVersion_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	err := b.DeleteHostedConfigurationVersion("app-1", "prof-1", 1)
	require.Error(t, err)
}

func TestBackend_GetEnvironment_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.GetEnvironment("app-1", "env-1")
	require.Error(t, err)
}

func TestBackend_ListEnvironments_AppNotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	_, _, err := b.ListEnvironments("nonexistent", "", 0)
	require.Error(t, err)
}

func TestBackend_GetConfigurationProfile_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.GetConfigurationProfile("app-1", "prof-1")
	require.Error(t, err)
}

func TestBackend_ListConfigurationProfiles_AppNotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	_, _, err := b.ListConfigurationProfiles("nonexistent", "", 0)
	require.Error(t, err)
}

func TestBackend_UpdateConfigurationProfile_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.UpdateConfigurationProfile("app-1", "prof-1", new("name"), new(""), nil, nil)
	require.Error(t, err)
}

func TestBackend_DeleteConfigurationProfile_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	err := b.DeleteConfigurationProfile("app-1", "prof-1")
	require.Error(t, err)
}

func TestBackend_UpdateEnvironment_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.UpdateEnvironment("app-1", "env-1", new("name"), new(""), nil)
	require.Error(t, err)
}

func TestBackend_DeleteEnvironment_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
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

	rec = doRequest(
		t,
		h,
		http.MethodPost,
		"/applications/"+app.ID+"/environments",
		[]byte(`{"name":"staging"}`),
	)
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

// TestHandler_UpdateEnvironment_OmittedFieldsPreserved verifies that
// UpdateEnvironment leaves Description and Monitors unchanged when they are
// omitted from the request, and that a present Monitors list does replace
// the existing one -- matching UpdateEnvironmentInput's optional members.
func TestHandler_UpdateEnvironment_OmittedFieldsPreserved(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"Name":"upd-env-app2"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var app appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))

	rec = doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/environments",
		[]byte(`{"Name":"staging","Description":"keep-me","Monitors":[{"AlarmArn":"arn:aws:cloudwatch:alarm1"}]}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var env appconfig.Environment
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))
	require.Len(t, env.Monitors, 1)

	// Update only Name; Description and Monitors are omitted.
	rec = doRequest(t, h, http.MethodPatch, "/applications/"+app.ID+"/environments/"+env.ID,
		[]byte(`{"Name":"production"}`))
	assert.Equal(t, http.StatusOK, rec.Code)

	var updated appconfig.Environment
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
	assert.Equal(t, "production", updated.Name)
	assert.Equal(t, "keep-me", updated.Description, "omitted Description must be preserved")
	require.Len(t, updated.Monitors, 1, "omitted Monitors must be preserved")
	assert.Equal(t, "arn:aws:cloudwatch:alarm1", updated.Monitors[0].AlarmArn)

	// A present (even empty) Monitors list replaces the existing one.
	rec = doRequest(t, h, http.MethodPatch, "/applications/"+app.ID+"/environments/"+env.ID,
		[]byte(`{"Monitors":[]}`))
	assert.Equal(t, http.StatusOK, rec.Code)

	var cleared appconfig.Environment
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cleared))
	assert.Empty(t, cleared.Monitors, "an explicit empty Monitors list must replace the existing one")
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
		rec = doRequest(
			t,
			h,
			http.MethodPost,
			"/applications/"+app.ID+"/configurationprofiles",
			body,
		)
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
	rec = doRequest(
		t,
		h,
		http.MethodPost,
		"/applications/"+app.ID+"/configurationprofiles",
		profileBody,
	)
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

// TestHandler_UpdateConfigurationProfile_RetrievalRoleArnAndValidators verifies
// that UpdateConfigurationProfile applies RetrievalRoleArn and Validators
// when present (previously silently dropped -- the backend only accepted
// Name/Description) and preserves Description when it is omitted, matching
// real UpdateConfigurationProfileInput's optional members.
func TestHandler_UpdateConfigurationProfile_RetrievalRoleArnAndValidators(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"Name":"upd-prof-app2"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var app appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))

	profileBody := []byte(
		`{"Name":"old-name","Description":"keep-me","LocationUri":"hosted","Type":"AWS.Freeform"}`,
	)
	rec = doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/configurationprofiles", profileBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	var profile appconfig.ConfigurationProfile
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &profile))

	rec = doRequest(t, h, http.MethodPatch,
		"/applications/"+app.ID+"/configurationprofiles/"+profile.ID,
		[]byte(`{"RetrievalRoleArn":"arn:aws:iam::123456789012:role/retrieval",`+
			`"Validators":[{"Type":"JSON_SCHEMA","Content":"{}"}]}`))
	assert.Equal(t, http.StatusOK, rec.Code)

	var updated appconfig.ConfigurationProfile
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
	assert.Equal(t, "arn:aws:iam::123456789012:role/retrieval", updated.RetrievalRoleArn,
		"RetrievalRoleArn must be applied, not silently dropped")
	require.Len(t, updated.Validators, 1, "Validators must be applied, not silently dropped")
	assert.Equal(t, "keep-me", updated.Description, "omitted Description must be preserved")
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

// TestHandler_UpdateDeploymentStrategy_OmittedDescriptionPreserved verifies
// that updating only GrowthFactor leaves Description unchanged when it is
// omitted, matching real UpdateDeploymentStrategyInput's optional *string
// Description member.
func TestHandler_UpdateDeploymentStrategy_OmittedDescriptionPreserved(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := []byte(`{"Name":"upd-strat","Description":"keep-me",` +
		`"DeploymentDurationInMinutes":0,"FinalBakeTimeInMinutes":0,` +
		`"GrowthFactor":10,"GrowthType":"LINEAR","ReplicateTo":"NONE"}`)
	rec := doRequest(t, h, http.MethodPost, "/deploymentstrategies", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var strat appconfig.DeploymentStrategy
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &strat))

	rec = doRequest(t, h, http.MethodPatch, "/deploymentstrategies/"+strat.ID,
		[]byte(`{"GrowthFactor":50}`))
	assert.Equal(t, http.StatusOK, rec.Code)

	var updated appconfig.DeploymentStrategy
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
	assert.InDelta(t, float32(50), updated.GrowthFactor, 0.001)
	assert.Equal(t, "keep-me", updated.Description, "omitted Description must be preserved")
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

	profRec := doRequest(
		t, h, http.MethodPost,
		"/applications/"+appOut.ID+"/configurationprofiles",
		[]byte(`{"Name":"my-profile","LocationUri":"hosted"}`),
	)
	require.Equal(t, http.StatusCreated, profRec.Code)

	var profOut struct {
		ID string `json:"Id"`
	}

	require.NoError(t, json.Unmarshal(profRec.Body.Bytes(), &profOut))

	listRec := doRequest(
		t,
		h,
		http.MethodGet,
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
	body := []byte(
		`{"configurationProfileId":"prof-1","deploymentStrategyId":"strat-1","configurationVersion":"1"}`,
	)
	rec := doRequest(
		t,
		h,
		http.MethodPost,
		"/applications/nonexistent/environments/env-1/deployments",
		body,
	)
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
	appRec := doRequest(
		t,
		h,
		http.MethodPost,
		"/applications",
		[]byte(`{"Name":"list-deploy-app"}`),
	)
	require.Equal(t, http.StatusCreated, appRec.Code)

	var appOut struct {
		ID string `json:"Id"`
	}

	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &appOut))

	envRec := doRequest(
		t, h, http.MethodPost,
		"/applications/"+appOut.ID+"/environments",
		[]byte(`{"Name":"production"}`),
	)
	require.Equal(t, http.StatusCreated, envRec.Code)

	var envOut struct {
		ID string `json:"Id"`
	}

	require.NoError(t, json.Unmarshal(envRec.Body.Bytes(), &envOut))

	listRec := doRequest(
		t, h, http.MethodGet,
		"/applications/"+appOut.ID+"/environments/"+envOut.ID+"/deployments",
		nil,
	)
	assert.Equal(t, http.StatusOK, listRec.Code)
}

func TestHandler_ListApplicationsPagination(t *testing.T) {
	t.Parallel()

	t.Run("no_pagination_returns_all", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		for _, name := range []string{"app1", "app2", "app3", "app4"} {
			rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"name":"`+name+`"}`))
			require.Equal(t, http.StatusCreated, rec.Code)
		}
		rec := doRequest(t, h, http.MethodGet, "/applications", nil)
		require.Equal(t, http.StatusOK, rec.Code)
		var resp struct {
			NextToken string `json:"NextToken"`
			Items     []any  `json:"Items"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Len(t, resp.Items, 4)
		assert.Empty(t, resp.NextToken)
	})

	t.Run("first_page", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		for _, name := range []string{"app1", "app2", "app3", "app4"} {
			rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"name":"`+name+`"}`))
			require.Equal(t, http.StatusCreated, rec.Code)
		}
		rec := doRequest(t, h, http.MethodGet, "/applications?max_results=2", nil)
		require.Equal(t, http.StatusOK, rec.Code)
		var resp struct {
			NextToken string `json:"NextToken"`
			Items     []any  `json:"Items"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Len(t, resp.Items, 2)
		assert.NotEmpty(t, resp.NextToken)
	})

	t.Run("second_page", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		for _, name := range []string{"app1", "app2", "app3", "app4"} {
			rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"name":"`+name+`"}`))
			require.Equal(t, http.StatusCreated, rec.Code)
		}
		// Get the first page to obtain a valid HMAC token.
		rec := doRequest(t, h, http.MethodGet, "/applications?max_results=2", nil)
		require.Equal(t, http.StatusOK, rec.Code)
		var first struct {
			NextToken string `json:"NextToken"`
			Items     []any  `json:"Items"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &first))
		require.NotEmpty(t, first.NextToken)

		rec2 := doRequest(
			t,
			h,
			http.MethodGet,
			"/applications?max_results=2&next_token="+first.NextToken,
			nil,
		)
		require.Equal(t, http.StatusOK, rec2.Code)
		var resp struct {
			NextToken string `json:"NextToken"`
			Items     []any  `json:"Items"`
		}
		require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
		assert.Len(t, resp.Items, 2)
		assert.Empty(t, resp.NextToken)
	})

	t.Run("token_beyond_end", func(t *testing.T) {
		t.Parallel()
		backend := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
		h2 := appconfig.NewHandler(backend)
		for _, name := range []string{"app1", "app2", "app3", "app4"} {
			rec := doRequest(t, h2, http.MethodPost, "/applications", []byte(`{"name":"`+name+`"}`))
			require.Equal(t, http.StatusCreated, rec.Code)
		}
		beyondToken := page.EncodeHMACToken(100, backend.PaginationSecret())
		rec := doRequest(
			t,
			h2,
			http.MethodGet,
			"/applications?max_results=2&next_token="+beyondToken,
			nil,
		)
		require.Equal(t, http.StatusOK, rec.Code)
		var resp struct {
			NextToken string `json:"NextToken"`
			Items     []any  `json:"Items"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Empty(t, resp.Items)
		assert.Empty(t, resp.NextToken)
	})
}

func TestBackend_appConfigPaginate_EdgeCases(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	secret := b.PaginationSecret()

	// Create 4 apps.
	for _, name := range []string{"a", "b", "c", "d"} {
		_, err := b.CreateApplication(name, "")
		require.NoError(t, err)
	}

	tests := []struct {
		name          string
		nextToken     string
		maxResults    int
		wantCount     int
		wantNextToken bool
	}{
		{
			name:       "zero_max_returns_all",
			maxResults: 0,
			wantCount:  4,
		},
		{
			name:          "first_page",
			maxResults:    2,
			wantCount:     2,
			wantNextToken: true,
		},
		{
			name:       "second_page",
			maxResults: 2,
			nextToken:  page.EncodeHMACToken(2, secret),
			wantCount:  2,
		},
		{
			name:       "token_beyond_end",
			maxResults: 2,
			nextToken:  page.EncodeHMACToken(50, secret),
			wantCount:  0,
		},
		{
			name:          "invalid_token_treated_as_start",
			maxResults:    2,
			nextToken:     "bogus",
			wantCount:     2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			apps, outToken := b.ListApplications(tt.nextToken, tt.maxResults)
			assert.Len(t, apps, tt.wantCount)

			if tt.wantNextToken {
				assert.NotEmpty(t, outToken)
			} else {
				assert.Empty(t, outToken)
			}
		})
	}
}

func TestHandler_Extension_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name       string
		method     string
		path       string
		wantField  string
		wantValue  string
		body       []byte
		wantStatus int
	}{
		{
			name:       "list extensions empty",
			method:     http.MethodGet,
			path:       "/extensions",
			wantStatus: http.StatusOK,
		},
		{
			name:       "create extension",
			method:     http.MethodPost,
			path:       "/extensions",
			body:       []byte(`{"Name":"my-ext","Description":"test extension"}`),
			wantStatus: http.StatusCreated,
			wantField:  "Name",
			wantValue:  "my-ext",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantField != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantValue, resp[tt.wantField])
			}
		})
	}
}

func TestHandler_Extension_GetDeleteByID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create extension.
	rec := doRequest(t, h, http.MethodPost, "/extensions", []byte(`{"Name":"ext-to-delete"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var ext appconfig.Extension
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ext))
	assert.NotEmpty(t, ext.ID)
	assert.Equal(t, "ext-to-delete", ext.Name)

	// Get by ID.
	rec = doRequest(t, h, http.MethodGet, "/extensions/"+ext.ID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Get by name.
	rec = doRequest(t, h, http.MethodGet, "/extensions/ext-to-delete", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete by ID.
	rec = doRequest(t, h, http.MethodDelete, "/extensions/"+ext.ID, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Get should now return 404.
	rec = doRequest(t, h, http.MethodGet, "/extensions/"+ext.ID, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_Extension_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{name: "get not found", method: http.MethodGet, path: "/extensions/nonexistent"},
		{name: "delete not found", method: http.MethodDelete, path: "/extensions/nonexistent"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, tt.method, tt.path, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_Extension_ListPaginated(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 3 {
		body := []byte(`{"Name":"ext-` + strconv.Itoa(i) + `"}`)
		rec := doRequest(t, h, http.MethodPost, "/extensions", body)
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	rec := doRequest(t, h, http.MethodGet, "/extensions?max_results=2", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items, ok := resp["Items"].([]any)
	require.True(t, ok)
	assert.Len(t, items, 2)
	assert.NotEmpty(t, resp["NextToken"])
}

func TestHandler_ExtensionAssociation_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create an extension first.
	rec := doRequest(t, h, http.MethodPost, "/extensions", []byte(`{"Name":"assoc-ext"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var ext appconfig.Extension
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ext))

	// Create an application to use as resource.
	rec = doRequest(t, h, http.MethodPost, "/applications", []byte(`{"name":"assoc-app"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var app appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))

	// List associations empty.
	rec = doRequest(t, h, http.MethodGet, "/extensionassociations", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Create association.
	resourceID := "arn:aws:appconfig:us-east-1:123456789012:application/" + app.ID
	body := []byte(
		`{"ExtensionIdentifier":"` + ext.ID + `","ResourceIdentifier":"` + resourceID + `"}`,
	)
	rec = doRequest(t, h, http.MethodPost, "/extensionassociations", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var assoc appconfig.ExtensionAssociation
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &assoc))
	assert.NotEmpty(t, assoc.ID)
	assert.Equal(t, ext.Arn, assoc.ExtensionArn)

	// Get association.
	rec = doRequest(t, h, http.MethodGet, "/extensionassociations/"+assoc.ID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List associations shows one.
	rec = doRequest(t, h, http.MethodGet, "/extensionassociations", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	items, ok := listResp["Items"].([]any)
	require.True(t, ok)
	assert.Len(t, items, 1)

	// Delete association.
	rec = doRequest(t, h, http.MethodDelete, "/extensionassociations/"+assoc.ID, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Get should now return 404.
	rec = doRequest(t, h, http.MethodGet, "/extensionassociations/"+assoc.ID, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_ExtensionAssociation_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name   string
		method string
		path   string
		body   []byte
	}{
		{
			name:   "get not found",
			method: http.MethodGet,
			path:   "/extensionassociations/nonexistent",
		},
		{
			name:   "delete not found",
			method: http.MethodDelete,
			path:   "/extensionassociations/nonexistent",
		},
		{
			name:   "create with missing extension",
			method: http.MethodPost,
			path:   "/extensionassociations",
			body: []byte(`{"ExtensionIdentifier":"missing-ext",` +
				`"ResourceIdentifier":"arn:aws:appconfig:us-east-1:123456789012:application/abc"}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_GetAccountSettings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/settings", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var settings appconfig.AccountSettings
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &settings))
}

func TestHandler_GetConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Setup: create application, environment, config profile, and hosted config version.
	rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"name":"conf-app"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var app appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))

	rec = doRequest(
		t,
		h,
		http.MethodPost,
		"/applications/"+app.ID+"/environments",
		[]byte(`{"name":"conf-env"}`),
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var env appconfig.Environment
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))

	rec = doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/configurationprofiles",
		[]byte(`{"Name":"conf-profile","LocationUri":"hosted","Type":"AWS.Freeform"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var profile appconfig.ConfigurationProfile
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &profile))

	tests := []struct {
		name       string
		path       string
		wantStatus int
	}{
		{
			name:       "no version returns 204",
			path:       "/applications/" + app.ID + "/environments/" + env.ID + "/configurations/" + profile.ID,
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "app not found",
			path:       "/applications/nonexistent/environments/" + env.ID + "/configurations/" + profile.ID,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "env not found",
			path:       "/applications/" + app.ID + "/environments/nonexistent/configurations/" + profile.ID,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "profile not found",
			path:       "/applications/" + app.ID + "/environments/" + env.ID + "/configurations/nonexistent",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := doRequest(t, h, http.MethodGet, tt.path, nil)
			assert.Equal(t, tt.wantStatus, result.Code)
		})
	}
}

func TestHandler_GetConfiguration_WithContent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Setup.
	rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"name":"content-app"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var app appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))

	rec = doRequest(
		t,
		h,
		http.MethodPost,
		"/applications/"+app.ID+"/environments",
		[]byte(`{"name":"content-env"}`),
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var env appconfig.Environment
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))

	rec = doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/configurationprofiles",
		[]byte(`{"Name":"content-profile","LocationUri":"hosted","Type":"AWS.Freeform"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var profile appconfig.ConfigurationProfile
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &profile))

	// Create a hosted configuration version.
	e := echo.New()
	req := httptest.NewRequest(
		http.MethodPost,
		"/applications/"+app.ID+"/configurationprofiles/"+profile.ID+"/hostedconfigurationversions",
		bytes.NewReader([]byte(`{"feature":"enabled"}`)),
	)
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(logger.Save(t.Context(), slog.Default()))
	recVer := httptest.NewRecorder()
	c := e.NewContext(req, recVer)
	err := h.Handler()(c)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, recVer.Code)

	// GetConfiguration should now return 200 with content.
	rec = doRequest(t, h, http.MethodGet,
		"/applications/"+app.ID+"/environments/"+env.ID+"/configurations/"+profile.ID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.NotEmpty(t, rec.Body.Bytes())
	assert.Equal(t, "1", rec.Header().Get("Configuration-Version"))
}

func TestHandler_RouteMatcher_NewPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "extensions prefix", path: "/extensions", want: true},
		{name: "extensions with id", path: "/extensions/ext-1", want: true},
		{name: "extensionassociations prefix", path: "/extensionassociations", want: true},
		{name: "extensionassociations with id", path: "/extensionassociations/assoc-1", want: true},
		{name: "settings", path: "/settings", want: true},
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

func TestHandler_TagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create an application to have a tagged resource ARN.
	rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"name":"tag-app"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var app appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))

	resourceArn := "arn:aws:appconfig:us-east-1:123456789012:application/" + app.ID

	// List tags — initially empty.
	result := doRequest(t, h, http.MethodGet, "/tags/"+resourceArn, nil)
	assert.Equal(t, http.StatusOK, result.Code)

	// Tag the resource.
	result = doRequest(t, h, http.MethodPost, "/tags/"+resourceArn,
		[]byte(`{"Tags":{"env":"prod","owner":"team"}}`))
	assert.Equal(t, http.StatusNoContent, result.Code)

	// List tags — should be present.
	result = doRequest(t, h, http.MethodGet, "/tags/"+resourceArn, nil)
	assert.Equal(t, http.StatusOK, result.Code)

	// Untag.
	result = doRequest(t, h, http.MethodDelete, "/tags/"+resourceArn+"?tagKeys=env", nil)
	assert.Equal(t, http.StatusNoContent, result.Code)
}

func TestHandler_TagResource_VerifyTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	resourceArn := "arn:aws:appconfig:us-east-1:123456789012:application/app-123"

	// Tag resource.
	rec := doRequest(t, h, http.MethodPost, "/tags/"+resourceArn,
		[]byte(`{"Tags":{"env":"prod","version":"1.0"}}`))
	require.Equal(t, http.StatusNoContent, rec.Code)

	// List tags - should have 2.
	rec = doRequest(t, h, http.MethodGet, "/tags/"+resourceArn, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "prod", resp["Tags"]["env"])
	assert.Equal(t, "1.0", resp["Tags"]["version"])

	// Remove one tag.
	rec = doRequest(t, h, http.MethodDelete, "/tags/"+resourceArn+"?tagKeys=env", nil)
	require.Equal(t, http.StatusNoContent, rec.Code)

	// List tags - should have 1.
	rec = doRequest(t, h, http.MethodGet, "/tags/"+resourceArn, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp["Tags"], 1)
	assert.Equal(t, "1.0", resp["Tags"]["version"])
}

func TestHandler_UpdateExtension(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create extension.
	rec := doRequest(
		t,
		h,
		http.MethodPost,
		"/extensions",
		[]byte(`{"Name":"update-ext","Description":"original"}`),
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var ext appconfig.Extension
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ext))
	assert.Equal(t, int32(1), ext.VersionNumber)

	// Update extension.
	rec = doRequest(t, h, http.MethodPatch, "/extensions/"+ext.ID,
		[]byte(`{"Description":"updated"}`))
	require.Equal(t, http.StatusOK, rec.Code)

	var updated appconfig.Extension
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
	assert.Equal(t, "updated", updated.Description)
	assert.Equal(t, int32(2), updated.VersionNumber)
}

// TestHandler_UpdateExtension_OmittedDescriptionPreserved verifies that
// updating an extension's Actions without including Description leaves the
// existing description unchanged, matching real UpdateExtensionInput's
// optional *string Description member.
func TestHandler_UpdateExtension_OmittedDescriptionPreserved(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/extensions",
		[]byte(`{"Name":"update-ext2","Description":"keep-me"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var ext appconfig.Extension
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ext))

	// Update Actions only; Description is omitted.
	rec = doRequest(t, h, http.MethodPatch, "/extensions/"+ext.ID,
		[]byte(`{"Actions":{"ON_DEPLOYMENT_START":[{"Name":"a","Uri":"lambda:1"}]}}`))
	require.Equal(t, http.StatusOK, rec.Code)

	var updated appconfig.Extension
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
	assert.Equal(t, "keep-me", updated.Description, "omitted Description must not be cleared")
}

func TestHandler_UpdateExtension_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPatch, "/extensions/nonexistent",
		[]byte(`{"Description":"updated"}`))
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_UpdateExtensionAssociation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create extension and association.
	rec := doRequest(t, h, http.MethodPost, "/extensions", []byte(`{"Name":"update-assoc-ext"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var ext appconfig.Extension
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ext))

	resourceID := "arn:aws:appconfig:us-east-1:123456789012:application/abc"
	assocBody := []byte(
		`{"ExtensionIdentifier":"` + ext.ID + `","ResourceIdentifier":"` + resourceID + `"}`,
	)
	rec = doRequest(t, h, http.MethodPost, "/extensionassociations", assocBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	var assoc appconfig.ExtensionAssociation
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &assoc))

	// Update the association parameters.
	rec = doRequest(t, h, http.MethodPatch, "/extensionassociations/"+assoc.ID,
		[]byte(`{"Parameters":{"key":"value"}}`))
	require.Equal(t, http.StatusOK, rec.Code)

	var updated appconfig.ExtensionAssociation
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
	assert.Equal(t, "value", updated.Parameters["key"])
}

func TestHandler_UpdateExtensionAssociation_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPatch, "/extensionassociations/nonexistent",
		[]byte(`{"Parameters":{}}`))
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_UpdateAccountSettings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Update deletion protection.
	body := []byte(`{"DeletionProtection":{"Enabled":true,"ProtectionPeriodInMinutes":30}}`)
	rec := doRequest(t, h, http.MethodPatch, "/settings", body)
	require.Equal(t, http.StatusOK, rec.Code)

	var settings appconfig.AccountSettings
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &settings))
	require.NotNil(t, settings.DeletionProtection)
	require.NotNil(t, settings.DeletionProtection.Enabled)
	assert.True(t, *settings.DeletionProtection.Enabled)

	// GetAccountSettings should reflect the update.
	rec = doRequest(t, h, http.MethodGet, "/settings", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &settings))
	require.NotNil(t, settings.DeletionProtection)
	assert.True(t, *settings.DeletionProtection.Enabled)
}

func TestHandler_ValidateConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create application and profile.
	rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"name":"validate-app"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var app appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))

	rec = doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/configurationprofiles",
		[]byte(`{"Name":"validate-profile","LocationUri":"hosted","Type":"AWS.Freeform"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var profile appconfig.ConfigurationProfile
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &profile))

	validatorBase := "/applications/" + app.ID + "/configurationprofiles/" + profile.ID + "/validators"

	tests := []struct {
		name       string
		path       string
		wantStatus int
	}{
		{
			name:       "valid configuration",
			path:       validatorBase + "?configuration_version=1",
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "app not found",
			path:       "/applications/nonexistent/configurationprofiles/" + profile.ID + "/validators",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "profile not found",
			path:       "/applications/" + app.ID + "/configurationprofiles/nonexistent/validators",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := doRequest(t, h, http.MethodPost, tt.path, nil)
			assert.Equal(t, tt.wantStatus, result.Code)
		})
	}
}

func TestHandler_CreateExtension_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       []byte
		wantStatus int
	}{
		{
			name:       "missing name returns 400",
			body:       []byte(`{"Description":"no name"}`),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty name returns 400",
			body:       []byte(`{"Name":""}`),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "duplicate name returns 409",
			body:       []byte(`{"Name":"duplicate-ext"}`),
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.wantStatus == http.StatusConflict {
				// Pre-create extension to force duplicate.
				rec := doRequest(
					t,
					h,
					http.MethodPost,
					"/extensions",
					[]byte(`{"Name":"duplicate-ext"}`),
				)
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			rec := doRequest(t, h, http.MethodPost, "/extensions", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateExtensionAssociation_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       []byte
		wantStatus int
	}{
		{
			name: "missing extension identifier returns 400",
			body: []byte(
				`{"ResourceIdentifier":"arn:aws:appconfig:us-east-1:123456789012:application/abc"}`,
			),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing resource identifier returns 400",
			body:       []byte(`{"ExtensionIdentifier":"my-ext"}`),
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/extensionassociations", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateApplication_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"description":"no name"}`))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListExtensions_NameFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create two extensions.
	for _, name := range []string{"alpha-ext", "beta-ext"} {
		body := []byte(`{"Name":"` + name + `"}`)
		rec := doRequest(t, h, http.MethodPost, "/extensions", body)
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	// Filter by name.
	rec := doRequest(t, h, http.MethodGet, "/extensions?name=alpha-ext", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items, ok := resp["Items"].([]any)
	require.True(t, ok)
	assert.Len(t, items, 1)

	// Filter by non-matching name.
	rec = doRequest(t, h, http.MethodGet, "/extensions?name=nonexistent", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items, ok = resp["Items"].([]any)
	require.True(t, ok)
	assert.Empty(t, items)
}

func TestHandler_GetSupportedOperations_NewOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	newOps := []string{
		"UpdateExtension",
		"UpdateExtensionAssociation",
		"UpdateAccountSettings",
		"ValidateConfiguration",
	}

	for _, op := range newOps {
		assert.Contains(t, ops, op, "expected %s in supported operations", op)
	}
}

func TestHandler_ExtractOperation_NewPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		want   string
	}{
		{
			name:   "update extension",
			method: http.MethodPatch,
			path:   "/extensions/ext-1",
			want:   "UpdateExtension",
		},
		{
			name:   "update extension association",
			method: http.MethodPatch,
			path:   "/extensionassociations/assoc-1",
			want:   "UpdateExtensionAssociation",
		},
		{
			name:   "get account settings",
			method: http.MethodGet,
			path:   "/settings",
			want:   "GetAccountSettings",
		},
		{
			name:   "update account settings",
			method: http.MethodPatch,
			path:   "/settings",
			want:   "UpdateAccountSettings",
		},
		{
			name:   "validate configuration",
			method: http.MethodPost,
			path:   "/applications/app-1/configurationprofiles/profile-1/validators",
			want:   "ValidateConfiguration",
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
