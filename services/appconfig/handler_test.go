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

// newTestHandler creates a Handler backed by a fresh InMemoryBackend. Shared
// by every _test.go file in this package.
func newTestHandler(t *testing.T) *appconfig.Handler {
	t.Helper()

	return appconfig.NewHandler(appconfig.NewInMemoryBackend("123456789012", "us-east-1"))
}

// doRequest drives a request through the Handler's echo.HandlerFunc and
// returns the recorded response. Shared by every _test.go file in this
// package.
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

// TestHandler_GetSupportedOperations verifies both the original operation
// set and the extension/account-settings/validate-configuration operations
// added later, matching the union of GetSupportedOperations()'s contract.
func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	want := []string{
		"CreateApplication",
		"GetApplication",
		"ListApplications",
		"DeleteApplication",
		"CreateDeploymentStrategy",
		"StartDeployment",
		"UpdateExtension",
		"UpdateExtensionAssociation",
		"UpdateAccountSettings",
		"ValidateConfiguration",
		"CreateExperimentDefinition",
		"GetExperimentDefinition",
		"ListExperimentDefinitions",
		"UpdateExperimentDefinition",
		"DeleteExperimentDefinition",
		"StartExperimentRun",
		"GetExperimentRun",
		"ListExperimentRuns",
		"UpdateExperimentRun",
		"StopExperimentRun",
		"ListExperimentRunEvents",
	}

	for _, op := range want {
		assert.Contains(t, ops, op, "expected %s in supported operations", op)
	}
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

// TestHandler_RouteMatcher covers every path prefix RouteMatcher recognizes,
// including the extension/extensionassociation/settings paths added later.
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
		{name: "extensions prefix", path: "/extensions", want: true},
		{name: "extensions with id", path: "/extensions/ext-1", want: true},
		{name: "extensionassociations prefix", path: "/extensionassociations", want: true},
		{name: "extensionassociations with id", path: "/extensionassociations/assoc-1", want: true},
		{name: "settings", path: "/settings", want: true},
		{name: "experimentdefinitions prefix (account-wide list)", path: "/experimentdefinitions", want: true},
		{
			name: "application-scoped experimentdefinitions",
			path: "/applications/app-1/experimentdefinitions/def-1/experimentruns/1/stop",
			want: true,
		},
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

// TestHandler_ExtractOperation covers path/method combinations across every
// resource family, including the extension/settings/validator paths added
// later.
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
		{
			name:   "create experiment definition",
			method: http.MethodPost,
			path:   "/applications/app-1/experimentdefinitions",
			want:   "CreateExperimentDefinition",
		},
		{
			name:   "get experiment definition",
			method: http.MethodGet,
			path:   "/applications/app-1/experimentdefinitions/def-1",
			want:   "GetExperimentDefinition",
		},
		{
			name:   "update experiment definition",
			method: http.MethodPatch,
			path:   "/applications/app-1/experimentdefinitions/def-1",
			want:   "UpdateExperimentDefinition",
		},
		{
			name:   "delete experiment definition",
			method: http.MethodDelete,
			path:   "/applications/app-1/experimentdefinitions/def-1",
			want:   "DeleteExperimentDefinition",
		},
		{
			name:   "list experiment definitions (account-wide)",
			method: http.MethodGet,
			path:   "/experimentdefinitions",
			want:   "ListExperimentDefinitions",
		},
		{
			name:   "start experiment run",
			method: http.MethodPost,
			path:   "/applications/app-1/experimentdefinitions/def-1/experimentruns",
			want:   "StartExperimentRun",
		},
		{
			name:   "list experiment runs",
			method: http.MethodGet,
			path:   "/applications/app-1/experimentdefinitions/def-1/experimentruns",
			want:   "ListExperimentRuns",
		},
		{
			name:   "get experiment run",
			method: http.MethodGet,
			path:   "/applications/app-1/experimentdefinitions/def-1/experimentruns/1",
			want:   "GetExperimentRun",
		},
		{
			name:   "update experiment run",
			method: http.MethodPatch,
			path:   "/applications/app-1/experimentdefinitions/def-1/experimentruns/1/update",
			want:   "UpdateExperimentRun",
		},
		{
			name:   "stop experiment run",
			method: http.MethodPatch,
			path:   "/applications/app-1/experimentdefinitions/def-1/experimentruns/1/stop",
			want:   "StopExperimentRun",
		},
		{
			name:   "list experiment run events",
			method: http.MethodGet,
			path:   "/applications/app-1/experimentdefinitions/def-1/experimentruns/1/events",
			want:   "ListExperimentRunEvents",
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

func TestHandler_UnknownRoute(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/unknown/path", nil)
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

// TestHandler_ResourceIDsPresentAcrossResources verifies that all resource
// types return a non-empty Id field in Create responses — required for
// subsequent operations. Exercises the dispatch table across every resource
// family in one pass, so it lives here rather than in a single family's
// test file.
func TestHandler_ResourceIDsPresentAcrossResources(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create application.
	appRec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"Name":"id-app"}`))
	require.Equal(t, http.StatusCreated, appRec.Code)

	var app struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &app))
	require.NotEmpty(t, app.ID, "Application must have a non-empty Id")

	// Create environment.
	envRec := doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/environments",
		[]byte(`{"Name":"id-env"}`))
	require.Equal(t, http.StatusCreated, envRec.Code)

	var env struct {
		ID            string `json:"Id"`
		ApplicationID string `json:"ApplicationId"`
	}
	require.NoError(t, json.Unmarshal(envRec.Body.Bytes(), &env))
	require.NotEmpty(t, env.ID, "Environment must have a non-empty Id")
	assert.Equal(t, app.ID, env.ApplicationID,
		"Environment.ApplicationId must match the parent application")

	// Create configuration profile.
	profRec := doRequest(t, h, http.MethodPost,
		"/applications/"+app.ID+"/configurationprofiles",
		[]byte(`{"Name":"id-profile","LocationUri":"hosted","Type":"AWS.Freeform"}`))
	require.Equal(t, http.StatusCreated, profRec.Code)

	var prof struct {
		ID            string `json:"Id"`
		ApplicationID string `json:"ApplicationId"`
		LocationURI   string `json:"LocationUri"`
	}
	require.NoError(t, json.Unmarshal(profRec.Body.Bytes(), &prof))
	require.NotEmpty(t, prof.ID, "ConfigurationProfile must have a non-empty Id")
	assert.Equal(t, app.ID, prof.ApplicationID)
	assert.Equal(t, "hosted", prof.LocationURI)

	// Create deployment strategy.
	stratRec := doRequest(t, h, http.MethodPost, "/deploymentstrategies",
		[]byte(`{"Name":"id-strat","DeploymentDurationInMinutes":5,"GrowthFactor":100,"ReplicateTo":"NONE"}`))
	require.Equal(t, http.StatusCreated, stratRec.Code)

	var strat struct {
		ID   string `json:"Id"`
		Name string `json:"Name"`
	}
	require.NoError(t, json.Unmarshal(stratRec.Body.Bytes(), &strat))
	require.NotEmpty(t, strat.ID, "DeploymentStrategy must have a non-empty Id")
	assert.Equal(t, "id-strat", strat.Name)
}

// TestHandler_ListResponsesUseItemsKey verifies that all list endpoints use
// "Items" as the response key, matching the real AWS AppConfig REST API.
// Exercises every family's list route in one pass, so it lives here rather
// than in a single family's test file.
func TestHandler_ListResponsesUseItemsKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	appRec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"Name":"items-app"}`))
	require.Equal(t, http.StatusCreated, appRec.Code)

	var app struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &app))

	envRec := doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/environments",
		[]byte(`{"Name":"items-env"}`))
	require.Equal(t, http.StatusCreated, envRec.Code)

	profRec := doRequest(t, h, http.MethodPost,
		"/applications/"+app.ID+"/configurationprofiles",
		[]byte(`{"Name":"items-prof","LocationUri":"hosted"}`))
	require.Equal(t, http.StatusCreated, profRec.Code)

	var prof struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal(profRec.Body.Bytes(), &prof))

	stratRec := doRequest(t, h, http.MethodPost, "/deploymentstrategies",
		[]byte(`{"Name":"items-strat","DeploymentDurationInMinutes":0,"GrowthFactor":100,"ReplicateTo":"NONE"}`))
	require.Equal(t, http.StatusCreated, stratRec.Code)

	var env struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal(envRec.Body.Bytes(), &env))

	listPaths := []struct {
		name string
		path string
	}{
		{"ListApplications", "/applications"},
		{"ListEnvironments", "/applications/" + app.ID + "/environments"},
		{"ListConfigurationProfiles", "/applications/" + app.ID + "/configurationprofiles"},
		{"ListDeploymentStrategies", "/deploymentstrategies"},
		{"ListDeployments", "/applications/" + app.ID + "/environments/" + env.ID + "/deployments"},
		{
			"ListHostedConfigurationVersions",
			"/applications/" + app.ID + "/configurationprofiles/" + prof.ID + "/hostedconfigurationversions",
		},
		{"ListExtensions", "/extensions"},
		{"ListExtensionAssociations", "/extensionassociations"},
	}

	for _, tt := range listPaths {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, http.MethodGet, tt.path, nil)
			require.Equal(t, http.StatusOK, rec.Code, "path: %s", tt.path)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			_, ok := resp["Items"]
			assert.True(t, ok, "%s: response must have 'Items' key, got keys: %v",
				tt.name, mapKeys(resp))
		})
	}
}

func mapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	return keys
}
