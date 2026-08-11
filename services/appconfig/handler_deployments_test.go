package appconfig_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

// doRequestWithHeader is like doRequest but sets an additional request
// header -- used for AppConfig ops whose real request shape binds a field to
// a header rather than the JSON body (e.g. StopDeployment's Allow-Revert).
func doRequestWithHeader(
	t *testing.T,
	h *appconfig.Handler,
	method, path, headerName, headerValue string,
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
	req.Header.Set(headerName, headerValue)
	req = req.WithContext(logger.Save(t.Context(), slog.Default()))

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// waitForDeploymentTerminal polls GetDeployment until State reaches a
// terminal value (COMPLETE/ROLLED_BACK/REVERTED) or the deadline elapses,
// returning the last-observed deployment.
func waitForDeploymentTerminal(
	t *testing.T, h *appconfig.Handler, appID, envID string, deploymentNumber int,
) appconfig.Deployment {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)

	var dep appconfig.Deployment

	for time.Now().Before(deadline) {
		rec := doRequest(t, h, http.MethodGet,
			"/applications/"+appID+"/environments/"+envID+"/deployments/"+strconv.Itoa(deploymentNumber), nil)
		require.Equal(t, http.StatusOK, rec.Code)
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &dep))

		switch dep.State {
		case "COMPLETE", "ROLLED_BACK", "REVERTED":
			return dep
		}

		time.Sleep(time.Millisecond)
	}

	t.Fatalf("deployment did not reach a terminal state within the deadline, last state: %s", dep.State)

	return dep
}

// seedExperimentRunHTTP creates an application, environment, feature-flag
// configuration profile, and experiment definition through the real router
// path, returning the application ID and the created
// appconfig.ExperimentDefinition.
func seedExperimentRunHTTP(t *testing.T, h *appconfig.Handler) (string, appconfig.ExperimentDefinition) {
	t.Helper()

	appRec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"Name":"run-http-app"}`))
	require.Equal(t, http.StatusCreated, appRec.Code)

	var app struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &app))

	envRec := doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/environments",
		[]byte(`{"Name":"run-http-env"}`))
	require.Equal(t, http.StatusCreated, envRec.Code)

	var env struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal(envRec.Body.Bytes(), &env))

	profRec := doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/configurationprofiles",
		[]byte(`{"Name":"run-http-profile","LocationUri":"hosted","Type":"AWS.AppConfig.FeatureFlags"}`))
	require.Equal(t, http.StatusCreated, profRec.Code)

	var prof struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal(profRec.Body.Bytes(), &prof))

	defBody := `{
		"Name": "run-http-def",
		"AudienceRule": "true",
		"FlagKey": "flag1",
		"EnvironmentIdentifier": "` + env.ID + `",
		"ConfigurationProfileIdentifier": "` + prof.ID + `",
		"Control": {"FlagValue": {"Enabled": false}, "Weight": 50},
		"Treatments": [{"FlagValue": {"Enabled": true}, "Weight": 50}]
	}`
	defRec := doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/experimentdefinitions", []byte(defBody))
	require.Equal(t, http.StatusCreated, defRec.Code)

	var def appconfig.ExperimentDefinition
	require.NoError(t, json.Unmarshal(defRec.Body.Bytes(), &def))

	return app.ID, def
}

// TestHandler_ExperimentRun_Lifecycle drives StartExperimentRun/
// GetExperimentRun/ListExperimentRuns/UpdateExperimentRun/
// StopExperimentRun/ListExperimentRunEvents through the real router path,
// asserting the REST-JSON wire shapes and HTTP methods field-diffed against
// aws-sdk-go-v2/service/appconfig@v1.48.4's serializers.go (POST .../
// experimentruns, PATCH .../stop, PATCH .../update, GET .../events).
func TestHandler_ExperimentRun_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	appID, def := seedExperimentRunHTTP(t, h)

	base := "/applications/" + appID + "/experimentdefinitions/" + def.ID + "/experimentruns"

	startRec := doRequest(t, h, http.MethodPost, base, []byte(`{"ExposurePercentage":10}`))
	require.Equal(t, http.StatusCreated, startRec.Code)

	var run appconfig.ExperimentRun
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &run))
	assert.Equal(t, int32(1), run.Run)
	assert.Equal(t, "RUNNING", run.Status)
	assert.InDelta(t, float32(10), run.ExposurePercentage, 0.001)

	getRec := doRequest(t, h, http.MethodGet, base+"/1", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	listRec := doRequest(t, h, http.MethodGet, base, nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp struct {
		Items []appconfig.ExperimentRun `json:"Items"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	require.Len(t, listResp.Items, 1)

	updateRec := doRequest(t, h, http.MethodPatch, base+"/1/update", []byte(`{"ExposurePercentage":25}`))
	require.Equal(t, http.StatusOK, updateRec.Code)

	var updated appconfig.ExperimentRun
	require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updated))
	assert.InDelta(t, float32(25), updated.ExposurePercentage, 0.001)

	stopRec := doRequest(t, h, http.MethodPatch, base+"/1/stop",
		[]byte(`{"Result":{"ExecutiveSummary":"treatment won"}}`))
	require.Equal(t, http.StatusOK, stopRec.Code)

	var stopped appconfig.ExperimentRun
	require.NoError(t, json.Unmarshal(stopRec.Body.Bytes(), &stopped))
	assert.Equal(t, "DONE", stopped.Status)
	require.NotNil(t, stopped.Result)
	assert.Equal(t, "treatment won", stopped.Result.ExecutiveSummary)

	eventsRec := doRequest(t, h, http.MethodGet, base+"/1/events", nil)
	require.Equal(t, http.StatusOK, eventsRec.Code)

	var eventsResp struct {
		Items []appconfig.ExperimentRunEvent `json:"Items"`
	}
	require.NoError(t, json.Unmarshal(eventsRec.Body.Bytes(), &eventsResp))
	require.Len(t, eventsResp.Items, 3, "RUN_STARTED, EXPOSURE_UPDATED, RUN_STOPPED")
	assert.Equal(t, "RUN_STOPPED", eventsResp.Items[0].EventType, "most-recent-first ordering")
}

// TestHandler_ExperimentRun_HTTP_Errors covers not-found and validation
// error status codes across the experiment-run routes.
func TestHandler_ExperimentRun_HTTP_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		pathSuffix string
		body       []byte
		wantStatus int
	}{
		{
			name:       "get run not found",
			method:     http.MethodGet,
			pathSuffix: "/applications/nonexistent/experimentdefinitions/def-1/experimentruns/1",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "stop run not found",
			method:     http.MethodPatch,
			pathSuffix: "/applications/nonexistent/experimentdefinitions/def-1/experimentruns/1/stop",
			body:       []byte(`{}`),
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "start run definition not found",
			method:     http.MethodPost,
			pathSuffix: "/applications/nonexistent/experimentdefinitions/def-1/experimentruns",
			body:       []byte(`{}`),
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.method, tt.pathSuffix, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
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

	// Create a hosted configuration version (required for StartDeployment
	// to validate ConfigurationVersion against, for a "hosted" profile).
	rec = doRequest(
		t, h, http.MethodPost,
		"/applications/"+app.ID+"/configurationprofiles/"+prof.ID+"/hostedconfigurationversions",
		[]byte(`{"content":"enabled"}`),
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	// Create deployment strategy (required by StartDeployment validation).
	// A non-zero duration and bake time exercise the real DEPLOYING ->
	// BAKING -> COMPLETE state machine (see waitForDeploymentTerminal).
	stratBody := []byte(
		`{"name":"my-strategy","deploymentDurationInMinutes":10,"growthFactor":20,"finalBakeTimeInMinutes":5}`,
	)
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
	assert.Equal(t, "DEPLOYING", dep.State, "a non-zero-duration strategy must not complete synchronously")
	assert.NotEmpty(t, dep.EventLog, "StartDeployment must record a DEPLOYMENT_STARTED event")

	final := waitForDeploymentTerminal(t, h, app.ID, env.ID, 1)
	assert.Equal(t, "COMPLETE", final.State)
	assert.InDelta(t, float32(100.0), final.PercentageComplete, 0.001)

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

	// Stopping a COMPLETE deployment without Allow-Revert is rejected --
	// real AWS only allows it via AllowRevert (see
	// TestHandler_StopDeployment_AllowRevert for that path).
	rec = doRequest(
		t,
		h,
		http.MethodDelete,
		"/applications/"+app.ID+"/environments/"+env.ID+"/deployments/1",
		nil,
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Stop deployment with Allow-Revert reverts it.
	rec = doRequestWithHeader(
		t, h, http.MethodDelete,
		"/applications/"+app.ID+"/environments/"+env.ID+"/deployments/1",
		"Allow-Revert", "true", nil,
	)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	rec = doRequest(t, h, http.MethodGet,
		"/applications/"+app.ID+"/environments/"+env.ID+"/deployments/1", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &dep))
	assert.Equal(t, "REVERTED", dep.State)
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

// TestParity_DeploymentFieldsPresent verifies StartDeployment returns the
// required AWS-accurate fields: DeploymentNumber, State, PercentageComplete,
// ApplicationId, EnvironmentId, ConfigurationProfileId, DeploymentStrategyId.
func TestHandler_DeploymentFieldsPresent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	appRec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"Name":"dep-app"}`))
	require.Equal(t, http.StatusCreated, appRec.Code)

	var app struct {
		ID string `json:"Id"`
	}

	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &app))

	envRec := doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/environments",
		[]byte(`{"Name":"dep-env"}`))
	require.Equal(t, http.StatusCreated, envRec.Code)

	var env struct {
		ID string `json:"Id"`
	}

	require.NoError(t, json.Unmarshal(envRec.Body.Bytes(), &env))

	profRec := doRequest(t, h, http.MethodPost,
		"/applications/"+app.ID+"/configurationprofiles",
		[]byte(`{"Name":"dep-profile","LocationUri":"hosted"}`))
	require.Equal(t, http.StatusCreated, profRec.Code)

	var prof struct {
		ID string `json:"Id"`
	}

	require.NoError(t, json.Unmarshal(profRec.Body.Bytes(), &prof))

	hcvRec := doRequest(t, h, http.MethodPost,
		"/applications/"+app.ID+"/configurationprofiles/"+prof.ID+"/hostedconfigurationversions",
		[]byte(`{"feature":"enabled"}`))
	require.Equal(t, http.StatusCreated, hcvRec.Code)

	stratRec := doRequest(t, h, http.MethodPost, "/deploymentstrategies",
		[]byte(`{"Name":"dep-strat","DeploymentDurationInMinutes":0,"GrowthFactor":100,"ReplicateTo":"NONE"}`))
	require.Equal(t, http.StatusCreated, stratRec.Code)

	var strat struct {
		ID string `json:"Id"`
	}

	require.NoError(t, json.Unmarshal(stratRec.Body.Bytes(), &strat))

	depBody, _ := json.Marshal(map[string]string{
		"ConfigurationProfileId": prof.ID,
		"DeploymentStrategyId":   strat.ID,
		"ConfigurationVersion":   "1",
	})
	depRec := doRequest(t, h, http.MethodPost,
		"/applications/"+app.ID+"/environments/"+env.ID+"/deployments", depBody)
	require.Equal(t, http.StatusCreated, depRec.Code)

	var dep struct {
		ApplicationID          string  `json:"ApplicationId"`
		EnvironmentID          string  `json:"EnvironmentId"`
		ConfigurationProfileID string  `json:"ConfigurationProfileId"`
		DeploymentStrategyID   string  `json:"DeploymentStrategyId"`
		State                  string  `json:"State"`
		PercentageComplete     float32 `json:"PercentageComplete"`
		DeploymentNumber       int32   `json:"DeploymentNumber"`
	}
	require.NoError(t, json.Unmarshal(depRec.Body.Bytes(), &dep))

	assert.Equal(t, app.ID, dep.ApplicationID)
	assert.Equal(t, env.ID, dep.EnvironmentID)
	assert.Equal(t, prof.ID, dep.ConfigurationProfileID)
	assert.Equal(t, strat.ID, dep.DeploymentStrategyID)
	assert.Equal(t, "COMPLETE", dep.State)
	assert.InDelta(t, float32(100.0), dep.PercentageComplete, 0.001)
	assert.Equal(t, int32(1), dep.DeploymentNumber)
}

// TestParity_DeploymentNumberIncrements verifies that successive deployments to
// the same environment get sequential DeploymentNumbers starting at 1.
func TestHandler_DeploymentNumberIncrements(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	appRec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"Name":"seq-app"}`))
	require.Equal(t, http.StatusCreated, appRec.Code)

	var app struct {
		ID string `json:"Id"`
	}

	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &app))

	envRec := doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/environments",
		[]byte(`{"Name":"seq-env"}`))
	require.Equal(t, http.StatusCreated, envRec.Code)

	var env struct {
		ID string `json:"Id"`
	}

	require.NoError(t, json.Unmarshal(envRec.Body.Bytes(), &env))

	profRec := doRequest(t, h, http.MethodPost,
		"/applications/"+app.ID+"/configurationprofiles",
		[]byte(`{"Name":"seq-profile","LocationUri":"hosted"}`))
	require.Equal(t, http.StatusCreated, profRec.Code)

	var prof struct {
		ID string `json:"Id"`
	}

	require.NoError(t, json.Unmarshal(profRec.Body.Bytes(), &prof))

	hcvRec := doRequest(t, h, http.MethodPost,
		"/applications/"+app.ID+"/configurationprofiles/"+prof.ID+"/hostedconfigurationversions",
		[]byte(`{"feature":"enabled"}`))
	require.Equal(t, http.StatusCreated, hcvRec.Code)

	stratRec := doRequest(t, h, http.MethodPost, "/deploymentstrategies",
		[]byte(`{"Name":"seq-strat","DeploymentDurationInMinutes":0,"GrowthFactor":100,"ReplicateTo":"NONE"}`))
	require.Equal(t, http.StatusCreated, stratRec.Code)

	var strat struct {
		ID string `json:"Id"`
	}

	require.NoError(t, json.Unmarshal(stratRec.Body.Bytes(), &strat))

	depBody, _ := json.Marshal(map[string]string{
		"ConfigurationProfileId": prof.ID,
		"DeploymentStrategyId":   strat.ID,
		"ConfigurationVersion":   "1",
	})

	for wantNum := int32(1); wantNum <= 3; wantNum++ {
		rec := doRequest(t, h, http.MethodPost,
			"/applications/"+app.ID+"/environments/"+env.ID+"/deployments", depBody)
		require.Equal(t, http.StatusCreated, rec.Code)

		var dep struct {
			DeploymentNumber int32 `json:"DeploymentNumber"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &dep))
		assert.Equal(t, wantNum, dep.DeploymentNumber,
			"deployment must have sequential DeploymentNumber")
	}
}
