package appconfig_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

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
