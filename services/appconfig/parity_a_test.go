package appconfig_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_EnvironmentStateIsReadyForDeployment verifies that a newly created
// environment has State "READY_FOR_DEPLOYMENT", matching the real AWS AppConfig API.
func TestParity_EnvironmentStateIsReadyForDeployment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	appRec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"Name":"state-app"}`))
	require.Equal(t, http.StatusCreated, appRec.Code)

	var app struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &app))

	tests := []struct {
		name string
		want string
		body []byte
	}{
		{
			name: "newly created environment is READY_FOR_DEPLOYMENT",
			body: []byte(`{"Name":"prod"}`),
			want: "READY_FOR_DEPLOYMENT",
		},
		{
			name: "second environment also is READY_FOR_DEPLOYMENT",
			body: []byte(`{"Name":"staging"}`),
			want: "READY_FOR_DEPLOYMENT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, http.MethodPost,
				"/applications/"+app.ID+"/environments", tt.body)
			require.Equal(t, http.StatusCreated, rec.Code)

			var env struct {
				State string `json:"State"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))
			assert.Equal(t, tt.want, env.State,
				"environment State must be READY_FOR_DEPLOYMENT (SCREAMING_SNAKE_CASE)")
		})
	}
}

// TestParity_EnvironmentStateRoundTrip verifies the state is persisted correctly
// through Get and List after creation.
func TestParity_EnvironmentStateRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	appRec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"Name":"rt-app"}`))
	require.Equal(t, http.StatusCreated, appRec.Code)

	var app struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &app))

	envRec := doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/environments",
		[]byte(`{"Name":"staging"}`))
	require.Equal(t, http.StatusCreated, envRec.Code)

	var env struct {
		ID    string `json:"Id"`
		State string `json:"State"`
	}
	require.NoError(t, json.Unmarshal(envRec.Body.Bytes(), &env))
	assert.Equal(t, "READY_FOR_DEPLOYMENT", env.State)
	require.NotEmpty(t, env.ID)

	// GetEnvironment must also return correct state.
	getRec := doRequest(t, h, http.MethodGet,
		"/applications/"+app.ID+"/environments/"+env.ID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getEnv struct {
		State string `json:"State"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getEnv))
	assert.Equal(t, "READY_FOR_DEPLOYMENT", getEnv.State, "GetEnvironment must also return READY_FOR_DEPLOYMENT")

	// ListEnvironments must also return correct state.
	listRec := doRequest(t, h, http.MethodGet, "/applications/"+app.ID+"/environments", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp struct {
		Items []struct {
			State string `json:"State"`
		} `json:"Items"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	require.Len(t, listResp.Items, 1)
	assert.Equal(t, "READY_FOR_DEPLOYMENT", listResp.Items[0].State,
		"ListEnvironments must also return READY_FOR_DEPLOYMENT")
}

// TestParity_ResourceIDsPresent verifies that all resource types return a
// non-empty Id field in Create responses — required for subsequent operations.
func TestParity_ResourceIDsPresent(t *testing.T) {
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

// TestParity_DeploymentFieldsPresent verifies StartDeployment returns the
// required AWS-accurate fields: DeploymentNumber, State, PercentageComplete,
// ApplicationId, EnvironmentId, ConfigurationProfileId, DeploymentStrategyId.
func TestParity_DeploymentFieldsPresent(t *testing.T) {
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
func TestParity_DeploymentNumberIncrements(t *testing.T) {
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

// TestParity_HostedConfigVersionResponseHeaders verifies that
// CreateHostedConfigurationVersion sets the Appconfig-Configuration-Version
// response header, matching real AWS AppConfig behavior.
func TestParity_HostedConfigVersionResponseHeaders(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	appRec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"Name":"hcv-hdr-app"}`))
	require.Equal(t, http.StatusCreated, appRec.Code)

	var app struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &app))

	profRec := doRequest(t, h, http.MethodPost,
		"/applications/"+app.ID+"/configurationprofiles",
		[]byte(`{"Name":"hcv-hdr-profile","LocationUri":"hosted","Type":"AWS.Freeform"}`))
	require.Equal(t, http.StatusCreated, profRec.Code)

	var prof struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal(profRec.Body.Bytes(), &prof))

	base := "/applications/" + app.ID + "/configurationprofiles/" + prof.ID +
		"/hostedconfigurationversions"

	for wantVer, content := range [][]byte{
		[]byte(`{"value":"first"}`),
		[]byte(`{"value":"second"}`),
		[]byte(`{"value":"third"}`),
	} {
		rec := doRequest(t, h, http.MethodPost, base, content)
		require.Equal(t, http.StatusCreated, rec.Code)

		verHeader := rec.Header().Get("Appconfig-Configuration-Version")
		assert.NotEmpty(t, verHeader,
			"Appconfig-Configuration-Version header must be set on CreateHostedConfigurationVersion")
		assert.Equal(t, string(rune('1'+wantVer)), verHeader,
			"version header must increment with each version")
	}
}

// TestParity_ListResponsesUseItemsKey verifies that all list endpoints use "Items"
// as the response key, matching the real AWS AppConfig REST API.
func TestParity_ListResponsesUseItemsKey(t *testing.T) {
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
