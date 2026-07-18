package apigateway_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

// TestDeleteDeployment_StageProtection verifies that deleting a deployment referenced
// by a stage returns an error. Real AWS returns BadRequestException in this case.
func TestDeleteDeployment_StageProtection(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()
	apiID := createParityAPI(t, h, "depl-protect-api")

	// Create a deployment via CreateDeployment.
	rec := restRequest(t, h, http.MethodPost,
		fmt.Sprintf("/restapis/%s/deployments", apiID),
		`{"stageName":"prod","description":"initial"}`,
	)
	require.True(t, rec.Code >= 200 && rec.Code < 300,
		"create deployment: %d %s", rec.Code, rec.Body.String())

	var depl map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &depl))
	deplID, _ := depl["id"].(string)
	require.NotEmpty(t, deplID)

	// Attempt to delete the deployment — stage "prod" references it.
	rec = restRequest(t, h, http.MethodDelete,
		fmt.Sprintf("/restapis/%s/deployments/%s", apiID, deplID), "")

	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"deleting a deployment referenced by a stage must return 400; body: %s", rec.Body.String())
}

// TestDeleteDeployment_Unreferenced verifies that a deployment not referenced by
// any stage can be deleted successfully.
func TestDeleteDeployment_Unreferenced(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()
	apiID := createParityAPI(t, h, "depl-unref-api")

	// Create a deployment with no stage.
	rec := restRequest(t, h, http.MethodPost,
		fmt.Sprintf("/restapis/%s/deployments", apiID),
		`{"description":"no stage"}`,
	)
	require.True(t, rec.Code >= 200 && rec.Code < 300,
		"create deployment: %d %s", rec.Code, rec.Body.String())

	var depl map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &depl))
	deplID, _ := depl["id"].(string)
	require.NotEmpty(t, deplID)

	// Delete succeeds — no stage references it.
	rec = restRequest(t, h, http.MethodDelete,
		fmt.Sprintf("/restapis/%s/deployments/%s", apiID, deplID), "")

	assert.Equal(t, http.StatusNoContent, rec.Code,
		"deleting an unreferenced deployment must succeed; body: %s", rec.Body.String())
}

// TestCreateDeployment_StageDescription verifies that stageDescription
// provided in CreateDeployment is applied to the automatically-created stage,
// matching AWS behaviour. Previously stageDescription was silently dropped.
func TestCreateDeployment_StageDescription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		stageDescription string
		wantDescription  string
	}{
		{
			name:             "stage_description_propagated",
			stageDescription: "prod stage for v1",
			wantDescription:  "prod stage for v1",
		},
		{
			name:             "no_stage_description_means_stage_description_empty",
			stageDescription: "",
			wantDescription:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(b)

			createAPIRec := restRequest(t, h, http.MethodPost, "/restapis", `{"name":"depl-sd-api"}`)
			require.True(t, createAPIRec.Code >= 200 && createAPIRec.Code < 300)

			var apiResp map[string]any
			require.NoError(t, json.NewDecoder(createAPIRec.Body).Decode(&apiResp))
			apiID, _ := apiResp["id"].(string)
			require.NotEmpty(t, apiID)

			deplBody := `{"stageName":"prod","description":"deploy v1","stageDescription":"` +
				tt.stageDescription + `"}`
			deplRec := restRequest(t, h, http.MethodPost, "/restapis/"+apiID+"/deployments", deplBody)
			require.True(t, deplRec.Code >= 200 && deplRec.Code < 300)

			stageRec := restRequest(t, h, http.MethodGet, "/restapis/"+apiID+"/stages/prod", "")
			require.True(t, stageRec.Code >= 200 && stageRec.Code < 300)

			var stage map[string]any
			require.NoError(t, json.NewDecoder(stageRec.Body).Decode(&stage))
			// JSON omitempty means empty description is absent (nil), not "".
			got, _ := stage["description"].(string)
			assert.Equal(t, tt.wantDescription, got)
		})
	}
}

// TestCreateDeployment_StageVariables verifies that variables provided
// in CreateDeployment are applied to the automatically-created stage, matching
// AWS behaviour. Previously variables were silently dropped.
func TestCreateDeployment_StageVariables(t *testing.T) {
	t.Parallel()

	tests := []struct {
		variables    map[string]string
		name         string
		wantVarKey   string
		wantVarValue string
		wantVarsLen  int
	}{
		{
			name:         "variables_propagated_to_stage",
			variables:    map[string]string{"env": "production", "version": "v1"},
			wantVarsLen:  2,
			wantVarKey:   "env",
			wantVarValue: "production",
		},
		{
			name:        "no_variables_is_fine",
			variables:   nil,
			wantVarsLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(b)

			createAPIRec := restRequest(t, h, http.MethodPost, "/restapis", `{"name":"depl-vars-api"}`)
			require.True(t, createAPIRec.Code >= 200 && createAPIRec.Code < 300)

			var apiResp map[string]any
			require.NoError(t, json.NewDecoder(createAPIRec.Body).Decode(&apiResp))
			apiID, _ := apiResp["id"].(string)
			require.NotEmpty(t, apiID)

			var deplBody string
			if tt.variables != nil {
				varsJSON, _ := json.Marshal(tt.variables)
				deplBody = `{"stageName":"prod","variables":` + string(varsJSON) + `}`
			} else {
				deplBody = `{"stageName":"prod"}`
			}
			deplRec := restRequest(t, h, http.MethodPost, "/restapis/"+apiID+"/deployments", deplBody)
			require.True(t, deplRec.Code >= 200 && deplRec.Code < 300)

			stageRec := restRequest(t, h, http.MethodGet, "/restapis/"+apiID+"/stages/prod", "")
			require.True(t, stageRec.Code >= 200 && stageRec.Code < 300)

			var stage map[string]any
			require.NoError(t, json.NewDecoder(stageRec.Body).Decode(&stage))

			vars, _ := stage["variables"].(map[string]any)
			assert.Len(t, vars, tt.wantVarsLen)
			if tt.wantVarKey != "" {
				assert.Equal(t, tt.wantVarValue, vars[tt.wantVarKey])
			}
		})
	}
}

// TestUpdateDeployment tests UpdateDeployment.
func TestUpdateDeployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		description string
		wantCode    int
		useValid    bool
	}{
		{
			name:        "update_description",
			description: "updated deployment",
			wantCode:    http.StatusOK,
			useValid:    true,
		},
		{
			name:     "deployment_not_found",
			wantCode: http.StatusNotFound,
			useValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			apiID := boostAPI(t, handler, e)
			deplID := boostDeployment(t, handler, e, apiID, "prod")

			lookupID := deplID
			if !tt.useValid {
				lookupID = "notexist"
			}

			rec := postWithHandler(t, handler, e, "UpdateDeployment",
				fmt.Sprintf(`{"restApiId":%q,"deploymentId":%q,"description":%q}`,
					apiID, lookupID, tt.description))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.description, resp["description"])
			}
		})
	}
}

// TestHandler_GetAndDeleteDeployment exercises the GetDeployment and DeleteDeployment
// action closures in deploymentActions which are not hit by other tests.
func TestHandler_GetAndDeleteDeployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "GetDeployment_returns_200",
			action:   "GetDeployment",
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteDeployment_returns_204",
			action:   "DeleteDeployment",
			wantCode: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e := sharedSetup()

			createRec := postWithHandler(t, h, e, "CreateRestApi", `{"name":"api"}`)
			var created map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
			apiID := created["id"].(string)

			deplRec := postWithHandler(t, h, e, "CreateDeployment",
				fmt.Sprintf(`{"restApiId":%q,"stageName":"prod","description":""}`, apiID))
			var depl map[string]any
			require.NoError(t, json.Unmarshal(deplRec.Body.Bytes(), &depl))
			deplID := depl["id"].(string)

			if tt.action == "DeleteDeployment" {
				// Delete the referencing stage first so the deployment can be removed.
				stageRec := restRequest(t, h, http.MethodDelete,
					fmt.Sprintf("/restapis/%s/stages/prod", apiID), "")
				require.Equal(t, http.StatusNoContent, stageRec.Code)
			}

			rec := postWithHandler(t, h, e, tt.action,
				fmt.Sprintf(`{"restApiId":%q,"deploymentId":%q}`, apiID, deplID))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestHandler_RESTPath_Deployments exercises all deployment REST-path branches in
// parseAPIGWRESTPath that are not covered by the X-Amz-Target tests.
func TestHandler_RESTPath_Deployments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *apigateway.InMemoryBackend) string
		name     string
		method   string
		body     string
		wantCode int
	}{
		{
			name:   "POST_deployments_creates_deployment",
			method: http.MethodPost,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})

				return fmt.Sprintf("/restapis/%s/deployments", api.ID)
			},
			body:     `{"stageName":"v1","description":""}`,
			wantCode: http.StatusCreated,
		},
		{
			name:   "GET_deployments_lists",
			method: http.MethodGet,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
				_, _ = b.CreateDeployment(api.ID, "prod", "")

				return fmt.Sprintf("/restapis/%s/deployments", api.ID)
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "GET_deployment_by_id",
			method: http.MethodGet,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
				dep, _ := b.CreateDeployment(api.ID, "prod", "")

				return fmt.Sprintf("/restapis/%s/deployments/%s", api.ID, dep.ID)
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DELETE_deployment_returns_204",
			method: http.MethodDelete,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
				dep, _ := b.CreateDeployment(api.ID, "", "")

				return fmt.Sprintf("/restapis/%s/deployments/%s", api.ID, dep.ID)
			},
			wantCode: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(backend)

			path := tt.setup(backend)
			rec := restRequest(t, h, tt.method, path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestBackend_DeploymentAndStage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "create_deployment_and_stage",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})

				depl, err := b.CreateDeployment(api.ID, "prod", "initial")
				require.NoError(t, err)
				assert.NotEmpty(t, depl.ID)

				depls, err := b.GetDeployments(api.ID)
				require.NoError(t, err)
				assert.Len(t, depls, 1)

				stages, err := b.GetStages(api.ID)
				require.NoError(t, err)
				assert.Len(t, stages, 1)
				assert.Equal(t, "prod", stages[0].StageName)
			},
		},
		{
			name: "get_and_delete_stage",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
				_, _ = b.CreateDeployment(api.ID, "v1", "")

				stage, err := b.GetStage(api.ID, "v1")
				require.NoError(t, err)
				assert.Equal(t, "v1", stage.StageName)

				err = b.DeleteStage(api.ID, "v1")
				require.NoError(t, err)

				_, err = b.GetStage(api.ID, "v1")
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}
