package apigatewayv2_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateDeployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		desc       string
		wantStatus int
		apiExists  bool
	}{
		{
			name:       "success",
			desc:       "initial deploy",
			wantStatus: http.StatusCreated,
			apiExists:  true,
		},
		{
			name:       "api_not_found",
			desc:       "deploy",
			wantStatus: http.StatusNotFound,
			apiExists:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			apiID := "nonexistent"
			if tt.apiExists {
				apiID = createAPI(t, h, "test-api")
			}

			rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/deployments", apiID), map[string]any{
				"description": tt.desc,
			})

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusCreated {
				var deployment apigatewayv2.Deployment
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &deployment))
				assert.NotEmpty(t, deployment.DeploymentID)
				assert.Equal(t, "DEPLOYED", deployment.DeploymentStatus)
			}
		})
	}
}

func TestHandler_GetDeployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		wantStatus      int
		setupDeployment bool
	}{
		{
			name:            "existing",
			wantStatus:      http.StatusOK,
			setupDeployment: true,
		},
		{
			name:            "not_found",
			wantStatus:      http.StatusNotFound,
			setupDeployment: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			deploymentID := "nonexistent"
			if tt.setupDeployment {
				rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/deployments", apiID), map[string]any{})
				require.Equal(t, http.StatusCreated, rr.Code)

				var deployment apigatewayv2.Deployment
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &deployment))
				deploymentID = deployment.DeploymentID
			}

			rr := doRequest(t, h, http.MethodGet, fmt.Sprintf("/v2/apis/%s/deployments/%s", apiID, deploymentID), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_DeleteDeployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		wantStatus      int
		setupDeployment bool
	}{
		{
			name:            "success",
			wantStatus:      http.StatusNoContent,
			setupDeployment: true,
		},
		{
			name:            "not_found",
			wantStatus:      http.StatusNotFound,
			setupDeployment: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			deploymentID := "nonexistent"
			if tt.setupDeployment {
				rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/deployments", apiID), map[string]any{})
				require.Equal(t, http.StatusCreated, rr.Code)

				var deployment apigatewayv2.Deployment
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &deployment))
				deploymentID = deployment.DeploymentID
			}

			rr := doRequest(
				t,
				h,
				http.MethodDelete,
				fmt.Sprintf("/v2/apis/%s/deployments/%s", apiID, deploymentID),
				nil,
			)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_GetDeployments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		deployCount int
		wantStatus  int
		apiExists   bool
	}{
		{
			name:        "empty",
			deployCount: 0,
			wantStatus:  http.StatusOK,
			apiExists:   true,
		},
		{
			name:        "multiple",
			deployCount: 2,
			wantStatus:  http.StatusOK,
			apiExists:   true,
		},
		{
			name:       "api_not_found",
			wantStatus: http.StatusNotFound,
			apiExists:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			apiID := "nonexistent"
			if tt.apiExists {
				apiID = createAPI(t, h, "test-api")

				for range tt.deployCount {
					rr := doRequest(
						t,
						h,
						http.MethodPost,
						fmt.Sprintf("/v2/apis/%s/deployments", apiID),
						map[string]any{},
					)
					require.Equal(t, http.StatusCreated, rr.Code)
				}
			}

			rr := doRequest(t, h, http.MethodGet, fmt.Sprintf("/v2/apis/%s/deployments", apiID), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusOK {
				type listResp struct {
					Items []apigatewayv2.Deployment `json:"items"`
				}

				var resp listResp
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
				assert.Len(t, resp.Items, tt.deployCount)
			}
		})
	}
}

func TestHandler_UpdateDeployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *apigatewayv2.Handler) (apiID, deploymentID string)
		body       any
		name       string
		wantDesc   string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *apigatewayv2.Handler) (string, string) {
				apiID := createAPI(t, h, "test-api")
				rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/deployments", apiID), map[string]any{
					"description": "initial",
				})
				require.Equal(t, http.StatusCreated, rr.Code)
				var dep apigatewayv2.Deployment
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &dep))

				return apiID, dep.DeploymentID
			},
			body:       map[string]any{"description": "updated"},
			wantStatus: http.StatusOK,
			wantDesc:   "updated",
		},
		{
			name: "api_not_found",
			setup: func(_ *apigatewayv2.Handler) (string, string) {
				return "nonexistent", "dep123"
			},
			body:       map[string]any{"description": "x"},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "deployment_not_found",
			setup: func(h *apigatewayv2.Handler) (string, string) {
				return createAPI(t, h, "test-api"), "nonexistent"
			},
			body:       map[string]any{"description": "x"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID, deploymentID := tt.setup(h)

			rr := doRequest(t, h, http.MethodPatch,
				fmt.Sprintf("/v2/apis/%s/deployments/%s", apiID, deploymentID), tt.body)

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantDesc != "" {
				var dep apigatewayv2.Deployment
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &dep))
				assert.Equal(t, tt.wantDesc, dep.Description)
			}
		})
	}
}

// createStageAutoDeploy creates a stage with the given autoDeploy flag.
func createStageAutoDeploy(t *testing.T, h *apigatewayv2.Handler, apiID, stageName string, autoDeploy bool) {
	t.Helper()

	rr := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/stages", map[string]any{
		"stageName":  stageName,
		"autoDeploy": autoDeploy,
	})
	require.Equal(t, http.StatusCreated, rr.Code)
}

func getStage(t *testing.T, h *apigatewayv2.Handler, apiID, stageName string) apigatewayv2.Stage {
	t.Helper()

	rr := doRequest(t, h, http.MethodGet, "/v2/apis/"+apiID+"/stages/"+stageName, nil)
	require.Equal(t, http.StatusOK, rr.Code)

	var s apigatewayv2.Stage
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &s))

	return s
}

func createIntegrationHelper(t *testing.T, h *apigatewayv2.Handler, apiID string) string {
	t.Helper()

	rr := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/integrations", map[string]any{
		"integrationType":      "AWS_PROXY",
		"integrationUri":       backendFnURI,
		"payloadFormatVersion": "2.0",
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	var integ apigatewayv2.Integration
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &integ))

	return integ.IntegrationID
}

func TestAutoDeploy_RouteAndIntegrationChangesDeploy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		autoDeploy   bool
		wantDeployed bool
	}{
		{name: "auto_deploy_enabled_deploys", autoDeploy: true, wantDeployed: true},
		{name: "auto_deploy_disabled_no_deploy", autoDeploy: false, wantDeployed: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "autodeploy-api")
			createStageAutoDeploy(t, h, apiID, "prod", tt.autoDeploy)

			// Before any change the stage has no deployment.
			require.Empty(t, getStage(t, h, apiID, "prod").DeploymentID)

			// Creating an integration then a route mutates routing config.
			integID := createIntegrationHelper(t, h, apiID)

			rr := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/routes", map[string]any{
				"routeKey": "GET /a",
				"target":   "integrations/" + integID,
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			stage := getStage(t, h, apiID, "prod")

			if tt.wantDeployed {
				require.NotEmpty(t, stage.DeploymentID, "auto-deploy should link a deployment")

				// The linked deployment must be marked autoDeployed.
				drr := doRequest(t, h, http.MethodGet, "/v2/apis/"+apiID+"/deployments", nil)
				require.Equal(t, http.StatusOK, drr.Code)

				var out struct {
					Items []apigatewayv2.Deployment `json:"items"`
				}
				require.NoError(t, json.Unmarshal(drr.Body.Bytes(), &out))
				require.NotEmpty(t, out.Items)

				var foundAuto bool
				for _, d := range out.Items {
					if d.AutoDeployed {
						foundAuto = true
					}
				}
				assert.True(t, foundAuto, "expected an autoDeployed deployment")
			} else {
				assert.Empty(t, stage.DeploymentID, "no auto-deploy expected")
			}
		})
	}
}

func TestGetDeployments_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	apiID := createAPI(t, h, "depl-api")

	// Create a stage first (required for deployments in some configurations)
	doRequest(t, h, http.MethodPost,
		fmt.Sprintf("/v2/apis/%s/stages", apiID),
		map[string]any{"stageName": "prod"},
	)

	for i := range 3 {
		rr := doRequest(t, h, http.MethodPost,
			fmt.Sprintf("/v2/apis/%s/deployments", apiID),
			map[string]any{"description": fmt.Sprintf("deploy %d", i)},
		)
		require.Equal(t, http.StatusCreated, rr.Code)
	}

	seen := map[string]int{}
	nextToken := ""
	pages := 0

	for {
		path := fmt.Sprintf("/v2/apis/%s/deployments?maxResults=2", apiID)
		if nextToken != "" {
			path += "&nextToken=" + nextToken
		}

		rr := doRequest(t, h, http.MethodGet, path, nil)
		require.Equal(t, http.StatusOK, rr.Code)

		var resp struct {
			NextToken string           `json:"nextToken"`
			Items     []map[string]any `json:"items"`
		}
		require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
		require.LessOrEqual(t, len(resp.Items), 2)

		for _, d := range resp.Items {
			id, _ := d["deploymentId"].(string)
			seen[id]++
		}

		pages++
		nextToken = resp.NextToken

		if nextToken == "" {
			break
		}

		require.Less(t, pages, 20)
	}

	assert.Equal(t, 2, pages)
	assert.Len(t, seen, 3)

	for id, count := range seen {
		assert.Equalf(t, 1, count, "deployment %s duplicated", id)
	}
}

// TestCreateDeployment_InvalidStageName404 verifies that supplying a
// non-existent stageName in CreateDeployment returns HTTP 404, not 500.
// Real AWS returns NotFoundException when the stage does not exist.
func TestCreateDeployment_InvalidStageName404(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	apiID := createAPI(t, h, "deploy-api")

	rr := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/deployments",
		map[string]any{"stageName": "nonexistent-stage"})

	assert.Equal(t, http.StatusNotFound, rr.Code,
		"CreateDeployment with unknown stageName must return 404, got body: %s", rr.Body.String())
}

// TestCreateDeployment_ValidStageName201 verifies the happy path:
// when the stage exists the deployment is created successfully.
func TestCreateDeployment_ValidStageName201(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	apiID := createAPI(t, h, "deploy-api2")

	// Create a stage first.
	rrStage := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/stages",
		map[string]any{"stageName": "prod"})
	require.Equal(t, http.StatusCreated, rrStage.Code)

	// Create a deployment referencing that stage.
	rr := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/deployments",
		map[string]any{"stageName": "prod"})
	assert.Equal(t, http.StatusCreated, rr.Code)

	var dep map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &dep))
	assert.Equal(t, "DEPLOYED", dep["deploymentStatus"])
}
