package apigateway_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/apigateway"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// mockAPIGWConfigProvider implements config.Provider for testing.
type mockAPIGWConfigProvider struct{}

func (m *mockAPIGWConfigProvider) GetGlobalConfig() config.GlobalConfig {
	return config.GlobalConfig{AccountID: "111111111111", Region: "eu-west-1"}
}

func TestProvider_APIGateway(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ctx         *service.AppContext
		name        string
		wantSvcName string
		wantName    bool
	}{
		{
			name:     "name_returns_APIGateway",
			wantName: true,
		},
		{
			name: "init_with_config",
			ctx: &service.AppContext{
				Logger: slog.Default(),
				Config: &mockAPIGWConfigProvider{},
			},
			wantSvcName: "APIGateway",
		},
		{
			name: "init_without_config",
			ctx:  &service.AppContext{Logger: slog.Default()},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &apigateway.Provider{}

			if tt.wantName {
				assert.Equal(t, "APIGateway", p.Name())

				return
			}

			svc, err := p.Init(tt.ctx)
			require.NoError(t, err)
			require.NotNil(t, svc)

			if tt.wantSvcName != "" {
				assert.Equal(t, tt.wantSvcName, svc.Name())
			}
		})
	}
}

func TestHandler_APIGateway_Metadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantName      string
		wantOps       []string
		wantPriority  int
		checkPriority bool
	}{
		{
			name:     "name",
			wantName: "APIGateway",
		},
		{
			name:          "match_priority",
			wantPriority:  100,
			checkPriority: true,
		},
		{
			name:    "supported_operations",
			wantOps: []string{"CreateRestApi", "GetRestApis", "PutMethod"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := apigateway.NewHandler(apigateway.NewInMemoryBackend())

			switch {
			case tt.wantName != "":
				assert.Equal(t, tt.wantName, h.Name())
			case tt.checkPriority:
				assert.Equal(t, tt.wantPriority, h.MatchPriority())
			case len(tt.wantOps) > 0:
				ops := h.GetSupportedOperations()
				for _, op := range tt.wantOps {
					assert.Contains(t, ops, op)
				}
			}
		})
	}
}

func TestHandler_APIGateway_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		target    string
		wantMatch bool
	}{
		{
			name:      "matches_APIGateway_target",
			target:    "APIGateway.CreateRestApi",
			wantMatch: true,
		},
		{
			name:      "no_match_for_other_service",
			target:    "AmazonSQS.CreateQueue",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := apigateway.NewHandler(apigateway.NewInMemoryBackend())
			matcher := h.RouteMatcher()
			e := echo.New()

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			ctx := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantMatch, matcher(ctx))
		})
	}
}

func TestHandler_APIGateway_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		wantOp string
	}{
		{
			name:   "known_target_extracts_operation",
			target: "APIGateway.CreateRestApi",
			wantOp: "CreateRestApi",
		},
		{
			name:   "missing_target_returns_Unknown",
			target: "",
			wantOp: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := apigateway.NewHandler(apigateway.NewInMemoryBackend())
			e := echo.New()

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}

			assert.Equal(t, tt.wantOp, h.ExtractOperation(e.NewContext(req, httptest.NewRecorder())))
		})
	}
}

func TestHandler_APIGateway_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantResource string
	}{
		{
			name:         "body_with_restApiId",
			body:         `{"restApiId":"abc123"}`,
			wantResource: "abc123",
		},
		{
			name:         "body_without_restApiId",
			body:         `{}`,
			wantResource: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := apigateway.NewHandler(apigateway.NewInMemoryBackend())
			e := echo.New()

			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			got := h.ExtractResource(e.NewContext(req, httptest.NewRecorder()))

			assert.Equal(t, tt.wantResource, got)
		})
	}
}

func TestHandler_APIGateway_RequestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		method   string
		path     string
		target   string
		body     string
		wantCode int
	}{
		{
			name:     "missing_target_returns_400",
			method:   http.MethodPost,
			path:     "/",
			body:     "{}",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "PUT_method_returns_405",
			method:   http.MethodPut,
			path:     "/something",
			target:   "CreateRestApi",
			wantCode: http.StatusMethodNotAllowed,
		},
		{
			name:     "invalid_JSON_returns_500",
			method:   http.MethodPost,
			path:     "/",
			target:   "CreateRestApi",
			body:     "not-json",
			wantCode: http.StatusInternalServerError,
		},
		{
			name:     "missing_required_params_returns_400",
			method:   http.MethodPost,
			path:     "/",
			target:   "CreateRestApi",
			body:     "{}",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()

			h := apigateway.NewHandler(apigateway.NewInMemoryBackend())

			var req *http.Request
			if tt.body != "" {
				req = httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			} else {
				req = httptest.NewRequest(tt.method, tt.path, nil)
			}
			if tt.target != "" {
				req.Header.Set("X-Amz-Target", "APIGateway."+tt.target)
			}

			rec := httptest.NewRecorder()
			err := h.Handler()(e.NewContext(req, rec))
			require.NoError(t, err)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_APIGateway_NotFoundErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
		body   string
	}{
		{
			name:   "DeleteRestApi_nonexistent",
			action: "DeleteRestApi",
			body:   `{"restApiId":"nonexistent"}`,
		},
		{
			name:   "GetRestApi_nonexistent",
			action: "GetRestApi",
			body:   `{"restApiId":"nonexistent"}`,
		},
		{
			name:   "GetResources_nonexistent",
			action: "GetResources",
			body:   `{"restApiId":"nonexistent"}`,
		},
		{
			name:   "GetResource_nonexistent",
			action: "GetResource",
			body:   `{"restApiId":"nonexistent","resourceId":"r1"}`,
		},
		{
			name:   "CreateResource_nonexistent_api",
			action: "CreateResource",
			body:   `{"restApiId":"nonexistent","parentId":"r1","pathPart":"pets"}`,
		},
		{
			name:   "DeleteResource_nonexistent",
			action: "DeleteResource",
			body:   `{"restApiId":"nonexistent","resourceId":"r1"}`,
		},
		{
			name:   "PutMethod_nonexistent",
			action: "PutMethod",
			body:   `{"restApiId":"nonexistent","resourceId":"r1","httpMethod":"GET","authorizationType":"NONE"}`,
		},
		{
			name:   "GetMethod_nonexistent",
			action: "GetMethod",
			body:   `{"restApiId":"nonexistent","resourceId":"r1","httpMethod":"GET"}`,
		},
		{
			name:   "DeleteMethod_nonexistent",
			action: "DeleteMethod",
			body:   `{"restApiId":"nonexistent","resourceId":"r1","httpMethod":"GET"}`,
		},
		{
			name:   "PutIntegration_nonexistent",
			action: "PutIntegration",
			body:   `{"restApiId":"nonexistent","resourceId":"r1","httpMethod":"GET","type":"MOCK"}`,
		},
		{
			name:   "GetIntegration_nonexistent",
			action: "GetIntegration",
			body:   `{"restApiId":"nonexistent","resourceId":"r1","httpMethod":"GET"}`,
		},
		{
			name:   "DeleteIntegration_nonexistent",
			action: "DeleteIntegration",
			body:   `{"restApiId":"nonexistent","resourceId":"r1","httpMethod":"GET"}`,
		},
		{
			name:   "CreateDeployment_nonexistent",
			action: "CreateDeployment",
			body:   `{"restApiId":"nonexistent","stageName":"prod"}`,
		},
		{
			name:   "GetDeployments_nonexistent",
			action: "GetDeployments",
			body:   `{"restApiId":"nonexistent"}`,
		},
		{
			name:   "GetStages_nonexistent",
			action: "GetStages",
			body:   `{"restApiId":"nonexistent"}`,
		},
		{
			name:   "GetStage_nonexistent",
			action: "GetStage",
			body:   `{"restApiId":"nonexistent","stageName":"prod"}`,
		},
		{
			name:   "DeleteStage_nonexistent",
			action: "DeleteStage",
			body:   `{"restApiId":"nonexistent","stageName":"prod"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := post(t, tt.action, tt.body)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_APIGateway_FullWorkflow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		apiName     string
		apiDesc     string
		pathPart    string
		stageName   string
		wantCreated int
	}{
		{
			name:        "complete_REST_API_lifecycle",
			apiName:     "my-api",
			apiDesc:     "Test API",
			pathPart:    "pets",
			stageName:   "prod",
			wantCreated: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e := sharedSetup()

			createRec := postWithHandler(t, h, e, "CreateRestApi",
				`{"name":"`+tt.apiName+`","description":"`+tt.apiDesc+`"}`)
			require.Equal(t, tt.wantCreated, createRec.Code)

			var createResp map[string]any
			require.NoError(t, parseJSON(createRec, &createResp))
			apiID := createResp["id"].(string)
			require.NotEmpty(t, apiID)

			listRec := postWithHandler(t, h, e, "GetRestApis", `{"limit":10,"position":""}`)
			assert.Equal(t, http.StatusOK, listRec.Code)

			var resourcesResp map[string]any
			resListRec := postWithHandler(t, h, e, "GetResources",
				`{"restApiId":"`+apiID+`","limit":10}`)
			require.Equal(t, http.StatusOK, resListRec.Code)
			require.NoError(t, parseJSON(resListRec, &resourcesResp))

			items := resourcesResp["item"].([]any)
			require.Len(t, items, 1)
			rootID := items[0].(map[string]any)["id"].(string)

			childRec := postWithHandler(t, h, e, "CreateResource",
				`{"restApiId":"`+apiID+`","parentId":"`+rootID+`","pathPart":"`+tt.pathPart+`"}`)
			require.Equal(t, http.StatusCreated, childRec.Code)

			var childResp map[string]any
			require.NoError(t, parseJSON(childRec, &childResp))
			resourceID := childResp["id"].(string)

			methodRec := postWithHandler(t, h, e, "PutMethod",
				`{"restApiId":"`+apiID+`","resourceId":"`+resourceID+`","httpMethod":"GET","authorizationType":"NONE"}`)
			assert.Equal(t, http.StatusCreated, methodRec.Code)

			intRec := postWithHandler(t, h, e, "PutIntegration",
				`{"restApiId":"`+apiID+`","resourceId":"`+resourceID+`","httpMethod":"GET","type":"MOCK"}`)
			assert.Equal(t, http.StatusCreated, intRec.Code)

			deplRec := postWithHandler(t, h, e, "CreateDeployment",
				`{"restApiId":"`+apiID+`","stageName":"`+tt.stageName+`","description":"Initial"}`)
			assert.Equal(t, http.StatusCreated, deplRec.Code)

			deplListRec := postWithHandler(t, h, e, "GetDeployments",
				`{"restApiId":"`+apiID+`"}`)
			assert.Equal(t, http.StatusOK, deplListRec.Code)

			stageRec := postWithHandler(t, h, e, "GetStage",
				`{"restApiId":"`+apiID+`","stageName":"`+tt.stageName+`"}`)
			assert.Equal(t, http.StatusOK, stageRec.Code)

			delStageRec := postWithHandler(t, h, e, "DeleteStage",
				`{"restApiId":"`+apiID+`","stageName":"`+tt.stageName+`"}`)
			assert.Equal(t, http.StatusNoContent, delStageRec.Code)

			delMethodRec := postWithHandler(t, h, e, "DeleteMethod",
				`{"restApiId":"`+apiID+`","resourceId":"`+resourceID+`","httpMethod":"GET"}`)
			assert.Equal(t, http.StatusNoContent, delMethodRec.Code)

			delResRec := postWithHandler(t, h, e, "DeleteResource",
				`{"restApiId":"`+apiID+`","resourceId":"`+resourceID+`"}`)
			assert.Equal(t, http.StatusNoContent, delResRec.Code)

			delRec := postWithHandler(t, h, e, "DeleteRestApi", `{"restApiId":"`+apiID+`"}`)
			assert.Equal(t, http.StatusAccepted, delRec.Code)
		})
	}
}

// parseJSON is a helper to decode JSON from a response recorder.
func parseJSON(rec *httptest.ResponseRecorder, v any) error {
	return json.Unmarshal(rec.Body.Bytes(), v)
}
