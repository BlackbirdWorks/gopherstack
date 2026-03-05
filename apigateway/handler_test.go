package apigateway_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/apigateway"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// post sends a POST request to the APIGateway handler.
func post(t *testing.T, action, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	backend := apigateway.NewInMemoryBackend()
	handler := apigateway.NewHandler(backend)

	return postWithHandler(t, handler, e, action, body)
}

// postWithHandler sends a POST to a specific handler instance.
func postWithHandler(
	t *testing.T,
	handler *apigateway.Handler,
	e *echo.Echo,
	action, body string,
) *httptest.ResponseRecorder {
	t.Helper()

	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	} else {
		req = httptest.NewRequest(http.MethodPost, "/", nil)
	}
	if action != "" {
		req.Header.Set("X-Amz-Target", "APIGateway."+action)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err := handler.Handler()(c)
	require.NoError(t, err)

	return rec
}

// sharedSetup creates a handler and Echo instance for multi-step tests.
func sharedSetup() (*apigateway.Handler, *echo.Echo) {
	backend := apigateway.NewInMemoryBackend()
	handler := apigateway.NewHandler(backend)
	e := echo.New()

	return handler, e
}

// mockLambdaInvoker is a simple mock for Lambda invocation in tests.
type mockLambdaInvoker struct {
	returnError error
	response    []byte
	statusCode  int
}

func (m *mockLambdaInvoker) InvokeFunction(_ context.Context, _, _ string, _ []byte) ([]byte, int, error) {
	if m.returnError != nil {
		return nil, 500, m.returnError
	}

	if m.response != nil {
		return m.response, m.statusCode, nil
	}

	return []byte(`{"statusCode":200,"body":"hello"}`), 200, nil
}

func TestHandler_RestAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		primaryAPIName   string
		extraAPINames    []string
		wantCreateCode   int
		wantGetCode      int
		wantListCount    int
		doDelete         bool
		wantDeleteCode   int
		wantAfterDelCode int
	}{
		{
			name:           "create_and_get",
			primaryAPIName: "my-api",
			wantCreateCode: http.StatusCreated,
			wantGetCode:    http.StatusOK,
		},
		{
			name:           "list_multiple",
			primaryAPIName: "api-a",
			extraAPINames:  []string{"api-b"},
			wantCreateCode: http.StatusCreated,
			wantListCount:  2,
		},
		{
			name:             "delete_then_not_found",
			primaryAPIName:   "to-delete",
			wantCreateCode:   http.StatusCreated,
			doDelete:         true,
			wantDeleteCode:   http.StatusAccepted,
			wantAfterDelCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := sharedSetup()

			rec := postWithHandler(t, handler, e, "CreateRestApi", fmt.Sprintf(`{"name":%q}`, tt.primaryAPIName))
			assert.Equal(t, tt.wantCreateCode, rec.Code)

			var created map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
			assert.NotEmpty(t, created["id"])
			assert.Equal(t, tt.primaryAPIName, created["name"])

			apiID := created["id"].(string)

			for _, name := range tt.extraAPINames {
				postWithHandler(t, handler, e, "CreateRestApi", fmt.Sprintf(`{"name":%q}`, name))
			}

			if tt.wantGetCode != 0 {
				rec2 := postWithHandler(t, handler, e, "GetRestApi", fmt.Sprintf(`{"restApiId":%q}`, apiID))
				assert.Equal(t, tt.wantGetCode, rec2.Code)

				var got map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &got))
				assert.Equal(t, apiID, got["id"])
				assert.Equal(t, tt.primaryAPIName, got["name"])
			}

			if tt.wantListCount > 0 {
				rec3 := postWithHandler(t, handler, e, "GetRestApis", `{}`)
				assert.Equal(t, http.StatusOK, rec3.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &resp))
				assert.Len(t, resp["item"].([]any), tt.wantListCount)
			}

			if tt.doDelete {
				rec4 := postWithHandler(t, handler, e, "DeleteRestApi", fmt.Sprintf(`{"restApiId":%q}`, apiID))
				assert.Equal(t, tt.wantDeleteCode, rec4.Code)

				rec5 := postWithHandler(t, handler, e, "GetRestApi", fmt.Sprintf(`{"restApiId":%q}`, apiID))
				assert.Equal(t, tt.wantAfterDelCode, rec5.Code)
			}
		})
	}
}

func TestHandler_Resources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		pathPart           string
		wantChildPath      string
		wantResourceCount  int
		wantDeleteCode     int
		wantGetAfterDelete int
		doDelete           bool
	}{
		{
			name:              "root_resource_on_api_create",
			wantResourceCount: 1,
		},
		{
			name:          "create_child_resource",
			pathPart:      "users",
			wantChildPath: "/users",
		},
		{
			name:               "delete_child_resource",
			pathPart:           "items",
			doDelete:           true,
			wantDeleteCode:     http.StatusNoContent,
			wantGetAfterDelete: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := sharedSetup()

			rec := postWithHandler(t, handler, e, "CreateRestApi", `{"name":"api"}`)
			var created map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
			apiID := created["id"].(string)

			rec2 := postWithHandler(t, handler, e, "GetResources", fmt.Sprintf(`{"restApiId":%q}`, apiID))
			assert.Equal(t, http.StatusOK, rec2.Code)

			var res map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &res))
			items := res["item"].([]any)

			if tt.wantResourceCount > 0 {
				assert.Len(t, items, tt.wantResourceCount)
				root := items[0].(map[string]any)
				assert.Equal(t, "/", root["path"])
			}

			rootID := items[0].(map[string]any)["id"].(string)

			if tt.pathPart != "" {
				rec3 := postWithHandler(t, handler, e, "CreateResource",
					fmt.Sprintf(`{"restApiId":%q,"parentId":%q,"pathPart":%q}`, apiID, rootID, tt.pathPart))
				assert.Equal(t, http.StatusCreated, rec3.Code)

				var child map[string]any
				require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &child))

				if tt.wantChildPath != "" {
					assert.Equal(t, tt.wantChildPath, child["path"])
				}

				if tt.doDelete {
					childID := child["id"].(string)

					rec4 := postWithHandler(t, handler, e, "DeleteResource",
						fmt.Sprintf(`{"restApiId":%q,"resourceId":%q}`, apiID, childID))
					assert.Equal(t, tt.wantDeleteCode, rec4.Code)

					rec5 := postWithHandler(t, handler, e, "GetResource",
						fmt.Sprintf(`{"restApiId":%q,"resourceId":%q}`, apiID, childID))
					assert.Equal(t, tt.wantGetAfterDelete, rec5.Code)
				}
			}
		})
	}
}

func TestHandler_Method(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		httpMethod       string
		authType         string
		wantPutCode      int
		wantGetCode      int
		wantDeleteCode   int
		wantGetAfterCode int
	}{
		{
			name:             "put_get_delete",
			httpMethod:       "GET",
			authType:         "NONE",
			wantPutCode:      http.StatusCreated,
			wantGetCode:      http.StatusOK,
			wantDeleteCode:   http.StatusNoContent,
			wantGetAfterCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := sharedSetup()

			rec := postWithHandler(t, handler, e, "CreateRestApi", `{"name":"api"}`)
			var created map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
			apiID := created["id"].(string)

			rec2 := postWithHandler(t, handler, e, "GetResources", fmt.Sprintf(`{"restApiId":%q}`, apiID))
			var res map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &res))
			rootID := res["item"].([]any)[0].(map[string]any)["id"].(string)

			rec3 := postWithHandler(t, handler, e, "PutMethod",
				fmt.Sprintf(`{"restApiId":%q,"resourceId":%q,"httpMethod":%q,"authorizationType":%q}`,
					apiID, rootID, tt.httpMethod, tt.authType))
			assert.Equal(t, tt.wantPutCode, rec3.Code)

			var m map[string]any
			require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &m))
			assert.Equal(t, tt.httpMethod, m["httpMethod"])

			rec4 := postWithHandler(t, handler, e, "GetMethod",
				fmt.Sprintf(`{"restApiId":%q,"resourceId":%q,"httpMethod":%q}`, apiID, rootID, tt.httpMethod))
			assert.Equal(t, tt.wantGetCode, rec4.Code)

			rec5 := postWithHandler(t, handler, e, "DeleteMethod",
				fmt.Sprintf(`{"restApiId":%q,"resourceId":%q,"httpMethod":%q}`, apiID, rootID, tt.httpMethod))
			assert.Equal(t, tt.wantDeleteCode, rec5.Code)

			rec6 := postWithHandler(t, handler, e, "GetMethod",
				fmt.Sprintf(`{"restApiId":%q,"resourceId":%q,"httpMethod":%q}`, apiID, rootID, tt.httpMethod))
			assert.Equal(t, tt.wantGetAfterCode, rec6.Code)
		})
	}
}

func TestHandler_Integration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		httpMethod       string
		integrationType  string
		wantPutCode      int
		wantGetCode      int
		wantDeleteCode   int
		wantGetAfterCode int
	}{
		{
			name:             "put_get_delete",
			httpMethod:       "POST",
			integrationType:  "MOCK",
			wantPutCode:      http.StatusCreated,
			wantGetCode:      http.StatusOK,
			wantDeleteCode:   http.StatusNoContent,
			wantGetAfterCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := sharedSetup()

			rec := postWithHandler(t, handler, e, "CreateRestApi", `{"name":"api"}`)
			var created map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
			apiID := created["id"].(string)

			rec2 := postWithHandler(t, handler, e, "GetResources", fmt.Sprintf(`{"restApiId":%q}`, apiID))
			var res map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &res))
			rootID := res["item"].([]any)[0].(map[string]any)["id"].(string)

			postWithHandler(t, handler, e, "PutMethod",
				fmt.Sprintf(`{"restApiId":%q,"resourceId":%q,"httpMethod":%q,"authorizationType":"NONE"}`,
					apiID, rootID, tt.httpMethod))

			rec3 := postWithHandler(t, handler, e, "PutIntegration",
				fmt.Sprintf(`{"restApiId":%q,"resourceId":%q,"httpMethod":%q,"type":%q}`,
					apiID, rootID, tt.httpMethod, tt.integrationType))
			assert.Equal(t, tt.wantPutCode, rec3.Code)

			var integ map[string]any
			require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &integ))
			assert.Equal(t, tt.integrationType, integ["type"])

			rec4 := postWithHandler(t, handler, e, "GetIntegration",
				fmt.Sprintf(`{"restApiId":%q,"resourceId":%q,"httpMethod":%q}`, apiID, rootID, tt.httpMethod))
			assert.Equal(t, tt.wantGetCode, rec4.Code)

			rec5 := postWithHandler(t, handler, e, "DeleteIntegration",
				fmt.Sprintf(`{"restApiId":%q,"resourceId":%q,"httpMethod":%q}`, apiID, rootID, tt.httpMethod))
			assert.Equal(t, tt.wantDeleteCode, rec5.Code)

			rec6 := postWithHandler(t, handler, e, "GetIntegration",
				fmt.Sprintf(`{"restApiId":%q,"resourceId":%q,"httpMethod":%q}`, apiID, rootID, tt.httpMethod))
			assert.Equal(t, tt.wantGetAfterCode, rec6.Code)
		})
	}
}

func TestHandler_Deployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		stageName      string
		description    string
		wantCreateCode int
		wantListCount  int
	}{
		{
			name:           "create_and_list",
			stageName:      "prod",
			description:    "first",
			wantCreateCode: http.StatusCreated,
			wantListCount:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := sharedSetup()

			rec := postWithHandler(t, handler, e, "CreateRestApi", `{"name":"api"}`)
			var created map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
			apiID := created["id"].(string)

			rec2 := postWithHandler(t, handler, e, "CreateDeployment",
				fmt.Sprintf(`{"restApiId":%q,"stageName":%q,"description":%q}`,
					apiID, tt.stageName, tt.description))
			assert.Equal(t, tt.wantCreateCode, rec2.Code)

			var depl map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &depl))
			assert.NotEmpty(t, depl["id"])

			rec3 := postWithHandler(t, handler, e, "GetDeployments", fmt.Sprintf(`{"restApiId":%q}`, apiID))
			assert.Equal(t, http.StatusOK, rec3.Code)

			var depls map[string]any
			require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &depls))
			assert.Len(t, depls["item"].([]any), tt.wantListCount)
		})
	}
}

func TestHandler_Stages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		stageName          string
		wantListCode       int
		wantListCount      int
		wantGetStageCode   int
		wantDeleteCode     int
		wantGetAfterDelete int
	}{
		{
			name:               "get_and_delete",
			stageName:          "staging",
			wantListCode:       http.StatusOK,
			wantListCount:      1,
			wantGetStageCode:   http.StatusOK,
			wantDeleteCode:     http.StatusNoContent,
			wantGetAfterDelete: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := sharedSetup()

			rec := postWithHandler(t, handler, e, "CreateRestApi", `{"name":"api"}`)
			var created map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
			apiID := created["id"].(string)

			postWithHandler(t, handler, e, "CreateDeployment",
				fmt.Sprintf(`{"restApiId":%q,"stageName":%q,"description":""}`, apiID, tt.stageName))

			rec2 := postWithHandler(t, handler, e, "GetStages", fmt.Sprintf(`{"restApiId":%q}`, apiID))
			assert.Equal(t, tt.wantListCode, rec2.Code)

			var stages map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &stages))
			assert.Len(t, stages["item"].([]any), tt.wantListCount)

			rec3 := postWithHandler(t, handler, e, "GetStage",
				fmt.Sprintf(`{"restApiId":%q,"stageName":%q}`, apiID, tt.stageName))
			assert.Equal(t, tt.wantGetStageCode, rec3.Code)

			rec4 := postWithHandler(t, handler, e, "DeleteStage",
				fmt.Sprintf(`{"restApiId":%q,"stageName":%q}`, apiID, tt.stageName))
			assert.Equal(t, tt.wantDeleteCode, rec4.Code)

			rec5 := postWithHandler(t, handler, e, "GetStage",
				fmt.Sprintf(`{"restApiId":%q,"stageName":%q}`, apiID, tt.stageName))
			assert.Equal(t, tt.wantGetAfterDelete, rec5.Code)
		})
	}
}

func TestHandler_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		action    string
		body      string
		wantType  string
		wantCode  int
		hasTarget bool
	}{
		{
			name:      "unknown_operation",
			action:    "UnknownOp",
			body:      `{}`,
			hasTarget: true,
			wantCode:  http.StatusBadRequest,
			wantType:  "UnknownOperationException",
		},
		{
			name:      "missing_target",
			body:      `{}`,
			hasTarget: false,
			wantCode:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := apigateway.NewInMemoryBackend()
			handler := apigateway.NewHandler(backend)

			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			if tt.hasTarget {
				req.Header.Set("X-Amz-Target", "APIGateway."+tt.action)
			}

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			err := handler.Handler()(c)
			require.NoError(t, err)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantType != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantType, resp["__type"])
			}
		})
	}
}

func TestHandler_SetLambdaInvoker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "set_no_panic"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := apigateway.NewInMemoryBackend()
			handler := apigateway.NewHandler(backend)
			mock := &mockLambdaInvoker{}

			assert.NotPanics(t, func() {
				handler.SetLambdaInvoker(mock)
			})
		})
	}
}

func TestBuildProxyEvent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		method         string
		url            string
		body           string
		apiID          string
		stageName      string
		resource       string
		requestPath    string
		contentType    string
		wantHTTPMethod string
		wantPath       string
		wantResource   string
		wantStage      string
		wantAPIID      string
		wantQueryKey   string
		wantQueryVal   string
		wantBody       string
		wantBase64     bool
	}{
		{
			name:           "json_body",
			method:         http.MethodPost,
			url:            "/my-stage/items?key=val",
			body:           `{"data":"test"}`,
			apiID:          "abc123",
			stageName:      "my-stage",
			resource:       "/items",
			requestPath:    "/my-stage/items",
			contentType:    "application/json",
			wantHTTPMethod: http.MethodPost,
			wantPath:       "/my-stage/items",
			wantResource:   "/items",
			wantStage:      "my-stage",
			wantAPIID:      "abc123",
			wantQueryKey:   "key",
			wantQueryVal:   "val",
			wantBody:       `{"data":"test"}`,
		},
		{
			name:        "binary_body",
			method:      http.MethodPost,
			url:         "/test",
			body:        string([]byte{0xFF, 0xFE, 0x00, 0x01}),
			apiID:       "id1",
			stageName:   "stage1",
			resource:    "/",
			requestPath: "/",
			wantBase64:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.url, strings.NewReader(tt.body))
			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			}

			event, err := apigateway.BuildProxyEvent(req, tt.apiID, tt.stageName, tt.resource, tt.requestPath)
			require.NoError(t, err)

			if tt.wantHTTPMethod != "" {
				assert.Equal(t, tt.wantHTTPMethod, event.HTTPMethod)
			}
			if tt.wantPath != "" {
				assert.Equal(t, tt.wantPath, event.Path)
			}
			if tt.wantResource != "" {
				assert.Equal(t, tt.wantResource, event.Resource)
			}
			if tt.wantQueryKey != "" {
				assert.Equal(t, tt.wantQueryVal, event.QueryStringParameters[tt.wantQueryKey])
			}
			if tt.wantBody != "" {
				assert.JSONEq(t, tt.wantBody, event.Body)
			}
			if tt.wantStage != "" {
				assert.Equal(t, tt.wantStage, event.RequestContext.Stage)
			}
			if tt.wantAPIID != "" {
				assert.Equal(t, tt.wantAPIID, event.RequestContext.APIId)
			}
			if tt.wantBase64 {
				assert.True(t, event.IsBase64Encoded)
			}
		})
	}
}
