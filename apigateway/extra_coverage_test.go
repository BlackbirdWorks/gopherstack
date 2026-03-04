package apigateway_test

import (
	"encoding/json"
	"errors"
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
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// errNoopNotImplemented is returned by noopBackend for methods that are not expected
// to be called in the fallback-persistence tests.
var errNoopNotImplemented = errors.New("not implemented")

// noopBackend implements StorageBackend without Snapshot/Restore so we can test
// the persistence fallback branches in Handler.Snapshot and Handler.Restore.
type noopBackend struct{}

func (n *noopBackend) CreateRestAPI(_ string, _ string, _ *tags.Tags) (*apigateway.RestAPI, error) {
	return &apigateway.RestAPI{ID: "x", Name: "x"}, nil
}

func (n *noopBackend) DeleteRestAPI(_ string) error { return nil }

func (n *noopBackend) GetRestAPI(_ string) (*apigateway.RestAPI, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetRestAPIs(_ int, _ string) ([]apigateway.RestAPI, string, error) {
	return nil, "", nil
}

func (n *noopBackend) GetResources(_ string, _ string, _ int) ([]apigateway.Resource, string, error) {
	return nil, "", nil
}

func (n *noopBackend) GetResource(_ string, _ string) (*apigateway.Resource, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) CreateResource(_ string, _ string, _ string) (*apigateway.Resource, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) DeleteResource(_ string, _ string) error { return nil }

func (n *noopBackend) PutMethod(
	_ string, _ string, _ string, _ string, _ bool,
) (*apigateway.Method, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetMethod(_ string, _ string, _ string) (*apigateway.Method, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) DeleteMethod(_ string, _ string, _ string) error { return nil }

func (n *noopBackend) PutIntegration(
	_ string, _ string, _ string, _ apigateway.PutIntegrationInput,
) (*apigateway.Integration, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetIntegration(_ string, _ string, _ string) (*apigateway.Integration, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) DeleteIntegration(_ string, _ string, _ string) error { return nil }

func (n *noopBackend) CreateDeployment(_ string, _ string, _ string) (*apigateway.Deployment, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetDeployment(_ string, _ string) (*apigateway.Deployment, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetDeployments(_ string) ([]apigateway.Deployment, error) {
	return nil, nil
}

func (n *noopBackend) DeleteDeployment(_ string, _ string) error { return nil }

func (n *noopBackend) GetStages(_ string) ([]apigateway.Stage, error) { return nil, nil }

func (n *noopBackend) GetStage(_ string, _ string) (*apigateway.Stage, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) DeleteStage(_ string, _ string) error { return nil }

// restRequest sends a REST-style request (no X-Amz-Target header) to the handler.
func restRequest(t *testing.T, handler *apigateway.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err := handler.Handler()(c)
	require.NoError(t, err)

	return rec
}

// TestHandlerPersistence_NoopBackend covers the fallback branches in Handler.Snapshot
// and Handler.Restore when the backend does not implement those interfaces.
func TestHandlerPersistence_NoopBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantNilSnap bool
		wantNoErr   bool
	}{
		{
			name:        "Snapshot_returns_nil_for_non_snapshotter",
			wantNilSnap: true,
		},
		{
			name:      "Restore_returns_nil_for_non_restorer",
			wantNoErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := apigateway.NewHandler(&noopBackend{}, slog.Default())

			if tt.wantNilSnap {
				snap := h.Snapshot()
				assert.Nil(t, snap)

				return
			}

			err := h.Restore([]byte(`{"apis":{}}`))
			require.NoError(t, err)
		})
	}
}

// TestInMemoryBackend_RestoreWithNilMaps ensures that nil maps in the JSON snapshot
// are initialised to empty maps after Restore (covers the nil-map branches in Restore).
func TestInMemoryBackend_RestoreWithNilMaps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		snapshot string
	}{
		{
			name: "null_resources_deployments_stages",
			snapshot: `{"apis":{"api1":{"api":{"id":"api1","name":"n","createdDate":0},` +
				`"resources":null,"deployments":null,"stages":null}}}`,
		},
		{
			name:     "missing_inner_maps",
			snapshot: `{"apis":{"api2":{"api":{"id":"api2","name":"m","createdDate":0}}}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			err := b.Restore([]byte(tt.snapshot))
			require.NoError(t, err)

			// The Restore should have initialised the empty maps – calling GetResources
			// should succeed without a nil-pointer panic.
			apiID := "api1"
			if tt.name == "missing_inner_maps" {
				apiID = "api2"
			}

			resources, _, err := b.GetResources(apiID, "", 0)
			require.NoError(t, err)
			assert.Empty(t, resources)
		})
	}
}

// TestHandleRESTAPI_Branches covers the branches inside handleRESTAPI that are not
// hit by the existing REST-path test: unknown path → 404, dispatch error → 4xx,
// and successful DELETE that returns 204.
func TestHandleRESTAPI_Branches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *apigateway.InMemoryBackend) string
		name     string
		method   string
		path     string
		body     string
		wantCode int
	}{
		{
			name:     "unknown_rest_path_returns_404",
			method:   http.MethodGet,
			path:     "/restapis/abc/unknownsegment",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "dispatch_error_nonexistent_api",
			method:   http.MethodGet,
			path:     "/restapis/nonexistent",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "delete_resource_returns_204",
			method:   http.MethodDelete,
			wantCode: http.StatusNoContent,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI("api", "", nil)
				resources, _, _ := b.GetResources(api.ID, "", 0)
				rootID := resources[0].ID
				child, _ := b.CreateResource(api.ID, rootID, "items")

				return fmt.Sprintf("/restapis/%s/resources/%s", api.ID, child.ID)
			},
		},
		{
			name:     "delete_stage_returns_204",
			method:   http.MethodDelete,
			wantCode: http.StatusNoContent,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI("api", "", nil)
				_, _ = b.CreateDeployment(api.ID, "prod", "")

				return fmt.Sprintf("/restapis/%s/stages/prod", api.ID)
			},
		},
		{
			name:     "delete_method_returns_204",
			method:   http.MethodDelete,
			wantCode: http.StatusNoContent,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI("api", "", nil)
				resources, _, _ := b.GetResources(api.ID, "", 0)
				rootID := resources[0].ID
				_, _ = b.PutMethod(api.ID, rootID, "GET", "NONE", false)

				return fmt.Sprintf("/restapis/%s/resources/%s/methods/GET", api.ID, rootID)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(backend, slog.Default())

			path := tt.path
			if tt.setup != nil {
				path = tt.setup(backend)
			}

			rec := restRequest(t, h, tt.method, path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestBackend_DeleteResource_NotFound ensures the "resource not found" error branch
// in DeleteResource is covered (API exists but resource ID is absent).
func TestBackend_DeleteResource_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "resource_not_found"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			api, err := b.CreateRestAPI("api", "", nil)
			require.NoError(t, err)

			err = b.DeleteResource(api.ID, "nonexistent-resource")
			require.Error(t, err)
		})
	}
}

// TestBackend_DeleteMethod_NotFound covers the "resource not found" and
// "method not found" error branches in DeleteMethod.
func TestBackend_DeleteMethod_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		resourceID string
		httpMethod string
		wantErr    bool
	}{
		{
			name:       "resource_not_found",
			resourceID: "nonexistent",
			httpMethod: "GET",
			wantErr:    true,
		},
		{
			name:       "method_not_found",
			resourceID: "", // filled in by setup
			httpMethod: "DELETE",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			api, err := b.CreateRestAPI("api", "", nil)
			require.NoError(t, err)

			resources, _, err := b.GetResources(api.ID, "", 0)
			require.NoError(t, err)
			rootID := resources[0].ID

			resID := tt.resourceID
			if resID == "" {
				resID = rootID
			}

			err = b.DeleteMethod(api.ID, resID, tt.httpMethod)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestBackend_PutIntegration_NotFound covers the "resource not found" and
// "method not found" error branches in PutIntegration.
func TestBackend_PutIntegration_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		resourceID string
		httpMethod string
	}{
		{
			name:       "resource_not_found",
			resourceID: "nonexistent",
			httpMethod: "GET",
		},
		{
			name:       "method_not_found",
			resourceID: "", // uses root ID (method not set)
			httpMethod: "PATCH",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			api, err := b.CreateRestAPI("api", "", nil)
			require.NoError(t, err)

			resources, _, err := b.GetResources(api.ID, "", 0)
			require.NoError(t, err)
			rootID := resources[0].ID

			resID := tt.resourceID
			if resID == "" {
				resID = rootID
			}

			_, err = b.PutIntegration(api.ID, resID, tt.httpMethod, apigateway.PutIntegrationInput{Type: "MOCK"})
			require.Error(t, err)
		})
	}
}

// TestBackend_DeleteStage_NotFound covers the "stage not found" error branch in DeleteStage.
func TestBackend_DeleteStage_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "stage_not_found"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			api, err := b.CreateRestAPI("api", "", nil)
			require.NoError(t, err)

			err = b.DeleteStage(api.ID, "nonexistent-stage")
			require.Error(t, err)
		})
	}
}

// TestComputePath_NonRootParent covers the computePath branch where parentPath != "/".
// This is exercised indirectly by creating a nested (grandchild) resource.
func TestComputePath_NonRootParent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		parts     []string
		wantPaths []string
	}{
		{
			name:      "two_level_nesting",
			parts:     []string{"users", "profile"},
			wantPaths: []string{"/users", "/users/profile"},
		},
		{
			name:      "three_level_nesting",
			parts:     []string{"v1", "pets", "{petId}"},
			wantPaths: []string{"/v1", "/v1/pets", "/v1/pets/{petId}"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			api, err := b.CreateRestAPI("api", "", nil)
			require.NoError(t, err)

			resources, _, err := b.GetResources(api.ID, "", 0)
			require.NoError(t, err)
			parentID := resources[0].ID

			for i, part := range tt.parts {
				child, cerr := b.CreateResource(api.ID, parentID, part)
				require.NoError(t, cerr)
				assert.Equal(t, tt.wantPaths[i], child.Path)
				parentID = child.ID
			}
		})
	}
}

// TestParsePosition_EdgeCases covers the invalid-string and negative-value branches
// of parsePosition by passing those values as the position parameter to GetRestAPIs and GetResources.
func TestParsePosition_EdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		position string
		wantLen  int
	}{
		{
			name:     "invalid_position_string_treated_as_zero",
			position: "not-a-number",
			wantLen:  2,
		},
		{
			name:     "negative_position_treated_as_zero",
			position: "-99",
			wantLen:  2,
		},
		{
			name:     "valid_position_paginates",
			position: "1",
			wantLen:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			_, _ = b.CreateRestAPI("api-a", "", nil)
			_, _ = b.CreateRestAPI("api-b", "", nil)

			apis, _, err := b.GetRestAPIs(0, tt.position)
			require.NoError(t, err)
			assert.Len(t, apis, tt.wantLen)
		})
	}
}

// TestExtractResource_AdditionalBranches covers the "name" key fallback and the
// non-string-value branch in ExtractResource.
func TestExtractResource_AdditionalBranches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantResource string
	}{
		{
			name:         "name_key_fallback",
			body:         `{"name":"my-api"}`,
			wantResource: "my-api",
		},
		{
			name:         "non_string_restApiId_falls_through_to_name",
			body:         `{"restApiId":42,"name":"fallback-api"}`,
			wantResource: "fallback-api",
		},
		{
			name:         "invalid_json_returns_empty",
			body:         `not-json`,
			wantResource: "",
		},
		{
			name:         "no_matching_keys",
			body:         `{"other":"value"}`,
			wantResource: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := apigateway.NewHandler(apigateway.NewInMemoryBackend(), slog.Default())
			e := echo.New()

			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			got := h.ExtractResource(e.NewContext(req, httptest.NewRecorder()))

			assert.Equal(t, tt.wantResource, got)
		})
	}
}

// TestRestAPIActions_RESTPathCoverage exercises the restAPIActions closures via REST
// path requests (GET/DELETE /restapis/...) to cover branches not reached by the
// X-Amz-Target path.
func TestRestAPIActions_RESTPathCoverage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *apigateway.InMemoryBackend) string
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "GET_restapis_returns_200",
			method:   http.MethodGet,
			path:     "/restapis",
			wantCode: http.StatusOK,
		},
		{
			name:   "GET_restapis_by_id_returns_200",
			method: http.MethodGet,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI("test-api", "", nil)

				return "/restapis/" + api.ID
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DELETE_restapi_returns_202",
			method: http.MethodDelete,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI("del-api", "", nil)

				return "/restapis/" + api.ID
			},
			wantCode: http.StatusAccepted,
		},
		{
			name:     "POST_restapis_returns_201",
			method:   http.MethodPost,
			path:     "/restapis",
			wantCode: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(backend, slog.Default())

			path := tt.path
			if tt.setup != nil {
				path = tt.setup(backend)
			}

			body := ""
			if tt.method == http.MethodPost {
				body = `{"name":"rest-created-api"}`
			}

			rec := restRequest(t, h, tt.method, path, body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestResourceActions_RESTPathCoverage exercises resourceActions closures via REST paths.
func TestResourceActions_RESTPathCoverage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *apigateway.InMemoryBackend) (apiID, path string)
		name     string
		method   string
		body     string
		wantCode int
	}{
		{
			name:   "GET_resources_returns_200",
			method: http.MethodGet,
			setup: func(b *apigateway.InMemoryBackend) (string, string) {
				api, _ := b.CreateRestAPI("api", "", nil)

				return api.ID, "/restapis/" + api.ID + "/resources"
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "GET_resource_by_id_returns_200",
			method: http.MethodGet,
			setup: func(b *apigateway.InMemoryBackend) (string, string) {
				api, _ := b.CreateRestAPI("api", "", nil)
				resources, _, _ := b.GetResources(api.ID, "", 0)

				return api.ID, fmt.Sprintf("/restapis/%s/resources/%s", api.ID, resources[0].ID)
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "POST_resource_creates_child",
			method: http.MethodPost,
			setup: func(b *apigateway.InMemoryBackend) (string, string) {
				api, _ := b.CreateRestAPI("api", "", nil)
				resources, _, _ := b.GetResources(api.ID, "", 0)

				return api.ID, fmt.Sprintf("/restapis/%s/resources/%s", api.ID, resources[0].ID)
			},
			body:     `{"pathPart":"widgets"}`,
			wantCode: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(backend, slog.Default())

			_, path := tt.setup(backend)

			rec := restRequest(t, h, tt.method, path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestMethodActions_RESTPathCoverage exercises methodActions closures via REST paths.
func TestMethodActions_RESTPathCoverage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *apigateway.InMemoryBackend) string
		name     string
		method   string
		body     string
		wantCode int
	}{
		{
			name:   "PUT_method_via_REST",
			method: http.MethodPut,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI("api", "", nil)
				resources, _, _ := b.GetResources(api.ID, "", 0)

				return fmt.Sprintf("/restapis/%s/resources/%s/methods/GET", api.ID, resources[0].ID)
			},
			body:     `{"authorizationType":"NONE"}`,
			wantCode: http.StatusCreated,
		},
		{
			name:   "GET_method_via_REST",
			method: http.MethodGet,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI("api", "", nil)
				resources, _, _ := b.GetResources(api.ID, "", 0)
				_, _ = b.PutMethod(api.ID, resources[0].ID, "POST", "NONE", false)

				return fmt.Sprintf("/restapis/%s/resources/%s/methods/POST", api.ID, resources[0].ID)
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(backend, slog.Default())

			path := tt.setup(backend)
			rec := restRequest(t, h, tt.method, path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestGetRestAPIs_Pagination exercises GetRestAPIs with a limit that triggers
// the pagination position output.
func TestGetRestAPIs_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		limit        int
		wantLen      int
		wantPosition bool
	}{
		{
			name:         "limit_1_returns_position",
			limit:        1,
			wantLen:      1,
			wantPosition: true,
		},
		{
			name:    "limit_0_returns_all",
			limit:   0,
			wantLen: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			for i := range 3 {
				_, err := b.CreateRestAPI(fmt.Sprintf("api-%d", i), "", nil)
				require.NoError(t, err)
			}

			apis, pos, err := b.GetRestAPIs(tt.limit, "")
			require.NoError(t, err)
			assert.Len(t, apis, tt.wantLen)

			if tt.wantPosition {
				assert.NotEmpty(t, pos)
			} else {
				assert.Empty(t, pos)
			}
		})
	}
}

// TestGetRestAPIs_RESTPath_WithLimit exercises GetRestApis via REST path with
// limit and position query parameters (covers the restAPIActions GetRestApis closure
// when limit/position are passed via body from REST path merging).
func TestGetRestAPIs_RESTPath_WithLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "get_rest_apis_via_REST_path",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := apigateway.NewInMemoryBackend()
			_, _ = backend.CreateRestAPI("api-x", "", nil)

			h := apigateway.NewHandler(backend, slog.Default())
			rec := restRequest(t, h, http.MethodGet, "/restapis", "")

			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp["item"])
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

			rec := postWithHandler(t, h, e, tt.action,
				fmt.Sprintf(`{"restApiId":%q,"deploymentId":%q}`, apiID, deplID))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestParseAPIGWMethodPath_EdgeCases covers the branches in parseAPIGWMethodPath
// that are unreachable via normal REST calls:
//   - path ending at "methods" with no httpMethod segment → returns false
//   - integration segment with an unsupported HTTP method → returns false
func TestParseAPIGWMethodPath_EdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "methods_segment_without_httpMethod_returns_404",
			method:   http.MethodGet,
			path:     "/restapis/abc123/resources/resxyz/methods",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "integration_with_POST_method_returns_404",
			method:   http.MethodPost,
			path:     "/restapis/abc123/resources/resxyz/methods/GET/integration",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := apigateway.NewHandler(apigateway.NewInMemoryBackend(), slog.Default())
			rec := restRequest(t, h, tt.method, tt.path, "")
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
				api, _ := b.CreateRestAPI("api", "", nil)

				return fmt.Sprintf("/restapis/%s/deployments", api.ID)
			},
			body:     `{"stageName":"v1","description":""}`,
			wantCode: http.StatusCreated,
		},
		{
			name:   "GET_deployments_lists",
			method: http.MethodGet,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI("api", "", nil)
				_, _ = b.CreateDeployment(api.ID, "prod", "")

				return fmt.Sprintf("/restapis/%s/deployments", api.ID)
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "GET_deployment_by_id",
			method: http.MethodGet,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI("api", "", nil)
				dep, _ := b.CreateDeployment(api.ID, "prod", "")

				return fmt.Sprintf("/restapis/%s/deployments/%s", api.ID, dep.ID)
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DELETE_deployment_returns_204",
			method: http.MethodDelete,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI("api", "", nil)
				dep, _ := b.CreateDeployment(api.ID, "prod", "")

				return fmt.Sprintf("/restapis/%s/deployments/%s", api.ID, dep.ID)
			},
			wantCode: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(backend, slog.Default())

			path := tt.setup(backend)
			rec := restRequest(t, h, tt.method, path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestHandler_RESTPath_Stages exercises the GET stages REST-path branches in
// parseAPIGWRESTPath that are not covered by existing tests.
func TestHandler_RESTPath_Stages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *apigateway.InMemoryBackend) string
		name     string
		method   string
		wantCode int
	}{
		{
			name:   "GET_stages_lists",
			method: http.MethodGet,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI("api", "", nil)
				_, _ = b.CreateDeployment(api.ID, "staging", "")

				return fmt.Sprintf("/restapis/%s/stages", api.ID)
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "GET_stage_by_name",
			method: http.MethodGet,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI("api", "", nil)
				_, _ = b.CreateDeployment(api.ID, "prod", "")

				return fmt.Sprintf("/restapis/%s/stages/prod", api.ID)
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(backend, slog.Default())

			path := tt.setup(backend)
			rec := restRequest(t, h, tt.method, path, "")
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestHandler_RESTPath_Integration exercises the PUT/GET/DELETE integration REST-path
// branches in parseAPIGWMethodPath.
func TestHandler_RESTPath_Integration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		method   string
		body     string
		wantCode int
	}{
		{
			name:     "PUT_integration_via_REST",
			method:   http.MethodPut,
			body:     `{"type":"MOCK"}`,
			wantCode: http.StatusCreated,
		},
		{
			name:     "GET_integration_via_REST",
			method:   http.MethodGet,
			wantCode: http.StatusOK,
		},
		{
			name:     "DELETE_integration_via_REST",
			method:   http.MethodDelete,
			wantCode: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := apigateway.NewInMemoryBackend()
			api, err := backend.CreateRestAPI("api", "", nil)
			require.NoError(t, err)

			resources, _, err := backend.GetResources(api.ID, "", 0)
			require.NoError(t, err)
			rootID := resources[0].ID

			_, err = backend.PutMethod(api.ID, rootID, "GET", "NONE", false)
			require.NoError(t, err)

			// Ensure integration exists for GET and DELETE operations.
			if tt.method != http.MethodPut {
				_, err = backend.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{Type: "MOCK"})
				require.NoError(t, err)
			}

			h := apigateway.NewHandler(backend, slog.Default())
			path := fmt.Sprintf("/restapis/%s/resources/%s/methods/GET/integration", api.ID, rootID)

			rec := restRequest(t, h, tt.method, path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestHandler_GetSupportedOperations covers the `GET /` handler branch that returns
// the list of supported operations.
func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "GET_root_returns_operations",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h := apigateway.NewHandler(apigateway.NewInMemoryBackend(), slog.Default())

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			rec := httptest.NewRecorder()
			err := h.Handler()(e.NewContext(req, rec))
			require.NoError(t, err)

			assert.Equal(t, tt.wantCode, rec.Code)

			var ops []string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ops))
			assert.Contains(t, ops, "CreateRestApi")
		})
	}
}

// TestHandler_InvalidTarget covers the branch that rejects an X-Amz-Target header
// that does not contain exactly one dot (e.g. "NoDotsHere").
func TestHandler_InvalidTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		target   string
		wantCode int
	}{
		{
			name:     "target_without_dot_returns_400",
			target:   "NoDotInTarget",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h := apigateway.NewHandler(apigateway.NewInMemoryBackend(), slog.Default())

			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			err := h.Handler()(e.NewContext(req, rec))
			require.NoError(t, err)

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestVTL_DefaultJSONType covers the default branch in jsonValueToString (objects/arrays).
func TestVTL_DefaultJSONType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		tmpl         string
		ctx          apigateway.VTLContext
		wantContains string
	}{
		{
			name:         "input_path_array_returns_json_encoded",
			tmpl:         `$input.path('$.items')`,
			ctx:          apigateway.VTLContext{Body: `{"items":[1,2,3]}`},
			wantContains: "1",
		},
		{
			name:         "input_path_object_returns_json_encoded",
			tmpl:         `$input.path('$.obj')`,
			ctx:          apigateway.VTLContext{Body: `{"obj":{"key":"val"}}`},
			wantContains: "key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out := apigateway.RenderTemplate(tt.tmpl, tt.ctx)
			assert.Contains(t, out, tt.wantContains)
		})
	}
}

// TestGetResources_SortWithMultipleItems ensures the sort closure in GetResources is
// exercised by requesting all resources when at least two exist.
func TestGetResources_SortWithMultipleItems(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		childParts  []string
		wantAtLeast int
	}{
		{
			name:        "two_resources_triggers_sort",
			childParts:  []string{"orders"},
			wantAtLeast: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			api, err := b.CreateRestAPI("api", "", nil)
			require.NoError(t, err)

			resources, _, err := b.GetResources(api.ID, "", 0)
			require.NoError(t, err)
			rootID := resources[0].ID

			for _, part := range tt.childParts {
				_, err = b.CreateResource(api.ID, rootID, part)
				require.NoError(t, err)
			}

			all, _, err := b.GetResources(api.ID, "", 0)
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(all), tt.wantAtLeast)
		})
	}
}

// TestGetStages_SortWithMultipleItems ensures the sort closure in GetStages is exercised
// by creating two deployments with different stage names and then listing stages.
func TestGetStages_SortWithMultipleItems(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		stageNames []string
	}{
		{
			name:       "two_stages_triggers_sort",
			stageNames: []string{"prod", "staging"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			api, err := b.CreateRestAPI("api", "", nil)
			require.NoError(t, err)

			for _, s := range tt.stageNames {
				_, err = b.CreateDeployment(api.ID, s, "")
				require.NoError(t, err)
			}

			stages, err := b.GetStages(api.ID)
			require.NoError(t, err)
			assert.Len(t, stages, len(tt.stageNames))
		})
	}
}

// TestVTL_AdditionalBranches covers the false-bool, fractional-float, and
// remaining escapeJavaScript character branches in vtl.go.
func TestVTL_AdditionalBranches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tmpl      string
		ctx       apigateway.VTLContext
		wantEqual string
	}{
		{
			name:      "input_path_bool_false",
			tmpl:      `$input.path('$.active')`,
			ctx:       apigateway.VTLContext{Body: `{"active":false}`},
			wantEqual: "false",
		},
		{
			name:      "input_path_float_with_fractional",
			tmpl:      `$input.path('$.ratio')`,
			ctx:       apigateway.VTLContext{Body: `{"ratio":3.14}`},
			wantEqual: "3.14",
		},
		{
			name:      "escape_javascript_single_quote",
			tmpl:      `$util.escapeJavaScript("it's here")`,
			ctx:       apigateway.VTLContext{},
			wantEqual: `it\'s here`,
		},
		{
			name:      "escape_javascript_tab",
			tmpl:      "$util.escapeJavaScript('col1\tcol2')",
			ctx:       apigateway.VTLContext{},
			wantEqual: `col1\tcol2`,
		},
		{
			name:      "escape_javascript_carriage_return",
			tmpl:      "$util.escapeJavaScript('line\r\n')",
			ctx:       apigateway.VTLContext{},
			wantEqual: `line\r\n`,
		},
		{
			name:      "escape_javascript_backslash",
			tmpl:      `$util.escapeJavaScript('path\file')`,
			ctx:       apigateway.VTLContext{},
			wantEqual: `path\\file`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out := apigateway.RenderTemplate(tt.tmpl, tt.ctx)
			assert.Equal(t, tt.wantEqual, out)
		})
	}
}
