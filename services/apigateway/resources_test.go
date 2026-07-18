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

// TestGetResources_NoPositionOnLastPage verifies that GetResources
// omits the position field from the response when all resources fit in one page,
// matching AWS behaviour. Previously position was always serialised as "".
func TestGetResources_NoPositionOnLastPage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		addChild     bool
		wantPosition bool
	}{
		{
			name:         "root_only_omits_position",
			addChild:     false,
			wantPosition: false,
		},
		{
			name:         "root_and_child_omits_position",
			addChild:     true,
			wantPosition: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(b)

			apiRec := restRequest(t, h, http.MethodPost, "/restapis", `{"name":"res-pag-api"}`)
			require.True(t, apiRec.Code >= 200 && apiRec.Code < 300)

			var apiResp map[string]any
			require.NoError(t, json.NewDecoder(apiRec.Body).Decode(&apiResp))
			apiID := apiResp["id"].(string)
			require.NotEmpty(t, apiID)

			if tt.addChild {
				resRec := restRequest(t, h, http.MethodGet, "/restapis/"+apiID+"/resources", "")
				require.True(t, resRec.Code >= 200 && resRec.Code < 300)

				var resResp map[string]any
				require.NoError(t, json.NewDecoder(resRec.Body).Decode(&resResp))
				items := resResp["item"].([]any)
				rootID := items[0].(map[string]any)["id"].(string)

				childRec := restRequest(t, h, http.MethodPost,
					"/restapis/"+apiID+"/resources/"+rootID,
					`{"pathPart":"items"}`)
				require.True(t, childRec.Code >= 200 && childRec.Code < 300)
			}

			rec := restRequest(t, h, http.MethodGet, "/restapis/"+apiID+"/resources", "")
			require.True(t, rec.Code >= 200 && rec.Code < 300)

			var resp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

			_, hasPosition := resp["position"]
			assert.False(t, hasPosition,
				"position must be absent when all resources fit on one page")
		})
	}
}

// TestGetResources_PositionPresentWhenPaginating verifies that
// GetResources includes position when there are more pages (backend-level test).
func TestGetResources_PositionPresentWhenPaginating(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "pag-api"})
	require.NoError(t, err)

	// Get root resource.
	resources, _, err := b.GetResources(api.ID, "", 0)
	require.NoError(t, err)
	rootID := resources[0].ID

	// Add two children so there are 3 resources total.
	for _, part := range []string{"items", "users"} {
		_, err = b.CreateResource(api.ID, rootID, part)
		require.NoError(t, err)
	}

	// Fetch limit=1 → should get non-empty position.
	_, position, err := b.GetResources(api.ID, "", 1)
	require.NoError(t, err)
	assert.NotEmpty(t, position, "position must be non-empty when more resources remain")
}

func TestBackend_GetResources_DefaultPageSizeIs25(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "a"})
	require.NoError(t, err)
	resources, _, err := b.GetResources(api.ID, "", 0)
	require.NoError(t, err)
	rootID := resources[0].ID

	// Root + 30 children = 31 resources.
	for i := range 30 {
		_, cerr := b.CreateResource(api.ID, rootID, "p"+string(rune('a'+i%26))+string(rune('0'+i/26)))
		require.NoError(t, cerr)
	}

	page, token, err := b.GetResources(api.ID, "", 0)
	require.NoError(t, err)
	assert.Len(t, page, 25, "default page size must be AWS's 25, not 500")
	assert.NotEmpty(t, token, "a next-page cursor must be returned when more resources remain")
}

// TestUpdateResource tests UpdateResource.
func TestUpdateResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		newPathPart string
		wantCode    int
		useValid    bool
	}{
		{
			name:        "update_path_part",
			newPathPart: "items",
			wantCode:    http.StatusOK,
			useValid:    true,
		},
		{
			name:     "resource_not_found",
			wantCode: http.StatusNotFound,
			useValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			apiID := boostAPI(t, handler, e)
			rootID := boostRootResource(t, handler, e, apiID)

			// Create a child resource to update
			createRec := postWithHandler(t, handler, e, "CreateResource",
				fmt.Sprintf(`{"restApiId":%q,"parentId":%q,"pathPart":"original"}`, apiID, rootID))
			require.Equal(t, http.StatusCreated, createRec.Code)
			var childResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &childResp))
			childID := childResp["id"].(string)

			lookupID := childID
			if !tt.useValid {
				lookupID = "notexist"
			}

			rec := postWithHandler(t, handler, e, "UpdateResource",
				fmt.Sprintf(`{"restApiId":%q,"resourceId":%q,"pathPart":%q}`,
					apiID, lookupID, tt.newPathPart))
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
			api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
			require.NoError(t, err)

			err = b.DeleteResource(api.ID, "nonexistent-resource")
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
			api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
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
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})

				return api.ID, "/restapis/" + api.ID + "/resources"
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "GET_resource_by_id_returns_200",
			method: http.MethodGet,
			setup: func(b *apigateway.InMemoryBackend) (string, string) {
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
				resources, _, _ := b.GetResources(api.ID, "", 0)

				return api.ID, fmt.Sprintf("/restapis/%s/resources/%s", api.ID, resources[0].ID)
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "POST_resource_creates_child",
			method: http.MethodPost,
			setup: func(b *apigateway.InMemoryBackend) (string, string) {
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
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
			h := apigateway.NewHandler(backend)

			_, path := tt.setup(backend)

			rec := restRequest(t, h, tt.method, path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
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
			api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
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

func TestBackend_Resource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "root_resource_created_on_api_creation",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})

				resources, _, err := b.GetResources(api.ID, "", 0)
				require.NoError(t, err)
				assert.Len(t, resources, 1)
				assert.Equal(t, "/", resources[0].Path)
				assert.Empty(t, resources[0].PathPart)
			},
		},
		{
			name: "create_and_get_child",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})

				resources, _, _ := b.GetResources(api.ID, "", 0)
				rootID := resources[0].ID

				child, err := b.CreateResource(api.ID, rootID, "users")
				require.NoError(t, err)
				assert.Equal(t, "/users", child.Path)
				assert.Equal(t, rootID, child.ParentID)

				got, err := b.GetResource(api.ID, child.ID)
				require.NoError(t, err)
				assert.Equal(t, child.ID, got.ID)
			},
		},
		{
			name: "delete_existing",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
				resources, _, _ := b.GetResources(api.ID, "", 0)
				rootID := resources[0].ID

				child, _ := b.CreateResource(api.ID, rootID, "items")

				err := b.DeleteResource(api.ID, child.ID)
				require.NoError(t, err)

				_, err = b.GetResource(api.ID, child.ID)
				require.Error(t, err)
			},
		},
		{
			name: "create_with_empty_path_part_returns_error",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
				resources, _, _ := b.GetResources(api.ID, "", 0)
				rootID := resources[0].ID

				_, err := b.CreateResource(api.ID, rootID, "")
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
