package appsync_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestHandler_Types_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

	// PUT on resolver should return method not allowed.
	rec := doRequest(t, h, http.MethodPut, "/v1/apis/"+api.APIID+"/types/Query/resolvers", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_ListTypesByAssociation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		createAssoc bool
		wantStatus  int
	}{
		{
			name:        "success",
			createAssoc: true,
			wantStatus:  http.StatusOK,
		},
		{
			name:        "not_found",
			createAssoc: false,
			wantStatus:  http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			mergedAPIID := "nomatch"
			assocID := "noassoc"

			if tt.createAssoc {
				merged, err := b.CreateGraphqlAPI(
					"MergedAPI", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil,
				)
				require.NoError(t, err)
				source, err := b.CreateGraphqlAPI("SourceAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
				require.NoError(t, err)

				assoc, err := b.AssociateSourceGraphqlAPI(merged.APIID, source.APIID, "desc", "")
				require.NoError(t, err)
				mergedAPIID = merged.APIID
				assocID = assoc.AssociationID
			}

			path := fmt.Sprintf("/v1/mergedApis/%s/sourceApiAssociations/%s/types", mergedAPIID, assocID)
			rec := doRequest(t, h, http.MethodGet, path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_Types_ShortPath(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

	// GET /v1/apis/{id}/types now returns list of types (200 OK).
	rec := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/types", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_CreateType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*appsync.InMemoryBackend) string
		body       map[string]any
		name       string
		wantName   string
		wantStatus int
	}{
		{
			name: "creates_type_successfully",
			setup: func(b *appsync.InMemoryBackend) string {
				api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

				return api.APIID
			},
			body: map[string]any{
				"definition": "type Post { id: ID! title: String }",
				"format":     "SDL",
			},
			wantStatus: http.StatusCreated,
			wantName:   "Post",
		},
		{
			name: "missing_definition_returns_400",
			setup: func(b *appsync.InMemoryBackend) string {
				api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

				return api.APIID
			},
			body:       map[string]any{"format": "SDL"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "returns_404_for_missing_api",
			setup: func(_ *appsync.InMemoryBackend) string {
				return "nonexistent"
			},
			body:       map[string]any{"definition": "type Foo { id: ID! }", "format": "SDL"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			apiID := tt.setup(b)

			rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+apiID+"/types", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantName != "" {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				typ, ok := resp["type"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantName, typ["name"])
			}
		})
	}
}

func TestHandler_CreateType_DuplicateRejected(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	// Create an API.
	body := map[string]any{"name": "TestAPI", "authenticationType": "API_KEY"}
	rec := doRequest(t, h, http.MethodPost, "/v1/apis", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	gqlAPI := resp["graphqlApi"].(map[string]any)
	apiID := gqlAPI["apiId"].(string)

	typeBody := map[string]any{"definition": "type MyType { id: ID! }", "format": "SDL"}

	// First creation succeeds.
	rec2 := doRequest(t, h, http.MethodPost, "/v1/apis/"+apiID+"/types", typeBody)
	assert.Equal(t, http.StatusCreated, rec2.Code)

	// Second creation with same type name fails.
	rec3 := doRequest(t, h, http.MethodPost, "/v1/apis/"+apiID+"/types", typeBody)
	assert.Equal(t, http.StatusBadRequest, rec3.Code)
}

func TestHandler_ListTypes(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateType(api.APIID, "type MyType { id: ID! }", appsync.TypeFormatSDL)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/types", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	types := resp["types"].([]any)
	assert.Len(t, types, 1)
}

func TestHandler_GetType(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateType(api.APIID, "type MyType { id: ID! }", appsync.TypeFormatSDL)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/types/MyType", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.NotNil(t, resp["type"])
}

func TestHandler_DeleteType(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateType(api.APIID, "type MyType { id: ID! }", appsync.TypeFormatSDL)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodDelete, "/v1/apis/"+api.APIID+"/types/MyType", nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Second delete returns 404.
	rec2 := doRequest(t, h, http.MethodDelete, "/v1/apis/"+api.APIID+"/types/MyType", nil)
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

func TestHandler_UpdateType(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateType(api.APIID, "type MyType { id: ID! }", appsync.TypeFormatSDL)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPut, "/v1/apis/"+api.APIID+"/types/MyType",
		map[string]any{"definition": "type MyType { id: ID! name: String! }", "format": "SDL"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	typeDef := resp["type"].(map[string]any)
	assert.Contains(t, typeDef["definition"], "name")
}
