package appsync_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestType_CRUD_AllFormats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		definition string
		format     appsync.TypeDefinitionFormat
		typeName   string
	}{
		{
			name:       "sdl_type",
			definition: "type User { id: ID! name: String }",
			format:     appsync.TypeFormatSDL,
			typeName:   "User",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)

			created, err := b.CreateType(api.APIID, tt.definition, tt.format)
			require.NoError(t, err)
			assert.Equal(t, tt.typeName, created.Name)
			assert.Equal(t, tt.format, created.Format)

			got, err := b.GetType(api.APIID, tt.typeName)
			require.NoError(t, err)
			assert.Equal(t, tt.typeName, got.Name)

			types, err := b.ListTypes(api.APIID)
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(types), 1)

			newDef := "type User { id: ID! name: String email: String }"
			updated, err := b.UpdateType(api.APIID, tt.typeName, newDef, tt.format)
			require.NoError(t, err)
			assert.Equal(t, tt.typeName, updated.Name)

			err = b.DeleteType(api.APIID, tt.typeName)
			require.NoError(t, err)

			_, err = b.GetType(api.APIID, tt.typeName)
			require.Error(t, err)
			assert.ErrorIs(t, err, appsync.ErrNotFound)
		})
	}
}

func TestInMemoryBackend_CreateType_DuplicateDetection(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	def := "type MyType { id: ID! }"

	_, err = b.CreateType(api.APIID, def, appsync.TypeFormatSDL)
	require.NoError(t, err)

	// Creating the same type again should fail.
	_, err = b.CreateType(api.APIID, def, appsync.TypeFormatSDL)
	require.ErrorIs(t, err, awserr.ErrAlreadyExists)
}

func TestInMemoryBackend_TypeCRUD(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateType(api.APIID, "type MyType { id: ID! }", appsync.TypeFormatSDL)
	require.NoError(t, err)

	// Get by name.
	got, err := b.GetType(api.APIID, "MyType")
	require.NoError(t, err)
	assert.Equal(t, "MyType", got.Name)

	// List returns 1.
	types, err := b.ListTypes(api.APIID)
	require.NoError(t, err)
	assert.Len(t, types, 1)

	// Delete.
	err = b.DeleteType(api.APIID, "MyType")
	require.NoError(t, err)

	// Get after delete returns error.
	_, err = b.GetType(api.APIID, "MyType")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_UpdateType(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateType(api.APIID, "type MyType { id: ID! }", appsync.TypeFormatSDL)
	require.NoError(t, err)

	updated, err := b.UpdateType(api.APIID, "MyType", "type MyType { id: ID! name: String! }", "")
	require.NoError(t, err)
	assert.Contains(t, updated.Definition, "name")

	// Not found returns error.
	_, err = b.UpdateType(api.APIID, "NonExistent", "type X {}", "")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_GetType_APINotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, err := b.GetType("nonexistent", "MyType")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_ListTypes_APINotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, err := b.ListTypes("nonexistent")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_CreateType_FormatValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		format  appsync.TypeDefinitionFormat
		wantErr bool
	}{
		{name: "sdl_format", format: appsync.TypeFormatSDL},
		{name: "json_format", format: appsync.TypeFormatJSON},
		{name: "invalid_format", format: "XML", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)

			_, err = b.CreateType(api.APIID, "type Item { id: ID! }", tt.format)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestBackend_ListTypesByAssociation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		createAssoc bool
		createTypes bool
		wantErr     bool
		wantCount   int
	}{
		{
			name:        "success_no_types",
			createAssoc: true,
			createTypes: false,
			wantCount:   0,
		},
		{
			name:        "success_with_types",
			createAssoc: true,
			createTypes: true,
			wantCount:   1,
		},
		{
			name:        "api_not_found",
			createAssoc: false,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			mergedAPIID := "nonexistent"
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

				if tt.createTypes {
					_, err = b.CreateType(merged.APIID, "type Query { hello: String }", appsync.TypeFormatSDL)
					require.NoError(t, err)
				}
			}

			types, err := b.ListTypesByAssociation(mergedAPIID, assocID, "SDL")

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, types, tt.wantCount)
		})
	}
}

// TestListTypes_Pagination verifies maxResults/nextToken on ListTypes.
func TestListTypes_Pagination(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec := doRequest(t, h, http.MethodPost, "/v1/apis", map[string]any{
		"name":               "type-api",
		"authenticationType": "API_KEY",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var apiOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &apiOut))
	apiID := apiOut["graphqlApi"].(map[string]any)["apiId"].(string)

	for i := range 4 {
		rec = doRequest(t, h, http.MethodPost, fmt.Sprintf("/v1/apis/%s/types", apiID), map[string]any{
			"definition": fmt.Sprintf("type Type%d { id: ID }", i),
			"format":     "SDL",
		})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	tests := []struct {
		name          string
		path          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			path:          fmt.Sprintf("/v1/apis/%s/types?format=SDL", apiID),
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			path:          fmt.Sprintf("/v1/apis/%s/types?format=SDL&maxResults=2", apiID),
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			listRec := doRequest(t, h, http.MethodGet, tt.path, nil)
			require.Equal(t, http.StatusOK, listRec.Code)

			var out struct {
				NextToken string           `json:"nextToken"`
				Types     []map[string]any `json:"types"`
			}
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
			assert.Len(t, out.Types, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out.NextToken)
			} else {
				assert.Empty(t, out.NextToken)
			}
		})
	}
}
