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

func TestHandler_UpdateSourceAPIAssociation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        any
		name        string
		wantStatus  int
		createAssoc bool
		useWrongID  bool
	}{
		{
			name:        "success",
			createAssoc: true,
			body:        map[string]any{"description": "new desc"},
			wantStatus:  http.StatusOK,
		},
		{
			name:        "not_found",
			createAssoc: false,
			useWrongID:  true,
			body:        map[string]any{"description": "desc"},
			wantStatus:  http.StatusNotFound,
		},
		{
			name:        "bad_body",
			createAssoc: true,
			body:        "not-json-string",
			wantStatus:  http.StatusBadRequest,
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

				assoc, err := b.AssociateSourceGraphqlAPI(merged.APIID, source.APIID, "initial", "")
				require.NoError(t, err)

				if !tt.useWrongID {
					assocID = assoc.AssociationID
					mergedAPIID = merged.APIID
				}
			}

			path := fmt.Sprintf("/v1/mergedApis/%s/sourceApiAssociations/%s", mergedAPIID, assocID)
			rec := doRequest(t, h, http.MethodPut, path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_AssociateMergedGraphqlApi(t *testing.T) {
	t.Parallel()

	t.Run("associates_merged_api_successfully", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler()
		src, err := b.CreateGraphqlAPI("SourceAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
		require.NoError(t, err)
		mrg, err := b.CreateGraphqlAPI("MergedAPI", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
		require.NoError(t, err)

		rec := doRequest(
			t, h, http.MethodPost,
			"/v1/sourceApis/"+src.APIID+"/mergedApiAssociations",
			map[string]any{"mergedApiIdentifier": mrg.APIID, "description": "test"},
		)
		assert.Equal(t, http.StatusCreated, rec.Code)

		var resp map[string]any
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assoc, ok := resp["sourceApiAssociation"].(map[string]any)
		require.True(t, ok)
		assert.NotEmpty(t, assoc["associationId"])
		assert.Equal(t, src.APIID, assoc["sourceApiId"])
	})

	t.Run("missing_merged_api_identifier_returns_400", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler()
		src, err := b.CreateGraphqlAPI("SourceAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
		require.NoError(t, err)

		rec := doRequest(
			t, h, http.MethodPost,
			"/v1/sourceApis/"+src.APIID+"/mergedApiAssociations",
			map[string]any{"description": "test"},
		)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestHandler_AssociateSourceGraphqlApi(t *testing.T) {
	t.Parallel()

	t.Run("associates_source_api_successfully", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler()
		src, err := b.CreateGraphqlAPI("SourceAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
		require.NoError(t, err)
		mrg, err := b.CreateGraphqlAPI("MergedAPI", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
		require.NoError(t, err)

		rec := doRequest(
			t, h, http.MethodPost,
			"/v1/mergedApis/"+mrg.APIID+"/sourceApiAssociations",
			map[string]any{"sourceApiIdentifier": src.APIID, "description": "test"},
		)
		assert.Equal(t, http.StatusCreated, rec.Code)

		var resp map[string]any
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assoc, ok := resp["sourceApiAssociation"].(map[string]any)
		require.True(t, ok)
		assert.NotEmpty(t, assoc["associationId"])
		assert.Equal(t, mrg.APIID, assoc["mergedApiId"])
	})

	t.Run("missing_source_api_identifier_returns_400", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler()
		mrg, err := b.CreateGraphqlAPI("MergedAPI", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
		require.NoError(t, err)

		rec := doRequest(
			t, h, http.MethodPost,
			"/v1/mergedApis/"+mrg.APIID+"/sourceApiAssociations",
			map[string]any{"description": "test"},
		)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestHandler_SourceAPIAssociations_CRUD(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()

	// Create the APIs first (validation requires both to exist).
	srcAPI, err := b.CreateGraphqlAPI("SourceAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	mrgAPI, err := b.CreateGraphqlAPI("MergedAPI", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
	require.NoError(t, err)

	// Associate source graphql API.
	rec1 := doRequest(t, h, http.MethodPost, "/v1/mergedApis/"+mrgAPI.APIID+"/sourceApiAssociations",
		map[string]any{"sourceApiIdentifier": srcAPI.APIID, "description": "test"})
	require.Equal(t, http.StatusCreated, rec1.Code)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec1.Body).Decode(&createResp))
	assocID := createResp["sourceApiAssociation"].(map[string]any)["associationId"].(string)

	// List source API associations.
	rec2 := doRequest(t, h, http.MethodGet, "/v1/mergedApis/"+mrgAPI.APIID+"/sourceApiAssociations", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var listResp map[string]any
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&listResp))
	assocs := listResp["sourceApiAssociationSummaries"].([]any)
	assert.Len(t, assocs, 1)

	// Get source API association.
	rec3 := doRequest(t, h, http.MethodGet, "/v1/mergedApis/"+mrgAPI.APIID+"/sourceApiAssociations/"+assocID, nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	var getResp map[string]any
	require.NoError(t, json.NewDecoder(rec3.Body).Decode(&getResp))
	assoc := getResp["sourceApiAssociation"].(map[string]any)
	assert.Equal(t, mrgAPI.APIID, assoc["mergedApiId"])

	// Disassociate source graphql API.
	rec4 := doRequest(t, h, http.MethodDelete, "/v1/mergedApis/"+mrgAPI.APIID+"/sourceApiAssociations/"+assocID, nil)
	assert.Equal(t, http.StatusNoContent, rec4.Code)

	// Get after delete returns 404.
	rec5 := doRequest(t, h, http.MethodGet, "/v1/mergedApis/"+mrgAPI.APIID+"/sourceApiAssociations/"+assocID, nil)
	assert.Equal(t, http.StatusNotFound, rec5.Code)
}

func TestHandler_DisassociateMergedGraphqlApi(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()

	// Create the APIs first.
	srcAPI, err := b.CreateGraphqlAPI("SourceAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	mrgAPI, err := b.CreateGraphqlAPI("MergedAPI", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
	require.NoError(t, err)

	// Associate first.
	rec1 := doRequest(t, h, http.MethodPost, "/v1/sourceApis/"+srcAPI.APIID+"/mergedApiAssociations",
		map[string]any{"mergedApiIdentifier": mrgAPI.APIID})
	require.Equal(t, http.StatusCreated, rec1.Code)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec1.Body).Decode(&createResp))
	assocID := createResp["sourceApiAssociation"].(map[string]any)["associationId"].(string)

	// Disassociate.
	rec2 := doRequest(t, h, http.MethodDelete, "/v1/sourceApis/"+srcAPI.APIID+"/mergedApiAssociations/"+assocID, nil)
	assert.Equal(t, http.StatusNoContent, rec2.Code)

	// Second disassociate returns 404.
	rec3 := doRequest(t, h, http.MethodDelete, "/v1/sourceApis/"+srcAPI.APIID+"/mergedApiAssociations/"+assocID, nil)
	assert.Equal(t, http.StatusNotFound, rec3.Code)
}

func TestHandler_ListSourceApiAssociations_Empty(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec := doRequest(t, h, http.MethodGet, "/v1/mergedApis/empty-id/sourceApiAssociations", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Empty(t, resp["sourceApiAssociationSummaries"])
}

// TestHandler_StartSchemaMerge locks the real AWS SDK endpoint
// POST /v1/mergedApis/{mergedApiIdentifier}/sourceApiAssociations/{associationId}/merge
// -- both the route itself (keyed by BOTH mergedApiIdentifier and associationId, unlike
// the removed /v1/apis/{apiId}/schemaMerge invented endpoint) and the response wire
// shape ({"sourceApiAssociationStatus": "..."}).
func TestHandler_StartSchemaMerge(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		method      string
		wantStatus  int
		createAssoc bool
	}{
		{
			name:        "post_success",
			method:      http.MethodPost,
			createAssoc: true,
			wantStatus:  http.StatusOK,
		},
		{
			name:        "association_not_found",
			method:      http.MethodPost,
			createAssoc: false,
			wantStatus:  http.StatusNotFound,
		},
		{
			name:        "method_not_allowed",
			method:      http.MethodGet,
			createAssoc: true,
			wantStatus:  http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			mergedAPIID := "nonexistent-merged"
			assocID := "nonexistent-assoc"

			if tt.createAssoc {
				merged, err := b.CreateGraphqlAPI(
					"MergedAPI", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil,
				)
				require.NoError(t, err)
				source, err := b.CreateGraphqlAPI("SourceAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
				require.NoError(t, err)

				assoc, assocErr := b.AssociateSourceGraphqlAPI(merged.APIID, source.APIID, "initial", "")
				require.NoError(t, assocErr)

				mergedAPIID = merged.APIID
				assocID = assoc.AssociationID
			}

			path := fmt.Sprintf(
				"/v1/mergedApis/%s/sourceApiAssociations/%s/merge", mergedAPIID, assocID,
			)
			rec := doRequest(t, h, tt.method, path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
			assert.Equal(t, appsync.SourceAPIAssociationStatusMergeSuccess, resp["sourceApiAssociationStatus"])
		})
	}
}

// TestHandler_LegacySchemaMergeEndpointRemoved locks that the invented
// /v1/apis/{apiId}/schemaMerge endpoint (removed for not matching the real SDK path,
// request shape, or response shape) is no longer routed to StartSchemaMerge -- POST
// now falls through to UpdateGraphqlApi's dispatch (segment 3 "schemaMerge" is not a
// recognized subresource, so the request 404s).
func TestHandler_LegacySchemaMergeEndpointRemoved(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("MergedAPI", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+api.APIID+"/schemaMerge", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
