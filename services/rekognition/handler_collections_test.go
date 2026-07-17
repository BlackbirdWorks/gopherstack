package rekognition_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rekognition"
)

func TestRekognition_Collections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		setup    func(h *rekognition.Handler)
		check    func(t *testing.T, body []byte)
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "CreateCollection returns ARN",
			action:   "CreateCollection",
			body:     map[string]any{"CollectionId": "my-coll"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["CollectionArn"], "arn:aws:rekognition:")
				assert.Equal(t, "7.0", resp["FaceModelVersion"])
			},
		},
		{
			name:   "CreateCollection duplicate returns error",
			action: "CreateCollection",
			setup: func(h *rekognition.Handler) {
				doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "dup-coll"})
			},
			body:     map[string]any{"CollectionId": "dup-coll"},
			wantCode: http.StatusBadRequest,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "ResourceAlreadyExistsException", resp["__type"])
			},
		},
		{
			name:   "DeleteCollection returns 200",
			action: "DeleteCollection",
			setup: func(h *rekognition.Handler) {
				doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "del-coll"})
			},
			body:     map[string]any{"CollectionId": "del-coll"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.InDelta(t, float64(200), resp["StatusCode"], 0)
			},
		},
		{
			name:     "DeleteCollection unknown returns error",
			action:   "DeleteCollection",
			body:     map[string]any{"CollectionId": "no-such"},
			wantCode: http.StatusBadRequest,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "ResourceNotFoundException", resp["__type"])
			},
		},
		{
			name:   "DescribeCollection returns face count",
			action: "DescribeCollection",
			setup: func(h *rekognition.Handler) {
				doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "desc-coll"})
			},
			body:     map[string]any{"CollectionId": "desc-coll"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["CollectionARN"], "arn:aws:rekognition:")
				assert.InDelta(t, float64(0), resp["FaceCount"], 0)
			},
		},
		{
			name:     "ListCollections empty returns empty list",
			action:   "ListCollections",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				ids, _ := resp["CollectionIds"].([]any)
				assert.Empty(t, ids)
			},
		},
		{
			name:   "ListCollections shows created collection",
			action: "ListCollections",
			setup: func(h *rekognition.Handler) {
				doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "list-coll"})
			},
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				ids, _ := resp["CollectionIds"].([]any)
				assert.Len(t, ids, 1)
				assert.Equal(t, "list-coll", ids[0])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			if tc.setup != nil {
				tc.setup(h)
			}

			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Pagination: ListCollections
// ---------------------------------------------------------------------------

func TestListCollections_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create 3 collections: alpha, beta, gamma (sorted order).
	for _, id := range []string{"alpha", "beta", "gamma"} {
		rec := doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": id})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Page 1: MaxResults=2 → alpha, beta, NextToken=gamma.
	rec := doRequest(t, h, "ListCollections", map[string]any{"MaxResults": 2})
	require.Equal(t, http.StatusOK, rec.Code)

	var page1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))

	ids1, _ := page1["CollectionIds"].([]any)
	require.Len(t, ids1, 2)
	assert.Equal(t, "alpha", ids1[0])
	assert.Equal(t, "beta", ids1[1])

	nextToken1, _ := page1["NextToken"].(string)
	require.NotEmpty(t, nextToken1, "NextToken must be set when there are more results")

	// Page 2: NextToken from page 1 → gamma, no NextToken.
	rec = doRequest(t, h, "ListCollections", map[string]any{"NextToken": nextToken1})
	require.Equal(t, http.StatusOK, rec.Code)

	var page2 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page2))

	ids2, _ := page2["CollectionIds"].([]any)
	require.Len(t, ids2, 1)
	assert.Equal(t, "gamma", ids2[0])
	assert.Empty(t, page2["NextToken"], "NextToken must be absent on last page")
}

// ---------------------------------------------------------------------------
// DeleteCollection cascades to faces
// ---------------------------------------------------------------------------

func TestDeleteCollection_CascadesToFaces(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "cascade-coll"})
	doRequest(t, h, "IndexFaces", map[string]any{"CollectionId": "cascade-coll"})
	doRequest(t, h, "IndexFaces", map[string]any{"CollectionId": "cascade-coll"})

	// Verify faces exist before delete.
	rec := doRequest(t, h, "ListFaces", map[string]any{"CollectionId": "cascade-coll"})
	require.Equal(t, http.StatusOK, rec.Code)
	var lf map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lf))
	faces, _ := lf["Faces"].([]any)
	require.Len(t, faces, 2)

	// Delete collection.
	rec = doRequest(t, h, "DeleteCollection", map[string]any{"CollectionId": "cascade-coll"})
	require.Equal(t, http.StatusOK, rec.Code)

	// ListFaces should now return ResourceNotFoundException.
	rec = doRequest(t, h, "ListFaces", map[string]any{"CollectionId": "cascade-coll"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "ResourceNotFoundException", errResp["__type"])
}

// ---------------------------------------------------------------------------
// DescribeCollection not-found returns ResourceNotFoundException
// ---------------------------------------------------------------------------

func TestDescribeCollection_NotFound_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DescribeCollection", map[string]any{"CollectionId": "no-coll"})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ResourceNotFoundException", resp["__type"])
}

// ---------------------------------------------------------------------------
// Tag validation on CreateCollection initial tags
// ---------------------------------------------------------------------------

func TestCreateCollection_InvalidInitialTags_Rejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		name     string
		wantCode int
	}{
		{
			name:     "empty tag key rejected",
			tags:     map[string]string{"": "v"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "tag key too long rejected",
			tags:     map[string]string{strings.Repeat("k", 129): "v"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "tag value too long rejected",
			tags:     map[string]string{"k": strings.Repeat("v", 257)},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateCollection", map[string]any{
				"CollectionId": "init-tags-coll",
				"Tags":         tc.tags,
			})
			assert.Equal(t, tc.wantCode, rec.Code, tc.name)
		})
	}
}

func TestCreateCollection_ValidInitialTags_Accepted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateCollection", map[string]any{
		"CollectionId": "valid-tags-coll",
		"Tags":         map[string]string{"env": "test", "team": "infra"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---------------------------------------------------------------------------
// CollectionId length validation
// ---------------------------------------------------------------------------

func TestCreateCollection_IDLengthValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		id       string
		wantCode int
	}{
		{
			name:     "empty ID rejected",
			id:       "",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "ID at limit (255) accepted",
			id:       strings.Repeat("a", 255),
			wantCode: http.StatusOK,
		},
		{
			name:     "ID over limit (256) rejected",
			id:       strings.Repeat("a", 256),
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": tc.id})
			assert.Equal(t, tc.wantCode, rec.Code, tc.name)
		})
	}
}

// ---------------------------------------------------------------------------
// ListCollections never returns null slices
// ---------------------------------------------------------------------------

func TestListCollections_EmptyReturnsSlices(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListCollections", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	// Decode raw to check for non-null slice presence.
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	_, hasIDs := raw["CollectionIds"]
	assert.True(t, hasIDs, "CollectionIds must be present")

	var ids []any
	require.NoError(t, json.Unmarshal(raw["CollectionIds"], &ids))
	assert.NotNil(t, ids, "CollectionIds must not be null")
}
