package rekognition_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUsers(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	// Create collection
	rec := doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "user-coll"})
	require.Equal(t, http.StatusOK, rec.Code)

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "CreateUser succeeds",
			action:   "CreateUser",
			body:     map[string]any{"CollectionId": "user-coll", "UserId": "alice"},
			wantCode: http.StatusOK,
		},
		{
			name:     "CreateUser duplicate returns error",
			action:   "CreateUser",
			body:     map[string]any{"CollectionId": "user-coll", "UserId": "alice"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "ListUsers returns user",
			action:   "ListUsers",
			body:     map[string]any{"CollectionId": "user-coll"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				users, ok := resp["Users"].([]any)
				require.True(t, ok)
				assert.Len(t, users, 1)
				u := users[0].(map[string]any)
				assert.Equal(t, "alice", u["UserId"])
			},
		},
		{
			name:     "DeleteUser succeeds",
			action:   "DeleteUser",
			body:     map[string]any{"CollectionId": "user-coll", "UserId": "alice"},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteUser not found returns error",
			action:   "DeleteUser",
			body:     map[string]any{"CollectionId": "user-coll", "UserId": "alice"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.action, tc.body) //nolint:govet // existing issue.
			assert.Equal(t, tc.wantCode, rec.Code, tc.name)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestAssociateFaces(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	// Create collection, index face, create user
	rec := doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "assoc-coll"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "IndexFaces", map[string]any{
		"CollectionId":    "assoc-coll",
		"ExternalImageId": "ext-img-1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var indexResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &indexResp))
	faceRecords := indexResp["FaceRecords"].([]any)
	require.Len(t, faceRecords, 1)
	faceID := faceRecords[0].(map[string]any)["Face"].(map[string]any)["FaceId"].(string)

	rec = doRequest(t, h, "CreateUser", map[string]any{"CollectionId": "assoc-coll", "UserId": "bob"})
	require.Equal(t, http.StatusOK, rec.Code)

	// AssociateFaces
	t.Run("AssociateFaces succeeds", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rec := doRequest(t, h, "AssociateFaces", map[string]any{ //nolint:govet // existing issue.
			"CollectionId": "assoc-coll",
			"UserId":       "bob",
			"FaceIds":      []string{faceID},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		associated, ok := resp["AssociatedFaces"].([]any)
		require.True(t, ok)
		assert.Len(t, associated, 1)
	})

	// AssociateFaces with unknown face
	t.Run("AssociateFaces unknown face is unsuccessful", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rec := doRequest(t, h, "AssociateFaces", map[string]any{ //nolint:govet // existing issue.
			"CollectionId": "assoc-coll",
			"UserId":       "bob",
			"FaceIds":      []string{"00000000-0000-0000-0000-000000000000"},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		unsuccessful, ok := resp["UnsuccessfulFaceAssociations"].([]any)
		require.True(t, ok)
		assert.Len(t, unsuccessful, 1)
	})

	// DisassociateFaces
	t.Run("DisassociateFaces removes associated face", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rec := doRequest(t, h, "DisassociateFaces", map[string]any{ //nolint:govet // existing issue.
			"CollectionId": "assoc-coll",
			"UserId":       "bob",
			"FaceIds":      []string{faceID},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		disassociated, ok := resp["DisassociatedFaces"].([]any)
		require.True(t, ok)
		assert.Len(t, disassociated, 1)
	})
}

func TestSearchUsers(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "search-coll"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateUser", map[string]any{"CollectionId": "search-coll", "UserId": "user1"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateUser", map[string]any{"CollectionId": "search-coll", "UserId": "user2"})
	require.Equal(t, http.StatusOK, rec.Code)

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "SearchUsers returns matches",
			action: "SearchUsers",
			body: map[string]any{
				"CollectionId": "search-coll",
				"UserId":       "user1",
				"MaxUsers":     10,
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, faceModelVersion, resp["FaceModelVersion"])
				matches, ok := resp["UserMatches"].([]any)
				require.True(t, ok)
				assert.Len(t, matches, 1)
			},
		},
		{
			name:   "SearchUsersByImage returns matches",
			action: "SearchUsersByImage",
			body: map[string]any{
				"CollectionId": "search-coll",
				"MaxUsers":     10,
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, faceModelVersion, resp["FaceModelVersion"])
				matches, ok := resp["UserMatches"].([]any)
				require.True(t, ok)
				assert.GreaterOrEqual(t, len(matches), 2)
			},
		},
		{
			name:     "SearchUsers missing CollectionId returns error",
			action:   "SearchUsers",
			body:     map[string]any{"UserId": "user1"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "SearchUsers missing UserId returns error",
			action:   "SearchUsers",
			body:     map[string]any{"CollectionId": "search-coll"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "SearchUsersByImage missing CollectionId returns error",
			action:   "SearchUsersByImage",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.action, tc.body) //nolint:govet // existing issue.
			assert.Equal(t, tc.wantCode, rec.Code, tc.name)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}
