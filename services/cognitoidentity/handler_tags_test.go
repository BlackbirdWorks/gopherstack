package cognitoidentity_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		arn      string
		wantCode int
	}{
		{name: "success", wantCode: http.StatusOK},
		{
			name:     "not_found",
			arn:      "arn:aws:cognito-identity:us-east-1:000000000000:identitypool/nonexistent",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arn := tt.arn

			if tt.name == "success" {
				createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
					"IdentityPoolName":               "tags-pool",
					"AllowUnauthenticatedIdentities": true,
					"IdentityPoolTags": map[string]string{
						"env": "test",
					},
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				var created map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
				poolID := created["IdentityPoolId"].(string)
				arn = "arn:aws:cognito-identity:us-east-1:000000000000:identitypool/" + poolID
			}

			rec := doCognitoIdentityRequest(t, h, "ListTagsForResource", map[string]any{
				"ResourceArn": arn,
			})

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				tags, _ := out["Tags"].(map[string]any)
				assert.Equal(t, "test", tags["env"])
			}
		})
	}
}

func TestHandler_TagResource_UntagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "tagging-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	poolID := created["IdentityPoolId"].(string)
	arn := "arn:aws:cognito-identity:us-east-1:000000000000:identitypool/" + poolID

	// TagResource.
	tagRec := doCognitoIdentityRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": arn,
		"Tags": map[string]string{
			"team":  "backend",
			"owner": "alice",
		},
	})
	require.Equal(t, http.StatusOK, tagRec.Code)

	// Verify tags were set.
	listRec := doCognitoIdentityRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceArn": arn,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	tags, _ := listOut["Tags"].(map[string]any)
	assert.Equal(t, "backend", tags["team"])
	assert.Equal(t, "alice", tags["owner"])

	// UntagResource.
	untagRec := doCognitoIdentityRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": arn,
		"TagKeys":     []string{"owner"},
	})
	require.Equal(t, http.StatusOK, untagRec.Code)

	// Verify tag removed.
	listRec2 := doCognitoIdentityRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceArn": arn,
	})
	require.Equal(t, http.StatusOK, listRec2.Code)

	var listOut2 map[string]any
	require.NoError(t, json.Unmarshal(listRec2.Body.Bytes(), &listOut2))
	tags2, _ := listOut2["Tags"].(map[string]any)
	assert.Equal(t, "backend", tags2["team"])
	assert.NotContains(t, tags2, "owner")
}

func TestHandler_TagResource_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": "arn:aws:cognito-identity:us-east-1:000000000000:identitypool/nonexistent",
		"Tags": map[string]string{
			"key": "value",
		},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UntagResource_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": "arn:aws:cognito-identity:us-east-1:000000000000:identitypool/nonexistent",
		"TagKeys":     []string{"key"},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestListTagsForResource_EmptyTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "empty-tags-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

	poolID := created["IdentityPoolId"].(string)
	arn := "arn:aws:cognito-identity:us-east-1:000000000000:identitypool/" + poolID

	rec := doCognitoIdentityRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceArn": arn,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	// Tags should always be present (even if empty), not omitted.
	tags, hasKey := out["Tags"]
	require.True(t, hasKey, "Tags key should always be present in ListTagsForResource response")
	tagsMap, isMap := tags.(map[string]any)
	require.True(t, isMap, "Tags should be a map")
	assert.Empty(t, tagsMap)
}

func TestHandler_TagResource_EmptyArn_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": "",
		"Tags":        map[string]string{"k": "v"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UntagResource_EmptyArn_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": "",
		"TagKeys":     []string{"k"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
