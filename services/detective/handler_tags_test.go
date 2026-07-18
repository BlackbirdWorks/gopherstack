package detective_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/detective"
)

func TestDetective_Tags(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	// Create graph first
	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	graphArn := createResp["GraphArn"].(string)

	tests := []struct {
		name     string
		body     any
		check    func(t *testing.T, body []byte)
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "ListTagsForResource returns empty tags",
			method:   http.MethodGet,
			path:     "/tags/" + graphArn,
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				tags, ok := resp["Tags"].(map[string]any)
				require.True(t, ok)
				assert.Empty(t, tags)
			},
		},
		{
			name:     "TagResource returns 200",
			method:   http.MethodPost,
			path:     "/tags/" + graphArn,
			body:     map[string]any{"Tags": map[string]string{"key1": "val1"}},
			wantCode: http.StatusOK,
		},
		{
			name:     "ListTagsForResource after tag returns tags",
			method:   http.MethodGet,
			path:     "/tags/" + graphArn,
			wantCode: http.StatusOK,
			check: func(t *testing.T, _ []byte) {
				t.Helper()

				// tag first
				doRequest(t, h, http.MethodPost, "/tags/"+graphArn, map[string]any{
					"Tags": map[string]string{"mykey": "myval"},
				})

				tagRec := doRequest(t, h, http.MethodGet, "/tags/"+graphArn, nil)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(tagRec.Body.Bytes(), &resp))
				tags := resp["Tags"].(map[string]any)
				assert.Equal(t, "myval", tags["mykey"])
			},
		},
		{
			name:     "TagResource unknown arn returns 404",
			method:   http.MethodPost,
			path:     "/tags/arn:aws:detective:us-east-1:000000000000:graph:notexists",
			body:     map[string]any{"Tags": map[string]string{"k": "v"}},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "UntagResource returns 200",
			method:   http.MethodDelete,
			path:     "/tags/" + graphArn,
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec2 := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec2.Code)

			if tc.check != nil {
				tc.check(t, rec2.Body.Bytes())
			}
		})
	}
}

func TestDetective_TagValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		name     string
		wantCode int
	}{
		{
			name:     "valid tags",
			tags:     map[string]string{"env": "prod", "team": "infra"},
			wantCode: http.StatusOK,
		},
		{
			name:     "empty key rejected",
			tags:     map[string]string{"": "value"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "key at limit accepted",
			tags:     map[string]string{strings.Repeat("k", 128): "v"},
			wantCode: http.StatusOK,
		},
		{
			name:     "key over limit rejected",
			tags:     map[string]string{strings.Repeat("k", 129): "v"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "value at limit accepted",
			tags:     map[string]string{"k": strings.Repeat("v", 256)},
			wantCode: http.StatusOK,
		},
		{
			name:     "value over limit rejected",
			tags:     map[string]string{"k": strings.Repeat("v", 257)},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
			require.Equal(t, http.StatusOK, createRec.Code)

			rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{"Tags": tc.tags})
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestDetective_TagResourceLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *detective.Handler, graphArn string)
		tags     map[string]string
		name     string
		wantCode int
	}{
		{
			name:     "tag unknown resource returns 404",
			tags:     map[string]string{"k": "v"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "tag key over limit returns 400",
			tags:     map[string]string{strings.Repeat("k", 129): "v"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "tag value over limit returns 400",
			tags:     map[string]string{"k": strings.Repeat("v", 257)},
			wantCode: http.StatusBadRequest,
		},
	}

	unknownARN := "arn:aws:detective:us-east-1:000000000000:graph:doesnotexist"

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			arn := unknownARN
			if tc.setup != nil {
				rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
				require.Equal(t, http.StatusOK, rec.Code)
				tc.setup(h, arn)
			}

			rec := doRequest(t, h, http.MethodPost, "/tags/"+arn, map[string]any{"Tags": tc.tags})
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestDetective_TagCountLimit(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	parseJSON(t, createRec.Body.Bytes(), &createResp)
	graphArn := createResp["GraphArn"].(string)

	bulk := make(map[string]string, 50)
	for i := range 50 {
		bulk[strings.Repeat("k", 1)+string(rune('a'+i%26))+strings.Repeat("x", i)] = "v"
	}
	rec := doRequest(t, h, http.MethodPost, "/tags/"+graphArn, map[string]any{"Tags": bulk})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodPost, "/tags/"+graphArn, map[string]any{"Tags": map[string]string{"new-key": "v"}})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// TagResource: empty Tags body succeeds
// ---------------------------------------------------------------------------

func TestTagResource_EmptyTags_Succeeds(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	graphArn := createResp["GraphArn"].(string)

	rec2 := doRequest(t, h, http.MethodPost, "/tags/"+graphArn, map[string]any{
		"Tags": map[string]string{},
	})
	assert.Equal(t, http.StatusOK, rec2.Code, "TagResource with empty Tags must succeed")
}

// ---------------------------------------------------------------------------
// UntagResource: non-existent key is silent no-op
// ---------------------------------------------------------------------------

func TestUntagResource_NonExistentKey_Succeeds(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	graphArn := createResp["GraphArn"].(string)

	req := doRequest(t, h, http.MethodDelete, "/tags/"+graphArn+"?tagKeys=does-not-exist", nil)
	assert.Equal(t, http.StatusOK, req.Code, "UntagResource with non-existent key must succeed (no-op)")
}

// ---------------------------------------------------------------------------
// ListTagsForResource: Tags is {} not null when no tags set
// ---------------------------------------------------------------------------

func TestListTagsForResource_NoTags_Returns_EmptyObject(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	graphArn := createResp["GraphArn"].(string)

	rec2 := doRequest(t, h, http.MethodGet, "/tags/"+graphArn, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))

	tags, hasKey := resp["Tags"]
	assert.True(t, hasKey, "ListTagsForResource must include Tags field")
	_, ok := tags.(map[string]any)
	assert.True(t, ok, "Tags must be an object (not null), got %T: %v", tags, tags)
}

// ---------------------------------------------------------------------------
// CreateGraph with tags: visible via ListTagsForResource
// ---------------------------------------------------------------------------

func TestCreateGraph_Tags_VisibleInListTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{
		"Tags": map[string]string{"env": "prod", "team": "security"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	graphArn := createResp["GraphArn"].(string)

	rec2 := doRequest(t, h, http.MethodGet, "/tags/"+graphArn, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var tagsResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &tagsResp))
	tags, _ := tagsResp["Tags"].(map[string]any)

	assert.Equal(t, "prod", tags["env"], "tags set at graph creation must be visible via ListTagsForResource")
	assert.Equal(t, "security", tags["team"])
}
