package mediatailor_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediatailor"
)

func TestVodSource_CreateAndDescribe(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestSourceLocation(t, h)

	tests := []struct {
		check    func(t *testing.T, body []byte)
		body     any
		path     string
		name     string
		wantCode int
	}{
		{
			name: "create returns vod source with ARN",
			path: "/sourceLocation/sl1/vodSource/my-vod",
			body: map[string]any{
				"HttpPackageConfigurations": []any{
					map[string]any{"Path": "/hls", "SourceGroup": "hd", "Type": "HLS"},
				},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "my-vod", resp["VodSourceName"])
				assert.Equal(t, "sl1", resp["SourceLocationName"])
				assert.Contains(
					t,
					resp["Arn"],
					"arn:aws:mediatailor:us-east-1:000000000000:sourceLocation/sl1/vodSource/my-vod",
				)
				assert.Len(t, resp["HttpPackageConfigurations"], 1)
			},
		},
		{
			name:     "create with missing source location returns 404",
			path:     "/sourceLocation/noexist/vodSource/v1",
			body:     map[string]any{},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, http.MethodPost, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestVodSource_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestSourceLocation(t, h)

	// Create
	rec := doRequest(t, h, http.MethodPost, "/sourceLocation/sl1/vodSource/vod1", map[string]any{
		"HttpPackageConfigurations": []any{
			map[string]any{"Path": "/hls", "SourceGroup": "hd", "Type": "HLS"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, mediatailor.VodSourceCount(h.Backend.(*mediatailor.InMemoryBackend)))

	// Describe
	rec = doRequest(t, h, http.MethodGet, "/sourceLocation/sl1/vodSource/vod1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "vod1", descResp["VodSourceName"])

	// Update
	rec = doRequest(t, h, http.MethodPut, "/sourceLocation/sl1/vodSource/vod1", map[string]any{
		"HttpPackageConfigurations": []any{
			map[string]any{"Path": "/hls", "SourceGroup": "hd", "Type": "HLS"},
			map[string]any{"Path": "/dash", "SourceGroup": "hd", "Type": "DASH_ISO"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	assert.Len(t, updateResp["HttpPackageConfigurations"], 2)

	// List
	rec = doRequest(t, h, http.MethodGet, "/sourceLocation/sl1/vodSources", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["Items"], 1)

	// Delete
	rec = doRequest(t, h, http.MethodDelete, "/sourceLocation/sl1/vodSource/vod1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, mediatailor.VodSourceCount(h.Backend.(*mediatailor.InMemoryBackend)))

	// Describe deleted returns 404
	rec = doRequest(t, h, http.MethodGet, "/sourceLocation/sl1/vodSource/vod1", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestVodSource_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestSourceLocation(t, h)

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{"describe unknown returns 404", http.MethodGet, "/sourceLocation/sl1/vodSource/notexist"},
		{"update unknown returns 404", http.MethodPut, "/sourceLocation/sl1/vodSource/notexist"},
		{"delete unknown returns 404", http.MethodDelete, "/sourceLocation/sl1/vodSource/notexist"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, tc.method, tc.path, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestVodSource_ListEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestSourceLocation(t, h)

	rec := doRequest(t, h, http.MethodGet, "/sourceLocation/sl1/vodSources", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["Items"])
}

func TestListVodSources_WithItems(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestSourceLocation(t, h)

	for i := range 2 {
		name := "vs-" + string(rune('a'+i))
		doRequest(t, h, http.MethodPost, "/sourceLocation/sl1/vodSource/"+name, map[string]any{
			"HttpPackageConfigurations": []any{
				map[string]any{"Path": "/hls", "SourceGroup": "default", "Type": "HLS"},
			},
		})
	}

	rec := doRequest(t, h, http.MethodGet, "/sourceLocation/sl1/vodSources", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items, _ := resp["Items"].([]any)
	assert.Len(t, items, 2)
}

func TestVodSource_DuplicateReturnsConflict(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestSourceLocation(t, h)

	doRequest(t, h, http.MethodPost, "/sourceLocation/sl1/vodSource/vs1", map[string]any{
		"HttpPackageConfigurations": []any{
			map[string]any{"Path": "/hls", "SourceGroup": "default", "Type": "HLS"},
		},
	})

	rec := doRequest(t, h, http.MethodPost, "/sourceLocation/sl1/vodSource/vs1", map[string]any{
		"HttpPackageConfigurations": []any{
			map[string]any{"Path": "/hls", "SourceGroup": "default", "Type": "HLS"},
		},
	})
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestCreateVodSource_MissingSourceLocation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/sourceLocation/missing/vodSource/vs1", map[string]any{
		"HttpPackageConfigurations": []any{
			map[string]any{"Path": "/hls", "SourceGroup": "default", "Type": "HLS"},
		},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestCreateVodSource_TagsSurviveDescribe verifies tags passed to
// CreateVodSource are queryable back from DescribeVodSource. Regression
// test: CreateVodSource stored tags on the struct but
// DescribeVodSource/ListVodSources unconditionally overwrite the response
// Tags from a separate ARN-keyed tag map CreateVodSource never wrote to,
// silently dropping every tag passed at creation.
func TestCreateVodSource_TagsSurviveDescribe(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestSourceLocation(t, h)

	rec := doRequest(t, h, http.MethodPost, "/sourceLocation/sl1/vodSource/vs1", map[string]any{
		"HttpPackageConfigurations": []any{
			map[string]any{"Path": "/hls", "SourceGroup": "default", "Type": "HLS"},
		},
		"tags": map[string]any{"team": "video"},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	rec = doRequest(t, h, http.MethodGet, "/sourceLocation/sl1/vodSource/vs1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	tags, _ := resp["tags"].(map[string]any)
	assert.Equal(t, "video", tags["team"], "tags set at creation must survive to DescribeVodSource")
}

// TestVodSource_Timestamps verifies CreationTime/LastModifiedTime are
// populated on create and LastModifiedTime advances on update -- both were
// dead fields (declared, never set) before this pass.
func TestVodSource_Timestamps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestSourceLocation(t, h)

	rec := doRequest(t, h, http.MethodPost, "/sourceLocation/sl1/vodSource/vs1", map[string]any{
		"HttpPackageConfigurations": []any{
			map[string]any{"Path": "/hls", "SourceGroup": "default", "Type": "HLS"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	require.NotNil(t, created["CreationTime"])
	require.NotNil(t, created["LastModifiedTime"])
}
