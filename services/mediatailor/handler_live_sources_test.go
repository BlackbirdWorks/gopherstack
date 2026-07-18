package mediatailor_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediatailor"
)

func TestLiveSource_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check    func(t *testing.T, rec *httptest.ResponseRecorder)
		setup    func(t *testing.T, h *mediatailor.Handler)
		body     any
		path     string
		name     string
		wantCode int
	}{
		{
			name: "create live source returns ARN and configs",
			setup: func(t *testing.T, h *mediatailor.Handler) {
				t.Helper()
				createTestSourceLocation(t, h)
			},
			path: "/sourceLocation/sl1/liveSource/ls1",
			body: map[string]any{
				"HttpPackageConfigurations": []any{
					map[string]any{"Path": "/hls", "SourceGroup": "sg1", "Type": "HLS"},
				},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "ls1", resp["LiveSourceName"])
				assert.Equal(t, "sl1", resp["SourceLocationName"])
				assert.Contains(t, resp["Arn"], ":mediatailor:")
				cfgs, ok := resp["HttpPackageConfigurations"].([]any)
				require.True(t, ok)
				assert.Len(t, cfgs, 1)
			},
		},
		{
			name:     "create live source under missing source location returns 404",
			setup:    func(_ *testing.T, _ *mediatailor.Handler) {},
			path:     "/sourceLocation/nope/liveSource/ls1",
			body:     map[string]any{},
			wantCode: http.StatusNotFound,
		},
		{
			name: "create duplicate live source returns 409",
			setup: func(t *testing.T, h *mediatailor.Handler) {
				t.Helper()
				createTestSourceLocation(t, h)
				doRequest(t, h, http.MethodPost, "/sourceLocation/sl1/liveSource/ls1", map[string]any{})
			},
			path:     "/sourceLocation/sl1/liveSource/ls1",
			body:     map[string]any{},
			wantCode: http.StatusConflict,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tc.setup(t, h)

			rec := doRequest(t, h, http.MethodPost, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec)
			}
		})
	}
}

func TestLiveSource_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestSourceLocation(t, h)

	// create
	rec := doRequest(t, h, http.MethodPost, "/sourceLocation/sl1/liveSource/ls1", map[string]any{
		"HttpPackageConfigurations": []any{
			map[string]any{"Path": "/hls", "SourceGroup": "sg1", "Type": "HLS"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// describe
	rec = doRequest(t, h, http.MethodGet, "/sourceLocation/sl1/liveSource/ls1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ls1", resp["LiveSourceName"])

	// update
	rec = doRequest(t, h, http.MethodPut, "/sourceLocation/sl1/liveSource/ls1", map[string]any{
		"HttpPackageConfigurations": []any{
			map[string]any{"Path": "/dash", "SourceGroup": "sg2", "Type": "DASH_ISO"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var updated map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
	cfgs, _ := updated["HttpPackageConfigurations"].([]any)
	require.Len(t, cfgs, 1)
	cfg0, _ := cfgs[0].(map[string]any)
	assert.Equal(t, "/dash", cfg0["Path"])

	// list
	rec = doRequest(t, h, http.MethodGet, "/sourceLocation/sl1/liveSources", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	items, _ := listResp["Items"].([]any)
	assert.Len(t, items, 1)

	// delete
	rec = doRequest(t, h, http.MethodDelete, "/sourceLocation/sl1/liveSource/ls1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// describe after delete returns 404
	rec = doRequest(t, h, http.MethodGet, "/sourceLocation/sl1/liveSource/ls1", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestLiveSource_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		method   string
		path     string
		name     string
		wantCode int
	}{
		{
			name:     "describe missing live source returns 404",
			method:   http.MethodGet,
			path:     "/sourceLocation/sl1/liveSource/missing",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "update missing live source returns 404",
			method:   http.MethodPut,
			path:     "/sourceLocation/sl1/liveSource/missing",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "delete missing live source returns 404",
			method:   http.MethodDelete,
			path:     "/sourceLocation/sl1/liveSource/missing",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestSourceLocation(t, h)

			rec := doRequest(t, h, tc.method, tc.path, nil)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestListLiveSources_WithItems(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestSourceLocation(t, h)

	for i := range 2 {
		name := "ls-" + string(rune('a'+i))
		doRequest(t, h, http.MethodPost, "/sourceLocation/sl1/liveSource/"+name, map[string]any{
			"HttpPackageConfigurations": []any{
				map[string]any{"Path": "/hls", "SourceGroup": "default", "Type": "HLS"},
			},
		})
	}

	rec := doRequest(t, h, http.MethodGet, "/sourceLocation/sl1/liveSources", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items, _ := resp["Items"].([]any)
	assert.Len(t, items, 2)
}
