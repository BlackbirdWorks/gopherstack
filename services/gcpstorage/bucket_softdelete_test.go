package gcpstorage_test

import (
	"encoding/json/v2"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

func (s *sdkEnv) rawJSON(t *testing.T, method, path string) (int, map[string]any) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, s.url+path, strings.NewReader(""))
	require.NoError(t, err)

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	out := map[string]any{}
	if len(data) > 0 {
		require.NoError(t, json.Unmarshal(data, &out, jsonx.V1()))
	}

	return resp.StatusCode, out
}

func deletedBucket(t *testing.T, name string) (*sdkEnv, *fakeClock, string) {
	t.Helper()

	e, clk := newClockedSDK(t)
	require.NoError(t, e.bucket(t, name, nil).Delete(t.Context()))

	status, list := e.rawJSON(t, http.MethodGet, "/storage/v1/b?project="+sdkProject+"&softDeleted=true")
	require.Equal(t, http.StatusOK, status)

	items, _ := list["items"].([]any)
	require.Len(t, items, 1)

	item, _ := items[0].(map[string]any)
	gen, _ := item["generation"].(string)
	require.NotEmpty(t, gen)
	assert.NotEmpty(t, item["softDeleteTime"])
	assert.NotEmpty(t, item["hardDeleteTime"])

	return e, clk, gen
}

func TestBucketSoftDeleteRestore(t *testing.T) {
	t.Parallel()

	e, clk, gen := deletedBucket(t, "soft-bkt")

	_, live := e.rawJSON(t, http.MethodGet, "/storage/v1/b?project="+sdkProject)
	assert.Empty(t, live["items"])

	clk.advance(time.Hour)

	status, got := e.rawJSON(t, http.MethodPost, "/storage/v1/b/soft-bkt/restore?generation="+url.QueryEscape(gen))
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "soft-bkt", got["name"])
	assert.NotEqual(t, gen, got["generation"])
	assert.Nil(t, got["softDeleteTime"])

	_, err := e.client.Bucket("soft-bkt").Attrs(t.Context())
	require.NoError(t, err)

	status, _ = e.rawJSON(t, http.MethodPost, "/storage/v1/b/soft-bkt/restore?generation="+url.QueryEscape(gen))
	assert.Equal(t, http.StatusNotFound, status)

	_, soft := e.rawJSON(t, http.MethodGet, "/storage/v1/b?project="+sdkProject+"&softDeleted=true")
	assert.Empty(t, soft["items"])
}

func TestBucketSoftDeleteErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		status int
	}{
		{
			name:   "get needs generation",
			method: http.MethodGet,
			path:   "?softDeleted=true",
			status: http.StatusBadRequest,
		},
		{
			name:   "get unknown",
			method: http.MethodGet,
			path:   "?softDeleted=true&generation=1",
			status: http.StatusNotFound,
		},
		{name: "get live missing", method: http.MethodGet, path: "", status: http.StatusNotFound},
		{
			name:   "bad generation",
			method: http.MethodGet,
			path:   "?softDeleted=true&generation=x",
			status: http.StatusBadRequest,
		},
		{name: "restore needs generation", method: http.MethodPost, path: "/restore", status: http.StatusBadRequest},
		{name: "restore unknown", method: http.MethodPost, path: "/restore?generation=1", status: http.StatusNotFound},
		{name: "get ok", method: http.MethodGet, path: "?softDeleted=true&generation=GEN", status: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, _, gen := deletedBucket(t, "soft-err")

			status, _ := e.rawJSON(t, tt.method, "/storage/v1/b/soft-err"+strings.ReplaceAll(tt.path, "GEN", gen))
			assert.Equal(t, tt.status, status)
		})
	}
}

func TestBucketSoftDeleteConflictAndExpiry(t *testing.T) {
	t.Parallel()

	e, clk, gen := deletedBucket(t, "soft-two")

	e.bucket(t, "soft-two", nil)

	status, _ := e.rawJSON(t, http.MethodPost, "/storage/v1/b/soft-two/restore?generation="+gen)
	assert.Equal(t, http.StatusConflict, status)

	clk.advance(week + time.Hour)
	e.backend.RunLifecycle()

	status, _ = e.rawJSON(t, http.MethodGet, "/storage/v1/b/soft-two?softDeleted=true&generation="+gen)
	assert.Equal(t, http.StatusNotFound, status)
}
