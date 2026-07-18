package medialive_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSdiSource_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/prod/sdiSources", map[string]any{
		"name": "sdi-1", "type": "SINGLE", "mode": "QUADRANT",
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	created := decodeBody(t, rec.Body.Bytes())["sdiSource"].(map[string]any)
	id := created["id"].(string)
	assert.Equal(t, "IDLE", created["state"])

	rec = doRequest(t, h, http.MethodGet, "/prod/sdiSources/"+id, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodPut, "/prod/sdiSources/"+id, map[string]any{"name": "sdi-upd"})
	require.Equal(t, http.StatusOK, rec.Code)
	got := decodeBody(t, rec.Body.Bytes())["sdiSource"].(map[string]any)
	assert.Equal(t, "sdi-upd", got["name"])

	rec = doRequest(t, h, http.MethodGet, "/prod/sdiSources", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Len(t, decodeBody(t, rec.Body.Bytes())["sdiSources"].([]any), 1)

	rec = doRequest(t, h, http.MethodDelete, "/prod/sdiSources/"+id, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/prod/sdiSources/"+id, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestSdiSource_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name: "create without name", method: http.MethodPost, path: "/prod/sdiSources",
			body: map[string]any{}, wantCode: http.StatusBadRequest,
		},
		{
			name: "describe missing", method: http.MethodGet,
			path: "/prod/sdiSources/missing", wantCode: http.StatusNotFound,
		},
		{
			name: "delete missing", method: http.MethodDelete,
			path: "/prod/sdiSources/missing", wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}
