package medialive_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChannelPlacementGroup_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/prod/clusters", map[string]any{"name": "clu-1"})
	require.Equal(t, http.StatusCreated, rec.Code)
	clusterID := decodeBody(t, rec.Body.Bytes())["id"].(string)

	base := "/prod/clusters/" + clusterID + "/channelplacementgroups"
	rec = doRequest(t, h, http.MethodPost, base, map[string]any{
		"name": "cpg-1", "nodes": []string{"node-a"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	created := decodeBody(t, rec.Body.Bytes())
	groupID := created["id"].(string)
	assert.Equal(t, "UNASSIGNED", created["state"])
	assert.Equal(t, clusterID, created["clusterId"])

	rec = doRequest(t, h, http.MethodGet, base+"/"+groupID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodPut, base+"/"+groupID, map[string]any{"name": "cpg-upd"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "cpg-upd", decodeBody(t, rec.Body.Bytes())["name"])

	rec = doRequest(t, h, http.MethodGet, base, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Len(t, decodeBody(t, rec.Body.Bytes())["channelPlacementGroups"].([]any), 1)

	rec = doRequest(t, h, http.MethodDelete, base+"/"+groupID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodGet, base+"/"+groupID, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestChannelPlacementGroup_ClusterNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name: "create under missing cluster", method: http.MethodPost,
			path: "/prod/clusters/missing/channelplacementgroups",
			body: map[string]any{"name": "x"}, wantCode: http.StatusNotFound,
		},
		{
			name: "list under missing cluster", method: http.MethodGet,
			path: "/prod/clusters/missing/channelplacementgroups", wantCode: http.StatusNotFound,
		},
		{
			name: "describe missing group", method: http.MethodGet,
			path: "/prod/clusters/missing/channelplacementgroups/g1", wantCode: http.StatusNotFound,
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
