package medialive_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/medialive"
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

// TestChannelPlacementGroup_CascadeDeletedWithCluster locks in a leak fix:
// ChannelPlacementGroup lives in its own top-level table keyed by
// "clusterID/groupID" (unlike Nodes, which are embedded directly in
// storedCluster.Nodes and so vanish automatically with their parent
// Cluster). Before this fix, DeleteCluster never removed the Cluster's
// ChannelPlacementGroups, leaving orphaned rows (and their b.tags entries)
// referencing a Cluster ID that no longer exists.
func TestChannelPlacementGroup_CascadeDeletedWithCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	backend := h.Backend.(*medialive.InMemoryBackend)

	rec := doRequest(t, h, http.MethodPost, "/prod/clusters", map[string]any{"name": "cascade-clu"})
	require.Equal(t, http.StatusCreated, rec.Code)
	cluster := decodeBody(t, rec.Body.Bytes())
	clusterID := cluster["id"].(string)

	base := "/prod/clusters/" + clusterID + "/channelplacementgroups"
	rec = doRequest(t, h, http.MethodPost, base, map[string]any{"name": "cascade-cpg"})
	require.Equal(t, http.StatusCreated, rec.Code)
	group := decodeBody(t, rec.Body.Bytes())
	groupID := group["id"].(string)
	groupARN := group["arn"].(string)

	rec = doRequest(t, h, http.MethodPost, "/prod/tags/"+groupARN, map[string]any{
		"tags": map[string]any{"env": "cascade-test"},
	})
	require.Equal(t, http.StatusNoContent, rec.Code)

	assert.Equal(t, 1, medialive.ChannelPlacementGroupCount(backend, clusterID))

	rec = doRequest(t, h, http.MethodDelete, "/prod/clusters/"+clusterID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	assert.Equal(t, 0, medialive.ChannelPlacementGroupCount(backend, clusterID),
		"deleting the cluster must cascade-delete its ChannelPlacementGroups")

	// Directly probing the CPG table (bypassing the cluster-existence check
	// that ListChannelPlacementGroups performs) confirms it's really gone,
	// not just unreachable via the (now 404) cluster route.
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
