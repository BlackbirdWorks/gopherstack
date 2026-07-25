package medialive_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAnywhereSettings_ChannelClusterPlacementGroupAssociation locks in the
// fix for a family of related gaps: gopherstack previously tracked NO
// Channel<->Cluster/ChannelPlacementGroup association at all (Channel had no
// "anywhereSettings" field), so Cluster.ChannelIds, ChannelPlacementGroup.
// Channels, and Node.ChannelPlacementGroups (all real fields on the
// respective Describe/List responses -- verified against
// aws-sdk-go-v2/service/medialive's DescribeClusterOutput/
// DescribeChannelPlacementGroupOutput/DescribeNodeOutput) were always
// hardcoded to empty lists regardless of what a caller configured. This
// exercises the full chain: a Channel's CreateChannel "anywhereSettings"
// (clusterId/channelPlacementGroupId) now derives real values for all three.
func TestAnywhereSettings_ChannelClusterPlacementGroupAssociation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Cluster with networkSettings.
	rec := doRequest(t, h, http.MethodPost, "/prod/clusters", map[string]any{
		"name":        "anywhere-cluster",
		"clusterType": "ON_PREMISES",
		"networkSettings": map[string]any{
			"defaultRoute": "my-if",
			"interfaceMappings": []map[string]any{
				{"logicalInterfaceName": "my-if", "networkId": "net-1"},
			},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	cluster := decodeBody(t, rec.Body.Bytes())
	clusterID := cluster["id"].(string)

	ns := cluster["networkSettings"].(map[string]any)
	assert.Equal(t, "my-if", ns["defaultRoute"])
	mappings := ns["interfaceMappings"].([]any)
	require.Len(t, mappings, 1)
	mapping := mappings[0].(map[string]any)
	assert.Equal(t, "my-if", mapping["logicalInterfaceName"])
	assert.Equal(t, "net-1", mapping["networkId"])
	assert.Equal(t, []any{}, cluster["channelIds"], "no channels attached yet")

	// Node within the cluster.
	nodeBase := "/prod/clusters/" + clusterID + "/nodes"
	rec = doRequest(t, h, http.MethodPost, nodeBase, map[string]any{"name": "node-1"})
	require.Equal(t, http.StatusCreated, rec.Code)
	nodeID := decodeBody(t, rec.Body.Bytes())["id"].(string)

	// ChannelPlacementGroup attaching the node.
	cpgBase := "/prod/clusters/" + clusterID + "/channelplacementgroups"
	rec = doRequest(t, h, http.MethodPost, cpgBase, map[string]any{
		"name": "cpg-1", "nodes": []string{nodeID},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	cpgID := decodeBody(t, rec.Body.Bytes())["id"].(string)

	// Node should now report the CPG it belongs to.
	rec = doRequest(t, h, http.MethodGet, nodeBase+"/"+nodeID, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	nodeBody := decodeBody(t, rec.Body.Bytes())
	assert.Equal(t, []any{cpgID}, nodeBody["channelPlacementGroups"])

	// Channel with anywhereSettings pointing at the cluster + CPG.
	rec = doRequest(t, h, http.MethodPost, "/prod/channels", map[string]any{
		"name":         "anywhere-channel",
		"channelClass": "SINGLE_PIPELINE",
		"anywhereSettings": map[string]any{
			"clusterId":               clusterID,
			"channelPlacementGroupId": cpgID,
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	channelResp := decodeBody(t, rec.Body.Bytes())
	channel := channelResp["channel"].(map[string]any)
	channelID := channel["id"].(string)
	as := channel["anywhereSettings"].(map[string]any)
	assert.Equal(t, clusterID, as["clusterId"])
	assert.Equal(t, cpgID, as["channelPlacementGroupId"])

	// Cluster.channelIds should now include the channel.
	rec = doRequest(t, h, http.MethodGet, "/prod/clusters/"+clusterID, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, []any{channelID}, decodeBody(t, rec.Body.Bytes())["channelIds"])

	// ChannelPlacementGroup.channels should now include the channel.
	rec = doRequest(t, h, http.MethodGet, cpgBase+"/"+cpgID, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, []any{channelID}, decodeBody(t, rec.Body.Bytes())["channels"])
}

// TestAnywhereSettings_UnknownClusterRejected verifies CreateChannel/
// UpdateChannel validate a caller-supplied anywhereSettings.clusterId
// against real Cluster state instead of accepting (and silently storing) a
// reference to a cluster that doesn't exist.
func TestAnywhereSettings_UnknownClusterRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/prod/channels", map[string]any{
		"name":             "bad-channel",
		"channelClass":     "STANDARD",
		"anywhereSettings": map[string]any{"clusterId": "does-not-exist"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	channelID := createTestChannel(t, h)
	rec = doRequest(t, h, http.MethodPut, "/prod/channels/"+channelID, map[string]any{
		"anywhereSettings": map[string]any{"clusterId": "still-missing"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestCluster_UpdateNetworkSettingsPreservedWhenOmitted verifies
// UpdateCluster only overwrites networkSettings when the caller includes
// the key -- mirroring UpdateClusterInput's "include this parameter only if
// you want to change it" semantics (verified against the real
// UpdateClusterInput doc comment on NetworkSettings).
func TestCluster_UpdateNetworkSettingsPreservedWhenOmitted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/prod/clusters", map[string]any{
		"name": "keep-ns",
		"networkSettings": map[string]any{
			"defaultRoute": "if-a",
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	clusterID := decodeBody(t, rec.Body.Bytes())["id"].(string)

	// Update name only -- networkSettings key omitted entirely.
	rec = doRequest(t, h, http.MethodPut, "/prod/clusters/"+clusterID, map[string]any{
		"name": "renamed",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	updated := decodeBody(t, rec.Body.Bytes())
	ns := updated["networkSettings"].(map[string]any)
	assert.Equal(t, "if-a", ns["defaultRoute"], "networkSettings must survive an update that omits it")
}
