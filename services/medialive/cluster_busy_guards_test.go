package medialive_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeleteCluster_AndNode_RejectBusyChannels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		target     string
		wantStatus int
		start      bool
	}{
		{name: "cluster_idle_channel", target: "cluster", wantStatus: http.StatusOK},
		{name: "cluster_running_channel", start: true, target: "cluster", wantStatus: http.StatusConflict},
		{name: "node_idle_channel", target: "node", wantStatus: http.StatusOK},
		{name: "node_running_channel", start: true, target: "node", wantStatus: http.StatusConflict},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, http.MethodPost, "/prod/clusters", map[string]any{
				"name": "busy-cluster", "clusterType": "ON_PREMISES",
			})
			require.Equal(t, http.StatusCreated, rec.Code)
			clusterID := decodeBody(t, rec.Body.Bytes())["id"].(string)

			nodeBase := "/prod/clusters/" + clusterID + "/nodes"
			rec = doRequest(t, h, http.MethodPost, nodeBase, map[string]any{"name": "node-1"})
			require.Equal(t, http.StatusCreated, rec.Code)
			nodeID := decodeBody(t, rec.Body.Bytes())["id"].(string)

			rec = doRequest(t, h, http.MethodPost, "/prod/clusters/"+clusterID+"/channelplacementgroups",
				map[string]any{"name": "cpg-1", "nodes": []string{nodeID}})
			require.Equal(t, http.StatusCreated, rec.Code)
			cpgID := decodeBody(t, rec.Body.Bytes())["id"].(string)

			rec = doRequest(t, h, http.MethodPost, "/prod/channels", map[string]any{
				"name": "busy-channel", "channelClass": "SINGLE_PIPELINE",
				"anywhereSettings": map[string]any{"clusterId": clusterID, "channelPlacementGroupId": cpgID},
			})
			require.Equal(t, http.StatusCreated, rec.Code)
			channelID := decodeBody(t, rec.Body.Bytes())["channel"].(map[string]any)["id"].(string)

			if tt.start {
				rec = doRequest(t, h, http.MethodPost, "/prod/channels/"+channelID+"/start", nil)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			path := "/prod/clusters/" + clusterID
			if tt.target == "node" {
				path = nodeBase + "/" + nodeID
			}

			rec = doRequest(t, h, http.MethodDelete, path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			if tt.wantStatus == http.StatusConflict {
				assert.Equal(t, "ConflictException", rec.Header().Get("X-Amzn-Errortype"))
			}
		})
	}
}

func TestUpdateNodeState_ValidatesState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		state      string
		wantStatus int
	}{
		{name: "active", state: "ACTIVE", wantStatus: http.StatusOK},
		{name: "draining", state: "DRAINING", wantStatus: http.StatusOK},
		{name: "bogus", state: "IN_USE", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, http.MethodPost, "/prod/clusters", map[string]any{
				"name": "ns-cluster", "clusterType": "ON_PREMISES",
			})
			require.Equal(t, http.StatusCreated, rec.Code)
			clusterID := decodeBody(t, rec.Body.Bytes())["id"].(string)

			rec = doRequest(t, h, http.MethodPost, "/prod/clusters/"+clusterID+"/nodes", map[string]any{"name": "n"})
			require.Equal(t, http.StatusCreated, rec.Code)
			nodeID := decodeBody(t, rec.Body.Bytes())["id"].(string)

			rec = doRequest(t, h, http.MethodPut,
				"/prod/clusters/"+clusterID+"/nodes/"+nodeID+"/state", map[string]any{"state": tt.state})
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}
