package sagemaker_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ClusterLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create.
	rec := doSageMakerRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName": "my-cluster",
		"InstanceGroups": []map[string]any{
			{
				"InstanceGroupName": "worker-group",
				"InstanceType":      "ml.m5.xlarge",
				"InstanceCount":     2,
				"ExecutionRole":     "arn:aws:iam::000000000000:role/HyperPodRole",
			},
		},
		"NodeRecovery": "Automatic",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	clusterArn := createResp["ClusterArn"]
	assert.Contains(t, clusterArn, "arn:aws:sagemaker")
	assert.Contains(t, clusterArn, "cluster/my-cluster")

	// Describe.
	rec = doSageMakerRequest(t, h, "DescribeCluster", map[string]any{"ClusterName": "my-cluster"})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "my-cluster", descResp["ClusterName"])
	assert.Equal(t, "InService", descResp["ClusterStatus"])
	assert.Equal(t, "Automatic", descResp["NodeRecovery"])
	assert.NotEmpty(t, descResp["CreationTime"])

	groups, ok := descResp["InstanceGroups"].([]any)
	require.True(t, ok)
	require.Len(t, groups, 1)

	group, ok := groups[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "worker-group", group["InstanceGroupName"])
	assert.InDelta(t, 2, group["CurrentCount"], 0)
	assert.InDelta(t, 2, group["TargetCount"], 0)
	assert.Equal(t, "InService", group["Status"])

	// List.
	rec = doSageMakerRequest(t, h, "ListClusters", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	summaries, ok := listResp["ClusterSummaries"].([]any)
	require.True(t, ok)
	assert.Len(t, summaries, 1)

	// ListClusterNodes: two nodes should have been auto-provisioned.
	rec = doSageMakerRequest(t, h, "ListClusterNodes", map[string]any{"ClusterName": "my-cluster"})
	require.Equal(t, http.StatusOK, rec.Code)

	var nodesResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &nodesResp))
	nodeSummaries, ok := nodesResp["ClusterNodeSummaries"].([]any)
	require.True(t, ok)
	require.Len(t, nodeSummaries, 2)

	firstNode, ok := nodeSummaries[0].(map[string]any)
	require.True(t, ok)
	nodeID, ok := firstNode["InstanceId"].(string)
	require.True(t, ok)
	assert.Equal(t, "worker-group", firstNode["InstanceGroupName"])

	// DescribeClusterNode.
	rec = doSageMakerRequest(t, h, "DescribeClusterNode", map[string]any{
		"ClusterName": "my-cluster",
		"NodeId":      nodeID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var nodeResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &nodeResp))
	nodeDetails, ok := nodeResp["NodeDetails"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, nodeID, nodeDetails["InstanceId"])
	assert.Equal(t, "ml.m5.xlarge", nodeDetails["InstanceType"])

	// UpdateCluster: grow the instance group to 3 nodes and switch node recovery off.
	rec = doSageMakerRequest(t, h, "UpdateCluster", map[string]any{
		"ClusterName": "my-cluster",
		"InstanceGroups": []map[string]any{
			{"InstanceGroupName": "worker-group", "InstanceCount": 3},
		},
		"NodeRecovery": "None",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeCluster", map[string]any{"ClusterName": "my-cluster"})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "None", descResp["NodeRecovery"])
	groups, ok = descResp["InstanceGroups"].([]any)
	require.True(t, ok)
	group, ok = groups[0].(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, 3, group["CurrentCount"], 0)

	// UpdateClusterSoftware.
	rec = doSageMakerRequest(t, h, "UpdateClusterSoftware", map[string]any{"ClusterName": "my-cluster"})
	require.Equal(t, http.StatusOK, rec.Code)

	var softwareResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &softwareResp))
	assert.Equal(t, clusterArn, softwareResp["ClusterArn"])

	// ListClusterEvents / DescribeClusterEvent: correct empty shape, no events ever exist.
	rec = doSageMakerRequest(t, h, "ListClusterEvents", map[string]any{"ClusterName": "my-cluster"})
	require.Equal(t, http.StatusOK, rec.Code)

	var eventsResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &eventsResp))
	events, ok := eventsResp["Events"].([]any)
	require.True(t, ok)
	assert.Empty(t, events)

	rec = doSageMakerRequest(t, h, "DescribeClusterEvent", map[string]any{
		"ClusterName": "my-cluster",
		"EventId":     "evt-1",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// AttachClusterNodeVolume then DetachClusterNodeVolume round-trip.
	rec = doSageMakerRequest(t, h, "AttachClusterNodeVolume", map[string]any{
		"ClusterName":  "my-cluster",
		"NodeId":       nodeID,
		"VolumeConfig": map[string]any{"VolumeName": "vol-123", "SizeInGB": 100},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DetachClusterNodeVolume", map[string]any{
		"ClusterArn": clusterArn,
		"NodeId":     nodeID,
		"VolumeId":   "vol-123",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var detachResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &detachResp))
	assert.Equal(t, clusterArn, detachResp["ClusterArn"])
	assert.Equal(t, nodeID, detachResp["NodeId"])
	assert.Equal(t, "vol-123", detachResp["VolumeId"])
	assert.Equal(t, "detached", detachResp["Status"])
	assert.NotEmpty(t, detachResp["AttachTime"])

	// Detaching again fails: the volume is gone.
	rec = doSageMakerRequest(t, h, "DetachClusterNodeVolume", map[string]any{
		"ClusterArn": clusterArn,
		"NodeId":     nodeID,
		"VolumeId":   "vol-123",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Delete.
	rec = doSageMakerRequest(t, h, "DeleteCluster", map[string]any{"ClusterName": "my-cluster"})
	require.Equal(t, http.StatusOK, rec.Code)

	var deleteResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &deleteResp))
	assert.Equal(t, clusterArn, deleteResp["ClusterArn"])

	// Verify deleted.
	rec = doSageMakerRequest(t, h, "DescribeCluster", map[string]any{"ClusterName": "my-cluster"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreateCluster_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{"ClusterName": "dup-cluster"}

	rec := doSageMakerRequest(t, h, "CreateCluster", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSageMakerRequest(t, h, "CreateCluster", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestHandler_CreateCluster_MissingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateCluster", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DescribeCluster_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DescribeCluster", map[string]any{"ClusterName": "nonexistent"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DeleteCluster_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DeleteCluster", map[string]any{"ClusterName": "nonexistent"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdateCluster_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateCluster", map[string]any{"ClusterName": "nonexistent"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdateCluster_DeleteInstanceGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName": "shrink-cluster",
		"InstanceGroups": []map[string]any{
			{"InstanceGroupName": "group-a", "InstanceCount": 2},
			{"InstanceGroupName": "group-b", "InstanceCount": 1},
		},
	})

	rec := doSageMakerRequest(t, h, "UpdateCluster", map[string]any{
		"ClusterName":            "shrink-cluster",
		"InstanceGroupsToDelete": []string{"group-b"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeCluster", map[string]any{"ClusterName": "shrink-cluster"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	groups, ok := resp["InstanceGroups"].([]any)
	require.True(t, ok)
	assert.Len(t, groups, 1)

	rec = doSageMakerRequest(t, h, "ListClusterNodes", map[string]any{"ClusterName": "shrink-cluster"})
	require.Equal(t, http.StatusOK, rec.Code)

	var nodesResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &nodesResp))
	nodes, ok := nodesResp["ClusterNodeSummaries"].([]any)
	require.True(t, ok)
	assert.Len(t, nodes, 2) // only group-a's 2 nodes remain
}

func TestHandler_DescribeClusterNode_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateCluster", map[string]any{"ClusterName": "node-cluster"})

	rec := doSageMakerRequest(t, h, "DescribeClusterNode", map[string]any{
		"ClusterName": "node-cluster",
		"NodeId":      "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListClusterNodes_ClusterNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListClusterNodes", map[string]any{"ClusterName": "nonexistent"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListClusterEvents_ClusterNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListClusterEvents", map[string]any{"ClusterName": "nonexistent"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DetachClusterNodeVolume_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "missing ClusterArn",
			body: map[string]any{"NodeId": "node-1", "VolumeId": "vol-1"},
		},
		{
			name: "missing NodeId",
			body: map[string]any{
				"ClusterArn": "arn:aws:sagemaker:us-east-1:000000000000:cluster/x",
				"VolumeId":   "vol-1",
			},
		},
		{
			name: "missing VolumeId",
			body: map[string]any{
				"ClusterArn": "arn:aws:sagemaker:us-east-1:000000000000:cluster/x",
				"NodeId":     "node-1",
			},
		},
		{
			name: "cluster not found",
			body: map[string]any{
				"ClusterArn": "arn:aws:sagemaker:us-east-1:000000000000:cluster/nonexistent",
				"NodeId":     "node-1",
				"VolumeId":   "vol-1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doSageMakerRequest(t, h, "DetachClusterNodeVolume", tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_ListClusters_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 5 {
		doSageMakerRequest(t, h, "CreateCluster", map[string]any{
			"ClusterName": fmt.Sprintf("cluster-%d", i),
		})
	}

	seen := map[string]bool{}
	nextToken := ""

	for {
		body := map[string]any{}
		if nextToken != "" {
			body["NextToken"] = nextToken
		}

		rec := doSageMakerRequest(t, h, "ListClusters", body)
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		summaries, ok := resp["ClusterSummaries"].([]any)
		require.True(t, ok)

		for _, s := range summaries {
			m, mapOK := s.(map[string]any)
			require.True(t, mapOK)
			name, nameOK := m["ClusterName"].(string)
			require.True(t, nameOK)
			seen[name] = true
		}

		token, _ := resp["NextToken"].(string)
		if token == "" {
			break
		}

		nextToken = token
	}

	assert.Len(t, seen, 5)
}

func TestHandler_ListClusters_NameContains(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateCluster", map[string]any{"ClusterName": "prod-cluster"})
	doSageMakerRequest(t, h, "CreateCluster", map[string]any{"ClusterName": "dev-cluster"})

	rec := doSageMakerRequest(t, h, "ListClusters", map[string]any{"NameContains": "prod"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	summaries, ok := resp["ClusterSummaries"].([]any)
	require.True(t, ok)
	require.Len(t, summaries, 1)

	m, ok := summaries[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "prod-cluster", m["ClusterName"])
}
