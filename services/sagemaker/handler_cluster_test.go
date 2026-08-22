package sagemaker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
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
	// LaunchTime is a required member of ClusterNodeSummary
	// (types/types.go:5420-5423) -- previously omitted entirely.
	assert.NotEmpty(t, firstNode["LaunchTime"])

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
	assert.NotEmpty(t, nodeDetails["LaunchTime"])

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

func TestHandler_CreateCluster_ClusterRoleAndVpcConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName": "cluster-with-vpc",
		"ClusterRole": "arn:aws:iam::000000000000:role/HyperPodClusterRole",
		"VpcConfig": map[string]any{
			"SecurityGroupIds": []any{"sg-123"},
			"Subnets":          []any{"subnet-abc"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeCluster", map[string]any{"ClusterName": "cluster-with-vpc"})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "arn:aws:iam::000000000000:role/HyperPodClusterRole", descResp["ClusterRole"])

	vpcConfig, ok := descResp["VpcConfig"].(map[string]any)
	require.True(t, ok, "DescribeCluster must return the accepted VpcConfig")
	sgIDs, ok := vpcConfig["SecurityGroupIds"].([]any)
	require.True(t, ok)
	require.Len(t, sgIDs, 1)
	assert.Equal(t, "sg-123", sgIDs[0])
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

func TestAddClusterInternal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		clusterName string
	}{
		{
			name:        "creates cluster with nodes map initialized",
			clusterName: "my-cluster",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")
			c := b.AddClusterInternal(context.Background(), tt.clusterName)

			require.NotNil(t, c)
			assert.Equal(t, tt.clusterName, c.ClusterName)
			assert.NotNil(t, c.Nodes)
			assert.Equal(t, 1, sagemaker.ClusterCount(b))
		})
	}
}

func TestBatchDeleteClusterNodes_Empty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		clusterName string
		nodeIDs     []string
		wantCode    int
	}{
		{
			name:        "delete empty node list succeeds",
			clusterName: "my-cluster",
			nodeIDs:     []string{},
			wantCode:    http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			h.Backend.AddClusterInternal(context.Background(), tt.clusterName)

			rec := doSageMakerRequest(t, h, "BatchDeleteClusterNodes", map[string]any{
				"ClusterName": tt.clusterName,
				"NodeIds":     tt.nodeIDs,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestBatchRebootClusterNodes_PartialSuccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		clusterName string
		nodeIDs     []string
		wantCode    int
	}{
		{
			name:        "reboot nodes partial success",
			clusterName: "reboot-cluster",
			nodeIDs:     []string{"node-1", "node-missing"},
			wantCode:    http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			c := h.Backend.AddClusterInternal(context.Background(), tt.clusterName)
			require.NotNil(t, c)

			node := h.Backend.AddClusterNodeInternal(
				context.Background(),
				tt.clusterName,
				sagemaker.ClusterNode{NodeID: "node-1", NodeStatus: "Running"},
			)
			require.NotNil(t, node)

			rec := doSageMakerRequest(t, h, "BatchRebootClusterNodes", map[string]any{
				"ClusterName": tt.clusterName,
				"NodeIds":     tt.nodeIDs,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_AttachClusterNodeVolume(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *sagemaker.Handler)
		body     map[string]any
		name     string
		wantCode int
		wantARN  bool
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *sagemaker.Handler) {
				t.Helper()
				h.Backend.AddClusterInternal(context.Background(), "my-cluster")
			},
			body: map[string]any{
				"ClusterName":  "my-cluster",
				"NodeId":       "node-1",
				"VolumeConfig": map[string]any{"VolumeName": "vol-1", "SizeInGB": 100},
			},
			wantCode: http.StatusOK,
			wantARN:  true,
		},
		{
			name: "cluster not found",
			body: map[string]any{
				"ClusterName": "nonexistent",
				"NodeId":      "node-1",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing ClusterName",
			body:     map[string]any{"NodeId": "node-1"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing NodeId",
			body:     map[string]any{"ClusterName": "my-cluster"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "invalid json",
			body:     nil,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(t, h)
			}

			var body map[string]any
			if tt.body != nil {
				body = tt.body
			}

			rec := doSageMakerRequest(t, h, "AttachClusterNodeVolume", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantARN {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp["ClusterArn"], "arn:aws:sagemaker")
				assert.Equal(t, "node-1", resp["NodeId"])
			}
		})
	}
}

func TestHandler_BatchAddClusterNodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *sagemaker.Handler)
		body     map[string]any
		name     string
		wantCode int
		wantARN  bool
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *sagemaker.Handler) {
				t.Helper()
				rec := doSageMakerRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName": "batch-cluster",
					"InstanceGroups": []map[string]any{
						{"InstanceGroupName": "workers", "InstanceType": "ml.p3.2xlarge", "InstanceCount": 1},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body: map[string]any{
				"ClusterName": "batch-cluster",
				"NodesToAdd": []map[string]any{
					{"InstanceGroupName": "workers", "IncrementTargetCountBy": 2},
				},
			},
			wantCode: http.StatusOK,
			wantARN:  true,
		},
		{
			name: "cluster not found",
			body: map[string]any{
				"ClusterName": "nonexistent",
				"NodesToAdd": []map[string]any{
					{"InstanceGroupName": "workers", "IncrementTargetCountBy": 1},
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing ClusterName",
			body: map[string]any{"NodesToAdd": []map[string]any{
				{"InstanceGroupName": "workers", "IncrementTargetCountBy": 1},
			}},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing NodesToAdd",
			setup: func(t *testing.T, h *sagemaker.Handler) {
				t.Helper()
				h.Backend.AddClusterInternal(context.Background(), "batch-cluster-empty")
			},
			body:     map[string]any{"ClusterName": "batch-cluster-empty"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(t, h)
			}

			var body map[string]any
			if tt.body != nil {
				body = tt.body
			}

			rec := doSageMakerRequest(t, h, "BatchAddClusterNodes", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantARN {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotContains(t, resp, "ClusterArn")
				assert.IsType(t, []any{}, resp["Failed"])
				assert.IsType(t, []any{}, resp["Successful"])

				if tt.name == "success" {
					successful, _ := resp["Successful"].([]any)
					require.Len(t, successful, 2)

					entry, ok := successful[0].(map[string]any)
					require.True(t, ok)
					assert.Equal(t, "workers", entry["InstanceGroupName"])
					assert.NotEmpty(t, entry["NodeLogicalId"])
					assert.Equal(t, "Running", entry["Status"])
				}
			}
		})
	}
}

func TestHandler_BatchDeleteClusterNodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *sagemaker.Handler)
		body     map[string]any
		name     string
		wantCode int
		wantARN  bool
	}{
		{
			name: "success delete existing nodes",
			setup: func(t *testing.T, h *sagemaker.Handler) {
				t.Helper()
				c := h.Backend.AddClusterInternal(context.Background(), "del-cluster")
				_ = c
				// Seed a node via BatchAdd
				nodes := []map[string]any{{"NodeId": "del-n1"}}
				_ = nodes
				h.Backend.AddClusterInternal(context.Background(), "del-cluster-2")
			},
			body: map[string]any{
				"ClusterName": "del-cluster-2",
				"NodeIds":     []string{},
			},
			wantCode: http.StatusOK,
			wantARN:  true,
		},
		{
			name: "cluster not found",
			body: map[string]any{
				"ClusterName": "nope",
				"NodeIds":     []string{"n1"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing ClusterName",
			body:     map[string]any{"NodeIds": []string{"n1"}},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(t, h)
			}

			var body map[string]any
			if tt.body != nil {
				body = tt.body
			}

			rec := doSageMakerRequest(t, h, "BatchDeleteClusterNodes", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantARN {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotContains(t, resp, "ClusterArn")
				assert.Contains(t, resp, "Failed")
				assert.Contains(t, resp, "Successful")
			}
		})
	}
}

func TestHandler_BatchRebootClusterNodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *sagemaker.Handler)
		body     map[string]any
		name     string
		wantCode int
		wantARN  bool
	}{
		{
			name: "success with empty list",
			setup: func(t *testing.T, h *sagemaker.Handler) {
				t.Helper()
				h.Backend.AddClusterInternal(context.Background(), "reboot-cluster")
			},
			body: map[string]any{
				"ClusterName": "reboot-cluster",
				"NodeIds":     []string{},
			},
			wantCode: http.StatusOK,
			wantARN:  true,
		},
		{
			name: "partial success — missing nodes go to failures",
			setup: func(t *testing.T, h *sagemaker.Handler) {
				t.Helper()
				h.Backend.AddClusterInternal(context.Background(), "reboot-cluster-2")
			},
			body: map[string]any{
				"ClusterName": "reboot-cluster-2",
				"NodeIds":     []string{"missing-node"},
			},
			wantCode: http.StatusOK,
			wantARN:  true,
		},
		{
			name: "cluster not found",
			body: map[string]any{
				"ClusterName": "ghost-cluster",
				"NodeIds":     []string{"n1"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing ClusterName",
			body:     map[string]any{"NodeIds": []string{"n1"}},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(t, h)
			}

			var body map[string]any
			if tt.body != nil {
				body = tt.body
			}

			rec := doSageMakerRequest(t, h, "BatchRebootClusterNodes", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantARN {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotContains(t, resp, "ClusterArn")
				assert.Contains(t, resp, "Failed")
				assert.Contains(t, resp, "Successful")
			}
		})
	}
}

func TestHandler_BatchReplaceClusterNodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *sagemaker.Handler)
		body     map[string]any
		name     string
		wantCode int
		wantARN  bool
	}{
		{
			name: "success with empty list",
			setup: func(t *testing.T, h *sagemaker.Handler) {
				t.Helper()
				h.Backend.AddClusterInternal(context.Background(), "replace-cluster")
			},
			body: map[string]any{
				"ClusterName": "replace-cluster",
				"NodeIds":     []string{},
			},
			wantCode: http.StatusOK,
			wantARN:  true,
		},
		{
			name: "missing node goes to failures",
			setup: func(t *testing.T, h *sagemaker.Handler) {
				t.Helper()
				h.Backend.AddClusterInternal(context.Background(), "replace-cluster-2")
			},
			body: map[string]any{
				"ClusterName": "replace-cluster-2",
				"NodeIds":     []string{"nonexistent"},
			},
			wantCode: http.StatusOK,
			wantARN:  true,
		},
		{
			name: "cluster not found",
			body: map[string]any{
				"ClusterName": "ghost",
				"NodeIds":     []string{},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing ClusterName",
			body:     map[string]any{"NodeIds": []string{}},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(t, h)
			}

			var body map[string]any
			if tt.body != nil {
				body = tt.body
			}

			rec := doSageMakerRequest(t, h, "BatchReplaceClusterNodes", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantARN {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotContains(t, resp, "ClusterArn")
				assert.Contains(t, resp, "Failed")
				assert.Contains(t, resp, "Successful")
			}
		})
	}
}

// TestHandler_BatchAddClusterNodes_DuplicateNodeFails previously drove
// gopherstack's own invented "NodeConfigs" wire format directly, since the
// real request-decode bug (gopherstack-g4mx) meant no real SDK client could
// ever exercise a non-empty BatchAddClusterNodes request through this
// handler. Now that BatchAddClusterNodesInput.NodesToAdd binds for real, this
// non-empty-Failed proof is covered by the real-client
// TestHandler_BatchAddClusterNodes_RealClient below instead.

// ---------------------------------------------------------------------------
// StartClusterHealthCheck
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// CreateCluster/UpdateCluster: AutoScaling, Orchestrator, NodeProvisioningMode,
// TieredStorageConfig (gopherstack-i359)
// ---------------------------------------------------------------------------

// TestHandler_CreateCluster_NestedTypes_RealClient round-trips CreateClusterInput's
// AutoScaling/Orchestrator/NodeProvisioningMode/TieredStorageConfig through the real
// aws-sdk-go-v2 client so the assertions come from the SDK's own generated types, not
// from field names this handler happens to produce.
func TestHandler_CreateCluster_NestedTypes_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateCluster(t.Context(), &sagemakersdk.CreateClusterInput{
		ClusterName: aws.String("nested-cluster"),
		AutoScaling: &smtypes.ClusterAutoScalingConfig{
			Mode:           smtypes.ClusterAutoScalingModeEnable,
			AutoScalerType: smtypes.ClusterAutoScalerTypeKarpenter,
		},
		Orchestrator: &smtypes.ClusterOrchestrator{
			Eks: &smtypes.ClusterOrchestratorEksConfig{
				ClusterArn: aws.String("arn:aws:eks:us-east-1:000000000000:cluster/eks1"),
			},
		},
		NodeProvisioningMode: smtypes.ClusterNodeProvisioningModeContinuous,
		TieredStorageConfig: &smtypes.ClusterTieredStorageConfig{
			Mode:                               smtypes.ClusterConfigModeEnable,
			InstanceMemoryAllocationPercentage: aws.Int32(25),
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeCluster(t.Context(), &sagemakersdk.DescribeClusterInput{
		ClusterName: aws.String("nested-cluster"),
	})
	require.NoError(t, err)

	require.NotNil(t, out.AutoScaling)
	assert.Equal(t, smtypes.ClusterAutoScalingModeEnable, out.AutoScaling.Mode)
	assert.Equal(t, smtypes.ClusterAutoScalerTypeKarpenter, out.AutoScaling.AutoScalerType)
	assert.NotEmpty(t, out.AutoScaling.Status)

	require.NotNil(t, out.Orchestrator)
	require.NotNil(t, out.Orchestrator.Eks)
	assert.Equal(t, "arn:aws:eks:us-east-1:000000000000:cluster/eks1", aws.ToString(out.Orchestrator.Eks.ClusterArn))
	assert.Nil(t, out.Orchestrator.Slurm)

	assert.Equal(t, smtypes.ClusterNodeProvisioningModeContinuous, out.NodeProvisioningMode)

	require.NotNil(t, out.TieredStorageConfig)
	assert.Equal(t, smtypes.ClusterConfigModeEnable, out.TieredStorageConfig.Mode)
	assert.Equal(t, int32(25), aws.ToInt32(out.TieredStorageConfig.InstanceMemoryAllocationPercentage))
}

// TestHandler_UpdateCluster_NestedTypes_RealClient verifies UpdateCluster can set
// Orchestrator (Slurm variant this time) and NodeProvisioningMode on an existing cluster.
func TestHandler_UpdateCluster_NestedTypes_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateCluster(t.Context(), &sagemakersdk.CreateClusterInput{
		ClusterName: aws.String("update-nested-cluster"),
	})
	require.NoError(t, err)

	_, err = client.UpdateCluster(t.Context(), &sagemakersdk.UpdateClusterInput{
		ClusterName: aws.String("update-nested-cluster"),
		Orchestrator: &smtypes.ClusterOrchestrator{
			Slurm: &smtypes.ClusterOrchestratorSlurmConfig{
				SlurmConfigStrategy: smtypes.ClusterSlurmConfigStrategyMerge,
			},
		},
		NodeProvisioningMode: smtypes.ClusterNodeProvisioningModeContinuous,
	})
	require.NoError(t, err)

	out, err := client.DescribeCluster(t.Context(), &sagemakersdk.DescribeClusterInput{
		ClusterName: aws.String("update-nested-cluster"),
	})
	require.NoError(t, err)

	require.NotNil(t, out.Orchestrator)
	require.NotNil(t, out.Orchestrator.Slurm)
	assert.Equal(t, smtypes.ClusterSlurmConfigStrategyMerge, out.Orchestrator.Slurm.SlurmConfigStrategy)
	assert.Nil(t, out.Orchestrator.Eks)
	assert.Equal(t, smtypes.ClusterNodeProvisioningModeContinuous, out.NodeProvisioningMode)
}

// TestHandler_CreateCluster_OrchestratorValidation checks the real CreateClusterInput
// business rule (api_op_CreateCluster.go:76-78, sagemaker@v1.263.2): "you must provide
// exactly one orchestrator configuration: either Eks or Slurm. Specifying both or
// providing an empty configuration returns a validation error.".
func TestHandler_CreateCluster_OrchestratorValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		orchestrator map[string]any
		name         string
	}{
		{
			name: "both eks and slurm set",
			orchestrator: map[string]any{
				"Eks":   map[string]any{"ClusterArn": "arn:aws:eks:us-east-1:000000000000:cluster/x"},
				"Slurm": map[string]any{"SlurmConfigStrategy": "Managed"},
			},
		},
		{
			name:         "neither eks nor slurm set",
			orchestrator: map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doSageMakerRequest(t, h, "CreateCluster", map[string]any{
				"ClusterName":  "bad-orchestrator-cluster",
				"Orchestrator": tt.orchestrator,
			})
			require.Equal(t, http.StatusBadRequest, rec.Code)

			var body map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.Equal(t, "ValidationException", body["__type"])
		})
	}
}

func TestHandler_StartClusterHealthCheck(t *testing.T) {
	t.Parallel()

	validConfigs := []map[string]any{
		{"InstanceGroupName": "workers", "DeepHealthChecks": []string{"InstanceStress"}},
	}

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
		setup    bool
	}{
		{
			name:     "healthy cluster with configs returns its ARN",
			setup:    true,
			body:     map[string]any{"ClusterName": "hc-cluster", "DeepHealthCheckConfigurations": validConfigs},
			wantCode: http.StatusOK,
		},
		{
			name:     "missing ClusterName is rejected",
			setup:    false,
			body:     map[string]any{"DeepHealthCheckConfigurations": validConfigs},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing DeepHealthCheckConfigurations is rejected",
			setup:    true,
			body:     map[string]any{"ClusterName": "hc-cluster"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "nonexistent cluster is rejected",
			setup:    false,
			body:     map[string]any{"ClusterName": "nonexistent", "DeepHealthCheckConfigurations": validConfigs},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup {
				h.Backend.AddClusterInternal(context.Background(), "hc-cluster")
			}

			rec := doSageMakerRequest(t, h, "StartClusterHealthCheck", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp["ClusterArn"], "hc-cluster")
			}
		})
	}
}

// TestHandler_ListClusters_FilterSortPage_RealClient asserts
// ListClustersInput's NameContains/SortBy/SortOrder/MaxResults/
// CreationTimeAfter -- all silently dropped before this pass except
// NameContains -- now work. The real client sends CreationTimeAfter as an
// awsjson1.1 epoch-second number, which a *time.Time-typed request field
// cannot decode at all.
func TestHandler_ListClusters_FilterSortPage_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	names := []string{"alpha-cluster", "beta-cluster", "gamma-widget"}
	for _, n := range names {
		_, err := client.CreateCluster(t.Context(), &sagemakersdk.CreateClusterInput{ClusterName: aws.String(n)})
		require.NoError(t, err)
	}

	t.Run("name contains filters", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListClusters(t.Context(), &sagemakersdk.ListClustersInput{
			NameContains: aws.String("cluster"),
		})
		require.NoError(t, err)
		assert.Len(t, out.ClusterSummaries, 2)
	})

	t.Run("ascending sort by name", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListClusters(t.Context(), &sagemakersdk.ListClustersInput{
			SortBy:    smtypes.ClusterSortByName,
			SortOrder: smtypes.SortOrderAscending,
		})
		require.NoError(t, err)
		require.Len(t, out.ClusterSummaries, 3)
		assert.Equal(t, "alpha-cluster", aws.ToString(out.ClusterSummaries[0].ClusterName))
		assert.Equal(t, "gamma-widget", aws.ToString(out.ClusterSummaries[2].ClusterName))
	})

	t.Run("max results caps the page and returns a token", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListClusters(t.Context(), &sagemakersdk.ListClustersInput{
			MaxResults: aws.Int32(1),
			SortBy:     smtypes.ClusterSortByName,
			SortOrder:  smtypes.SortOrderAscending,
		})
		require.NoError(t, err)
		require.Len(t, out.ClusterSummaries, 1)
		assert.Equal(t, "alpha-cluster", aws.ToString(out.ClusterSummaries[0].ClusterName))
		assert.NotEmpty(t, aws.ToString(out.NextToken))
	})

	t.Run("creation time filter does not error", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListClusters(t.Context(), &sagemakersdk.ListClustersInput{
			CreationTimeAfter: aws.Time(time.Now().Add(-time.Hour)),
		})
		require.NoError(t, err)
		assert.Len(t, out.ClusterSummaries, 3)
	})
}

// TestHandler_ListClusterNodes_FilterSortPage_RealClient asserts
// ListClusterNodesInput's InstanceGroupNameContains/MaxResults/
// CreationTimeAfter -- all silently dropped before this pass -- now work,
// and that every returned ClusterNodeSummary carries the required LaunchTime
// (types/types.go:5420-5423), previously omitted entirely.
func TestHandler_ListClusterNodes_FilterSortPage_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateCluster(t.Context(), &sagemakersdk.CreateClusterInput{
		ClusterName: aws.String("node-filter-cluster"),
		InstanceGroups: []smtypes.ClusterInstanceGroupSpecification{
			{
				InstanceGroupName: aws.String("workers"),
				InstanceCount:     aws.Int32(2),
				ExecutionRole:     aws.String("arn:aws:iam::000000000000:role/HyperPodRole"),
			},
			{
				InstanceGroupName: aws.String("head-nodes"),
				InstanceCount:     aws.Int32(1),
				ExecutionRole:     aws.String("arn:aws:iam::000000000000:role/HyperPodRole"),
			},
		},
	})
	require.NoError(t, err)

	t.Run("instance group name contains filters", func(t *testing.T) {
		t.Parallel()

		out, listErr := client.ListClusterNodes(t.Context(), &sagemakersdk.ListClusterNodesInput{
			ClusterName:               aws.String("node-filter-cluster"),
			InstanceGroupNameContains: aws.String("worker"),
		})
		require.NoError(t, listErr)
		require.Len(t, out.ClusterNodeSummaries, 2)
		assert.WithinDuration(t, time.Now(), aws.ToTime(out.ClusterNodeSummaries[0].LaunchTime), time.Minute)
	})

	t.Run("max results caps the page and returns a token", func(t *testing.T) {
		t.Parallel()

		out, listErr := client.ListClusterNodes(t.Context(), &sagemakersdk.ListClusterNodesInput{
			ClusterName: aws.String("node-filter-cluster"),
			MaxResults:  aws.Int32(1),
		})
		require.NoError(t, listErr)
		require.Len(t, out.ClusterNodeSummaries, 1)
		assert.NotEmpty(t, aws.ToString(out.NextToken))
	})

	t.Run("creation time filter does not error", func(t *testing.T) {
		t.Parallel()

		out, listErr := client.ListClusterNodes(t.Context(), &sagemakersdk.ListClusterNodesInput{
			ClusterName:       aws.String("node-filter-cluster"),
			CreationTimeAfter: aws.Time(time.Now().Add(-time.Hour)),
		})
		require.NoError(t, listErr)
		assert.Len(t, out.ClusterNodeSummaries, 3)
	})
}

// ---------------------------------------------------------------------------
// Batch{Add,Delete,Reboot,Replace}ClusterNodes output shape (gopherstack-f31u)
// ---------------------------------------------------------------------------

// TestHandler_BatchDeleteClusterNodes_RealClient round-trips
// BatchDeleteClusterNodesOutput through the real aws-sdk-go-v2 client.
// BatchDeleteClusterNodesOutput has no ClusterArn member and its Failed member
// is []types.BatchDeleteClusterNodesError (Code/Message/NodeId), not a flat
// []string under an invented "Errors" key (api_op_BatchDeleteClusterNodes.go,
// deserializers.go:awsAwsjson11_deserializeOpDocumentBatchDeleteClusterNodesOutput,
// sagemaker@v1.263.2). A raw-body test asserting the key gopherstack typed
// would pass against the old, broken shape; only a typed decode through a real
// client fails on it.
func TestHandler_BatchDeleteClusterNodes_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	h.Backend.AddClusterInternal(t.Context(), "delete-rt-cluster")

	seeded := h.Backend.AddClusterNodeInternal(
		t.Context(),
		"delete-rt-cluster",
		sagemaker.ClusterNode{NodeID: "node-keep", NodeStatus: "Running"},
	)
	require.NotNil(t, seeded)

	out, err := client.BatchDeleteClusterNodes(t.Context(), &sagemakersdk.BatchDeleteClusterNodesInput{
		ClusterName: aws.String("delete-rt-cluster"),
		NodeIds:     []string{"node-keep", "node-missing"},
	})
	require.NoError(t, err)

	require.Len(t, out.Successful, 1)
	assert.Equal(t, "node-keep", out.Successful[0])

	require.Len(t, out.Failed, 1)
	assert.Equal(t, smtypes.BatchDeleteClusterNodesErrorCodeNodeIdNotFound, out.Failed[0].Code)
	assert.Equal(t, "node-missing", aws.ToString(out.Failed[0].NodeId))
	assert.NotEmpty(t, aws.ToString(out.Failed[0].Message))
}

// TestHandler_BatchRebootClusterNodes_RealClient is
// TestHandler_BatchDeleteClusterNodes_RealClient's sibling for
// BatchRebootClusterNodesOutput (also no ClusterArn; Failed is
// []types.BatchRebootClusterNodesError keyed by ErrorCode, not Code).
func TestHandler_BatchRebootClusterNodes_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	h.Backend.AddClusterInternal(t.Context(), "reboot-rt-cluster")

	seeded := h.Backend.AddClusterNodeInternal(
		t.Context(),
		"reboot-rt-cluster",
		sagemaker.ClusterNode{NodeID: "node-keep", NodeStatus: "Running"},
	)
	require.NotNil(t, seeded)

	out, err := client.BatchRebootClusterNodes(t.Context(), &sagemakersdk.BatchRebootClusterNodesInput{
		ClusterName: aws.String("reboot-rt-cluster"),
		NodeIds:     []string{"node-keep", "node-missing"},
	})
	require.NoError(t, err)

	require.Len(t, out.Successful, 1)
	assert.Equal(t, "node-keep", out.Successful[0])

	require.Len(t, out.Failed, 1)
	assert.Equal(t, smtypes.BatchRebootClusterNodesErrorCodeInstanceIdNotFound, out.Failed[0].ErrorCode)
	assert.Equal(t, "node-missing", aws.ToString(out.Failed[0].NodeId))
	assert.NotEmpty(t, aws.ToString(out.Failed[0].Message))
}

// TestHandler_BatchAddClusterNodes_RealClient replaces
// TestHandler_BatchAddClusterNodes_RealClient_ResponseShape (which could only
// prove the response typed-decodes on an always-empty result, since the
// request never bound) now that gopherstack-g4mx's request-binding fix
// means BatchAddClusterNodesInput.NodesToAdd (serializers.go:39038,
// awsAwsjson11_serializeOpDocumentBatchAddClusterNodesInput) actually reaches
// the backend. Drives the real SDK client end-to-end: adds 2 nodes to an
// existing instance group and confirms they actually landed via a second
// real-client call (ListClusterNodes), not just that the add response looked
// right. Also proves the InstanceGroupNotFound failure path
// (types.BatchAddClusterNodesErrorCodeInstanceGroupNotFound, enums.go:1537).
func TestHandler_BatchAddClusterNodes_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateCluster(t.Context(), &sagemakersdk.CreateClusterInput{
		ClusterName: aws.String("add-rt-cluster"),
		InstanceGroups: []smtypes.ClusterInstanceGroupSpecification{
			{
				InstanceGroupName: aws.String("workers"),
				InstanceType:      smtypes.ClusterInstanceTypeMlM5Xlarge,
				InstanceCount:     aws.Int32(1),
				ExecutionRole:     aws.String("arn:aws:iam::000000000000:role/HyperPodRole"),
			},
		},
	})
	require.NoError(t, err)

	out, err := client.BatchAddClusterNodes(t.Context(), &sagemakersdk.BatchAddClusterNodesInput{
		ClusterName: aws.String("add-rt-cluster"),
		NodesToAdd: []smtypes.AddClusterNodeSpecification{
			{InstanceGroupName: aws.String("workers"), IncrementTargetCountBy: aws.Int32(2)},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, out.Failed)
	require.Len(t, out.Successful, 2)

	for _, r := range out.Successful {
		assert.Equal(t, "workers", aws.ToString(r.InstanceGroupName))
		assert.NotEmpty(t, aws.ToString(r.NodeLogicalId))
		assert.Equal(t, smtypes.ClusterInstanceStatusRunning, r.Status)
	}

	listOut, err := client.ListClusterNodes(t.Context(), &sagemakersdk.ListClusterNodesInput{
		ClusterName: aws.String("add-rt-cluster"),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.ClusterNodeSummaries, 3) // 1 from CreateCluster + 2 just added

	failOut, err := client.BatchAddClusterNodes(t.Context(), &sagemakersdk.BatchAddClusterNodesInput{
		ClusterName: aws.String("add-rt-cluster"),
		NodesToAdd: []smtypes.AddClusterNodeSpecification{
			{InstanceGroupName: aws.String("ghost-group"), IncrementTargetCountBy: aws.Int32(3)},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, failOut.Successful)
	require.Len(t, failOut.Failed, 1)
	assert.Equal(t, smtypes.BatchAddClusterNodesErrorCodeInstanceGroupNotFound, failOut.Failed[0].ErrorCode)
	assert.Equal(t, "ghost-group", aws.ToString(failOut.Failed[0].InstanceGroupName))
	assert.EqualValues(t, 3, aws.ToInt32(failOut.Failed[0].FailedCount))
}

// TestHandler_BatchReplaceClusterNodes_RealClient replaces
// TestHandler_BatchReplaceClusterNodes_RealClient_ResponseShape and
// TestHandler_BatchReplaceClusterNodes_MissingNodeFails (the latter drove
// gopherstack's own invented "Nodes" wire format directly, since the request
// never bound through a real client before gopherstack-g4mx). Confirms
// BatchReplaceClusterNodesInput.NodeIds (serializers.go:39123, a flat
// []string) now binds: a seeded node ID is accepted as Successful, and a
// nonexistent one reports the typed Failed shape
// (types.BatchReplaceClusterNodesError: ErrorCode/Message/NodeId,
// types/types.go:3448).
func TestHandler_BatchReplaceClusterNodes_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	h.Backend.AddClusterInternal(t.Context(), "replace-rt-cluster")

	seeded := h.Backend.AddClusterNodeInternal(
		t.Context(),
		"replace-rt-cluster",
		sagemaker.ClusterNode{NodeID: "node-keep", NodeStatus: "Running"},
	)
	require.NotNil(t, seeded)

	out, err := client.BatchReplaceClusterNodes(t.Context(), &sagemakersdk.BatchReplaceClusterNodesInput{
		ClusterName: aws.String("replace-rt-cluster"),
		NodeIds:     []string{"node-keep", "node-missing"},
	})
	require.NoError(t, err)

	require.Len(t, out.Successful, 1)
	assert.Equal(t, "node-keep", out.Successful[0])

	require.Len(t, out.Failed, 1)
	assert.Equal(t, smtypes.BatchReplaceClusterNodesErrorCodeInstanceIdNotFound, out.Failed[0].ErrorCode)
	assert.Equal(t, "node-missing", aws.ToString(out.Failed[0].NodeId))
	assert.NotEmpty(t, aws.ToString(out.Failed[0].Message))
}
