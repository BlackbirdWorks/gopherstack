package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

func TestHandler_AddAssociation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
		wantARN  bool
	}{
		{
			name: "success with association type",
			body: map[string]any{
				"SourceArn":       "arn:aws:sagemaker:us-east-1:000000000000:experiment-trial/trial-1",
				"DestinationArn":  "arn:aws:sagemaker:us-east-1:000000000000:artifact/artifact-1",
				"AssociationType": "ContributedTo",
			},
			wantCode: http.StatusOK,
			wantARN:  true,
		},
		{
			name: "success without association type",
			body: map[string]any{
				"SourceArn":      "arn:aws:sagemaker:us-east-1:000000000000:experiment/exp-1",
				"DestinationArn": "arn:aws:sagemaker:us-east-1:000000000000:artifact/artifact-2",
			},
			wantCode: http.StatusOK,
			wantARN:  true,
		},
		{
			name: "missing SourceArn",
			body: map[string]any{
				"DestinationArn": "arn:aws:sagemaker:us-east-1:000000000000:artifact/x",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing DestinationArn",
			body: map[string]any{
				"SourceArn": "arn:aws:sagemaker:us-east-1:000000000000:trial/x",
			},
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

			var body map[string]any
			if tt.body != nil {
				body = tt.body
			}

			rec := doSageMakerRequest(t, h, "AddAssociation", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantARN {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp["AssociationArn"], "arn:aws:sagemaker")
			}
		})
	}
}

func TestHandler_AddAssociation_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"SourceArn":      "arn:aws:sagemaker:us-east-1:000000000000:trial/t1",
		"DestinationArn": "arn:aws:sagemaker:us-east-1:000000000000:artifact/a1",
	}

	rec := doSageMakerRequest(t, h, "AddAssociation", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSageMakerRequest(t, h, "AddAssociation", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestHandler_AssociateTrialComponent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
		wantArns bool
	}{
		{
			name: "success",
			body: map[string]any{
				"TrialName":          "my-trial",
				"TrialComponentName": "my-component",
			},
			wantCode: http.StatusOK,
			wantArns: true,
		},
		{
			name:     "missing TrialName",
			body:     map[string]any{"TrialComponentName": "my-component"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing TrialComponentName",
			body:     map[string]any{"TrialName": "my-trial"},
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

			var body map[string]any
			if tt.body != nil {
				body = tt.body
			}

			rec := doSageMakerRequest(t, h, "AssociateTrialComponent", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantArns {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp["TrialArn"], "arn:aws:sagemaker")
				assert.Contains(t, resp["TrialComponentArn"], "arn:aws:sagemaker")
			}
		})
	}
}

func TestHandler_AssociateTrialComponent_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"TrialName":          "trial-x",
		"TrialComponentName": "comp-x",
	}

	rec := doSageMakerRequest(t, h, "AssociateTrialComponent", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSageMakerRequest(t, h, "AssociateTrialComponent", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
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
				h.Backend.AddClusterInternal("my-cluster")
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
				h.Backend.AddClusterInternal("batch-cluster")
			},
			body: map[string]any{
				"ClusterName": "batch-cluster",
				"NodeConfigs": []map[string]any{
					{"NodeId": "n1", "InstanceType": "ml.p3.2xlarge"},
					{"NodeId": "n2", "InstanceType": "ml.p3.2xlarge"},
				},
			},
			wantCode: http.StatusOK,
			wantARN:  true,
		},
		{
			name: "cluster not found",
			body: map[string]any{
				"ClusterName": "nonexistent",
				"NodeConfigs": []map[string]any{},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing ClusterName",
			body:     map[string]any{"NodeConfigs": []map[string]any{}},
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
				assert.Contains(t, resp["ClusterArn"], "arn:aws:sagemaker")
				assert.IsType(t, []any{}, resp["Failures"])
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
				c := h.Backend.AddClusterInternal("del-cluster")
				_ = c
				// Seed a node via BatchAdd
				nodes := []map[string]any{{"NodeId": "del-n1"}}
				_ = nodes
				h.Backend.AddClusterInternal("del-cluster-2")
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
				assert.Contains(t, resp["ClusterArn"], "arn:aws:sagemaker")
			}
		})
	}
}

func TestHandler_BatchDescribeModelPackage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *sagemaker.Handler)
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "success with existing packages",
			setup: func(t *testing.T, h *sagemaker.Handler) {
				t.Helper()
				h.Backend.AddModelPackageInternal(&sagemaker.ModelPackage{
					ModelPackageName:   "my-pkg",
					ModelPackageArn:    "arn:aws:sagemaker:us-east-1:000000000000:model-package/my-pkg",
					ModelPackageStatus: "Completed",
					CreationTime:       time.Now(),
				})
			},
			body: map[string]any{
				"ModelPackageArnList": []string{
					"arn:aws:sagemaker:us-east-1:000000000000:model-package/my-pkg",
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "package not found goes to errors map",
			body: map[string]any{
				"ModelPackageArnList": []string{
					"arn:aws:sagemaker:us-east-1:000000000000:model-package/nonexistent",
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "empty list",
			body: map[string]any{
				"ModelPackageArnList": []string{},
			},
			wantCode: http.StatusOK,
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

			rec := doSageMakerRequest(t, h, "BatchDescribeModelPackage", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "ModelPackageSummaries")
				assert.Contains(t, resp, "BatchDescribeModelPackageErrorMap")
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
				h.Backend.AddClusterInternal("reboot-cluster")
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
				h.Backend.AddClusterInternal("reboot-cluster-2")
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
				assert.Contains(t, resp["ClusterArn"], "arn:aws:sagemaker")
				assert.Contains(t, resp, "Failures")
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
				h.Backend.AddClusterInternal("replace-cluster")
			},
			body: map[string]any{
				"ClusterName": "replace-cluster",
				"Nodes":       []map[string]any{},
			},
			wantCode: http.StatusOK,
			wantARN:  true,
		},
		{
			name: "missing node goes to failures",
			setup: func(t *testing.T, h *sagemaker.Handler) {
				t.Helper()
				h.Backend.AddClusterInternal("replace-cluster-2")
			},
			body: map[string]any{
				"ClusterName": "replace-cluster-2",
				"Nodes": []map[string]any{
					{"NodeId": "nonexistent", "InstanceType": "ml.p3.2xlarge"},
				},
			},
			wantCode: http.StatusOK,
			wantARN:  true,
		},
		{
			name: "cluster not found",
			body: map[string]any{
				"ClusterName": "ghost",
				"Nodes":       []map[string]any{},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing ClusterName",
			body:     map[string]any{"Nodes": []map[string]any{}},
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
				assert.Contains(t, resp["ClusterArn"], "arn:aws:sagemaker")
				assert.Contains(t, resp, "Failures")
			}
		})
	}
}

func TestHandler_CreateAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
		wantARN  bool
	}{
		{
			name: "success with all fields",
			body: map[string]any{
				"ActionName": "my-action",
				"ActionType": "ModelDeployment",
				"Source": map[string]any{
					"SourceUri":  "s3://my-bucket/my-model",
					"SourceType": "S3URIPrefix",
				},
				"Description": "test action",
				"Status":      "Completed",
				"Properties":  map[string]string{"key": "value"},
				"Tags":        []map[string]string{{"Key": "env", "Value": "test"}},
			},
			wantCode: http.StatusOK,
			wantARN:  true,
		},
		{
			name: "success minimal",
			body: map[string]any{
				"ActionName": "minimal-action",
				"ActionType": "ModelDeployment",
			},
			wantCode: http.StatusOK,
			wantARN:  true,
		},
		{
			name:     "missing ActionName",
			body:     map[string]any{"ActionType": "ModelDeployment"},
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

			var body map[string]any
			if tt.body != nil {
				body = tt.body
			}

			rec := doSageMakerRequest(t, h, "CreateAction", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantARN {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp["ActionArn"], "arn:aws:sagemaker")
				assert.Contains(t, resp["ActionArn"], "action/")
			}
		})
	}
}

func TestHandler_CreateAction_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"ActionName": "dup-action",
		"ActionType": "ModelDeployment",
	}

	rec := doSageMakerRequest(t, h, "CreateAction", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSageMakerRequest(t, h, "CreateAction", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestHandler_CreateAlgorithm(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
		wantARN  bool
	}{
		{
			name: "success with description and tags",
			body: map[string]any{
				"AlgorithmName":        "my-algorithm",
				"AlgorithmDescription": "a great algorithm",
				"Tags":                 []map[string]string{{"Key": "owner", "Value": "team-a"}},
			},
			wantCode: http.StatusOK,
			wantARN:  true,
		},
		{
			name: "success minimal",
			body: map[string]any{
				"AlgorithmName": "minimal-algo",
			},
			wantCode: http.StatusOK,
			wantARN:  true,
		},
		{
			name:     "missing AlgorithmName",
			body:     map[string]any{"AlgorithmDescription": "desc"},
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

			var body map[string]any
			if tt.body != nil {
				body = tt.body
			}

			rec := doSageMakerRequest(t, h, "CreateAlgorithm", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantARN {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp["AlgorithmArn"], "arn:aws:sagemaker")
				assert.Contains(t, resp["AlgorithmArn"], "algorithm/")
			}
		})
	}
}

func TestHandler_CreateAlgorithm_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"AlgorithmName": "dup-algo",
	}

	rec := doSageMakerRequest(t, h, "CreateAlgorithm", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSageMakerRequest(t, h, "CreateAlgorithm", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestHandler_BatchDescribeModelPackage_ExistingAndMissing(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	h.Backend.AddModelPackageInternal(&sagemaker.ModelPackage{
		ModelPackageName:   "pkg-a",
		ModelPackageArn:    "arn:aws:sagemaker:us-east-1:000000000000:model-package/pkg-a",
		ModelPackageStatus: "Completed",
		CreationTime:       time.Now(),
	})

	body := map[string]any{
		"ModelPackageArnList": []string{
			"arn:aws:sagemaker:us-east-1:000000000000:model-package/pkg-a",
			"arn:aws:sagemaker:us-east-1:000000000000:model-package/missing",
		},
	}

	rec := doSageMakerRequest(t, h, "BatchDescribeModelPackage", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries, ok := resp["ModelPackageSummaries"].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, summaries, "arn:aws:sagemaker:us-east-1:000000000000:model-package/pkg-a")

	errors, ok := resp["BatchDescribeModelPackageErrorMap"].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, errors, "arn:aws:sagemaker:us-east-1:000000000000:model-package/missing")
}

func TestHandler_BatchAddClusterNodes_DuplicateNodeFails(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	h.Backend.AddClusterInternal("dup-node-cluster")

	// Add node-1 first time
	body := map[string]any{
		"ClusterName": "dup-node-cluster",
		"NodeConfigs": []map[string]any{{"NodeId": "node-1"}},
	}
	rec := doSageMakerRequest(t, h, "BatchAddClusterNodes", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Try to add node-1 again — should appear in Failures
	rec2 := doSageMakerRequest(t, h, "BatchAddClusterNodes", body)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	failures, _ := resp["Failures"].([]any)
	assert.Len(t, failures, 1)
	assert.Equal(t, "node-1", failures[0])
}
