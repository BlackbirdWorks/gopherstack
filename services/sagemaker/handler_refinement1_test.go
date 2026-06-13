package sagemaker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		seedModel bool
		wantCount int
	}{
		{
			name:      "reset clears models",
			seedModel: true,
			wantCount: 0,
		},
		{
			name:      "reset on empty backend",
			seedModel: false,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.seedModel {
				_, err := b.CreateModel(context.Background(),
					"test-model",
					"arn:aws:iam::000000000000:role/role",
					nil,
					nil,
					nil,
				)
				require.NoError(t, err)
			}

			b.Reset()

			assert.Equal(t, tt.wantCount, sagemaker.ModelCount(b))
		})
	}
}

func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		seedModel bool
		wantCount int
	}{
		{
			name:      "handler reset clears models",
			seedModel: true,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.seedModel {
				rec := doSageMakerRequest(t, h, "CreateModel", map[string]any{
					"ModelName":        "my-model",
					"ExecutionRoleArn": "arn:aws:iam::000000000000:role/role",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			h.Reset()

			assert.Equal(t, tt.wantCount, sagemaker.ModelCount(h.Backend))
		})
	}
}

func TestRefinement1_Region(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		region     string
		wantRegion string
	}{
		{
			name:       "returns configured region",
			region:     "us-west-2",
			wantRegion: "us-west-2",
		},
		{
			name:       "returns us-east-1",
			region:     "us-east-1",
			wantRegion: "us-east-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend("000000000000", tt.region)
			assert.Equal(t, tt.wantRegion, b.Region())
		})
	}
}

func TestRefinement1_AccountID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		accountID     string
		wantAccountID string
	}{
		{
			name:          "returns configured account ID",
			accountID:     "123456789012",
			wantAccountID: "123456789012",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend(tt.accountID, "us-east-1")
			assert.Equal(t, tt.wantAccountID, b.AccountID())
		})
	}
}

func TestRefinement1_ModelCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numModels int
		wantCount int
	}{
		{name: "zero models", numModels: 0, wantCount: 0},
		{name: "one model", numModels: 1, wantCount: 1},
		{name: "three models", numModels: 3, wantCount: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")

			for i := range tt.numModels {
				_, err := b.CreateModel(context.Background(),
					fmt.Sprintf("model-%d", i),
					"arn:aws:iam::000000000000:role/role",
					nil, nil, nil,
				)
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantCount, sagemaker.ModelCount(b))
		})
	}
}

func TestRefinement1_EndpointConfigCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numConfs  int
		wantCount int
	}{
		{name: "zero configs", numConfs: 0, wantCount: 0},
		{name: "two configs", numConfs: 2, wantCount: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")

			for i := range tt.numConfs {
				_, err := b.CreateEndpointConfig(context.Background(), fmt.Sprintf("cfg-%d", i), nil, nil)
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantCount, sagemaker.EndpointConfigCount(b))
		})
	}
}

func TestRefinement1_AssociationCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numAssocs int
		wantCount int
	}{
		{name: "no associations", numAssocs: 0, wantCount: 0},
		{name: "one association", numAssocs: 1, wantCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")

			for i := range tt.numAssocs {
				src := fmt.Sprintf("arn:aws:sagemaker:us-east-1:000000000000:trial/t%d", i)
				dst := fmt.Sprintf("arn:aws:sagemaker:us-east-1:000000000000:artifact/a%d", i)
				_, err := b.AddAssociation(context.Background(), src, dst, "ContributedTo", nil)
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantCount, sagemaker.AssociationCount(b))
		})
	}
}

func TestRefinement1_ActionCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numActs   int
		wantCount int
	}{
		{name: "no actions", numActs: 0, wantCount: 0},
		{name: "two actions", numActs: 2, wantCount: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")

			for i := range tt.numActs {
				b.AddActionInternal(context.Background(), fmt.Sprintf("action-%d", i), "ModelDeployment")
			}

			assert.Equal(t, tt.wantCount, sagemaker.ActionCount(b))
		})
	}
}

func TestRefinement1_AlgorithmCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numAlgos  int
		wantCount int
	}{
		{name: "no algorithms", numAlgos: 0, wantCount: 0},
		{name: "one algorithm", numAlgos: 1, wantCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")

			for i := range tt.numAlgos {
				b.AddAlgorithmInternal(context.Background(), "algo-"+strconv.Itoa(i))
			}

			assert.Equal(t, tt.wantCount, sagemaker.AlgorithmCount(b))
		})
	}
}

func TestRefinement1_ClusterCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		numClusters int
		wantCount   int
	}{
		{name: "no clusters", numClusters: 0, wantCount: 0},
		{name: "one cluster", numClusters: 1, wantCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")

			for i := range tt.numClusters {
				b.AddClusterInternal(context.Background(), "cluster-"+strconv.Itoa(i))
			}

			assert.Equal(t, tt.wantCount, sagemaker.ClusterCount(b))
		})
	}
}

func TestRefinement1_ModelPackageCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numPkgs   int
		wantCount int
	}{
		{name: "no packages", numPkgs: 0, wantCount: 0},
		{name: "two packages", numPkgs: 2, wantCount: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")

			for i := range tt.numPkgs {
				arnStr := fmt.Sprintf(
					"arn:aws:sagemaker:us-east-1:000000000000:model-package/pkg-%d",
					i,
				)
				b.AddModelPackageInternal(context.Background(), &sagemaker.ModelPackage{
					ModelPackageName:   fmt.Sprintf("pkg-%d", i),
					ModelPackageArn:    arnStr,
					ModelPackageStatus: "Approved",
				})
			}

			assert.Equal(t, tt.wantCount, sagemaker.ModelPackageCount(b))
		})
	}
}

func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantLen int
	}{
		{
			name:    "returns 381 operations",
			wantLen: 381,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			assert.Equal(t, tt.wantLen, sagemaker.HandlerOpsLen(h))
		})
	}
}

func TestRefinement1_AddActionInternal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		actionName string
		actionType string
	}{
		{
			name:       "creates action with correct fields",
			actionName: "my-action",
			actionType: "ModelDeployment",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")
			a := b.AddActionInternal(context.Background(), tt.actionName, tt.actionType)

			require.NotNil(t, a)
			assert.Equal(t, tt.actionName, a.ActionName)
			assert.Equal(t, tt.actionType, a.ActionType)
			assert.Contains(t, a.ActionArn, "arn:aws:sagemaker")
			assert.Equal(t, 1, sagemaker.ActionCount(b))
		})
	}
}

func TestRefinement1_AddAlgorithmInternal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		algoName  string
		wantCount int
	}{
		{
			name:      "creates algorithm with correct fields",
			algoName:  "my-algo",
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")
			al := b.AddAlgorithmInternal(context.Background(), tt.algoName)

			require.NotNil(t, al)
			assert.Equal(t, tt.algoName, al.AlgorithmName)
			assert.Contains(t, al.AlgorithmArn, "arn:aws:sagemaker")
			assert.Equal(t, tt.wantCount, sagemaker.AlgorithmCount(b))
		})
	}
}

func TestRefinement1_AddClusterInternal(t *testing.T) {
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

func TestRefinement1_AddModelPackageInternal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		pkgName   string
		wantCount int
	}{
		{
			name:      "creates model package",
			pkgName:   "my-pkg",
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")
			arnStr := "arn:aws:sagemaker:us-east-1:000000000000:model-package/" + tt.pkgName
			b.AddModelPackageInternal(context.Background(), &sagemaker.ModelPackage{
				ModelPackageName:   tt.pkgName,
				ModelPackageArn:    arnStr,
				ModelPackageStatus: "Approved",
			})

			assert.Equal(t, tt.wantCount, sagemaker.ModelPackageCount(b))
		})
	}
}

func TestRefinement1_DeleteModel_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "delete non-existent model returns 400",
			body:     map[string]any{"ModelName": "does-not-exist"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doSageMakerRequest(t, h, "DeleteModel", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestRefinement1_DeleteEndpointConfig_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "delete non-existent config returns 400",
			body:     map[string]any{"EndpointConfigName": "does-not-exist"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doSageMakerRequest(t, h, "DeleteEndpointConfig", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestRefinement1_DeleteTags_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "delete tags on non-existent resource returns 400",
			body: map[string]any{
				"ResourceArn": "arn:aws:sagemaker:us-east-1:000000000000:model/does-not-exist",
				"TagKeys":     []string{"env"},
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doSageMakerRequest(t, h, "DeleteTags", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestRefinement1_AddTags_ModelPackage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantTags map[string]string
		name     string
		wantCode int
	}{
		{
			name:     "tags on model package via ARN",
			wantCode: http.StatusOK,
			wantTags: map[string]string{"env": "prod"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arnStr := "arn:aws:sagemaker:us-east-1:000000000000:model-package/my-pkg"
			h.Backend.AddModelPackageInternal(context.Background(), &sagemaker.ModelPackage{
				ModelPackageName:   "my-pkg",
				ModelPackageArn:    arnStr,
				ModelPackageStatus: "Approved",
				Tags:               make(map[string]string),
			})

			rec := doSageMakerRequest(t, h, "AddTags", map[string]any{
				"ResourceArn": arnStr,
				"Tags":        []map[string]string{{"Key": "env", "Value": "prod"}},
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			rec2 := doSageMakerRequest(t, h, "ListTags", map[string]any{
				"ResourceArn": arnStr,
			})
			require.Equal(t, http.StatusOK, rec2.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))

			tags := resp["Tags"].([]any)
			require.Len(t, tags, 1)

			tag := tags[0].(map[string]any)
			assert.Equal(t, "env", tag["Key"])
			assert.Equal(t, "prod", tag["Value"])
		})
	}
}

func TestRefinement1_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		numModels     int
		numClusters   int
		wantModelsCnt int
		wantClustCnt  int
	}{
		{
			name:          "snapshot and restore preserves models and clusters",
			numModels:     2,
			numClusters:   1,
			wantModelsCnt: 2,
			wantClustCnt:  1,
		},
		{
			name:          "empty backend snapshot and restore",
			numModels:     0,
			numClusters:   0,
			wantModelsCnt: 0,
			wantClustCnt:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b1 := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")

			for i := range tt.numModels {
				_, err := b1.CreateModel(context.Background(),
					fmt.Sprintf("model-%d", i),
					"arn:aws:iam::000000000000:role/r",
					nil,
					nil,
					nil,
				)
				require.NoError(t, err)
			}

			for i := range tt.numClusters {
				b1.AddClusterInternal(context.Background(), "cluster-"+strconv.Itoa(i))
			}

			snap := b1.Snapshot()
			require.NotNil(t, snap)

			b2 := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b2.Restore(snap))

			assert.Equal(t, tt.wantModelsCnt, sagemaker.ModelCount(b2))
			assert.Equal(t, tt.wantClustCnt, sagemaker.ClusterCount(b2))
		})
	}
}

func TestRefinement1_CreateAction_TagsPresent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
		wantTags bool
	}{
		{
			name: "create action with tags, tags survive",
			body: map[string]any{
				"ActionName": "my-action",
				"ActionType": "ModelDeployment",
				"Source": map[string]any{
					"SourceUri": "s3://bucket/key",
				},
				"Tags": []map[string]string{{"Key": "team", "Value": "ml"}},
			},
			wantCode: http.StatusOK,
			wantTags: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doSageMakerRequest(t, h, "CreateAction", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantTags {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				arnStr := resp["ActionArn"]
				require.NotEmpty(t, arnStr)

				rec2 := doSageMakerRequest(t, h, "ListTags", map[string]any{"ResourceArn": arnStr})
				require.Equal(t, http.StatusOK, rec2.Code)

				var tagsResp map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &tagsResp))

				tags := tagsResp["Tags"].([]any)
				require.Len(t, tags, 1)
			}
		})
	}
}

func TestRefinement1_CreateAlgorithm_TagsPresent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
		wantTags bool
	}{
		{
			name: "create algorithm with tags",
			body: map[string]any{
				"AlgorithmName": "my-algo",
				"Tags":          []map[string]string{{"Key": "env", "Value": "dev"}},
			},
			wantCode: http.StatusOK,
			wantTags: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doSageMakerRequest(t, h, "CreateAlgorithm", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantTags {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				arnStr := resp["AlgorithmArn"]
				require.NotEmpty(t, arnStr)

				rec2 := doSageMakerRequest(t, h, "ListTags", map[string]any{"ResourceArn": arnStr})
				require.Equal(t, http.StatusOK, rec2.Code)
			}
		})
	}
}

func TestRefinement1_BatchDescribeModelPackage_MixedResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		seedArns  []string
		queryArns []string
		wantFound int
		wantErr   int
	}{
		{
			name:     "some found some not",
			seedArns: []string{"arn:aws:sagemaker:us-east-1:000000000000:model-package/pkg-1"},
			queryArns: []string{
				"arn:aws:sagemaker:us-east-1:000000000000:model-package/pkg-1",
				"arn:aws:sagemaker:us-east-1:000000000000:model-package/pkg-missing",
			},
			wantFound: 1,
			wantErr:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")

			for _, arnStr := range tt.seedArns {
				b.AddModelPackageInternal(context.Background(), &sagemaker.ModelPackage{
					ModelPackageName:   "pkg-1",
					ModelPackageArn:    arnStr,
					ModelPackageStatus: "Approved",
				})
			}

			results := b.BatchDescribeModelPackage(context.Background(), tt.queryArns)
			found, errs := 0, 0

			for _, r := range results {
				if r.ModelPackage != nil {
					found++
				} else {
					errs++
				}
			}

			assert.Equal(t, tt.wantFound, found)
			assert.Equal(t, tt.wantErr, errs)
		})
	}
}

func TestRefinement1_BatchDeleteClusterNodes_Empty(t *testing.T) {
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

func TestRefinement1_BatchRebootClusterNodes_PartialSuccess(t *testing.T) {
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

			_, _, err := h.Backend.BatchAddClusterNodes(context.Background(), tt.clusterName, []sagemaker.ClusterNode{
				{NodeID: "node-1", NodeStatus: "Running"},
			})
			require.NoError(t, err)

			rec := doSageMakerRequest(t, h, "BatchRebootClusterNodes", map[string]any{
				"ClusterName": tt.clusterName,
				"NodeIds":     tt.nodeIDs,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestRefinement1_UnknownOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		op       string
		wantCode int
	}{
		{
			name:     "unknown op returns 400",
			op:       "DoSomethingUnknown",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doSageMakerRequest(t, h, tt.op, map[string]any{})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestRefinement1_NilAppContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ctx     *service.AppContext
		name    string
		wantErr bool
	}{
		{
			name:    "nil context returns error",
			ctx:     nil,
			wantErr: true,
		},
		{
			name:    "non-nil context returns handler",
			ctx:     &service.AppContext{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &sagemaker.Provider{}
			h, err := p.Init(tt.ctx)

			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, h)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, h)
			}
		})
	}
}
