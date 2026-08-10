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

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

func TestSnapshotRestore(t *testing.T) {
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

			snap := b1.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b2.Restore(t.Context(), snap))

			assert.Equal(t, tt.wantModelsCnt, sagemaker.ModelCount(b2))
			assert.Equal(t, tt.wantClustCnt, sagemaker.ClusterCount(b2))
		})
	}
}

func TestPersistenceRoundtrip_PolicyCatalogAndPipelineVersions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelPackageGroup", map[string]any{"ModelPackageGroupName": "persist-group"})
	doSageMakerRequest(t, h, "PutModelPackageGroupPolicy", map[string]any{
		"ModelPackageGroupName": "persist-group", "ResourcePolicy": `{"Version":"2012-10-17"}`,
	})

	doSageMakerRequest(t, h, "EnableSagemakerServicecatalogPortfolio", map[string]any{})

	doSageMakerRequest(t, h, "CreatePipeline", map[string]any{
		"PipelineName": "persist-pipeline", "PipelineDefinition": `{"Version":"v1"}`,
	})
	doSageMakerRequest(t, h, "UpdatePipeline", map[string]any{
		"PipelineName": "persist-pipeline", "PipelineDefinition": `{"Version":"v2"}`,
	})

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := newTestHandler(t)
	require.NoError(t, h2.Restore(t.Context(), snap))

	recPolicy := doSageMakerRequest(t, h2, "GetModelPackageGroupPolicy", map[string]any{
		"ModelPackageGroupName": "persist-group",
	})
	require.Equal(t, http.StatusOK, recPolicy.Code)

	var policyOut map[string]any
	require.NoError(t, json.Unmarshal(recPolicy.Body.Bytes(), &policyOut))
	assert.JSONEq(t, `{"Version":"2012-10-17"}`, policyOut["ResourcePolicy"].(string))

	recStatus := doSageMakerRequest(t, h2, "GetSagemakerServicecatalogPortfolioStatus", map[string]any{})
	var statusOut map[string]any
	require.NoError(t, json.Unmarshal(recStatus.Body.Bytes(), &statusOut))
	assert.Equal(t, "Enabled", statusOut["Status"])

	recVersions := doSageMakerRequest(t, h2, "ListPipelineVersions", map[string]any{"PipelineName": "persist-pipeline"})
	require.Equal(t, http.StatusOK, recVersions.Code)

	var versionsOut map[string]any
	require.NoError(t, json.Unmarshal(recVersions.Body.Bytes(), &versionsOut))
	versions, _ := versionsOut["PipelineVersionSummaries"].([]any)
	assert.Len(t, versions, 2)
}

// TestPersistenceRoundtrip_ClusterFullFields guards persistedCluster (the
// hand-maintained DTO in persistence.go): ClusterRole and VpcConfig were
// previously accepted by CreateCluster and returned by DescribeCluster but
// silently dropped by Snapshot/Restore because they were never added to the
// DTO alongside the fields fixed in the earlier ClusterRole/VpcConfig pass.
// This also covers the AutoScaling/Orchestrator/NodeProvisioningMode/
// TieredStorageConfig fields added by gopherstack-i359.
func TestPersistenceRoundtrip_ClusterFullFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName": "persist-cluster",
		"ClusterRole": "arn:aws:iam::000000000000:role/HyperPodClusterRole",
		"VpcConfig": map[string]any{
			"SecurityGroupIds": []any{"sg-1"},
			"Subnets":          []any{"subnet-1"},
		},
		"AutoScaling": map[string]any{
			"Mode":           "Enable",
			"AutoScalerType": "Karpenter",
		},
		"Orchestrator": map[string]any{
			"Eks": map[string]any{"ClusterArn": "arn:aws:eks:us-east-1:000000000000:cluster/eks1"},
		},
		"NodeProvisioningMode": "Continuous",
		"TieredStorageConfig": map[string]any{
			"Mode":                               "Enable",
			"InstanceMemoryAllocationPercentage": 25,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := newTestHandler(t)
	require.NoError(t, h2.Restore(t.Context(), snap))

	rec = doSageMakerRequest(t, h2, "DescribeCluster", map[string]any{"ClusterName": "persist-cluster"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.Equal(t, "arn:aws:iam::000000000000:role/HyperPodClusterRole", resp["ClusterRole"])

	vpc, ok := resp["VpcConfig"].(map[string]any)
	require.True(t, ok, "VpcConfig must survive snapshot/restore")
	sgIDs, _ := vpc["SecurityGroupIds"].([]any)
	require.Len(t, sgIDs, 1)
	assert.Equal(t, "sg-1", sgIDs[0])

	as, ok := resp["AutoScaling"].(map[string]any)
	require.True(t, ok, "AutoScaling must survive snapshot/restore")
	assert.Equal(t, "Enable", as["Mode"])
	assert.Equal(t, "Karpenter", as["AutoScalerType"])

	orch, ok := resp["Orchestrator"].(map[string]any)
	require.True(t, ok, "Orchestrator must survive snapshot/restore")
	eks, ok := orch["Eks"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "arn:aws:eks:us-east-1:000000000000:cluster/eks1", eks["ClusterArn"])

	assert.Equal(t, "Continuous", resp["NodeProvisioningMode"])

	tsc, ok := resp["TieredStorageConfig"].(map[string]any)
	require.True(t, ok, "TieredStorageConfig must survive snapshot/restore")
	assert.Equal(t, "Enable", tsc["Mode"])
	assert.InDelta(t, 25, tsc["InstanceMemoryAllocationPercentage"], 0)
}

// TestPersistenceRoundtrip_PipelineDefinitionFromS3 confirms a pipeline
// created from a PipelineDefinitionS3Location (gopherstack-i359, s3pipeline.go)
// carries its fetched PipelineDefinition through Snapshot/Restore like any
// other pipeline field -- Pipeline has no hand-maintained persisted DTO (see
// persistence.go's persistedCluster doc comment for the one type that does),
// so this also guards against one being added later that forgets the field.
func TestPersistenceRoundtrip_PipelineDefinitionFromS3(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	s3 := newMockPipelineS3()
	s3.put("persist-bucket", "defs/pipeline.json", `{"Version":"2020-12-01","Steps":[{"Name":"Step1"}]}`)
	h.Backend.SetS3Backend(s3)

	rec := doSageMakerRequest(t, h, "CreatePipeline", map[string]any{
		"PipelineName": "persist-s3-pipeline",
		"RoleArn":      "arn:aws:iam::000000000000:role/Role",
		"PipelineDefinitionS3Location": map[string]any{
			"Bucket":    "persist-bucket",
			"ObjectKey": "defs/pipeline.json",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := newTestHandler(t)
	require.NoError(t, h2.Restore(t.Context(), snap))

	rec = doSageMakerRequest(t, h2, "DescribePipeline", map[string]any{"PipelineName": "persist-s3-pipeline"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.JSONEq(t, `{"Version":"2020-12-01","Steps":[{"Name":"Step1"}]}`, resp["PipelineDefinition"].(string))
}

func TestBackend_Persistence_SnapshotRestore(t *testing.T) {
	t.Parallel()

	b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")

	// Create some resources.
	_, err := b.CreateModel(context.Background(), "snap-model", "arn:aws:iam::000000000000:role/test", nil, nil, nil)
	require.NoError(t, err)

	// Snapshot.
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	// Destroy some state.
	b.Reset()

	// Verify gone.
	_, err = b.DescribeModel(context.Background(), "snap-model")
	require.Error(t, err)

	// Restore.
	restErr := b.Restore(t.Context(), snap)
	require.NoError(t, restErr)

	// Verify restored.
	_, err = b.DescribeModel(context.Background(), "snap-model")
	assert.NoError(t, err)
}

// TestPersistenceRoundtrip_AIAndGenericJobFamilies verifies that
// AIWorkloadConfig, AIBenchmarkJob, AIRecommendationJob, and the generic Job
// family (all added by the SDK bump that introduced CreateJob et al.) are
// wired into backendSnapshot: created via one handler, they must still be
// describable via a second handler restored from the first's snapshot.
func TestPersistenceRoundtrip_AIAndGenericJobFamilies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		createOp    string
		createBody  map[string]any
		describeOp  string
		describeKey map[string]any
		wantField   string
		wantValue   string
	}{
		{
			name:        "AIWorkloadConfig",
			createOp:    "CreateAIWorkloadConfig",
			createBody:  map[string]any{"AIWorkloadConfigName": "persist-wc"},
			describeOp:  "DescribeAIWorkloadConfig",
			describeKey: map[string]any{"AIWorkloadConfigName": "persist-wc"},
			wantField:   "AIWorkloadConfigName",
			wantValue:   "persist-wc",
		},
		{
			name:     "AIBenchmarkJob",
			createOp: "CreateAIBenchmarkJob",
			createBody: map[string]any{
				"AIBenchmarkJobName":         "persist-bench",
				"AIWorkloadConfigIdentifier": "persist-wc-dep",
				"RoleArn":                    "arn:aws:iam::000000000000:role/TestRole",
				"BenchmarkTarget":            map[string]any{"Endpoint": map[string]any{"Identifier": "ep"}},
				"OutputConfig":               map[string]any{"S3OutputLocation": "s3://bucket/out/"},
			},
			describeOp:  "DescribeAIBenchmarkJob",
			describeKey: map[string]any{"AIBenchmarkJobName": "persist-bench"},
			wantField:   "AIBenchmarkJobName",
			wantValue:   "persist-bench",
		},
		{
			name:     "AIRecommendationJob",
			createOp: "CreateAIRecommendationJob",
			createBody: map[string]any{
				"AIRecommendationJobName":    "persist-rec",
				"AIWorkloadConfigIdentifier": "persist-wc-dep",
				"RoleArn":                    "arn:aws:iam::000000000000:role/TestRole",
				"ModelSource":                map[string]any{"S3": map[string]any{"S3Uri": "s3://bucket/model/"}},
				"OutputConfig":               map[string]any{"S3OutputLocation": "s3://bucket/out/"},
				"PerformanceTarget":          map[string]any{"MetricName": "ttft-ms", "Threshold": 100},
			},
			describeOp:  "DescribeAIRecommendationJob",
			describeKey: map[string]any{"AIRecommendationJobName": "persist-rec"},
			wantField:   "AIRecommendationJobName",
			wantValue:   "persist-rec",
		},
		{
			name:     "Job",
			createOp: "CreateJob",
			createBody: map[string]any{
				"JobName":                "persist-job",
				"JobCategory":            "AgentRFT",
				"JobConfigDocument":      `{"foo":"bar"}`,
				"JobConfigSchemaVersion": "1.0",
				"RoleArn":                "arn:aws:iam::000000000000:role/TestRole",
			},
			describeOp:  "DescribeJob",
			describeKey: map[string]any{"JobCategory": "AgentRFT", "JobName": "persist-job"},
			wantField:   "JobName",
			wantValue:   "persist-job",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h1 := newTestHandler(t)

			// AIBenchmarkJob/AIRecommendationJob need an AIWorkloadConfig to
			// reference; unused (but harmless) for the other cases. Named
			// distinctly from the AIWorkloadConfig subtest's own "persist-wc"
			// so the two don't collide on a duplicate name.
			doSageMakerRequest(
				t,
				h1,
				"CreateAIWorkloadConfig",
				map[string]any{"AIWorkloadConfigName": "persist-wc-dep"},
			)

			createRec := doSageMakerRequest(t, h1, tt.createOp, tt.createBody)
			require.Equal(t, http.StatusOK, createRec.Code, createRec.Body.String())

			snap := h1.Snapshot(t.Context())
			require.NotNil(t, snap)

			h2 := newTestHandler(t)
			require.NoError(t, h2.Restore(t.Context(), snap))

			descRec := doSageMakerRequest(t, h2, tt.describeOp, tt.describeKey)
			require.Equal(t, http.StatusOK, descRec.Code, descRec.Body.String())

			var out map[string]any
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &out))
			assert.Equal(t, tt.wantValue, out[tt.wantField])
		})
	}
}
