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

func TestPersistenceRoundtrip_MiscDestubState(t *testing.T) {
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
