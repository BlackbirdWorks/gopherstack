package elasticache_test

import (
	"testing"

	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	elasticachetypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_DescribeServiceUpdates_ReturnsSeedData(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.DescribeServiceUpdates(t.Context(), &elasticachesdk.DescribeServiceUpdatesInput{})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(out.ServiceUpdates), 1)
}

func TestHandler_DescribeServiceUpdates_FilterByName(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.DescribeServiceUpdates(t.Context(), &elasticachesdk.DescribeServiceUpdatesInput{
		ServiceUpdateName: aws.String("20240101-001-security-patch"),
	})
	require.NoError(t, err)
	require.Len(t, out.ServiceUpdates, 1)
	assert.Equal(t, "20240101-001-security-patch", aws.ToString(out.ServiceUpdates[0].ServiceUpdateName))
}

func TestHandler_DescribeServiceUpdates_FilterByStatus(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.DescribeServiceUpdates(t.Context(), &elasticachesdk.DescribeServiceUpdatesInput{
		ServiceUpdateStatus: []elasticachetypes.ServiceUpdateStatus{"available"},
	})
	require.NoError(t, err)

	for _, su := range out.ServiceUpdates {
		assert.Equal(t, elasticachetypes.ServiceUpdateStatusAvailable, su.ServiceUpdateStatus)
	}
}

// ----------------------------------------
// DescribeUpdateActions — after BatchApply
// ----------------------------------------

func TestHandler_DescribeUpdateActions_AfterBatchApply(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("ua-rg"),
		ReplicationGroupDescription: aws.String("update actions test"),
	})
	require.NoError(t, err)

	_, err = client.BatchApplyUpdateAction(t.Context(), &elasticachesdk.BatchApplyUpdateActionInput{
		ReplicationGroupIds: []string{"ua-rg"},
		ServiceUpdateName:   aws.String("20240101-001-security-patch"),
	})
	require.NoError(t, err)

	out, err := client.DescribeUpdateActions(t.Context(), &elasticachesdk.DescribeUpdateActionsInput{
		ServiceUpdateName: aws.String("20240101-001-security-patch"),
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(out.UpdateActions), 1)
}

// ----------------------------------------
// DescribeEngineDefaultParameters — real defaults
// ----------------------------------------

func TestHandler_DescribeServiceUpdates_Empty(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.DescribeServiceUpdates(t.Context(), &elasticachesdk.DescribeServiceUpdatesInput{})
	require.NoError(t, err)
	assert.NotNil(t, out.ServiceUpdates)
}

// ----------------------------------------
// UpdateAction — describe with service update name
// ----------------------------------------

func TestHandler_DescribeUpdateActions_WithServiceUpdateName(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.DescribeUpdateActions(t.Context(), &elasticachesdk.DescribeUpdateActionsInput{
		ServiceUpdateName: aws.String("nonexistent-update"),
	})
	require.NoError(t, err)
	assert.NotNil(t, out.UpdateActions)
}

// ----------------------------------------
// ListAllowedNodeTypeModifications
// ----------------------------------------

func TestHandler_BatchApplyUpdateAction_MixedResults(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	// Create one RG and one cluster
	_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("rg-exists"),
		ReplicationGroupDescription: aws.String("Exists"),
	})
	require.NoError(t, err)

	out, err := client.BatchApplyUpdateAction(t.Context(), &elasticachesdk.BatchApplyUpdateActionInput{
		ServiceUpdateName:   aws.String("update-test"),
		ReplicationGroupIds: []string{"rg-exists", "rg-missing"},
	})

	require.NoError(t, err)
	assert.Len(t, out.ProcessedUpdateActions, 1)
	assert.Len(t, out.UnprocessedUpdateActions, 1)
}

func TestHandler_BatchStopUpdateAction_WithCluster(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId: aws.String("cluster-stop"),
		Engine:         aws.String("redis"),
	})
	require.NoError(t, err)

	out, err := client.BatchStopUpdateAction(t.Context(), &elasticachesdk.BatchStopUpdateActionInput{
		ServiceUpdateName: aws.String("stop-update"),
		CacheClusterIds:   []string{"cluster-stop", "missing-cluster"},
	})

	require.NoError(t, err)
	assert.Len(t, out.ProcessedUpdateActions, 1)
	assert.Len(t, out.UnprocessedUpdateActions, 1)
}

func TestDescribeServiceUpdates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{
			name: "empty_list",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			out, err := client.DescribeServiceUpdates(t.Context(), &elasticachesdk.DescribeServiceUpdatesInput{})

			require.NoError(t, err)
			assert.NotNil(t, out.ServiceUpdates)
		})
	}
}

// ----------------------------------------
// DescribeUpdateActions
// ----------------------------------------

func TestDescribeUpdateActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{
			name: "empty_list",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			out, err := client.DescribeUpdateActions(t.Context(), &elasticachesdk.DescribeUpdateActionsInput{})

			require.NoError(t, err)
			assert.NotNil(t, out.UpdateActions)
		})
	}
}

// ----------------------------------------
// ListAllowedNodeTypeModifications
// ----------------------------------------

func TestBatchApplyUpdateAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup                  func(t *testing.T, client *elasticachesdk.Client)
		name                   string
		serviceUpdateName      string
		replicationGroupIDs    []string
		cacheClusterIDs        []string
		wantProcessedRGCount   int
		wantUnprocessedRGCount int
		wantProcessedCCCount   int
		wantUnprocessedCCCount int
	}{
		{
			name:                "apply_to_existing_rg",
			serviceUpdateName:   "update-2024-01",
			replicationGroupIDs: []string{"my-rg"},
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("my-rg"),
					ReplicationGroupDescription: aws.String("RG for update"),
				})
				require.NoError(t, err)
			},
			wantProcessedRGCount:   1,
			wantUnprocessedRGCount: 0,
		},
		{
			name:                   "apply_to_nonexistent_rg",
			serviceUpdateName:      "update-2024-01",
			replicationGroupIDs:    []string{"nonexistent-rg"},
			wantProcessedRGCount:   0,
			wantUnprocessedRGCount: 1,
		},
		{
			name:              "apply_to_existing_cluster",
			serviceUpdateName: "update-2024-02",
			cacheClusterIDs:   []string{"my-cluster"},
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
					CacheClusterId: aws.String("my-cluster"),
					Engine:         aws.String("redis"),
				})
				require.NoError(t, err)
			},
			wantProcessedCCCount:   1,
			wantUnprocessedCCCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			out, err := client.BatchApplyUpdateAction(t.Context(), &elasticachesdk.BatchApplyUpdateActionInput{
				ServiceUpdateName:   aws.String(tt.serviceUpdateName),
				ReplicationGroupIds: tt.replicationGroupIDs,
				CacheClusterIds:     tt.cacheClusterIDs,
			})

			require.NoError(t, err)
			assert.Len(t, out.ProcessedUpdateActions, tt.wantProcessedRGCount+tt.wantProcessedCCCount)
			assert.Len(t, out.UnprocessedUpdateActions, tt.wantUnprocessedRGCount+tt.wantUnprocessedCCCount)
		})
	}
}

// ----------------------------------------
// BatchStopUpdateAction
// ----------------------------------------

func TestBatchStopUpdateAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup                func(t *testing.T, client *elasticachesdk.Client)
		name                 string
		serviceUpdateName    string
		replicationGroupIDs  []string
		cacheClusterIDs      []string
		wantProcessedCount   int
		wantUnprocessedCount int
	}{
		{
			name:                "stop_existing_rg",
			serviceUpdateName:   "update-2024-01",
			replicationGroupIDs: []string{"rg-for-stop"},
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("rg-for-stop"),
					ReplicationGroupDescription: aws.String("RG for stop"),
				})
				require.NoError(t, err)
			},
			wantProcessedCount:   1,
			wantUnprocessedCount: 0,
		},
		{
			name:                 "stop_nonexistent_rg",
			serviceUpdateName:    "update-2024-01",
			replicationGroupIDs:  []string{"nonexistent-rg"},
			wantProcessedCount:   0,
			wantUnprocessedCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			out, err := client.BatchStopUpdateAction(t.Context(), &elasticachesdk.BatchStopUpdateActionInput{
				ServiceUpdateName:   aws.String(tt.serviceUpdateName),
				ReplicationGroupIds: tt.replicationGroupIDs,
				CacheClusterIds:     tt.cacheClusterIDs,
			})

			require.NoError(t, err)
			assert.Len(t, out.ProcessedUpdateActions, tt.wantProcessedCount)
			assert.Len(t, out.UnprocessedUpdateActions, tt.wantUnprocessedCount)
		})
	}
}
