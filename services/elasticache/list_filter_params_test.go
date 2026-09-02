package elasticache_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeUpdateActions_ReplicationGroupIdsFilter verifies
// ReplicationGroupIds (elasticache@v1.56.4 api_op_DescribeUpdateActions.go)
// restricts results to actions targeting those replication groups.
func TestDescribeUpdateActions_ReplicationGroupIdsFilter(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	for _, id := range []string{"ua-rgids-a", "ua-rgids-b"} {
		_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
			ReplicationGroupId:          aws.String(id),
			ReplicationGroupDescription: aws.String("ua rg ids filter"),
		})
		require.NoError(t, err)
	}

	_, err := client.BatchApplyUpdateAction(t.Context(), &elasticachesdk.BatchApplyUpdateActionInput{
		ReplicationGroupIds: []string{"ua-rgids-a", "ua-rgids-b"},
		ServiceUpdateName:   aws.String("ua-rgids-patch"),
	})
	require.NoError(t, err)

	out, err := client.DescribeUpdateActions(t.Context(), &elasticachesdk.DescribeUpdateActionsInput{
		ServiceUpdateName:   aws.String("ua-rgids-patch"),
		ReplicationGroupIds: []string{"ua-rgids-a"},
	})
	require.NoError(t, err)

	require.Len(t, out.UpdateActions, 1)
	assert.Equal(t, "ua-rgids-a", aws.ToString(out.UpdateActions[0].ReplicationGroupId))
}

// TestDescribeUpdateActions_CacheClusterIdsFilter verifies CacheClusterIds
// restricts results to actions targeting those clusters.
func TestDescribeUpdateActions_CacheClusterIdsFilter(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	for _, id := range []string{"ua-ccids-a", "ua-ccids-b"} {
		_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
			CacheClusterId: aws.String(id),
			Engine:         aws.String("redis"),
			NumCacheNodes:  aws.Int32(1),
		})
		require.NoError(t, err)
	}

	_, err := client.BatchApplyUpdateAction(t.Context(), &elasticachesdk.BatchApplyUpdateActionInput{
		CacheClusterIds:   []string{"ua-ccids-a", "ua-ccids-b"},
		ServiceUpdateName: aws.String("ua-ccids-patch"),
	})
	require.NoError(t, err)

	out, err := client.DescribeUpdateActions(t.Context(), &elasticachesdk.DescribeUpdateActionsInput{
		ServiceUpdateName: aws.String("ua-ccids-patch"),
		CacheClusterIds:   []string{"ua-ccids-a"},
	})
	require.NoError(t, err)

	require.Len(t, out.UpdateActions, 1)
	assert.Equal(t, "ua-ccids-a", aws.ToString(out.UpdateActions[0].CacheClusterId))
}

// TestDescribeUpdateActions_UpdateActionStatusFilter verifies
// UpdateActionStatus excludes actions not in the requested status set.
func TestDescribeUpdateActions_UpdateActionStatusFilter(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	for _, id := range []string{"ua-status-a", "ua-status-b"} {
		_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
			ReplicationGroupId:          aws.String(id),
			ReplicationGroupDescription: aws.String("ua status filter"),
		})
		require.NoError(t, err)
	}

	_, err := client.BatchApplyUpdateAction(t.Context(), &elasticachesdk.BatchApplyUpdateActionInput{
		ReplicationGroupIds: []string{"ua-status-a", "ua-status-b"},
		ServiceUpdateName:   aws.String("ua-status-patch"),
	})
	require.NoError(t, err)

	_, err = client.BatchStopUpdateAction(t.Context(), &elasticachesdk.BatchStopUpdateActionInput{
		ReplicationGroupIds: []string{"ua-status-a"},
		ServiceUpdateName:   aws.String("ua-status-patch"),
	})
	require.NoError(t, err)

	out, err := client.DescribeUpdateActions(t.Context(), &elasticachesdk.DescribeUpdateActionsInput{
		ServiceUpdateName:  aws.String("ua-status-patch"),
		UpdateActionStatus: []types.UpdateActionStatus{types.UpdateActionStatusStopped},
	})
	require.NoError(t, err)

	require.Len(t, out.UpdateActions, 1)
	assert.Equal(t, "ua-status-a", aws.ToString(out.UpdateActions[0].ReplicationGroupId))
	assert.Equal(t, types.UpdateActionStatusStopped, out.UpdateActions[0].UpdateActionStatus)
}

// TestDescribeReservedCacheNodesOfferings_DurationFilter verifies Duration
// (elasticache@v1.56.4 api_op_DescribeReservedCacheNodesOfferings.go, valid
// values "1 | 3 | 31536000 | 94608000") excludes offerings whose duration
// doesn't match. Every builtin offering is a 1-year ("31536000" seconds)
// term, so a 3-year filter must exclude them all.
func TestDescribeReservedCacheNodesOfferings_DurationFilter(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	matching, err := client.DescribeReservedCacheNodesOfferings(
		t.Context(),
		&elasticachesdk.DescribeReservedCacheNodesOfferingsInput{Duration: aws.String("1")},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, matching.ReservedCacheNodesOfferings)

	nonMatching, err := client.DescribeReservedCacheNodesOfferings(
		t.Context(),
		&elasticachesdk.DescribeReservedCacheNodesOfferingsInput{Duration: aws.String("3")},
	)
	require.NoError(t, err)
	assert.Empty(t, nonMatching.ReservedCacheNodesOfferings)
}

// TestDescribeReservedCacheNodesOfferings_ProductDescriptionFilter verifies
// ProductDescription excludes non-matching offerings.
func TestDescribeReservedCacheNodesOfferings_ProductDescriptionFilter(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	matching, err := client.DescribeReservedCacheNodesOfferings(
		t.Context(),
		&elasticachesdk.DescribeReservedCacheNodesOfferingsInput{ProductDescription: aws.String("Redis")},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, matching.ReservedCacheNodesOfferings)

	nonMatching, err := client.DescribeReservedCacheNodesOfferings(
		t.Context(),
		&elasticachesdk.DescribeReservedCacheNodesOfferingsInput{ProductDescription: aws.String("memcached")},
	)
	require.NoError(t, err)
	assert.Empty(t, nonMatching.ReservedCacheNodesOfferings)
}

// TestDescribeReservedCacheNodes_DurationAndProductDescriptionFilters
// verifies both filters carry through to purchased reservations too.
func TestDescribeReservedCacheNodes_DurationAndProductDescriptionFilters(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.PurchaseReservedCacheNodesOffering(
		t.Context(),
		&elasticachesdk.PurchaseReservedCacheNodesOfferingInput{
			ReservedCacheNodesOfferingId: aws.String("31153cd5-4ce6-45a9-b6ce-7f0b6789b8fa"),
			ReservedCacheNodeId:          aws.String("lfp-reserved-node"),
			CacheNodeCount:               aws.Int32(1),
		},
	)
	require.NoError(t, err)

	nonMatchingDuration, err := client.DescribeReservedCacheNodes(
		t.Context(),
		&elasticachesdk.DescribeReservedCacheNodesInput{Duration: aws.String("3")},
	)
	require.NoError(t, err)
	assert.Empty(t, nonMatchingDuration.ReservedCacheNodes)

	nonMatchingProduct, err := client.DescribeReservedCacheNodes(
		t.Context(),
		&elasticachesdk.DescribeReservedCacheNodesInput{ProductDescription: aws.String("memcached")},
	)
	require.NoError(t, err)
	assert.Empty(t, nonMatchingProduct.ReservedCacheNodes)

	matching, err := client.DescribeReservedCacheNodes(
		t.Context(),
		&elasticachesdk.DescribeReservedCacheNodesInput{
			Duration:            aws.String("31536000"),
			ProductDescription:  aws.String("Redis"),
			ReservedCacheNodeId: aws.String("lfp-reserved-node"),
		},
	)
	require.NoError(t, err)
	require.Len(t, matching.ReservedCacheNodes, 1)
}

// TestDescribeUsers_EngineFilter verifies the Engine param excludes users of
// a different engine.
func TestDescribeUsers_EngineFilter(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateUser(t.Context(), &elasticachesdk.CreateUserInput{
		UserId:             aws.String("lfp-user-redis"),
		UserName:           aws.String("lfp-user-redis"),
		Engine:             aws.String("redis"),
		AccessString:       aws.String("on ~* +@all"),
		NoPasswordRequired: aws.Bool(true),
	})
	require.NoError(t, err)

	out, err := client.DescribeUsers(t.Context(), &elasticachesdk.DescribeUsersInput{
		Engine: aws.String("valkey"),
	})
	require.NoError(t, err)
	assert.Empty(t, out.Users)

	out, err = client.DescribeUsers(t.Context(), &elasticachesdk.DescribeUsersInput{
		Engine: aws.String("redis"),
	})
	require.NoError(t, err)
	found := false
	for _, u := range out.Users {
		if aws.ToString(u.UserId) == "lfp-user-redis" {
			found = true
		}
	}
	assert.True(t, found)
}

// TestDescribeUsers_FiltersUserIdFilter verifies a Filters entry named
// "UserId" (elasticache@v1.56.4 api_op_DescribeUsers.go) restricts results to
// the listed user IDs.
func TestDescribeUsers_FiltersUserIdFilter(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	for _, id := range []string{"lfp-filt-a", "lfp-filt-b"} {
		_, err := client.CreateUser(t.Context(), &elasticachesdk.CreateUserInput{
			UserId:             aws.String(id),
			UserName:           aws.String(id),
			Engine:             aws.String("redis"),
			AccessString:       aws.String("on ~* +@all"),
			NoPasswordRequired: aws.Bool(true),
		})
		require.NoError(t, err)
	}

	out, err := client.DescribeUsers(t.Context(), &elasticachesdk.DescribeUsersInput{
		Filters: []types.Filter{
			{Name: aws.String("UserId"), Values: []string{"lfp-filt-a"}},
		},
	})
	require.NoError(t, err)

	ids := make([]string, 0, len(out.Users))
	for _, u := range out.Users {
		ids = append(ids, aws.ToString(u.UserId))
	}
	assert.Contains(t, ids, "lfp-filt-a")
	assert.NotContains(t, ids, "lfp-filt-b")
}
