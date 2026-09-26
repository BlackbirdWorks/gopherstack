package quicksight_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	quicksightsdk "github.com/aws/aws-sdk-go-v2/service/quicksight"
	"github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

// TestRealClient_BatchDescribeUserLimits proves BatchDescribeUserLimits
// against the real aws-sdk-go-v2 client: a known user resolves SYSTEM_DEFAULT
// limits for both resource types (this backend has no LimitsProfile
// assignment mechanism at all -- see userlimits.go), and an unknown user is
// reported in Errors rather than silently given default limits.
func TestRealClient_BatchDescribeUserLimits(t *testing.T) {
	t.Parallel()

	backend := quicksight.NewInMemoryBackend(appTestAccountID, rtQSTestRegion)
	h := quicksight.NewHandler(backend)
	client := newTestQuickSightClient(t, h)
	ctx := t.Context()

	_, err := backend.RegisterUser(
		appTestAccountID, "default", "alice", "alice@example.com", "READER", "QUICKSIGHT", "", "", nil,
	)
	require.NoError(t, err)

	out, err := client.BatchDescribeUserLimits(ctx, &quicksightsdk.BatchDescribeUserLimitsInput{
		AccountId: aws.String(appTestAccountID),
		Users: []types.UserLimitsEntry{
			{Namespace: aws.String("default"), UserName: aws.String("alice")},
			{Namespace: aws.String("default"), UserName: aws.String("no-such-user")},
		},
	})
	require.NoError(t, err)

	require.Len(t, out.UserLimits, 1)
	assert.Equal(t, "alice", aws.ToString(out.UserLimits[0].UserName))
	require.Len(t, out.UserLimits[0].EffectiveLimits, 2)

	byType := map[types.ResourceType]types.EffectiveLimit{}
	for _, l := range out.UserLimits[0].EffectiveLimits {
		byType[l.ResourceType] = l
	}

	indexLimit, ok := byType[types.ResourceTypeIndexStorage]
	require.True(t, ok)
	assert.Equal(t, types.LimitSourceSystemDefault, indexLimit.Source)
	assert.Equal(t, types.LimitUnitGb, indexLimit.LimitUnit)
	assert.Equal(t, int64(25), aws.ToInt64(indexLimit.LimitValue))

	agentLimit, ok := byType[types.ResourceTypeAgentHours]
	require.True(t, ok)
	assert.Equal(t, types.LimitSourceSystemDefault, agentLimit.Source)
	assert.Equal(t, types.LimitUnitHours, agentLimit.LimitUnit)

	require.Len(t, out.Errors, 1)
	assert.Equal(t, "no-such-user", aws.ToString(out.Errors[0].UserName))

	// A ResourceTypes filter narrows the response to just that type.
	filtered, err := client.BatchDescribeUserLimits(ctx, &quicksightsdk.BatchDescribeUserLimitsInput{
		AccountId:     aws.String(appTestAccountID),
		ResourceTypes: []types.ResourceType{types.ResourceTypeAgentHours},
		Users: []types.UserLimitsEntry{
			{Namespace: aws.String("default"), UserName: aws.String("alice")},
		},
	})
	require.NoError(t, err)
	require.Len(t, filtered.UserLimits, 1)
	require.Len(t, filtered.UserLimits[0].EffectiveLimits, 1)
	assert.Equal(t, types.ResourceTypeAgentHours, filtered.UserLimits[0].EffectiveLimits[0].ResourceType)
}

// TestRealClient_BatchDescribeUserLimits_EnterpriseEdition proves the
// account-edition mapping: an ENTERPRISE account resolves the higher
// documented default (50 GB vs STANDARD's 25 GB index storage).
func TestRealClient_BatchDescribeUserLimits_EnterpriseEdition(t *testing.T) {
	t.Parallel()

	backend := quicksight.NewInMemoryBackend(appTestAccountID, rtQSTestRegion)
	h := quicksight.NewHandler(backend)
	client := newTestQuickSightClient(t, h)
	ctx := t.Context()

	_, err := backend.CreateAccountSubscription(
		appTestAccountID, "enterprise-acct", "ENTERPRISE", "IAM_AND_QUICKSIGHT", "notify@example.com",
	)
	require.NoError(t, err)

	_, err = backend.RegisterUser(
		appTestAccountID, "default", "bob", "bob@example.com", "AUTHOR", "QUICKSIGHT", "", "", nil,
	)
	require.NoError(t, err)

	out, err := client.BatchDescribeUserLimits(ctx, &quicksightsdk.BatchDescribeUserLimitsInput{
		AccountId:     aws.String(appTestAccountID),
		ResourceTypes: []types.ResourceType{types.ResourceTypeIndexStorage},
		Users: []types.UserLimitsEntry{
			{Namespace: aws.String("default"), UserName: aws.String("bob")},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.UserLimits, 1)
	require.Len(t, out.UserLimits[0].EffectiveLimits, 1)
	assert.Equal(t, int64(50), aws.ToInt64(out.UserLimits[0].EffectiveLimits[0].LimitValue))
}
