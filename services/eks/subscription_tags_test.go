package eks_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ekssdk "github.com/aws/aws-sdk-go-v2/service/eks"
	ekstypes "github.com/aws/aws-sdk-go-v2/service/eks/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eks"
)

// TestEksAnywhereSubscription_Tags_RealClient drives
// CreateEksAnywhereSubscription/DescribeEksAnywhereSubscription/
// ListEksAnywhereSubscriptions through the real aws-sdk-go-v2 client.
// types.EksAnywhereSubscription.Tags (eks@v1.98.0 deserializers.go's
// awsRestjson1_deserializeDocumentEksAnywhereSubscription, case "tags") was
// tracked on the backend model and set at create time, but subscriptionToJSON
// never emitted it on any of the three responses -- a real client's Tags
// always decoded empty regardless of what it created the subscription with.
func TestEksAnywhereSubscription_Tags_RealClient(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	h := eks.NewHandler(b)
	client := newTestEKSClient(t, h)
	ctx := t.Context()

	created, err := client.CreateEksAnywhereSubscription(ctx, &ekssdk.CreateEksAnywhereSubscriptionInput{
		Name: aws.String("sub-tags"),
		Term: &ekstypes.EksAnywhereSubscriptionTerm{
			Duration: 12,
			Unit:     ekstypes.EksAnywhereSubscriptionTermUnitMonths,
		},
		Tags: map[string]string{"team": "platform"},
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "platform"}, created.Subscription.Tags)

	described, err := client.DescribeEksAnywhereSubscription(ctx, &ekssdk.DescribeEksAnywhereSubscriptionInput{
		Id: created.Subscription.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "platform"}, described.Subscription.Tags)

	listed, err := client.ListEksAnywhereSubscriptions(ctx, &ekssdk.ListEksAnywhereSubscriptionsInput{})
	require.NoError(t, err)
	require.Len(t, listed.Subscriptions, 1)
	assert.Equal(t, map[string]string{"team": "platform"}, listed.Subscriptions[0].Tags)
}
