package neptune_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	neptunesdk "github.com/aws/aws-sdk-go-v2/service/neptune"
	"github.com/aws/aws-sdk-go-v2/service/neptune/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune"
)

// TestModifyEventSubscription_EventCategoriesKey proves EventCategories now
// reaches the backend under its real wire key. The real serializer
// (awsAwsquery_serializeDocumentEventCategoriesList, neptune@v1.48.4
// serializers.go:4971-4972) wraps each entry in "EventCategory", not the
// generic "member"; ModifyEventSubscription and DescribeEvents both read
// "EventCategories.member.N" and so always saw an empty list from a real
// client.
func TestModifyEventSubscription_EventCategoriesKey(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", testRegion)
	h := neptune.NewHandler(backend)
	client := newTestNeptuneClient(t, h)
	ctx := t.Context()

	_, err := client.CreateEventSubscription(ctx, &neptunesdk.CreateEventSubscriptionInput{
		SubscriptionName: aws.String("evcat-sub"),
		SnsTopicArn:      aws.String("arn:aws:sns:us-east-1:000000000000:topic"),
	})
	require.NoError(t, err)

	_, err = client.ModifyEventSubscription(ctx, &neptunesdk.ModifyEventSubscriptionInput{
		SubscriptionName: aws.String("evcat-sub"),
		EventCategories:  []string{"backup", "failover"},
	})
	require.NoError(t, err)

	out, err := client.DescribeEventSubscriptions(ctx, &neptunesdk.DescribeEventSubscriptionsInput{
		SubscriptionName: aws.String("evcat-sub"),
	})
	require.NoError(t, err)
	require.Len(t, out.EventSubscriptionsList, 1)
	assert.ElementsMatch(t, []string{"backup", "failover"}, out.EventSubscriptionsList[0].EventCategoriesList,
		"EventCategories sent under its real wire key must reach the subscription")
}

// TestCreateEventSubscription_EventCategories proves EventCategories set on
// CreateEventSubscription itself is honored. CreateEventSubscriptionInput's
// own serializer (awsAwsquery_serializeOpDocumentCreateEventSubscriptionInput,
// neptune@v1.48.4 serializers.go:5958-5972) calls the very same
// awsAwsquery_serializeDocumentEventCategoriesList used by
// ModifyEventSubscription -- confirmed independently on this op's own
// serializer, not inferred from that sibling -- so a real client's
// EventCategories arrives under "EventCategories.EventCategory.N" here too.
// Before this fix, handleCreateEventSubscription never read the field at
// all (not even under the wrong key), so it was silently dropped even though
// ModifyEventSubscription/DescribeEvents already parsed it correctly.
func TestCreateEventSubscription_EventCategories(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", testRegion)
	h := neptune.NewHandler(backend)
	client := newTestNeptuneClient(t, h)
	ctx := t.Context()

	out, err := client.CreateEventSubscription(ctx, &neptunesdk.CreateEventSubscriptionInput{
		SubscriptionName: aws.String("evcat-create-sub"),
		SnsTopicArn:      aws.String("arn:aws:sns:us-east-1:000000000000:topic"),
		EventCategories:  []string{"backup", "failover"},
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"backup", "failover"}, out.EventSubscription.EventCategoriesList,
		"CreateEventSubscription's own response must reflect the requested EventCategories")

	describeOut, err := client.DescribeEventSubscriptions(ctx, &neptunesdk.DescribeEventSubscriptionsInput{
		SubscriptionName: aws.String("evcat-create-sub"),
	})
	require.NoError(t, err)
	require.Len(t, describeOut.EventSubscriptionsList, 1)
	assert.ElementsMatch(t, []string{"backup", "failover"}, describeOut.EventSubscriptionsList[0].EventCategoriesList,
		"EventCategories set at creation time must persist and be visible on describe")
}

// TestDescribeEvents_EventCategoriesFilter proves DescribeEvents' Filter
// EventCategories reaches the backend under the same real wire key
// (EventCategories.EventCategory.N) and actually narrows the returned
// events, rather than silently matching nothing.
func TestDescribeEvents_EventCategoriesFilter(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", testRegion)
	h := neptune.NewHandler(backend)
	client := newTestNeptuneClient(t, h)
	ctx := t.Context()

	_, err := client.CreateDBCluster(ctx, &neptunesdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("evcat-cluster"),
		Engine:              aws.String("neptune"),
	})
	require.NoError(t, err)
	_, err = client.DeleteDBCluster(ctx, &neptunesdk.DeleteDBClusterInput{
		DBClusterIdentifier: aws.String("evcat-cluster"),
		SkipFinalSnapshot:   aws.Bool(true),
	})
	require.NoError(t, err)

	out, err := client.DescribeEvents(ctx, &neptunesdk.DescribeEventsInput{
		EventCategories: []string{"deletion"},
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.Events, "the deletion event should have matched the EventCategories filter")
	for _, e := range out.Events {
		assert.Contains(t, e.EventCategories, "deletion")
	}
}

// TestDescribeDBClusters_FilterValuesCardinality proves a multi-value Filter
// keeps every value. The real serializer
// (awsAwsquery_serializeDocumentFilterValueList, neptune@v1.48.4
// serializers.go:5012-5013) wraps Values in a repeated "Value" element, but
// the handler read only "Filters.Filter.N.Values.Value.1", silently dropping
// every value after the first -- a two-value engine-version filter behaved
// like a one-value filter and wrongly excluded a matching cluster.
func TestDescribeDBClusters_FilterValuesCardinality(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", testRegion)
	h := neptune.NewHandler(backend)
	client := newTestNeptuneClient(t, h)
	ctx := t.Context()

	_, err := client.CreateDBCluster(ctx, &neptunesdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("fv-cluster-a"),
		Engine:              aws.String("neptune"),
		EngineVersion:       aws.String("1.2.0.0"),
	})
	require.NoError(t, err)
	_, err = client.CreateDBCluster(ctx, &neptunesdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("fv-cluster-b"),
		Engine:              aws.String("neptune"),
		EngineVersion:       aws.String("1.3.0.0"),
	})
	require.NoError(t, err)

	out, err := client.DescribeDBClusters(ctx, &neptunesdk.DescribeDBClustersInput{
		Filters: []types.Filter{
			{Name: aws.String("engine-version"), Values: []string{"1.2.0.0", "1.3.0.0"}},
		},
	})
	require.NoError(t, err)

	ids := make([]string, 0, len(out.DBClusters))
	for _, c := range out.DBClusters {
		ids = append(ids, aws.ToString(c.DBClusterIdentifier))
	}
	assert.ElementsMatch(t, []string{"fv-cluster-a", "fv-cluster-b"}, ids,
		"both engine versions in the multi-value filter should have matched")
}
