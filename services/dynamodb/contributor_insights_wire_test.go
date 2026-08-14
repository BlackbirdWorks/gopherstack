// Package dynamodb_test covers gopherstack-rrtz item 1 (ListContributorInsights
// ignores TableName, MaxResults and NextToken entirely, always listing every
// table in-region) and item 3 (UpdateContributorInsights drops
// ContributorInsightsMode; the backend didn't track mode, and Describe/List
// didn't echo it either). Each test drives the real aws-sdk-go-v2 client over
// HTTP so the wire decode -> backend conversion is exercised end to end.
package dynamodb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func TestListContributorInsights_TableNameFilter(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	t.Cleanup(backend.Close)
	client := newTestDynamoDBClient(t, dynamodb.NewHandler(backend))

	createPPRTableViaClient(t, client, "ci-a")
	createPPRTableViaClient(t, client, "ci-b")

	for _, name := range []string{"ci-a", "ci-b"} {
		_, err := client.UpdateContributorInsights(t.Context(), &sdk.UpdateContributorInsightsInput{
			TableName:                 aws.String(name),
			ContributorInsightsAction: types.ContributorInsightsActionEnable,
		})
		require.NoError(t, err)
	}

	unfiltered, err := client.ListContributorInsights(t.Context(), &sdk.ListContributorInsightsInput{})
	require.NoError(t, err)
	require.Len(t, unfiltered.ContributorInsightsSummaries, 2)

	filtered, err := client.ListContributorInsights(t.Context(), &sdk.ListContributorInsightsInput{
		TableName: aws.String("ci-a"),
	})
	require.NoError(t, err)
	require.Len(t, filtered.ContributorInsightsSummaries, 1)
	assert.Less(t, len(filtered.ContributorInsightsSummaries), len(unfiltered.ContributorInsightsSummaries))
	assert.Equal(t, "ci-a", aws.ToString(filtered.ContributorInsightsSummaries[0].TableName))
}

func TestUpdateContributorInsights_ModeRoundTrip(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	t.Cleanup(backend.Close)
	client := newTestDynamoDBClient(t, dynamodb.NewHandler(backend))

	createPPRTableViaClient(t, client, "ci-mode")

	updateOut, err := client.UpdateContributorInsights(t.Context(), &sdk.UpdateContributorInsightsInput{
		TableName:                 aws.String("ci-mode"),
		ContributorInsightsAction: types.ContributorInsightsActionEnable,
		ContributorInsightsMode:   types.ContributorInsightsModeAccessedAndThrottledKeys,
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		types.ContributorInsightsModeAccessedAndThrottledKeys,
		updateOut.ContributorInsightsMode,
	)

	descOut, err := client.DescribeContributorInsights(t.Context(), &sdk.DescribeContributorInsightsInput{
		TableName: aws.String("ci-mode"),
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		types.ContributorInsightsModeAccessedAndThrottledKeys,
		descOut.ContributorInsightsMode,
	)

	listOut, err := client.ListContributorInsights(t.Context(), &sdk.ListContributorInsightsInput{
		TableName: aws.String("ci-mode"),
	})
	require.NoError(t, err)
	require.Len(t, listOut.ContributorInsightsSummaries, 1)
	assert.Equal(
		t,
		types.ContributorInsightsModeAccessedAndThrottledKeys,
		listOut.ContributorInsightsSummaries[0].ContributorInsightsMode,
	)
}
