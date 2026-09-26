package cloudtrail_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudtrailsdk "github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	"github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

// TestPutInsightSelectors_EventDataStoreInsightsDestinationRoundTrips covers
// gopherstack-6flj's GetInsightSelectors gap: the real
// PutInsightSelectorsInput/GetInsightSelectorsOutput both carry
// InsightsDestination (the ARN of the destination event data store that logs
// Insights events, required alongside EventDataStore to enable Insights on
// an event data store -- cloudtrail@v1.58.4 api_op_PutInsightSelectors.go:90-95,
// api_op_GetInsightSelectors.go's GetInsightSelectorsOutput.InsightsDestination).
// This backend previously accepted EventDataStore alone and silently dropped
// InsightsDestination entirely.
func TestPutInsightSelectors_EventDataStoreInsightsDestinationRoundTrips(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))

	source, err := client.CreateEventDataStore(t.Context(), &cloudtrailsdk.CreateEventDataStoreInput{
		Name: aws.String("insights-source"),
	})
	require.NoError(t, err)

	dest, err := client.CreateEventDataStore(t.Context(), &cloudtrailsdk.CreateEventDataStoreInput{
		Name: aws.String("insights-destination"),
	})
	require.NoError(t, err)

	_, err = client.PutInsightSelectors(t.Context(), &cloudtrailsdk.PutInsightSelectorsInput{
		EventDataStore:      source.EventDataStoreArn,
		InsightsDestination: dest.EventDataStoreArn,
		InsightSelectors: []types.InsightSelector{
			{InsightType: types.InsightTypeApiCallRateInsight},
		},
	})
	require.NoError(t, err)

	got, err := client.GetInsightSelectors(t.Context(), &cloudtrailsdk.GetInsightSelectorsInput{
		EventDataStore: source.EventDataStoreArn,
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(dest.EventDataStoreArn), aws.ToString(got.InsightsDestination))
	require.Len(t, got.InsightSelectors, 1)
	assert.Equal(t, types.InsightTypeApiCallRateInsight, got.InsightSelectors[0].InsightType)
}
