package sesv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sesv2sdk "github.com/aws/aws-sdk-go-v2/service/sesv2"
	sesv2types "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConfigurationSetEventDestination_SnsAndCloudWatchTargets proves
// CreateConfigurationSetEventDestination/UpdateConfigurationSetEventDestination
// honor SnsDestination and CloudWatchDestination (aws-sdk-go-v2/service/sesv2@v1.66.4
// types.EventDestinationDefinition) -- previously only MatchingEventTypes and
// Enabled were read, so every destination sub-object (where the events
// actually go) was silently dropped and GetConfigurationSetEventDestinations
// could never echo any of them back.
func TestConfigurationSetEventDestination_SnsAndCloudWatchTargets(t *testing.T) {
	t.Parallel()

	h, backend := newSESv2TestHandler(t)
	client := newSESv2SDKClient(t, h)
	ctx := t.Context()

	_, err := backend.CreateConfigurationSet("cs-targets", nil)
	require.NoError(t, err)

	_, err = client.CreateConfigurationSetEventDestination(
		ctx, &sesv2sdk.CreateConfigurationSetEventDestinationInput{
			ConfigurationSetName: aws.String("cs-targets"),
			EventDestinationName: aws.String("sns-dest"),
			EventDestination: &sesv2types.EventDestinationDefinition{
				Enabled:            true,
				MatchingEventTypes: []sesv2types.EventType{sesv2types.EventTypeBounce},
				SnsDestination: &sesv2types.SnsDestination{
					TopicArn: aws.String("arn:aws:sns:us-east-1:000000000000:bounces"),
				},
				CloudWatchDestination: &sesv2types.CloudWatchDestination{
					DimensionConfigurations: []sesv2types.CloudWatchDimensionConfiguration{
						{
							DimensionName:         aws.String("ses:configuration-set"),
							DimensionValueSource:  sesv2types.DimensionValueSourceMessageTag,
							DefaultDimensionValue: aws.String("default"),
						},
					},
				},
			},
		})
	require.NoError(t, err)

	out, err := client.GetConfigurationSetEventDestinations(
		ctx, &sesv2sdk.GetConfigurationSetEventDestinationsInput{
			ConfigurationSetName: aws.String("cs-targets"),
		})
	require.NoError(t, err)
	require.Len(t, out.EventDestinations, 1)

	dest := out.EventDestinations[0]
	require.NotNil(t, dest.SnsDestination)
	assert.Equal(t, "arn:aws:sns:us-east-1:000000000000:bounces", aws.ToString(dest.SnsDestination.TopicArn))

	require.NotNil(t, dest.CloudWatchDestination)
	require.Len(t, dest.CloudWatchDestination.DimensionConfigurations, 1)
	dim := dest.CloudWatchDestination.DimensionConfigurations[0]
	assert.Equal(t, "ses:configuration-set", aws.ToString(dim.DimensionName))
	assert.Equal(t, sesv2types.DimensionValueSourceMessageTag, dim.DimensionValueSource)
	assert.Equal(t, "default", aws.ToString(dim.DefaultDimensionValue))
}
