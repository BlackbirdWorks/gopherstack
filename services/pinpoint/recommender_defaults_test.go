package pinpoint_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	pinpointsdk "github.com/aws/aws-sdk-go-v2/service/pinpoint"
	"github.com/aws/aws-sdk-go-v2/service/pinpoint/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecommenderConfiguration_DocumentedDefaults(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		idType   *string
		perMsg   *int32
		wantType string
		wantPer  int32
	}{
		{name: "omitted", wantType: "PINPOINT_ENDPOINT_ID", wantPer: 5},
		{
			name:     "explicit",
			idType:   aws.String("PINPOINT_USER_ID"),
			perMsg:   aws.Int32(3),
			wantType: "PINPOINT_USER_ID",
			wantPer:  3,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestPinpointClient(t, newHandlerForTest(t))
			created, err := c.CreateRecommenderConfiguration(
				t.Context(),
				&pinpointsdk.CreateRecommenderConfigurationInput{
					CreateRecommenderConfiguration: &types.CreateRecommenderConfigurationShape{
						RecommendationProviderRoleArn: aws.String("arn:aws:iam::123456789012:role/r"),
						RecommendationProviderUri: aws.String(
							"arn:aws:personalize:us-east-1:123456789012:campaign/c",
						),
						RecommendationProviderIdType: tc.idType,
						RecommendationsPerMessage:    tc.perMsg,
					},
				},
			)
			require.NoError(t, err)

			got, err := c.GetRecommenderConfiguration(t.Context(), &pinpointsdk.GetRecommenderConfigurationInput{
				RecommenderId: created.RecommenderConfigurationResponse.Id,
			})
			require.NoError(t, err)
			assert.Equal(
				t,
				tc.wantType,
				aws.ToString(got.RecommenderConfigurationResponse.RecommendationProviderIdType),
			)
			assert.Equal(t, tc.wantPer, aws.ToInt32(got.RecommenderConfigurationResponse.RecommendationsPerMessage))
		})
	}
}
