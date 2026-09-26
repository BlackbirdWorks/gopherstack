package personalize_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/personalizeruntime"
	personalizeruntimetypes "github.com/aws/aws-sdk-go-v2/service/personalizeruntime/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/personalize"
)

// TestGetActionRecommendations_RealSDKClient asserts the two real declared
// error paths (ResourceNotFoundException for an unknown campaign,
// InvalidInputException for a campaign not trained with a
// PERSONALIZED_ACTIONS recipe -- the only recipe family
// GetActionRecommendations accepts). This backend's built-in recipe catalog
// has no PERSONALIZED_ACTIONS entry, so an existing campaign always hits the
// latter -- see PARITY.md for why no scored action list is ever fabricated.
func TestGetActionRecommendations_RealSDKClient(t *testing.T) {
	t.Parallel()

	t.Run("unknown_campaign_is_resource_not_found", func(t *testing.T) {
		t.Parallel()

		b := personalize.NewInMemoryBackend("000000000000", "us-east-1")
		h := personalize.NewHandler(b)
		client := newTestPersonalizeRuntimeClient(t, h)

		_, err := client.GetActionRecommendations(t.Context(), &personalizeruntime.GetActionRecommendationsInput{
			CampaignArn: aws.String(
				"arn:aws:personalize:us-east-1:000000000000:campaign/does-not-exist",
			),
			UserId: aws.String("user-real-sdk"),
		})
		require.Error(t, err)

		var nf *personalizeruntimetypes.ResourceNotFoundException
		require.ErrorAs(t, err, &nf)
	})

	t.Run("wrong_recipe_type_is_invalid_input", func(t *testing.T) {
		t.Parallel()

		b := personalize.NewInMemoryBackend("000000000000", "us-east-1")
		h := personalize.NewHandler(b)
		client := newTestPersonalizeRuntimeClient(t, h)
		personalizeCreateCampaign(t, h, "action-rec-campaign")

		_, err := client.GetActionRecommendations(t.Context(), &personalizeruntime.GetActionRecommendationsInput{
			CampaignArn: aws.String(
				"arn:aws:personalize:us-east-1:000000000000:campaign/action-rec-campaign",
			),
			UserId: aws.String("user-real-sdk"),
		})
		require.Error(t, err)

		var badReq *personalizeruntimetypes.InvalidInputException
		require.ErrorAs(t, err, &badReq)
	})
}
