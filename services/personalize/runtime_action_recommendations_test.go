package personalize_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/personalize"
)

func TestValidateActionRecommenderCampaign(t *testing.T) {
	t.Parallel()

	t.Run("unknown_campaign", func(t *testing.T) {
		t.Parallel()

		b := personalize.NewInMemoryBackend("000000000000", "us-east-1")
		err := b.ValidateActionRecommenderCampaign("arn:aws:personalize:us-east-1:000000000000:campaign/missing")
		require.ErrorIs(t, err, personalize.ErrNotFound)
	})

	t.Run("wrong_recipe_type", func(t *testing.T) {
		t.Parallel()

		b := personalize.NewInMemoryBackend("000000000000", "us-east-1")
		h := personalize.NewHandler(b)
		personalizeCreateCampaign(t, h, "action-rec-backend-campaign")

		err := b.ValidateActionRecommenderCampaign(
			"arn:aws:personalize:us-east-1:000000000000:campaign/action-rec-backend-campaign",
		)
		require.ErrorIs(t, err, personalize.ErrValidation)
	})
}
