package codepipeline_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cpsdk "github.com/aws/aws-sdk-go-v2/service/codepipeline"
	"github.com/aws/aws-sdk-go-v2/service/codepipeline/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codepipeline"
)

func TestDeleteCustomActionType_InUseIsValidationException(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		provider string
		inUse    bool
	}{
		{name: "in use by pipeline", provider: "InUseA", inUse: true},
		{name: "in use other provider", provider: "InUseB", inUse: true},
		{name: "unreferenced deletes", provider: "FreeA", inUse: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			client := newTestCodePipelineClient(t, h)

			_, err := client.CreateCustomActionType(t.Context(), &cpsdk.CreateCustomActionTypeInput{
				Category:              types.ActionCategoryBuild,
				Provider:              aws.String(tt.provider),
				Version:               aws.String("1"),
				InputArtifactDetails:  &types.ArtifactDetails{MinimumCount: 0, MaximumCount: 5},
				OutputArtifactDetails: &types.ArtifactDetails{MinimumCount: 0, MaximumCount: 5},
			})
			require.NoError(t, err)

			if tt.inUse {
				p := samplePipeline("uses-" + tt.provider)
				p.Stages[0].Actions[0].ActionTypeID = codepipeline.ActionTypeID{
					Category: "Build", Owner: "Custom", Provider: tt.provider, Version: "1",
				}
				_, err = h.Backend.CreatePipeline(t.Context(), p, nil)
				require.NoError(t, err)
			}

			_, err = client.DeleteCustomActionType(t.Context(), &cpsdk.DeleteCustomActionTypeInput{
				Category: types.ActionCategoryBuild,
				Provider: aws.String(tt.provider),
				Version:  aws.String("1"),
			})

			if !tt.inUse {
				require.NoError(t, err)

				return
			}

			var ve *types.ValidationException
			require.ErrorAs(t, err, &ve)
		})
	}
}
