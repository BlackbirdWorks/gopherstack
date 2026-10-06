package fis_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	fissdk "github.com/aws/aws-sdk-go-v2/service/fis"
	fistypes "github.com/aws/aws-sdk-go-v2/service/fis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateExperimentTemplate_OptionsKeepAccountTargeting(t *testing.T) {
	t.Parallel()

	cases := []struct {
		update   *fistypes.UpdateExperimentTemplateExperimentOptionsInput
		name     string
		wantMode fistypes.EmptyTargetResolutionMode
	}{
		{
			name:     "mode only keeps account targeting",
			update:   &fistypes.UpdateExperimentTemplateExperimentOptionsInput{EmptyTargetResolutionMode: "skip"},
			wantMode: "skip",
		},
		{
			name:     "empty options keep both",
			update:   &fistypes.UpdateExperimentTemplateExperimentOptionsInput{},
			wantMode: "fail",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, _ := newTestFISClient(t, newTestHandler(t))
			ctx := t.Context()

			created, err := client.CreateExperimentTemplate(ctx, &fissdk.CreateExperimentTemplateInput{
				Description: aws.String("d"),
				RoleArn:     aws.String("arn:aws:iam::000000000000:role/FISRole"),
				StopConditions: []fistypes.CreateExperimentTemplateStopConditionInput{
					{Source: aws.String("none")},
				},
				Actions: map[string]fistypes.CreateExperimentTemplateActionInput{
					"wait": {ActionId: aws.String("aws:fis:wait"), Parameters: map[string]string{"duration": "PT1S"}},
				},
				Targets: map[string]fistypes.CreateExperimentTemplateTargetInput{},
				ExperimentOptions: &fistypes.CreateExperimentTemplateExperimentOptionsInput{
					AccountTargeting:          fistypes.AccountTargetingMultiAccount,
					EmptyTargetResolutionMode: "fail",
				},
			})
			require.NoError(t, err)

			_, err = client.UpdateExperimentTemplate(ctx, &fissdk.UpdateExperimentTemplateInput{
				Id: created.ExperimentTemplate.Id, ExperimentOptions: tc.update,
			})
			require.NoError(t, err)

			got, err := client.GetExperimentTemplate(ctx, &fissdk.GetExperimentTemplateInput{
				Id: created.ExperimentTemplate.Id,
			})
			require.NoError(t, err)
			require.NotNil(t, got.ExperimentTemplate.ExperimentOptions)

			opts := got.ExperimentTemplate.ExperimentOptions
			assert.Equal(t, fistypes.AccountTargetingMultiAccount, opts.AccountTargeting)
			assert.Equal(t, tc.wantMode, opts.EmptyTargetResolutionMode)
		})
	}
}
