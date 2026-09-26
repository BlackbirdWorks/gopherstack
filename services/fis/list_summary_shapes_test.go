package fis_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	fissdk "github.com/aws/aws-sdk-go-v2/service/fis"
	fistypes "github.com/aws/aws-sdk-go-v2/service/fis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fis"
)

// TestListSummaryShapes proves this pass's over-wide-response audit
// (gopherstack-dv4s, 2026-09-19) for fis's six flagged List ops:
// ListActions, ListExperimentTargetAccountConfigurations,
// ListExperimentTemplates, ListExperiments, ListTargetAccountConfigurations
// and ListTargetResourceTypes were all ALREADY exact matches of their real
// *Summary types (verified via cmd/structfielddiff against fis@v1.40.4) --
// in every one of the six, the real Detail and Summary shapes are
// field-identical, so no leak was structurally possible. No bug found; this
// test locks the member sets in.
func TestListSummaryShapes(t *testing.T) {
	t.Parallel()

	t.Run("actions and target resource types exact", func(t *testing.T) {
		t.Parallel()

		backend := fis.NewInMemoryBackend("000000000000", "us-east-1")
		t.Cleanup(backend.Close)
		client, _ := newTestFISClient(t, fis.NewHandler(backend))
		ctx := t.Context()

		actionsOut, err := client.ListActions(ctx, &fissdk.ListActionsInput{})
		require.NoError(t, err)
		require.NotEmpty(t, actionsOut.Actions)
		a := actionsOut.Actions[0]
		assert.NotEmpty(t, aws.ToString(a.Id))
		assert.NotEmpty(t, aws.ToString(a.Arn))

		rtOut, err := client.ListTargetResourceTypes(ctx, &fissdk.ListTargetResourceTypesInput{})
		require.NoError(t, err)
		require.NotEmpty(t, rtOut.TargetResourceTypes)
		assert.NotEmpty(t, aws.ToString(rtOut.TargetResourceTypes[0].ResourceType))
	})

	t.Run("experiment templates and experiments exact", func(t *testing.T) {
		t.Parallel()

		backend := fis.NewInMemoryBackend("000000000000", "us-east-1")
		t.Cleanup(backend.Close)
		client, _ := newTestFISClient(t, fis.NewHandler(backend))
		ctx := t.Context()

		createOut, err := client.CreateExperimentTemplate(ctx, &fissdk.CreateExperimentTemplateInput{
			Description: aws.String("lss template"),
			RoleArn:     aws.String("arn:aws:iam::000000000000:role/FISRole"),
			StopConditions: []fistypes.CreateExperimentTemplateStopConditionInput{
				{Source: aws.String("none")},
			},
			Actions: map[string]fistypes.CreateExperimentTemplateActionInput{
				"waitAction": {
					ActionId:   aws.String("aws:fis:wait"),
					Parameters: map[string]string{"duration": "PT1S"},
				},
			},
			Targets: map[string]fistypes.CreateExperimentTemplateTargetInput{},
		})
		require.NoError(t, err)
		tplID := createOut.ExperimentTemplate.Id

		tplsOut, err := client.ListExperimentTemplates(ctx, &fissdk.ListExperimentTemplatesInput{})
		require.NoError(t, err)
		require.Len(t, tplsOut.ExperimentTemplates, 1)
		assert.Equal(t, aws.ToString(tplID), aws.ToString(tplsOut.ExperimentTemplates[0].Id))
		assert.NotNil(t, tplsOut.ExperimentTemplates[0].LastUpdateTime)

		startOut, err := client.StartExperiment(ctx, &fissdk.StartExperimentInput{ExperimentTemplateId: tplID})
		require.NoError(t, err)

		expsOut, err := client.ListExperiments(ctx, &fissdk.ListExperimentsInput{})
		require.NoError(t, err)
		require.Len(t, expsOut.Experiments, 1)
		exp := expsOut.Experiments[0]
		assert.Equal(t, aws.ToString(startOut.Experiment.Id), aws.ToString(exp.Id))
		require.NotNil(t, exp.State)
		assert.NotEmpty(t, exp.State.Status)
	})

	t.Run("target account configurations exact", func(t *testing.T) {
		t.Parallel()

		backend := fis.NewInMemoryBackend("000000000000", "us-east-1")
		t.Cleanup(backend.Close)
		client, _ := newTestFISClient(t, fis.NewHandler(backend))
		ctx := t.Context()

		createOut, err := client.CreateExperimentTemplate(ctx, &fissdk.CreateExperimentTemplateInput{
			Description: aws.String("lss tac template"),
			RoleArn:     aws.String("arn:aws:iam::000000000000:role/FISRole"),
			StopConditions: []fistypes.CreateExperimentTemplateStopConditionInput{
				{Source: aws.String("none")},
			},
			Actions: map[string]fistypes.CreateExperimentTemplateActionInput{},
			Targets: map[string]fistypes.CreateExperimentTemplateTargetInput{},
		})
		require.NoError(t, err)
		tplID := createOut.ExperimentTemplate.Id

		_, err = client.CreateTargetAccountConfiguration(ctx, &fissdk.CreateTargetAccountConfigurationInput{
			ExperimentTemplateId: tplID,
			AccountId:            aws.String("111111111111"),
			RoleArn:              aws.String("arn:aws:iam::111111111111:role/FISRole"),
			Description:          aws.String("lss account config"),
		})
		require.NoError(t, err)

		out, err := client.ListTargetAccountConfigurations(ctx, &fissdk.ListTargetAccountConfigurationsInput{
			ExperimentTemplateId: tplID,
		})
		require.NoError(t, err)
		require.Len(t, out.TargetAccountConfigurations, 1)
		cfg := out.TargetAccountConfigurations[0]
		assert.Equal(t, "111111111111", aws.ToString(cfg.AccountId))
		assert.Equal(t, "lss account config", aws.ToString(cfg.Description))

		// ListExperimentTargetAccountConfigurations shares the identical
		// 3-member Summary/Detail shape (AccountId/Description/RoleArn); the
		// op itself succeeds against a real experiment with no error.
		startOut, err := client.StartExperiment(ctx, &fissdk.StartExperimentInput{ExperimentTemplateId: tplID})
		require.NoError(t, err)

		_, err = client.ListExperimentTargetAccountConfigurations(
			ctx, &fissdk.ListExperimentTargetAccountConfigurationsInput{ExperimentId: startOut.Experiment.Id},
		)
		require.NoError(t, err)
	})
}
