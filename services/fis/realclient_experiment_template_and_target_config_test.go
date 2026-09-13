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

// TestRealClient_ExperimentTemplateAndTargetConfig drives fis's remaining typed-coverage-blind ops
// (gopherstack-n3zi) through the real aws-sdk-go-v2 client:
// DeleteTargetAccountConfiguration, GetAction, GetExperiment,
// GetExperimentTargetAccountConfiguration, GetTargetAccountConfiguration,
// GetTargetResourceType, ListExperimentResolvedTargets,
// ListExperimentTargetAccountConfigurations, ListTargetAccountConfigurations,
// StopExperiment, UpdateExperimentTemplate, UpdateTargetAccountConfiguration.
func TestRealClient_ExperimentTemplateAndTargetConfig(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "action and target resource type catalog", run: func(t *testing.T) {
			t.Helper()

			backend := fis.NewInMemoryBackend("000000000000", "us-east-1")
			client, _ := newTestFISClient(t, fis.NewHandler(backend))
			ctx := t.Context()

			getActionOut, err := client.GetAction(
				ctx,
				&fissdk.GetActionInput{Id: aws.String("aws:ec2:reboot-instances")},
			)
			require.NoError(t, err)
			require.NotNil(t, getActionOut.Action)
			assert.NotEmpty(t, aws.ToString(getActionOut.Action.Description))

			getRTOut, err := client.GetTargetResourceType(ctx, &fissdk.GetTargetResourceTypeInput{
				ResourceType: aws.String("aws:ec2:instance"),
			})
			require.NoError(t, err)
			require.NotNil(t, getRTOut.TargetResourceType)
			assert.Equal(t, "aws:ec2:instance", aws.ToString(getRTOut.TargetResourceType.ResourceType))
		}},
		{name: "experiment template lifecycle", run: func(t *testing.T) {
			t.Helper()

			backend := fis.NewInMemoryBackend("000000000000", "us-east-1")
			client, _ := newTestFISClient(t, fis.NewHandler(backend))
			ctx := t.Context()

			createOut, err := client.CreateExperimentTemplate(ctx, &fissdk.CreateExperimentTemplateInput{
				Description: aws.String("s21 template"),
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

			updOut, err := client.UpdateExperimentTemplate(ctx, &fissdk.UpdateExperimentTemplateInput{
				Id:          tplID,
				Description: aws.String("s21 template updated"),
			})
			require.NoError(t, err)
			require.NotNil(t, updOut.ExperimentTemplate)
			assert.Equal(t, "s21 template updated", aws.ToString(updOut.ExperimentTemplate.Description))

			createTACOut, err := client.CreateTargetAccountConfiguration(
				ctx, &fissdk.CreateTargetAccountConfigurationInput{
					ExperimentTemplateId: tplID,
					AccountId:            aws.String("111111111111"),
					RoleArn:              aws.String("arn:aws:iam::111111111111:role/FISRole"),
					Description:          aws.String("s21 account config"),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, createTACOut.TargetAccountConfiguration)

			getTACOut, err := client.GetTargetAccountConfiguration(ctx, &fissdk.GetTargetAccountConfigurationInput{
				ExperimentTemplateId: tplID,
				AccountId:            aws.String("111111111111"),
			})
			require.NoError(t, err)
			require.NotNil(t, getTACOut.TargetAccountConfiguration)
			assert.Equal(t, "s21 account config", aws.ToString(getTACOut.TargetAccountConfiguration.Description))

			updTACOut, err := client.UpdateTargetAccountConfiguration(
				ctx, &fissdk.UpdateTargetAccountConfigurationInput{
					ExperimentTemplateId: tplID,
					AccountId:            aws.String("111111111111"),
					Description:          aws.String("s21 account config updated"),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, updTACOut.TargetAccountConfiguration)
			assert.Equal(
				t,
				"s21 account config updated",
				aws.ToString(updTACOut.TargetAccountConfiguration.Description),
			)

			listTACOut, err := client.ListTargetAccountConfigurations(
				ctx, &fissdk.ListTargetAccountConfigurationsInput{ExperimentTemplateId: tplID},
			)
			require.NoError(t, err)
			require.Len(t, listTACOut.TargetAccountConfigurations, 1)
			assert.Equal(t, "111111111111", aws.ToString(listTACOut.TargetAccountConfigurations[0].AccountId))

			delTACOut, err := client.DeleteTargetAccountConfiguration(
				ctx, &fissdk.DeleteTargetAccountConfigurationInput{
					ExperimentTemplateId: tplID,
					AccountId:            aws.String("111111111111"),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, delTACOut.TargetAccountConfiguration)
			assert.Equal(t, "111111111111", aws.ToString(delTACOut.TargetAccountConfiguration.AccountId))

			listTACOut2, err := client.ListTargetAccountConfigurations(
				ctx, &fissdk.ListTargetAccountConfigurationsInput{ExperimentTemplateId: tplID},
			)
			require.NoError(t, err)
			assert.Empty(t, listTACOut2.TargetAccountConfigurations)
		}},
		{name: "experiment lifecycle", run: func(t *testing.T) {
			t.Helper()

			backend := fis.NewInMemoryBackend("000000000000", "us-east-1")
			client, _ := newTestFISClient(t, fis.NewHandler(backend))
			ctx := t.Context()

			createOut, err := client.CreateExperimentTemplate(ctx, &fissdk.CreateExperimentTemplateInput{
				Description: aws.String("s21 experiment template"),
				RoleArn:     aws.String("arn:aws:iam::000000000000:role/FISRole"),
				StopConditions: []fistypes.CreateExperimentTemplateStopConditionInput{
					{Source: aws.String("none")},
				},
				Actions: map[string]fistypes.CreateExperimentTemplateActionInput{
					"waitAction": {
						ActionId:   aws.String("aws:fis:wait"),
						Parameters: map[string]string{"duration": "PT30S"},
					},
				},
				Targets: map[string]fistypes.CreateExperimentTemplateTargetInput{},
			})
			require.NoError(t, err)
			tplID := createOut.ExperimentTemplate.Id

			_, err = client.CreateTargetAccountConfiguration(ctx, &fissdk.CreateTargetAccountConfigurationInput{
				ExperimentTemplateId: tplID,
				AccountId:            aws.String("222222222222"),
				RoleArn:              aws.String("arn:aws:iam::222222222222:role/FISRole"),
			})
			require.NoError(t, err)

			startOut, err := client.StartExperiment(ctx, &fissdk.StartExperimentInput{ExperimentTemplateId: tplID})
			require.NoError(t, err)
			require.NotNil(t, startOut.Experiment)
			expID := startOut.Experiment.Id

			getExpOut, err := client.GetExperiment(ctx, &fissdk.GetExperimentInput{Id: expID})
			require.NoError(t, err)
			require.NotNil(t, getExpOut.Experiment)
			assert.Equal(t, aws.ToString(expID), aws.ToString(getExpOut.Experiment.Id))
			assert.Equal(t, aws.ToString(tplID), aws.ToString(getExpOut.Experiment.ExperimentTemplateId))

			getExpTACOut, err := client.GetExperimentTargetAccountConfiguration(
				ctx, &fissdk.GetExperimentTargetAccountConfigurationInput{
					ExperimentId: expID,
					AccountId:    aws.String("222222222222"),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, getExpTACOut.TargetAccountConfiguration)
			assert.Equal(t, "222222222222", aws.ToString(getExpTACOut.TargetAccountConfiguration.AccountId))

			listExpTACOut, err := client.ListExperimentTargetAccountConfigurations(
				ctx, &fissdk.ListExperimentTargetAccountConfigurationsInput{ExperimentId: expID},
			)
			require.NoError(t, err)
			require.Len(t, listExpTACOut.TargetAccountConfigurations, 1)
			assert.Equal(t, "222222222222", aws.ToString(listExpTACOut.TargetAccountConfigurations[0].AccountId))

			resolvedOut, err := client.ListExperimentResolvedTargets(
				ctx, &fissdk.ListExperimentResolvedTargetsInput{ExperimentId: expID},
			)
			require.NoError(t, err)
			assert.Empty(t, resolvedOut.ResolvedTargets, "template declared no targets")

			stopOut, err := client.StopExperiment(ctx, &fissdk.StopExperimentInput{Id: expID})
			require.NoError(t, err)
			require.NotNil(t, stopOut.Experiment)
			assert.NotEqual(t, fistypes.ExperimentStatusCompleted, stopOut.Experiment.State.Status)
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
