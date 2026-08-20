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

// TestExperimentTemplate_TargetAccountConfigurationsCount proves
// GetExperimentTemplate/CreateExperimentTemplate carry the real
// targetAccountConfigurationsCount wire field (types.ExperimentTemplate,
// deserializers.go's awsRestjson1_deserializeDocumentExperimentTemplate), found
// missing incidentally while auditing ExperimentTemplate's nested shape.
func TestExperimentTemplate_TargetAccountConfigurationsCount(t *testing.T) {
	t.Parallel()

	backend := fis.NewInMemoryBackend(fisTestRegion, "000000000000")
	client, _ := newTestFISClient(t, fis.NewHandler(backend))
	ctx := t.Context()

	createOut, err := client.CreateExperimentTemplate(ctx, &fissdk.CreateExperimentTemplateInput{
		Description: aws.String("template for target account configuration count test"),
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
	require.NotNil(t, createOut.ExperimentTemplate)
	require.Equal(t, int64(0), aws.ToInt64(createOut.ExperimentTemplate.TargetAccountConfigurationsCount))
	tplID := aws.ToString(createOut.ExperimentTemplate.Id)

	_, err = client.CreateTargetAccountConfiguration(ctx, &fissdk.CreateTargetAccountConfigurationInput{
		ExperimentTemplateId: aws.String(tplID),
		AccountId:            aws.String("111111111111"),
		RoleArn:              aws.String("arn:aws:iam::111111111111:role/FISRole"),
	})
	require.NoError(t, err)

	getOut, err := client.GetExperimentTemplate(ctx, &fissdk.GetExperimentTemplateInput{Id: aws.String(tplID)})
	require.NoError(t, err)
	require.NotNil(t, getOut.ExperimentTemplate)
	assert.Equal(t, int64(1), aws.ToInt64(getOut.ExperimentTemplate.TargetAccountConfigurationsCount))
}

// TestExperimentAction_StartAfter proves a running experiment's action carries
// the real startAfter wire field (types.ExperimentAction, deserializers.go's
// awsRestjson1_deserializeDocumentExperimentAction), found missing incidentally
// while auditing Experiment's nested shape.
func TestExperimentAction_StartAfter(t *testing.T) {
	t.Parallel()

	backend := fis.NewInMemoryBackend(fisTestRegion, "000000000000")
	client, _ := newTestFISClient(t, fis.NewHandler(backend))
	ctx := t.Context()

	createOut, err := client.CreateExperimentTemplate(ctx, &fissdk.CreateExperimentTemplateInput{
		Description: aws.String("template for startAfter test"),
		RoleArn:     aws.String("arn:aws:iam::000000000000:role/FISRole"),
		StopConditions: []fistypes.CreateExperimentTemplateStopConditionInput{
			{Source: aws.String("none")},
		},
		Actions: map[string]fistypes.CreateExperimentTemplateActionInput{
			"firstAction": {
				ActionId:   aws.String("aws:fis:wait"),
				Parameters: map[string]string{"duration": "PT1S"},
			},
			"secondAction": {
				ActionId:   aws.String("aws:fis:wait"),
				Parameters: map[string]string{"duration": "PT1S"},
				StartAfter: []string{"firstAction"},
			},
		},
		Targets: map[string]fistypes.CreateExperimentTemplateTargetInput{},
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.ExperimentTemplate)
	tplID := createOut.ExperimentTemplate.Id

	startOut, err := client.StartExperiment(ctx, &fissdk.StartExperimentInput{ExperimentTemplateId: tplID})
	require.NoError(t, err)
	require.NotNil(t, startOut.Experiment)

	second, ok := startOut.Experiment.Actions["secondAction"]
	require.True(t, ok)
	assert.Equal(t, []string{"firstAction"}, second.StartAfter)

	first, ok := startOut.Experiment.Actions["firstAction"]
	require.True(t, ok)
	assert.Empty(t, first.StartAfter)
}
