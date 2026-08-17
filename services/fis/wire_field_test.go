package fis_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	fissdk "github.com/aws/aws-sdk-go-v2/service/fis"
	fistypes "github.com/aws/aws-sdk-go-v2/service/fis/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/fis"
)

const fisTestRegion = "us-east-1"

func newTestFISClient(t *testing.T, h *fis.Handler) *fissdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(fisTestRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return fissdk.NewFromConfig(cfg, func(o *fissdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

func TestFISListOps_NarrowSummaryParity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		test func(t *testing.T, client *fissdk.Client)
		name string
	}{
		{
			name: "list_experiment_templates_narrow_shape",
			test: func(t *testing.T, client *fissdk.Client) {
				t.Helper()
				ctx := t.Context()

				createOut, err := client.CreateExperimentTemplate(ctx, &fissdk.CreateExperimentTemplateInput{
					Description: aws.String("template for narrow list test"),
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
					Tags: map[string]string{
						"env": "test",
					},
				})
				require.NoError(t, err)
				require.NotNil(t, createOut.ExperimentTemplate)
				tplID := createOut.ExperimentTemplate.Id

				listOut, err := client.ListExperimentTemplates(ctx, &fissdk.ListExperimentTemplatesInput{})
				require.NoError(t, err)
				require.NotEmpty(t, listOut.ExperimentTemplates)

				var found *fistypes.ExperimentTemplateSummary
				for i := range listOut.ExperimentTemplates {
					if aws.ToString(listOut.ExperimentTemplates[i].Id) == aws.ToString(tplID) {
						found = &listOut.ExperimentTemplates[i]

						break
					}
				}
				require.NotNil(t, found)
				assert.Equal(t, "template for narrow list test", aws.ToString(found.Description))
				assert.Equal(t, "test", found.Tags["env"])
			},
		},
		{
			name: "list_experiments_narrow_shape",
			test: func(t *testing.T, client *fissdk.Client) {
				t.Helper()
				ctx := t.Context()

				createTplOut, err := client.CreateExperimentTemplate(ctx, &fissdk.CreateExperimentTemplateInput{
					Description: aws.String("template for experiment narrow list test"),
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
				require.NotNil(t, createTplOut.ExperimentTemplate)
				tplID := createTplOut.ExperimentTemplate.Id

				startExpOut, err := client.StartExperiment(ctx, &fissdk.StartExperimentInput{
					ExperimentTemplateId: tplID,
					Tags: map[string]string{
						"team": "chaos",
					},
				})
				require.NoError(t, err)
				require.NotNil(t, startExpOut.Experiment)
				expID := startExpOut.Experiment.Id

				listExpOut, err := client.ListExperiments(ctx, &fissdk.ListExperimentsInput{})
				require.NoError(t, err)
				require.NotEmpty(t, listExpOut.Experiments)

				var found *fistypes.ExperimentSummary
				for i := range listExpOut.Experiments {
					if aws.ToString(listExpOut.Experiments[i].Id) == aws.ToString(expID) {
						found = &listExpOut.Experiments[i]

						break
					}
				}
				require.NotNil(t, found)
				assert.Equal(t, aws.ToString(tplID), aws.ToString(found.ExperimentTemplateId))
				assert.Equal(t, "chaos", found.Tags["team"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := fis.NewInMemoryBackend(fisTestRegion, "000000000000")
			client := newTestFISClient(t, fis.NewHandler(backend))
			tt.test(t, client)
		})
	}
}
