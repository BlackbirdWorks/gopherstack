package main

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	fissdk "github.com/aws/aws-sdk-go-v2/service/fis"
	fistypes "github.com/aws/aws-sdk-go-v2/service/fis/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	fisbackend "github.com/blackbirdworks/gopherstack/services/fis"
)

func TestInitializeServices_FISTargetResolution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		emptyMode      fistypes.EmptyTargetResolutionMode
		tagValue       string
		wantStatus     fistypes.ExperimentStatus
		wantInstanceOn string
		filters        []fistypes.ExperimentTemplateTargetInputFilter
	}{
		{
			name:           "tag-match-stops-instance",
			tagValue:       "chaos",
			wantStatus:     fistypes.ExperimentStatusCompleted,
			wantInstanceOn: "stopped",
		},
		{
			name:           "filter-match",
			tagValue:       "chaos",
			wantStatus:     fistypes.ExperimentStatusCompleted,
			wantInstanceOn: "stopped",
			filters: []fistypes.ExperimentTemplateTargetInputFilter{
				{Path: aws.String("State.Name"), Values: []string{"running"}},
			},
		},
		{
			name:           "filter-miss-fails",
			tagValue:       "chaos",
			wantStatus:     fistypes.ExperimentStatusFailed,
			wantInstanceOn: "running",
			filters: []fistypes.ExperimentTemplateTargetInputFilter{
				{Path: aws.String("State.Name"), Values: []string{"stopped"}},
			},
		},
		{
			name:           "no-match-fails",
			tagValue:       "other",
			wantStatus:     fistypes.ExperimentStatusFailed,
			wantInstanceOn: "running",
		},
		{
			name: "no-match-skip", tagValue: "other", emptyMode: fistypes.EmptyTargetResolutionModeSkip,
			wantStatus: fistypes.ExperimentStatusCompleted, wantInstanceOn: "running",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			services, err := initializeServices(newTestAppContext(t, 19700, 19800))
			require.NoError(t, err)

			byName := serviceByName(services)

			ec2H, ok := byName["EC2"].(*ec2backend.Handler)
			require.True(t, ok)

			fisH, ok := byName["FIS"].(*fisbackend.Handler)
			require.True(t, ok)

			insts, err := ec2H.Backend.RunInstances("ami-12345678", "t3.micro", "", 1)
			require.NoError(t, err)
			require.NoError(t, ec2H.Backend.CreateTags([]string{insts[0].ID}, map[string]string{"env": "chaos"}))

			require.Eventually(t, func() bool {
				got := ec2H.Backend.DescribeInstances([]string{insts[0].ID}, "")

				return len(got) == 1 && got[0].State.Name == "running"
			}, 10*time.Second, 20*time.Millisecond)

			e := echo.New()
			registry := service.NewRegistry()
			require.NoError(t, registry.Register(fisH))
			e.Use(service.NewServiceRouter(registry).RouteHandler())

			srv := httptest.NewServer(e)
			t.Cleanup(srv.Close)

			cfg, err := awscfg.LoadDefaultConfig(
				t.Context(),
				awscfg.WithRegion("us-east-1"),
				awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
			)
			require.NoError(t, err)

			client := fissdk.NewFromConfig(cfg, func(o *fissdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })

			tplIn := &fissdk.CreateExperimentTemplateInput{
				ClientToken: aws.String("tpl-" + tt.name),
				Description: aws.String("resolve by tag"),
				RoleArn:     aws.String("arn:aws:iam::000000000000:role/fis"),
				StopConditions: []fistypes.CreateExperimentTemplateStopConditionInput{
					{Source: aws.String("none")},
				},
				Targets: map[string]fistypes.CreateExperimentTemplateTargetInput{
					"Instances": {
						ResourceType:  aws.String("aws:ec2:instance"),
						SelectionMode: aws.String("ALL"),
						ResourceTags:  map[string]string{"env": tt.tagValue},
						Filters:       tt.filters,
					},
				},
				Actions: map[string]fistypes.CreateExperimentTemplateActionInput{
					"stop": {
						ActionId: aws.String("aws:ec2:stop-instances"),
						Targets:  map[string]string{"Instances": "Instances"},
					},
				},
			}
			if tt.emptyMode != "" {
				tplIn.ExperimentOptions = &fistypes.CreateExperimentTemplateExperimentOptionsInput{
					EmptyTargetResolutionMode: tt.emptyMode,
				}
			}

			tpl, err := client.CreateExperimentTemplate(t.Context(), tplIn)
			require.NoError(t, err)

			started, err := client.StartExperiment(t.Context(), &fissdk.StartExperimentInput{
				ClientToken:          aws.String("exp-" + tt.name),
				ExperimentTemplateId: tpl.ExperimentTemplate.Id,
			})
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				got, gerr := client.GetExperiment(t.Context(), &fissdk.GetExperimentInput{Id: started.Experiment.Id})
				if gerr != nil {
					return false
				}

				return got.Experiment.State.Status == tt.wantStatus
			}, 10*time.Second, 20*time.Millisecond)

			assert.Eventually(t, func() bool {
				got := ec2H.Backend.DescribeInstances([]string{insts[0].ID}, "")

				return len(got) == 1 && got[0].State.Name == tt.wantInstanceOn
			}, 10*time.Second, 20*time.Millisecond)
		})
	}
}
