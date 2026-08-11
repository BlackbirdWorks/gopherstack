package databrew_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	databrewsdk "github.com/aws/aws-sdk-go-v2/service/databrew"
	"github.com/aws/aws-sdk-go-v2/service/databrew/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/databrew"
)

// TestCreateOpsWithTags_RoundTrip drives every databrew Create op whose real
// Input struct accepts Tags (databrew@v1.42.4: api_op_CreateDataset.go:54,
// api_op_CreateRecipe.go:47, api_op_CreateProject.go:57,
// api_op_CreateRecipeJob.go:87, api_op_CreateProfileJob.go:86,
// api_op_CreateRuleset.go:53, api_op_CreateSchedule.go:48) through the real
// SDK client and asserts ListTagsForResource sees what was supplied at
// creation (gopherstack-2mwl). Complements
// Test_SDKRoundTrip_TagOperations_BarePath (handler_sdk_roundtrip_test.go),
// which proves the /tags/{arn} wire path works but does not exercise any
// Create op's own Tags field.
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *databrewsdk.Client) string
		name  string
	}{
		{
			name: "dataset",
			setup: func(t *testing.T, client *databrewsdk.Client) string {
				t.Helper()

				_, err := client.CreateDataset(t.Context(), &databrewsdk.CreateDatasetInput{
					Name:  aws.String("tagged-ds"),
					Input: &types.Input{S3InputDefinition: &types.S3Location{Bucket: aws.String("b")}},
					Tags:  map[string]string{"env": "test"},
				})
				require.NoError(t, err)

				out, err := client.DescribeDataset(t.Context(), &databrewsdk.DescribeDatasetInput{
					Name: aws.String("tagged-ds"),
				})
				require.NoError(t, err)

				return aws.ToString(out.ResourceArn)
			},
		},
		{
			name: "recipe",
			setup: func(t *testing.T, client *databrewsdk.Client) string {
				t.Helper()

				_, err := client.CreateRecipe(t.Context(), &databrewsdk.CreateRecipeInput{
					Name: aws.String("tagged-recipe"),
					Steps: []types.RecipeStep{
						{Action: &types.RecipeAction{Operation: aws.String("UPPER_CASE")}},
					},
					Tags: map[string]string{"env": "test"},
				})
				require.NoError(t, err)

				out, err := client.DescribeRecipe(t.Context(), &databrewsdk.DescribeRecipeInput{
					Name: aws.String("tagged-recipe"),
				})
				require.NoError(t, err)

				return aws.ToString(out.ResourceArn)
			},
		},
		{
			name: "project",
			setup: func(t *testing.T, client *databrewsdk.Client) string {
				t.Helper()

				_, err := client.CreateDataset(t.Context(), &databrewsdk.CreateDatasetInput{
					Name:  aws.String("project-host-ds"),
					Input: &types.Input{S3InputDefinition: &types.S3Location{Bucket: aws.String("b")}},
				})
				require.NoError(t, err)

				_, err = client.CreateRecipe(t.Context(), &databrewsdk.CreateRecipeInput{
					Name: aws.String("project-host-recipe"),
					Steps: []types.RecipeStep{
						{Action: &types.RecipeAction{Operation: aws.String("UPPER_CASE")}},
					},
				})
				require.NoError(t, err)

				_, err = client.CreateProject(t.Context(), &databrewsdk.CreateProjectInput{
					Name:        aws.String("tagged-project"),
					DatasetName: aws.String("project-host-ds"),
					RecipeName:  aws.String("project-host-recipe"),
					RoleArn:     aws.String("arn:aws:iam::000000000000:role/databrew"),
					Tags:        map[string]string{"env": "test"},
				})
				require.NoError(t, err)

				out, err := client.DescribeProject(t.Context(), &databrewsdk.DescribeProjectInput{
					Name: aws.String("tagged-project"),
				})
				require.NoError(t, err)

				return aws.ToString(out.ResourceArn)
			},
		},
		{
			name: "recipe job",
			setup: func(t *testing.T, client *databrewsdk.Client) string {
				t.Helper()

				_, err := client.CreateDataset(t.Context(), &databrewsdk.CreateDatasetInput{
					Name:  aws.String("recipejob-host-ds"),
					Input: &types.Input{S3InputDefinition: &types.S3Location{Bucket: aws.String("b")}},
				})
				require.NoError(t, err)

				_, err = client.CreateRecipeJob(t.Context(), &databrewsdk.CreateRecipeJobInput{
					Name:        aws.String("tagged-recipe-job"),
					RoleArn:     aws.String("arn:aws:iam::000000000000:role/databrew"),
					DatasetName: aws.String("recipejob-host-ds"),
					Outputs: []types.Output{
						{Location: &types.S3Location{Bucket: aws.String("out")}},
					},
					Tags: map[string]string{"env": "test"},
				})
				require.NoError(t, err)

				out, err := client.DescribeJob(t.Context(), &databrewsdk.DescribeJobInput{
					Name: aws.String("tagged-recipe-job"),
				})
				require.NoError(t, err)

				return aws.ToString(out.ResourceArn)
			},
		},
		{
			name: "profile job",
			setup: func(t *testing.T, client *databrewsdk.Client) string {
				t.Helper()

				_, err := client.CreateDataset(t.Context(), &databrewsdk.CreateDatasetInput{
					Name:  aws.String("profilejob-host-ds"),
					Input: &types.Input{S3InputDefinition: &types.S3Location{Bucket: aws.String("b")}},
				})
				require.NoError(t, err)

				_, err = client.CreateProfileJob(t.Context(), &databrewsdk.CreateProfileJobInput{
					Name:           aws.String("tagged-profile-job"),
					RoleArn:        aws.String("arn:aws:iam::000000000000:role/databrew"),
					DatasetName:    aws.String("profilejob-host-ds"),
					OutputLocation: &types.S3Location{Bucket: aws.String("out")},
					Tags:           map[string]string{"env": "test"},
				})
				require.NoError(t, err)

				out, err := client.DescribeJob(t.Context(), &databrewsdk.DescribeJobInput{
					Name: aws.String("tagged-profile-job"),
				})
				require.NoError(t, err)

				return aws.ToString(out.ResourceArn)
			},
		},
		{
			name: "ruleset",
			setup: func(t *testing.T, client *databrewsdk.Client) string {
				t.Helper()

				_, err := client.CreateRuleset(t.Context(), &databrewsdk.CreateRulesetInput{
					Name:      aws.String("tagged-ruleset"),
					TargetArn: aws.String("arn:aws:databrew:us-east-1:000000000000:dataset/target"),
					Rules: []types.Rule{
						{CheckExpression: aws.String("1"), Name: aws.String("rule1")},
					},
					Tags: map[string]string{"env": "test"},
				})
				require.NoError(t, err)

				out, err := client.DescribeRuleset(t.Context(), &databrewsdk.DescribeRulesetInput{
					Name: aws.String("tagged-ruleset"),
				})
				require.NoError(t, err)

				return aws.ToString(out.ResourceArn)
			},
		},
		{
			name: "schedule",
			setup: func(t *testing.T, client *databrewsdk.Client) string {
				t.Helper()

				_, err := client.CreateSchedule(t.Context(), &databrewsdk.CreateScheduleInput{
					Name:           aws.String("tagged-schedule"),
					CronExpression: aws.String("cron(0 1 * * ? *)"),
					Tags:           map[string]string{"env": "test"},
				})
				require.NoError(t, err)

				out, err := client.DescribeSchedule(t.Context(), &databrewsdk.DescribeScheduleInput{
					Name: aws.String("tagged-schedule"),
				})
				require.NoError(t, err)

				return aws.ToString(out.ResourceArn)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := databrew.NewInMemoryBackend("000000000000", rtTestRegion)
			client := newRoundTripClient(t, databrew.NewHandler(backend))

			arn := tt.setup(t, client)
			require.NotEmpty(t, arn)

			got, err := client.ListTagsForResource(
				t.Context(),
				&databrewsdk.ListTagsForResourceInput{ResourceArn: aws.String(arn)},
			)
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"env": "test"}, got.Tags)
		})
	}
}
