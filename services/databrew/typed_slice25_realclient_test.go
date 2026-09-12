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

// TestDataBrew_TypedSlice25 drives every remaining uncovered op through a
// real aws-sdk-go-v2 databrew client: DeleteDataset, DeleteJob,
// DeleteProject, DeleteRecipeVersion, DeleteRuleset, DeleteSchedule,
// ListJobs, ListProjects, ListSchedules, SendProjectSessionAction,
// StopJobRun, UpdateDataset, UpdateProject, UpdateRecipe, UpdateRecipeJob,
// UpdateRuleset, UpdateSchedule.
func TestDataBrew_TypedSlice25(t *testing.T) {
	t.Parallel()

	t.Run("dataset lifecycle: UpdateDataset, DeleteDataset", func(t *testing.T) {
		t.Parallel()

		h := databrew.NewHandler(databrew.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newRoundTripClient(t, h)
		ctx := t.Context()

		_, err := client.CreateDataset(ctx, &databrewsdk.CreateDatasetInput{
			Name: aws.String("slice25-dataset"),
			Input: &types.Input{
				S3InputDefinition: &types.S3Location{
					Bucket: aws.String("slice25-bucket"),
					Key:    aws.String("in.csv"),
				},
			},
		})
		require.NoError(t, err)

		_, err = client.UpdateDataset(ctx, &databrewsdk.UpdateDatasetInput{
			Name: aws.String("slice25-dataset"),
			Input: &types.Input{
				S3InputDefinition: &types.S3Location{
					Bucket: aws.String("slice25-bucket"),
					Key:    aws.String("updated.csv"),
				},
			},
		})
		require.NoError(t, err)

		got, err := client.DescribeDataset(
			ctx,
			&databrewsdk.DescribeDatasetInput{Name: aws.String("slice25-dataset")},
		)
		require.NoError(t, err)
		require.NotNil(t, got.Input.S3InputDefinition)
		assert.Equal(t, "updated.csv", aws.ToString(got.Input.S3InputDefinition.Key))

		_, err = client.DeleteDataset(
			ctx,
			&databrewsdk.DeleteDatasetInput{Name: aws.String("slice25-dataset")},
		)
		require.NoError(t, err)

		_, err = client.DescribeDataset(
			ctx,
			&databrewsdk.DescribeDatasetInput{Name: aws.String("slice25-dataset")},
		)
		require.Error(t, err)
	})

	t.Run(
		"recipe and job lifecycle: UpdateRecipe, DeleteRecipeVersion, UpdateRecipeJob, ListJobs, StopJobRun, DeleteJob",
		func(t *testing.T) {
			t.Parallel()

			h := databrew.NewHandler(databrew.NewInMemoryBackend("123456789012", "us-east-1"))
			client := newRoundTripClient(t, h)
			ctx := t.Context()

			_, err := client.CreateDataset(ctx, &databrewsdk.CreateDatasetInput{
				Name: aws.String("slice25-job-dataset"),
				Input: &types.Input{
					S3InputDefinition: &types.S3Location{
						Bucket: aws.String("slice25-bucket"),
						Key:    aws.String("in.csv"),
					},
				},
			})
			require.NoError(t, err)

			_, err = client.CreateRecipe(ctx, &databrewsdk.CreateRecipeInput{
				Name: aws.String("slice25-recipe"),
				Steps: []types.RecipeStep{
					{Action: &types.RecipeAction{Operation: aws.String("UPPER_CASE")}},
				},
			})
			require.NoError(t, err)

			_, err = client.UpdateRecipe(ctx, &databrewsdk.UpdateRecipeInput{
				Name: aws.String("slice25-recipe"),
				Steps: []types.RecipeStep{
					{Action: &types.RecipeAction{Operation: aws.String("LOWER_CASE")}},
				},
			})
			require.NoError(t, err)

			_, err = client.PublishRecipe(
				ctx,
				&databrewsdk.PublishRecipeInput{Name: aws.String("slice25-recipe")},
			)
			require.NoError(t, err)

			_, err = client.DeleteRecipeVersion(ctx, &databrewsdk.DeleteRecipeVersionInput{
				Name:          aws.String("slice25-recipe"),
				RecipeVersion: aws.String("1.0"),
			})
			require.NoError(t, err)

			_, err = client.CreateRecipeJob(ctx, &databrewsdk.CreateRecipeJobInput{
				Name:        aws.String("slice25-job"),
				RoleArn:     aws.String("arn:aws:iam::123456789012:role/databrew-role"),
				DatasetName: aws.String("slice25-job-dataset"),
				RecipeReference: &types.RecipeReference{
					Name: aws.String("slice25-recipe"),
				},
				Outputs: []types.Output{
					{
						Location: &types.S3Location{
							Bucket: aws.String("slice25-bucket"),
							Key:    aws.String("out.csv"),
						},
					},
				},
			})
			require.NoError(t, err)

			_, err = client.UpdateRecipeJob(ctx, &databrewsdk.UpdateRecipeJobInput{
				Name:    aws.String("slice25-job"),
				RoleArn: aws.String("arn:aws:iam::123456789012:role/databrew-role-2"),
				Outputs: []types.Output{
					{
						Location: &types.S3Location{
							Bucket: aws.String("slice25-bucket"),
							Key:    aws.String("out2.csv"),
						},
					},
				},
			})
			require.NoError(t, err)

			listOut, err := client.ListJobs(ctx, &databrewsdk.ListJobsInput{})
			require.NoError(t, err)
			var found bool
			for _, j := range listOut.Jobs {
				if aws.ToString(j.Name) == "slice25-job" {
					found = true
					assert.Equal(
						t,
						"arn:aws:iam::123456789012:role/databrew-role-2",
						aws.ToString(j.RoleArn),
					)
				}
			}
			assert.True(t, found, "created job must appear in ListJobs")

			started, err := client.StartJobRun(
				ctx,
				&databrewsdk.StartJobRunInput{Name: aws.String("slice25-job")},
			)
			require.NoError(t, err)
			runID := aws.ToString(started.RunId)
			require.NotEmpty(t, runID)

			stopped, err := client.StopJobRun(ctx, &databrewsdk.StopJobRunInput{
				Name:  aws.String("slice25-job"),
				RunId: aws.String(runID),
			})
			require.NoError(t, err)
			assert.Equal(t, runID, aws.ToString(stopped.RunId))

			run, err := client.DescribeJobRun(ctx, &databrewsdk.DescribeJobRunInput{
				Name:  aws.String("slice25-job"),
				RunId: aws.String(runID),
			})
			require.NoError(t, err)
			assert.Equal(t, types.JobRunStateStopped, run.State)

			_, err = client.DeleteJob(
				ctx,
				&databrewsdk.DeleteJobInput{Name: aws.String("slice25-job")},
			)
			require.NoError(t, err)
		},
	)

	t.Run(
		"project lifecycle: UpdateProject, ListProjects, SendProjectSessionAction, DeleteProject",
		func(t *testing.T) {
			t.Parallel()

			h := databrew.NewHandler(databrew.NewInMemoryBackend("123456789012", "us-east-1"))
			client := newRoundTripClient(t, h)
			ctx := t.Context()

			_, err := client.CreateDataset(ctx, &databrewsdk.CreateDatasetInput{
				Name: aws.String("slice25-proj-dataset"),
				Input: &types.Input{
					S3InputDefinition: &types.S3Location{
						Bucket: aws.String("slice25-bucket"),
						Key:    aws.String("in.csv"),
					},
				},
			})
			require.NoError(t, err)

			_, err = client.CreateRecipe(ctx, &databrewsdk.CreateRecipeInput{
				Name: aws.String("slice25-proj-recipe"),
				Steps: []types.RecipeStep{
					{Action: &types.RecipeAction{Operation: aws.String("UPPER_CASE")}},
				},
			})
			require.NoError(t, err)

			_, err = client.CreateProject(ctx, &databrewsdk.CreateProjectInput{
				Name:        aws.String("slice25-project"),
				DatasetName: aws.String("slice25-proj-dataset"),
				RecipeName:  aws.String("slice25-proj-recipe"),
				RoleArn:     aws.String("arn:aws:iam::123456789012:role/databrew-role"),
			})
			require.NoError(t, err)

			_, err = client.UpdateProject(ctx, &databrewsdk.UpdateProjectInput{
				Name:    aws.String("slice25-project"),
				RoleArn: aws.String("arn:aws:iam::123456789012:role/databrew-role-updated"),
			})
			require.NoError(t, err)

			listOut, err := client.ListProjects(ctx, &databrewsdk.ListProjectsInput{})
			require.NoError(t, err)
			var found bool
			for _, p := range listOut.Projects {
				if aws.ToString(p.Name) == "slice25-project" {
					found = true
					assert.Equal(
						t,
						"arn:aws:iam::123456789012:role/databrew-role-updated",
						aws.ToString(p.RoleArn),
					)
				}
			}
			assert.True(t, found, "created project must appear in ListProjects")

			_, err = client.SendProjectSessionAction(
				ctx,
				&databrewsdk.SendProjectSessionActionInput{
					Name: aws.String("slice25-project"),
				},
			)
			require.NoError(t, err)

			_, err = client.DeleteProject(
				ctx,
				&databrewsdk.DeleteProjectInput{Name: aws.String("slice25-project")},
			)
			require.NoError(t, err)
		},
	)

	t.Run("schedule lifecycle: UpdateSchedule, ListSchedules, DeleteSchedule", func(t *testing.T) {
		t.Parallel()

		h := databrew.NewHandler(databrew.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newRoundTripClient(t, h)
		ctx := t.Context()

		_, err := client.CreateSchedule(ctx, &databrewsdk.CreateScheduleInput{
			Name:           aws.String("slice25-schedule"),
			CronExpression: aws.String("cron(0 12 * * ? *)"),
		})
		require.NoError(t, err)

		_, err = client.UpdateSchedule(ctx, &databrewsdk.UpdateScheduleInput{
			Name:           aws.String("slice25-schedule"),
			CronExpression: aws.String("cron(0 18 * * ? *)"),
			JobNames:       []string{"some-job"},
		})
		require.NoError(t, err)

		listOut, err := client.ListSchedules(ctx, &databrewsdk.ListSchedulesInput{})
		require.NoError(t, err)
		var found bool
		for _, s := range listOut.Schedules {
			if aws.ToString(s.Name) == "slice25-schedule" {
				found = true
				assert.Equal(t, "cron(0 18 * * ? *)", aws.ToString(s.CronExpression))
			}
		}
		assert.True(t, found, "created schedule must appear in ListSchedules")

		_, err = client.DeleteSchedule(
			ctx,
			&databrewsdk.DeleteScheduleInput{Name: aws.String("slice25-schedule")},
		)
		require.NoError(t, err)
	})

	t.Run("ruleset lifecycle: UpdateRuleset, DeleteRuleset", func(t *testing.T) {
		t.Parallel()

		h := databrew.NewHandler(databrew.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newRoundTripClient(t, h)
		ctx := t.Context()

		_, err := client.CreateDataset(ctx, &databrewsdk.CreateDatasetInput{
			Name: aws.String("slice25-ruleset-dataset"),
			Input: &types.Input{
				S3InputDefinition: &types.S3Location{
					Bucket: aws.String("slice25-bucket"),
					Key:    aws.String("in.csv"),
				},
			},
		})
		require.NoError(t, err)

		targetArn := "arn:aws:databrew:us-east-1:123456789012:dataset/slice25-ruleset-dataset"

		_, err = client.CreateRuleset(ctx, &databrewsdk.CreateRulesetInput{
			Name:      aws.String("slice25-ruleset"),
			TargetArn: aws.String(targetArn),
			Rules: []types.Rule{
				{Name: aws.String("rule-1"), CheckExpression: aws.String(":col1 is_not_null")},
			},
		})
		require.NoError(t, err)

		_, err = client.UpdateRuleset(ctx, &databrewsdk.UpdateRulesetInput{
			Name:        aws.String("slice25-ruleset"),
			Description: aws.String("updated via typed client"),
			Rules: []types.Rule{
				{Name: aws.String("rule-1"), CheckExpression: aws.String(":col1 is_not_null")},
				{Name: aws.String("rule-2"), CheckExpression: aws.String(":col2 is_not_null")},
			},
		})
		require.NoError(t, err)

		got, err := client.DescribeRuleset(
			ctx,
			&databrewsdk.DescribeRulesetInput{Name: aws.String("slice25-ruleset")},
		)
		require.NoError(t, err)
		assert.Equal(t, "updated via typed client", aws.ToString(got.Description))
		require.Len(t, got.Rules, 2)

		_, err = client.DeleteRuleset(
			ctx,
			&databrewsdk.DeleteRulesetInput{Name: aws.String("slice25-ruleset")},
		)
		require.NoError(t, err)
	})
}
