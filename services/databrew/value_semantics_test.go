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

const vsRole = "arn:aws:iam::123456789012:role/r"

func vsInput(key string) *types.Input {
	return &types.Input{S3InputDefinition: &types.S3Location{Bucket: aws.String("b"), Key: aws.String(key)}}
}

func TestPartialUpdatesKeepOmittedMembers(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T, client *databrewsdk.Client)
		name string
	}{
		{name: "project keeps sample", run: func(t *testing.T, client *databrewsdk.Client) {
			t.Helper()
			ctx := t.Context()

			_, err := client.CreateRecipe(ctx, &databrewsdk.CreateRecipeInput{
				Name:  aws.String("r"),
				Steps: []types.RecipeStep{{Action: &types.RecipeAction{Operation: aws.String("UPPER_CASE")}}},
			})
			require.NoError(t, err)
			_, err = client.CreateDataset(
				ctx,
				&databrewsdk.CreateDatasetInput{Name: aws.String("d"), Input: vsInput("a")},
			)
			require.NoError(t, err)
			_, err = client.CreateProject(ctx, &databrewsdk.CreateProjectInput{
				Name: aws.String("p"), DatasetName: aws.String("d"), RecipeName: aws.String("r"),
				RoleArn: aws.String(vsRole),
				Sample:  &types.Sample{Type: types.SampleTypeLastN, Size: aws.Int32(50)},
			})
			require.NoError(t, err)
			_, err = client.UpdateProject(ctx, &databrewsdk.UpdateProjectInput{
				Name: aws.String("p"), RoleArn: aws.String(vsRole + "2"),
			})
			require.NoError(t, err)

			got, err := client.DescribeProject(ctx, &databrewsdk.DescribeProjectInput{Name: aws.String("p")})
			require.NoError(t, err)
			assert.Equal(t, vsRole+"2", aws.ToString(got.RoleArn))
			require.NotNil(t, got.Sample)
			assert.Equal(t, types.SampleTypeLastN, got.Sample.Type)
			assert.EqualValues(t, 50, aws.ToInt32(got.Sample.Size))
		}},
		{name: "dataset keeps format", run: func(t *testing.T, client *databrewsdk.Client) {
			t.Helper()
			ctx := t.Context()

			_, err := client.CreateDataset(ctx, &databrewsdk.CreateDatasetInput{
				Name: aws.String("d"), Input: vsInput("a"), Format: types.InputFormatCsv,
				FormatOptions: &types.FormatOptions{Csv: &types.CsvOptions{Delimiter: aws.String(";")}},
			})
			require.NoError(t, err)
			_, err = client.UpdateDataset(
				ctx,
				&databrewsdk.UpdateDatasetInput{Name: aws.String("d"), Input: vsInput("b")},
			)
			require.NoError(t, err)

			got, err := client.DescribeDataset(ctx, &databrewsdk.DescribeDatasetInput{Name: aws.String("d")})
			require.NoError(t, err)
			assert.Equal(t, "b", aws.ToString(got.Input.S3InputDefinition.Key))
			assert.Equal(t, types.InputFormatCsv, got.Format)
			require.NotNil(t, got.FormatOptions)
			require.NotNil(t, got.FormatOptions.Csv)
			assert.Equal(t, ";", aws.ToString(got.FormatOptions.Csv.Delimiter))
		}},
		{name: "recipe job keeps retries", run: func(t *testing.T, client *databrewsdk.Client) {
			t.Helper()
			ctx := t.Context()

			_, err := client.CreateDataset(
				ctx,
				&databrewsdk.CreateDatasetInput{Name: aws.String("d"), Input: vsInput("a")},
			)
			require.NoError(t, err)
			_, err = client.CreateRecipe(ctx, &databrewsdk.CreateRecipeInput{
				Name:  aws.String("r"),
				Steps: []types.RecipeStep{{Action: &types.RecipeAction{Operation: aws.String("UPPER_CASE")}}},
			})
			require.NoError(t, err)
			_, err = client.CreateRecipeJob(ctx, &databrewsdk.CreateRecipeJobInput{
				Name: aws.String("j"), RoleArn: aws.String(vsRole), DatasetName: aws.String("d"),
				RecipeReference: &types.RecipeReference{Name: aws.String("r")},
				Outputs:         []types.Output{{Location: &types.S3Location{Bucket: aws.String("b")}}},
				MaxRetries:      3, MaxCapacity: 7, Timeout: 90,
			})
			require.NoError(t, err)
			_, err = client.UpdateRecipeJob(ctx, &databrewsdk.UpdateRecipeJobInput{
				Name: aws.String("j"), RoleArn: aws.String(vsRole + "2"),
				Outputs: []types.Output{{Location: &types.S3Location{Bucket: aws.String("b2")}}},
			})
			require.NoError(t, err)

			got, err := client.DescribeJob(ctx, &databrewsdk.DescribeJobInput{Name: aws.String("j")})
			require.NoError(t, err)
			assert.EqualValues(t, 3, got.MaxRetries)
			assert.EqualValues(t, 7, got.MaxCapacity)
			assert.EqualValues(t, 90, got.Timeout)
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newRoundTripClient(
				t,
				databrew.NewHandler(databrew.NewInMemoryBackend("123456789012", "us-east-1")),
			)
			tc.run(t, client)
		})
	}
}
