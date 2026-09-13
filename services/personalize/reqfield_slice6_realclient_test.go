package personalize_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	personalizesdk "github.com/aws/aws-sdk-go-v2/service/personalize"
	"github.com/aws/aws-sdk-go-v2/service/personalize/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test_Slice6_BatchInferenceJobMode proves CreateBatchInferenceJob's
// BatchInferenceJobMode (reqfielddiff slice 6, gopherstack-xhu2t; dropped:
// undeclared) is validated, defaults to BATCH_INFERENCE, and round-trips on
// both Describe and List.
func Test_Slice6_BatchInferenceJobMode(t *testing.T) {
	t.Parallel()

	t.Run("defaults_to_batch_inference", func(t *testing.T) {
		t.Parallel()

		h, client := newPersonalizeClient(t)
		svArn := personalizeCreateSolutionVersion(t, h, "bij-default-sol")

		out, err := client.CreateBatchInferenceJob(t.Context(), &personalizesdk.CreateBatchInferenceJobInput{
			JobName:            aws.String("bij-default"),
			SolutionVersionArn: aws.String(svArn),
			RoleArn:            aws.String("arn:aws:iam::000000000000:role/personalize"),
			JobInput: &types.BatchInferenceJobInput{
				S3DataSource: &types.S3DataConfig{Path: aws.String("s3://in")},
			},
			JobOutput: &types.BatchInferenceJobOutput{
				S3DataDestination: &types.S3DataConfig{Path: aws.String("s3://out")},
			},
		})
		require.NoError(t, err)

		desc, err := client.DescribeBatchInferenceJob(t.Context(), &personalizesdk.DescribeBatchInferenceJobInput{
			BatchInferenceJobArn: out.BatchInferenceJobArn,
		})
		require.NoError(t, err)
		assert.Equal(t, types.BatchInferenceJobModeBatchInference, desc.BatchInferenceJob.BatchInferenceJobMode)
	})

	t.Run("honors_theme_generation", func(t *testing.T) {
		t.Parallel()

		h, client := newPersonalizeClient(t)
		svArn := personalizeCreateSolutionVersion(t, h, "bij-theme-sol")

		out, err := client.CreateBatchInferenceJob(t.Context(), &personalizesdk.CreateBatchInferenceJobInput{
			JobName:               aws.String("bij-theme"),
			SolutionVersionArn:    aws.String(svArn),
			RoleArn:               aws.String("arn:aws:iam::000000000000:role/personalize"),
			BatchInferenceJobMode: types.BatchInferenceJobModeThemeGeneration,
			JobInput: &types.BatchInferenceJobInput{
				S3DataSource: &types.S3DataConfig{Path: aws.String("s3://in")},
			},
			JobOutput: &types.BatchInferenceJobOutput{
				S3DataDestination: &types.S3DataConfig{Path: aws.String("s3://out")},
			},
		})
		require.NoError(t, err)

		desc, err := client.DescribeBatchInferenceJob(t.Context(), &personalizesdk.DescribeBatchInferenceJobInput{
			BatchInferenceJobArn: out.BatchInferenceJobArn,
		})
		require.NoError(t, err)
		assert.Equal(t, types.BatchInferenceJobModeThemeGeneration, desc.BatchInferenceJob.BatchInferenceJobMode)

		list, err := client.ListBatchInferenceJobs(t.Context(), &personalizesdk.ListBatchInferenceJobsInput{
			SolutionVersionArn: aws.String(svArn),
		})
		require.NoError(t, err)
		require.Len(t, list.BatchInferenceJobs, 1)
		assert.Equal(t, types.BatchInferenceJobModeThemeGeneration, list.BatchInferenceJobs[0].BatchInferenceJobMode)
	})

	t.Run("rejects_invalid_mode", func(t *testing.T) {
		t.Parallel()

		h, client := newPersonalizeClient(t)
		svArn := personalizeCreateSolutionVersion(t, h, "bij-bad-sol")

		_, err := client.CreateBatchInferenceJob(t.Context(), &personalizesdk.CreateBatchInferenceJobInput{
			JobName:               aws.String("bij-bad"),
			SolutionVersionArn:    aws.String(svArn),
			RoleArn:               aws.String("arn:aws:iam::000000000000:role/personalize"),
			BatchInferenceJobMode: "BOGUS",
			JobInput: &types.BatchInferenceJobInput{
				S3DataSource: &types.S3DataConfig{Path: aws.String("s3://in")},
			},
			JobOutput: &types.BatchInferenceJobOutput{
				S3DataDestination: &types.S3DataConfig{Path: aws.String("s3://out")},
			},
		})
		require.Error(t, err)
	})
}

// Test_Slice6_DatasetImportExportModes proves CreateDatasetImportJob's
// ImportMode and CreateDatasetExportJob's IngestionMode (both dropped:
// undeclared) default correctly and round-trip on Describe.
func Test_Slice6_DatasetImportExportModes(t *testing.T) {
	t.Parallel()

	t.Run("import_mode_defaults_to_full", func(t *testing.T) {
		t.Parallel()

		h, client := newPersonalizeClient(t)
		dsArn := personalizeCreateDataset(t, h, "dij-full")

		out, err := client.CreateDatasetImportJob(t.Context(), &personalizesdk.CreateDatasetImportJobInput{
			JobName:    aws.String("dij-full"),
			DatasetArn: aws.String(dsArn),
			RoleArn:    aws.String("arn:aws:iam::000000000000:role/personalize"),
			DataSource: &types.DataSource{DataLocation: aws.String("s3://bucket/key")},
		})
		require.NoError(t, err)

		desc, err := client.DescribeDatasetImportJob(t.Context(), &personalizesdk.DescribeDatasetImportJobInput{
			DatasetImportJobArn: out.DatasetImportJobArn,
		})
		require.NoError(t, err)
		assert.Equal(t, types.ImportModeFull, desc.DatasetImportJob.ImportMode)
	})

	t.Run("import_mode_honors_incremental", func(t *testing.T) {
		t.Parallel()

		h, client := newPersonalizeClient(t)
		dsArn := personalizeCreateDataset(t, h, "dij-inc")

		out, err := client.CreateDatasetImportJob(t.Context(), &personalizesdk.CreateDatasetImportJobInput{
			JobName:    aws.String("dij-inc"),
			DatasetArn: aws.String(dsArn),
			RoleArn:    aws.String("arn:aws:iam::000000000000:role/personalize"),
			ImportMode: types.ImportModeIncremental,
			DataSource: &types.DataSource{DataLocation: aws.String("s3://bucket/key")},
		})
		require.NoError(t, err)

		desc, err := client.DescribeDatasetImportJob(t.Context(), &personalizesdk.DescribeDatasetImportJobInput{
			DatasetImportJobArn: out.DatasetImportJobArn,
		})
		require.NoError(t, err)
		assert.Equal(t, types.ImportModeIncremental, desc.DatasetImportJob.ImportMode)
	})

	t.Run("ingestion_mode_defaults_to_put", func(t *testing.T) {
		t.Parallel()

		h, client := newPersonalizeClient(t)
		dsArn := personalizeCreateDataset(t, h, "dej-put")

		out, err := client.CreateDatasetExportJob(t.Context(), &personalizesdk.CreateDatasetExportJobInput{
			JobName:    aws.String("dej-put"),
			DatasetArn: aws.String(dsArn),
			RoleArn:    aws.String("arn:aws:iam::000000000000:role/personalize"),
			JobOutput: &types.DatasetExportJobOutput{
				S3DataDestination: &types.S3DataConfig{Path: aws.String("s3://out")},
			},
		})
		require.NoError(t, err)

		desc, err := client.DescribeDatasetExportJob(t.Context(), &personalizesdk.DescribeDatasetExportJobInput{
			DatasetExportJobArn: out.DatasetExportJobArn,
		})
		require.NoError(t, err)
		assert.Equal(t, types.IngestionModePut, desc.DatasetExportJob.IngestionMode)
	})

	t.Run("ingestion_mode_honors_all", func(t *testing.T) {
		t.Parallel()

		h, client := newPersonalizeClient(t)
		dsArn := personalizeCreateDataset(t, h, "dej-all")

		out, err := client.CreateDatasetExportJob(t.Context(), &personalizesdk.CreateDatasetExportJobInput{
			JobName:       aws.String("dej-all"),
			DatasetArn:    aws.String(dsArn),
			RoleArn:       aws.String("arn:aws:iam::000000000000:role/personalize"),
			IngestionMode: types.IngestionModeAll,
			JobOutput: &types.DatasetExportJobOutput{
				S3DataDestination: &types.S3DataConfig{Path: aws.String("s3://out")},
			},
		})
		require.NoError(t, err)

		desc, err := client.DescribeDatasetExportJob(t.Context(), &personalizesdk.DescribeDatasetExportJobInput{
			DatasetExportJobArn: out.DatasetExportJobArn,
		})
		require.NoError(t, err)
		assert.Equal(t, types.IngestionModeAll, desc.DatasetExportJob.IngestionMode)
	})
}

// Test_Slice6_ListRecipesRecipeProvider proves ListRecipes rejects a
// RecipeProvider other than SERVICE (dropped: undeclared) -- the real API
// currently defines only one RecipeProvider enum value, matching every
// built-in recipe this backend serves.
func Test_Slice6_ListRecipesRecipeProvider(t *testing.T) {
	t.Parallel()

	_, client := newPersonalizeClient(t)

	out, err := client.ListRecipes(t.Context(), &personalizesdk.ListRecipesInput{
		RecipeProvider: types.RecipeProviderService,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, out.Recipes)

	_, err = client.ListRecipes(t.Context(), &personalizesdk.ListRecipesInput{
		RecipeProvider: "BOGUS",
	})
	require.Error(t, err)
}
