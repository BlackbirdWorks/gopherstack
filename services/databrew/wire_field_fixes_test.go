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

// TestCreateDataset_DataCatalogInputExtras_SDKRoundTrip proves
// DataCatalogInputDefinition.CatalogId/TempDirectory (types/types.go,
// confirmed real via deserializers.go's
// awsRestjson1_deserializeDocumentDataCatalogInputDefinition case list:
// CatalogId/DatabaseName/TableName/TempDirectory) survive DescribeDataset --
// gopherstack's DataCatalogInput struct only had DatabaseName/TableName, so
// CatalogId/TempDirectory were silently dropped on ingest.
func TestCreateDataset_DataCatalogInputExtras_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := databrew.NewInMemoryBackend("000000000000", rtTestRegion)
	h := databrew.NewHandler(backend)
	client := newRoundTripClient(t, h)

	_, err := client.CreateDataset(t.Context(), &databrewsdk.CreateDatasetInput{
		Name: aws.String("data-catalog-ds"),
		Input: &types.Input{
			DataCatalogInputDefinition: &types.DataCatalogInputDefinition{
				DatabaseName: aws.String("my-database"),
				TableName:    aws.String("my-table"),
				CatalogId:    aws.String("111122223333"),
				TempDirectory: &types.S3Location{
					Bucket: aws.String("temp-bucket"),
					Key:    aws.String("temp/"),
				},
			},
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeDataset(t.Context(), &databrewsdk.DescribeDatasetInput{
		Name: aws.String("data-catalog-ds"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Input.DataCatalogInputDefinition)
	assert.Equal(t, "111122223333", aws.ToString(out.Input.DataCatalogInputDefinition.CatalogId))
	require.NotNil(t, out.Input.DataCatalogInputDefinition.TempDirectory)
	assert.Equal(t, "temp-bucket", aws.ToString(out.Input.DataCatalogInputDefinition.TempDirectory.Bucket))
}

// TestCreateDataset_DatabaseInputExtras_SDKRoundTrip proves
// DatabaseInputDefinition.QueryString/TempDirectory (types/types.go,
// confirmed real via deserializers.go's
// awsRestjson1_deserializeDocumentDatabaseInputDefinition case list:
// DatabaseTableName/GlueConnectionName/QueryString/TempDirectory) survive
// DescribeDataset -- gopherstack's DatabaseInput struct only had
// GlueConnectionName/DatabaseTableName, so QueryString/TempDirectory were
// silently dropped on ingest.
func TestCreateDataset_DatabaseInputExtras_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := databrew.NewInMemoryBackend("000000000000", rtTestRegion)
	h := databrew.NewHandler(backend)
	client := newRoundTripClient(t, h)

	_, err := client.CreateDataset(t.Context(), &databrewsdk.CreateDatasetInput{
		Name: aws.String("database-input-ds"),
		Input: &types.Input{
			DatabaseInputDefinition: &types.DatabaseInputDefinition{
				GlueConnectionName: aws.String("my-connection"),
				DatabaseTableName:  aws.String("my-table"),
				QueryString:        aws.String("SELECT * FROM my_table"),
				TempDirectory: &types.S3Location{
					Bucket: aws.String("temp-bucket"),
					Key:    aws.String("temp/"),
				},
			},
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeDataset(t.Context(), &databrewsdk.DescribeDatasetInput{
		Name: aws.String("database-input-ds"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Input.DatabaseInputDefinition)
	assert.Equal(t, "SELECT * FROM my_table", aws.ToString(out.Input.DatabaseInputDefinition.QueryString))
	require.NotNil(t, out.Input.DatabaseInputDefinition.TempDirectory)
	assert.Equal(t, "temp-bucket", aws.ToString(out.Input.DatabaseInputDefinition.TempDirectory.Bucket))
}

// TestJobRun_DatasetNameAndValidationConfigurations_SDKRoundTrip proves two
// real types.JobRun members (deserializers.go's
// awsRestjson1_deserializeDocumentJobRun, case list confirmed against
// Attempt/CompletedOn/DatabaseOutputs/DataCatalogOutputs/DatasetName/
// ErrorMessage/ExecutionTime/JobName/JobSample/LogGroupName/LogSubscription/
// Outputs/RecipeReference/RunId/StartedBy/StartedOn/State/
// ValidationConfigurations) survive StartJobRun/DescribeJobRun/ListJobRuns:
//
//   - DatasetName: the Go field already existed on gopherstack's JobRun
//     struct, but StartJobRun's snapshot-from-parent-Job construction never
//     set it, so it was always empty regardless of the profile job's real
//     DatasetName.
//   - ValidationConfigurations: missing from the JobRun struct entirely --
//     the 2026-08-15 gopherstack-6flj sweep found and fixed 7 of the 8 real
//     JobRun members StartJobRun needed to snapshot from the parent Job
//     (Attempt/DataCatalogOutputs/DatabaseOutputs/JobSample/LogSubscription/
//     Outputs/RecipeReference) but missed this 8th one.
func TestJobRun_DatasetNameAndValidationConfigurations_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := databrew.NewInMemoryBackend("000000000000", rtTestRegion)
	h := databrew.NewHandler(backend)
	client := newRoundTripClient(t, h)

	_, err := client.CreateDataset(t.Context(), &databrewsdk.CreateDatasetInput{
		Name: aws.String("jobrun-ds"),
		Input: &types.Input{
			S3InputDefinition: &types.S3Location{
				Bucket: aws.String("my-bucket"),
				Key:    aws.String("my-key.csv"),
			},
		},
	})
	require.NoError(t, err)

	rulesetArn := "arn:aws:databrew:us-east-1:000000000000:ruleset/jobrun-ruleset"

	_, err = client.CreateProfileJob(t.Context(), &databrewsdk.CreateProfileJobInput{
		Name:        aws.String("jobrun-profile-job"),
		DatasetName: aws.String("jobrun-ds"),
		RoleArn:     aws.String("arn:aws:iam::000000000000:role/DataBrewRole"),
		OutputLocation: &types.S3Location{
			Bucket: aws.String("my-bucket"),
			Key:    aws.String("output/"),
		},
		ValidationConfigurations: []types.ValidationConfiguration{
			{RulesetArn: aws.String(rulesetArn), ValidationMode: types.ValidationModeCheckAll},
		},
	})
	require.NoError(t, err)

	startOut, err := client.StartJobRun(t.Context(), &databrewsdk.StartJobRunInput{
		Name: aws.String("jobrun-profile-job"),
	})
	require.NoError(t, err)

	runID := aws.ToString(startOut.RunId)

	t.Run("describejobrun", func(t *testing.T) {
		t.Parallel()

		out, describeErr := client.DescribeJobRun(t.Context(), &databrewsdk.DescribeJobRunInput{
			Name:  aws.String("jobrun-profile-job"),
			RunId: aws.String(runID),
		})
		require.NoError(t, describeErr)
		assert.Equal(t, "jobrun-ds", aws.ToString(out.DatasetName), "DatasetName must snapshot from the parent Job")
		require.Len(t, out.ValidationConfigurations, 1)
		assert.Equal(t, rulesetArn, aws.ToString(out.ValidationConfigurations[0].RulesetArn))
		assert.Equal(t, types.ValidationModeCheckAll, out.ValidationConfigurations[0].ValidationMode)
	})

	t.Run("listjobruns", func(t *testing.T) {
		t.Parallel()

		out, listErr := client.ListJobRuns(t.Context(), &databrewsdk.ListJobRunsInput{
			Name: aws.String("jobrun-profile-job"),
		})
		require.NoError(t, listErr)
		require.Len(t, out.JobRuns, 1)
		assert.Equal(t, "jobrun-ds", aws.ToString(out.JobRuns[0].DatasetName))
		require.Len(t, out.JobRuns[0].ValidationConfigurations, 1)
		assert.Equal(t, rulesetArn, aws.ToString(out.JobRuns[0].ValidationConfigurations[0].RulesetArn))
	})
}
