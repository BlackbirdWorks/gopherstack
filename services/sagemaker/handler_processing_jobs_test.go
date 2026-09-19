package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_DeleteProcessingJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateProcessingJob", map[string]any{
		"ProcessingJobName": "del-pj",
		"RoleArn":           "arn:aws:iam::000000000000:role/test",
		"AppSpecification":  map[string]any{"ImageUri": "img:latest"},
		"ProcessingResources": map[string]any{
			"ClusterConfig": map[string]any{"InstanceType": "ml.m5.large", "InstanceCount": 1, "VolumeSizeInGB": 10},
		},
	})

	// Cannot delete while still InProgress.
	recEarly := doSageMakerRequest(t, h, "DeleteProcessingJob", map[string]any{"ProcessingJobName": "del-pj"})
	assert.Equal(t, http.StatusBadRequest, recEarly.Code)

	// Wait for the simulated job to reach a terminal state.
	time.Sleep(400 * time.Millisecond)

	recDelete := doSageMakerRequest(t, h, "DeleteProcessingJob", map[string]any{"ProcessingJobName": "del-pj"})
	require.Equal(t, http.StatusOK, recDelete.Code)

	recDescribe := doSageMakerRequest(t, h, "DescribeProcessingJob", map[string]any{"ProcessingJobName": "del-pj"})
	assert.Equal(t, http.StatusBadRequest, recDescribe.Code)
}

func TestHandler_DeleteProcessingJob_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DeleteProcessingJob", map[string]any{"ProcessingJobName": "no-such-job"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_CreateProcessingJob_NetworkConfig_RealClient proves
// CreateProcessingJobInput's real VPC settings (which nest under
// NetworkConfig.VpcConfig -- there is no top-level VpcConfig field on the
// real request at all) survive the real aws-sdk-go-v2 client's own
// serializer, round-tripping through DescribeProcessingJob.
func TestHandler_CreateProcessingJob_NetworkConfig_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	enableIsolation := true
	_, err := client.CreateProcessingJob(t.Context(), &sagemakersdk.CreateProcessingJobInput{
		ProcessingJobName: aws.String("networked-pj"),
		RoleArn:           aws.String("arn:aws:iam::000000000000:role/test"),
		AppSpecification:  &smtypes.AppSpecification{ImageUri: aws.String("img:latest")},
		ProcessingResources: &smtypes.ProcessingResources{
			ClusterConfig: &smtypes.ProcessingClusterConfig{
				InstanceType:   smtypes.ProcessingInstanceTypeMlM5Large,
				InstanceCount:  aws.Int32(1),
				VolumeSizeInGB: aws.Int32(10),
			},
		},
		NetworkConfig: &smtypes.NetworkConfig{
			EnableNetworkIsolation: aws.Bool(enableIsolation),
			VpcConfig: &smtypes.VpcConfig{
				SecurityGroupIds: []string{"sg-1"},
				Subnets:          []string{"subnet-1"},
			},
		},
		StoppingCondition: &smtypes.ProcessingStoppingCondition{
			MaxRuntimeInSeconds: aws.Int32(3600),
		},
		ExperimentConfig: &smtypes.ExperimentConfig{
			ExperimentName: aws.String("exp-1"),
			TrialName:      aws.String("trial-1"),
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeProcessingJob(t.Context(), &sagemakersdk.DescribeProcessingJobInput{
		ProcessingJobName: aws.String("networked-pj"),
	})
	require.NoError(t, err)

	require.NotNil(t, out.NetworkConfig, "DescribeProcessingJob must return NetworkConfig")
	require.NotNil(t, out.NetworkConfig.VpcConfig, "NetworkConfig.VpcConfig must round-trip")
	assert.Equal(t, []string{"sg-1"}, out.NetworkConfig.VpcConfig.SecurityGroupIds)
	assert.Equal(t, []string{"subnet-1"}, out.NetworkConfig.VpcConfig.Subnets)
	assert.True(t, aws.ToBool(out.NetworkConfig.EnableNetworkIsolation))

	require.NotNil(t, out.StoppingCondition)
	assert.Equal(t, int32(3600), aws.ToInt32(out.StoppingCondition.MaxRuntimeInSeconds))

	require.NotNil(t, out.ExperimentConfig)
	assert.Equal(t, "exp-1", aws.ToString(out.ExperimentConfig.ExperimentName))
	assert.Equal(t, "trial-1", aws.ToString(out.ExperimentConfig.TrialName))
}

// TestHandler_CreateProcessingJob_DatasetDefinitionAndFeatureStoreOutput
// proves ProcessingInput.DatasetDefinition's Athena/Redshift sub-fields and
// ProcessingOutput.FeatureStoreOutput (real, optional CreateProcessingJobInput
// fields previously entirely absent from decode) round-trip through
// DescribeProcessingJob via the real aws-sdk-go-v2 client.
func TestHandler_CreateProcessingJob_DatasetDefinitionAndFeatureStoreOutput(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateProcessingJob(t.Context(), &sagemakersdk.CreateProcessingJobInput{
		ProcessingJobName: aws.String("dataset-def-pj"),
		RoleArn:           aws.String("arn:aws:iam::000000000000:role/test"),
		AppSpecification:  &smtypes.AppSpecification{ImageUri: aws.String("img:latest")},
		ProcessingResources: &smtypes.ProcessingResources{
			ClusterConfig: &smtypes.ProcessingClusterConfig{
				InstanceType:   smtypes.ProcessingInstanceTypeMlM5Large,
				InstanceCount:  aws.Int32(1),
				VolumeSizeInGB: aws.Int32(10),
			},
		},
		ProcessingInputs: []smtypes.ProcessingInput{
			{
				InputName: aws.String("athena-input"),
				DatasetDefinition: &smtypes.DatasetDefinition{
					AthenaDatasetDefinition: &smtypes.AthenaDatasetDefinition{
						Catalog:      aws.String("cat"),
						Database:     aws.String("db"),
						OutputFormat: smtypes.AthenaResultFormatJson,
						OutputS3Uri:  aws.String("s3://b/athena-out"),
						QueryString:  aws.String("SELECT 1"),
					},
				},
			},
			{
				InputName: aws.String("redshift-input"),
				DatasetDefinition: &smtypes.DatasetDefinition{
					RedshiftDatasetDefinition: &smtypes.RedshiftDatasetDefinition{
						ClusterId:      aws.String("cluster-1"),
						ClusterRoleArn: aws.String("arn:aws:iam::000000000000:role/redshift"),
						Database:       aws.String("db"),
						DbUser:         aws.String("user"),
						OutputFormat:   smtypes.RedshiftResultFormatCsv,
						OutputS3Uri:    aws.String("s3://b/redshift-out"),
						QueryString:    aws.String("SELECT 1"),
					},
				},
			},
		},
		ProcessingOutputConfig: &smtypes.ProcessingOutputConfig{
			Outputs: []smtypes.ProcessingOutput{
				{
					OutputName: aws.String("fs-output"),
					AppManaged: aws.Bool(true),
					FeatureStoreOutput: &smtypes.ProcessingFeatureStoreOutput{
						FeatureGroupName: aws.String("my-feature-group"),
					},
				},
			},
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeProcessingJob(t.Context(), &sagemakersdk.DescribeProcessingJobInput{
		ProcessingJobName: aws.String("dataset-def-pj"),
	})
	require.NoError(t, err)

	require.Len(t, out.ProcessingInputs, 2)

	athena := out.ProcessingInputs[0]
	require.NotNil(t, athena.DatasetDefinition)
	require.NotNil(t, athena.DatasetDefinition.AthenaDatasetDefinition)
	assert.Equal(t, "cat", aws.ToString(athena.DatasetDefinition.AthenaDatasetDefinition.Catalog))
	assert.Equal(t, "s3://b/athena-out", aws.ToString(athena.DatasetDefinition.AthenaDatasetDefinition.OutputS3Uri))

	redshift := out.ProcessingInputs[1]
	require.NotNil(t, redshift.DatasetDefinition)
	require.NotNil(t, redshift.DatasetDefinition.RedshiftDatasetDefinition)
	assert.Equal(t, "cluster-1", aws.ToString(redshift.DatasetDefinition.RedshiftDatasetDefinition.ClusterId))
	assert.Equal(t, "user", aws.ToString(redshift.DatasetDefinition.RedshiftDatasetDefinition.DbUser))

	require.Len(t, out.ProcessingOutputConfig.Outputs, 1)
	fsOut := out.ProcessingOutputConfig.Outputs[0]
	require.NotNil(t, fsOut.FeatureStoreOutput)
	assert.Equal(t, "my-feature-group", aws.ToString(fsOut.FeatureStoreOutput.FeatureGroupName))
}

func TestHandler_ListProcessingJobs_FilterSort(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	makeJob := func(name string) map[string]any {
		return map[string]any{
			"ProcessingJobName": name,
			"RoleArn":           "arn:aws:iam::000000000000:role/test",
			"AppSpecification":  map[string]any{"ImageUri": "img:latest"},
			"ProcessingResources": map[string]any{
				"ClusterConfig": map[string]any{
					"InstanceType": "ml.m5.large", "InstanceCount": 1, "VolumeSizeInGB": 10,
				},
			},
		}
	}

	doSageMakerRequest(t, h, "CreateProcessingJob", makeJob("pj-alpha"))
	doSageMakerRequest(t, h, "CreateProcessingJob", makeJob("pj-beta"))

	rec := doSageMakerRequest(t, h, "ListProcessingJobs", map[string]any{
		"NameContains": "beta",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	summaries := resp["ProcessingJobSummaries"].([]any)
	require.Len(t, summaries, 1)
	assert.Equal(t, "pj-beta", summaries[0].(map[string]any)["ProcessingJobName"])

	rec = doSageMakerRequest(t, h, "ListProcessingJobs", map[string]any{
		"SortBy":    "Name",
		"SortOrder": "Descending",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	summaries = resp["ProcessingJobSummaries"].([]any)
	require.Len(t, summaries, 2)
	assert.Equal(t, "pj-beta", summaries[0].(map[string]any)["ProcessingJobName"])
	assert.Equal(t, "pj-alpha", summaries[1].(map[string]any)["ProcessingJobName"])
}
