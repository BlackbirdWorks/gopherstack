package sagemaker_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

// These ops' error switches (sagemaker@v1.263.2 deserializers.go) model only
// ResourceNotFound -- not ValidationException, which is what this backend's
// generic not-found sentinel emits for most "older" resource families
// (services/sagemaker/errors.go). A real client's errors.As against
// *types.ResourceNotFound fails when the wire type is ValidationException,
// since ValidationException isn't a registered case for these ops either --
// it falls through to a generic smithy.GenericAPIError.
func TestDescribeTrainingJob_NotFound(t *testing.T) {
	t.Parallel()

	backend := sagemaker.NewInMemoryBackend("000000000000", smTagsRTRegion)
	client := newTestSageMakerClient(t, sagemaker.NewHandler(backend))

	_, err := client.DescribeTrainingJob(t.Context(), &sagemakersdk.DescribeTrainingJobInput{
		TrainingJobName: aws.String("missing"),
	})
	require.Error(t, err)

	var rnf *smtypes.ResourceNotFound
	require.ErrorAs(t, err, &rnf, "DescribeTrainingJob has no ValidationException case")
}

func TestStopTrainingJob_NotFound(t *testing.T) {
	t.Parallel()

	backend := sagemaker.NewInMemoryBackend("000000000000", smTagsRTRegion)
	client := newTestSageMakerClient(t, sagemaker.NewHandler(backend))

	_, err := client.StopTrainingJob(t.Context(), &sagemakersdk.StopTrainingJobInput{
		TrainingJobName: aws.String("missing"),
	})
	require.Error(t, err)

	var rnf *smtypes.ResourceNotFound
	require.ErrorAs(t, err, &rnf, "StopTrainingJob has no ValidationException case")
}

func TestDescribeTransformJob_NotFound(t *testing.T) {
	t.Parallel()

	backend := sagemaker.NewInMemoryBackend("000000000000", smTagsRTRegion)
	client := newTestSageMakerClient(t, sagemaker.NewHandler(backend))

	_, err := client.DescribeTransformJob(t.Context(), &sagemakersdk.DescribeTransformJobInput{
		TransformJobName: aws.String("missing"),
	})
	require.Error(t, err)

	var rnf *smtypes.ResourceNotFound
	require.ErrorAs(t, err, &rnf, "DescribeTransformJob has no ValidationException case")
}

func TestDescribeHyperParameterTuningJob_NotFound(t *testing.T) {
	t.Parallel()

	backend := sagemaker.NewInMemoryBackend("000000000000", smTagsRTRegion)
	client := newTestSageMakerClient(t, sagemaker.NewHandler(backend))

	_, err := client.DescribeHyperParameterTuningJob(t.Context(), &sagemakersdk.DescribeHyperParameterTuningJobInput{
		HyperParameterTuningJobName: aws.String("missing"),
	})
	require.Error(t, err)

	var rnf *smtypes.ResourceNotFound
	require.ErrorAs(t, err, &rnf, "DescribeHyperParameterTuningJob has no ValidationException case")
}

func TestDescribeDeviceFleet_NotFound(t *testing.T) {
	t.Parallel()

	backend := sagemaker.NewInMemoryBackend("000000000000", smTagsRTRegion)
	client := newTestSageMakerClient(t, sagemaker.NewHandler(backend))

	_, err := client.DescribeDeviceFleet(t.Context(), &sagemakersdk.DescribeDeviceFleetInput{
		DeviceFleetName: aws.String("missing"),
	})
	require.Error(t, err)

	var rnf *smtypes.ResourceNotFound
	require.ErrorAs(t, err, &rnf, "DescribeDeviceFleet has no ValidationException case")
}

func TestDescribeDevice_NotFound(t *testing.T) {
	t.Parallel()

	backend := sagemaker.NewInMemoryBackend("000000000000", smTagsRTRegion)
	client := newTestSageMakerClient(t, sagemaker.NewHandler(backend))

	_, err := client.DescribeDevice(t.Context(), &sagemakersdk.DescribeDeviceInput{
		DeviceFleetName: aws.String("fleet1"),
		DeviceName:      aws.String("missing"),
	})
	require.Error(t, err)

	var rnf *smtypes.ResourceNotFound
	require.ErrorAs(t, err, &rnf, "DescribeDevice has no ValidationException case")
}

func TestDescribeEdgeDeploymentPlan_NotFound(t *testing.T) {
	t.Parallel()

	backend := sagemaker.NewInMemoryBackend("000000000000", smTagsRTRegion)
	client := newTestSageMakerClient(t, sagemaker.NewHandler(backend))

	_, err := client.DescribeEdgeDeploymentPlan(t.Context(), &sagemakersdk.DescribeEdgeDeploymentPlanInput{
		EdgeDeploymentPlanName: aws.String("missing"),
	})
	require.Error(t, err)

	var rnf *smtypes.ResourceNotFound
	require.ErrorAs(t, err, &rnf, "DescribeEdgeDeploymentPlan has no ValidationException case")
}

func TestDescribeInferenceRecommendationsJob_NotFound(t *testing.T) {
	t.Parallel()

	backend := sagemaker.NewInMemoryBackend("000000000000", smTagsRTRegion)
	client := newTestSageMakerClient(t, sagemaker.NewHandler(backend))

	_, err := client.DescribeInferenceRecommendationsJob(
		t.Context(),
		&sagemakersdk.DescribeInferenceRecommendationsJobInput{
			JobName: aws.String("missing"),
		},
	)
	require.Error(t, err)

	var rnf *smtypes.ResourceNotFound
	require.ErrorAs(t, err, &rnf, "DescribeInferenceRecommendationsJob has no ValidationException case")
}

func TestDescribeEdgePackagingJob_NotFound(t *testing.T) {
	t.Parallel()

	backend := sagemaker.NewInMemoryBackend("000000000000", smTagsRTRegion)
	client := newTestSageMakerClient(t, sagemaker.NewHandler(backend))

	_, err := client.DescribeEdgePackagingJob(t.Context(), &sagemakersdk.DescribeEdgePackagingJobInput{
		EdgePackagingJobName: aws.String("missing"),
	})
	require.Error(t, err)

	var rnf *smtypes.ResourceNotFound
	require.ErrorAs(t, err, &rnf, "DescribeEdgePackagingJob has no ValidationException case")
}
