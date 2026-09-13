package sagemaker_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSlice28_SageMaker_RealClient covers sagemaker's remaining
// typed-client-uncovered op families (gopherstack-n3zi slice 28): AI
// benchmark/recommendation jobs, edge deployment, the generic Job family,
// transform/processing job Stop/Delete/List, lineage-adjacent extras,
// device fleet management, hub content, human task UI, MLflow apps,
// spaces, training plan extension history, cluster software, inference
// component/experiment extras, inference recommendations job extras,
// model card export, pipeline versions and the presigned-session family.
func TestSlice28_SageMaker_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testAIBenchmarkJobRealClient, "ai_benchmark_job"},
		{testAIWorkloadConfigRealClient, "ai_workload_config"},
		{testAIRecommendationJobRealClient, "ai_recommendation_job"},
		{testGenericJobFamilyRealClient, "generic_job_family"},
		{testTransformJobExtraRealClient, "transform_job_extra"},
		{testProcessingJobExtraRealClient, "processing_job_extra"},
		{testTrainingJobDeleteRealClient, "training_job_delete"},
		{testEdgeDeploymentRealClient, "edge_deployment"},
		{testDeviceManagementRealClient, "device_management"},
		{testEdgePackagingExtraRealClient, "edge_packaging_extra"},
		{testAppImageConfigExtraRealClient, "app_image_config_extra"},
		{testFlowDefinitionExtraRealClient, "flow_definition_extra"},
		{testHubContentExtraRealClient, "hub_content_extra"},
		{testHumanTaskUIExtraRealClient, "human_task_ui_extra"},
		{testMlflowAppExtraRealClient, "mlflow_app_extra"},
		{testSpaceExtraRealClient, "space_extra"},
		{testTrainingPlanExtensionExtraRealClient, "training_plan_extension_extra"},
		{testClusterSoftwareRealClient, "cluster_software"},
		{testInferenceComponentRuntimeRealClient, "inference_component_runtime"},
		{testInferenceExperimentExtraRealClient, "inference_experiment_extra"},
		{testInferenceRecommendationsExtraRealClient, "inference_recommendations_extra"},
		{testModelCardExportRealClient, "model_card_export"},
		{testPipelineVersionRealClient, "pipeline_version"},
		{testPresignedSessionFamilyRealClient, "presigned_session_family"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

func newSlice28SageMakerClient(t *testing.T) *sagemakersdk.Client {
	t.Helper()

	h := newTestHandler(t)

	return newTestSageMakerClient(t, h)
}

// testAIBenchmarkJobRealClient covers CreateAIBenchmarkJob,
// DescribeAIBenchmarkJob, StopAIBenchmarkJob, DeleteAIBenchmarkJob.
func testAIBenchmarkJobRealClient(t *testing.T) {
	t.Helper()

	client := newSlice28SageMakerClient(t)

	_, err := client.CreateAIWorkloadConfig(t.Context(), &sagemakersdk.CreateAIWorkloadConfigInput{
		AIWorkloadConfigName: aws.String("slice28-awc-bench"),
		AIWorkloadConfigs: &smtypes.AIWorkloadConfigs{
			WorkloadSpec: &smtypes.WorkloadSpecMemberInline{Value: "{}"},
		},
	})
	require.NoError(t, err)

	created, err := client.CreateAIBenchmarkJob(t.Context(), &sagemakersdk.CreateAIBenchmarkJobInput{
		AIBenchmarkJobName:         aws.String("slice28-bench-job"),
		AIWorkloadConfigIdentifier: aws.String("slice28-awc-bench"),
		RoleArn:                    aws.String("arn:aws:iam::000000000000:role/bench"),
		BenchmarkTarget: &smtypes.AIBenchmarkTargetMemberEndpoint{
			Value: smtypes.AIBenchmarkEndpoint{Identifier: aws.String("some-endpoint")},
		},
		OutputConfig: &smtypes.AIBenchmarkOutputConfig{S3OutputLocation: aws.String("s3://bucket/bench-out")},
		Tags:         []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(created.AIBenchmarkJobArn))

	described, err := client.DescribeAIBenchmarkJob(t.Context(), &sagemakersdk.DescribeAIBenchmarkJobInput{
		AIBenchmarkJobName: aws.String("slice28-bench-job"),
	})
	require.NoError(t, err)
	// Tags must decode as the real []{Key,Value} list -- a real client's
	// decode fails outright if this backend ever regresses to emitting the
	// {"k":"v"} map shape (see AIBenchmarkJob.MarshalJSON).
	require.Len(t, described.Tags, 1)
	assert.Equal(t, "env", aws.ToString(described.Tags[0].Key))

	_, err = client.StopAIBenchmarkJob(t.Context(), &sagemakersdk.StopAIBenchmarkJobInput{
		AIBenchmarkJobName: aws.String("slice28-bench-job"),
	})
	require.NoError(t, err)

	_, err = client.DeleteAIBenchmarkJob(t.Context(), &sagemakersdk.DeleteAIBenchmarkJobInput{
		AIBenchmarkJobName: aws.String("slice28-bench-job"),
	})
	require.NoError(t, err)
}

// testAIWorkloadConfigRealClient covers ListAIWorkloadConfigs and
// DeleteAIWorkloadConfig.
func testAIWorkloadConfigRealClient(t *testing.T) {
	t.Helper()

	client := newSlice28SageMakerClient(t)

	_, err := client.CreateAIWorkloadConfig(t.Context(), &sagemakersdk.CreateAIWorkloadConfigInput{
		AIWorkloadConfigName: aws.String("slice28-awc-list"),
		AIWorkloadConfigs: &smtypes.AIWorkloadConfigs{
			WorkloadSpec: &smtypes.WorkloadSpecMemberInline{Value: "{}"},
		},
	})
	require.NoError(t, err)

	listed, err := client.ListAIWorkloadConfigs(t.Context(), &sagemakersdk.ListAIWorkloadConfigsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, listed.AIWorkloadConfigs)

	_, err = client.DeleteAIWorkloadConfig(t.Context(), &sagemakersdk.DeleteAIWorkloadConfigInput{
		AIWorkloadConfigName: aws.String("slice28-awc-list"),
	})
	require.NoError(t, err)
}

// testAIRecommendationJobRealClient covers DeleteAIRecommendationJob and
// StopAIRecommendationJob.
func testAIRecommendationJobRealClient(t *testing.T) {
	t.Helper()

	client := newSlice28SageMakerClient(t)

	_, err := client.CreateAIWorkloadConfig(t.Context(), &sagemakersdk.CreateAIWorkloadConfigInput{
		AIWorkloadConfigName: aws.String("slice28-awc-rec"),
		AIWorkloadConfigs: &smtypes.AIWorkloadConfigs{
			WorkloadSpec: &smtypes.WorkloadSpecMemberInline{Value: "{}"},
		},
	})
	require.NoError(t, err)

	created, err := client.CreateAIRecommendationJob(t.Context(), &sagemakersdk.CreateAIRecommendationJobInput{
		AIRecommendationJobName:    aws.String("slice28-rec-job"),
		AIWorkloadConfigIdentifier: aws.String("slice28-awc-rec"),
		RoleArn:                    aws.String("arn:aws:iam::000000000000:role/rec"),
		ModelSource: &smtypes.AIModelSourceMemberS3{
			Value: smtypes.AIModelSourceS3{S3Uri: aws.String("s3://bucket/model")},
		},
		OutputConfig: &smtypes.AIRecommendationOutputConfig{
			S3OutputLocation: aws.String("s3://bucket/rec-out"),
		},
		PerformanceTarget: &smtypes.AIRecommendationPerformanceTarget{
			Constraints: []smtypes.AIRecommendationConstraint{{Metric: smtypes.AIRecommendationMetricCost}},
		},
		Tags: []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(created.AIRecommendationJobArn))

	described, err := client.DescribeAIRecommendationJob(
		t.Context(), &sagemakersdk.DescribeAIRecommendationJobInput{
			AIRecommendationJobName: aws.String("slice28-rec-job"),
		},
	)
	require.NoError(t, err)
	require.Len(t, described.Tags, 1)

	_, err = client.StopAIRecommendationJob(t.Context(), &sagemakersdk.StopAIRecommendationJobInput{
		AIRecommendationJobName: aws.String("slice28-rec-job"),
	})
	require.NoError(t, err)

	_, err = client.DeleteAIRecommendationJob(t.Context(), &sagemakersdk.DeleteAIRecommendationJobInput{
		AIRecommendationJobName: aws.String("slice28-rec-job"),
	})
	require.NoError(t, err)
}

// testGenericJobFamilyRealClient covers CreateJob, DescribeJob, StopJob,
// DeleteJob, DescribeJobSchemaVersion, ListJobSchemaVersions, ListJobs.
func testGenericJobFamilyRealClient(t *testing.T) {
	t.Helper()

	client := newSlice28SageMakerClient(t)

	schemaInfo, err := client.DescribeJobSchemaVersion(t.Context(), &sagemakersdk.DescribeJobSchemaVersionInput{
		JobCategory: smtypes.JobCategoryAgentRft,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(schemaInfo.JobConfigSchema))

	schemaVersions, err := client.ListJobSchemaVersions(t.Context(), &sagemakersdk.ListJobSchemaVersionsInput{
		JobCategory: smtypes.JobCategoryAgentRft,
	})
	require.NoError(t, err)
	require.NotEmpty(t, schemaVersions.JobConfigSchemas)
	version := aws.ToString(schemaVersions.JobConfigSchemas[0].JobConfigSchemaVersion)

	created, err := client.CreateJob(t.Context(), &sagemakersdk.CreateJobInput{
		JobName:                aws.String("slice28-job"),
		JobCategory:            smtypes.JobCategoryAgentRft,
		JobConfigDocument:      aws.String("{}"),
		JobConfigSchemaVersion: aws.String(version),
		RoleArn:                aws.String("arn:aws:iam::000000000000:role/job"),
		Tags:                   []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(created.JobArn))

	described, err := client.DescribeJob(t.Context(), &sagemakersdk.DescribeJobInput{
		JobName: aws.String("slice28-job"), JobCategory: smtypes.JobCategoryAgentRft,
	})
	require.NoError(t, err)
	// Tags must decode as the real []{Key,Value} list, not the
	// {"k":"v"}-shaped map Job.MarshalJSON's embedded alias emits for its
	// own persisted representation (see marshalJobResponse).
	require.Len(t, described.Tags, 1)
	assert.Equal(t, "env", aws.ToString(described.Tags[0].Key))

	listed, err := client.ListJobs(t.Context(), &sagemakersdk.ListJobsInput{
		JobCategory: smtypes.JobCategoryAgentRft,
	})
	require.NoError(t, err)
	found := false

	for _, j := range listed.JobSummaries {
		if aws.ToString(j.JobName) == "slice28-job" {
			found = true
		}
	}
	assert.True(t, found)

	_, err = client.StopJob(t.Context(), &sagemakersdk.StopJobInput{
		JobName: aws.String("slice28-job"), JobCategory: smtypes.JobCategoryAgentRft,
	})
	require.NoError(t, err)

	_, err = client.DeleteJob(t.Context(), &sagemakersdk.DeleteJobInput{
		JobName: aws.String("slice28-job"), JobCategory: smtypes.JobCategoryAgentRft,
	})
	require.NoError(t, err)
}

// testTransformJobExtraRealClient covers CreateTransformJob,
// ListTransformJobs, StopTransformJob.
func testTransformJobExtraRealClient(t *testing.T) {
	t.Helper()

	client := newSlice28SageMakerClient(t)

	_, err := client.CreateModel(t.Context(), &sagemakersdk.CreateModelInput{
		ModelName:        aws.String("slice28-transform-model"),
		ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/model"),
		PrimaryContainer: &smtypes.ContainerDefinition{Image: aws.String("image:latest")},
	})
	require.NoError(t, err)

	created, err := client.CreateTransformJob(t.Context(), &sagemakersdk.CreateTransformJobInput{
		TransformJobName: aws.String("slice28-transform-job"),
		ModelName:        aws.String("slice28-transform-model"),
		TransformInput: &smtypes.TransformInput{
			DataSource: &smtypes.TransformDataSource{
				S3DataSource: &smtypes.TransformS3DataSource{
					S3DataType: smtypes.S3DataTypeS3Prefix,
					S3Uri:      aws.String("s3://bucket/input"),
				},
			},
		},
		TransformOutput: &smtypes.TransformOutput{S3OutputPath: aws.String("s3://bucket/output")},
		TransformResources: &smtypes.TransformResources{
			InstanceType:  smtypes.TransformInstanceTypeMlM5Large,
			InstanceCount: aws.Int32(1),
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(created.TransformJobArn))

	listed, err := client.ListTransformJobs(t.Context(), &sagemakersdk.ListTransformJobsInput{})
	require.NoError(t, err)
	found := false

	for _, j := range listed.TransformJobSummaries {
		if aws.ToString(j.TransformJobName) == "slice28-transform-job" {
			found = true
		}
	}
	assert.True(t, found)

	_, err = client.StopTransformJob(t.Context(), &sagemakersdk.StopTransformJobInput{
		TransformJobName: aws.String("slice28-transform-job"),
	})
	require.NoError(t, err)
}

// testProcessingJobExtraRealClient covers DeleteProcessingJob,
// ListProcessingJobs, StopProcessingJob.
func testProcessingJobExtraRealClient(t *testing.T) {
	t.Helper()

	client := newSlice28SageMakerClient(t)

	_, err := client.CreateProcessingJob(t.Context(), &sagemakersdk.CreateProcessingJobInput{
		ProcessingJobName: aws.String("slice28-processing-job"),
		RoleArn:           aws.String("arn:aws:iam::000000000000:role/processing"),
		AppSpecification:  &smtypes.AppSpecification{ImageUri: aws.String("image:latest")},
		ProcessingResources: &smtypes.ProcessingResources{
			ClusterConfig: &smtypes.ProcessingClusterConfig{
				InstanceType:   smtypes.ProcessingInstanceTypeMlM5Large,
				InstanceCount:  aws.Int32(1),
				VolumeSizeInGB: aws.Int32(10),
			},
		},
	})
	require.NoError(t, err)

	listed, err := client.ListProcessingJobs(t.Context(), &sagemakersdk.ListProcessingJobsInput{})
	require.NoError(t, err)
	found := false

	for _, j := range listed.ProcessingJobSummaries {
		if aws.ToString(j.ProcessingJobName) == "slice28-processing-job" {
			found = true
		}
	}
	assert.True(t, found)

	_, err = client.StopProcessingJob(t.Context(), &sagemakersdk.StopProcessingJobInput{
		ProcessingJobName: aws.String("slice28-processing-job"),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		polled, pollErr := client.DescribeProcessingJob(t.Context(), &sagemakersdk.DescribeProcessingJobInput{
			ProcessingJobName: aws.String("slice28-processing-job"),
		})
		require.NoError(t, pollErr)

		return polled.ProcessingJobStatus == smtypes.ProcessingJobStatusStopped
	}, 2*time.Second, 10*time.Millisecond)

	_, err = client.DeleteProcessingJob(t.Context(), &sagemakersdk.DeleteProcessingJobInput{
		ProcessingJobName: aws.String("slice28-processing-job"),
	})
	require.NoError(t, err)
}

// testTrainingJobDeleteRealClient covers DeleteTrainingJob.
func testTrainingJobDeleteRealClient(t *testing.T) {
	t.Helper()

	client := newSlice28SageMakerClient(t)

	_, err := client.CreateTrainingJob(t.Context(), &sagemakersdk.CreateTrainingJobInput{
		TrainingJobName:        aws.String("slice28-training-job"),
		RoleArn:                aws.String("arn:aws:iam::000000000000:role/service-role"),
		AlgorithmSpecification: &smtypes.AlgorithmSpecification{TrainingInputMode: smtypes.TrainingInputModeFile},
		OutputDataConfig:       &smtypes.OutputDataConfig{S3OutputPath: aws.String("s3://bucket/output")},
		ResourceConfig: &smtypes.ResourceConfig{
			InstanceType:   smtypes.TrainingInstanceTypeMlM5Large,
			InstanceCount:  aws.Int32(1),
			VolumeSizeInGB: aws.Int32(20),
		},
		StoppingCondition: &smtypes.StoppingCondition{MaxRuntimeInSeconds: aws.Int32(3600)},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		polled, pollErr := client.DescribeTrainingJob(t.Context(), &sagemakersdk.DescribeTrainingJobInput{
			TrainingJobName: aws.String("slice28-training-job"),
		})
		require.NoError(t, pollErr)

		return polled.TrainingJobStatus == smtypes.TrainingJobStatusCompleted
	}, 2*time.Second, 10*time.Millisecond)

	_, err = client.DeleteTrainingJob(t.Context(), &sagemakersdk.DeleteTrainingJobInput{
		TrainingJobName: aws.String("slice28-training-job"),
	})
	require.NoError(t, err)
}

// testEdgeDeploymentRealClient covers CreateEdgeDeploymentStage,
// DeleteEdgeDeploymentPlan, DeleteEdgeDeploymentStage,
// StartEdgeDeploymentStage, StopEdgeDeploymentStage, GetDeviceFleetReport.
func testEdgeDeploymentRealClient(t *testing.T) {
	t.Helper()

	client := newSlice28SageMakerClient(t)

	_, err := client.CreateDeviceFleet(t.Context(), &sagemakersdk.CreateDeviceFleetInput{
		DeviceFleetName: aws.String("slice28-edge-fleet"),
		OutputConfig:    &smtypes.EdgeOutputConfig{S3OutputLocation: aws.String("s3://bucket/edge-out")},
	})
	require.NoError(t, err)

	report, err := client.GetDeviceFleetReport(t.Context(), &sagemakersdk.GetDeviceFleetReportInput{
		DeviceFleetName: aws.String("slice28-edge-fleet"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice28-edge-fleet", aws.ToString(report.DeviceFleetName))

	_, err = client.CreateEdgeDeploymentPlan(t.Context(), &sagemakersdk.CreateEdgeDeploymentPlanInput{
		EdgeDeploymentPlanName: aws.String("slice28-edge-plan"),
		DeviceFleetName:        aws.String("slice28-edge-fleet"),
		ModelConfigs: []smtypes.EdgeDeploymentModelConfig{
			{EdgePackagingJobName: aws.String("some-packaging-job"), ModelHandle: aws.String("handle-1")},
		},
	})
	require.NoError(t, err)

	_, err = client.CreateEdgeDeploymentStage(t.Context(), &sagemakersdk.CreateEdgeDeploymentStageInput{
		EdgeDeploymentPlanName: aws.String("slice28-edge-plan"),
		Stages: []smtypes.DeploymentStage{
			{
				StageName: aws.String("stage-1"),
				DeviceSelectionConfig: &smtypes.DeviceSelectionConfig{
					DeviceSubsetType: smtypes.DeviceSubsetTypePercentage,
					Percentage:       aws.Int32(100),
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = client.StartEdgeDeploymentStage(t.Context(), &sagemakersdk.StartEdgeDeploymentStageInput{
		EdgeDeploymentPlanName: aws.String("slice28-edge-plan"), StageName: aws.String("stage-1"),
	})
	require.NoError(t, err)

	described, err := client.DescribeEdgeDeploymentPlan(t.Context(), &sagemakersdk.DescribeEdgeDeploymentPlanInput{
		EdgeDeploymentPlanName: aws.String("slice28-edge-plan"),
	})
	require.NoError(t, err)
	require.Len(t, described.Stages, 1)
	require.NotNil(t, described.Stages[0].DeploymentStatus)
	assert.Equal(t, smtypes.StageStatusInProgress, described.Stages[0].DeploymentStatus.StageStatus)

	_, err = client.StopEdgeDeploymentStage(t.Context(), &sagemakersdk.StopEdgeDeploymentStageInput{
		EdgeDeploymentPlanName: aws.String("slice28-edge-plan"), StageName: aws.String("stage-1"),
	})
	require.NoError(t, err)

	_, err = client.DeleteEdgeDeploymentStage(t.Context(), &sagemakersdk.DeleteEdgeDeploymentStageInput{
		EdgeDeploymentPlanName: aws.String("slice28-edge-plan"), StageName: aws.String("stage-1"),
	})
	require.NoError(t, err)

	_, err = client.DeleteEdgeDeploymentPlan(t.Context(), &sagemakersdk.DeleteEdgeDeploymentPlanInput{
		EdgeDeploymentPlanName: aws.String("slice28-edge-plan"),
	})
	require.NoError(t, err)
}

// testDeviceManagementRealClient covers DeleteDeviceFleet, DeregisterDevices,
// UpdateDevices.
func testDeviceManagementRealClient(t *testing.T) {
	t.Helper()

	client := newSlice28SageMakerClient(t)

	_, err := client.CreateDeviceFleet(t.Context(), &sagemakersdk.CreateDeviceFleetInput{
		DeviceFleetName: aws.String("slice28-device-fleet"),
		OutputConfig:    &smtypes.EdgeOutputConfig{S3OutputLocation: aws.String("s3://bucket/out")},
	})
	require.NoError(t, err)

	_, err = client.RegisterDevices(t.Context(), &sagemakersdk.RegisterDevicesInput{
		DeviceFleetName: aws.String("slice28-device-fleet"),
		Devices: []smtypes.Device{
			{DeviceName: aws.String("slice28-device-1"), Description: aws.String("original")},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateDevices(t.Context(), &sagemakersdk.UpdateDevicesInput{
		DeviceFleetName: aws.String("slice28-device-fleet"),
		Devices: []smtypes.Device{
			{DeviceName: aws.String("slice28-device-1"), Description: aws.String("updated")},
		},
	})
	require.NoError(t, err)

	described, err := client.DescribeDevice(t.Context(), &sagemakersdk.DescribeDeviceInput{
		DeviceFleetName: aws.String("slice28-device-fleet"), DeviceName: aws.String("slice28-device-1"),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated", aws.ToString(described.Description))

	_, err = client.DeregisterDevices(t.Context(), &sagemakersdk.DeregisterDevicesInput{
		DeviceFleetName: aws.String("slice28-device-fleet"), DeviceNames: []string{"slice28-device-1"},
	})
	require.NoError(t, err)

	_, err = client.DeleteDeviceFleet(t.Context(), &sagemakersdk.DeleteDeviceFleetInput{
		DeviceFleetName: aws.String("slice28-device-fleet"),
	})
	require.NoError(t, err)
}

// testEdgePackagingExtraRealClient covers StopEdgePackagingJob.
func testEdgePackagingExtraRealClient(t *testing.T) {
	t.Helper()

	client := newSlice28SageMakerClient(t)

	_, err := client.CreateEdgePackagingJob(t.Context(), &sagemakersdk.CreateEdgePackagingJobInput{
		EdgePackagingJobName: aws.String("slice28-edge-pkg-job"),
		ModelName:            aws.String("some-model"),
		ModelVersion:         aws.String("1.0"),
		RoleArn:              aws.String("arn:aws:iam::000000000000:role/edge-pkg"),
		CompilationJobName:   aws.String("some-compilation-job"),
		OutputConfig:         &smtypes.EdgeOutputConfig{S3OutputLocation: aws.String("s3://bucket/edge-pkg-out")},
	})
	require.NoError(t, err)

	_, err = client.StopEdgePackagingJob(t.Context(), &sagemakersdk.StopEdgePackagingJobInput{
		EdgePackagingJobName: aws.String("slice28-edge-pkg-job"),
	})
	require.NoError(t, err)
}

// testAppImageConfigExtraRealClient covers DeleteAppImageConfig.
func testAppImageConfigExtraRealClient(t *testing.T) {
	t.Helper()

	client := newSlice28SageMakerClient(t)

	_, err := client.CreateAppImageConfig(t.Context(), &sagemakersdk.CreateAppImageConfigInput{
		AppImageConfigName: aws.String("slice28-app-image-config"),
	})
	require.NoError(t, err)

	_, err = client.DeleteAppImageConfig(t.Context(), &sagemakersdk.DeleteAppImageConfigInput{
		AppImageConfigName: aws.String("slice28-app-image-config"),
	})
	require.NoError(t, err)
}

// testFlowDefinitionExtraRealClient covers DeleteFlowDefinition.
func testFlowDefinitionExtraRealClient(t *testing.T) {
	t.Helper()

	client := newSlice28SageMakerClient(t)

	_, err := client.CreateFlowDefinition(t.Context(), &sagemakersdk.CreateFlowDefinitionInput{
		FlowDefinitionName: aws.String("slice28-flow-def"),
		RoleArn:            aws.String("arn:aws:iam::000000000000:role/flow"),
		OutputConfig:       &smtypes.FlowDefinitionOutputConfig{S3OutputPath: aws.String("s3://bucket/flow-out")},
		HumanLoopConfig: &smtypes.HumanLoopConfig{
			HumanTaskUiArn:  aws.String("arn:aws:sagemaker:us-east-1:000000000000:human-task-ui/some-ui"),
			WorkteamArn:     aws.String("arn:aws:sagemaker:us-east-1:000000000000:workteam/private-crowd/some-team"),
			TaskTitle:       aws.String("Review"),
			TaskDescription: aws.String("Review the output"),
			TaskCount:       aws.Int32(1),
		},
	})
	require.NoError(t, err)

	_, err = client.DeleteFlowDefinition(t.Context(), &sagemakersdk.DeleteFlowDefinitionInput{
		FlowDefinitionName: aws.String("slice28-flow-def"),
	})
	require.NoError(t, err)
}

// testHubContentExtraRealClient covers DeleteHubContentReference,
// UpdateHubContent, UpdateHubContentReference.
func testHubContentExtraRealClient(t *testing.T) {
	t.Helper()

	client := newSlice28SageMakerClient(t)

	_, err := client.CreateHub(t.Context(), &sagemakersdk.CreateHubInput{
		HubName:        aws.String("slice28-hub"),
		HubDescription: aws.String("a test hub"),
	})
	require.NoError(t, err)

	_, err = client.ImportHubContent(t.Context(), &sagemakersdk.ImportHubContentInput{
		HubName:               aws.String("slice28-hub"),
		HubContentName:        aws.String("slice28-content"),
		HubContentType:        smtypes.HubContentTypeModel,
		HubContentVersion:     aws.String("1.0.0"),
		HubContentDocument:    aws.String(`{"Url":"s3://bucket/model/"}`),
		DocumentSchemaVersion: aws.String("1.0.0"),
	})
	require.NoError(t, err)

	_, err = client.UpdateHubContent(t.Context(), &sagemakersdk.UpdateHubContentInput{
		HubName:               aws.String("slice28-hub"),
		HubContentName:        aws.String("slice28-content"),
		HubContentType:        smtypes.HubContentTypeModel,
		HubContentVersion:     aws.String("1.0.0"),
		HubContentDescription: aws.String("new description"),
	})
	require.NoError(t, err)

	describedContent, err := client.DescribeHubContent(t.Context(), &sagemakersdk.DescribeHubContentInput{
		HubName: aws.String("slice28-hub"), HubContentName: aws.String("slice28-content"),
		HubContentType: smtypes.HubContentTypeModel,
	})
	require.NoError(t, err)
	assert.Equal(t, "new description", aws.ToString(describedContent.HubContentDescription))

	publicArn := "arn:aws:sagemaker:us-east-1:aws:hub-content/SageMakerPublicHub/Model/slice28-public-model/1.0.0"

	_, err = client.CreateHubContentReference(t.Context(), &sagemakersdk.CreateHubContentReferenceInput{
		HubName:                      aws.String("slice28-hub"),
		SageMakerPublicHubContentArn: aws.String(publicArn),
	})
	require.NoError(t, err)

	updatedRef, err := client.UpdateHubContentReference(t.Context(), &sagemakersdk.UpdateHubContentReferenceInput{
		HubName: aws.String("slice28-hub"), HubContentName: aws.String("slice28-public-model"),
		HubContentType: smtypes.HubContentTypeModelReference, MinVersion: aws.String("2.0.0"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(updatedRef.HubContentArn))

	_, err = client.DeleteHubContentReference(t.Context(), &sagemakersdk.DeleteHubContentReferenceInput{
		HubName: aws.String("slice28-hub"), HubContentName: aws.String("slice28-public-model"),
		HubContentType: smtypes.HubContentTypeModelReference,
	})
	require.NoError(t, err)
}

// testHumanTaskUIExtraRealClient covers DeleteHumanTaskUi.
func testHumanTaskUIExtraRealClient(t *testing.T) {
	t.Helper()

	client := newSlice28SageMakerClient(t)

	_, err := client.CreateHumanTaskUi(t.Context(), &sagemakersdk.CreateHumanTaskUiInput{
		HumanTaskUiName: aws.String("slice28-human-task-ui"),
		UiTemplate:      &smtypes.UiTemplate{Content: aws.String("<html></html>")},
	})
	require.NoError(t, err)

	_, err = client.DeleteHumanTaskUi(t.Context(), &sagemakersdk.DeleteHumanTaskUiInput{
		HumanTaskUiName: aws.String("slice28-human-task-ui"),
	})
	require.NoError(t, err)
}

// testMlflowAppExtraRealClient covers CreatePresignedMlflowAppUrl,
// DeleteMlflowApp.
func testMlflowAppExtraRealClient(t *testing.T) {
	t.Helper()

	client := newSlice28SageMakerClient(t)

	created, err := client.CreateMlflowApp(t.Context(), &sagemakersdk.CreateMlflowAppInput{
		Name:             aws.String("slice28-mlflow-app"),
		ArtifactStoreUri: aws.String("s3://bucket/mlflow"),
		RoleArn:          aws.String("arn:aws:iam::000000000000:role/mlflow"),
	})
	require.NoError(t, err)

	presigned, err := client.CreatePresignedMlflowAppUrl(t.Context(), &sagemakersdk.CreatePresignedMlflowAppUrlInput{
		Arn: created.Arn,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(presigned.AuthorizedUrl))

	_, err = client.DeleteMlflowApp(t.Context(), &sagemakersdk.DeleteMlflowAppInput{Arn: created.Arn})
	require.NoError(t, err)
}

// testSpaceExtraRealClient covers DeleteSpace.
func testSpaceExtraRealClient(t *testing.T) {
	t.Helper()

	client := newSlice28SageMakerClient(t)

	_, err := client.CreateSpace(t.Context(), &sagemakersdk.CreateSpaceInput{
		DomainId:  aws.String("d-slice28"),
		SpaceName: aws.String("slice28-space"),
	})
	require.NoError(t, err)

	_, err = client.DeleteSpace(t.Context(), &sagemakersdk.DeleteSpaceInput{
		DomainId: aws.String("d-slice28"), SpaceName: aws.String("slice28-space"),
	})
	require.NoError(t, err)
}

// testTrainingPlanExtensionExtraRealClient covers
// DescribeTrainingPlanExtensionHistory and ListUltraServersByReservedCapacity.
func testTrainingPlanExtensionExtraRealClient(t *testing.T) {
	t.Helper()

	client := newSlice28SageMakerClient(t)

	plan, err := client.CreateTrainingPlan(t.Context(), &sagemakersdk.CreateTrainingPlanInput{
		TrainingPlanName:       aws.String("slice28-training-plan"),
		TrainingPlanOfferingId: aws.String("tpo-ultraserver-gb200-30d"),
	})
	require.NoError(t, err)

	described, err := client.DescribeTrainingPlan(t.Context(), &sagemakersdk.DescribeTrainingPlanInput{
		TrainingPlanName: aws.String("slice28-training-plan"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, described.ReservedCapacitySummaries)
	reservedCapacityArn := aws.ToString(described.ReservedCapacitySummaries[0].ReservedCapacityArn)

	ultraServers, err := client.ListUltraServersByReservedCapacity(
		t.Context(), &sagemakersdk.ListUltraServersByReservedCapacityInput{
			ReservedCapacityArn: aws.String(reservedCapacityArn),
		},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, ultraServers.UltraServers)

	searchOut, err := client.SearchTrainingPlanOfferings(
		t.Context(), &sagemakersdk.SearchTrainingPlanOfferingsInput{
			TrainingPlanArn: plan.TrainingPlanArn,
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, searchOut.TrainingPlanExtensionOfferings)

	_, err = client.ExtendTrainingPlan(t.Context(), &sagemakersdk.ExtendTrainingPlanInput{
		TrainingPlanExtensionOfferingId: searchOut.TrainingPlanExtensionOfferings[0].TrainingPlanExtensionOfferingId,
	})
	require.NoError(t, err)

	history, err := client.DescribeTrainingPlanExtensionHistory(
		t.Context(), &sagemakersdk.DescribeTrainingPlanExtensionHistoryInput{
			TrainingPlanArn: plan.TrainingPlanArn,
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, history.TrainingPlanExtensions)
}

// testClusterSoftwareRealClient covers UpdateClusterSoftware.
func testClusterSoftwareRealClient(t *testing.T) {
	t.Helper()

	client := newSlice28SageMakerClient(t)

	_, err := client.CreateCluster(t.Context(), &sagemakersdk.CreateClusterInput{
		ClusterName: aws.String("slice28-cluster"),
	})
	require.NoError(t, err)

	updated, err := client.UpdateClusterSoftware(t.Context(), &sagemakersdk.UpdateClusterSoftwareInput{
		ClusterName: aws.String("slice28-cluster"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(updated.ClusterArn))
}

// testInferenceComponentRuntimeRealClient covers
// UpdateInferenceComponentRuntimeConfig.
func testInferenceComponentRuntimeRealClient(t *testing.T) {
	t.Helper()

	client := newSlice28SageMakerClient(t)

	_, err := client.CreateInferenceComponent(t.Context(), &sagemakersdk.CreateInferenceComponentInput{
		InferenceComponentName: aws.String("slice28-inf-component"),
		EndpointName:           aws.String("some-endpoint"),
	})
	require.NoError(t, err)

	updated, err := client.UpdateInferenceComponentRuntimeConfig(
		t.Context(), &sagemakersdk.UpdateInferenceComponentRuntimeConfigInput{
			InferenceComponentName: aws.String("slice28-inf-component"),
			DesiredRuntimeConfig:   &smtypes.InferenceComponentRuntimeConfig{CopyCount: aws.Int32(3)},
		},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(updated.InferenceComponentArn))

	described, err := client.DescribeInferenceComponent(t.Context(), &sagemakersdk.DescribeInferenceComponentInput{
		InferenceComponentName: aws.String("slice28-inf-component"),
	})
	require.NoError(t, err)
	require.NotNil(t, described.RuntimeConfig)
	assert.Equal(t, int32(3), aws.ToInt32(described.RuntimeConfig.DesiredCopyCount))
}

// testInferenceExperimentExtraRealClient covers StartInferenceExperiment,
// UpdateInferenceExperiment.
func testInferenceExperimentExtraRealClient(t *testing.T) {
	t.Helper()

	client := newSlice28SageMakerClient(t)

	_, err := client.CreateModel(t.Context(), &sagemakersdk.CreateModelInput{
		ModelName:        aws.String("slice28-exp-model"),
		ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/model"),
		PrimaryContainer: &smtypes.ContainerDefinition{Image: aws.String("image:latest")},
	})
	require.NoError(t, err)

	_, err = client.CreateEndpointConfig(t.Context(), &sagemakersdk.CreateEndpointConfigInput{
		EndpointConfigName: aws.String("slice28-exp-endpoint-config"),
		ProductionVariants: []smtypes.ProductionVariant{
			{
				VariantName:          aws.String("v1"),
				ModelName:            aws.String("slice28-exp-model"),
				InitialInstanceCount: aws.Int32(1),
				InstanceType:         smtypes.ProductionVariantInstanceTypeMlM5Large,
			},
		},
	})
	require.NoError(t, err)

	_, err = client.CreateEndpoint(t.Context(), &sagemakersdk.CreateEndpointInput{
		EndpointName:       aws.String("slice28-exp-endpoint"),
		EndpointConfigName: aws.String("slice28-exp-endpoint-config"),
	})
	require.NoError(t, err)

	_, err = client.CreateInferenceExperiment(t.Context(), &sagemakersdk.CreateInferenceExperimentInput{
		Name:         aws.String("slice28-exp"),
		Type:         smtypes.InferenceExperimentTypeShadowMode,
		EndpointName: aws.String("slice28-exp-endpoint"),
		RoleArn:      aws.String("arn:aws:iam::000000000000:role/exp"),
		ModelVariants: []smtypes.ModelVariantConfig{
			{
				ModelName:   aws.String("slice28-exp-model"),
				VariantName: aws.String("v1"),
				InfrastructureConfig: &smtypes.ModelInfrastructureConfig{
					InfrastructureType: smtypes.ModelInfrastructureTypeRealTimeInference,
					RealTimeInferenceConfig: &smtypes.RealTimeInferenceConfig{
						InstanceType:  smtypes.ProductionVariantInstanceTypeMlM5Large,
						InstanceCount: aws.Int32(1),
					},
				},
			},
		},
		ShadowModeConfig: &smtypes.ShadowModeConfig{
			SourceModelVariantName: aws.String("v1"),
			ShadowModelVariants:    []smtypes.ShadowModelVariantConfig{},
		},
	})
	require.NoError(t, err)

	started, err := client.StartInferenceExperiment(t.Context(), &sagemakersdk.StartInferenceExperimentInput{
		Name: aws.String("slice28-exp"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(started.InferenceExperimentArn))

	_, err = client.UpdateInferenceExperiment(t.Context(), &sagemakersdk.UpdateInferenceExperimentInput{
		Name:        aws.String("slice28-exp"),
		Description: aws.String("updated description"),
	})
	require.NoError(t, err)

	described, err := client.DescribeInferenceExperiment(t.Context(), &sagemakersdk.DescribeInferenceExperimentInput{
		Name: aws.String("slice28-exp"),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated description", aws.ToString(described.Description))
	assert.Equal(t, smtypes.InferenceExperimentStatusRunning, described.Status)
}

// testInferenceRecommendationsExtraRealClient covers
// ListInferenceRecommendationsJobSteps and StopInferenceRecommendationsJob.
func testInferenceRecommendationsExtraRealClient(t *testing.T) {
	t.Helper()

	client := newSlice28SageMakerClient(t)

	_, err := client.CreateInferenceRecommendationsJob(
		t.Context(), &sagemakersdk.CreateInferenceRecommendationsJobInput{
			JobName: aws.String("slice28-rec-job-2"),
			JobType: smtypes.RecommendationJobTypeDefault,
			RoleArn: aws.String("arn:aws:iam::000000000000:role/rec-job"),
			InputConfig: &smtypes.RecommendationJobInputConfig{
				ModelPackageVersionArn: aws.String(
					"arn:aws:sagemaker:us-east-1:000000000000:model-package/pkg/1",
				),
			},
		},
	)
	require.NoError(t, err)

	steps, err := client.ListInferenceRecommendationsJobSteps(
		t.Context(), &sagemakersdk.ListInferenceRecommendationsJobStepsInput{
			JobName: aws.String("slice28-rec-job-2"),
		},
	)
	require.NoError(t, err)
	assert.Empty(t, steps.Steps)

	_, err = client.StopInferenceRecommendationsJob(
		t.Context(), &sagemakersdk.StopInferenceRecommendationsJobInput{
			JobName: aws.String("slice28-rec-job-2"),
		},
	)
	require.NoError(t, err)
}

// testModelCardExportRealClient covers DescribeModelCardExportJob.
func testModelCardExportRealClient(t *testing.T) {
	t.Helper()

	client := newSlice28SageMakerClient(t)

	_, err := client.CreateModelCard(t.Context(), &sagemakersdk.CreateModelCardInput{
		ModelCardName:   aws.String("slice28-model-card"),
		Content:         aws.String("{}"),
		ModelCardStatus: smtypes.ModelCardStatusDraft,
	})
	require.NoError(t, err)

	created, err := client.CreateModelCardExportJob(t.Context(), &sagemakersdk.CreateModelCardExportJobInput{
		ModelCardExportJobName: aws.String("slice28-mc-export-job"),
		ModelCardName:          aws.String("slice28-model-card"),
		OutputConfig:           &smtypes.ModelCardExportOutputConfig{S3OutputPath: aws.String("s3://bucket/mc-out")},
	})
	require.NoError(t, err)

	described, err := client.DescribeModelCardExportJob(t.Context(), &sagemakersdk.DescribeModelCardExportJobInput{
		ModelCardExportJobArn: created.ModelCardExportJobArn,
	})
	require.NoError(t, err)
	assert.Equal(t, "slice28-mc-export-job", aws.ToString(described.ModelCardExportJobName))
	assert.Equal(t, smtypes.ModelCardExportJobStatusCompleted, described.Status)
}

// testPipelineVersionRealClient covers UpdatePipelineVersion.
func testPipelineVersionRealClient(t *testing.T) {
	t.Helper()

	client := newSlice28SageMakerClient(t)

	created, err := client.CreatePipeline(t.Context(), &sagemakersdk.CreatePipelineInput{
		PipelineName:       aws.String("slice28-pipeline"),
		PipelineDefinition: aws.String(`{"Version":"2020-12-01","Steps":[]}`),
		RoleArn:            aws.String("arn:aws:iam::000000000000:role/pipeline"),
	})
	require.NoError(t, err)

	versions, err := client.ListPipelineVersions(t.Context(), &sagemakersdk.ListPipelineVersionsInput{
		PipelineName: aws.String("slice28-pipeline"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, versions.PipelineVersionSummaries)
	versionID := versions.PipelineVersionSummaries[0].PipelineVersionId

	updated, err := client.UpdatePipelineVersion(t.Context(), &sagemakersdk.UpdatePipelineVersionInput{
		PipelineArn:                created.PipelineArn,
		PipelineVersionId:          versionID,
		PipelineVersionDescription: aws.String("updated version description"),
		PipelineVersionDisplayName: aws.String("v1-display"),
	})
	require.NoError(t, err)
	assert.Equal(t, versionID, updated.PipelineVersionId)

	versionsAfter, err := client.ListPipelineVersions(t.Context(), &sagemakersdk.ListPipelineVersionsInput{
		PipelineName: aws.String("slice28-pipeline"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, versionsAfter.PipelineVersionSummaries)
	assert.Equal(
		t,
		"updated version description",
		aws.ToString(versionsAfter.PipelineVersionSummaries[0].PipelineVersionDescription),
	)
	assert.Equal(t, "v1-display", aws.ToString(versionsAfter.PipelineVersionSummaries[0].PipelineVersionDisplayName))
}

// testPresignedSessionFamilyRealClient covers RenderUiTemplate and
// StartSession.
func testPresignedSessionFamilyRealClient(t *testing.T) {
	t.Helper()

	client := newSlice28SageMakerClient(t)

	rendered, err := client.RenderUiTemplate(t.Context(), &sagemakersdk.RenderUiTemplateInput{
		RoleArn: aws.String("arn:aws:iam::000000000000:role/render"),
		Task:    &smtypes.RenderableTask{Input: aws.String(`{"name":"world"}`)},
		UiTemplate: &smtypes.UiTemplate{
			Content: aws.String("Hello {{ task.input.name }}"),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "Hello world", aws.ToString(rendered.RenderedContent))
	assert.Empty(t, rendered.Errors)

	session, err := client.StartSession(t.Context(), &sagemakersdk.StartSessionInput{
		ResourceIdentifier: aws.String("arn:aws:sagemaker:us-east-1:000000000000:space/d-1/some-space"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(session.SessionId))
}
