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

func TestRealClient_SummaryFields(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testTrainingJobSummaryPlanArn, "training_job_plan_arn"},
		{testCompilationSummaryTargetPlatform, "compilation_target_platform"},
		{testInferenceExperimentCompletionTime, "inference_experiment_completion_time"},
		{testAutoMLEndTime, "automl_end_time"},
		{testClusterNodeSoftwareUpdate, "cluster_node_software_update"},
		{testHubContentOriginalCreationTime, "hub_content_original_creation_time"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

func testTrainingJobSummaryPlanArn(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	planArn := "arn:aws:sagemaker:us-east-1:000000000000:training-plan/summary-plan"

	for name, arn := range map[string]*string{"summary-tj-plan": aws.String(planArn), "summary-tj-noplan": nil} {
		_, err := client.CreateTrainingJob(t.Context(), &sagemakersdk.CreateTrainingJobInput{
			TrainingJobName:        aws.String(name),
			RoleArn:                aws.String("arn:aws:iam::000000000000:role/service-role"),
			AlgorithmSpecification: &smtypes.AlgorithmSpecification{TrainingInputMode: smtypes.TrainingInputModeFile},
			OutputDataConfig:       &smtypes.OutputDataConfig{S3OutputPath: aws.String("s3://bucket/output")},
			ResourceConfig: &smtypes.ResourceConfig{
				InstanceType:    smtypes.TrainingInstanceTypeMlM5Large,
				InstanceCount:   aws.Int32(1),
				VolumeSizeInGB:  aws.Int32(20),
				TrainingPlanArn: arn,
			},
			StoppingCondition: &smtypes.StoppingCondition{MaxRuntimeInSeconds: aws.Int32(3600)},
		})
		require.NoError(t, err)
	}

	out, err := client.ListTrainingJobs(t.Context(), &sagemakersdk.ListTrainingJobsInput{
		NameContains: aws.String("summary-tj-"),
	})
	require.NoError(t, err)
	require.Len(t, out.TrainingJobSummaries, 2)

	got := map[string]string{}
	for _, s := range out.TrainingJobSummaries {
		got[aws.ToString(s.TrainingJobName)] = aws.ToString(s.TrainingPlanArn)
	}

	assert.Equal(t, planArn, got["summary-tj-plan"])
	assert.Empty(t, got["summary-tj-noplan"])

	desc, err := client.DescribeTrainingJob(t.Context(), &sagemakersdk.DescribeTrainingJobInput{
		TrainingJobName: aws.String("summary-tj-plan"),
	})
	require.NoError(t, err)
	assert.Equal(t, planArn, aws.ToString(desc.ResourceConfig.TrainingPlanArn))
}

func testCompilationSummaryTargetPlatform(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	_, err := client.CreateCompilationJob(t.Context(), &sagemakersdk.CreateCompilationJobInput{
		CompilationJobName: aws.String("summary-compile"),
		OutputConfig: &smtypes.OutputConfig{
			S3OutputLocation: aws.String("s3://bucket/output/"),
			TargetPlatform: &smtypes.TargetPlatform{
				Os:          smtypes.TargetPlatformOsLinux,
				Arch:        smtypes.TargetPlatformArchArm64,
				Accelerator: smtypes.TargetPlatformAcceleratorNvidia,
			},
		},
		RoleArn:           aws.String("arn:aws:iam::000000000000:role/sagemaker"),
		StoppingCondition: &smtypes.StoppingCondition{MaxRuntimeInSeconds: aws.Int32(3600)},
		ModelPackageVersionArn: aws.String(
			"arn:aws:sagemaker:us-east-1:000000000000:model-package/pkg/1",
		),
	})
	require.NoError(t, err)

	list, err := client.ListCompilationJobs(t.Context(), &sagemakersdk.ListCompilationJobsInput{
		NameContains: aws.String("summary-compile"),
	})
	require.NoError(t, err)
	require.Len(t, list.CompilationJobSummaries, 1)

	s := list.CompilationJobSummaries[0]
	assert.Equal(t, smtypes.TargetPlatformOsLinux, s.CompilationTargetPlatformOs)
	assert.Equal(t, smtypes.TargetPlatformArchArm64, s.CompilationTargetPlatformArch)
	assert.Equal(t, smtypes.TargetPlatformAcceleratorNvidia, s.CompilationTargetPlatformAccelerator)

	desc, err := client.DescribeCompilationJob(t.Context(), &sagemakersdk.DescribeCompilationJobInput{
		CompilationJobName: aws.String("summary-compile"),
	})
	require.NoError(t, err)
	require.NotNil(t, desc.OutputConfig.TargetPlatform)
	assert.Equal(t, smtypes.TargetPlatformArchArm64, desc.OutputConfig.TargetPlatform.Arch)
}

func testInferenceExperimentCompletionTime(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	_, err := client.CreateInferenceExperiment(t.Context(), &sagemakersdk.CreateInferenceExperimentInput{
		Name:         aws.String("summary-exp"),
		Type:         smtypes.InferenceExperimentTypeShadowMode,
		EndpointName: aws.String("summary-exp-endpoint"),
		RoleArn:      aws.String("arn:aws:iam::000000000000:role/exp"),
		ModelVariants: []smtypes.ModelVariantConfig{{
			ModelName:   aws.String("summary-exp-model"),
			VariantName: aws.String("v1"),
			InfrastructureConfig: &smtypes.ModelInfrastructureConfig{
				InfrastructureType: smtypes.ModelInfrastructureTypeRealTimeInference,
				RealTimeInferenceConfig: &smtypes.RealTimeInferenceConfig{
					InstanceType:  smtypes.ProductionVariantInstanceTypeMlM5Large,
					InstanceCount: aws.Int32(1),
				},
			},
		}},
		ShadowModeConfig: &smtypes.ShadowModeConfig{
			SourceModelVariantName: aws.String("v1"),
			ShadowModelVariants:    []smtypes.ShadowModelVariantConfig{},
		},
	})
	require.NoError(t, err)

	before, err := client.DescribeInferenceExperiment(t.Context(), &sagemakersdk.DescribeInferenceExperimentInput{
		Name: aws.String("summary-exp"),
	})
	require.NoError(t, err)
	assert.Nil(t, before.CompletionTime)

	_, err = client.StopInferenceExperiment(t.Context(), &sagemakersdk.StopInferenceExperimentInput{
		Name:                aws.String("summary-exp"),
		ModelVariantActions: map[string]smtypes.ModelVariantAction{"v1": smtypes.ModelVariantActionRetain},
		DesiredState:        smtypes.InferenceExperimentStopDesiredStateCompleted,
	})
	require.NoError(t, err)

	after, err := client.DescribeInferenceExperiment(t.Context(), &sagemakersdk.DescribeInferenceExperimentInput{
		Name: aws.String("summary-exp"),
	})
	require.NoError(t, err)
	require.NotNil(t, after.CompletionTime)
	assert.WithinDuration(t, time.Now(), *after.CompletionTime, time.Minute)

	list, err := client.ListInferenceExperiments(t.Context(), &sagemakersdk.ListInferenceExperimentsInput{
		NameContains: aws.String("summary-exp"),
	})
	require.NoError(t, err)
	require.Len(t, list.InferenceExperiments, 1)
	require.NotNil(t, list.InferenceExperiments[0].CompletionTime)
}

func testAutoMLEndTime(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	_, err := client.CreateAutoMLJob(t.Context(), &sagemakersdk.CreateAutoMLJobInput{
		AutoMLJobName: aws.String("summary-automl"),
		InputDataConfig: []smtypes.AutoMLChannel{{
			TargetAttributeName: aws.String("target"),
			DataSource: &smtypes.AutoMLDataSource{S3DataSource: &smtypes.AutoMLS3DataSource{
				S3DataType: smtypes.AutoMLS3DataTypeS3Prefix,
				S3Uri:      aws.String("s3://bucket/input/"),
			}},
		}},
		OutputDataConfig: &smtypes.AutoMLOutputDataConfig{S3OutputPath: aws.String("s3://bucket/output/")},
		RoleArn:          aws.String("arn:aws:iam::000000000000:role/sagemaker"),
	})
	require.NoError(t, err)

	running, err := client.DescribeAutoMLJob(t.Context(), &sagemakersdk.DescribeAutoMLJobInput{
		AutoMLJobName: aws.String("summary-automl"),
	})
	require.NoError(t, err)
	assert.Nil(t, running.EndTime)

	_, err = client.StopAutoMLJob(t.Context(), &sagemakersdk.StopAutoMLJobInput{
		AutoMLJobName: aws.String("summary-automl"),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		d, descErr := client.DescribeAutoMLJob(t.Context(), &sagemakersdk.DescribeAutoMLJobInput{
			AutoMLJobName: aws.String("summary-automl"),
		})
		require.NoError(t, descErr)

		return d.AutoMLJobStatus == smtypes.AutoMLJobStatusStopped && d.EndTime != nil
	}, 5*time.Second, 10*time.Millisecond)

	list, err := client.ListAutoMLJobs(t.Context(), &sagemakersdk.ListAutoMLJobsInput{
		NameContains: aws.String("summary-automl"),
	})
	require.NoError(t, err)
	require.Len(t, list.AutoMLJobSummaries, 1)
	require.NotNil(t, list.AutoMLJobSummaries[0].EndTime)
}

func testHubContentOriginalCreationTime(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	_, err := client.CreateHub(t.Context(), &sagemakersdk.CreateHubInput{
		HubName:        aws.String("summary-hub"),
		HubDescription: aws.String("hub"),
	})
	require.NoError(t, err)

	for _, v := range []string{"1.0.0", "2.0.0"} {
		_, err = client.ImportHubContent(t.Context(), &sagemakersdk.ImportHubContentInput{
			HubName:               aws.String("summary-hub"),
			HubContentName:        aws.String("summary-content"),
			HubContentType:        smtypes.HubContentTypeModel,
			HubContentVersion:     aws.String(v),
			HubContentDocument:    aws.String(`{"Url":"s3://bucket/model/"}`),
			DocumentSchemaVersion: aws.String("1.0.0"),
		})
		require.NoError(t, err)
	}

	out, err := client.ListHubContentVersions(t.Context(), &sagemakersdk.ListHubContentVersionsInput{
		HubName:        aws.String("summary-hub"),
		HubContentName: aws.String("summary-content"),
		HubContentType: smtypes.HubContentTypeModel,
	})
	require.NoError(t, err)
	require.Len(t, out.HubContentSummaries, 2)

	var first time.Time

	for _, s := range out.HubContentSummaries {
		require.NotNil(t, s.OriginalCreationTime)

		if aws.ToString(s.HubContentVersion) == "1.0.0" {
			first = *s.CreationTime
		}
	}

	for _, s := range out.HubContentSummaries {
		assert.True(t, first.Equal(*s.OriginalCreationTime), "version %s", aws.ToString(s.HubContentVersion))
	}
}

func testClusterNodeSoftwareUpdate(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	role := aws.String("arn:aws:iam::000000000000:role/HyperPodRole")

	_, err := client.CreateCluster(t.Context(), &sagemakersdk.CreateClusterInput{
		ClusterName: aws.String("summary-cluster"),
		InstanceGroups: []smtypes.ClusterInstanceGroupSpecification{
			{
				InstanceGroupName: aws.String("workers"), InstanceType: smtypes.ClusterInstanceTypeMlM5Xlarge,
				InstanceCount: aws.Int32(1), ExecutionRole: role,
			},
			{
				InstanceGroupName: aws.String("head"), InstanceType: smtypes.ClusterInstanceTypeMlM5Xlarge,
				InstanceCount: aws.Int32(1), ExecutionRole: role,
			},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateClusterSoftware(t.Context(), &sagemakersdk.UpdateClusterSoftwareInput{
		ClusterName: aws.String("summary-cluster"),
		InstanceGroups: []smtypes.UpdateClusterSoftwareInstanceGroupSpecification{
			{InstanceGroupName: aws.String("workers"), ImageReleaseVersion: aws.String("1.2.3")},
		},
	})
	require.NoError(t, err)

	out, err := client.ListClusterNodes(t.Context(), &sagemakersdk.ListClusterNodesInput{
		ClusterName: aws.String("summary-cluster"),
	})
	require.NoError(t, err)
	require.Len(t, out.ClusterNodeSummaries, 2)

	var workerID string

	for _, n := range out.ClusterNodeSummaries {
		if aws.ToString(n.InstanceGroupName) == "workers" {
			workerID = aws.ToString(n.InstanceId)
			assert.Equal(t, "1.2.3", aws.ToString(n.CurrentImageReleaseVersion))
			require.NotNil(t, n.LastSoftwareUpdateTime)

			continue
		}

		assert.Nil(t, n.LastSoftwareUpdateTime)
		assert.Empty(t, aws.ToString(n.CurrentImageReleaseVersion))
	}

	desc, err := client.DescribeClusterNode(t.Context(), &sagemakersdk.DescribeClusterNodeInput{
		ClusterName: aws.String("summary-cluster"),
		NodeId:      aws.String(workerID),
	})
	require.NoError(t, err)
	assert.Equal(t, "1.2.3", aws.ToString(desc.NodeDetails.CurrentImageReleaseVersion))
	require.NotNil(t, desc.NodeDetails.LastSoftwareUpdateTime)
}
