package sagemaker_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSlice10_SageMaker_RealClient covers sagemaker's highest-priority
// remaining typed-client-uncovered op families (gopherstack-n3zi slice 10):
// inference components, hub/hub content, MLflow tracking servers, partner
// apps, optimization jobs, labeling jobs, studio lifecycle configs,
// workforces/workteams and monitoring schedule lifecycle.
func TestSlice10_SageMaker_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testInferenceComponentsRealClient, "inference_components"},
		{testHubRealClient, "hub"},
		{testMlflowTrackingServerRealClient, "mlflow_tracking_server"},
		{testPartnerAppRealClient, "partner_app"},
		{testOptimizationJobRealClient, "optimization_job"},
		{testLabelingJobRealClient, "labeling_job"},
		{testStudioLifecycleConfigRealClient, "studio_lifecycle_config"},
		{testWorkforceRealClient, "workforce"},
		{testWorkteamRealClient, "workteam"},
		{testMonitoringScheduleLifecycleRealClient, "monitoring_schedule_lifecycle"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

func newSlice10SageMakerClient(t *testing.T) *sagemakersdk.Client {
	t.Helper()

	h := newTestHandler(t)

	return newTestSageMakerClient(t, h)
}

// testInferenceComponentsRealClient covers CreateInferenceComponent,
// DescribeInferenceComponent, UpdateInferenceComponent,
// ListInferenceComponents, DeleteInferenceComponent.
func testInferenceComponentsRealClient(t *testing.T) {
	t.Helper()

	client := newSlice10SageMakerClient(t)

	created, err := client.CreateInferenceComponent(t.Context(), &sagemakersdk.CreateInferenceComponentInput{
		EndpointName:           aws.String("slice10-endpoint"),
		InferenceComponentName: aws.String("slice10-ic"),
		VariantName:            aws.String("variant-1"),
		Specification: &smtypes.InferenceComponentSpecification{
			ModelName:    aws.String("slice10-model"),
			InstanceType: smtypes.ProductionVariantInstanceTypeMlM5Xlarge,
		},
		RuntimeConfig: &smtypes.InferenceComponentRuntimeConfig{CopyCount: aws.Int32(1)},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(created.InferenceComponentArn))

	described, err := client.DescribeInferenceComponent(t.Context(), &sagemakersdk.DescribeInferenceComponentInput{
		InferenceComponentName: aws.String("slice10-ic"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice10-endpoint", aws.ToString(described.EndpointName))
	assert.Equal(t, "variant-1", aws.ToString(described.VariantName))

	_, err = client.UpdateInferenceComponent(t.Context(), &sagemakersdk.UpdateInferenceComponentInput{
		InferenceComponentName: aws.String("slice10-ic"),
		RuntimeConfig:          &smtypes.InferenceComponentRuntimeConfig{CopyCount: aws.Int32(2)},
	})
	require.NoError(t, err)

	listed, err := client.ListInferenceComponents(t.Context(), &sagemakersdk.ListInferenceComponentsInput{
		EndpointNameEquals: aws.String("slice10-endpoint"),
	})
	require.NoError(t, err)
	require.Len(t, listed.InferenceComponents, 1)
	assert.Equal(t, "slice10-ic", aws.ToString(listed.InferenceComponents[0].InferenceComponentName))

	_, err = client.DeleteInferenceComponent(t.Context(), &sagemakersdk.DeleteInferenceComponentInput{
		InferenceComponentName: aws.String("slice10-ic"),
	})
	require.NoError(t, err)
}

// testHubRealClient covers CreateHub, DescribeHub, ImportHubContent,
// DescribeHubContent, DeleteHubContent, DeleteHub.
func testHubRealClient(t *testing.T) {
	t.Helper()

	client := newSlice10SageMakerClient(t)

	created, err := client.CreateHub(t.Context(), &sagemakersdk.CreateHubInput{
		HubName:        aws.String("slice10-hub"),
		HubDescription: aws.String("a test hub"),
		S3StorageConfig: &smtypes.HubS3StorageConfig{
			S3OutputPath: aws.String("s3://bucket/hub/"),
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(created.HubArn))

	described, err := client.DescribeHub(t.Context(), &sagemakersdk.DescribeHubInput{
		HubName: aws.String("slice10-hub"),
	})
	require.NoError(t, err)
	assert.Equal(t, "a test hub", aws.ToString(described.HubDescription))

	_, err = client.ImportHubContent(t.Context(), &sagemakersdk.ImportHubContentInput{
		HubName:               aws.String("slice10-hub"),
		HubContentName:        aws.String("slice10-content"),
		HubContentType:        smtypes.HubContentTypeModel,
		HubContentVersion:     aws.String("1.0.0"),
		HubContentDocument:    aws.String(`{"Url":"s3://bucket/model/"}`),
		DocumentSchemaVersion: aws.String("1.0.0"),
	})
	require.NoError(t, err)

	describedContent, err := client.DescribeHubContent(t.Context(), &sagemakersdk.DescribeHubContentInput{
		HubName:        aws.String("slice10-hub"),
		HubContentName: aws.String("slice10-content"),
		HubContentType: smtypes.HubContentTypeModel,
	})
	require.NoError(t, err)
	assert.Equal(t, "1.0.0", aws.ToString(describedContent.HubContentVersion))

	_, err = client.DeleteHubContent(t.Context(), &sagemakersdk.DeleteHubContentInput{
		HubName:           aws.String("slice10-hub"),
		HubContentName:    aws.String("slice10-content"),
		HubContentType:    smtypes.HubContentTypeModel,
		HubContentVersion: aws.String("1.0.0"),
	})
	require.NoError(t, err)

	_, err = client.DeleteHub(t.Context(), &sagemakersdk.DeleteHubInput{HubName: aws.String("slice10-hub")})
	require.NoError(t, err)
}

// testMlflowTrackingServerRealClient covers CreateMlflowTrackingServer,
// DescribeMlflowTrackingServer, CreatePresignedMlflowTrackingServerUrl,
// DeleteMlflowTrackingServer.
func testMlflowTrackingServerRealClient(t *testing.T) {
	t.Helper()

	client := newSlice10SageMakerClient(t)

	created, err := client.CreateMlflowTrackingServer(t.Context(), &sagemakersdk.CreateMlflowTrackingServerInput{
		TrackingServerName: aws.String("slice10-mlflow"),
		ArtifactStoreUri:   aws.String("s3://bucket/mlflow/"),
		RoleArn:            aws.String("arn:aws:iam::000000000000:role/MlflowRole"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(created.TrackingServerArn))

	described, err := client.DescribeMlflowTrackingServer(
		t.Context(), &sagemakersdk.DescribeMlflowTrackingServerInput{TrackingServerName: aws.String("slice10-mlflow")},
	)
	require.NoError(t, err)
	assert.Equal(t, "s3://bucket/mlflow/", aws.ToString(described.ArtifactStoreUri))

	presigned, err := client.CreatePresignedMlflowTrackingServerUrl(
		t.Context(), &sagemakersdk.CreatePresignedMlflowTrackingServerUrlInput{
			TrackingServerName: aws.String("slice10-mlflow"),
		},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(presigned.AuthorizedUrl))

	_, err = client.DeleteMlflowTrackingServer(
		t.Context(), &sagemakersdk.DeleteMlflowTrackingServerInput{TrackingServerName: aws.String("slice10-mlflow")},
	)
	require.NoError(t, err)
}

// testPartnerAppRealClient covers CreatePartnerApp, DescribePartnerApp,
// CreatePartnerAppPresignedUrl, DeletePartnerApp.
func testPartnerAppRealClient(t *testing.T) {
	t.Helper()

	client := newSlice10SageMakerClient(t)

	created, err := client.CreatePartnerApp(t.Context(), &sagemakersdk.CreatePartnerAppInput{
		Name:             aws.String("slice10-partner-app"),
		Type:             smtypes.PartnerAppTypeComet,
		Tier:             aws.String("basic"),
		AuthType:         smtypes.PartnerAppAuthTypeIam,
		ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/PartnerAppRole"),
	})
	require.NoError(t, err)
	appArn := aws.ToString(created.Arn)
	require.NotEmpty(t, appArn)

	described, err := client.DescribePartnerApp(
		t.Context(),
		&sagemakersdk.DescribePartnerAppInput{Arn: aws.String(appArn)},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice10-partner-app", aws.ToString(described.Name))

	presigned, err := client.CreatePartnerAppPresignedUrl(
		t.Context(), &sagemakersdk.CreatePartnerAppPresignedUrlInput{Arn: aws.String(appArn)},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(presigned.Url))

	_, err = client.DeletePartnerApp(t.Context(), &sagemakersdk.DeletePartnerAppInput{Arn: aws.String(appArn)})
	require.NoError(t, err)
}

// testOptimizationJobRealClient covers CreateOptimizationJob,
// DescribeOptimizationJob, StopOptimizationJob, DeleteOptimizationJob.
func testOptimizationJobRealClient(t *testing.T) {
	t.Helper()

	client := newSlice10SageMakerClient(t)

	created, err := client.CreateOptimizationJob(t.Context(), &sagemakersdk.CreateOptimizationJobInput{
		OptimizationJobName: aws.String("slice10-opt-job"),
		RoleArn:             aws.String("arn:aws:iam::000000000000:role/OptRole"),
		ModelSource: &smtypes.OptimizationJobModelSource{
			S3: &smtypes.OptimizationJobModelSourceS3{S3Uri: aws.String("s3://bucket/model/")},
		},
		DeploymentInstanceType: smtypes.OptimizationJobDeploymentInstanceTypeMlG5Xlarge,
		OptimizationConfigs: []smtypes.OptimizationConfig{
			&smtypes.OptimizationConfigMemberModelQuantizationConfig{
				Value: smtypes.ModelQuantizationConfig{},
			},
		},
		OutputConfig: &smtypes.OptimizationJobOutputConfig{
			S3OutputLocation: aws.String("s3://bucket/output/"),
		},
		StoppingCondition: &smtypes.StoppingCondition{},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(created.OptimizationJobArn))

	described, err := client.DescribeOptimizationJob(
		t.Context(), &sagemakersdk.DescribeOptimizationJobInput{OptimizationJobName: aws.String("slice10-opt-job")},
	)
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::000000000000:role/OptRole", aws.ToString(described.RoleArn))

	_, err = client.StopOptimizationJob(
		t.Context(), &sagemakersdk.StopOptimizationJobInput{OptimizationJobName: aws.String("slice10-opt-job")},
	)
	require.NoError(t, err)

	_, err = client.DeleteOptimizationJob(
		t.Context(), &sagemakersdk.DeleteOptimizationJobInput{OptimizationJobName: aws.String("slice10-opt-job")},
	)
	require.NoError(t, err)
}

// testLabelingJobRealClient covers CreateLabelingJob, DescribeLabelingJob,
// ListLabelingJobs, StopLabelingJob.
func testLabelingJobRealClient(t *testing.T) {
	t.Helper()

	client := newSlice10SageMakerClient(t)

	created, err := client.CreateLabelingJob(t.Context(), &sagemakersdk.CreateLabelingJobInput{
		LabelingJobName:    aws.String("slice10-labeling-job"),
		LabelAttributeName: aws.String("label"),
		RoleArn:            aws.String("arn:aws:iam::000000000000:role/LabelingRole"),
		InputConfig: &smtypes.LabelingJobInputConfig{
			DataSource: &smtypes.LabelingJobDataSource{
				S3DataSource: &smtypes.LabelingJobS3DataSource{ManifestS3Uri: aws.String("s3://bucket/manifest.json")},
			},
		},
		OutputConfig: &smtypes.LabelingJobOutputConfig{S3OutputPath: aws.String("s3://bucket/output/")},
		HumanTaskConfig: &smtypes.HumanTaskConfig{
			WorkteamArn: aws.String(
				"arn:aws:sagemaker:us-east-1:000000000000:workteam/private-crowd/slice10-team",
			),
			UiConfig: &smtypes.UiConfig{
				UiTemplateS3Uri: aws.String("s3://bucket/template.html"),
			},
			TaskTitle:                         aws.String("Label it"),
			TaskDescription:                   aws.String("Please label"),
			NumberOfHumanWorkersPerDataObject: aws.Int32(1),
			TaskTimeLimitInSeconds:            aws.Int32(3600),
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(created.LabelingJobArn))

	described, err := client.DescribeLabelingJob(
		t.Context(), &sagemakersdk.DescribeLabelingJobInput{LabelingJobName: aws.String("slice10-labeling-job")},
	)
	require.NoError(t, err)
	assert.Equal(t, "label", aws.ToString(described.LabelAttributeName))

	listed, err := client.ListLabelingJobs(t.Context(), &sagemakersdk.ListLabelingJobsInput{
		NameContains: aws.String("slice10-labeling-job"),
	})
	require.NoError(t, err)
	require.Len(t, listed.LabelingJobSummaryList, 1)

	_, err = client.StopLabelingJob(
		t.Context(), &sagemakersdk.StopLabelingJobInput{LabelingJobName: aws.String("slice10-labeling-job")},
	)
	require.NoError(t, err)
}

// testStudioLifecycleConfigRealClient covers CreateStudioLifecycleConfig,
// DescribeStudioLifecycleConfig, DeleteStudioLifecycleConfig.
func testStudioLifecycleConfigRealClient(t *testing.T) {
	t.Helper()

	client := newSlice10SageMakerClient(t)

	created, err := client.CreateStudioLifecycleConfig(t.Context(), &sagemakersdk.CreateStudioLifecycleConfigInput{
		StudioLifecycleConfigName:    aws.String("slice10-lifecycle-config"),
		StudioLifecycleConfigAppType: smtypes.StudioLifecycleConfigAppTypeJupyterServer,
		StudioLifecycleConfigContent: aws.String("IyEvYmluL2Jhc2gK"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(created.StudioLifecycleConfigArn))

	described, err := client.DescribeStudioLifecycleConfig(
		t.Context(), &sagemakersdk.DescribeStudioLifecycleConfigInput{
			StudioLifecycleConfigName: aws.String("slice10-lifecycle-config"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, smtypes.StudioLifecycleConfigAppTypeJupyterServer, described.StudioLifecycleConfigAppType)

	_, err = client.DeleteStudioLifecycleConfig(
		t.Context(), &sagemakersdk.DeleteStudioLifecycleConfigInput{
			StudioLifecycleConfigName: aws.String("slice10-lifecycle-config"),
		},
	)
	require.NoError(t, err)
}

// testWorkforceRealClient covers CreateWorkforce, DescribeWorkforce,
// UpdateWorkforce, ListWorkforces, DeleteWorkforce.
func testWorkforceRealClient(t *testing.T) {
	t.Helper()

	client := newSlice10SageMakerClient(t)

	created, err := client.CreateWorkforce(t.Context(), &sagemakersdk.CreateWorkforceInput{
		WorkforceName: aws.String("slice10-workforce"),
		SourceIpConfig: &smtypes.SourceIpConfig{
			Cidrs: []string{"10.0.0.0/16"},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(created.WorkforceArn))

	described, err := client.DescribeWorkforce(
		t.Context(), &sagemakersdk.DescribeWorkforceInput{WorkforceName: aws.String("slice10-workforce")},
	)
	require.NoError(t, err)
	require.NotNil(t, described.Workforce)
	assert.Equal(t, "slice10-workforce", aws.ToString(described.Workforce.WorkforceName))

	_, err = client.UpdateWorkforce(t.Context(), &sagemakersdk.UpdateWorkforceInput{
		WorkforceName: aws.String("slice10-workforce"),
		SourceIpConfig: &smtypes.SourceIpConfig{
			Cidrs: []string{"10.0.0.0/8"},
		},
	})
	require.NoError(t, err)

	listed, err := client.ListWorkforces(t.Context(), &sagemakersdk.ListWorkforcesInput{
		NameContains: aws.String("slice10-workforce"),
	})
	require.NoError(t, err)
	require.Len(t, listed.Workforces, 1)

	_, err = client.DeleteWorkforce(
		t.Context(), &sagemakersdk.DeleteWorkforceInput{WorkforceName: aws.String("slice10-workforce")},
	)
	require.NoError(t, err)
}

// testWorkteamRealClient covers CreateWorkteam, DescribeWorkteam,
// ListWorkteams, DeleteWorkteam.
func testWorkteamRealClient(t *testing.T) {
	t.Helper()

	client := newSlice10SageMakerClient(t)

	created, err := client.CreateWorkteam(t.Context(), &sagemakersdk.CreateWorkteamInput{
		WorkteamName: aws.String("slice10-workteam"),
		Description:  aws.String("a test workteam"),
		MemberDefinitions: []smtypes.MemberDefinition{
			{CognitoMemberDefinition: &smtypes.CognitoMemberDefinition{
				UserPool: aws.String(
					"us-east-1_abc123",
				), UserGroup: aws.String("labelers"), ClientId: aws.String("client-1"),
			}},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(created.WorkteamArn))

	described, err := client.DescribeWorkteam(
		t.Context(), &sagemakersdk.DescribeWorkteamInput{WorkteamName: aws.String("slice10-workteam")},
	)
	require.NoError(t, err)
	require.NotNil(t, described.Workteam)
	assert.Equal(t, "a test workteam", aws.ToString(described.Workteam.Description))

	listed, err := client.ListWorkteams(t.Context(), &sagemakersdk.ListWorkteamsInput{
		NameContains: aws.String("slice10-workteam"),
	})
	require.NoError(t, err)
	require.Len(t, listed.Workteams, 1)

	_, err = client.DeleteWorkteam(
		t.Context(), &sagemakersdk.DeleteWorkteamInput{WorkteamName: aws.String("slice10-workteam")},
	)
	require.NoError(t, err)
}

// testMonitoringScheduleLifecycleRealClient covers StartMonitoringSchedule,
// StopMonitoringSchedule and DeleteMonitoringSchedule -- built on top of a
// real monitoring schedule (CreateMonitoringSchedule is already
// typed-covered from an earlier slice).
func testMonitoringScheduleLifecycleRealClient(t *testing.T) {
	t.Helper()

	client := newSlice10SageMakerClient(t)

	_, err := client.CreateMonitoringSchedule(t.Context(), &sagemakersdk.CreateMonitoringScheduleInput{
		MonitoringScheduleName: aws.String("slice10-monitoring-schedule"),
		MonitoringScheduleConfig: &smtypes.MonitoringScheduleConfig{
			MonitoringJobDefinitionName: aws.String("slice10-job-def"),
			MonitoringType:              smtypes.MonitoringTypeDataQuality,
		},
	})
	require.NoError(t, err)

	_, err = client.StopMonitoringSchedule(t.Context(), &sagemakersdk.StopMonitoringScheduleInput{
		MonitoringScheduleName: aws.String("slice10-monitoring-schedule"),
	})
	require.NoError(t, err)

	_, err = client.StartMonitoringSchedule(t.Context(), &sagemakersdk.StartMonitoringScheduleInput{
		MonitoringScheduleName: aws.String("slice10-monitoring-schedule"),
	})
	require.NoError(t, err)

	_, err = client.DeleteMonitoringSchedule(t.Context(), &sagemakersdk.DeleteMonitoringScheduleInput{
		MonitoringScheduleName: aws.String("slice10-monitoring-schedule"),
	})
	require.NoError(t, err)
}
