package terraform_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersvc "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	sagemakertypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_SagemakerResources provisions SageMaker (code repository, notebook
// instance + lifecycle configuration, app image config, studio lifecycle
// config, domain + user profile + space + app, image + image version, model
// package group + policy, workforce + workteam, human task UI + flow
// definition, hub, service catalog portfolio status, device fleet + device,
// model + endpoint configuration + endpoint, pipeline, project, feature
// group, MLflow tracking server, data quality job definition + monitoring
// schedule) resources via Terraform and verifies each through its own SDK
// client's Describe/Get path.
func TestTerraform_SagemakerResources(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "sagemaker-resources",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifySagemakerResourcesSageMaker(ctx, t)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

func verifySagemakerResourcesSageMaker(ctx context.Context, t *testing.T) {
	t.Helper()

	client := createSageMakerClient(t)

	codeRepoOut, err := client.DescribeCodeRepository(ctx, &sagemakersvc.DescribeCodeRepositoryInput{
		CodeRepositoryName: aws.String("smkr-code-repo"),
	})
	require.NoError(t, err, "DescribeCodeRepository should succeed")
	require.NotNil(t, codeRepoOut.GitConfig)
	assert.Contains(t, aws.ToString(codeRepoOut.GitConfig.RepositoryUrl), "smkr-repo")

	lifecycleOut, err := client.DescribeNotebookInstanceLifecycleConfig(ctx,
		&sagemakersvc.DescribeNotebookInstanceLifecycleConfigInput{
			NotebookInstanceLifecycleConfigName: aws.String("smkr-nb-lifecycle"),
		})
	require.NoError(t, err, "DescribeNotebookInstanceLifecycleConfig should succeed")
	require.NotEmpty(t, lifecycleOut.OnCreate)

	nbOut, err := client.DescribeNotebookInstance(ctx, &sagemakersvc.DescribeNotebookInstanceInput{
		NotebookInstanceName: aws.String("smkr-notebook"),
	})
	require.NoError(t, err, "DescribeNotebookInstance should succeed")
	assert.Equal(t, "smkr-nb-lifecycle", aws.ToString(nbOut.NotebookInstanceLifecycleConfigName))

	aicOut, err := client.DescribeAppImageConfig(ctx, &sagemakersvc.DescribeAppImageConfigInput{
		AppImageConfigName: aws.String("smkr-app-image-config"),
	})
	require.NoError(t, err, "DescribeAppImageConfig should succeed")
	require.NotNil(t, aicOut.KernelGatewayImageConfig)

	slcOut, err := client.DescribeStudioLifecycleConfig(ctx, &sagemakersvc.DescribeStudioLifecycleConfigInput{
		StudioLifecycleConfigName: aws.String("smkr-studio-lifecycle"),
	})
	require.NoError(t, err, "DescribeStudioLifecycleConfig should succeed")
	assert.Equal(t, sagemakertypes.StudioLifecycleConfigAppTypeJupyterServer, slcOut.StudioLifecycleConfigAppType)

	domainOut, err := client.DescribeDomain(ctx, &sagemakersvc.DescribeDomainInput{
		DomainId: aws.String(domainIDByName(ctx, t, client, "smkr-domain")),
	})
	require.NoError(t, err, "DescribeDomain should succeed")
	assert.Equal(t, "smkr-domain", aws.ToString(domainOut.DomainName))
	domainID := aws.ToString(domainOut.DomainId)

	upOut, err := client.DescribeUserProfile(ctx, &sagemakersvc.DescribeUserProfileInput{
		DomainId:        aws.String(domainID),
		UserProfileName: aws.String("smkr-user-profile"),
	})
	require.NoError(t, err, "DescribeUserProfile should succeed")
	assert.Equal(t, sagemakertypes.UserProfileStatusInService, upOut.Status)

	spaceOut, err := client.DescribeSpace(ctx, &sagemakersvc.DescribeSpaceInput{
		DomainId:  aws.String(domainID),
		SpaceName: aws.String("smkr-space"),
	})
	require.NoError(t, err, "DescribeSpace should succeed")
	assert.Equal(t, "smkr-space", aws.ToString(spaceOut.SpaceName))

	appOut, err := client.DescribeApp(ctx, &sagemakersvc.DescribeAppInput{
		DomainId:        aws.String(domainID),
		UserProfileName: aws.String("smkr-user-profile"),
		AppName:         aws.String("smkr-app"),
		AppType:         sagemakertypes.AppTypeJupyterServer,
	})
	require.NoError(t, err, "DescribeApp should succeed")
	assert.Equal(t, "smkr-app", aws.ToString(appOut.AppName))

	imgOut, err := client.DescribeImage(ctx, &sagemakersvc.DescribeImageInput{
		ImageName: aws.String("smkr-image"),
	})
	require.NoError(t, err, "DescribeImage should succeed")
	assert.Equal(t, "smkr-image", aws.ToString(imgOut.ImageName))

	imgVerOut, err := client.DescribeImageVersion(ctx, &sagemakersvc.DescribeImageVersionInput{
		ImageName: aws.String("smkr-image"),
	})
	require.NoError(t, err, "DescribeImageVersion should succeed")
	assert.Contains(t, aws.ToString(imgVerOut.BaseImage), "smkr-sagemaker-repo")

	mpgOut, err := client.DescribeModelPackageGroup(ctx, &sagemakersvc.DescribeModelPackageGroupInput{
		ModelPackageGroupName: aws.String("smkr-model-package-group"),
	})
	require.NoError(t, err, "DescribeModelPackageGroup should succeed")
	assert.Equal(t, "smkr-model-package-group", aws.ToString(mpgOut.ModelPackageGroupName))

	mpgPolicyOut, err := client.GetModelPackageGroupPolicy(ctx, &sagemakersvc.GetModelPackageGroupPolicyInput{
		ModelPackageGroupName: aws.String("smkr-model-package-group"),
	})
	require.NoError(t, err, "GetModelPackageGroupPolicy should succeed")
	assert.Contains(t, aws.ToString(mpgPolicyOut.ResourcePolicy), "AddPermModelPackageGroup")

	wfOut, err := client.DescribeWorkforce(ctx, &sagemakersvc.DescribeWorkforceInput{
		WorkforceName: aws.String("smkr-workforce"),
	})
	require.NoError(t, err, "DescribeWorkforce should succeed")
	require.NotNil(t, wfOut.Workforce)
	require.NotNil(t, wfOut.Workforce.CognitoConfig)

	wtOut, err := client.DescribeWorkteam(ctx, &sagemakersvc.DescribeWorkteamInput{
		WorkteamName: aws.String("smkr-workteam"),
	})
	require.NoError(t, err, "DescribeWorkteam should succeed")
	require.NotNil(t, wtOut.Workteam)
	assert.Len(t, wtOut.Workteam.MemberDefinitions, 1)

	htuiOut, err := client.DescribeHumanTaskUi(ctx, &sagemakersvc.DescribeHumanTaskUiInput{
		HumanTaskUiName: aws.String("smkr-human-task-ui"),
	})
	require.NoError(t, err, "DescribeHumanTaskUi should succeed")
	require.NotNil(t, htuiOut.UiTemplate)

	fdOut, err := client.DescribeFlowDefinition(ctx, &sagemakersvc.DescribeFlowDefinitionInput{
		FlowDefinitionName: aws.String("smkr-flow-definition"),
	})
	require.NoError(t, err, "DescribeFlowDefinition should succeed")
	require.NotNil(t, fdOut.HumanLoopConfig)
	assert.Equal(t, int32(1), aws.ToInt32(fdOut.HumanLoopConfig.TaskCount))

	hubOut, err := client.DescribeHub(ctx, &sagemakersvc.DescribeHubInput{
		HubName: aws.String("smkr-hub"),
	})
	require.NoError(t, err, "DescribeHub should succeed")
	assert.Equal(t, "Smkr Hub", aws.ToString(hubOut.HubDisplayName))

	portfolioOut, err := client.GetSagemakerServicecatalogPortfolioStatus(ctx,
		&sagemakersvc.GetSagemakerServicecatalogPortfolioStatusInput{})
	require.NoError(t, err, "GetSagemakerServicecatalogPortfolioStatus should succeed")
	assert.Equal(t, sagemakertypes.SagemakerServicecatalogStatusEnabled, portfolioOut.Status)

	dfOut, err := client.DescribeDeviceFleet(ctx, &sagemakersvc.DescribeDeviceFleetInput{
		DeviceFleetName: aws.String("smkr-device-fleet"),
	})
	require.NoError(t, err, "DescribeDeviceFleet should succeed")
	assert.Contains(t, aws.ToString(dfOut.OutputConfig.S3OutputLocation), "device-fleet-output")

	devOut, err := client.DescribeDevice(ctx, &sagemakersvc.DescribeDeviceInput{
		DeviceFleetName: aws.String("smkr-device-fleet"),
		DeviceName:      aws.String("smkr-device"),
	})
	require.NoError(t, err, "DescribeDevice should succeed")
	assert.Equal(t, "smkr-device", aws.ToString(devOut.DeviceName))

	modelOut, err := client.DescribeModel(ctx, &sagemakersvc.DescribeModelInput{
		ModelName: aws.String("smkr-model"),
	})
	require.NoError(t, err, "DescribeModel should succeed")
	assert.Equal(t, "smkr-model", aws.ToString(modelOut.ModelName))

	epConfigOut, err := client.DescribeEndpointConfig(ctx, &sagemakersvc.DescribeEndpointConfigInput{
		EndpointConfigName: aws.String("smkr-endpoint-config"),
	})
	require.NoError(t, err, "DescribeEndpointConfig should succeed")
	require.Len(t, epConfigOut.ProductionVariants, 1)

	epOut, err := client.DescribeEndpoint(ctx, &sagemakersvc.DescribeEndpointInput{
		EndpointName: aws.String("smkr-endpoint"),
	})
	require.NoError(t, err, "DescribeEndpoint should succeed")
	assert.Equal(t, sagemakertypes.EndpointStatusInService, epOut.EndpointStatus)

	pipelineOut, err := client.DescribePipeline(ctx, &sagemakersvc.DescribePipelineInput{
		PipelineName: aws.String("smkr-pipeline"),
	})
	require.NoError(t, err, "DescribePipeline should succeed")
	assert.Equal(t, "SmkrPipeline", aws.ToString(pipelineOut.PipelineDisplayName))

	projOut, err := client.DescribeProject(ctx, &sagemakersvc.DescribeProjectInput{
		ProjectName: aws.String("smkr-project"),
	})
	require.NoError(t, err, "DescribeProject should succeed")
	require.NotNil(t, projOut.ServiceCatalogProvisioningDetails)
	assert.Equal(t, "prod-abcdefghijklm", aws.ToString(projOut.ServiceCatalogProvisioningDetails.ProductId))

	fgOut, err := client.DescribeFeatureGroup(ctx, &sagemakersvc.DescribeFeatureGroupInput{
		FeatureGroupName: aws.String("smkr-feature-group"),
	})
	require.NoError(t, err, "DescribeFeatureGroup should succeed")
	assert.Len(t, fgOut.FeatureDefinitions, 2)

	mlflowOut, err := client.DescribeMlflowTrackingServer(ctx, &sagemakersvc.DescribeMlflowTrackingServerInput{
		TrackingServerName: aws.String("smkr-mlflow"),
	})
	require.NoError(t, err, "DescribeMlflowTrackingServer should succeed")
	assert.Contains(t, aws.ToString(mlflowOut.ArtifactStoreUri), "mlflow")

	dqjdOut, err := client.DescribeDataQualityJobDefinition(ctx, &sagemakersvc.DescribeDataQualityJobDefinitionInput{
		JobDefinitionName: aws.String("smkr-data-quality-job"),
	})
	require.NoError(t, err, "DescribeDataQualityJobDefinition should succeed")
	require.NotNil(t, dqjdOut.DataQualityJobInput.EndpointInput)
	assert.Equal(t, "smkr-endpoint", aws.ToString(dqjdOut.DataQualityJobInput.EndpointInput.EndpointName))

	msOut, err := client.DescribeMonitoringSchedule(ctx, &sagemakersvc.DescribeMonitoringScheduleInput{
		MonitoringScheduleName: aws.String("smkr-monitoring-schedule"),
	})
	require.NoError(t, err, "DescribeMonitoringSchedule should succeed")
	assert.Equal(t, "smkr-data-quality-job",
		aws.ToString(msOut.MonitoringScheduleConfig.MonitoringJobDefinitionName))
}

// domainIDByName finds a SageMaker domain's ID by name via ListDomains, since
// Terraform's aws_sagemaker_domain resource ID is the domain ID (not the
// name) and this test has no other way to learn it.
func domainIDByName(ctx context.Context, t *testing.T, client *sagemakersvc.Client, name string) string {
	t.Helper()

	out, err := client.ListDomains(ctx, &sagemakersvc.ListDomainsInput{})
	require.NoError(t, err, "ListDomains should succeed")

	for _, d := range out.Domains {
		if aws.ToString(d.DomainName) == name {
			return aws.ToString(d.DomainId)
		}
	}

	t.Fatalf("domain %q not found in ListDomains", name)

	return ""
}
