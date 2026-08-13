package sagemaker_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

const smTagsRTRegion = "us-east-1"

// newTestSageMakerClient stands up the real aws-sdk-go-v2 sagemaker client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestSageMakerClient(t *testing.T, h *sagemaker.Handler) *sagemakersdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(smTagsRTRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return sagemakersdk.NewFromConfig(cfg, func(o *sagemakersdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOpsWithTags_RoundTrip spot-checks resource kinds that ARE in
// findTagMapLocked's tag-lookup registry (services/sagemaker/tags.go) through
// the real SDK client, asserting ListTags sees what was supplied at creation
// (gopherstack-2mwl, gopherstack-qyon).
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *sagemakersdk.Client) *string
		name  string
	}{
		{
			name: "model",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateModel(t.Context(), &sagemakersdk.CreateModelInput{
					ModelName:        aws.String("tagged-model"),
					ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/access"),
					Tags: []smtypes.Tag{
						{Key: aws.String("env"), Value: aws.String("test")},
					},
				})
				require.NoError(t, err)

				return out.ModelArn
			},
		},
		{
			name: "endpoint config",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateEndpointConfig(
					t.Context(),
					&sagemakersdk.CreateEndpointConfigInput{
						EndpointConfigName: aws.String("tagged-epc"),
						ProductionVariants: []smtypes.ProductionVariant{
							{
								VariantName:          aws.String("v1"),
								ModelName:            aws.String("some-model"),
								InitialInstanceCount: aws.Int32(1),
								InstanceType:         smtypes.ProductionVariantInstanceTypeMlM5Large,
							},
						},
						Tags: []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
					},
				)
				require.NoError(t, err)

				return out.EndpointConfigArn
			},
		},
		{
			name: "algorithm",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateAlgorithm(t.Context(), &sagemakersdk.CreateAlgorithmInput{
					AlgorithmName: aws.String("tagged-algo"),
					TrainingSpecification: &smtypes.TrainingSpecification{
						TrainingImage: aws.String(
							"123456789012.dkr.ecr.us-east-1.amazonaws.com/algo:latest",
						),
						SupportedTrainingInstanceTypes: []smtypes.TrainingInstanceType{
							smtypes.TrainingInstanceTypeMlM5Large,
						},
						TrainingChannels: []smtypes.ChannelSpecification{
							{Name: aws.String("train"), SupportedContentTypes: []string{"text/csv"},
								SupportedInputModes: []smtypes.TrainingInputMode{
									smtypes.TrainingInputModeFile,
								}},
						},
					},
					Tags: []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return out.AlgorithmArn
			},
		},
		{
			// CreateModelPackage's handler decoded Tags as a JSON object instead
			// of the wire's array-of-{Key,Value} shape, so any tagged create
			// failed outright (gopherstack-qyon) -- model package was never
			// actually spot-verified despite being in the 18-kind registry.
			name: "model package",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateModelPackage(t.Context(), &sagemakersdk.CreateModelPackageInput{
					ModelPackageName: aws.String("tagged-model-package"),
					Tags:             []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return out.ModelPackageArn
			},
		},
		{
			name: "action",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateAction(t.Context(), &sagemakersdk.CreateActionInput{
					ActionName: aws.String("tagged-action"),
					ActionType: aws.String("ModelDeployment"),
					Source:     &smtypes.ActionSource{SourceUri: aws.String("s3://bucket/key")},
					Tags:       []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return out.ActionArn
			},
		},
		{
			name: "experiment",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateExperiment(
					t.Context(),
					&sagemakersdk.CreateExperimentInput{
						ExperimentName: aws.String("tagged-experiment"),
						Tags: []smtypes.Tag{
							{Key: aws.String("env"), Value: aws.String("test")},
						},
					},
				)
				require.NoError(t, err)

				return out.ExperimentArn
			},
		},
		{
			name: "context",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateContext(t.Context(), &sagemakersdk.CreateContextInput{
					ContextName: aws.String("tagged-context"),
					ContextType: aws.String("Endpoint"),
					Source:      &smtypes.ContextSource{SourceUri: aws.String("s3://bucket/key")},
					Tags:        []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return out.ContextArn
			},
		},
		{
			name: "artifact",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateArtifact(t.Context(), &sagemakersdk.CreateArtifactInput{
					ArtifactType: aws.String("Model"),
					Source:       &smtypes.ArtifactSource{SourceUri: aws.String("s3://bucket/key")},
					Tags:         []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return out.ArtifactArn
			},
		},
		{
			name: "model package group",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateModelPackageGroup(
					t.Context(),
					&sagemakersdk.CreateModelPackageGroupInput{
						ModelPackageGroupName: aws.String("tagged-mpg"),
						Tags: []smtypes.Tag{
							{Key: aws.String("env"), Value: aws.String("test")},
						},
					},
				)
				require.NoError(t, err)

				return out.ModelPackageGroupArn
			},
		},
		{
			name: "app",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateApp(t.Context(), &sagemakersdk.CreateAppInput{
					DomainId:        aws.String("d-tagged"),
					UserProfileName: aws.String("tagged-user"),
					AppType:         smtypes.AppTypeJupyterServer,
					AppName:         aws.String("tagged-app"),
					Tags:            []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return out.AppArn
			},
		},
		{
			name: "app image config",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateAppImageConfig(t.Context(), &sagemakersdk.CreateAppImageConfigInput{
					AppImageConfigName: aws.String("tagged-app-image-config"),
					Tags:               []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return out.AppImageConfigArn
			},
		},
		{
			// CreateAutoMLJob's handler decoded Tags as a JSON object instead of the
			// wire's array-of-{Key,Value} shape (gopherstack-qyon).
			name: "autoML job",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateAutoMLJob(t.Context(), &sagemakersdk.CreateAutoMLJobInput{
					AutoMLJobName: aws.String("tagged-automl-job"),
					InputDataConfig: []smtypes.AutoMLChannel{
						{TargetAttributeName: aws.String("target")},
					},
					OutputDataConfig: &smtypes.AutoMLOutputDataConfig{S3OutputPath: aws.String("s3://bucket/out")},
					RoleArn:          aws.String("arn:aws:iam::000000000000:role/access"),
					Tags:             []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return out.AutoMLJobArn
			},
		},
		{
			name: "autoML job v2",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateAutoMLJobV2(t.Context(), &sagemakersdk.CreateAutoMLJobV2Input{
					AutoMLJobName:            aws.String("tagged-automl-job-v2"),
					AutoMLJobInputDataConfig: []smtypes.AutoMLJobChannel{{}},
					OutputDataConfig: &smtypes.AutoMLOutputDataConfig{
						S3OutputPath: aws.String("s3://bucket/out"),
					},
					AutoMLProblemTypeConfig: &smtypes.AutoMLProblemTypeConfigMemberTabularJobConfig{
						Value: smtypes.TabularJobConfig{TargetAttributeName: aws.String("target")},
					},
					RoleArn: aws.String("arn:aws:iam::000000000000:role/access"),
					Tags:    []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return out.AutoMLJobArn
			},
		},
		{
			name: "cluster scheduler config",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateClusterSchedulerConfig(
					t.Context(),
					&sagemakersdk.CreateClusterSchedulerConfigInput{
						Name: aws.String("tagged-cluster-scheduler-config"),
						ClusterArn: aws.String(
							"arn:aws:sagemaker:us-east-1:000000000000:cluster/tagged-cluster",
						),
						SchedulerConfig: &smtypes.SchedulerConfig{},
						Tags:            []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
					},
				)
				require.NoError(t, err)

				return out.ClusterSchedulerConfigArn
			},
		},
		{
			name: "code repository",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateCodeRepository(t.Context(), &sagemakersdk.CreateCodeRepositoryInput{
					CodeRepositoryName: aws.String("tagged-code-repo"),
					GitConfig: &smtypes.GitConfig{
						RepositoryUrl: aws.String("https://example.com/repo.git"),
					},
					Tags: []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return out.CodeRepositoryArn
			},
		},
		{
			name: "compilation job",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateCompilationJob(t.Context(), &sagemakersdk.CreateCompilationJobInput{
					CompilationJobName: aws.String("tagged-compilation-job"),
					RoleArn:            aws.String("arn:aws:iam::000000000000:role/access"),
					OutputConfig:       &smtypes.OutputConfig{S3OutputLocation: aws.String("s3://bucket/out")},
					StoppingCondition:  &smtypes.StoppingCondition{},
					Tags:               []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return out.CompilationJobArn
			},
		},
		{
			name: "compute quota",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateComputeQuota(t.Context(), &sagemakersdk.CreateComputeQuotaInput{
					Name: aws.String("tagged-compute-quota"),
					ClusterArn: aws.String(
						"arn:aws:sagemaker:us-east-1:000000000000:cluster/tagged-cluster",
					),
					ComputeQuotaConfig: &smtypes.ComputeQuotaConfig{},
					ComputeQuotaTarget: &smtypes.ComputeQuotaTarget{TeamName: aws.String("team-1")},
					Tags:               []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return out.ComputeQuotaArn
			},
		},
		{
			name: "data quality job definition",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateDataQualityJobDefinition(
					t.Context(),
					&sagemakersdk.CreateDataQualityJobDefinitionInput{
						JobDefinitionName: aws.String("tagged-dq-job-def"),
						DataQualityAppSpecification: &smtypes.DataQualityAppSpecification{
							ImageUri: aws.String(
								"123456789012.dkr.ecr.us-east-1.amazonaws.com/dq:latest",
							),
						},
						DataQualityJobInput: &smtypes.DataQualityJobInput{},
						DataQualityJobOutputConfig: &smtypes.MonitoringOutputConfig{
							MonitoringOutputs: []smtypes.MonitoringOutput{
								{S3Output: &smtypes.MonitoringS3Output{
									S3Uri:     aws.String("s3://bucket/out"),
									LocalPath: aws.String("/opt/ml/output"),
								}},
							},
						},
						JobResources: &smtypes.MonitoringResources{
							ClusterConfig: &smtypes.MonitoringClusterConfig{
								InstanceCount:  aws.Int32(1),
								InstanceType:   smtypes.ProcessingInstanceTypeMlT3Medium,
								VolumeSizeInGB: aws.Int32(10),
							},
						},
						RoleArn: aws.String("arn:aws:iam::000000000000:role/access"),
						Tags:    []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
					},
				)
				require.NoError(t, err)

				return out.JobDefinitionArn
			},
		},
		{
			// The four job-definition Create ops share one decode path
			// (parseJobDefRequest); model bias exercises GroundTruthS3Input,
			// the sibling data-quality case exercises the plain shape.
			name: "model bias job definition",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateModelBiasJobDefinition(
					t.Context(),
					&sagemakersdk.CreateModelBiasJobDefinitionInput{
						JobDefinitionName: aws.String("tagged-mb-job-def"),
						ModelBiasAppSpecification: &smtypes.ModelBiasAppSpecification{
							ImageUri: aws.String(
								"123456789012.dkr.ecr.us-east-1.amazonaws.com/mb:latest",
							),
							ConfigUri: aws.String("s3://bucket/config.json"),
						},
						ModelBiasJobInput: &smtypes.ModelBiasJobInput{
							GroundTruthS3Input: &smtypes.MonitoringGroundTruthS3Input{
								S3Uri: aws.String("s3://bucket/gt"),
							},
						},
						ModelBiasJobOutputConfig: &smtypes.MonitoringOutputConfig{
							MonitoringOutputs: []smtypes.MonitoringOutput{
								{S3Output: &smtypes.MonitoringS3Output{
									S3Uri:     aws.String("s3://bucket/out"),
									LocalPath: aws.String("/opt/ml/output"),
								}},
							},
						},
						JobResources: &smtypes.MonitoringResources{
							ClusterConfig: &smtypes.MonitoringClusterConfig{
								InstanceCount:  aws.Int32(1),
								InstanceType:   smtypes.ProcessingInstanceTypeMlT3Medium,
								VolumeSizeInGB: aws.Int32(10),
							},
						},
						RoleArn: aws.String("arn:aws:iam::000000000000:role/access"),
						Tags:    []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
					},
				)
				require.NoError(t, err)

				return out.JobDefinitionArn
			},
		},
		{
			name: "model explainability job definition",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateModelExplainabilityJobDefinition(
					t.Context(),
					&sagemakersdk.CreateModelExplainabilityJobDefinitionInput{
						JobDefinitionName: aws.String("tagged-me-job-def"),
						ModelExplainabilityAppSpecification: &smtypes.ModelExplainabilityAppSpecification{
							ImageUri: aws.String(
								"123456789012.dkr.ecr.us-east-1.amazonaws.com/me:latest",
							),
							ConfigUri: aws.String("s3://bucket/config.json"),
						},
						ModelExplainabilityJobInput: &smtypes.ModelExplainabilityJobInput{},
						ModelExplainabilityJobOutputConfig: &smtypes.MonitoringOutputConfig{
							MonitoringOutputs: []smtypes.MonitoringOutput{
								{S3Output: &smtypes.MonitoringS3Output{
									S3Uri:     aws.String("s3://bucket/out"),
									LocalPath: aws.String("/opt/ml/output"),
								}},
							},
						},
						JobResources: &smtypes.MonitoringResources{
							ClusterConfig: &smtypes.MonitoringClusterConfig{
								InstanceCount:  aws.Int32(1),
								InstanceType:   smtypes.ProcessingInstanceTypeMlT3Medium,
								VolumeSizeInGB: aws.Int32(10),
							},
						},
						RoleArn: aws.String("arn:aws:iam::000000000000:role/access"),
						Tags:    []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
					},
				)
				require.NoError(t, err)

				return out.JobDefinitionArn
			},
		},
		{
			name: "model quality job definition",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateModelQualityJobDefinition(
					t.Context(),
					&sagemakersdk.CreateModelQualityJobDefinitionInput{
						JobDefinitionName: aws.String("tagged-mq-job-def"),
						ModelQualityAppSpecification: &smtypes.ModelQualityAppSpecification{
							ImageUri: aws.String(
								"123456789012.dkr.ecr.us-east-1.amazonaws.com/mq:latest",
							),
						},
						ModelQualityJobInput: &smtypes.ModelQualityJobInput{
							GroundTruthS3Input: &smtypes.MonitoringGroundTruthS3Input{
								S3Uri: aws.String("s3://bucket/gt"),
							},
						},
						ModelQualityJobOutputConfig: &smtypes.MonitoringOutputConfig{
							MonitoringOutputs: []smtypes.MonitoringOutput{
								{S3Output: &smtypes.MonitoringS3Output{
									S3Uri:     aws.String("s3://bucket/out"),
									LocalPath: aws.String("/opt/ml/output"),
								}},
							},
						},
						JobResources: &smtypes.MonitoringResources{
							ClusterConfig: &smtypes.MonitoringClusterConfig{
								InstanceCount:  aws.Int32(1),
								InstanceType:   smtypes.ProcessingInstanceTypeMlT3Medium,
								VolumeSizeInGB: aws.Int32(10),
							},
						},
						RoleArn: aws.String("arn:aws:iam::000000000000:role/access"),
						Tags:    []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
					},
				)
				require.NoError(t, err)

				return out.JobDefinitionArn
			},
		},
		{
			// CreateDeviceFleet's own output carries no ARN (a real void-result
			// op), so the ARN comes from a follow-up Describe.
			name: "device fleet",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				_, err := client.CreateDeviceFleet(t.Context(), &sagemakersdk.CreateDeviceFleetInput{
					DeviceFleetName: aws.String("tagged-device-fleet"),
					OutputConfig: &smtypes.EdgeOutputConfig{
						S3OutputLocation: aws.String("s3://bucket/out"),
					},
					Tags: []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				desc, err := client.DescribeDeviceFleet(t.Context(), &sagemakersdk.DescribeDeviceFleetInput{
					DeviceFleetName: aws.String("tagged-device-fleet"),
				})
				require.NoError(t, err)

				return desc.DeviceFleetArn
			},
		},
		{
			name: "edge deployment plan",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				_, err := client.CreateDeviceFleet(t.Context(), &sagemakersdk.CreateDeviceFleetInput{
					DeviceFleetName: aws.String("tagged-device-fleet-2"),
					OutputConfig: &smtypes.EdgeOutputConfig{
						S3OutputLocation: aws.String("s3://bucket/out"),
					},
				})
				require.NoError(t, err)

				out, err := client.CreateEdgeDeploymentPlan(t.Context(), &sagemakersdk.CreateEdgeDeploymentPlanInput{
					EdgeDeploymentPlanName: aws.String("tagged-edge-deployment-plan"),
					DeviceFleetName:        aws.String("tagged-device-fleet-2"),
					ModelConfigs: []smtypes.EdgeDeploymentModelConfig{
						{ModelHandle: aws.String("handle-1"), EdgePackagingJobName: aws.String("job-1")},
					},
					Tags: []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return out.EdgeDeploymentPlanArn
			},
		},
		{
			// CreateEdgePackagingJob's own output carries no ARN either; the ARN
			// comes from a follow-up Describe.
			name: "edge packaging job",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				_, err := client.CreateEdgePackagingJob(t.Context(), &sagemakersdk.CreateEdgePackagingJobInput{
					EdgePackagingJobName: aws.String("tagged-edge-packaging-job"),
					CompilationJobName:   aws.String("some-compilation-job"),
					ModelName:            aws.String("some-model"),
					ModelVersion:         aws.String("1"),
					RoleArn:              aws.String("arn:aws:iam::000000000000:role/access"),
					OutputConfig: &smtypes.EdgeOutputConfig{
						S3OutputLocation: aws.String("s3://bucket/out"),
					},
					Tags: []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				desc, err := client.DescribeEdgePackagingJob(
					t.Context(),
					&sagemakersdk.DescribeEdgePackagingJobInput{
						EdgePackagingJobName: aws.String("tagged-edge-packaging-job"),
					},
				)
				require.NoError(t, err)

				return desc.EdgePackagingJobArn
			},
		},
		{
			name: "flow definition",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateFlowDefinition(t.Context(), &sagemakersdk.CreateFlowDefinitionInput{
					FlowDefinitionName: aws.String("tagged-flow-definition"),
					OutputConfig: &smtypes.FlowDefinitionOutputConfig{
						S3OutputPath: aws.String("s3://bucket/out"),
					},
					RoleArn: aws.String("arn:aws:iam::000000000000:role/access"),
					Tags:    []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return out.FlowDefinitionArn
			},
		},
		{
			name: "hub",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateHub(t.Context(), &sagemakersdk.CreateHubInput{
					HubName:        aws.String("tagged-hub"),
					HubDescription: aws.String("a hub"),
					Tags:           []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return out.HubArn
			},
		},
		{
			name: "human task ui",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateHumanTaskUi(t.Context(), &sagemakersdk.CreateHumanTaskUiInput{
					HumanTaskUiName: aws.String("tagged-human-task-ui"),
					UiTemplate:      &smtypes.UiTemplate{Content: aws.String("<html></html>")},
					Tags:            []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return out.HumanTaskUiArn
			},
		},
		{
			name: "image",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateImage(t.Context(), &sagemakersdk.CreateImageInput{
					ImageName: aws.String("tagged-image"),
					RoleArn:   aws.String("arn:aws:iam::000000000000:role/access"),
					Tags:      []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return out.ImageArn
			},
		},
		{
			name: "inference component",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateInferenceComponent(t.Context(), &sagemakersdk.CreateInferenceComponentInput{
					InferenceComponentName: aws.String("tagged-inference-component"),
					EndpointName:           aws.String("some-endpoint"),
					Tags:                   []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return out.InferenceComponentArn
			},
		},
		{
			name: "inference experiment",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateInferenceExperiment(t.Context(), &sagemakersdk.CreateInferenceExperimentInput{
					Name:         aws.String("tagged-inference-experiment"),
					Type:         smtypes.InferenceExperimentTypeShadowMode,
					RoleArn:      aws.String("arn:aws:iam::000000000000:role/access"),
					EndpointName: aws.String("some-endpoint"),
					ModelVariants: []smtypes.ModelVariantConfig{
						{
							ModelName:   aws.String("some-model"),
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
						ShadowModelVariants: []smtypes.ShadowModelVariantConfig{
							{ShadowModelVariantName: aws.String("v1"), SamplingPercentage: aws.Int32(10)},
						},
					},
					Tags: []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return out.InferenceExperimentArn
			},
		},
		{
			name: "inference recommendations job",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateInferenceRecommendationsJob(
					t.Context(),
					&sagemakersdk.CreateInferenceRecommendationsJobInput{
						JobName:     aws.String("tagged-inference-recommendations-job"),
						JobType:     smtypes.RecommendationJobTypeDefault,
						RoleArn:     aws.String("arn:aws:iam::000000000000:role/access"),
						InputConfig: &smtypes.RecommendationJobInputConfig{},
						Tags:        []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
					},
				)
				require.NoError(t, err)

				return out.JobArn
			},
		},
		{
			name: "mlflow app",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateMlflowApp(t.Context(), &sagemakersdk.CreateMlflowAppInput{
					Name:             aws.String("tagged-mlflow-app"),
					ArtifactStoreUri: aws.String("s3://bucket/mlflow"),
					RoleArn:          aws.String("arn:aws:iam::000000000000:role/access"),
					Tags:             []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return out.Arn
			},
		},
		{
			name: "mlflow tracking server",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateMlflowTrackingServer(
					t.Context(),
					&sagemakersdk.CreateMlflowTrackingServerInput{
						TrackingServerName: aws.String("tagged-mlflow-tracking-server"),
						ArtifactStoreUri:   aws.String("s3://bucket/mlflow"),
						RoleArn:            aws.String("arn:aws:iam::000000000000:role/access"),
						Tags:               []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
					},
				)
				require.NoError(t, err)

				return out.TrackingServerArn
			},
		},
		{
			name: "model card",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateModelCard(t.Context(), &sagemakersdk.CreateModelCardInput{
					ModelCardName:   aws.String("tagged-model-card"),
					Content:         aws.String("{}"),
					ModelCardStatus: smtypes.ModelCardStatusDraft,
					Tags:            []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return out.ModelCardArn
			},
		},
		{
			name: "monitoring schedule",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateMonitoringSchedule(t.Context(), &sagemakersdk.CreateMonitoringScheduleInput{
					MonitoringScheduleName:   aws.String("tagged-monitoring-schedule"),
					MonitoringScheduleConfig: &smtypes.MonitoringScheduleConfig{},
					Tags:                     []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return out.MonitoringScheduleArn
			},
		},
		{
			name: "notebook instance lifecycle config",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateNotebookInstanceLifecycleConfig(
					t.Context(),
					&sagemakersdk.CreateNotebookInstanceLifecycleConfigInput{
						NotebookInstanceLifecycleConfigName: aws.String("tagged-nb-lifecycle-config"),
						Tags: []smtypes.Tag{
							{Key: aws.String("env"), Value: aws.String("test")},
						},
					},
				)
				require.NoError(t, err)

				return out.NotebookInstanceLifecycleConfigArn
			},
		},
		{
			name: "optimization job",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateOptimizationJob(t.Context(), &sagemakersdk.CreateOptimizationJobInput{
					OptimizationJobName:    aws.String("tagged-optimization-job"),
					RoleArn:                aws.String("arn:aws:iam::000000000000:role/access"),
					ModelSource:            &smtypes.OptimizationJobModelSource{},
					DeploymentInstanceType: smtypes.OptimizationJobDeploymentInstanceTypeMlP4d24xlarge,
					OptimizationConfigs:    []smtypes.OptimizationConfig{},
					OutputConfig: &smtypes.OptimizationJobOutputConfig{
						S3OutputLocation: aws.String("s3://bucket/out"),
					},
					StoppingCondition: &smtypes.StoppingCondition{},
					Tags:              []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return out.OptimizationJobArn
			},
		},
		{
			name: "partner app",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreatePartnerApp(t.Context(), &sagemakersdk.CreatePartnerAppInput{
					Name:             aws.String("tagged-partner-app"),
					Type:             smtypes.PartnerAppTypeComet,
					ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/access"),
					Tier:             aws.String("small"),
					AuthType:         smtypes.PartnerAppAuthTypeIam,
					Tags:             []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return out.Arn
			},
		},
		{
			name: "project",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateProject(t.Context(), &sagemakersdk.CreateProjectInput{
					ProjectName: aws.String("tagged-project"),
					Tags:        []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return out.ProjectArn
			},
		},
		{
			name: "space",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateSpace(t.Context(), &sagemakersdk.CreateSpaceInput{
					DomainId:  aws.String("d-tagged"),
					SpaceName: aws.String("tagged-space"),
					Tags:      []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return out.SpaceArn
			},
		},
		{
			name: "studio lifecycle config",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateStudioLifecycleConfig(
					t.Context(),
					&sagemakersdk.CreateStudioLifecycleConfigInput{
						StudioLifecycleConfigName:    aws.String("tagged-studio-lifecycle-config"),
						StudioLifecycleConfigContent: aws.String("IyEvYmluL2Jhc2g="),
						StudioLifecycleConfigAppType: smtypes.StudioLifecycleConfigAppTypeJupyterServer,
						Tags: []smtypes.Tag{
							{Key: aws.String("env"), Value: aws.String("test")},
						},
					},
				)
				require.NoError(t, err)

				return out.StudioLifecycleConfigArn
			},
		},
		{
			name: "training plan",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateTrainingPlan(t.Context(), &sagemakersdk.CreateTrainingPlanInput{
					TrainingPlanName:       aws.String("tagged-training-plan"),
					TrainingPlanOfferingId: aws.String("offering-1"),
					Tags:                   []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return out.TrainingPlanArn
			},
		},
		{
			name: "user profile",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateUserProfile(t.Context(), &sagemakersdk.CreateUserProfileInput{
					DomainId:        aws.String("d-tagged"),
					UserProfileName: aws.String("tagged-user-profile"),
					Tags:            []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return out.UserProfileArn
			},
		},
		{
			name: "workforce",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateWorkforce(t.Context(), &sagemakersdk.CreateWorkforceInput{
					WorkforceName: aws.String("tagged-workforce"),
					CognitoConfig: &smtypes.CognitoConfig{
						UserPool: aws.String("pool-1"),
						ClientId: aws.String("client-1"),
					},
					Tags: []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return out.WorkforceArn
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sagemaker.NewInMemoryBackend("000000000000", smTagsRTRegion)
			client := newTestSageMakerClient(t, sagemaker.NewHandler(backend))

			resourceARN := tt.setup(t, client)
			require.NotNil(t, resourceARN)

			got, err := client.ListTags(
				t.Context(),
				&sagemakersdk.ListTagsInput{ResourceArn: resourceARN},
			)
			require.NoError(t, err)
			require.Len(t, got.Tags, 1)
			assert.Equal(t, "env", aws.ToString(got.Tags[0].Key))
			assert.Equal(t, "test", aws.ToString(got.Tags[0].Value))
		})
	}
}

// TestCreateWorkteam_TagsRoundTrip documents the fix for gopherstack-2mwl's
// largest outstanding instance: CreateWorkteam accepts Tags
// (sagemaker@v1.263.2 api_op_CreateWorkteam.go:78), and work teams are
// explicitly listed in the real AddTags doc comment (api_op_AddTags.go:13)
// as a taggable kind. Workteam is now one of the kinds covered by
// indexedTagLookups (services/sagemaker/tags.go); this was previously
// TestCreateWorkteam_TagsRoundTrip_KnownGap, a tripwire asserting the
// then-current broken behavior (gopherstack-qyon).
func TestCreateWorkteam_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	backend := sagemaker.NewInMemoryBackend("000000000000", smTagsRTRegion)
	client := newTestSageMakerClient(t, sagemaker.NewHandler(backend))

	out, err := client.CreateWorkteam(t.Context(), &sagemakersdk.CreateWorkteamInput{
		WorkteamName: aws.String("tagged-workteam"),
		Description:  aws.String("desc"),
		MemberDefinitions: []smtypes.MemberDefinition{
			{CognitoMemberDefinition: &smtypes.CognitoMemberDefinition{
				ClientId:  aws.String("client-1"),
				UserPool:  aws.String("pool-1"),
				UserGroup: aws.String("group-1"),
			}},
		},
		Tags: []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
	})
	require.NoError(t, err)

	got, err := client.ListTags(t.Context(), &sagemakersdk.ListTagsInput{ResourceArn: out.WorkteamArn})
	require.NoError(t, err)
	require.Len(t, got.Tags, 1)
	assert.Equal(t, "env", aws.ToString(got.Tags[0].Key))
	assert.Equal(t, "test", aws.ToString(got.Tags[0].Value))
}

// TestCreateHubContentReference_TagsRoundTrip covers the one remaining
// registered kind that needs a pre-existing resource (a Hub) rather than a
// standalone Create call, so it does not fit TestCreateOpsWithTags_RoundTrip's
// single-setup-func table shape.
func TestCreateHubContentReference_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	backend := sagemaker.NewInMemoryBackend("000000000000", smTagsRTRegion)
	client := newTestSageMakerClient(t, sagemaker.NewHandler(backend))

	_, err := client.CreateHub(t.Context(), &sagemakersdk.CreateHubInput{
		HubName:        aws.String("tagged-hub-for-ref"),
		HubDescription: aws.String("a hub"),
	})
	require.NoError(t, err)

	out, err := client.CreateHubContentReference(t.Context(), &sagemakersdk.CreateHubContentReferenceInput{
		HubName: aws.String("tagged-hub-for-ref"),
		SageMakerPublicHubContentArn: aws.String(
			"arn:aws:sagemaker:us-east-1:aws:hub-content/SageMakerPublicHub/Model/some-model/1.0.0",
		),
		Tags: []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
	})
	require.NoError(t, err)

	got, err := client.ListTags(t.Context(), &sagemakersdk.ListTagsInput{ResourceArn: out.HubContentArn})
	require.NoError(t, err)
	require.Len(t, got.Tags, 1)
	assert.Equal(t, "env", aws.ToString(got.Tags[0].Key))
	assert.Equal(t, "test", aws.ToString(got.Tags[0].Value))
}
