package sagemaker_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_DomainsPipelinesAndTags covers sagemaker's highest-priority
// typed-client-uncovered op families (gopherstack-n3zi): tags,
// domains/user profiles/apps, endpoints/endpoint configs, notebook
// instances, algorithms, code repositories, model package group policy,
// pipelines, experiments/trials, projects, images, hyperparameter tuning,
// model cards, clusters, feature groups and search. Each subtest creates
// real state through the typed aws-sdk-go-v2 client and asserts decoded
// response values.
func TestRealClient_DomainsPipelinesAndTags(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testTagsExtraRealClient, "tags_extra"},
		{testDomainsRealClient, "domains"},
		{testEndpointsExtraRealClient, "endpoints_extra"},
		{testNotebookInstancesExtraRealClient, "notebook_instances_extra"},
		{testAlgorithmsRealClient, "algorithms"},
		{testCodeRepositoriesRealClient, "code_repositories"},
		{testModelPackageGroupPolicyRealClient, "model_package_group_policy"},
		{testPipelinesExtraRealClient, "pipelines_extra"},
		{testExperimentsTrialsExtraRealClient, "experiments_trials_extra"},
		{testProjectsExtraRealClient, "projects_extra"},
		{testImagesExtraRealClient, "images_extra"},
		{testHPTuningExtraRealClient, "hp_tuning_extra"},
		{testModelCardsExtraRealClient, "model_cards_extra"},
		{testClustersExtraRealClient, "clusters_extra"},
		{testFeatureGroupsExtraRealClient, "feature_groups_extra"},
		{testSearchSuggestionsRealClient, "search_suggestions"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

func newRealClient(t *testing.T) *sagemakersdk.Client {
	t.Helper()

	h := newTestHandler(t)

	return newTestSageMakerClient(t, h)
}

// testTagsExtraRealClient covers AddTags and DeleteTags.
func testTagsExtraRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	created, err := client.CreateModelPackageGroup(t.Context(), &sagemakersdk.CreateModelPackageGroupInput{
		ModelPackageGroupName: aws.String("slice4-tags-mpg"),
	})
	require.NoError(t, err)

	_, err = client.AddTags(t.Context(), &sagemakersdk.AddTagsInput{
		ResourceArn: created.ModelPackageGroupArn,
		Tags:        []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)

	listed, err := client.ListTags(t.Context(), &sagemakersdk.ListTagsInput{ResourceArn: created.ModelPackageGroupArn})
	require.NoError(t, err)
	require.Len(t, listed.Tags, 1)
	assert.Equal(t, "env", aws.ToString(listed.Tags[0].Key))

	_, err = client.DeleteTags(t.Context(), &sagemakersdk.DeleteTagsInput{
		ResourceArn: created.ModelPackageGroupArn,
		TagKeys:     []string{"env"},
	})
	require.NoError(t, err)

	listed, err = client.ListTags(t.Context(), &sagemakersdk.ListTagsInput{ResourceArn: created.ModelPackageGroupArn})
	require.NoError(t, err)
	assert.Empty(t, listed.Tags)
}

// testDomainsRealClient covers CreateDomain, DescribeDomain, ListDomains,
// UpdateDomain, DeleteUserProfile, ListUserProfiles, DeleteApp, DescribeApp,
// ListApps, CreatePresignedDomainUrl, DeleteDomain.
func testDomainsRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	created, err := client.CreateDomain(t.Context(), &sagemakersdk.CreateDomainInput{
		DomainName:          aws.String("slice4-domain"),
		AuthMode:            smtypes.AuthModeIam,
		DefaultUserSettings: &smtypes.UserSettings{},
	})
	require.NoError(t, err)
	domainID := created.DomainId

	desc, err := client.DescribeDomain(t.Context(), &sagemakersdk.DescribeDomainInput{DomainId: domainID})
	require.NoError(t, err)
	assert.Equal(t, "slice4-domain", aws.ToString(desc.DomainName))

	list, err := client.ListDomains(t.Context(), &sagemakersdk.ListDomainsInput{})
	require.NoError(t, err)
	found := false

	for _, d := range list.Domains {
		if aws.ToString(d.DomainId) == aws.ToString(domainID) {
			found = true
		}
	}

	assert.True(t, found)

	_, err = client.UpdateDomain(t.Context(), &sagemakersdk.UpdateDomainInput{
		DomainId:             domainID,
		AppNetworkAccessType: smtypes.AppNetworkAccessTypeVpcOnly,
	})
	require.NoError(t, err)

	desc, err = client.DescribeDomain(t.Context(), &sagemakersdk.DescribeDomainInput{DomainId: domainID})
	require.NoError(t, err)
	assert.Equal(t, smtypes.AppNetworkAccessTypeVpcOnly, desc.AppNetworkAccessType)

	_, err = client.CreateUserProfile(t.Context(), &sagemakersdk.CreateUserProfileInput{
		DomainId:        domainID,
		UserProfileName: aws.String("slice4-user"),
	})
	require.NoError(t, err)

	presigned, err := client.CreatePresignedDomainUrl(t.Context(), &sagemakersdk.CreatePresignedDomainUrlInput{
		DomainId:        domainID,
		UserProfileName: aws.String("slice4-user"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(presigned.AuthorizedUrl))

	users, err := client.ListUserProfiles(t.Context(), &sagemakersdk.ListUserProfilesInput{
		DomainIdEquals: domainID,
	})
	require.NoError(t, err)
	require.Len(t, users.UserProfiles, 1)
	assert.Equal(t, "slice4-user", aws.ToString(users.UserProfiles[0].UserProfileName))

	_, err = client.CreateApp(t.Context(), &sagemakersdk.CreateAppInput{
		DomainId:        domainID,
		UserProfileName: aws.String("slice4-user"),
		AppType:         smtypes.AppTypeJupyterServer,
		AppName:         aws.String("default"),
	})
	require.NoError(t, err)

	appDesc, err := client.DescribeApp(t.Context(), &sagemakersdk.DescribeAppInput{
		DomainId: domainID, UserProfileName: aws.String("slice4-user"),
		AppType: smtypes.AppTypeJupyterServer, AppName: aws.String("default"),
	})
	require.NoError(t, err)
	assert.Equal(t, "default", aws.ToString(appDesc.AppName))

	apps, err := client.ListApps(t.Context(), &sagemakersdk.ListAppsInput{DomainIdEquals: domainID})
	require.NoError(t, err)
	require.Len(t, apps.Apps, 1)

	_, err = client.DeleteApp(t.Context(), &sagemakersdk.DeleteAppInput{
		DomainId: domainID, UserProfileName: aws.String("slice4-user"),
		AppType: smtypes.AppTypeJupyterServer, AppName: aws.String("default"),
	})
	require.NoError(t, err)

	_, err = client.DeleteUserProfile(t.Context(), &sagemakersdk.DeleteUserProfileInput{
		DomainId: domainID, UserProfileName: aws.String("slice4-user"),
	})
	require.NoError(t, err)

	_, err = client.DeleteDomain(t.Context(), &sagemakersdk.DeleteDomainInput{DomainId: domainID})
	require.NoError(t, err)
}

// testEndpointsExtraRealClient covers DeleteEndpointConfig, DeleteEndpoint,
// DescribeEndpoint, ListEndpointConfigs, ListEndpoints, UpdateEndpoint,
// UpdateEndpointWeightsAndCapacities.
func testEndpointsExtraRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	_, err := client.CreateModel(t.Context(), &sagemakersdk.CreateModelInput{
		ModelName:        aws.String("slice4-ep-model"),
		ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/exec"),
		PrimaryContainer: &smtypes.ContainerDefinition{Image: aws.String("image:latest")},
	})
	require.NoError(t, err)

	_, err = client.CreateEndpointConfig(t.Context(), &sagemakersdk.CreateEndpointConfigInput{
		EndpointConfigName: aws.String("slice4-epc"),
		ProductionVariants: []smtypes.ProductionVariant{{
			VariantName: aws.String("v1"), ModelName: aws.String("slice4-ep-model"),
			InitialInstanceCount: aws.Int32(1), InstanceType: smtypes.ProductionVariantInstanceTypeMlM5Large,
		}},
	})
	require.NoError(t, err)

	_, err = client.CreateEndpointConfig(t.Context(), &sagemakersdk.CreateEndpointConfigInput{
		EndpointConfigName: aws.String("slice4-epc-2"),
		ProductionVariants: []smtypes.ProductionVariant{{
			VariantName: aws.String("v1"), ModelName: aws.String("slice4-ep-model"),
			InitialInstanceCount: aws.Int32(1), InstanceType: smtypes.ProductionVariantInstanceTypeMlM5Large,
		}},
	})
	require.NoError(t, err)

	_, err = client.CreateEndpoint(t.Context(), &sagemakersdk.CreateEndpointInput{
		EndpointName: aws.String("slice4-ep"), EndpointConfigName: aws.String("slice4-epc"),
	})
	require.NoError(t, err)

	desc, err := client.DescribeEndpoint(t.Context(), &sagemakersdk.DescribeEndpointInput{
		EndpointName: aws.String("slice4-ep"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice4-epc", aws.ToString(desc.EndpointConfigName))

	eps, err := client.ListEndpoints(t.Context(), &sagemakersdk.ListEndpointsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, eps.Endpoints)

	epcs, err := client.ListEndpointConfigs(t.Context(), &sagemakersdk.ListEndpointConfigsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, epcs.EndpointConfigs)

	_, err = client.UpdateEndpoint(t.Context(), &sagemakersdk.UpdateEndpointInput{
		EndpointName: aws.String("slice4-ep"), EndpointConfigName: aws.String("slice4-epc-2"),
	})
	require.NoError(t, err)

	desc, err = client.DescribeEndpoint(t.Context(), &sagemakersdk.DescribeEndpointInput{
		EndpointName: aws.String("slice4-ep"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice4-epc-2", aws.ToString(desc.EndpointConfigName))

	_, err = client.UpdateEndpointWeightsAndCapacities(
		t.Context(),
		&sagemakersdk.UpdateEndpointWeightsAndCapacitiesInput{
			EndpointName: aws.String("slice4-ep"),
			DesiredWeightsAndCapacities: []smtypes.DesiredWeightAndCapacity{
				{VariantName: aws.String("v1"), DesiredWeight: aws.Float32(1), DesiredInstanceCount: aws.Int32(1)},
			},
		},
	)
	require.NoError(t, err)

	_, err = client.DeleteEndpoint(
		t.Context(),
		&sagemakersdk.DeleteEndpointInput{EndpointName: aws.String("slice4-ep")},
	)
	require.NoError(t, err)

	_, err = client.DeleteEndpointConfig(t.Context(), &sagemakersdk.DeleteEndpointConfigInput{
		EndpointConfigName: aws.String("slice4-epc"),
	})
	require.NoError(t, err)

	_, err = client.DeleteEndpointConfig(t.Context(), &sagemakersdk.DeleteEndpointConfigInput{
		EndpointConfigName: aws.String("slice4-epc-2"),
	})
	require.NoError(t, err)
}

// testNotebookInstancesExtraRealClient covers
// CreatePresignedNotebookInstanceUrl, StartNotebookInstance,
// DeleteNotebookInstance, DescribeNotebookInstanceLifecycleConfig,
// UpdateNotebookInstanceLifecycleConfig,
// DeleteNotebookInstanceLifecycleConfig.
func testNotebookInstancesExtraRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	_, err := client.CreateNotebookInstanceLifecycleConfig(
		t.Context(), &sagemakersdk.CreateNotebookInstanceLifecycleConfigInput{
			NotebookInstanceLifecycleConfigName: aws.String("slice4-lc"),
			OnCreate: []smtypes.NotebookInstanceLifecycleHook{
				{Content: aws.String("ZWNobyBoaQ==")},
			},
		})
	require.NoError(t, err)

	lc, err := client.DescribeNotebookInstanceLifecycleConfig(
		t.Context(), &sagemakersdk.DescribeNotebookInstanceLifecycleConfigInput{
			NotebookInstanceLifecycleConfigName: aws.String("slice4-lc"),
		})
	require.NoError(t, err)
	require.Len(t, lc.OnCreate, 1)

	_, err = client.UpdateNotebookInstanceLifecycleConfig(
		t.Context(), &sagemakersdk.UpdateNotebookInstanceLifecycleConfigInput{
			NotebookInstanceLifecycleConfigName: aws.String("slice4-lc"),
			OnStart: []smtypes.NotebookInstanceLifecycleHook{
				{Content: aws.String("ZWNobyBzdGFydA==")},
			},
		})
	require.NoError(t, err)

	lc, err = client.DescribeNotebookInstanceLifecycleConfig(
		t.Context(), &sagemakersdk.DescribeNotebookInstanceLifecycleConfigInput{
			NotebookInstanceLifecycleConfigName: aws.String("slice4-lc"),
		})
	require.NoError(t, err)
	require.Len(t, lc.OnStart, 1)

	_, err = client.DeleteNotebookInstanceLifecycleConfig(
		t.Context(), &sagemakersdk.DeleteNotebookInstanceLifecycleConfigInput{
			NotebookInstanceLifecycleConfigName: aws.String("slice4-lc"),
		})
	require.NoError(t, err)

	_, err = client.CreateNotebookInstance(t.Context(), &sagemakersdk.CreateNotebookInstanceInput{
		NotebookInstanceName: aws.String("slice4-nb"),
		InstanceType:         smtypes.InstanceTypeMlT2Medium,
		RoleArn:              aws.String("arn:aws:iam::000000000000:role/nb"),
	})
	require.NoError(t, err)

	presigned, err := client.CreatePresignedNotebookInstanceUrl(
		t.Context(), &sagemakersdk.CreatePresignedNotebookInstanceUrlInput{
			NotebookInstanceName: aws.String("slice4-nb"),
		})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(presigned.AuthorizedUrl))

	_, err = client.StartNotebookInstance(t.Context(), &sagemakersdk.StartNotebookInstanceInput{
		NotebookInstanceName: aws.String("slice4-nb"),
	})
	require.NoError(t, err)

	_, err = client.StopNotebookInstance(t.Context(), &sagemakersdk.StopNotebookInstanceInput{
		NotebookInstanceName: aws.String("slice4-nb"),
	})
	require.NoError(t, err)

	_, err = client.DeleteNotebookInstance(t.Context(), &sagemakersdk.DeleteNotebookInstanceInput{
		NotebookInstanceName: aws.String("slice4-nb"),
	})
	require.NoError(t, err)
}

// testAlgorithmsRealClient covers DeleteAlgorithm, DescribeAlgorithm,
// ListAlgorithms.
func testAlgorithmsRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	_, err := client.CreateAlgorithm(t.Context(), &sagemakersdk.CreateAlgorithmInput{
		AlgorithmName: aws.String("slice4-algo"),
		TrainingSpecification: &smtypes.TrainingSpecification{
			TrainingImage:                  aws.String("image:latest"),
			SupportedTrainingInstanceTypes: []smtypes.TrainingInstanceType{smtypes.TrainingInstanceTypeMlM5Large},
			TrainingChannels: []smtypes.ChannelSpecification{{
				Name: aws.String("train"), SupportedContentTypes: []string{"text/csv"},
				SupportedInputModes: []smtypes.TrainingInputMode{smtypes.TrainingInputModeFile},
			}},
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeAlgorithm(t.Context(), &sagemakersdk.DescribeAlgorithmInput{
		AlgorithmName: aws.String("slice4-algo"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice4-algo", aws.ToString(desc.AlgorithmName))

	list, err := client.ListAlgorithms(t.Context(), &sagemakersdk.ListAlgorithmsInput{})
	require.NoError(t, err)
	found := false

	for _, a := range list.AlgorithmSummaryList {
		if aws.ToString(a.AlgorithmName) == "slice4-algo" {
			found = true
		}
	}

	assert.True(t, found)

	_, err = client.DeleteAlgorithm(t.Context(), &sagemakersdk.DeleteAlgorithmInput{
		AlgorithmName: aws.String("slice4-algo"),
	})
	require.NoError(t, err)
}

// testCodeRepositoriesRealClient covers DeleteCodeRepository,
// DescribeCodeRepository, UpdateCodeRepository.
func testCodeRepositoriesRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	_, err := client.CreateCodeRepository(t.Context(), &sagemakersdk.CreateCodeRepositoryInput{
		CodeRepositoryName: aws.String("slice4-repo"),
		GitConfig:          &smtypes.GitConfig{RepositoryUrl: aws.String("https://example.com/repo.git")},
	})
	require.NoError(t, err)

	desc, err := client.DescribeCodeRepository(t.Context(), &sagemakersdk.DescribeCodeRepositoryInput{
		CodeRepositoryName: aws.String("slice4-repo"),
	})
	require.NoError(t, err)
	assert.Equal(t, "https://example.com/repo.git", aws.ToString(desc.GitConfig.RepositoryUrl))

	_, err = client.UpdateCodeRepository(t.Context(), &sagemakersdk.UpdateCodeRepositoryInput{
		CodeRepositoryName: aws.String("slice4-repo"),
		GitConfig: &smtypes.GitConfigForUpdate{
			SecretArn: aws.String("arn:aws:secretsmanager:us-east-1:000000000000:secret:s"),
		},
	})
	require.NoError(t, err)

	desc, err = client.DescribeCodeRepository(t.Context(), &sagemakersdk.DescribeCodeRepositoryInput{
		CodeRepositoryName: aws.String("slice4-repo"),
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:secretsmanager:us-east-1:000000000000:secret:s", aws.ToString(desc.GitConfig.SecretArn))

	_, err = client.DeleteCodeRepository(t.Context(), &sagemakersdk.DeleteCodeRepositoryInput{
		CodeRepositoryName: aws.String("slice4-repo"),
	})
	require.NoError(t, err)
}

// testModelPackageGroupPolicyRealClient covers PutModelPackageGroupPolicy,
// GetModelPackageGroupPolicy, DeleteModelPackageGroupPolicy,
// DeleteModelPackage.
func testModelPackageGroupPolicyRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	_, err := client.CreateModelPackageGroup(t.Context(), &sagemakersdk.CreateModelPackageGroupInput{
		ModelPackageGroupName: aws.String("slice4-mpg-policy"),
	})
	require.NoError(t, err)

	policy := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Principal":"*","Action":"sagemaker:DescribeModelPackage","Resource":"*"}]}`

	_, err = client.PutModelPackageGroupPolicy(t.Context(), &sagemakersdk.PutModelPackageGroupPolicyInput{
		ModelPackageGroupName: aws.String("slice4-mpg-policy"), ResourcePolicy: aws.String(policy),
	})
	require.NoError(t, err)

	got, err := client.GetModelPackageGroupPolicy(t.Context(), &sagemakersdk.GetModelPackageGroupPolicyInput{
		ModelPackageGroupName: aws.String("slice4-mpg-policy"),
	})
	require.NoError(t, err)
	assert.Equal(t, policy, aws.ToString(got.ResourcePolicy))

	_, err = client.DeleteModelPackageGroupPolicy(t.Context(), &sagemakersdk.DeleteModelPackageGroupPolicyInput{
		ModelPackageGroupName: aws.String("slice4-mpg-policy"),
	})
	require.NoError(t, err)

	_, err = client.GetModelPackageGroupPolicy(t.Context(), &sagemakersdk.GetModelPackageGroupPolicyInput{
		ModelPackageGroupName: aws.String("slice4-mpg-policy"),
	})
	require.Error(t, err)

	created, err := client.CreateModelPackage(t.Context(), &sagemakersdk.CreateModelPackageInput{
		ModelPackageName: aws.String("slice4-mp"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(created.ModelPackageArn))

	_, err = client.DeleteModelPackage(t.Context(), &sagemakersdk.DeleteModelPackageInput{
		ModelPackageName: aws.String("slice4-mp"),
	})
	require.NoError(t, err)
}

// testPipelinesExtraRealClient covers DeletePipeline,
// DescribePipelineDefinitionForExecution, StopPipelineExecution,
// UpdatePipelineExecution, ListPipelineVersions.
func testPipelinesExtraRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	pipelineDef := `{"Version":"2020-12-01","Steps":[]}`

	_, err := client.CreatePipeline(t.Context(), &sagemakersdk.CreatePipelineInput{
		PipelineName:       aws.String("slice4-pipeline"),
		PipelineDefinition: aws.String(pipelineDef),
		RoleArn:            aws.String("arn:aws:iam::000000000000:role/pipeline"),
	})
	require.NoError(t, err)

	exec, err := client.StartPipelineExecution(t.Context(), &sagemakersdk.StartPipelineExecutionInput{
		PipelineName: aws.String("slice4-pipeline"), ClientRequestToken: aws.String("token-1"),
	})
	require.NoError(t, err)

	defDesc, err := client.DescribePipelineDefinitionForExecution(
		t.Context(), &sagemakersdk.DescribePipelineDefinitionForExecutionInput{
			PipelineExecutionArn: exec.PipelineExecutionArn,
		})
	require.NoError(t, err)
	assert.Equal(t, pipelineDef, aws.ToString(defDesc.PipelineDefinition))

	_, err = client.UpdatePipelineExecution(t.Context(), &sagemakersdk.UpdatePipelineExecutionInput{
		PipelineExecutionArn:         exec.PipelineExecutionArn,
		PipelineExecutionDescription: aws.String("updated desc"),
	})
	require.NoError(t, err)

	execDesc, err := client.DescribePipelineExecution(t.Context(), &sagemakersdk.DescribePipelineExecutionInput{
		PipelineExecutionArn: exec.PipelineExecutionArn,
	})
	require.NoError(t, err)
	assert.Equal(t, "updated desc", aws.ToString(execDesc.PipelineExecutionDescription))

	_, err = client.StopPipelineExecution(t.Context(), &sagemakersdk.StopPipelineExecutionInput{
		PipelineExecutionArn: exec.PipelineExecutionArn, ClientRequestToken: aws.String("token-2"),
	})
	require.NoError(t, err)

	versions, err := client.ListPipelineVersions(t.Context(), &sagemakersdk.ListPipelineVersionsInput{
		PipelineName: aws.String("slice4-pipeline"),
	})
	require.NoError(t, err)
	_ = versions

	_, err = client.DeletePipeline(t.Context(), &sagemakersdk.DeletePipelineInput{
		PipelineName: aws.String("slice4-pipeline"), ClientRequestToken: aws.String("token-3"),
	})
	require.NoError(t, err)
}

// testExperimentsTrialsExtraRealClient covers DeleteExperiment, DeleteTrial,
// DeleteTrialComponent, UpdateTrial, AssociateTrialComponent,
// DisassociateTrialComponent.
func testExperimentsTrialsExtraRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	_, err := client.CreateExperiment(t.Context(), &sagemakersdk.CreateExperimentInput{
		ExperimentName: aws.String("slice4-exp"),
	})
	require.NoError(t, err)

	_, err = client.CreateTrial(t.Context(), &sagemakersdk.CreateTrialInput{
		TrialName: aws.String("slice4-trial"), ExperimentName: aws.String("slice4-exp"),
	})
	require.NoError(t, err)

	_, err = client.CreateTrialComponent(t.Context(), &sagemakersdk.CreateTrialComponentInput{
		TrialComponentName: aws.String("slice4-tc"),
	})
	require.NoError(t, err)

	_, err = client.AssociateTrialComponent(t.Context(), &sagemakersdk.AssociateTrialComponentInput{
		TrialComponentName: aws.String("slice4-tc"), TrialName: aws.String("slice4-trial"),
	})
	require.NoError(t, err)

	trialDesc, err := client.DescribeTrial(t.Context(), &sagemakersdk.DescribeTrialInput{
		TrialName: aws.String("slice4-trial"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice4-trial", aws.ToString(trialDesc.TrialName))

	_, err = client.UpdateTrial(t.Context(), &sagemakersdk.UpdateTrialInput{
		TrialName: aws.String("slice4-trial"), DisplayName: aws.String("Trial Display"),
	})
	require.NoError(t, err)

	trialDesc, err = client.DescribeTrial(t.Context(), &sagemakersdk.DescribeTrialInput{
		TrialName: aws.String("slice4-trial"),
	})
	require.NoError(t, err)
	assert.Equal(t, "Trial Display", aws.ToString(trialDesc.DisplayName))

	_, err = client.DisassociateTrialComponent(t.Context(), &sagemakersdk.DisassociateTrialComponentInput{
		TrialComponentName: aws.String("slice4-tc"), TrialName: aws.String("slice4-trial"),
	})
	require.NoError(t, err)

	_, err = client.DeleteTrialComponent(t.Context(), &sagemakersdk.DeleteTrialComponentInput{
		TrialComponentName: aws.String("slice4-tc"),
	})
	require.NoError(t, err)

	_, err = client.DeleteTrial(t.Context(), &sagemakersdk.DeleteTrialInput{TrialName: aws.String("slice4-trial")})
	require.NoError(t, err)

	_, err = client.DeleteExperiment(t.Context(), &sagemakersdk.DeleteExperimentInput{
		ExperimentName: aws.String("slice4-exp"),
	})
	require.NoError(t, err)
}

// testProjectsExtraRealClient covers DeleteProject, ListProjects,
// UpdateProject.
func testProjectsExtraRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	_, err := client.CreateProject(t.Context(), &sagemakersdk.CreateProjectInput{
		ProjectName: aws.String("slice4-project"),
	})
	require.NoError(t, err)

	list, err := client.ListProjects(t.Context(), &sagemakersdk.ListProjectsInput{})
	require.NoError(t, err)
	found := false

	for _, p := range list.ProjectSummaryList {
		if aws.ToString(p.ProjectName) == "slice4-project" {
			found = true
		}
	}

	assert.True(t, found)

	_, err = client.UpdateProject(t.Context(), &sagemakersdk.UpdateProjectInput{
		ProjectName: aws.String("slice4-project"), ProjectDescription: aws.String("updated"),
	})
	require.NoError(t, err)

	desc, err := client.DescribeProject(t.Context(), &sagemakersdk.DescribeProjectInput{
		ProjectName: aws.String("slice4-project"),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated", aws.ToString(desc.ProjectDescription))

	_, err = client.DeleteProject(t.Context(), &sagemakersdk.DeleteProjectInput{
		ProjectName: aws.String("slice4-project"),
	})
	require.NoError(t, err)
}

// testImagesExtraRealClient covers DeleteImage, UpdateImage.
func testImagesExtraRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	_, err := client.CreateImage(t.Context(), &sagemakersdk.CreateImageInput{
		ImageName: aws.String("slice4-image"), RoleArn: aws.String("arn:aws:iam::000000000000:role/image"),
	})
	require.NoError(t, err)

	_, err = client.UpdateImage(t.Context(), &sagemakersdk.UpdateImageInput{
		ImageName: aws.String("slice4-image"), DisplayName: aws.String("Slice4 Image"),
	})
	require.NoError(t, err)

	desc, err := client.DescribeImage(t.Context(), &sagemakersdk.DescribeImageInput{
		ImageName: aws.String("slice4-image"),
	})
	require.NoError(t, err)
	assert.Equal(t, "Slice4 Image", aws.ToString(desc.DisplayName))

	_, err = client.DeleteImage(t.Context(), &sagemakersdk.DeleteImageInput{ImageName: aws.String("slice4-image")})
	require.NoError(t, err)
}

// testHPTuningExtraRealClient covers StopHyperParameterTuningJob,
// DeleteHyperParameterTuningJob, ListTrainingJobsForHyperParameterTuningJob.
func testHPTuningExtraRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	_, err := client.CreateHyperParameterTuningJob(t.Context(), &sagemakersdk.CreateHyperParameterTuningJobInput{
		HyperParameterTuningJobName: aws.String("slice4-hpt"),
		HyperParameterTuningJobConfig: &smtypes.HyperParameterTuningJobConfig{
			Strategy: smtypes.HyperParameterTuningJobStrategyTypeBayesian,
			ResourceLimits: &smtypes.ResourceLimits{
				MaxNumberOfTrainingJobs: aws.Int32(1), MaxParallelTrainingJobs: aws.Int32(1),
			},
		},
		TrainingJobDefinition: &smtypes.HyperParameterTrainingJobDefinition{
			AlgorithmSpecification: &smtypes.HyperParameterAlgorithmSpecification{
				TrainingImage: aws.String("image:latest"), TrainingInputMode: smtypes.TrainingInputModeFile,
			},
			RoleArn: aws.String("arn:aws:iam::000000000000:role/hpt"),
			OutputDataConfig: &smtypes.OutputDataConfig{
				S3OutputPath: aws.String("s3://bucket/output"),
			},
			ResourceConfig: &smtypes.ResourceConfig{
				InstanceType: smtypes.TrainingInstanceTypeMlM5Large, InstanceCount: aws.Int32(1),
				VolumeSizeInGB: aws.Int32(10),
			},
			StoppingCondition: &smtypes.StoppingCondition{MaxRuntimeInSeconds: aws.Int32(3600)},
		},
	})
	require.NoError(t, err)

	trainingJobs, err := client.ListTrainingJobsForHyperParameterTuningJob(
		t.Context(), &sagemakersdk.ListTrainingJobsForHyperParameterTuningJobInput{
			HyperParameterTuningJobName: aws.String("slice4-hpt"),
		})
	require.NoError(t, err)
	assert.NotNil(t, trainingJobs.TrainingJobSummaries)

	_, err = client.StopHyperParameterTuningJob(t.Context(), &sagemakersdk.StopHyperParameterTuningJobInput{
		HyperParameterTuningJobName: aws.String("slice4-hpt"),
	})
	require.NoError(t, err)

	desc, err := client.DescribeHyperParameterTuningJob(
		t.Context(), &sagemakersdk.DescribeHyperParameterTuningJobInput{
			HyperParameterTuningJobName: aws.String("slice4-hpt"),
		})
	require.NoError(t, err)
	assert.Equal(t, smtypes.HyperParameterTuningJobStatusStopping, desc.HyperParameterTuningJobStatus)

	_, err = client.DeleteHyperParameterTuningJob(t.Context(), &sagemakersdk.DeleteHyperParameterTuningJobInput{
		HyperParameterTuningJobName: aws.String("slice4-hpt"),
	})
	require.NoError(t, err)
}

// testModelCardsExtraRealClient covers DeleteModelCard,
// ListModelCardVersions.
func testModelCardsExtraRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	_, err := client.CreateModelCard(t.Context(), &sagemakersdk.CreateModelCardInput{
		ModelCardName:   aws.String("slice4-card"),
		Content:         aws.String(`{"model_overview":{"model_description":"d"}}`),
		ModelCardStatus: smtypes.ModelCardStatusDraft,
	})
	require.NoError(t, err)

	versions, err := client.ListModelCardVersions(t.Context(), &sagemakersdk.ListModelCardVersionsInput{
		ModelCardName: aws.String("slice4-card"),
	})
	require.NoError(t, err)
	require.Len(t, versions.ModelCardVersionSummaryList, 1)

	_, err = client.DeleteModelCard(t.Context(), &sagemakersdk.DeleteModelCardInput{
		ModelCardName: aws.String("slice4-card"),
	})
	require.NoError(t, err)
}

// testClustersExtraRealClient covers DeleteCluster, DescribeClusterEvent,
// ListClusterEvents, StartClusterHealthCheck.
func testClustersExtraRealClient(t *testing.T) {
	t.Helper()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	cluster := h.Backend.AddClusterInternal(t.Context(), "slice4-cluster")

	_, err := client.StartClusterHealthCheck(t.Context(), &sagemakersdk.StartClusterHealthCheckInput{
		ClusterName: aws.String(cluster.ClusterName),
		DeepHealthCheckConfigurations: []smtypes.InstanceGroupHealthCheckConfiguration{{
			InstanceGroupName: aws.String("workers"),
			DeepHealthChecks:  []smtypes.DeepHealthCheckType{smtypes.DeepHealthCheckTypeInstanceStress},
		}},
	})
	require.NoError(t, err)

	events, err := client.ListClusterEvents(t.Context(), &sagemakersdk.ListClusterEventsInput{
		ClusterName: aws.String(cluster.ClusterName),
	})
	require.NoError(t, err)
	assert.Empty(t, events.Events)

	_, err = client.DescribeClusterEvent(t.Context(), &sagemakersdk.DescribeClusterEventInput{
		ClusterName: aws.String(cluster.ClusterName), EventId: aws.String("nonexistent"),
	})
	require.Error(t, err)

	_, err = client.DeleteCluster(t.Context(), &sagemakersdk.DeleteClusterInput{
		ClusterName: aws.String(cluster.ClusterName),
	})
	require.NoError(t, err)
}

// testFeatureGroupsExtraRealClient covers DeleteFeatureGroup,
// DescribeFeatureMetadata, UpdateFeatureMetadata.
func testFeatureGroupsExtraRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	_, err := client.CreateFeatureGroup(t.Context(), &sagemakersdk.CreateFeatureGroupInput{
		FeatureGroupName:            aws.String("slice4-fg"),
		RecordIdentifierFeatureName: aws.String("id"),
		EventTimeFeatureName:        aws.String("ts"),
		FeatureDefinitions: []smtypes.FeatureDefinition{
			{FeatureName: aws.String("id"), FeatureType: smtypes.FeatureTypeString},
			{FeatureName: aws.String("ts"), FeatureType: smtypes.FeatureTypeFractional},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateFeatureMetadata(t.Context(), &sagemakersdk.UpdateFeatureMetadataInput{
		FeatureGroupName: aws.String("slice4-fg"), FeatureName: aws.String("id"),
		Description: aws.String("identifier feature"),
	})
	require.NoError(t, err)

	meta, err := client.DescribeFeatureMetadata(t.Context(), &sagemakersdk.DescribeFeatureMetadataInput{
		FeatureGroupName: aws.String("slice4-fg"), FeatureName: aws.String("id"),
	})
	require.NoError(t, err)
	assert.Equal(t, "identifier feature", aws.ToString(meta.Description))

	_, err = client.DeleteFeatureGroup(t.Context(), &sagemakersdk.DeleteFeatureGroupInput{
		FeatureGroupName: aws.String("slice4-fg"),
	})
	require.NoError(t, err)
}

// testSearchSuggestionsRealClient covers GetSearchSuggestions.
func testSearchSuggestionsRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	out, err := client.GetSearchSuggestions(t.Context(), &sagemakersdk.GetSearchSuggestionsInput{
		Resource: smtypes.ResourceTypeTrainingJob,
	})
	require.NoError(t, err)
	assert.NotNil(t, out.PropertyNameSuggestions)
}
