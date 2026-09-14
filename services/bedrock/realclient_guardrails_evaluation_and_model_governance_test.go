package bedrock_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrocksdk "github.com/aws/aws-sdk-go-v2/service/bedrock"
	"github.com/aws/aws-sdk-go-v2/service/bedrock/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

// TestRealClient_GuardrailsEvaluationAndModelGovernance covers bedrock's highest-priority typed-
// client-uncovered op families (gopherstack-n3zi): guardrail
// versions, model invocation logging, account data retention, evaluation
// jobs, model import jobs, inference profiles, foundation model agreements,
// use-case-for-model-access, marketplace model endpoints, prompt routers,
// custom model deployments/customization/invocation job stops, advanced
// prompt optimization jobs, automated reasoning policies, and tags/resource
// policy. Each subtest creates real state through the typed aws-sdk-go-v2
// bedrock client and asserts decoded response values. The bedrock-agent
// (AgentsHandler) op family -- CreateAgent/Flow/Prompt/KnowledgeBase/
// DataSource and their Get/List/Update/Delete siblings -- is deliberately
// not attempted here: it is not among this slice's named priority families.
func TestRealClient_GuardrailsEvaluationAndModelGovernance(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testGuardrailVersionsRealClient, "guardrail_versions"},
		{testModelInvocationLoggingRealClient, "model_invocation_logging"},
		{testAccountDataRetentionRealClient, "account_data_retention"},
		{testEvaluationJobsExtraRealClient, "evaluation_jobs_extra"},
		{testModelImportJobsExtraRealClient, "model_import_jobs_extra"},
		{testInferenceProfilesRealClient, "inference_profiles"},
		{testFoundationModelAgreementsRealClient, "foundation_model_agreements"},
		{testUseCaseForModelAccessRealClient, "use_case_for_model_access"},
		{testMarketplaceModelEndpointsRealClient, "marketplace_model_endpoints"},
		{testPromptRoutersExtraRealClient, "prompt_routers_extra"},
		{testCustomModelDeploymentExtraRealClient, "custom_model_deployment_extra"},
		{testCustomModelAndJobStopsRealClient, "custom_model_and_job_stops"},
		{testAdvancedPromptOptimizationJobsRealClient, "advanced_prompt_optimization_jobs"},
		{testAutomatedReasoningPolicyExtraRealClient, "automated_reasoning_policy_extra"},
		{testTagsAndResourcePolicyRealClient, "tags_and_resource_policy"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

func newRealClient(t *testing.T) *bedrocksdk.Client {
	t.Helper()

	backend := bedrock.NewInMemoryBackend("123456789012", "us-east-1")
	h := bedrock.NewHandler(backend)

	return newTestBedrockClient(t, h)
}

// testGuardrailVersionsRealClient covers CreateGuardrailVersion and
// UpdateGuardrail.
func testGuardrailVersionsRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	created, err := client.CreateGuardrail(t.Context(), &bedrocksdk.CreateGuardrailInput{
		Name:                    aws.String("slice6-guardrail"),
		BlockedInputMessaging:   aws.String("blocked input"),
		BlockedOutputsMessaging: aws.String("blocked output"),
	})
	require.NoError(t, err)

	verOut, err := client.CreateGuardrailVersion(
		t.Context(),
		&bedrocksdk.CreateGuardrailVersionInput{
			GuardrailIdentifier: created.GuardrailId,
			Description:         aws.String("v1 snapshot"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.GuardrailId), aws.ToString(verOut.GuardrailId))
	assert.NotEmpty(t, aws.ToString(verOut.Version))

	updOut, err := client.UpdateGuardrail(t.Context(), &bedrocksdk.UpdateGuardrailInput{
		GuardrailIdentifier:     created.GuardrailId,
		Name:                    aws.String("slice6-guardrail"),
		BlockedInputMessaging:   aws.String("updated blocked input"),
		BlockedOutputsMessaging: aws.String("updated blocked output"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.GuardrailArn), aws.ToString(updOut.GuardrailArn))

	got, err := client.GetGuardrail(t.Context(), &bedrocksdk.GetGuardrailInput{
		GuardrailIdentifier: created.GuardrailId,
	})
	require.NoError(t, err)
	assert.Equal(t, "updated blocked input", aws.ToString(got.BlockedInputMessaging))
}

// testModelInvocationLoggingRealClient covers PutModelInvocationLoggingConfiguration,
// GetModelInvocationLoggingConfiguration, DeleteModelInvocationLoggingConfiguration.
func testModelInvocationLoggingRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	_, err := client.PutModelInvocationLoggingConfiguration(
		t.Context(),
		&bedrocksdk.PutModelInvocationLoggingConfigurationInput{
			LoggingConfig: &types.LoggingConfig{
				TextDataDeliveryEnabled: aws.Bool(true),
				S3Config: &types.S3Config{
					BucketName: aws.String("slice6-logging-bucket"),
				},
			},
		},
	)
	require.NoError(t, err)

	got, err := client.GetModelInvocationLoggingConfiguration(
		t.Context(), &bedrocksdk.GetModelInvocationLoggingConfigurationInput{},
	)
	require.NoError(t, err)
	require.NotNil(t, got.LoggingConfig)
	assert.True(t, aws.ToBool(got.LoggingConfig.TextDataDeliveryEnabled))
	require.NotNil(t, got.LoggingConfig.S3Config)
	assert.Equal(t, "slice6-logging-bucket", aws.ToString(got.LoggingConfig.S3Config.BucketName))

	_, err = client.DeleteModelInvocationLoggingConfiguration(
		t.Context(), &bedrocksdk.DeleteModelInvocationLoggingConfigurationInput{},
	)
	require.NoError(t, err)

	got, err = client.GetModelInvocationLoggingConfiguration(
		t.Context(), &bedrocksdk.GetModelInvocationLoggingConfigurationInput{},
	)
	require.NoError(t, err)
	assert.Nil(t, got.LoggingConfig)
}

// testAccountDataRetentionRealClient covers GetAccountDataRetention and
// PutAccountDataRetention.
func testAccountDataRetentionRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	got, err := client.GetAccountDataRetention(
		t.Context(),
		&bedrocksdk.GetAccountDataRetentionInput{},
	)
	require.NoError(t, err)
	assert.Equal(t, types.DataRetentionModeDefault, got.Mode)

	put, err := client.PutAccountDataRetention(
		t.Context(),
		&bedrocksdk.PutAccountDataRetentionInput{
			Mode: types.DataRetentionModeNone,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, types.DataRetentionModeNone, put.Mode)

	got, err = client.GetAccountDataRetention(
		t.Context(),
		&bedrocksdk.GetAccountDataRetentionInput{},
	)
	require.NoError(t, err)
	assert.Equal(t, types.DataRetentionModeNone, got.Mode)
}

// testEvaluationJobsExtraRealClient covers ListEvaluationJobs,
// StopEvaluationJob, BatchDeleteEvaluationJob.
func testEvaluationJobsExtraRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	newEvalJob := func(name string) *bedrocksdk.CreateEvaluationJobOutput {
		out, err := client.CreateEvaluationJob(t.Context(), &bedrocksdk.CreateEvaluationJobInput{
			JobName: aws.String(name),
			RoleArn: aws.String("arn:aws:iam::123456789012:role/eval-role"),
			OutputDataConfig: &types.EvaluationOutputDataConfig{
				S3Uri: aws.String("s3://bucket/eval-output"),
			},
			EvaluationConfig: &types.EvaluationConfigMemberAutomated{
				Value: types.AutomatedEvaluationConfig{
					DatasetMetricConfigs: []types.EvaluationDatasetMetricConfig{
						{
							TaskType:    types.EvaluationTaskTypeSummarization,
							MetricNames: []string{"Builtin.Accuracy"},
							Dataset:     &types.EvaluationDataset{Name: aws.String("squad-v2")},
						},
					},
				},
			},
			InferenceConfig: &types.EvaluationInferenceConfigMemberModels{
				Value: []types.EvaluationModelConfig{
					&types.EvaluationModelConfigMemberBedrockModel{
						Value: types.EvaluationBedrockModel{
							ModelIdentifier: aws.String("amazon.titan-text-express-v1"),
						},
					},
				},
			},
		})
		require.NoError(t, err)

		return out
	}

	job1 := newEvalJob("slice6-eval-1")
	job2 := newEvalJob("slice6-eval-2")

	list, err := client.ListEvaluationJobs(t.Context(), &bedrocksdk.ListEvaluationJobsInput{})
	require.NoError(t, err)

	names := make([]string, 0, len(list.JobSummaries))
	for _, s := range list.JobSummaries {
		names = append(names, aws.ToString(s.JobName))
	}

	assert.Contains(t, names, "slice6-eval-1")
	assert.Contains(t, names, "slice6-eval-2")

	_, err = client.StopEvaluationJob(t.Context(), &bedrocksdk.StopEvaluationJobInput{
		JobIdentifier: job1.JobArn,
	})
	require.NoError(t, err)

	got, err := client.GetEvaluationJob(t.Context(), &bedrocksdk.GetEvaluationJobInput{
		JobIdentifier: job1.JobArn,
	})
	require.NoError(t, err)
	assert.Equal(t, types.EvaluationJobStatusStopped, got.Status)

	// BatchDeleteEvaluationJob rejects a job still InProgress, so job2 must be
	// stopped first (evaluation_jobs.go's BatchDeleteEvaluationJob).
	_, err = client.StopEvaluationJob(t.Context(), &bedrocksdk.StopEvaluationJobInput{
		JobIdentifier: job2.JobArn,
	})
	require.NoError(t, err)

	batch, err := client.BatchDeleteEvaluationJob(
		t.Context(),
		&bedrocksdk.BatchDeleteEvaluationJobInput{
			JobIdentifiers: []string{aws.ToString(job2.JobArn)},
		},
	)
	require.NoError(t, err)
	require.Len(t, batch.EvaluationJobs, 1)
	assert.Equal(t, aws.ToString(job2.JobArn), aws.ToString(batch.EvaluationJobs[0].JobIdentifier))
}

// testModelImportJobsExtraRealClient covers GetModelImportJob,
// GetImportedModel, ListImportedModels, DeleteImportedModel.
func testModelImportJobsExtraRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	created, err := client.CreateModelImportJob(t.Context(), &bedrocksdk.CreateModelImportJobInput{
		JobName:           aws.String("slice6-import-job"),
		ImportedModelName: aws.String("slice6-imported-model"),
		RoleArn:           aws.String("arn:aws:iam::123456789012:role/import-role"),
		ModelDataSource: &types.ModelDataSourceMemberS3DataSource{
			Value: types.S3DataSource{S3Uri: aws.String("s3://bucket/model-weights/")},
		},
	})
	require.NoError(t, err)

	gotJob, err := client.GetModelImportJob(t.Context(), &bedrocksdk.GetModelImportJobInput{
		JobIdentifier: created.JobArn,
	})
	require.NoError(t, err)
	assert.Equal(t, "slice6-imported-model", aws.ToString(gotJob.ImportedModelName))
	require.NotNil(t, gotJob.ImportedModelArn)

	gotModel, err := client.GetImportedModel(t.Context(), &bedrocksdk.GetImportedModelInput{
		ModelIdentifier: gotJob.ImportedModelArn,
	})
	require.NoError(t, err)
	assert.Equal(t, "slice6-imported-model", aws.ToString(gotModel.ModelName))

	list, err := client.ListImportedModels(t.Context(), &bedrocksdk.ListImportedModelsInput{})
	require.NoError(t, err)

	found := false

	for _, m := range list.ModelSummaries {
		if aws.ToString(m.ModelArn) == aws.ToString(gotJob.ImportedModelArn) {
			found = true
		}
	}

	assert.True(t, found)

	_, err = client.DeleteImportedModel(t.Context(), &bedrocksdk.DeleteImportedModelInput{
		ModelIdentifier: gotJob.ImportedModelArn,
	})
	require.NoError(t, err)

	_, err = client.GetImportedModel(t.Context(), &bedrocksdk.GetImportedModelInput{
		ModelIdentifier: gotJob.ImportedModelArn,
	})
	require.Error(t, err)
}

// testInferenceProfilesRealClient covers GetInferenceProfile,
// ListInferenceProfiles, DeleteInferenceProfile.
func testInferenceProfilesRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	created, err := client.CreateInferenceProfile(
		t.Context(),
		&bedrocksdk.CreateInferenceProfileInput{
			InferenceProfileName: aws.String("slice6-inference-profile"),
			ModelSource: &types.InferenceProfileModelSourceMemberCopyFrom{
				Value: "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-v2",
			},
		},
	)
	require.NoError(t, err)

	got, err := client.GetInferenceProfile(t.Context(), &bedrocksdk.GetInferenceProfileInput{
		InferenceProfileIdentifier: created.InferenceProfileArn,
	})
	require.NoError(t, err)
	assert.Equal(t, "slice6-inference-profile", aws.ToString(got.InferenceProfileName))

	list, err := client.ListInferenceProfiles(t.Context(), &bedrocksdk.ListInferenceProfilesInput{})
	require.NoError(t, err)

	found := false

	for _, p := range list.InferenceProfileSummaries {
		if aws.ToString(p.InferenceProfileArn) == aws.ToString(created.InferenceProfileArn) {
			found = true
		}
	}

	assert.True(t, found)

	_, err = client.DeleteInferenceProfile(t.Context(), &bedrocksdk.DeleteInferenceProfileInput{
		InferenceProfileIdentifier: created.InferenceProfileArn,
	})
	require.NoError(t, err)

	_, err = client.GetInferenceProfile(t.Context(), &bedrocksdk.GetInferenceProfileInput{
		InferenceProfileIdentifier: created.InferenceProfileArn,
	})
	require.Error(t, err)
}

// testFoundationModelAgreementsRealClient covers CreateFoundationModelAgreement,
// ListFoundationModelAgreementOffers, GetFoundationModelAvailability,
// DeleteFoundationModelAgreement.
func testFoundationModelAgreementsRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	modelID := "anthropic.claude-v2"

	offers, err := client.ListFoundationModelAgreementOffers(
		t.Context(),
		&bedrocksdk.ListFoundationModelAgreementOffersInput{ModelId: aws.String(modelID)},
	)
	require.NoError(t, err)
	require.NotEmpty(t, offers.Offers)

	created, err := client.CreateFoundationModelAgreement(
		t.Context(),
		&bedrocksdk.CreateFoundationModelAgreementInput{
			ModelId:    aws.String(modelID),
			OfferToken: offers.Offers[0].OfferToken,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, modelID, aws.ToString(created.ModelId))

	avail, err := client.GetFoundationModelAvailability(
		t.Context(), &bedrocksdk.GetFoundationModelAvailabilityInput{ModelId: aws.String(modelID)},
	)
	require.NoError(t, err)
	assert.Equal(t, modelID, aws.ToString(avail.ModelId))
	require.NotNil(t, avail.AgreementAvailability)

	_, err = client.DeleteFoundationModelAgreement(
		t.Context(), &bedrocksdk.DeleteFoundationModelAgreementInput{ModelId: aws.String(modelID)},
	)
	require.NoError(t, err)
}

// testUseCaseForModelAccessRealClient covers PutUseCaseForModelAccess and
// GetUseCaseForModelAccess.
func testUseCaseForModelAccessRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	formData := []byte(`{"useCaseType":"COMMERCIAL","useCaseDescription":"slice6"}`)

	_, err := client.PutUseCaseForModelAccess(
		t.Context(),
		&bedrocksdk.PutUseCaseForModelAccessInput{
			FormData: formData,
		},
	)
	require.NoError(t, err)

	got, err := client.GetUseCaseForModelAccess(
		t.Context(),
		&bedrocksdk.GetUseCaseForModelAccessInput{},
	)
	require.NoError(t, err)
	assert.Equal(t, formData, got.FormData)
}

// testMarketplaceModelEndpointsRealClient covers RegisterMarketplaceModelEndpoint,
// GetMarketplaceModelEndpoint, ListMarketplaceModelEndpoints,
// UpdateMarketplaceModelEndpoint, DeregisterMarketplaceModelEndpoint,
// DeleteMarketplaceModelEndpoint.
func testMarketplaceModelEndpointsRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	created, err := client.CreateMarketplaceModelEndpoint(
		t.Context(),
		&bedrocksdk.CreateMarketplaceModelEndpointInput{
			EndpointName: aws.String("slice6-mme"),
			ModelSourceIdentifier: aws.String(
				"arn:aws:sagemaker:us-east-1:123456789012:hub-content/SageMakerPublicHub/Model/foo/1.0.0",
			),
			EndpointConfig: &types.EndpointConfigMemberSageMaker{
				Value: types.SageMakerEndpoint{
					InitialInstanceCount: aws.Int32(1),
					InstanceType:         aws.String("ml.m5.large"),
					ExecutionRole: aws.String(
						"arn:aws:iam::123456789012:role/sagemaker-exec",
					),
				},
			},
		},
	)
	require.NoError(t, err)

	registered, err := client.RegisterMarketplaceModelEndpoint(
		t.Context(),
		&bedrocksdk.RegisterMarketplaceModelEndpointInput{
			EndpointIdentifier: created.MarketplaceModelEndpoint.EndpointArn,
			ModelSourceIdentifier: aws.String(
				"arn:aws:sagemaker:us-east-1:123456789012:hub-content/registered-source",
			),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "Active", aws.ToString(registered.MarketplaceModelEndpoint.EndpointStatus))

	got, err := client.GetMarketplaceModelEndpoint(
		t.Context(),
		&bedrocksdk.GetMarketplaceModelEndpointInput{
			EndpointArn: created.MarketplaceModelEndpoint.EndpointArn,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:sagemaker:us-east-1:123456789012:hub-content/registered-source",
		aws.ToString(got.MarketplaceModelEndpoint.ModelSourceIdentifier))

	list, err := client.ListMarketplaceModelEndpoints(
		t.Context(), &bedrocksdk.ListMarketplaceModelEndpointsInput{},
	)
	require.NoError(t, err)

	found := false

	for _, e := range list.MarketplaceModelEndpoints {
		if aws.ToString(
			e.EndpointArn,
		) == aws.ToString(
			created.MarketplaceModelEndpoint.EndpointArn,
		) {
			found = true
		}
	}

	assert.True(t, found)

	updated, err := client.UpdateMarketplaceModelEndpoint(
		t.Context(),
		&bedrocksdk.UpdateMarketplaceModelEndpointInput{
			EndpointArn: created.MarketplaceModelEndpoint.EndpointArn,
			EndpointConfig: &types.EndpointConfigMemberSageMaker{
				Value: types.SageMakerEndpoint{
					InitialInstanceCount: aws.Int32(2),
					InstanceType:         aws.String("ml.m5.xlarge"),
					ExecutionRole: aws.String(
						"arn:aws:iam::123456789012:role/sagemaker-exec",
					),
				},
			},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, updated.MarketplaceModelEndpoint.EndpointConfig)

	sm, ok := updated.MarketplaceModelEndpoint.EndpointConfig.(*types.EndpointConfigMemberSageMaker)
	require.True(t, ok)
	assert.Equal(t, int32(2), aws.ToInt32(sm.Value.InitialInstanceCount))

	_, err = client.DeregisterMarketplaceModelEndpoint(
		t.Context(),
		&bedrocksdk.DeregisterMarketplaceModelEndpointInput{
			EndpointArn: created.MarketplaceModelEndpoint.EndpointArn,
		},
	)
	require.NoError(t, err)

	_, err = client.DeleteMarketplaceModelEndpoint(
		t.Context(),
		&bedrocksdk.DeleteMarketplaceModelEndpointInput{
			EndpointArn: created.MarketplaceModelEndpoint.EndpointArn,
		},
	)
	require.NoError(t, err)
}

// testPromptRoutersExtraRealClient covers ListPromptRouters, GetPromptRouter,
// DeletePromptRouter.
func testPromptRoutersExtraRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	created, err := client.CreatePromptRouter(t.Context(), &bedrocksdk.CreatePromptRouterInput{
		PromptRouterName: aws.String("slice6-router"),
		Models: []types.PromptRouterTargetModel{
			{
				ModelArn: aws.String(
					"arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-v2",
				),
			},
		},
		FallbackModel: &types.PromptRouterTargetModel{
			ModelArn: aws.String(
				"arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-instant-v1",
			),
		},
		RoutingCriteria: &types.RoutingCriteria{ResponseQualityDifference: aws.Float64(0.2)},
	})
	require.NoError(t, err)

	list, err := client.ListPromptRouters(t.Context(), &bedrocksdk.ListPromptRoutersInput{})
	require.NoError(t, err)

	found := false

	for _, r := range list.PromptRouterSummaries {
		if aws.ToString(r.PromptRouterArn) == aws.ToString(created.PromptRouterArn) {
			found = true
		}
	}

	assert.True(t, found)

	got, err := client.GetPromptRouter(t.Context(), &bedrocksdk.GetPromptRouterInput{
		PromptRouterArn: created.PromptRouterArn,
	})
	require.NoError(t, err)
	assert.Equal(t, "slice6-router", aws.ToString(got.PromptRouterName))

	_, err = client.DeletePromptRouter(t.Context(), &bedrocksdk.DeletePromptRouterInput{
		PromptRouterArn: created.PromptRouterArn,
	})
	require.NoError(t, err)

	_, err = client.GetPromptRouter(t.Context(), &bedrocksdk.GetPromptRouterInput{
		PromptRouterArn: created.PromptRouterArn,
	})
	require.Error(t, err)
}

// testCustomModelDeploymentExtraRealClient covers UpdateCustomModelDeployment,
// GetCustomModelDeployment, DeleteCustomModelDeployment. UpdateCustomModelDeployment
// previously dropped its required ModelArn entirely (handler never read the
// request body) -- fixed in handler_custom_model_deployments.go /
// custom_model_deployments.go; this test pins that fix.
func testCustomModelDeploymentExtraRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	created, err := client.CreateCustomModelDeployment(
		t.Context(),
		&bedrocksdk.CreateCustomModelDeploymentInput{
			ModelArn: aws.String(
				"arn:aws:bedrock:us-east-1:123456789012:custom-model/original",
			),
			ModelDeploymentName: aws.String("slice6-deployment"),
		},
	)
	require.NoError(t, err)

	updated, err := client.UpdateCustomModelDeployment(
		t.Context(),
		&bedrocksdk.UpdateCustomModelDeploymentInput{
			CustomModelDeploymentIdentifier: created.CustomModelDeploymentArn,
			ModelArn: aws.String(
				"arn:aws:bedrock:us-east-1:123456789012:custom-model/replacement",
			),
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		aws.ToString(created.CustomModelDeploymentArn),
		aws.ToString(updated.CustomModelDeploymentArn),
	)

	got, err := client.GetCustomModelDeployment(
		t.Context(),
		&bedrocksdk.GetCustomModelDeploymentInput{
			CustomModelDeploymentIdentifier: created.CustomModelDeploymentArn,
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		"arn:aws:bedrock:us-east-1:123456789012:custom-model/replacement",
		aws.ToString(got.ModelArn),
	)

	_, err = client.DeleteCustomModelDeployment(
		t.Context(),
		&bedrocksdk.DeleteCustomModelDeploymentInput{
			CustomModelDeploymentIdentifier: created.CustomModelDeploymentArn,
		},
	)
	require.NoError(t, err)
}

// testCustomModelAndJobStopsRealClient covers DeleteCustomModel,
// StopModelCustomizationJob, StopModelInvocationJob.
func testCustomModelAndJobStopsRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	model, err := client.CreateCustomModel(t.Context(), &bedrocksdk.CreateCustomModelInput{
		ModelName: aws.String("slice6-custom-model"),
		ModelSourceConfig: &types.ModelDataSourceMemberS3DataSource{
			Value: types.S3DataSource{S3Uri: aws.String("s3://bucket/weights/")},
		},
	})
	require.NoError(t, err)

	_, err = client.DeleteCustomModel(t.Context(), &bedrocksdk.DeleteCustomModelInput{
		ModelIdentifier: model.ModelArn,
	})
	require.NoError(t, err)

	_, err = client.GetCustomModel(t.Context(), &bedrocksdk.GetCustomModelInput{
		ModelIdentifier: model.ModelArn,
	})
	require.Error(t, err)

	custJob, err := client.CreateModelCustomizationJob(
		t.Context(),
		&bedrocksdk.CreateModelCustomizationJobInput{
			JobName:             aws.String("slice6-customization-job"),
			CustomModelName:     aws.String("slice6-customization-output-model"),
			BaseModelIdentifier: aws.String("anthropic.claude-v2"),
			RoleArn:             aws.String("arn:aws:iam::123456789012:role/customize"),
			OutputDataConfig: &types.OutputDataConfig{
				S3Uri: aws.String("s3://bucket/output"),
			},
			TrainingDataConfig: &types.TrainingDataConfig{
				S3Uri: aws.String("s3://bucket/training"),
			},
		},
	)
	require.NoError(t, err)

	_, err = client.StopModelCustomizationJob(
		t.Context(),
		&bedrocksdk.StopModelCustomizationJobInput{
			JobIdentifier: custJob.JobArn,
		},
	)
	require.NoError(t, err)

	gotCustJob, err := client.GetModelCustomizationJob(
		t.Context(), &bedrocksdk.GetModelCustomizationJobInput{JobIdentifier: custJob.JobArn},
	)
	require.NoError(t, err)
	assert.Equal(t, types.ModelCustomizationJobStatusStopped, gotCustJob.Status)

	invocJob, err := client.CreateModelInvocationJob(
		t.Context(),
		&bedrocksdk.CreateModelInvocationJobInput{
			JobName: aws.String("slice6-invocation-job"),
			ModelId: aws.String("anthropic.claude-v2"),
			RoleArn: aws.String("arn:aws:iam::123456789012:role/batch-role"),
			InputDataConfig: &types.ModelInvocationJobInputDataConfigMemberS3InputDataConfig{
				Value: types.ModelInvocationJobS3InputDataConfig{
					S3Uri: aws.String("s3://bucket/input.jsonl"),
				},
			},
			OutputDataConfig: &types.ModelInvocationJobOutputDataConfigMemberS3OutputDataConfig{
				Value: types.ModelInvocationJobS3OutputDataConfig{
					S3Uri: aws.String("s3://bucket/output/"),
				},
			},
		},
	)
	require.NoError(t, err)

	_, err = client.StopModelInvocationJob(t.Context(), &bedrocksdk.StopModelInvocationJobInput{
		JobIdentifier: invocJob.JobArn,
	})
	require.NoError(t, err)

	gotInvocJob, err := client.GetModelInvocationJob(
		t.Context(), &bedrocksdk.GetModelInvocationJobInput{JobIdentifier: invocJob.JobArn},
	)
	require.NoError(t, err)
	assert.Equal(t, types.ModelInvocationJobStatusStopped, gotInvocJob.Status)
}

// testAdvancedPromptOptimizationJobsRealClient covers
// CreateAdvancedPromptOptimizationJob, GetAdvancedPromptOptimizationJob,
// ListAdvancedPromptOptimizationJobs, StopAdvancedPromptOptimizationJob,
// BatchDeleteAdvancedPromptOptimizationJob.
func testAdvancedPromptOptimizationJobsRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	newJob := func(name string) *bedrocksdk.CreateAdvancedPromptOptimizationJobOutput {
		out, err := client.CreateAdvancedPromptOptimizationJob(
			t.Context(),
			&bedrocksdk.CreateAdvancedPromptOptimizationJobInput{
				JobName: aws.String(name),
				InputConfig: &types.AdvancedPromptOptimizationInputConfig{
					S3Uri: aws.String("s3://bucket/apo-input.jsonl"),
				},
				OutputConfig: &types.AdvancedPromptOptimizationOutputConfig{
					S3Uri: aws.String("s3://bucket/apo-output/"),
				},
				ModelConfigurations: []types.ModelConfiguration{
					{ModelId: aws.String("anthropic.claude-v2")},
				},
			},
		)
		require.NoError(t, err)

		return out
	}

	job1 := newJob("slice6-apo-1")
	job2 := newJob("slice6-apo-2")

	got, err := client.GetAdvancedPromptOptimizationJob(
		t.Context(), &bedrocksdk.GetAdvancedPromptOptimizationJobInput{JobIdentifier: job1.JobArn},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice6-apo-1", aws.ToString(got.JobName))

	list, err := client.ListAdvancedPromptOptimizationJobs(
		t.Context(), &bedrocksdk.ListAdvancedPromptOptimizationJobsInput{},
	)
	require.NoError(t, err)

	names := make([]string, 0, len(list.JobSummaries))
	for _, s := range list.JobSummaries {
		names = append(names, aws.ToString(s.JobName))
	}

	assert.Contains(t, names, "slice6-apo-1")
	assert.Contains(t, names, "slice6-apo-2")

	_, err = client.StopAdvancedPromptOptimizationJob(
		t.Context(), &bedrocksdk.StopAdvancedPromptOptimizationJobInput{JobIdentifier: job1.JobArn},
	)
	require.NoError(t, err)

	batch, err := client.BatchDeleteAdvancedPromptOptimizationJob(
		t.Context(),
		&bedrocksdk.BatchDeleteAdvancedPromptOptimizationJobInput{
			JobIdentifiers: []string{aws.ToString(job2.JobArn)},
		},
	)
	require.NoError(t, err)
	require.Len(t, batch.AdvancedPromptOptimizationJobs, 1)
}

// testAutomatedReasoningPolicyExtraRealClient covers
// CancelAutomatedReasoningPolicyBuildWorkflow, ListAutomatedReasoningPolicyTestCases,
// UpdateAutomatedReasoningPolicyTestCase, DeleteAutomatedReasoningPolicyTestCase,
// GetAutomatedReasoningPolicyNextScenario, StartAutomatedReasoningPolicyTestWorkflow,
// ListAutomatedReasoningPolicyTestResults, ExportAutomatedReasoningPolicyVersion,
// DeleteAutomatedReasoningPolicyBuildWorkflow, DeleteAutomatedReasoningPolicy.
func testAutomatedReasoningPolicyExtraRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	policy, err := client.CreateAutomatedReasoningPolicy(
		t.Context(),
		&bedrocksdk.CreateAutomatedReasoningPolicyInput{Name: aws.String("slice6-arp")},
	)
	require.NoError(t, err)

	wf, err := client.StartAutomatedReasoningPolicyBuildWorkflow(
		t.Context(),
		&bedrocksdk.StartAutomatedReasoningPolicyBuildWorkflowInput{
			PolicyArn:         policy.PolicyArn,
			BuildWorkflowType: types.AutomatedReasoningPolicyBuildWorkflowTypeIngestContent,
			SourceContent: &types.AutomatedReasoningPolicyBuildWorkflowSource{
				PolicyDefinition: &types.AutomatedReasoningPolicyDefinition{
					Version: aws.String("1"),
				},
			},
		},
	)
	require.NoError(t, err)

	tc, err := client.CreateAutomatedReasoningPolicyTestCase(
		t.Context(),
		&bedrocksdk.CreateAutomatedReasoningPolicyTestCaseInput{
			PolicyArn:                        policy.PolicyArn,
			GuardContent:                     aws.String("all cats are mammals"),
			ExpectedAggregatedFindingsResult: types.AutomatedReasoningCheckResultValid,
		},
	)
	require.NoError(t, err)

	listTC, err := client.ListAutomatedReasoningPolicyTestCases(
		t.Context(),
		&bedrocksdk.ListAutomatedReasoningPolicyTestCasesInput{PolicyArn: policy.PolicyArn},
	)
	require.NoError(t, err)
	require.Len(t, listTC.TestCases, 1)
	assert.Equal(t, aws.ToString(tc.TestCaseId), aws.ToString(listTC.TestCases[0].TestCaseId))

	updTC, err := client.UpdateAutomatedReasoningPolicyTestCase(
		t.Context(),
		&bedrocksdk.UpdateAutomatedReasoningPolicyTestCaseInput{
			PolicyArn:                        policy.PolicyArn,
			TestCaseId:                       tc.TestCaseId,
			GuardContent:                     aws.String("all dogs are mammals"),
			ExpectedAggregatedFindingsResult: types.AutomatedReasoningCheckResultValid,
			LastUpdatedAt:                    listTC.TestCases[0].UpdatedAt,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(tc.TestCaseId), aws.ToString(updTC.TestCaseId))

	scenario, err := client.GetAutomatedReasoningPolicyNextScenario(
		t.Context(),
		&bedrocksdk.GetAutomatedReasoningPolicyNextScenarioInput{
			PolicyArn:       policy.PolicyArn,
			BuildWorkflowId: wf.BuildWorkflowId,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(policy.PolicyArn), aws.ToString(scenario.PolicyArn))

	startTest, err := client.StartAutomatedReasoningPolicyTestWorkflow(
		t.Context(),
		&bedrocksdk.StartAutomatedReasoningPolicyTestWorkflowInput{
			PolicyArn:       policy.PolicyArn,
			BuildWorkflowId: wf.BuildWorkflowId,
			TestCaseIds:     []string{aws.ToString(tc.TestCaseId)},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(policy.PolicyArn), aws.ToString(startTest.PolicyArn))

	results, err := client.ListAutomatedReasoningPolicyTestResults(
		t.Context(),
		&bedrocksdk.ListAutomatedReasoningPolicyTestResultsInput{
			PolicyArn:       policy.PolicyArn,
			BuildWorkflowId: wf.BuildWorkflowId,
		},
	)
	require.NoError(t, err)
	require.Len(t, results.TestResults, 1)

	version, err := client.CreateAutomatedReasoningPolicyVersion(
		t.Context(),
		&bedrocksdk.CreateAutomatedReasoningPolicyVersionInput{
			PolicyArn:                 policy.PolicyArn,
			LastUpdatedDefinitionHash: policy.DefinitionHash,
		},
	)
	require.NoError(t, err)

	exported, err := client.ExportAutomatedReasoningPolicyVersion(
		t.Context(),
		&bedrocksdk.ExportAutomatedReasoningPolicyVersionInput{PolicyArn: version.PolicyArn},
	)
	require.NoError(t, err)
	require.NotNil(t, exported.PolicyDefinition)

	_, err = client.CancelAutomatedReasoningPolicyBuildWorkflow(
		t.Context(),
		&bedrocksdk.CancelAutomatedReasoningPolicyBuildWorkflowInput{
			PolicyArn:       policy.PolicyArn,
			BuildWorkflowId: wf.BuildWorkflowId,
		},
	)
	require.NoError(t, err)

	gotWF, err := client.GetAutomatedReasoningPolicyBuildWorkflow(
		t.Context(),
		&bedrocksdk.GetAutomatedReasoningPolicyBuildWorkflowInput{
			PolicyArn:       policy.PolicyArn,
			BuildWorkflowId: wf.BuildWorkflowId,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, types.AutomatedReasoningPolicyBuildWorkflowStatusCancelled, gotWF.Status)

	freshTC, err := client.GetAutomatedReasoningPolicyTestCase(
		t.Context(),
		&bedrocksdk.GetAutomatedReasoningPolicyTestCaseInput{
			PolicyArn:  policy.PolicyArn,
			TestCaseId: tc.TestCaseId,
		},
	)
	require.NoError(t, err)

	_, err = client.DeleteAutomatedReasoningPolicyTestCase(
		t.Context(),
		&bedrocksdk.DeleteAutomatedReasoningPolicyTestCaseInput{
			PolicyArn:     policy.PolicyArn,
			TestCaseId:    tc.TestCaseId,
			LastUpdatedAt: freshTC.TestCase.UpdatedAt,
		},
	)
	require.NoError(t, err)

	_, err = client.DeleteAutomatedReasoningPolicyBuildWorkflow(
		t.Context(),
		&bedrocksdk.DeleteAutomatedReasoningPolicyBuildWorkflowInput{
			PolicyArn:       policy.PolicyArn,
			BuildWorkflowId: wf.BuildWorkflowId,
			LastUpdatedAt:   gotWF.UpdatedAt,
		},
	)
	require.NoError(t, err)

	_, err = client.DeleteAutomatedReasoningPolicy(
		t.Context(),
		&bedrocksdk.DeleteAutomatedReasoningPolicyInput{
			PolicyArn: policy.PolicyArn,
			Force:     true,
		},
	)
	require.NoError(t, err)
}

// testTagsAndResourcePolicyRealClient covers TagResource, UntagResource,
// GetResourcePolicy, DeleteResourcePolicy.
func testTagsAndResourcePolicyRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	guardrail, err := client.CreateGuardrail(t.Context(), &bedrocksdk.CreateGuardrailInput{
		Name:                    aws.String("slice6-tag-guardrail"),
		BlockedInputMessaging:   aws.String("blocked"),
		BlockedOutputsMessaging: aws.String("blocked"),
	})
	require.NoError(t, err)

	_, err = client.TagResource(t.Context(), &bedrocksdk.TagResourceInput{
		ResourceARN: guardrail.GuardrailArn,
		Tags:        []types.Tag{{Key: aws.String("env"), Value: aws.String("slice6")}},
	})
	require.NoError(t, err)

	tags, err := client.ListTagsForResource(t.Context(), &bedrocksdk.ListTagsForResourceInput{
		ResourceARN: guardrail.GuardrailArn,
	})
	require.NoError(t, err)
	require.Len(t, tags.Tags, 1)
	assert.Equal(t, "env", aws.ToString(tags.Tags[0].Key))

	_, err = client.UntagResource(t.Context(), &bedrocksdk.UntagResourceInput{
		ResourceARN: guardrail.GuardrailArn,
		TagKeys:     []string{"env"},
	})
	require.NoError(t, err)

	tags, err = client.ListTagsForResource(t.Context(), &bedrocksdk.ListTagsForResourceInput{
		ResourceARN: guardrail.GuardrailArn,
	})
	require.NoError(t, err)
	assert.Empty(t, tags.Tags)

	_, err = client.PutResourcePolicy(t.Context(), &bedrocksdk.PutResourcePolicyInput{
		ResourceArn:    guardrail.GuardrailArn,
		ResourcePolicy: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
	})
	require.NoError(t, err)

	policy, err := client.GetResourcePolicy(t.Context(), &bedrocksdk.GetResourcePolicyInput{
		ResourceArn: guardrail.GuardrailArn,
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(policy.ResourcePolicy), "2012-10-17")

	_, err = client.DeleteResourcePolicy(t.Context(), &bedrocksdk.DeleteResourcePolicyInput{
		ResourceArn: guardrail.GuardrailArn,
	})
	require.NoError(t, err)

	_, err = client.GetResourcePolicy(t.Context(), &bedrocksdk.GetResourcePolicyInput{
		ResourceArn: guardrail.GuardrailArn,
	})
	require.Error(t, err)
}
