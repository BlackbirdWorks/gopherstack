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

func newDroppedMembersClient(t *testing.T) (*bedrock.InMemoryBackend, *bedrocksdk.Client) {
	t.Helper()

	b := bedrock.NewInMemoryBackend("123456789012", "us-east-1")

	return b, newTestBedrockClient(t, bedrock.NewHandler(b))
}

func TestSDK_GuardrailKmsCrossRegionReasoningAndToken(t *testing.T) {
	t.Parallel()

	_, client := newDroppedMembersClient(t)
	ctx := t.Context()

	in := &bedrocksdk.CreateGuardrailInput{
		Name:                    aws.String("gr-members"),
		BlockedInputMessaging:   aws.String("blocked in"),
		BlockedOutputsMessaging: aws.String("blocked out"),
		KmsKeyId:                aws.String("1234abcd-12ab-34cd-56ef-1234567890ab"),
		ClientRequestToken:      aws.String("gr-token"),
		CrossRegionConfig: &types.GuardrailCrossRegionConfig{
			GuardrailProfileIdentifier: aws.String("us.guardrail.v1:0"),
		},
		AutomatedReasoningPolicyConfig: &types.GuardrailAutomatedReasoningPolicyConfig{
			Policies:            []string{"arn:aws:bedrock:us-east-1:123456789012:automated-reasoning-policy/p1"},
			ConfidenceThreshold: aws.Float64(0.8),
		},
	}

	created, err := client.CreateGuardrail(ctx, in)
	require.NoError(t, err)
	retry, err := client.CreateGuardrail(ctx, in)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.GuardrailId), aws.ToString(retry.GuardrailId))

	conflict := *in
	conflict.Description = aws.String("changed")
	_, err = client.CreateGuardrail(ctx, &conflict)

	var typed *types.ConflictException

	require.ErrorAs(t, err, &typed)

	got, err := client.GetGuardrail(ctx, &bedrocksdk.GetGuardrailInput{GuardrailIdentifier: created.GuardrailId})
	require.NoError(t, err)
	assert.Equal(
		t, "arn:aws:kms:us-east-1:123456789012:key/1234abcd-12ab-34cd-56ef-1234567890ab", aws.ToString(got.KmsKeyArn),
	)
	require.NotNil(t, got.CrossRegionDetails)
	assert.Equal(t, "us.guardrail.v1:0", aws.ToString(got.CrossRegionDetails.GuardrailProfileId))
	assert.Contains(t, aws.ToString(got.CrossRegionDetails.GuardrailProfileArn), "guardrail-profile/us.guardrail.v1:0")
	require.NotNil(t, got.AutomatedReasoningPolicy)
	assert.InDelta(t, 0.8, aws.ToFloat64(got.AutomatedReasoningPolicy.ConfidenceThreshold), 0.0001)

	list, err := client.ListGuardrails(ctx, &bedrocksdk.ListGuardrailsInput{})
	require.NoError(t, err)
	require.Len(t, list.Guardrails, 1)
	require.NotNil(t, list.Guardrails[0].CrossRegionDetails)
	assert.Equal(t, "us.guardrail.v1:0", aws.ToString(list.Guardrails[0].CrossRegionDetails.GuardrailProfileId))

	_, err = client.UpdateGuardrail(ctx, &bedrocksdk.UpdateGuardrailInput{
		GuardrailIdentifier:     created.GuardrailId,
		Name:                    aws.String("gr-members"),
		BlockedInputMessaging:   aws.String("blocked in"),
		BlockedOutputsMessaging: aws.String("blocked out"),
		KmsKeyId:                aws.String("alias/gr-key"),
	})
	require.NoError(t, err)

	version, err := client.CreateGuardrailVersion(ctx, &bedrocksdk.CreateGuardrailVersionInput{
		GuardrailIdentifier: created.GuardrailId, ClientRequestToken: aws.String("ver-token"),
	})
	require.NoError(t, err)
	versionRetry, err := client.CreateGuardrailVersion(ctx, &bedrocksdk.CreateGuardrailVersionInput{
		GuardrailIdentifier: created.GuardrailId, ClientRequestToken: aws.String("ver-token"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(version.Version), aws.ToString(versionRetry.Version))

	pinned, err := client.GetGuardrail(ctx, &bedrocksdk.GetGuardrailInput{
		GuardrailIdentifier: created.GuardrailId, GuardrailVersion: version.Version,
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:alias/gr-key", aws.ToString(pinned.KmsKeyArn))
}

func TestSDK_CreateTokensReplayAcrossResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, client *bedrocksdk.Client) (first, retry string)
		name string
	}{
		{name: "inference_profile", run: func(t *testing.T, client *bedrocksdk.Client) (string, string) {
			t.Helper()

			in := &bedrocksdk.CreateInferenceProfileInput{
				InferenceProfileName: aws.String("ip-token"),
				ClientRequestToken:   aws.String("ip-token-1"),
				ModelSource: &types.InferenceProfileModelSourceMemberCopyFrom{
					Value: "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-text-express-v1",
				},
			}
			a, err := client.CreateInferenceProfile(t.Context(), in)
			require.NoError(t, err)
			b, err := client.CreateInferenceProfile(t.Context(), in)
			require.NoError(t, err)

			return aws.ToString(a.InferenceProfileArn), aws.ToString(b.InferenceProfileArn)
		}},
		{name: "provisioned_throughput", run: func(t *testing.T, client *bedrocksdk.Client) (string, string) {
			t.Helper()

			in := &bedrocksdk.CreateProvisionedModelThroughputInput{
				ProvisionedModelName: aws.String("pmt-token"),
				ModelId:              aws.String("anthropic.claude-v2"),
				ModelUnits:           aws.Int32(1),
				ClientRequestToken:   aws.String("pmt-token-1"),
			}
			a, err := client.CreateProvisionedModelThroughput(t.Context(), in)
			require.NoError(t, err)
			b, err := client.CreateProvisionedModelThroughput(t.Context(), in)
			require.NoError(t, err)

			return aws.ToString(a.ProvisionedModelArn), aws.ToString(b.ProvisionedModelArn)
		}},
		{name: "custom_model", run: func(t *testing.T, client *bedrocksdk.Client) (string, string) {
			t.Helper()

			in := &bedrocksdk.CreateCustomModelInput{
				ModelName:          aws.String("cm-token"),
				ClientRequestToken: aws.String("cm-token-1"),
				ModelKmsKeyArn:     aws.String("arn:aws:kms:us-east-1:123456789012:key/cm-key"),
			}
			a, err := client.CreateCustomModel(t.Context(), in)
			require.NoError(t, err)
			b, err := client.CreateCustomModel(t.Context(), in)
			require.NoError(t, err)

			got, err := client.GetCustomModel(t.Context(), &bedrocksdk.GetCustomModelInput{ModelIdentifier: a.ModelArn})
			require.NoError(t, err)
			assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/cm-key", aws.ToString(got.ModelKmsKeyArn))

			return aws.ToString(a.ModelArn), aws.ToString(b.ModelArn)
		}},
		{name: "custom_model_deployment", run: func(t *testing.T, client *bedrocksdk.Client) (string, string) {
			t.Helper()

			in := &bedrocksdk.CreateCustomModelDeploymentInput{
				ModelArn:            aws.String("arn:aws:bedrock:us-east-1:123456789012:custom-model/cm-1"),
				ModelDeploymentName: aws.String("cmd-token"),
				Description:         aws.String("blue deployment"),
				ClientRequestToken:  aws.String("cmd-token-1"),
			}
			a, err := client.CreateCustomModelDeployment(t.Context(), in)
			require.NoError(t, err)
			b, err := client.CreateCustomModelDeployment(t.Context(), in)
			require.NoError(t, err)

			got, err := client.GetCustomModelDeployment(t.Context(), &bedrocksdk.GetCustomModelDeploymentInput{
				CustomModelDeploymentIdentifier: a.CustomModelDeploymentArn,
			})
			require.NoError(t, err)
			assert.Equal(t, "blue deployment", aws.ToString(got.Description))

			return aws.ToString(a.CustomModelDeploymentArn), aws.ToString(b.CustomModelDeploymentArn)
		}},
		{name: "automated_reasoning_policy", run: func(t *testing.T, client *bedrocksdk.Client) (string, string) {
			t.Helper()

			in := &bedrocksdk.CreateAutomatedReasoningPolicyInput{
				Name:               aws.String("arp-token"),
				Description:        aws.String("policy"),
				KmsKeyId:           aws.String("arp-key"),
				ClientRequestToken: aws.String("arp-token-1"),
			}
			a, err := client.CreateAutomatedReasoningPolicy(t.Context(), in)
			require.NoError(t, err)
			assert.Equal(t, "policy", aws.ToString(a.Description))
			b, err := client.CreateAutomatedReasoningPolicy(t.Context(), in)
			require.NoError(t, err)

			got, err := client.GetAutomatedReasoningPolicy(t.Context(), &bedrocksdk.GetAutomatedReasoningPolicyInput{
				PolicyArn: a.PolicyArn,
			})
			require.NoError(t, err)
			assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/arp-key", aws.ToString(got.KmsKeyArn))

			return aws.ToString(a.PolicyArn), aws.ToString(b.PolicyArn)
		}},
		{name: "model_copy_job", run: func(t *testing.T, client *bedrocksdk.Client) (string, string) {
			t.Helper()

			srcARN := "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-text-express-v1"
			tags := []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}}
			in := &bedrocksdk.CreateModelCopyJobInput{
				SourceModelArn:     aws.String(srcARN),
				TargetModelName:    aws.String("copy-token"),
				ModelKmsKeyId:      aws.String("copy-key"),
				ClientRequestToken: aws.String("copy-token-1"),
				TargetModelTags:    tags,
			}
			a, err := client.CreateModelCopyJob(t.Context(), in)
			require.NoError(t, err)
			b, err := client.CreateModelCopyJob(t.Context(), in)
			require.NoError(t, err)

			got, err := client.GetModelCopyJob(t.Context(), &bedrocksdk.GetModelCopyJobInput{JobArn: a.JobArn})
			require.NoError(t, err)
			assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/copy-key", aws.ToString(got.TargetModelKmsKeyArn))
			assert.Equal(t, "copy-token", aws.ToString(got.TargetModelName))
			assert.Equal(t, tags, got.TargetModelTags)

			return aws.ToString(a.JobArn), aws.ToString(b.JobArn)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newDroppedMembersClient(t)
			first, retry := tt.run(t, client)

			assert.NotEmpty(t, first)
			assert.Equal(t, first, retry)
		})
	}
}

func TestSDK_ModelImportJobMembers(t *testing.T) {
	t.Parallel()

	_, client := newDroppedMembersClient(t)
	ctx := t.Context()

	in := &bedrocksdk.CreateModelImportJobInput{
		JobName:               aws.String("import-members"),
		ImportedModelName:     aws.String("imported-members"),
		RoleArn:               aws.String("arn:aws:iam::123456789012:role/import"),
		ImportedModelKmsKeyId: aws.String("import-key"),
		ClientRequestToken:    aws.String("import-token"),
		ImportedModelTags:     []types.Tag{{Key: aws.String("model"), Value: aws.String("tag")}},
		VpcConfig: &types.VpcConfig{
			SecurityGroupIds: []string{"sg-1"},
			SubnetIds:        []string{"subnet-1"},
		},
		ModelDataSource: &types.ModelDataSourceMemberS3DataSource{
			Value: types.S3DataSource{S3Uri: aws.String("s3://bucket/model")},
		},
	}

	created, err := client.CreateModelImportJob(ctx, in)
	require.NoError(t, err)
	retry, err := client.CreateModelImportJob(ctx, in)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.JobArn), aws.ToString(retry.JobArn))

	got, err := client.GetModelImportJob(ctx, &bedrocksdk.GetModelImportJobInput{JobIdentifier: created.JobArn})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/import-key", aws.ToString(got.ImportedModelKmsKeyArn))
	require.NotNil(t, got.VpcConfig)
	assert.Equal(t, []string{"subnet-1"}, got.VpcConfig.SubnetIds)

	tagsOut, err := client.ListTagsForResource(ctx, &bedrocksdk.ListTagsForResourceInput{
		ResourceARN: got.ImportedModelArn,
	})
	require.NoError(t, err)
	assert.Equal(t, []types.Tag{{Key: aws.String("model"), Value: aws.String("tag")}}, tagsOut.Tags)
}

func TestSDK_ModelCustomizationJobMembers(t *testing.T) {
	t.Parallel()

	b, client := newDroppedMembersClient(t)
	ctx := t.Context()

	in := &bedrocksdk.CreateModelCustomizationJobInput{
		JobName:             aws.String("cust-members"),
		CustomModelName:     aws.String("cust-members-model"),
		BaseModelIdentifier: aws.String("amazon.titan-text-express-v1"),
		RoleArn:             aws.String("arn:aws:iam::123456789012:role/customize"),
		TrainingDataConfig:  &types.TrainingDataConfig{S3Uri: aws.String("s3://bucket/training")},
		OutputDataConfig:    &types.OutputDataConfig{S3Uri: aws.String("s3://bucket/output")},
		ClientRequestToken:  aws.String("cust-token"),
		HyperParameters:     map[string]string{"epochCount": "3"},
		CustomModelKmsKeyId: aws.String("cust-key"),
		CustomModelTags:     []types.Tag{{Key: aws.String("model"), Value: aws.String("tag")}},
		VpcConfig:           &types.VpcConfig{SecurityGroupIds: []string{"sg-9"}, SubnetIds: []string{"subnet-9"}},
		CustomizationConfig: &types.CustomizationConfigMemberDistillationConfig{
			Value: types.DistillationConfig{TeacherModelConfig: &types.TeacherModelConfig{
				TeacherModelIdentifier: aws.String("anthropic.claude-v2"),
			}},
		},
	}

	created, err := client.CreateModelCustomizationJob(ctx, in)
	require.NoError(t, err)
	retry, err := client.CreateModelCustomizationJob(ctx, in)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.JobArn), aws.ToString(retry.JobArn))

	got, err := client.GetModelCustomizationJob(
		ctx,
		&bedrocksdk.GetModelCustomizationJobInput{JobIdentifier: created.JobArn},
	)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"epochCount": "3"}, got.HyperParameters)
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/cust-key", aws.ToString(got.OutputModelKmsKeyArn))
	require.NotNil(t, got.VpcConfig)
	assert.Equal(t, []string{"subnet-9"}, got.VpcConfig.SubnetIds)
	require.NotNil(t, got.CustomizationConfig)

	require.Equal(t, 1, b.AdvanceCustomizationJobStatuses(0))

	model, err := client.GetCustomModel(ctx, &bedrocksdk.GetCustomModelInput{ModelIdentifier: got.OutputModelArn})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/cust-key", aws.ToString(model.ModelKmsKeyArn))

	tagsOut, err := client.ListTagsForResource(
		ctx,
		&bedrocksdk.ListTagsForResourceInput{ResourceARN: got.OutputModelArn},
	)
	require.NoError(t, err)
	assert.Equal(t, []types.Tag{{Key: aws.String("model"), Value: aws.String("tag")}}, tagsOut.Tags)
}

func TestSDK_ModelInvocationJobMembers(t *testing.T) {
	t.Parallel()

	_, client := newDroppedMembersClient(t)
	ctx := t.Context()

	in := &bedrocksdk.CreateModelInvocationJobInput{
		JobName:                aws.String("invoc-members"),
		ModelId:                aws.String("anthropic.claude-v2"),
		RoleArn:                aws.String("arn:aws:iam::123456789012:role/batch"),
		ClientRequestToken:     aws.String("invoc-token"),
		ModelInvocationType:    types.ModelInvocationTypeInvokeModel,
		TimeoutDurationInHours: aws.Int32(48),
		VpcConfig:              &types.VpcConfig{SecurityGroupIds: []string{"sg-2"}, SubnetIds: []string{"subnet-2"}},
		InputDataConfig: &types.ModelInvocationJobInputDataConfigMemberS3InputDataConfig{
			Value: types.ModelInvocationJobS3InputDataConfig{S3Uri: aws.String("s3://bucket/in")},
		},
		OutputDataConfig: &types.ModelInvocationJobOutputDataConfigMemberS3OutputDataConfig{
			Value: types.ModelInvocationJobS3OutputDataConfig{S3Uri: aws.String("s3://bucket/out")},
		},
	}

	created, err := client.CreateModelInvocationJob(ctx, in)
	require.NoError(t, err)
	retry, err := client.CreateModelInvocationJob(ctx, in)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.JobArn), aws.ToString(retry.JobArn))

	got, err := client.GetModelInvocationJob(ctx, &bedrocksdk.GetModelInvocationJobInput{JobIdentifier: created.JobArn})
	require.NoError(t, err)
	assert.Equal(t, types.ModelInvocationTypeInvokeModel, got.ModelInvocationType)
	assert.EqualValues(t, 48, aws.ToInt32(got.TimeoutDurationInHours))
	require.NotNil(t, got.VpcConfig)
	assert.Equal(t, []string{"subnet-2"}, got.VpcConfig.SubnetIds)
}

func TestSDK_EvaluationJobEncryptionKeyAndToken(t *testing.T) {
	t.Parallel()

	_, client := newDroppedMembersClient(t)
	ctx := t.Context()

	in := &bedrocksdk.CreateEvaluationJobInput{
		JobName:                 aws.String("eval-members"),
		RoleArn:                 aws.String("arn:aws:iam::123456789012:role/eval"),
		CustomerEncryptionKeyId: aws.String("eval-key"),
		ClientRequestToken:      aws.String("eval-token"),
		OutputDataConfig:        &types.EvaluationOutputDataConfig{S3Uri: aws.String("s3://bucket/eval")},
		EvaluationConfig: &types.EvaluationConfigMemberAutomated{Value: types.AutomatedEvaluationConfig{
			DatasetMetricConfigs: []types.EvaluationDatasetMetricConfig{{
				TaskType:    types.EvaluationTaskTypeSummarization,
				MetricNames: []string{"Builtin.Accuracy"},
				Dataset:     &types.EvaluationDataset{Name: aws.String("squad-v2")},
			}},
		}},
		InferenceConfig: &types.EvaluationInferenceConfigMemberModels{Value: []types.EvaluationModelConfig{
			&types.EvaluationModelConfigMemberBedrockModel{Value: types.EvaluationBedrockModel{
				ModelIdentifier: aws.String("amazon.titan-text-express-v1"),
			}},
		}},
	}

	created, err := client.CreateEvaluationJob(ctx, in)
	require.NoError(t, err)
	retry, err := client.CreateEvaluationJob(ctx, in)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.JobArn), aws.ToString(retry.JobArn))

	got, err := client.GetEvaluationJob(ctx, &bedrocksdk.GetEvaluationJobInput{JobIdentifier: created.JobArn})
	require.NoError(t, err)
	assert.Equal(t, "eval-key", aws.ToString(got.CustomerEncryptionKeyId))
}
