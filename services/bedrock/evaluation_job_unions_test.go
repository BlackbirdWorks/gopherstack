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

// TestCreateEvaluationJob_UnionConfigs_RealClient covers gopherstack-39ps.
// bedrock@v1.66.4 models CreateEvaluationJobInput's EvaluationConfig and
// InferenceConfig as polymorphic unions (types.EvaluationConfig,
// types/types.go:2850; types.EvaluationInferenceConfig, types/types.go:2964).
// smithy-go encodes a union as a single-key object naming the variant
// (serializers.go:10697-10719, 10795-10817); gopherstack's request parser
// previously expected evaluationConfig as a bare array and inferenceConfig
// as an unrelated flat object, so a real SDK client sending genuine union
// content got a 400 ValidationException ("invalid request body") -- not a
// dropped field or zero value, the call failed outright (confirmed against
// unmodified code while fixing this bug).
//
// This test drives CreateEvaluationJob and GetEvaluationJob entirely through
// a real bedrocksdk.Client, constructing actual union values via their
// concrete SDK member types, to exercise the client's own union encoder and
// decoder -- not a raw-body test, which cannot reach that code path. It
// covers both EvaluationConfig variants (Automated, Human) and both
// InferenceConfig variants (Models, RagConfigs), including a RagConfigs
// entry whose own nested union (KnowledgeBaseConfig -> RetrieveConfig)
// round-trips back through the real client's decoder, proving gopherstack's
// verbatim (opaque) storage of that deeper content is byte-for-byte faithful
// end to end.
func TestCreateEvaluationJob_UnionConfigs_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		evaluationConfig types.EvaluationConfig
		inferenceConfig  types.EvaluationInferenceConfig
		checkGet         func(*testing.T, *bedrocksdk.GetEvaluationJobOutput)
		name             string
		wantJobType      types.EvaluationJobType
	}{
		{
			name: "automated evaluation config with models inference config",
			evaluationConfig: &types.EvaluationConfigMemberAutomated{
				Value: types.AutomatedEvaluationConfig{
					DatasetMetricConfigs: []types.EvaluationDatasetMetricConfig{
						{
							TaskType:    types.EvaluationTaskTypeSummarization,
							MetricNames: []string{"Builtin.Accuracy"},
							Dataset:     &types.EvaluationDataset{Name: aws.String("squad-v2")},
						},
					},
					EvaluatorModelConfig: &types.EvaluatorModelConfigMemberBedrockEvaluatorModels{
						Value: []types.BedrockEvaluatorModel{
							{ModelIdentifier: aws.String("anthropic.claude-3-sonnet-20240229-v1:0")},
						},
					},
				},
			},
			inferenceConfig: &types.EvaluationInferenceConfigMemberModels{
				Value: []types.EvaluationModelConfig{
					&types.EvaluationModelConfigMemberBedrockModel{
						Value: types.EvaluationBedrockModel{
							ModelIdentifier: aws.String("amazon.titan-text-express-v1"),
						},
					},
				},
			},
			wantJobType: types.EvaluationJobTypeAutomated,
			checkGet: func(t *testing.T, got *bedrocksdk.GetEvaluationJobOutput) {
				t.Helper()

				models, ok := got.InferenceConfig.(*types.EvaluationInferenceConfigMemberModels)
				require.True(t, ok)
				require.Len(t, models.Value, 1)
				bedrockModel, ok := models.Value[0].(*types.EvaluationModelConfigMemberBedrockModel)
				require.True(t, ok)
				assert.Equal(t, "amazon.titan-text-express-v1", aws.ToString(bedrockModel.Value.ModelIdentifier))
			},
		},
		{
			name: "human evaluation config with ragConfigs inference config",
			evaluationConfig: &types.EvaluationConfigMemberHuman{
				Value: types.HumanEvaluationConfig{
					DatasetMetricConfigs: []types.EvaluationDatasetMetricConfig{
						{
							TaskType:    types.EvaluationTaskTypeGeneration,
							MetricNames: []string{"Builtin.Correctness"},
							Dataset:     &types.EvaluationDataset{Name: aws.String("squad-v2")},
						},
					},
					HumanWorkflowConfig: &types.HumanWorkflowConfig{
						FlowDefinitionArn: aws.String(
							"arn:aws:sagemaker:us-east-1:123456789012:flow-definition/eval",
						),
					},
				},
			},
			inferenceConfig: &types.EvaluationInferenceConfigMemberRagConfigs{
				Value: []types.RAGConfig{
					&types.RAGConfigMemberKnowledgeBaseConfig{
						Value: &types.KnowledgeBaseConfigMemberRetrieveConfig{
							Value: types.RetrieveConfig{
								KnowledgeBaseId: aws.String("kb-0001"),
								KnowledgeBaseRetrievalConfiguration: &types.KnowledgeBaseRetrievalConfiguration{
									VectorSearchConfiguration: &types.KnowledgeBaseVectorSearchConfiguration{},
								},
							},
						},
					},
				},
			},
			wantJobType: types.EvaluationJobTypeHuman,
			checkGet: func(t *testing.T, got *bedrocksdk.GetEvaluationJobOutput) {
				t.Helper()

				ragConfigs, ok := got.InferenceConfig.(*types.EvaluationInferenceConfigMemberRagConfigs)
				require.True(t, ok)
				require.Len(t, ragConfigs.Value, 1)
				kbConfig, ok := ragConfigs.Value[0].(*types.RAGConfigMemberKnowledgeBaseConfig)
				require.True(t, ok)
				retrieveConfig, ok := kbConfig.Value.(*types.KnowledgeBaseConfigMemberRetrieveConfig)
				require.True(t, ok)
				assert.Equal(t, "kb-0001", aws.ToString(retrieveConfig.Value.KnowledgeBaseId))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := bedrock.NewHandler(bedrock.NewInMemoryBackend("123456789012", "us-east-1"))
			client := newTestBedrockClient(t, h)

			createOut, err := client.CreateEvaluationJob(t.Context(), &bedrocksdk.CreateEvaluationJobInput{
				JobName: aws.String("39ps-" + tt.name),
				RoleArn: aws.String("arn:aws:iam::123456789012:role/eval-role"),
				OutputDataConfig: &types.EvaluationOutputDataConfig{
					S3Uri: aws.String("s3://bucket/eval-output"),
				},
				EvaluationConfig: tt.evaluationConfig,
				InferenceConfig:  tt.inferenceConfig,
			})
			require.NoError(t, err, "real SDK client union request must not 400")
			require.NotNil(t, createOut.JobArn)

			got, err := client.GetEvaluationJob(
				t.Context(), &bedrocksdk.GetEvaluationJobInput{JobIdentifier: createOut.JobArn},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantJobType, got.JobType)
			tt.checkGet(t, got)
		})
	}
}
