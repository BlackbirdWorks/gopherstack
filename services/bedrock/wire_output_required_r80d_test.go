package bedrock_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrocksdk "github.com/aws/aws-sdk-go-v2/service/bedrock"
	"github.com/aws/aws-sdk-go-v2/service/bedrock/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

func newTestARPBuildWorkflow(
	t *testing.T, client *bedrocksdk.Client, policyName string,
) (string, string) {
	t.Helper()

	policy, err := client.CreateAutomatedReasoningPolicy(
		t.Context(), &bedrocksdk.CreateAutomatedReasoningPolicyInput{Name: aws.String(policyName)},
	)
	require.NoError(t, err)

	wf, err := client.StartAutomatedReasoningPolicyBuildWorkflow(
		t.Context(),
		&bedrocksdk.StartAutomatedReasoningPolicyBuildWorkflowInput{
			PolicyArn:         policy.PolicyArn,
			BuildWorkflowType: types.AutomatedReasoningPolicyBuildWorkflowTypeIngestContent,
			SourceContent: &types.AutomatedReasoningPolicyBuildWorkflowSource{
				PolicyDefinition: &types.AutomatedReasoningPolicyDefinition{Version: aws.String("1")},
			},
		},
	)
	require.NoError(t, err)

	return aws.ToString(policy.PolicyArn), aws.ToString(wf.BuildWorkflowId)
}

// TestGetARPBuildWorkflow_RequiredTimestamps_RealClient covers gopherstack-r80d
// (required-output-member sweep). GetAutomatedReasoningPolicyBuildWorkflowOutput
// requires CreatedAt and UpdatedAt (bedrock@v1.66.4
// api_op_GetAutomatedReasoningPolicyBuildWorkflow.go:59-78); the model tracked
// neither field at all, so both decoded as a real client's zero time.Time.
func TestGetARPBuildWorkflow_RequiredTimestamps_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestBedrockClient(
		t, bedrock.NewHandler(bedrock.NewInMemoryBackend("123456789012", "us-east-1")),
	)

	policyARN, workflowID := newTestARPBuildWorkflow(t, client, "r80d-bw-timestamps")

	got, err := client.GetAutomatedReasoningPolicyBuildWorkflow(
		t.Context(),
		&bedrocksdk.GetAutomatedReasoningPolicyBuildWorkflowInput{
			PolicyArn:       aws.String(policyARN),
			BuildWorkflowId: aws.String(workflowID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, got.CreatedAt)
	require.NotNil(t, got.UpdatedAt)
	assert.False(t, got.CreatedAt.IsZero())
	assert.False(t, got.UpdatedAt.IsZero())
}

// TestGetARPAnnotations_RequiredFields_RealClient covers gopherstack-r80d.
// GetAutomatedReasoningPolicyAnnotationsOutput requires BuildWorkflowId, Name,
// PolicyArn, and UpdatedAt in addition to Annotations/AnnotationSetHash
// (bedrock@v1.66.4 api_op_GetAutomatedReasoningPolicyAnnotations.go:62-80); the
// handler returned only annotations/annotationSetHash, so the other four
// decoded as a real client's zero values.
func TestGetARPAnnotations_RequiredFields_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestBedrockClient(
		t, bedrock.NewHandler(bedrock.NewInMemoryBackend("123456789012", "us-east-1")),
	)

	policyARN, workflowID := newTestARPBuildWorkflow(t, client, "r80d-ann-fields")

	got, err := client.GetAutomatedReasoningPolicyAnnotations(
		t.Context(),
		&bedrocksdk.GetAutomatedReasoningPolicyAnnotationsInput{
			PolicyArn:       aws.String(policyARN),
			BuildWorkflowId: aws.String(workflowID),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, workflowID, aws.ToString(got.BuildWorkflowId))
	assert.Equal(t, "r80d-ann-fields", aws.ToString(got.Name))
	assert.Equal(t, policyARN, aws.ToString(got.PolicyArn))
	require.NotNil(t, got.UpdatedAt)
	assert.False(t, got.UpdatedAt.IsZero())
}

// TestGetARPBuildWorkflowResultAssets_PolicyArn_RealClient covers
// gopherstack-r80d. GetAutomatedReasoningPolicyBuildWorkflowResultAssetsOutput
// requires PolicyArn (bedrock@v1.66.4
// api_op_GetAutomatedReasoningPolicyBuildWorkflowResultAssets.go:67-70); the
// handler dropped it entirely.
func TestGetARPBuildWorkflowResultAssets_PolicyArn_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestBedrockClient(
		t, bedrock.NewHandler(bedrock.NewInMemoryBackend("123456789012", "us-east-1")),
	)

	policyARN, workflowID := newTestARPBuildWorkflow(t, client, "r80d-result-assets")

	got, err := client.GetAutomatedReasoningPolicyBuildWorkflowResultAssets(
		t.Context(),
		&bedrocksdk.GetAutomatedReasoningPolicyBuildWorkflowResultAssetsInput{
			PolicyArn:       aws.String(policyARN),
			BuildWorkflowId: aws.String(workflowID),
			AssetType:       types.AutomatedReasoningPolicyBuildResultAssetTypePolicyDefinition,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, policyARN, aws.ToString(got.PolicyArn))
}

// TestGetARPTestCase_WrappedShape_RealClient covers gopherstack-r80d.
// GetAutomatedReasoningPolicyTestCaseOutput wraps the test case under a
// required "testCase" key (bedrock@v1.66.4 deserializers.go:7223-7233); the
// handler returned the test case's fields inlined at the top level with no
// "testCase" wrapper, so a real client's required TestCase decoded nil.
func TestGetARPTestCase_WrappedShape_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestBedrockClient(
		t, bedrock.NewHandler(bedrock.NewInMemoryBackend("123456789012", "us-east-1")),
	)

	policy, err := client.CreateAutomatedReasoningPolicy(
		t.Context(), &bedrocksdk.CreateAutomatedReasoningPolicyInput{Name: aws.String("r80d-tc-shape")},
	)
	require.NoError(t, err)

	tc, err := client.CreateAutomatedReasoningPolicyTestCase(
		t.Context(),
		&bedrocksdk.CreateAutomatedReasoningPolicyTestCaseInput{
			PolicyArn:                        policy.PolicyArn,
			GuardContent:                     aws.String("the model said the sky is blue"),
			ExpectedAggregatedFindingsResult: types.AutomatedReasoningCheckResultValid,
		},
	)
	require.NoError(t, err)

	got, err := client.GetAutomatedReasoningPolicyTestCase(
		t.Context(),
		&bedrocksdk.GetAutomatedReasoningPolicyTestCaseInput{
			PolicyArn:  policy.PolicyArn,
			TestCaseId: tc.TestCaseId,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, got.TestCase)
	assert.Equal(t, aws.ToString(tc.TestCaseId), aws.ToString(got.TestCase.TestCaseId))
	assert.False(t, got.TestCase.CreatedAt.IsZero())
}

// TestGetARPTestResult_WrappedShape_RealClient covers gopherstack-r80d.
// GetAutomatedReasoningPolicyTestResultOutput requires a "testResult" wrapper
// carrying policyArn/testCase/testRunStatus/updatedAt (bedrock@v1.66.4
// types.go:2055-2092); the handler returned a flat, differently-keyed object
// with no "testResult" key at all, so a real client's required TestResult
// decoded nil.
func TestGetARPTestResult_WrappedShape_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestBedrockClient(
		t, bedrock.NewHandler(bedrock.NewInMemoryBackend("123456789012", "us-east-1")),
	)

	policyARN, workflowID := newTestARPBuildWorkflow(t, client, "r80d-result-shape")

	tc, err := client.CreateAutomatedReasoningPolicyTestCase(
		t.Context(),
		&bedrocksdk.CreateAutomatedReasoningPolicyTestCaseInput{
			PolicyArn:                        aws.String(policyARN),
			GuardContent:                     aws.String("the model said the sky is blue"),
			ExpectedAggregatedFindingsResult: types.AutomatedReasoningCheckResultValid,
		},
	)
	require.NoError(t, err)

	got, err := client.GetAutomatedReasoningPolicyTestResult(
		t.Context(),
		&bedrocksdk.GetAutomatedReasoningPolicyTestResultInput{
			PolicyArn:       aws.String(policyARN),
			BuildWorkflowId: aws.String(workflowID),
			TestCaseId:      tc.TestCaseId,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, got.TestResult)
	assert.Equal(t, policyARN, aws.ToString(got.TestResult.PolicyArn))
	require.NotNil(t, got.TestResult.TestCase)
	assert.Equal(t, aws.ToString(tc.TestCaseId), aws.ToString(got.TestResult.TestCase.TestCaseId))
	assert.Equal(t, types.AutomatedReasoningPolicyTestRunStatusCompleted, got.TestResult.TestRunStatus)
	require.NotNil(t, got.TestResult.UpdatedAt)
	assert.False(t, got.TestResult.UpdatedAt.IsZero())
}

// TestGetModelCustomizationJob_ValidationDataConfig_RealClient covers
// gopherstack-r80d. GetModelCustomizationJobOutput requires
// ValidationDataConfig (bedrock@v1.66.4 api_op_GetModelCustomizationJob.go:85);
// the response shape had no field for it at all, so a real client's required
// ValidationDataConfig decoded nil.
func TestGetModelCustomizationJob_ValidationDataConfig_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestBedrockClient(
		t, bedrock.NewHandler(bedrock.NewInMemoryBackend("123456789012", "us-east-1")),
	)

	createOut, err := client.CreateModelCustomizationJob(t.Context(), &bedrocksdk.CreateModelCustomizationJobInput{
		JobName:             aws.String("r80d-validation-config"),
		CustomModelName:     aws.String("r80d-validation-config-model"),
		BaseModelIdentifier: aws.String("amazon.titan-text-express-v1"),
		RoleArn:             aws.String("arn:aws:iam::000000000000:role/customize"),
		TrainingDataConfig:  &types.TrainingDataConfig{S3Uri: aws.String("s3://bucket/training")},
		OutputDataConfig:    &types.OutputDataConfig{S3Uri: aws.String("s3://bucket/output")},
		ValidationDataConfig: &types.ValidationDataConfig{
			Validators: []types.Validator{{S3Uri: aws.String("s3://bucket/validation")}},
		},
	})
	require.NoError(t, err)

	got, err := client.GetModelCustomizationJob(
		t.Context(), &bedrocksdk.GetModelCustomizationJobInput{JobIdentifier: createOut.JobArn},
	)
	require.NoError(t, err)
	require.NotNil(t, got.ValidationDataConfig)
	require.Len(t, got.ValidationDataConfig.Validators, 1)
	assert.Equal(t, "s3://bucket/validation", aws.ToString(got.ValidationDataConfig.Validators[0].S3Uri))
}

// TestGetEvaluationJob_OutputDataConfig_RealClient covers gopherstack-r80d.
// GetEvaluationJobOutput requires OutputDataConfig (bedrock@v1.66.4
// api_op_GetEvaluationJob.go:78); it had no input parsing, no model field,
// and no response key at all -- structurally absent regardless of what a
// real client sent on Create (the "member with no struct field at all"
// class, matching iam's JobCompletionDate from the input-side sweep).
//
// Create is driven through doRequest with gopherstack's OWN (pre-existing,
// separately broken) evaluationConfig/inferenceConfig request shape rather
// than a real client, because the real CreateEvaluationJobInput serializes
// those two required members as polymorphic union objects
// (types.EvaluationConfig/types.EvaluationInferenceConfig) and gopherstack's
// createEvaluationJobInput instead expects an evaluationConfig ARRAY and a
// flat inferenceConfig object -- a real client's request 400s on this
// service today independent of the r80d fix here (found while writing this
// test; recorded in PARITY.md as a distinct, severe, input-side gap, not
// fixed by this pass). Get is still driven through the real client, which is
// what proves the OutputDataConfig fix itself.
func TestGetEvaluationJob_OutputDataConfig_RealClient(t *testing.T) {
	t.Parallel()

	h := bedrock.NewHandler(bedrock.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestBedrockClient(t, h)

	// evaluationConfig/inferenceConfig are omitted here: gopherstack's own
	// request/response shape for both is separately broken (see doc comment
	// above), and sending either would make GetEvaluationJob's real-client
	// deserialization fail for a reason unrelated to the fix under test.
	rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{
		"jobName":          "r80d-eval-output-config",
		"roleArn":          "arn:aws:iam::123456789012:role/eval-role",
		"outputDataConfig": map[string]any{"s3Uri": "s3://bucket/eval-output"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createOut map[string]any
	mustUnmarshal(t, rec, &createOut)
	jobARN, ok := createOut["jobArn"].(string)
	require.True(t, ok)

	got, err := client.GetEvaluationJob(
		t.Context(), &bedrocksdk.GetEvaluationJobInput{JobIdentifier: aws.String(jobARN)},
	)
	require.NoError(t, err)
	require.NotNil(t, got.OutputDataConfig)
	assert.Equal(t, "s3://bucket/eval-output", aws.ToString(got.OutputDataConfig.S3Uri))
}

// TestGetModelCopyJob_SourceAccountId_RealClient covers gopherstack-r80d.
// GetModelCopyJobOutput requires SourceAccountId (bedrock@v1.66.4
// api_op_GetModelCopyJob.go:55-59); it was dropped entirely even though the
// backend already stores SourceModelArn, which embeds the same account
// segment gopherstack itself minted when building the ARN.
func TestGetModelCopyJob_SourceAccountId_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestBedrockClient(
		t, bedrock.NewHandler(bedrock.NewInMemoryBackend("123456789012", "us-east-1")),
	)

	createOut, err := client.CreateCustomModel(t.Context(), &bedrocksdk.CreateCustomModelInput{
		ModelName: aws.String("r80d-copy-source-model"),
	})
	require.NoError(t, err)

	copyOut, err := client.CreateModelCopyJob(t.Context(), &bedrocksdk.CreateModelCopyJobInput{
		SourceModelArn:  createOut.ModelArn,
		TargetModelName: aws.String("r80d-copy-target-model"),
	})
	require.NoError(t, err)

	got, err := client.GetModelCopyJob(
		t.Context(), &bedrocksdk.GetModelCopyJobInput{JobArn: copyOut.JobArn},
	)
	require.NoError(t, err)
	assert.Equal(t, "123456789012", aws.ToString(got.SourceAccountId))
}
