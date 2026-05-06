package bedrock_test

import (
	"testing"

	bedrocksdk "github.com/aws/aws-sdk-go-v2/service/bedrock"
	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// bedrock client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &bedrocksdk.Client{}, h.GetSupportedOperations(), []string{
		"CreateModelCopyJob",
		"CreateModelImportJob",
		"CreateModelInvocationJob",
		"CreatePromptRouter",
		"DeleteAutomatedReasoningPolicy",
		"DeleteAutomatedReasoningPolicyBuildWorkflow",
		"DeleteAutomatedReasoningPolicyTestCase",
		"DeleteCustomModelDeployment",
		"DeleteEnforcedGuardrailConfiguration",
		"DeleteFoundationModelAgreement",
		"DeleteImportedModel",
		"DeletePromptRouter",
		"ExportAutomatedReasoningPolicyVersion",
		"GetAutomatedReasoningPolicy",
		"GetAutomatedReasoningPolicyAnnotations",
		"GetAutomatedReasoningPolicyBuildWorkflow",
		"GetAutomatedReasoningPolicyBuildWorkflowResultAssets",
		"GetAutomatedReasoningPolicyNextScenario",
		"GetAutomatedReasoningPolicyTestCase",
		"GetAutomatedReasoningPolicyTestResult",
		"GetCustomModelDeployment",
		"GetEvaluationJob",
		"GetFoundationModelAvailability",
		"GetImportedModel",
		"GetModelCopyJob",
		"GetModelImportJob",
		"GetModelInvocationJob",
		"GetPromptRouter",
		"GetUseCaseForModelAccess",
		"ListAutomatedReasoningPolicies",
		"ListAutomatedReasoningPolicyBuildWorkflows",
		"ListAutomatedReasoningPolicyTestCases",
		"ListAutomatedReasoningPolicyTestResults",
		"ListCustomModelDeployments",
		"ListEnforcedGuardrailsConfiguration",
		"ListEvaluationJobs",
		"ListFoundationModelAgreementOffers",
		"ListImportedModels",
		"ListModelCopyJobs",
		"ListModelImportJobs",
		"ListModelInvocationJobs",
		"ListPromptRouters",
		"PutEnforcedGuardrailConfiguration",
		"PutUseCaseForModelAccess",
		"StartAutomatedReasoningPolicyBuildWorkflow",
		"StartAutomatedReasoningPolicyTestWorkflow",
		"StopEvaluationJob",
		"StopModelInvocationJob",
		"UpdateAutomatedReasoningPolicy",
		"UpdateAutomatedReasoningPolicyAnnotations",
		"UpdateAutomatedReasoningPolicyTestCase",
		"UpdateCustomModelDeployment",
	})
}
