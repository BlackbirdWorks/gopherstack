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
		"BatchDeleteEvaluationJob",
		"CancelAutomatedReasoningPolicyBuildWorkflow",
		"CreateAutomatedReasoningPolicy",
		"CreateAutomatedReasoningPolicyTestCase",
		"CreateAutomatedReasoningPolicyVersion",
		"CreateCustomModel",
		"CreateCustomModelDeployment",
		"CreateEvaluationJob",
		"CreateFoundationModelAgreement",
		"CreateGuardrailVersion",
		"CreateInferenceProfile",
		"CreateMarketplaceModelEndpoint",
		"CreateModelCopyJob",
		"CreateModelCustomizationJob",
		"CreateModelImportJob",
		"CreateModelInvocationJob",
		"CreatePromptRouter",
		"DeleteAutomatedReasoningPolicy",
		"DeleteAutomatedReasoningPolicyBuildWorkflow",
		"DeleteAutomatedReasoningPolicyTestCase",
		"DeleteCustomModel",
		"DeleteCustomModelDeployment",
		"DeleteEnforcedGuardrailConfiguration",
		"DeleteFoundationModelAgreement",
		"DeleteImportedModel",
		"DeleteInferenceProfile",
		"DeleteMarketplaceModelEndpoint",
		"DeleteModelInvocationLoggingConfiguration",
		"DeletePromptRouter",
		"DeregisterMarketplaceModelEndpoint",
		"ExportAutomatedReasoningPolicyVersion",
		"GetAutomatedReasoningPolicy",
		"GetAutomatedReasoningPolicyAnnotations",
		"GetAutomatedReasoningPolicyBuildWorkflow",
		"GetAutomatedReasoningPolicyBuildWorkflowResultAssets",
		"GetAutomatedReasoningPolicyNextScenario",
		"GetAutomatedReasoningPolicyTestCase",
		"GetAutomatedReasoningPolicyTestResult",
		"GetCustomModel",
		"GetCustomModelDeployment",
		"GetEvaluationJob",
		"GetFoundationModelAvailability",
		"GetImportedModel",
		"GetInferenceProfile",
		"GetMarketplaceModelEndpoint",
		"GetModelCopyJob",
		"GetModelCustomizationJob",
		"GetModelImportJob",
		"GetModelInvocationJob",
		"GetModelInvocationLoggingConfiguration",
		"GetPromptRouter",
		"GetUseCaseForModelAccess",
		"ListAutomatedReasoningPolicies",
		"ListAutomatedReasoningPolicyBuildWorkflows",
		"ListAutomatedReasoningPolicyTestCases",
		"ListAutomatedReasoningPolicyTestResults",
		"ListCustomModelDeployments",
		"ListCustomModels",
		"ListEnforcedGuardrailsConfiguration",
		"ListEvaluationJobs",
		"ListFoundationModelAgreementOffers",
		"ListImportedModels",
		"ListInferenceProfiles",
		"ListMarketplaceModelEndpoints",
		"ListModelCopyJobs",
		"ListModelCustomizationJobs",
		"ListModelImportJobs",
		"ListModelInvocationJobs",
		"ListPromptRouters",
		"PutEnforcedGuardrailConfiguration",
		"PutModelInvocationLoggingConfiguration",
		"PutUseCaseForModelAccess",
		"RegisterMarketplaceModelEndpoint",
		"StartAutomatedReasoningPolicyBuildWorkflow",
		"StartAutomatedReasoningPolicyTestWorkflow",
		"StopEvaluationJob",
		"StopModelCustomizationJob",
		"StopModelInvocationJob",
		"UpdateAutomatedReasoningPolicy",
		"UpdateAutomatedReasoningPolicyAnnotations",
		"UpdateAutomatedReasoningPolicyTestCase",
		"UpdateCustomModelDeployment",
		"UpdateMarketplaceModelEndpoint",
	})
}
