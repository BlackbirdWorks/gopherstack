package bedrock_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

// sdkRouteCases is the authoritative method+path for every real core-Bedrock
// operation (the bedrock@v1.66.4 client, NOT the in-package BedrockAgents
// sub-API -- see handler_agent_sdk_route_table_test.go for that one's own
// table, sourced from the separate bedrockagent@v1.58.4 pinned SDK). Each
// entry's "request.Method" and the string passed to httpbinding.SplitURI in
// that op's awsRestjson1_serializeOp<Op>.HandleSerialize, extracted directly
// from bedrock@v1.66.4's serializers.go. PLACEHOLDER stands in for any
// {Param} URI label -- the router does not validate ID shape, so the literal
// value doesn't matter here, only that the path matches Op. No two ops in
// this table share the same (method, path-with-params-stripped) pair, so
// unlike s3/lambda no entry needed a required dynamic query/header member to
// disambiguate it from a sibling.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"BatchDeleteAdvancedPromptOptimizationJob", "POST", "/advanced-prompt-optimization-job/batch-delete"},
		{"BatchDeleteEvaluationJob", "POST", "/evaluation-jobs/batch-delete"},
		{
			"CancelAutomatedReasoningPolicyBuildWorkflow",
			"POST",
			"/automated-reasoning-policies/PLACEHOLDER/build-workflows/PLACEHOLDER/cancel",
		},
		{"CreateAdvancedPromptOptimizationJob", "POST", "/advanced-prompt-optimization-jobs"},
		{"CreateAutomatedReasoningPolicy", "POST", "/automated-reasoning-policies"},
		{"CreateAutomatedReasoningPolicyTestCase", "POST", "/automated-reasoning-policies/PLACEHOLDER/test-cases"},
		{"CreateAutomatedReasoningPolicyVersion", "POST", "/automated-reasoning-policies/PLACEHOLDER/versions"},
		{"CreateCustomModel", "POST", "/custom-models/create-custom-model"},
		{"CreateCustomModelDeployment", "POST", "/model-customization/custom-model-deployments"},
		{"CreateEvaluationJob", "POST", "/evaluation-jobs"},
		{"CreateFoundationModelAgreement", "POST", "/create-foundation-model-agreement"},
		{"CreateGuardrail", "POST", "/guardrails"},
		{"CreateGuardrailVersion", "POST", "/guardrails/PLACEHOLDER"},
		{"CreateInferenceProfile", "POST", "/inference-profiles"},
		{"CreateMarketplaceModelEndpoint", "POST", "/marketplace-model/endpoints"},
		{"CreateModelCopyJob", "POST", "/model-copy-jobs"},
		{"CreateModelCustomizationJob", "POST", "/model-customization-jobs"},
		{"CreateModelImportJob", "POST", "/model-import-jobs"},
		{"CreateModelInvocationJob", "POST", "/model-invocation-job"},
		{"CreatePromptRouter", "POST", "/prompt-routers"},
		{"CreateProvisionedModelThroughput", "POST", "/provisioned-model-throughput"},
		{"DeleteAutomatedReasoningPolicy", "DELETE", "/automated-reasoning-policies/PLACEHOLDER"},
		{
			"DeleteAutomatedReasoningPolicyBuildWorkflow",
			"DELETE",
			"/automated-reasoning-policies/PLACEHOLDER/build-workflows/PLACEHOLDER",
		},
		{
			"DeleteAutomatedReasoningPolicyTestCase",
			"DELETE",
			"/automated-reasoning-policies/PLACEHOLDER/test-cases/PLACEHOLDER",
		},
		{"DeleteCustomModel", "DELETE", "/custom-models/PLACEHOLDER"},
		{"DeleteCustomModelDeployment", "DELETE", "/model-customization/custom-model-deployments/PLACEHOLDER"},
		{"DeleteEnforcedGuardrailConfiguration", "DELETE", "/enforcedGuardrailsConfiguration/PLACEHOLDER"},
		{"DeleteFoundationModelAgreement", "POST", "/delete-foundation-model-agreement"},
		{"DeleteGuardrail", "DELETE", "/guardrails/PLACEHOLDER"},
		{"DeleteImportedModel", "DELETE", "/imported-models/PLACEHOLDER"},
		{"DeleteInferenceProfile", "DELETE", "/inference-profiles/PLACEHOLDER"},
		{"DeleteMarketplaceModelEndpoint", "DELETE", "/marketplace-model/endpoints/PLACEHOLDER"},
		{"DeleteModelInvocationLoggingConfiguration", "DELETE", "/logging/modelinvocations"},
		{"DeletePromptRouter", "DELETE", "/prompt-routers/PLACEHOLDER"},
		{"DeleteProvisionedModelThroughput", "DELETE", "/provisioned-model-throughput/PLACEHOLDER"},
		{"DeleteResourcePolicy", "DELETE", "/resource-policy/PLACEHOLDER"},
		{"DeregisterMarketplaceModelEndpoint", "DELETE", "/marketplace-model/endpoints/PLACEHOLDER/registration"},
		{"ExportAutomatedReasoningPolicyVersion", "GET", "/automated-reasoning-policies/PLACEHOLDER/export"},
		{"GetAccountDataRetention", "GET", "/data-retention"},
		{"GetAdvancedPromptOptimizationJob", "GET", "/advanced-prompt-optimization-jobs/PLACEHOLDER"},
		{"GetAutomatedReasoningPolicy", "GET", "/automated-reasoning-policies/PLACEHOLDER"},
		{
			"GetAutomatedReasoningPolicyAnnotations",
			"GET",
			"/automated-reasoning-policies/PLACEHOLDER/build-workflows/PLACEHOLDER/annotations",
		},
		{
			"GetAutomatedReasoningPolicyBuildWorkflow",
			"GET",
			"/automated-reasoning-policies/PLACEHOLDER/build-workflows/PLACEHOLDER",
		},
		{
			"GetAutomatedReasoningPolicyBuildWorkflowResultAssets",
			"GET",
			"/automated-reasoning-policies/PLACEHOLDER/build-workflows/PLACEHOLDER/result-assets",
		},
		{
			"GetAutomatedReasoningPolicyNextScenario",
			"GET",
			"/automated-reasoning-policies/PLACEHOLDER/build-workflows/PLACEHOLDER/scenarios",
		},
		{
			"GetAutomatedReasoningPolicyTestCase",
			"GET",
			"/automated-reasoning-policies/PLACEHOLDER/test-cases/PLACEHOLDER",
		},
		{
			"GetAutomatedReasoningPolicyTestResult",
			"GET",
			"/automated-reasoning-policies/PLACEHOLDER/build-workflows/PLACEHOLDER/test-cases/PLACEHOLDER/test-results",
		},
		{"GetCustomModel", "GET", "/custom-models/PLACEHOLDER"},
		{"GetCustomModelDeployment", "GET", "/model-customization/custom-model-deployments/PLACEHOLDER"},
		{"GetEvaluationJob", "GET", "/evaluation-jobs/PLACEHOLDER"},
		{"GetFoundationModel", "GET", "/foundation-models/PLACEHOLDER"},
		{"GetFoundationModelAvailability", "GET", "/foundation-model-availability/PLACEHOLDER"},
		{"GetGuardrail", "GET", "/guardrails/PLACEHOLDER"},
		{"GetImportedModel", "GET", "/imported-models/PLACEHOLDER"},
		{"GetInferenceProfile", "GET", "/inference-profiles/PLACEHOLDER"},
		{"GetMarketplaceModelEndpoint", "GET", "/marketplace-model/endpoints/PLACEHOLDER"},
		{"GetModelCopyJob", "GET", "/model-copy-jobs/PLACEHOLDER"},
		{"GetModelCustomizationJob", "GET", "/model-customization-jobs/PLACEHOLDER"},
		{"GetModelImportJob", "GET", "/model-import-jobs/PLACEHOLDER"},
		{"GetModelInvocationJob", "GET", "/model-invocation-job/PLACEHOLDER"},
		{"GetModelInvocationLoggingConfiguration", "GET", "/logging/modelinvocations"},
		{"GetPromptRouter", "GET", "/prompt-routers/PLACEHOLDER"},
		{"GetProvisionedModelThroughput", "GET", "/provisioned-model-throughput/PLACEHOLDER"},
		{"GetResourcePolicy", "GET", "/resource-policy/PLACEHOLDER"},
		{"GetUseCaseForModelAccess", "GET", "/use-case-for-model-access"},
		{"ListAdvancedPromptOptimizationJobs", "GET", "/advanced-prompt-optimization-jobs"},
		{"ListAutomatedReasoningPolicies", "GET", "/automated-reasoning-policies"},
		{
			"ListAutomatedReasoningPolicyBuildWorkflows",
			"GET",
			"/automated-reasoning-policies/PLACEHOLDER/build-workflows",
		},
		{"ListAutomatedReasoningPolicyTestCases", "GET", "/automated-reasoning-policies/PLACEHOLDER/test-cases"},
		{
			"ListAutomatedReasoningPolicyTestResults",
			"GET",
			"/automated-reasoning-policies/PLACEHOLDER/build-workflows/PLACEHOLDER/test-results",
		},
		{"ListCustomModelDeployments", "GET", "/model-customization/custom-model-deployments"},
		{"ListCustomModels", "GET", "/custom-models"},
		{"ListEnforcedGuardrailsConfiguration", "GET", "/enforcedGuardrailsConfiguration"},
		{"ListEvaluationJobs", "GET", "/evaluation-jobs"},
		{"ListFoundationModelAgreementOffers", "GET", "/list-foundation-model-agreement-offers/PLACEHOLDER"},
		{"ListFoundationModels", "GET", "/foundation-models"},
		{"ListGuardrails", "GET", "/guardrails"},
		{"ListImportedModels", "GET", "/imported-models"},
		{"ListInferenceProfiles", "GET", "/inference-profiles"},
		{"ListMarketplaceModelEndpoints", "GET", "/marketplace-model/endpoints"},
		{"ListModelCopyJobs", "GET", "/model-copy-jobs"},
		{"ListModelCustomizationJobs", "GET", "/model-customization-jobs"},
		{"ListModelImportJobs", "GET", "/model-import-jobs"},
		{"ListModelInvocationJobs", "GET", "/model-invocation-jobs"},
		{"ListPromptRouters", "GET", "/prompt-routers"},
		{"ListProvisionedModelThroughputs", "GET", "/provisioned-model-throughputs"},
		{"ListTagsForResource", "POST", "/listTagsForResource"},
		{"PutAccountDataRetention", "PUT", "/data-retention"},
		{"PutEnforcedGuardrailConfiguration", "PUT", "/enforcedGuardrailsConfiguration"},
		{"PutModelInvocationLoggingConfiguration", "PUT", "/logging/modelinvocations"},
		{"PutResourcePolicy", "POST", "/resource-policy"},
		{"PutUseCaseForModelAccess", "POST", "/use-case-for-model-access"},
		{"RegisterMarketplaceModelEndpoint", "POST", "/marketplace-model/endpoints/PLACEHOLDER/registration"},
		{
			"StartAutomatedReasoningPolicyBuildWorkflow",
			"POST",
			"/automated-reasoning-policies/PLACEHOLDER/build-workflows/PLACEHOLDER/start",
		},
		{
			"StartAutomatedReasoningPolicyTestWorkflow",
			"POST",
			"/automated-reasoning-policies/PLACEHOLDER/build-workflows/PLACEHOLDER/test-workflows",
		},
		{"StopAdvancedPromptOptimizationJob", "POST", "/advanced-prompt-optimization-jobs/PLACEHOLDER/stop"},
		{"StopEvaluationJob", "POST", "/evaluation-job/PLACEHOLDER/stop"},
		{"StopModelCustomizationJob", "POST", "/model-customization-jobs/PLACEHOLDER/stop"},
		{"StopModelInvocationJob", "POST", "/model-invocation-job/PLACEHOLDER/stop"},
		{"TagResource", "POST", "/tagResource"},
		{"UntagResource", "POST", "/untagResource"},
		{"UpdateAutomatedReasoningPolicy", "PATCH", "/automated-reasoning-policies/PLACEHOLDER"},
		{
			"UpdateAutomatedReasoningPolicyAnnotations",
			"PATCH",
			"/automated-reasoning-policies/PLACEHOLDER/build-workflows/PLACEHOLDER/annotations",
		},
		{
			"UpdateAutomatedReasoningPolicyTestCase",
			"PATCH",
			"/automated-reasoning-policies/PLACEHOLDER/test-cases/PLACEHOLDER",
		},
		{"UpdateCustomModelDeployment", "PATCH", "/model-customization/custom-model-deployments/PLACEHOLDER"},
		{"UpdateGuardrail", "PUT", "/guardrails/PLACEHOLDER"},
		{"UpdateMarketplaceModelEndpoint", "PATCH", "/marketplace-model/endpoints/PLACEHOLDER"},
		{"UpdateProvisionedModelThroughput", "PATCH", "/provisioned-model-throughput/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real core-Bedrock op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts the route table resolves it to the right op, then drives the same
// request through the real Handler() and asserts it did not fall through to
// the "UnknownOperationException" errType that handler.go's dispatch default
// case emits (handler.go:537-540) -- guarding against an op name that
// resolves correctly but has no matching case anywhere in the dispatch tree
// (gopherstack-ey26 class), not just an ExtractOperation mismatch.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h := bedrock.NewHandler(bedrock.NewInMemoryBackend("000000000000", "us-east-1"))

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "UnknownOperationException",
				"method=%s path=%s op=%s: dispatched to the unmatched-route handler", tc.method, tc.path, tc.op)
		})
	}
}
