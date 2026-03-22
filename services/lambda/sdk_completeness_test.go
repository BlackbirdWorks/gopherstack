package lambda_test

import (
	"testing"

	lambdasdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// lambda client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	// NewHandler accepts nil because GetSupportedOperations does not use the backend.
	h := lambda.NewHandler(nil)
	sdkcheck.CheckCompleteness(t, &lambdasdk.Client{}, h.GetSupportedOperations(), []string{
		// AddPermission — resource-based policy management not yet implemented.
		"AddPermission",
		// Durable execution (Lambda Workflows) — preview feature not yet implemented.
		"CheckpointDurableExecution",
		"GetDurableExecution",
		"GetDurableExecutionHistory",
		"GetDurableExecutionState",
		"ListDurableExecutionsByFunction",
		"SendDurableExecutionCallbackFailure",
		"SendDurableExecutionCallbackHeartbeat",
		"SendDurableExecutionCallbackSuccess",
		"StopDurableExecution",
		// Capacity providers — not yet implemented.
		"CreateCapacityProvider",
		"DeleteCapacityProvider",
		"GetCapacityProvider",
		"ListCapacityProviders",
		"ListFunctionVersionsByCapacityProvider",
		"UpdateCapacityProvider",
		// Code signing — not yet implemented.
		"CreateCodeSigningConfig",
		"DeleteCodeSigningConfig",
		"DeleteFunctionCodeSigningConfig",
		"GetCodeSigningConfig",
		"GetFunctionCodeSigningConfig",
		"ListCodeSigningConfigs",
		"ListFunctionsByCodeSigningConfig",
		"PutFunctionCodeSigningConfig",
		"UpdateCodeSigningConfig",
		// FunctionUrlConfig — SDK uses "Url" (not "URL"); gopherstack uses a different casing.
		"CreateFunctionUrlConfig",
		"DeleteFunctionUrlConfig",
		"GetFunctionUrlConfig",
		"ListFunctionUrlConfigs",
		"UpdateFunctionUrlConfig",
		// Invoke — SDK exposes "Invoke"; gopherstack registers it as "InvokeFunction".
		"Invoke",
		"InvokeAsync",
		"InvokeWithResponseStream",
		// Miscellaneous operations not yet implemented.
		"GetAccountSettings",
		"GetFunctionConfiguration",
		"GetFunctionRecursionConfig",
		"GetFunctionScalingConfig",
		"GetLayerVersionByArn",
		"GetPolicy",
		"GetRuntimeManagementConfig",
		"PutFunctionRecursionConfig",
		"PutFunctionScalingConfig",
		"PutRuntimeManagementConfig",
		"RemovePermission",
		"UpdateEventSourceMapping",
	})
}
