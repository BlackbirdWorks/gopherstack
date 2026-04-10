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
		// Durable execution (Lambda Workflows) — only checkpoint is implemented; others remain stubs.
		"GetDurableExecution",
		"GetDurableExecutionHistory",
		"GetDurableExecutionState",
		"ListDurableExecutionsByFunction",
		"SendDurableExecutionCallbackFailure",
		"SendDurableExecutionCallbackHeartbeat",
		"SendDurableExecutionCallbackSuccess",
		"StopDurableExecution",
		// Capacity providers — listing by capacity provider not yet implemented.
		"ListFunctionVersionsByCapacityProvider",
		// Invoke — SDK exposes "Invoke"; gopherstack registers it as "InvokeFunction".
		"Invoke",
		"InvokeAsync",
		"InvokeWithResponseStream",
		// Miscellaneous operations not yet implemented.
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
