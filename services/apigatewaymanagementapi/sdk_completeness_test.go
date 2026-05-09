package apigatewaymanagementapi_test

import (
	"testing"

	apigatewaymanagementapisdk "github.com/aws/aws-sdk-go-v2/service/apigatewaymanagementapi"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/apigatewaymanagementapi"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// apigatewaymanagementapi client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := apigatewaymanagementapi.NewInMemoryBackend()
	h := apigatewaymanagementapi.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &apigatewaymanagementapisdk.Client{}, h.GetSupportedOperations(), nil)
}
