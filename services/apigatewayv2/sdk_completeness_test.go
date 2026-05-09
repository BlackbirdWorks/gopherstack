package apigatewayv2_test

import (
	"testing"

	apigatewayv2sdk "github.com/aws/aws-sdk-go-v2/service/apigatewayv2"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// apigatewayv2 client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := apigatewayv2.NewInMemoryBackend()
	h := apigatewayv2.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &apigatewayv2sdk.Client{}, h.GetSupportedOperations(), []string{})
}
