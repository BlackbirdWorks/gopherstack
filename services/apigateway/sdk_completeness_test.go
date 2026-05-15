package apigateway_test

import (
	"testing"

	apigatewaysdk "github.com/aws/aws-sdk-go-v2/service/apigateway"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// apigateway client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &apigatewaysdk.Client{}, h.GetSupportedOperations(), []string{})
}
