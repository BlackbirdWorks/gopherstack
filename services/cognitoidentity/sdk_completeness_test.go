package cognitoidentity_test

import (
	"testing"

	cognitoidentitysdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentity"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/cognitoidentity"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// cognitoidentity client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")
	h := cognitoidentity.NewHandler(backend, "us-east-1")
	sdkcheck.CheckCompleteness(t, &cognitoidentitysdk.Client{}, h.GetSupportedOperations(), nil)
}
