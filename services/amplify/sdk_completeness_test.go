package amplify_test

import (
	"testing"

	amplifysdk "github.com/aws/aws-sdk-go-v2/service/amplify"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/amplify"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// amplify client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := amplify.NewInMemoryBackend("000000000000", "us-east-1")
	h := amplify.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &amplifysdk.Client{}, h.GetSupportedOperations(), []string{})
}
