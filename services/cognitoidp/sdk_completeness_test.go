package cognitoidp_test

import (
	"testing"

	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// cognitoidentityprovider client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := cognitoidp.NewInMemoryBackend("000000000000", "us-east-1", "")
	h := cognitoidp.NewHandler(backend, "us-east-1")
	sdkcheck.CheckCompleteness(t, &cognitoidpsdk.Client{}, h.GetSupportedOperations(), []string{})
}
