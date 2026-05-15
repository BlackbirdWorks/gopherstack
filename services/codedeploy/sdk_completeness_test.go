package codedeploy_test

import (
	"testing"

	codedeploysdk "github.com/aws/aws-sdk-go-v2/service/codedeploy"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/codedeploy"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// codedeploy client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := codedeploy.NewInMemoryBackend("000000000000", "us-east-1")
	h := codedeploy.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &codedeploysdk.Client{}, h.GetSupportedOperations(), []string{})
}
