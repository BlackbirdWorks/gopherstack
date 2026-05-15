package codebuild_test

import (
	"testing"

	codebuildsdk "github.com/aws/aws-sdk-go-v2/service/codebuild"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/codebuild"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// codebuild client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := codebuild.NewInMemoryBackend("000000000000", "us-east-1")
	h := codebuild.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &codebuildsdk.Client{}, h.GetSupportedOperations(), []string{})
}
