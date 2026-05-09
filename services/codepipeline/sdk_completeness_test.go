package codepipeline_test

import (
	"testing"

	codepipelinesdk "github.com/aws/aws-sdk-go-v2/service/codepipeline"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/codepipeline"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// codepipeline client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
	h := codepipeline.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &codepipelinesdk.Client{}, h.GetSupportedOperations(), []string{})
}
