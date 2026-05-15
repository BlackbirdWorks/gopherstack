package bedrock_test

import (
	"testing"

	bedrocksdk "github.com/aws/aws-sdk-go-v2/service/bedrock"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// bedrock client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &bedrocksdk.Client{}, h.GetSupportedOperations(), []string{})
}
