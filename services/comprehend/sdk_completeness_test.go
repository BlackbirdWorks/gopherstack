package comprehend_test

import (
	"testing"

	comprehendsdk "github.com/aws/aws-sdk-go-v2/service/comprehend"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/comprehend"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// comprehend client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := comprehend.NewInMemoryBackend("000000000000", "us-east-1")
	h := comprehend.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &comprehendsdk.Client{}, h.GetSupportedOperations(), []string{})
}
