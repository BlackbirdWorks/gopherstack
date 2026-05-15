package codecommit_test

import (
	"testing"

	codecommitsdk "github.com/aws/aws-sdk-go-v2/service/codecommit"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/codecommit"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// codecommit client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := codecommit.NewInMemoryBackend("000000000000", "us-east-1")
	h := codecommit.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &codecommitsdk.Client{}, h.GetSupportedOperations(), []string{})
}
