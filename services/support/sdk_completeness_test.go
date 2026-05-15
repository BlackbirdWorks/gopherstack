package support_test

import (
	"testing"

	supportsdk "github.com/aws/aws-sdk-go-v2/service/support"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/support"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// support client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := support.NewInMemoryBackend()
	h := support.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &supportsdk.Client{}, h.GetSupportedOperations(), []string{})
}
