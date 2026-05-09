package pinpoint_test

import (
	"testing"

	pinpointsdk "github.com/aws/aws-sdk-go-v2/service/pinpoint"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/pinpoint"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// pinpoint client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := pinpoint.NewInMemoryBackend("us-east-1", "000000000000")
	h := pinpoint.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &pinpointsdk.Client{}, h.GetSupportedOperations(), []string{})
}
