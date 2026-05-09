package timestreamwrite_test

import (
	"testing"

	timestreamwritesdk "github.com/aws/aws-sdk-go-v2/service/timestreamwrite"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/timestreamwrite"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// timestreamwrite client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := timestreamwrite.NewInMemoryBackend()
	h := timestreamwrite.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &timestreamwritesdk.Client{}, h.GetSupportedOperations(), []string{})
}
