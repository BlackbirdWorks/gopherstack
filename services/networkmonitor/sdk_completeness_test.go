package networkmonitor_test

import (
	"testing"

	networkmonitorsdk "github.com/aws/aws-sdk-go-v2/service/networkmonitor"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/networkmonitor"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// networkmonitor client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice. The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := networkmonitor.NewInMemoryBackend("us-east-1", "000000000000")
	h := networkmonitor.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &networkmonitorsdk.Client{}, h.GetSupportedOperations(), []string{})
}
