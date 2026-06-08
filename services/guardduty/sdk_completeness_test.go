package guardduty_test

import (
	"testing"

	guarddutysdk "github.com/aws/aws-sdk-go-v2/service/guardduty"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/guardduty"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// guardduty client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := guardduty.NewInMemoryBackend("000000000000", "us-east-1")
	h := guardduty.NewHandler(backend)

	notImplemented := []string{}

	sdkcheck.CheckCompleteness(t, &guarddutysdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
