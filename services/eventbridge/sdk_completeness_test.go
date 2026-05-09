package eventbridge_test

import (
	"testing"

	eventbridgesdk "github.com/aws/aws-sdk-go-v2/service/eventbridge"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// eventbridge client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := eventbridge.NewInMemoryBackend()
	h := eventbridge.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &eventbridgesdk.Client{}, h.GetSupportedOperations(), []string{})
}
