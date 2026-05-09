package neptune_test

import (
	"testing"

	neptunesdk "github.com/aws/aws-sdk-go-v2/service/neptune"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/neptune"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// neptune client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	h := neptune.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &neptunesdk.Client{}, h.GetSupportedOperations(), []string{})
}
