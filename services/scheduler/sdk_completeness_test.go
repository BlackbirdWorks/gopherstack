package scheduler_test

import (
	"testing"

	schedulersdk "github.com/aws/aws-sdk-go-v2/service/scheduler"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/scheduler"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// scheduler client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
	h := scheduler.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &schedulersdk.Client{}, h.GetSupportedOperations(), []string{})
}
