package applicationautoscaling_test

import (
	"testing"

	applicationautoscalingsdk "github.com/aws/aws-sdk-go-v2/service/applicationautoscaling"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/applicationautoscaling"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// applicationautoscaling client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := applicationautoscaling.NewInMemoryBackend("000000000000", "us-east-1")
	h := applicationautoscaling.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &applicationautoscalingsdk.Client{}, h.GetSupportedOperations(), []string{})
}
