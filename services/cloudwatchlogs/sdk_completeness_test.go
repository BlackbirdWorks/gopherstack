package cloudwatchlogs_test

import (
	"testing"

	cloudwatchlogssdk "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// cloudwatchlogs client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	h := cloudwatchlogs.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &cloudwatchlogssdk.Client{}, h.GetSupportedOperations(), []string{})
}
