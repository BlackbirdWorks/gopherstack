package cloudwatch_test

import (
	"testing"

	cloudwatchsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// cloudwatch client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := cloudwatch.NewInMemoryBackend()
	h := cloudwatch.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &cloudwatchsdk.Client{}, h.GetSupportedOperations(), []string{})
}
