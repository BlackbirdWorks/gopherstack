package cloudformation_test

import (
	"testing"

	cloudformationsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// cloudformation client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := cloudformation.NewInMemoryBackend()
	h := cloudformation.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &cloudformationsdk.Client{}, h.GetSupportedOperations(), []string{})
}
