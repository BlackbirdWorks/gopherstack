package cloudcontrol_test

import (
	"testing"

	cloudcontrolsdk "github.com/aws/aws-sdk-go-v2/service/cloudcontrol"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/cloudcontrol"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// cloudcontrol client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := cloudcontrol.NewInMemoryBackend("000000000000", "us-east-1")
	h := cloudcontrol.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &cloudcontrolsdk.Client{}, h.GetSupportedOperations(), []string{})
}
