package swf_test

import (
	"testing"

	swfsdk "github.com/aws/aws-sdk-go-v2/service/swf"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/swf"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// swf client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := swf.NewInMemoryBackend()
	h := swf.NewHandler(backend)
	notImplemented := []string{
		"DeleteActivityType",
		"DeleteWorkflowType",
	}
	sdkcheck.CheckCompleteness(t, &swfsdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
