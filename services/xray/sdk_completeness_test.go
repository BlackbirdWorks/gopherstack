package xray_test

import (
	"testing"

	xraysdk "github.com/aws/aws-sdk-go-v2/service/xray"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/xray"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// xray client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := xray.NewInMemoryBackend()
	h := xray.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &xraysdk.Client{}, h.GetSupportedOperations(), []string{})
}
