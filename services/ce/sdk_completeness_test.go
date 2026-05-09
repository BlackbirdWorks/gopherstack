package ce_test

import (
	"testing"

	cesdk "github.com/aws/aws-sdk-go-v2/service/costexplorer"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/ce"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// costexplorer client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := ce.NewInMemoryBackend("000000000000", "us-east-1")
	h := ce.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &cesdk.Client{}, h.GetSupportedOperations(), []string{})
}
