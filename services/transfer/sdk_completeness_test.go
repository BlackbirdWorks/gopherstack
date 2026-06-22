package transfer_test

import (
	"testing"

	transfersdk "github.com/aws/aws-sdk-go-v2/service/transfer"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/transfer"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// transfer client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	h := transfer.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &transfersdk.Client{}, h.GetSupportedOperations(), []string{})
}
