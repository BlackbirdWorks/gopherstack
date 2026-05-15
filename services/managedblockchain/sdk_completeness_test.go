package managedblockchain_test

import (
	"testing"

	managedblockchainsdk "github.com/aws/aws-sdk-go-v2/service/managedblockchain"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/managedblockchain"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// managedblockchain client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := managedblockchain.NewInMemoryBackend()
	h := managedblockchain.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &managedblockchainsdk.Client{}, h.GetSupportedOperations(), []string{})
}
