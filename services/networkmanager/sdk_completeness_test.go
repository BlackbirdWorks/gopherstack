package networkmanager_test

import (
	"testing"

	networkmanagersdk "github.com/aws/aws-sdk-go-v2/service/networkmanager"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/networkmanager"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK
// v2 networkmanager client is routed by GetSupportedOperations(). The test
// fails when the upstream SDK adds a new operation gopherstack has not yet
// handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := networkmanager.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	t.Cleanup(backend.Close)

	h := networkmanager.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &networkmanagersdk.Client{}, h.GetSupportedOperations(), []string{})
}
