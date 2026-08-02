package directconnect_test

import (
	"testing"

	directconnectsdk "github.com/aws/aws-sdk-go-v2/service/directconnect"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/directconnect"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK
// v2 directconnect client is routed by GetSupportedOperations(). The test
// fails when the upstream SDK adds a new operation gopherstack has not yet
// handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := directconnect.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	t.Cleanup(backend.Close)

	h := directconnect.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &directconnectsdk.Client{}, h.GetSupportedOperations(), []string{})
}
