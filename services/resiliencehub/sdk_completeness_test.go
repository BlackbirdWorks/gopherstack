package resiliencehub_test

import (
	"testing"

	resiliencehubsdk "github.com/aws/aws-sdk-go-v2/service/resiliencehub"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/resiliencehub"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// resiliencehub client is routed by GetSupportedOperations(). The test fails
// when the upstream SDK adds a new operation gopherstack has not yet
// handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := resiliencehub.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	t.Cleanup(backend.Close)

	h := resiliencehub.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &resiliencehubsdk.Client{}, h.GetSupportedOperations(), []string{})
}
