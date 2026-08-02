package mgn_test

import (
	"testing"

	mgnsdk "github.com/aws/aws-sdk-go-v2/service/mgn"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/mgn"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK
// v2 mgn client is routed by GetSupportedOperations(). The test fails when
// the upstream SDK adds a new operation gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := mgn.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	t.Cleanup(backend.Close)

	h := mgn.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &mgnsdk.Client{}, h.GetSupportedOperations(), []string{})
}
