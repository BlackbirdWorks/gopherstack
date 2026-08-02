package outposts_test

import (
	"testing"

	outpostssdk "github.com/aws/aws-sdk-go-v2/service/outposts"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/outposts"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// outposts client is routed by GetSupportedOperations(). The test fails when
// the upstream SDK adds a new operation gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := outposts.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	h := outposts.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &outpostssdk.Client{}, h.GetSupportedOperations(), []string{})
}
