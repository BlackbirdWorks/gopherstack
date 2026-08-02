package lightsail_test

import (
	"testing"

	lightsailsdk "github.com/aws/aws-sdk-go-v2/service/lightsail"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/lightsail"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK
// v2 lightsail client is routed by GetSupportedOperations(). The test fails
// when the upstream SDK adds a new operation gopherstack has not yet
// handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := lightsail.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	t.Cleanup(backend.Close)

	h := lightsail.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &lightsailsdk.Client{}, h.GetSupportedOperations(), []string{})
}
