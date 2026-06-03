package waf_test

import (
	"testing"

	wafsdk "github.com/aws/aws-sdk-go-v2/service/waf"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/waf"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// waf client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := waf.NewInMemoryBackend("000000000000", "us-east-1")
	h := waf.NewHandler(backend)

	// All operations are now implemented.
	notImplemented := []string{}

	sdkcheck.CheckCompleteness(t, &wafsdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
