package dlm_test

import (
	"testing"

	dlmsdk "github.com/aws/aws-sdk-go-v2/service/dlm"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/dlm"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// dlm client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := dlm.NewInMemoryBackend("000000000000", "us-east-1")
	h := dlm.NewHandler(backend)

	// All 8 DLM operations are implemented.
	notImplemented := []string{}

	sdkcheck.CheckCompleteness(t, &dlmsdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
