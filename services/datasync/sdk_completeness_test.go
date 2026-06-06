package datasync_test

import (
	"testing"

	datasyncsdk "github.com/aws/aws-sdk-go-v2/service/datasync"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/datasync"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// datasync client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := datasync.NewInMemoryBackend("000000000000", "us-east-1")
	h := datasync.NewHandler(backend)

	// Operations not yet implemented.
	notImplemented := []string{}

	sdkcheck.CheckCompleteness(t, &datasyncsdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
