package appstream_test

import (
	"testing"

	appstreamsdk "github.com/aws/aws-sdk-go-v2/service/appstream"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/appstream"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// appstream client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := appstream.NewInMemoryBackend("000000000000", "us-east-1")
	h := appstream.NewHandler(backend)

	notImplemented := []string{}

	sdkcheck.CheckCompleteness(t, &appstreamsdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
