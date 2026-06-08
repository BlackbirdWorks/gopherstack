package mediapackage_test

import (
	"testing"

	mediapackagestk "github.com/aws/aws-sdk-go-v2/service/mediapackage"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/mediapackage"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// mediapackage client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := mediapackage.NewInMemoryBackend("000000000000", "us-east-1")
	h := mediapackage.NewHandler(backend)

	notImplemented := []string{}

	sdkcheck.CheckCompleteness(t, &mediapackagestk.Client{}, h.GetSupportedOperations(), notImplemented)
}
