package directoryservice_test

import (
	"testing"

	dssdk "github.com/aws/aws-sdk-go-v2/service/directoryservice"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/directoryservice"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// directoryservice client is either listed in GetSupportedOperations() or
// explicitly acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := directoryservice.NewInMemoryBackend("000000000000", "us-east-1")
	h := directoryservice.NewHandler(backend)

	// All operations are now implemented.
	notImplemented := []string{}

	sdkcheck.CheckCompleteness(t, &dssdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
