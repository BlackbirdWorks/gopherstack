package mediastore_test

import (
	"testing"

	mediastoresdk "github.com/aws/aws-sdk-go-v2/service/mediastore"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/mediastore"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// mediastore client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := mediastore.NewInMemoryBackend()
	h := mediastore.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &mediastoresdk.Client{}, h.GetSupportedOperations(), nil)
}
