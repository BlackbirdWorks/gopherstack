package identitystore_test

import (
	"testing"

	identitystoresdk "github.com/aws/aws-sdk-go-v2/service/identitystore"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/identitystore"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// identitystore client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := identitystore.NewInMemoryBackend("000000000000", "us-east-1")
	h := identitystore.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &identitystoresdk.Client{}, h.GetSupportedOperations(), nil)
}
