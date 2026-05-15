package glacier_test

import (
	"testing"

	glaciersdk "github.com/aws/aws-sdk-go-v2/service/glacier"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/glacier"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// glacier client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := glacier.NewInMemoryBackend()
	h := glacier.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &glaciersdk.Client{}, h.GetSupportedOperations(), []string{})
}
