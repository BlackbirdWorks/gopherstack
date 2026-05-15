package ram_test

import (
	"testing"

	ramsdk "github.com/aws/aws-sdk-go-v2/service/ram"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/ram"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// ram client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := ram.NewInMemoryBackend("000000000000", "us-east-1")
	h := ram.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &ramsdk.Client{}, h.GetSupportedOperations(), []string{})
}
