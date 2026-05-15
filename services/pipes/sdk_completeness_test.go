package pipes_test

import (
	"testing"

	pipessdk "github.com/aws/aws-sdk-go-v2/service/pipes"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/pipes"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// pipes client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := pipes.NewInMemoryBackend("000000000000", "us-east-1")
	h := pipes.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &pipessdk.Client{}, h.GetSupportedOperations(), nil)
}
