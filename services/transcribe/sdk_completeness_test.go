package transcribe_test

import (
	"testing"

	transcribesdk "github.com/aws/aws-sdk-go-v2/service/transcribe"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/transcribe"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// transcribe client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := transcribe.NewInMemoryBackend()
	h := transcribe.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &transcribesdk.Client{}, h.GetSupportedOperations(), []string{})
}
