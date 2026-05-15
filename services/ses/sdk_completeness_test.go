package ses_test

import (
	"testing"

	sessdk "github.com/aws/aws-sdk-go-v2/service/ses"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/ses"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// ses client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := ses.NewInMemoryBackend()
	h := ses.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &sessdk.Client{}, h.GetSupportedOperations(), []string{})
}
