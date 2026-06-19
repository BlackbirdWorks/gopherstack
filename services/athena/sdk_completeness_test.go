package athena_test

import (
	"testing"

	athenasdk "github.com/aws/aws-sdk-go-v2/service/athena"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/athena"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// athena client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := athena.NewInMemoryBackend("", "")
	h := athena.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &athenasdk.Client{}, h.GetSupportedOperations(), []string{})
}
