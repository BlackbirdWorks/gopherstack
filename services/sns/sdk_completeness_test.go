package sns_test

import (
	"testing"

	snssdk "github.com/aws/aws-sdk-go-v2/service/sns"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/sns"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// sns client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := sns.NewInMemoryBackend()
	h := sns.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &snssdk.Client{}, h.GetSupportedOperations(), []string{})
}
