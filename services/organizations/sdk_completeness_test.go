package organizations_test

import (
	"testing"

	organizationssdk "github.com/aws/aws-sdk-go-v2/service/organizations"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/organizations"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// organizations client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := organizations.NewInMemoryBackend("000000000000", "us-east-1")
	h := organizations.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &organizationssdk.Client{}, h.GetSupportedOperations(), []string{})
}
