package serverlessrepo_test

import (
	"testing"

	serverlessreposdk "github.com/aws/aws-sdk-go-v2/service/serverlessapplicationrepository"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/serverlessrepo"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// serverlessapplicationrepository client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := serverlessrepo.NewInMemoryBackend("000000000000", "us-east-1")
	h := serverlessrepo.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &serverlessreposdk.Client{}, h.GetSupportedOperations(), []string{})
}
