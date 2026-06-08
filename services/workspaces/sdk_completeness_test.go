package workspaces_test

import (
	"testing"

	workspacessdk "github.com/aws/aws-sdk-go-v2/service/workspaces"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/workspaces"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// workspaces client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := workspaces.NewInMemoryBackend("000000000000", "us-east-1")
	h := workspaces.NewHandler(backend)

	// All 91 WorkSpaces operations are now implemented.
	notImplemented := []string{}

	sdkcheck.CheckCompleteness(t, &workspacessdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
