package appsync_test

import (
	"testing"

	appsyncsdk "github.com/aws/aws-sdk-go-v2/service/appsync"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/appsync"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// appsync client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := appsync.NewInMemoryBackend("000000000000", "us-east-1", "")
	h := appsync.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &appsyncsdk.Client{}, h.GetSupportedOperations(), []string{})
}
