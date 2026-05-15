package appconfigdata_test

import (
	"testing"

	appconfigdatasdk "github.com/aws/aws-sdk-go-v2/service/appconfigdata"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/appconfigdata"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// appconfigdata client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := appconfigdata.NewInMemoryBackend()
	h := appconfigdata.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &appconfigdatasdk.Client{}, h.GetSupportedOperations(), nil)
}
