package appconfig_test

import (
	"testing"

	appconfigsdk "github.com/aws/aws-sdk-go-v2/service/appconfig"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// appconfig client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := appconfig.NewInMemoryBackend("000000000000", "us-east-1")
	h := appconfig.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &appconfigsdk.Client{}, h.GetSupportedOperations(), []string{})
}
