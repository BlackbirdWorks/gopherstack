package servicediscovery_test

import (
	"testing"

	servicediscoverysdk "github.com/aws/aws-sdk-go-v2/service/servicediscovery"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/servicediscovery"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// servicediscovery client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := servicediscovery.NewInMemoryBackend("000000000000", "us-east-1")
	h := servicediscovery.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &servicediscoverysdk.Client{}, h.GetSupportedOperations(), []string{})
}
