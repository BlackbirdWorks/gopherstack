package resourcegroupstaggingapi_test

import (
	"testing"

	resourcegroupstaggingapisdk "github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// resourcegroupstaggingapi client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := resourcegroupstaggingapi.NewInMemoryBackend("000000000000", "us-east-1")
	h := resourcegroupstaggingapi.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &resourcegroupstaggingapisdk.Client{}, h.GetSupportedOperations(), []string{})
}
