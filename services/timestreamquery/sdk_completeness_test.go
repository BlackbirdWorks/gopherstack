package timestreamquery_test

import (
	"testing"

	timestreamquerysdk "github.com/aws/aws-sdk-go-v2/service/timestreamquery"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/timestreamquery"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// timestreamquery client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := timestreamquery.NewInMemoryBackend("000000000000", "us-east-1")
	h := timestreamquery.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &timestreamquerysdk.Client{}, h.GetSupportedOperations(), []string{})
}
