package emr_test

import (
	"testing"

	emrsdk "github.com/aws/aws-sdk-go-v2/service/emr"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/emr"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// emr client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := emr.NewInMemoryBackend("000000000000", "us-east-1")
	h := emr.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &emrsdk.Client{}, h.GetSupportedOperations(), []string{})
}
