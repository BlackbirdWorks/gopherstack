package mediastoredata_test

import (
	"testing"

	mediastoredatasdk "github.com/aws/aws-sdk-go-v2/service/mediastoredata"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/mediastoredata"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// mediastoredata client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := mediastoredata.NewInMemoryBackend("us-east-1")
	h := mediastoredata.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &mediastoredatasdk.Client{}, h.GetSupportedOperations(), nil)
}
