package firehose_test

import (
	"testing"

	firehosesdk "github.com/aws/aws-sdk-go-v2/service/firehose"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/firehose"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// firehose client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	h := firehose.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &firehosesdk.Client{}, h.GetSupportedOperations(), []string{})
}
