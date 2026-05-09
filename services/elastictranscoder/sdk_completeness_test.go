package elastictranscoder_test

import (
	"testing"

	elastictranscodersdk "github.com/aws/aws-sdk-go-v2/service/elastictranscoder" //nolint:staticcheck // AWS has deprecated this service; gopherstack still supports it

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/elastictranscoder"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// elastictranscoder client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := elastictranscoder.NewInMemoryBackend("000000000000", "us-east-1")
	h := elastictranscoder.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &elastictranscodersdk.Client{}, h.GetSupportedOperations(), []string{})
}
