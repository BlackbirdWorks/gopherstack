package s3_test

import (
	"testing"

	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/s3"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// s3 client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := s3.NewInMemoryBackend(nil)
	h := s3.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &s3sdk.Client{}, h.GetSupportedOperations(), []string{})
}
