package s3tables_test

import (
	"testing"

	s3tablessdk "github.com/aws/aws-sdk-go-v2/service/s3tables"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/s3tables"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// s3tables client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := s3tables.NewInMemoryBackend("000000000000", "us-east-1")
	h := s3tables.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &s3tablessdk.Client{}, h.GetSupportedOperations(), []string{})
}
