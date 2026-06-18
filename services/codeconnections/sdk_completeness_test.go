package codeconnections_test

import (
	"testing"

	codeconnectionssdk "github.com/aws/aws-sdk-go-v2/service/codeconnections"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/codeconnections"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// codeconnections client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := codeconnections.NewInMemoryBackend("000000000000", "us-east-1")
	h := codeconnections.NewHandler(backend)
	sdkcheck.CheckCompleteness(
		t,
		&codeconnectionssdk.Client{},
		h.GetSupportedOperations(),
		[]string{},
	)
}
