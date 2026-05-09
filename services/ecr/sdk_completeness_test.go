package ecr_test

import (
	"testing"

	ecrsdk "github.com/aws/aws-sdk-go-v2/service/ecr"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/ecr"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// ecr client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := ecr.NewInMemoryBackend("000000000000", "us-east-1", "")
	h := ecr.NewHandler(backend, nil)
	sdkcheck.CheckCompleteness(t, &ecrsdk.Client{}, h.GetSupportedOperations(), []string{})
}
