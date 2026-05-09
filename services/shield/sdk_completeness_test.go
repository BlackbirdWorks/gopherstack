package shield_test

import (
	"testing"

	shieldsdk "github.com/aws/aws-sdk-go-v2/service/shield"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/shield"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// shield client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := shield.NewInMemoryBackend("000000000000", "us-east-1")
	h := shield.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &shieldsdk.Client{}, h.GetSupportedOperations(), []string{})
}
