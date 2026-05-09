package ec2_test

import (
	"testing"

	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// ec2 client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &ec2sdk.Client{}, h.GetSupportedOperations(), []string{
		// All operations are now implemented (stubs or full handlers).
		// Add future unimplemented ops here if the SDK adds new ones.
	})
}
