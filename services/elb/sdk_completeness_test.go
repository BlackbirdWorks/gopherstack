package elb_test

import (
	"testing"

	elbsdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/elb"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// elasticloadbalancing client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := elb.NewInMemoryBackend("000000000000", "us-east-1")
	h := elb.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &elbsdk.Client{}, h.GetSupportedOperations(), []string{})
}
