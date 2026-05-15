package elbv2_test

import (
	"testing"

	elbv2sdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// elasticloadbalancingv2 client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := elbv2.NewInMemoryBackend("000000000000", "us-east-1")
	h := elbv2.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &elbv2sdk.Client{}, h.GetSupportedOperations(), []string{})
}
