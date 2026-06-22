package sts_test

import (
	"testing"

	stssdk "github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/sts"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// sts client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackend()
	h := sts.NewHandler(backend)
	// GetWebIdentityToken is an internal gopherstack extension, not a real AWS STS API action.
	notImplemented := []string{"GetWebIdentityToken"}
	sdkcheck.CheckCompleteness(t, &stssdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
