package kms_test

import (
	"testing"

	kmssdk "github.com/aws/aws-sdk-go-v2/service/kms"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/kms"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// kms client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &kmssdk.Client{}, h.GetSupportedOperations(), []string{})
}
