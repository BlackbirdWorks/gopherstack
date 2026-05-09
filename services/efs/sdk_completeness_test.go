package efs_test

import (
	"testing"

	efssdk "github.com/aws/aws-sdk-go-v2/service/efs"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/efs"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// efs client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := efs.NewInMemoryBackend("000000000000", "us-east-1")
	h := efs.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &efssdk.Client{}, h.GetSupportedOperations(), []string{})
}
