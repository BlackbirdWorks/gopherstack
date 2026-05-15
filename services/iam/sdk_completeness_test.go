package iam_test

import (
	"testing"

	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/iam"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// iam client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := iam.NewInMemoryBackend()
	h := iam.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &iamsdk.Client{}, h.GetSupportedOperations(), []string{})
}
