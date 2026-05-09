package mwaa_test

import (
	"testing"

	mwaasdk "github.com/aws/aws-sdk-go-v2/service/mwaa"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/mwaa"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// mwaa client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := mwaa.NewInMemoryBackend("us-east-1", "000000000000")
	h := mwaa.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &mwaasdk.Client{}, h.GetSupportedOperations(), []string{})
}
