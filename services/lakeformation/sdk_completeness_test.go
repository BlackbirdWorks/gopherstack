package lakeformation_test

import (
	"testing"

	lakeformationsdk "github.com/aws/aws-sdk-go-v2/service/lakeformation"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/lakeformation"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// lakeformation client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &lakeformationsdk.Client{}, h.GetSupportedOperations(), []string{})
}
