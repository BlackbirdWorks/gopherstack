package redshiftdata_test

import (
	"testing"

	redshiftdatasdk "github.com/aws/aws-sdk-go-v2/service/redshiftdata"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/redshiftdata"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// redshiftdata client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := redshiftdata.NewInMemoryBackend("000000000000", "us-east-1")
	h := redshiftdata.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &redshiftdatasdk.Client{}, h.GetSupportedOperations(), []string{})
}
