package ssm_test

import (
	"testing"

	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// ssm client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	h := ssm.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &ssmsdk.Client{}, h.GetSupportedOperations(), []string{})
}
