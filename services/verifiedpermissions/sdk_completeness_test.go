package verifiedpermissions_test

import (
	"testing"

	verifiedpermissionssdk "github.com/aws/aws-sdk-go-v2/service/verifiedpermissions"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/verifiedpermissions"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// verifiedpermissions client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := verifiedpermissions.NewInMemoryBackend("000000000000", "us-east-1")
	h := verifiedpermissions.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &verifiedpermissionssdk.Client{}, h.GetSupportedOperations(), []string{})
}
