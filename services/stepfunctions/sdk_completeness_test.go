package stepfunctions_test

import (
	"testing"

	stepfunctionssdk "github.com/aws/aws-sdk-go-v2/service/sfn"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// sfn client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &stepfunctionssdk.Client{}, h.GetSupportedOperations(), []string{})
}
