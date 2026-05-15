package rdsdata_test

import (
	"testing"

	rdsdatasdk "github.com/aws/aws-sdk-go-v2/service/rdsdata"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/rdsdata"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// rdsdata client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	h := rdsdata.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &rdsdatasdk.Client{}, h.GetSupportedOperations(), []string{})
}
