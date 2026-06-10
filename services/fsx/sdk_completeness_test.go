package fsx_test

import (
	"testing"

	fsxsdk "github.com/aws/aws-sdk-go-v2/service/fsx"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/fsx"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// fsx client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := fsx.NewInMemoryBackend("000000000000", "us-east-1")
	h := fsx.NewHandler(backend)

	notImplemented := []string{}

	sdkcheck.CheckCompleteness(t, &fsxsdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
