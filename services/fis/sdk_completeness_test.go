package fis_test

import (
	"testing"

	fissdk "github.com/aws/aws-sdk-go-v2/service/fis"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/fis"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// fis client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := fis.NewInMemoryBackend("000000000000", "us-east-1")
	h := fis.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &fissdk.Client{}, h.GetSupportedOperations(), []string{})
}
