package mediaconvert_test

import (
	"testing"

	mediaconvertsdk "github.com/aws/aws-sdk-go-v2/service/mediaconvert"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/mediaconvert"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// mediaconvert client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := mediaconvert.NewInMemoryBackend("000000000000", "us-east-1")
	h := mediaconvert.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &mediaconvertsdk.Client{}, h.GetSupportedOperations(), nil)
}
