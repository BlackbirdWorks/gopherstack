package omics_test

import (
	"testing"

	omicssdk "github.com/aws/aws-sdk-go-v2/service/omics"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/omics"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// omics client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", "us-east-1")
	h := omics.NewHandler(backend)

	notImplemented := []string{}

	sdkcheck.CheckCompleteness(t, &omicssdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
