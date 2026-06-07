package accessanalyzer_test

import (
	"testing"

	aasdk "github.com/aws/aws-sdk-go-v2/service/accessanalyzer"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// accessanalyzer client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
	h := accessanalyzer.NewHandler(backend)

	notImplemented := []string{}

	sdkcheck.CheckCompleteness(t, &aasdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
