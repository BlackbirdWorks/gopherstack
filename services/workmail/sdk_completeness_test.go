package workmail_test

import (
	"testing"

	workmailsdk "github.com/aws/aws-sdk-go-v2/service/workmail"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/workmail"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// workmail client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := workmail.NewInMemoryBackend("000000000000", "us-east-1")
	h := workmail.NewHandler(backend)

	notImplemented := []string{}

	sdkcheck.CheckCompleteness(t, &workmailsdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
