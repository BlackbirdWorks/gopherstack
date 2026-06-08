package quicksight_test

import (
	"testing"

	quicksightsdk "github.com/aws/aws-sdk-go-v2/service/quicksight"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// quicksight client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := quicksight.NewInMemoryBackend("000000000000", "us-east-1")
	h := quicksight.NewHandler(backend)

	notImplemented := []string{}

	sdkcheck.CheckCompleteness(t, &quicksightsdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
