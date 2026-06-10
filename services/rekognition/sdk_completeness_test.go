package rekognition_test

import (
	"testing"

	rekognitionsdk "github.com/aws/aws-sdk-go-v2/service/rekognition"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/rekognition"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// rekognition client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := rekognition.NewInMemoryBackend("000000000000", "us-east-1")
	h := rekognition.NewHandler(backend)

	notImplemented := []string{}

	sdkcheck.CheckCompleteness(t, &rekognitionsdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
