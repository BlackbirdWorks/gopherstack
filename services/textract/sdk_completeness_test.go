package textract_test

import (
	"testing"

	textractsdk "github.com/aws/aws-sdk-go-v2/service/textract"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/textract"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// textract client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := textract.NewInMemoryBackend("123456789012", "us-east-1")
	h := textract.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &textractsdk.Client{}, h.GetSupportedOperations(), []string{})
}
