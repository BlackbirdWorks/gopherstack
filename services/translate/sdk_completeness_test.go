package translate_test

import (
	"testing"

	translatestk "github.com/aws/aws-sdk-go-v2/service/translate"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/translate"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// translate client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := translate.NewInMemoryBackend("000000000000", "us-east-1")
	h := translate.NewHandler(backend)

	notImplemented := []string{}

	sdkcheck.CheckCompleteness(t, &translatestk.Client{}, h.GetSupportedOperations(), notImplemented)
}
