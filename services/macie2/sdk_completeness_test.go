package macie2_test

import (
	"testing"

	macie2sdk "github.com/aws/aws-sdk-go-v2/service/macie2"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/macie2"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// macie2 client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := macie2.NewInMemoryBackend("000000000000", "us-east-1")
	h := macie2.NewHandler(backend)

	notImplemented := []string{}

	sdkcheck.CheckCompleteness(t, &macie2sdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
