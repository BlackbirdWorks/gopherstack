package account_test

import (
	"testing"

	accountsdk "github.com/aws/aws-sdk-go-v2/service/account"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/account"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// account client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := account.NewInMemoryBackend("000000000000", "us-east-1")
	h := account.NewHandler(backend)

	notImplemented := []string{}

	sdkcheck.CheckCompleteness(t, &accountsdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
