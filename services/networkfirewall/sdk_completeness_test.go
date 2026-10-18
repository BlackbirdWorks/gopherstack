package networkfirewall_test

import (
	"testing"

	sdk "github.com/aws/aws-sdk-go-v2/service/networkfirewall"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	nf "github.com/blackbirdworks/gopherstack/services/networkfirewall"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := nf.NewHandler(nf.NewInMemoryBackend(testAccount, testRegion))
	var notImplemented []string
	sdkcheck.CheckCompleteness(t, &sdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
