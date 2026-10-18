package marketplaceentitlementservice_test

import (
	"testing"

	sdk "github.com/aws/aws-sdk-go-v2/service/marketplaceentitlementservice"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	ent "github.com/blackbirdworks/gopherstack/services/marketplaceentitlementservice"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := ent.NewHandler(ent.NewInMemoryBackend(testAccount, "us-east-1"))
	sdkcheck.CheckCompleteness(t, &sdk.Client{}, h.GetSupportedOperations(), []string{})
}
