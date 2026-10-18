package marketplacemetering_test

import (
	"testing"

	sdk "github.com/aws/aws-sdk-go-v2/service/marketplacemetering"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	mm "github.com/blackbirdworks/gopherstack/services/marketplacemetering"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := mm.NewHandler(mm.NewInMemoryBackend(testAccount, "us-east-1"))
	sdkcheck.CheckCompleteness(t, &sdk.Client{}, h.GetSupportedOperations(), []string{})
}
