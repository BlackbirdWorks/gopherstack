package appintegrations_test

import (
	"testing"

	sdk "github.com/aws/aws-sdk-go-v2/service/appintegrations"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	ai "github.com/blackbirdworks/gopherstack/services/appintegrations"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := ai.NewHandler(ai.NewInMemoryBackend(testAccount, "us-east-1"))
	sdkcheck.CheckCompleteness(t, &sdk.Client{}, h.GetSupportedOperations(), []string{})
}
