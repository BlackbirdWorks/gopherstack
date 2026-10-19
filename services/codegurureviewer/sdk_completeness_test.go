package codegurureviewer_test

import (
	"testing"

	sdk "github.com/aws/aws-sdk-go-v2/service/codegurureviewer"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	cg "github.com/blackbirdworks/gopherstack/services/codegurureviewer"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := cg.NewHandler(cg.NewInMemoryBackend(testAccount, "us-east-1"))
	sdkcheck.CheckCompleteness(t, &sdk.Client{}, h.GetSupportedOperations(), []string{})
}
