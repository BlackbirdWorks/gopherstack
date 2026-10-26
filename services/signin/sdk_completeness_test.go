package signin_test

import (
	"testing"

	signinsdk "github.com/aws/aws-sdk-go-v2/service/signin"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/signin"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := signin.NewHandler(signin.NewInMemoryBackend(testAccount, testRegion))
	sdkcheck.CheckCompleteness(t, &signinsdk.Client{}, h.GetSupportedOperations(), []string{})
}
