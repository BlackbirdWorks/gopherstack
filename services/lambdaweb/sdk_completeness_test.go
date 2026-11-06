package lambdaweb_test

import (
	"testing"

	lwsdk "github.com/aws/aws-sdk-go-v2/service/lambdaweb"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/lambdaweb"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := lambdaweb.NewHandler(lambdaweb.NewInMemoryBackend(testAccountID))
	sdkcheck.CheckCompleteness(t, &lwsdk.Client{}, h.GetSupportedOperations(), nil)
}
