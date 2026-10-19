package connect_test

import (
	"testing"

	sdk "github.com/aws/aws-sdk-go-v2/service/connect"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	cn "github.com/blackbirdworks/gopherstack/services/connect"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := cn.NewHandler(cn.NewBackend(testAccount, "us-east-1"))
	sdkcheck.CheckCompleteness(t, &sdk.Client{}, h.GetSupportedOperations(), notImplemented())
}
