package ssooidc_test

import (
	"testing"

	ssooidcsdk "github.com/aws/aws-sdk-go-v2/service/ssooidc"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/ssooidc"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := ssooidc.NewHandler(ssooidc.NewInMemoryBackend())
	sdkcheck.CheckCompleteness(t, &ssooidcsdk.Client{}, h.GetSupportedOperations(), nil)
}
