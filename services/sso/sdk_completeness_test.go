package sso_test

import (
	"testing"

	ssosdk "github.com/aws/aws-sdk-go-v2/service/sso"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/sso"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	sdkcheck.CheckCompleteness(t, &ssosdk.Client{}, sso.NewHandler().GetSupportedOperations(), nil)
}
