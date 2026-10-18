package oam_test

import (
	"testing"

	oamsdk "github.com/aws/aws-sdk-go-v2/service/oam"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/oam"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := oam.NewHandler(oam.NewInMemoryBackend("123456789012", "us-east-1"))
	sdkcheck.CheckCompleteness(t, &oamsdk.Client{}, h.GetSupportedOperations(), []string{})
}
