package ecrpublic_test

import (
	"testing"

	ecrpublicsdk "github.com/aws/aws-sdk-go-v2/service/ecrpublic"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/ecrpublic"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := ecrpublic.NewHandler(ecrpublic.NewInMemoryBackend("000000000000", "us-east-1"))
	sdkcheck.CheckCompleteness(t, &ecrpublicsdk.Client{}, h.GetSupportedOperations(), nil)
}
