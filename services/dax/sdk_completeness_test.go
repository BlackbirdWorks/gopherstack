package dax_test

import (
	"testing"

	daxsdk "github.com/aws/aws-sdk-go-v2/service/dax"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/dax"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()
	h := dax.NewHandler(nil)
	sdkcheck.CheckCompleteness(t, &daxsdk.Client{}, h.GetSupportedOperations(), []string{})
}
