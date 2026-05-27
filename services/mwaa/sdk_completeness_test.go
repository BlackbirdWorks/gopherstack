package mwaa_test

import (
	"testing"

	mwaasdk "github.com/aws/aws-sdk-go-v2/service/mwaa"
	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/mwaa"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()
	h := mwaa.NewHandler(nil)
	sdkcheck.CheckCompleteness(t, &mwaasdk.Client{}, h.GetSupportedOperations(), []string{})
}
