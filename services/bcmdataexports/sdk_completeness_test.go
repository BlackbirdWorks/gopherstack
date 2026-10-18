package bcmdataexports_test

import (
	"testing"

	bcmsdk "github.com/aws/aws-sdk-go-v2/service/bcmdataexports"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/bcmdataexports"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := bcmdataexports.NewHandler(bcmdataexports.NewInMemoryBackend("123456789012", "us-east-1"))
	sdkcheck.CheckCompleteness(t, &bcmsdk.Client{}, h.GetSupportedOperations(), []string{})
}
