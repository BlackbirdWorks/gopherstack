package amp_test

import (
	"testing"

	ampsdk "github.com/aws/aws-sdk-go-v2/service/amp"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/amp"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := amp.NewHandler(amp.NewInMemoryBackend(testAccount, "us-east-1"))
	sdkcheck.CheckCompleteness(t, &ampsdk.Client{}, h.GetSupportedOperations(), []string{})
}
