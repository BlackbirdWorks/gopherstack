package globalaccelerator_test

import (
	"testing"

	gasdk "github.com/aws/aws-sdk-go-v2/service/globalaccelerator"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/globalaccelerator"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := globalaccelerator.NewHandler(globalaccelerator.NewInMemoryBackend("123456789012", "us-west-2"))
	sdkcheck.CheckCompleteness(t, &gasdk.Client{}, h.GetSupportedOperations(), []string{})
}
