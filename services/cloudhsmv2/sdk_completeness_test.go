package cloudhsmv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/cloudhsmv2"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	hsm "github.com/blackbirdworks/gopherstack/services/cloudhsmv2"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := hsm.NewHandler(hsm.NewInMemoryBackend("123456789012", "us-east-1"))
	sdkcheck.CheckCompleteness(t, &cloudhsmv2.Client{}, h.GetSupportedOperations(), []string{})
}
