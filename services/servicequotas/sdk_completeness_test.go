package servicequotas_test

import (
	"testing"

	sqsdk "github.com/aws/aws-sdk-go-v2/service/servicequotas"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/servicequotas"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := servicequotas.NewHandler(servicequotas.NewInMemoryBackend("123456789012", "us-east-1"))
	sdkcheck.CheckCompleteness(t, &sqsdk.Client{}, h.GetSupportedOperations(), []string{})
}
