package rum_test

import (
	"testing"

	awsrum "github.com/aws/aws-sdk-go-v2/service/rum"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/rum"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := rum.NewHandler(rum.NewInMemoryBackend("123456789012", "us-east-1"))
	sdkcheck.CheckCompleteness(t, &awsrum.Client{}, h.GetSupportedOperations(), []string{})
}
