package polly_test

import (
	"testing"

	pollysdk "github.com/aws/aws-sdk-go-v2/service/polly"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/polly"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	handler := polly.NewHandler(polly.NewInMemoryBackend())
	sdkcheck.CheckCompleteness(t, &pollysdk.Client{}, handler.GetSupportedOperations(), []string{})
}
