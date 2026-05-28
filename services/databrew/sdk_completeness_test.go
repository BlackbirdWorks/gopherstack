package databrew_test

import (
	"testing"

	databrewsdk "github.com/aws/aws-sdk-go-v2/service/databrew"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/databrew"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := databrew.NewInMemoryBackend("000000000000", "us-east-1")
	h := databrew.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &databrewsdk.Client{}, h.GetSupportedOperations(), []string{})
}
