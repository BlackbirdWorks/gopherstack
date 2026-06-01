package polly_test

import (
	"testing"

	pollysdk "github.com/aws/aws-sdk-go-v2/service/polly"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/polly"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()
	h := polly.NewHandler(nil)
	sdkcheck.CheckCompleteness(t, &pollysdk.Client{}, h.GetSupportedOperations(), []string{})
}
