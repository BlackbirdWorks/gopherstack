package kinesisvideo_test

import (
	"testing"

	kinesisvideosdk "github.com/aws/aws-sdk-go-v2/service/kinesisvideo"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/kinesisvideo"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := kinesisvideo.NewHandler(kinesisvideo.NewInMemoryBackend())
	notImplemented := []string{"DescribeMappedResourceConfiguration"}
	sdkcheck.CheckCompleteness(t, &kinesisvideosdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
