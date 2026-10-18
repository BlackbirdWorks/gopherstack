package resourceexplorer2_test

import (
	"testing"

	sdk "github.com/aws/aws-sdk-go-v2/service/resourceexplorer2"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/resourceexplorer2"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := resourceexplorer2.NewHandler(resourceexplorer2.NewInMemoryBackend("123456789012", "us-east-1"))
	sdkcheck.CheckCompleteness(t, &sdk.Client{}, h.GetSupportedOperations(), []string{
		"GetServiceIndex", "GetServiceView", "ListServiceIndexes", "ListServiceViews",
		"ListStreamingAccessForServices",
	})
}
