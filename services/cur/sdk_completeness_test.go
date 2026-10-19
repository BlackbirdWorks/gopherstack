package cur_test

import (
	"testing"

	cursdk "github.com/aws/aws-sdk-go-v2/service/costandusagereportservice"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/cur"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := cur.NewHandler(cur.NewInMemoryBackend("123456789012", "us-east-1"))
	sdkcheck.CheckCompleteness(t, &cursdk.Client{}, h.GetSupportedOperations(), []string{})
}
