package mediastore_test

import (
	"testing"

	mediastoresdk "github.com/aws/aws-sdk-go-v2/service/mediastore"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/mediastore"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()
	h := mediastore.NewHandler(nil)
	sdkcheck.CheckCompleteness(t, &mediastoresdk.Client{}, h.GetSupportedOperations(), []string{})
}
