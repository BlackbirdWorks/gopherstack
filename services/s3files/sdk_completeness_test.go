package s3files_test

import (
	"testing"

	s3filessdk "github.com/aws/aws-sdk-go-v2/service/s3files"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/s3files"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := s3files.NewHandler(s3files.NewInMemoryBackend(testAccount, testRegion))
	sdkcheck.CheckCompleteness(t, &s3filessdk.Client{}, h.GetSupportedOperations(), []string{})
}
