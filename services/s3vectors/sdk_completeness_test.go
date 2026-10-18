package s3vectors_test

import (
	"testing"

	sdk "github.com/aws/aws-sdk-go-v2/service/s3vectors"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/s3vectors"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := s3vectors.NewHandler(s3vectors.NewInMemoryBackend("123456789012", "us-east-1"))
	sdkcheck.CheckCompleteness(t, &sdk.Client{}, h.GetSupportedOperations(), []string{})
}
