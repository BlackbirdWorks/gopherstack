package personalize_test

import (
	"testing"

	personalizesdk "github.com/aws/aws-sdk-go-v2/service/personalize"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/personalize"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := personalize.NewHandler(personalize.NewInMemoryBackend("000000000000", "us-east-1"))
	sdkcheck.CheckCompleteness(t, &personalizesdk.Client{}, h.GetSupportedOperations(), []string{})
}
