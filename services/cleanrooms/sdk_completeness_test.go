package cleanrooms_test

import (
	"testing"

	cleanroomssdk "github.com/aws/aws-sdk-go-v2/service/cleanrooms"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/cleanrooms"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := cleanrooms.NewInMemoryBackend("000000000000", "us-east-1")
	h := cleanrooms.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &cleanroomssdk.Client{}, h.GetSupportedOperations(), []string{})
}
