package kafkaconnect_test

import (
	"testing"

	kafkaconnectsdk "github.com/aws/aws-sdk-go-v2/service/kafkaconnect"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/kafkaconnect"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := kafkaconnect.NewHandler(kafkaconnect.NewInMemoryBackend())
	sdkcheck.CheckCompleteness(t, &kafkaconnectsdk.Client{}, h.GetSupportedOperations(), nil)
}
