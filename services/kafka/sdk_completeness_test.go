package kafka_test

import (
	"testing"

	kafkasdk "github.com/aws/aws-sdk-go-v2/service/kafka"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/kafka"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// kafka client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := kafka.NewInMemoryBackend("000000000000", "us-east-1")
	h := kafka.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &kafkasdk.Client{}, h.GetSupportedOperations(), []string{})
}
