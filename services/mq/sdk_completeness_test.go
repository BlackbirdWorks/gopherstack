package mq_test

import (
	"testing"

	mqsdk "github.com/aws/aws-sdk-go-v2/service/mq"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/mq"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// mq client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := mq.NewInMemoryBackend("000000000000", "us-east-1")
	h := mq.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &mqsdk.Client{}, h.GetSupportedOperations(), []string{})
}
