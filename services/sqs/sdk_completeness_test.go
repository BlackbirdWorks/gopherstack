package sqs_test

import (
	"testing"

	sqssdk "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/sqs"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// sqs client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := sqs.NewInMemoryBackend()
	t.Cleanup(backend.Close)
	h := sqs.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &sqssdk.Client{}, h.GetSupportedOperations(), []string{})
}
