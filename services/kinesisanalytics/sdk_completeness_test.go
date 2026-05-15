package kinesisanalytics_test

import (
	"testing"

	kinesisanalyticssdk "github.com/aws/aws-sdk-go-v2/service/kinesisanalytics"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/kinesisanalytics"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// kinesisanalytics client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := kinesisanalytics.NewInMemoryBackend("us-east-1", "000000000000")
	h := kinesisanalytics.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &kinesisanalyticssdk.Client{}, h.GetSupportedOperations(), []string{})
}
