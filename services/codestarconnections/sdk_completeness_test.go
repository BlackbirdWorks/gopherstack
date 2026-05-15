package codestarconnections_test

import (
	"testing"

	codestarconnectionssdk "github.com/aws/aws-sdk-go-v2/service/codestarconnections"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/codestarconnections"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// codestarconnections client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := codestarconnections.NewInMemoryBackend("000000000000", "us-east-1")
	h := codestarconnections.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &codestarconnectionssdk.Client{}, h.GetSupportedOperations(), []string{})
}
