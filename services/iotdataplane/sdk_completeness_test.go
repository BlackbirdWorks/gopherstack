package iotdataplane_test

import (
	"testing"

	iotdataplanesdk "github.com/aws/aws-sdk-go-v2/service/iotdataplane"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/iotdataplane"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// iotdataplane client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := iotdataplane.NewInMemoryBackend()
	h := iotdataplane.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &iotdataplanesdk.Client{}, h.GetSupportedOperations(), []string{})
}
