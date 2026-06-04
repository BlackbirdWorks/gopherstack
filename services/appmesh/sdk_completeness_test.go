package appmesh_test

import (
	"testing"

	appmeshsdk "github.com/aws/aws-sdk-go-v2/service/appmesh"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/appmesh"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// appmesh client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := appmesh.NewInMemoryBackend("000000000000", "us-east-1")
	h := appmesh.NewHandler(backend)

	// All 37 AppMesh SDK operations are implemented.
	notImplemented := []string{}

	sdkcheck.CheckCompleteness(t, &appmeshsdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
