package glue_test

import (
	"testing"

	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/glue"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// glue client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend("000000000000", "us-east-1")
	h := glue.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &gluesdk.Client{}, h.GetSupportedOperations(), []string{})
}
