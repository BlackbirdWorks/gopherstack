package dms_test

import (
	"testing"

	dmssdk "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/dms"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// databasemigrationservice client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := dms.NewInMemoryBackend("000000000000", "us-east-1")
	h := dms.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &dmssdk.Client{}, h.GetSupportedOperations(), []string{})
}
