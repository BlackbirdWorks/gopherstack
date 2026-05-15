package docdb_test

import (
	"testing"

	docdbsdk "github.com/aws/aws-sdk-go-v2/service/docdb"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/docdb"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// docdb client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := docdb.NewInMemoryBackend("000000000000", "us-east-1")
	h := docdb.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &docdbsdk.Client{}, h.GetSupportedOperations(), []string{})
}
