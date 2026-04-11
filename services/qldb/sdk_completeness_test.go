package qldb_test

import (
	"testing"

	qldbsdk "github.com/aws/aws-sdk-go-v2/service/qldb" //nolint:staticcheck // AWS has deprecated this service; gopherstack still supports it
	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/qldb"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// qldb client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := qldb.NewInMemoryBackend("000000000000", "us-east-1")
	h := qldb.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &qldbsdk.Client{}, h.GetSupportedOperations(), []string{
		"StreamJournalToKinesis",
		"UpdateLedgerPermissionsMode",
	})
}
