package cloudfrontkeyvaluestore_test

import (
	"testing"

	cfkvssdk "github.com/aws/aws-sdk-go-v2/service/cloudfrontkeyvaluestore"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/cloudfrontkeyvaluestore"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// cloudfrontkeyvaluestore client is listed in GetSupportedOperations(). The
// test fails when the upstream SDK adds a new operation this package hasn't
// handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := cloudfrontkeyvaluestore.NewHandler(nil)

	sdkcheck.CheckCompleteness(t, &cfkvssdk.Client{}, h.GetSupportedOperations(), []string{})
}
