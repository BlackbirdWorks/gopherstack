package cloudfront_test

import (
	"testing"

	cloudfrontsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	cfkvssdk "github.com/aws/aws-sdk-go-v2/service/cloudfrontkeyvaluestore"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// cloudfront client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend("000000000000", "us-east-1")
	h := cloudfront.NewHandler(backend)

	// keyValueStoreDataPlaneOps are the CloudFront KeyValueStore *data-plane*
	// operations (reading/writing individual key-value pairs within a
	// store). AWS models these on a separate SDK client,
	// cloudfrontkeyvaluestore.Client, distinct from the main
	// cloudfront.Client used for store *management*
	// (CreateKeyValueStore/DescribeKeyValueStore/ListKeyValueStores/
	// DeleteKeyValueStore/UpdateKeyValueStore, checked against
	// cloudfrontsdk.Client below). gopherstack's single Handler implements
	// both surfaces and reports them together from GetSupportedOperations(),
	// so this test splits them before checking each half against the SDK
	// client that actually owns it.
	keyValueStoreDataPlaneOps := map[string]bool{
		"DeleteKey":  true,
		"GetKey":     true,
		"ListKeys":   true,
		"PutKey":     true,
		"UpdateKeys": true,
	}

	var mainOps, kvsOps []string
	for _, op := range h.GetSupportedOperations() {
		if keyValueStoreDataPlaneOps[op] {
			kvsOps = append(kvsOps, op)
		} else {
			mainOps = append(mainOps, op)
		}
	}

	sdkcheck.CheckCompleteness(t, &cloudfrontsdk.Client{}, mainOps, []string{})
	// cloudfrontkeyvaluestore.Client also exposes DescribeKeyValueStore (a
	// data-plane read of a store's ETag/status). gopherstack does not
	// implement that data-plane variant -- the DescribeKeyValueStore checked
	// above, against cloudfrontsdk.Client, is the unrelated store-management
	// operation that happens to share the same name.
	sdkcheck.CheckCompleteness(t, &cfkvssdk.Client{}, kvsOps, []string{"DescribeKeyValueStore"})
}
