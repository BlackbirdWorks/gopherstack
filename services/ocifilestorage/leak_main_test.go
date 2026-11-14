package ocifilestorage_test

import (
	"os"
	"testing"

	"go.uber.org/goleak"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

func TestMain(m *testing.M) {
	// The SDK's default policy retries 409 for minutes, which tests assert on directly.
	_ = os.Setenv("OCI_SDK_DEFAULT_RETRY_ENABLED", "false")

	testleak.VerifyTestMain(m,
		goleak.IgnoreTopFunction("net/http.setRequestCancel.func4"),
		// the NFS test client starts a cache janitor that has no stop
		goleak.IgnoreAnyFunction("github.com/willscott/go-nfs-client/nfs.(*Target).cleanupCache"),
	)
}
