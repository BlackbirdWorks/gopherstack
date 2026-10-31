package ocicontainerinstances_test

import (
	"os"
	"testing"

	"go.uber.org/goleak"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

func TestMain(m *testing.M) {
	// The SDK's default policy retries 409 for minutes, which tests assert on directly.
	_ = os.Setenv("OCI_SDK_DEFAULT_RETRY_ENABLED", "false")

	// The embedded OCI registry used by the pull test keeps a notification broadcaster for its lifetime.
	testleak.VerifyTestMain(m,
		goleak.IgnoreTopFunction("net/http.setRequestCancel.func4"),
		goleak.IgnoreTopFunction("github.com/docker/go-events.(*Broadcaster).run"))
}
