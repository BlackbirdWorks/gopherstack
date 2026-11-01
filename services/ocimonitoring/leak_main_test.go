package ocimonitoring_test

import (
	"os"
	"testing"

	"go.uber.org/goleak"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

func TestMain(m *testing.M) {
	// The SDK's default policy retries 409 IncorrectState for minutes, which tests assert on directly.
	_ = os.Setenv("OCI_SDK_DEFAULT_RETRY_ENABLED", "false")

	// The OCI SDK client arms a per-request timeout timer that outlives unread response bodies.
	testleak.VerifyTestMain(m, goleak.IgnoreTopFunction("net/http.setRequestCancel.func4"))
}
