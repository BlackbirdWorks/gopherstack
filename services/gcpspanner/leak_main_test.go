package gcpspanner_test

import (
	"testing"

	"go.uber.org/goleak"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

func TestMain(m *testing.M) {
	// The Spanner client probes the GCE metadata server in the background; the dial may still be pending at exit.
	testleak.VerifyTestMain(m,
		goleak.IgnoreAnyFunction("net/http.(*Transport).dialConnFor"),
		goleak.IgnoreAnyFunction("cloud.google.com/go/compute/metadata.init.func2"),
	)
}
