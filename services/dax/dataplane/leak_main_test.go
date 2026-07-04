package dataplane_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

// TestMain asserts the DAX data-plane tests leave no goroutines running, which
// guards the accept/serve goroutines and per-connection handlers against leaks.
func TestMain(m *testing.M) {
	testleak.VerifyTestMain(m)
}
