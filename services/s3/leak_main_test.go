package s3_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

// TestMain asserts s3 tests leave no goroutines running, guarding background
// workers and per-request goroutines against leaks.
func TestMain(m *testing.M) {
	testleak.VerifyTestMain(m)
}
