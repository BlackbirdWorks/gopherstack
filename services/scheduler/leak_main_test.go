package scheduler_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

// TestMain asserts scheduler tests leave no goroutines running, guarding the
// runner's poll loop and per-schedule goroutines against leaks.
func TestMain(m *testing.M) {
	testleak.VerifyTestMain(m)
}
