package pipes_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

// TestMain asserts pipes tests leave no goroutines running, guarding the runner
// poll loop and per-pipe goroutines against leaks.
func TestMain(m *testing.M) {
	testleak.VerifyTestMain(m)
}
