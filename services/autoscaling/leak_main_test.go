package autoscaling_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

// TestMain asserts autoscaling tests leave no goroutines running, guarding the
// backend's scheduled state-transition timers against leaks.
func TestMain(m *testing.M) {
	testleak.VerifyTestMain(m)
}
