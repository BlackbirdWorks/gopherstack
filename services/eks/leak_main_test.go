package eks_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

// TestMain asserts eks tests leave no goroutines running, guarding the
// scheduled cluster/nodegroup state-transition timers against leaks.
func TestMain(m *testing.M) {
	testleak.VerifyTestMain(m)
}
