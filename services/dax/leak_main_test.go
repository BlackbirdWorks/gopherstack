package dax_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

// TestMain asserts dax tests leave no goroutines running. Cluster/node
// lifecycle transitions (CREATING -> AVAILABLE, reboot, replication-factor
// changes) are applied lazily by sweepClusterTransitionsLocked instead of a
// background goroutine, so there is nothing left running for goleak to catch
// -- this guards against that regressing.
func TestMain(m *testing.M) {
	testleak.VerifyTestMain(m)
}
