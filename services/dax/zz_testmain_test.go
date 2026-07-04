package dax_test

import (
	"os"
	"testing"
)

// TestMain forces synchronous DAX state transitions for the whole package's
// tests. In production, cluster/node lifecycle changes (CREATING -> AVAILABLE,
// reboot, replication-factor changes) complete asynchronously; the emulator
// honours DAX_TEST_SYNC=1 to apply them immediately so tests are deterministic
// without sleeping or polling. CI runs plain `go test`, so the tests must set
// this themselves rather than rely on the environment.
func TestMain(m *testing.M) {
	if err := os.Setenv("DAX_TEST_SYNC", "1"); err != nil {
		panic(err)
	}

	os.Exit(m.Run())
}
