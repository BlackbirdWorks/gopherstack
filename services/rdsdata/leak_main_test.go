package rdsdata_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

// TestMain asserts rdsdata tests leave no goroutines running, guarding the
// per-resource SQLite engine's database/sql connectionOpener against leaks.
func TestMain(m *testing.M) {
	testleak.VerifyTestMain(m)
}
