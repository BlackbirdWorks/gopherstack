package rds_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

// TestMain asserts rds tests leave no goroutines running, guarding the
// background reconciler against leaks.
func TestMain(m *testing.M) {
	testleak.VerifyTestMain(m)
}
