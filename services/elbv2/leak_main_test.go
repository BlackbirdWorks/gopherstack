package elbv2_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

// TestMain asserts elbv2 tests leave no goroutines running, guarding the
// background health reconciler against leaks.
func TestMain(m *testing.M) {
	testleak.VerifyTestMain(m)
}
