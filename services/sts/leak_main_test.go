package sts_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

// TestMain asserts sts tests leave no goroutines running, guarding
// background workers against leaks.
func TestMain(m *testing.M) {
	testleak.VerifyTestMain(m)
}
