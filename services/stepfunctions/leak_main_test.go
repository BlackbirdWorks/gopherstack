package stepfunctions_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

// TestMain asserts stepfunctions tests leave no goroutines running, guarding
// background workers and integration backends against leaks.
func TestMain(m *testing.M) {
	testleak.VerifyTestMain(m)
}
