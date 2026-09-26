package ssm_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

// TestMain asserts ssm tests leave no goroutines running, guarding
// background workers against leaks.
func TestMain(m *testing.M) {
	testleak.VerifyTestMain(m)
}
