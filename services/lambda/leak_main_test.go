package lambda_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

// TestMain asserts lambda tests leave no goroutines running, guarding the
// function-URL servers and background workers against leaks.
func TestMain(m *testing.M) {
	testleak.VerifyTestMain(m)
}
