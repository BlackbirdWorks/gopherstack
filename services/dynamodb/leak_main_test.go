package dynamodb_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

// TestMain asserts dynamodb tests leave no goroutines running, guarding background
// workers and per-request goroutines against leaks.
func TestMain(m *testing.M) {
	testleak.VerifyTestMain(m)
}
