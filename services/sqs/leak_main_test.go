package sqs_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

// TestMain asserts sqs tests leave no goroutines running, guarding the backend
// janitor and per-request goroutines against leaks.
func TestMain(m *testing.M) {
	testleak.VerifyTestMain(m)
}
