package secretsmanager_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

// TestMain asserts secretsmanager tests leave no goroutines running, guarding
// the background rotation scheduler against leaks.
func TestMain(m *testing.M) {
	testleak.VerifyTestMain(m)
}
